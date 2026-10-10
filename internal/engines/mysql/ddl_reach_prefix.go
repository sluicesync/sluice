// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "strings"

// # Bounding a statement the parser cannot read (Bug 301 review)
//
// The look-ahead classifies every DDL on the SERVER, not only the stream's
// databases, and a statement the Vitess parser cannot read used to reach
// every table. Plenty of ordinary DDL is outside Vitess's grammar — MariaDB's
// UUID/INET6 column types, `ADD COLUMN IF NOT EXISTS`, `CREATE OR REPLACE
// TABLE`, sequences, system versioning; MySQL's `CREATE TABLESPACE`,
// `ALTER INSTANCE`, resource groups, `SECONDARY_ENGINE` — so one such
// statement in any database on a shared server refused every stream behind
// the head, on every retry.
//
// [classifyUnparsedDDL] reads only the statement's fixed PREFIX — the
// keywords before the object name and the name itself, the part of every
// DDL grammar that names its target — and bounds the statement to that
// target. It never interprets what the statement does to the target: a
// bounded table is treated as reshaped (not neutral, not in place), so for
// the stream's own tables the answer is the same refusal as before, and only
// statements that provably name no table, or name tables elsewhere, stop
// reaching it. A prefix it does not recognise stays unbounded.

// ddlToken is one word of a statement prefix: an identifier or keyword, a
// backtick-quoted identifier (quoted), or one punctuation character.
type ddlToken struct {
	text   string
	quoted bool
}

// is reports whether the token is the unquoted keyword kw.
func (t ddlToken) is(kw string) bool { return !t.quoted && strings.EqualFold(t.text, kw) }

// ddlPrefixTokens tokenises stmt's prefix: words, backtick identifiers and
// the punctuation `.`, `,`, `(`, `)`, `;`, `=`. Plain comments are skipped.
// It stops at the first quote character it does not handle (a string
// literal, which no DDL prefix reaches before its target) and at any comment
// the server may execute (`/*!…*/`, `/*+…*/`), so a target hidden in one
// leaves the statement unbounded.
func ddlPrefixTokens(stmt string) []ddlToken {
	var out []ddlToken
	s := stmt
	for len(out) < 64 {
		s = strings.TrimLeft(stripLeadingSQLComments(s), " \t\r\n")
		if s == "" {
			return out
		}
		switch c := s[0]; {
		case c == '`':
			var b strings.Builder
			i := 1
			for ; i < len(s); i++ {
				if s[i] == '`' {
					if i+1 < len(s) && s[i+1] == '`' {
						b.WriteByte('`')
						i++
						continue
					}
					break
				}
				b.WriteByte(s[i])
			}
			if i >= len(s) {
				return out
			}
			out = append(out, ddlToken{text: b.String(), quoted: true})
			s = s[i+1:]
		case strings.IndexByte(".,();=", c) >= 0:
			out = append(out, ddlToken{text: string(c)})
			s = s[1:]
		case c == '\'' || c == '"' || strings.HasPrefix(s, "/*"):
			// A string literal, or a comment stripLeadingSQLComments keeps:
			// a versioned `/*!50100 t */` or an optimizer hint, which the
			// server may EXECUTE. Its content may be the target, so stop —
			// what follows cannot be bounded.
			return out
		default:
			i := 0
			for i < len(s) && strings.IndexByte(" \t\r\n`.,();='\"", s[i]) < 0 {
				i++
			}
			if w := s[:i]; strings.Contains(w, "/*") || strings.Contains(w, "*/") {
				return out
			}
			out = append(out, ddlToken{text: s[:i]})
			s = s[i:]
		}
	}
	return out
}

// noTableObjects are the object keywords whose CREATE/ALTER/DROP names no
// table and changes no table's columns.
var noTableObjects = []string{
	"DATABASE", "SCHEMA", "VIEW", "TRIGGER", "PROCEDURE", "FUNCTION", "EVENT",
	"TABLESPACE", "LOGFILE", "SERVER", "INSTANCE", "RESOURCE", "SPATIAL", "UNDO",
	"USER", "ROLE", "REFERENCE",
}

// isObjectKeyword reports whether t is one of [noTableObjects].
func isObjectKeyword(t ddlToken) bool {
	for _, kw := range noTableObjects {
		if t.is(kw) {
			return true
		}
	}
	return false
}

// prefixReader walks a statement prefix's tokens.
type prefixReader struct {
	toks          []ddlToken
	i             int
	defaultSchema string
}

func (p *prefixReader) peek(kw string) bool { return p.i < len(p.toks) && p.toks[p.i].is(kw) }

// accept consumes the next token if it is kw.
func (p *prefixReader) accept(kw string) bool {
	if p.peek(kw) {
		p.i++
		return true
	}
	return false
}

// skip consumes any run of the given keywords.
func (p *prefixReader) skip(kws ...string) {
	for {
		matched := false
		for _, kw := range kws {
			matched = matched || p.accept(kw)
		}
		if !matched {
			return
		}
	}
}

// skipClauses consumes the `DEFINER=…`, `ALGORITHM=…` and `SQL SECURITY x`
// clauses the server writes between CREATE and the object keyword.
func (p *prefixReader) skipClauses() {
	for p.i+1 < len(p.toks) {
		switch {
		case (p.peek("DEFINER") || p.peek("ALGORITHM")) && p.toks[p.i+1].text == "=":
			p.i += 2
			for p.i < len(p.toks) && !p.toks[p.i].quoted && !isObjectKeyword(p.toks[p.i]) {
				p.i++
			}
			for p.i < len(p.toks) && (p.toks[p.i].quoted || p.toks[p.i].text == "@") {
				p.i++
			}
		case p.peek("SQL") && p.toks[p.i+1].is("SECURITY") && p.i+2 < len(p.toks):
			p.i += 3
		default:
			return
		}
	}
}

// punct reports whether the next token is the punctuation c.
func (p *prefixReader) punct(c string) bool {
	return p.i < len(p.toks) && !p.toks[p.i].quoted && p.toks[p.i].text == c
}

// name reads a `schema.table` or `table` name; an unqualified one takes the
// default database, and with none it cannot be bounded.
func (p *prefixReader) name() (ddlTableRef, bool) {
	if p.i >= len(p.toks) || (!p.toks[p.i].quoted && strings.ContainsAny(p.toks[p.i].text[:1], ".,();=")) {
		return ddlTableRef{}, false
	}
	first := p.toks[p.i].text
	p.i++
	if p.punct(".") && p.i+1 < len(p.toks) {
		second := p.toks[p.i+1].text
		p.i += 2
		return ddlTableRef{schema: first, table: second}, true
	}
	if p.defaultSchema == "" {
		return ddlTableRef{}, false
	}
	return ddlTableRef{schema: p.defaultSchema, table: first}, true
}

// names reads a comma-separated name list.
func (p *prefixReader) names() (ddlReach, bool) {
	var out ddlReach
	for {
		ref, ok := p.name()
		if !ok {
			return ddlReach{}, false
		}
		out.tables = append(out.tables, ref)
		if !p.punct(",") {
			return out, true
		}
		p.i++
	}
}

// renames reads RENAME TABLE's `a TO b[, c TO d]` list.
func (p *prefixReader) renames() (ddlReach, bool) {
	var out ddlReach
	for {
		from, ok := p.name()
		if !ok || !p.accept("TO") {
			return ddlReach{}, false
		}
		to, ok := p.name()
		if !ok {
			return ddlReach{}, false
		}
		out.tables = append(out.tables, from, to)
		if !p.punct(",") {
			return out, true
		}
		p.i++
	}
}

// classifyUnparsedDDL bounds a statement the parser could not read to the
// tables its prefix names. ok is false when the prefix is not one it reads.
func classifyUnparsedDDL(stmt, defaultSchema string) (ddlReach, bool) {
	p := &prefixReader{toks: ddlPrefixTokens(stmt), defaultSchema: defaultSchema}
	switch {
	case p.accept("CHECK") || p.accept("CHECKSUM") || p.accept("ANALYZE") || p.accept("OPTIMIZE") ||
		p.accept("REPAIR") || p.accept("TRUNCATE"):
		return ddlReach{neutral: true}, true
	case p.accept("RENAME"):
		p.skip("TABLE", "TABLES")
		return p.renames()
	case p.peek("CREATE") || p.peek("ALTER") || p.peek("DROP"):
		return p.objectDDL()
	}
	return ddlReach{}, false
}

// objectDDL reads CREATE/ALTER/DROP <object> <name>.
func (p *prefixReader) objectDDL() (ddlReach, bool) {
	drop := p.peek("DROP")
	p.i++
	p.skip("OR", "REPLACE", "ONLINE", "OFFLINE", "IGNORE", "TEMPORARY", "UNIQUE", "FULLTEXT", "SPATIAL", "AGGREGATE")
	p.skipClauses()
	p.skip("OR", "REPLACE")
	switch {
	case p.accept("TABLE") || p.accept("SEQUENCE"):
		p.skip("IF", "NOT", "EXISTS")
		if drop {
			return p.names()
		}
		ref, ok := p.name()
		if !ok {
			return ddlReach{}, false
		}
		return ddlReach{tables: []ddlTableRef{ref}}, true
	case p.accept("INDEX") || p.accept("KEY"):
		// CREATE/DROP INDEX x ON t: bounded to t.
		p.skip("IF", "NOT", "EXISTS")
		if _, ok := p.name(); !ok || !p.accept("ON") {
			return ddlReach{}, false
		}
		ref, ok := p.name()
		if !ok {
			return ddlReach{}, false
		}
		return ddlReach{tables: []ddlTableRef{ref}}, true
	case p.accept("DATABASE") || p.accept("SCHEMA"):
		if !drop {
			return ddlReach{neutral: true}, true
		}
		p.skip("IF", "EXISTS")
		if p.i < len(p.toks) {
			return ddlReach{databases: []string{p.toks[p.i].text}}, true
		}
		return ddlReach{}, false
	case p.i < len(p.toks) && isObjectKeyword(p.toks[p.i]):
		return ddlReach{neutral: true}, true
	}
	return ddlReach{}, false
}
