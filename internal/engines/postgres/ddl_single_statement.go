// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The C1-1 single-statement door (audit 2026-08-11).
//
// Every DDL string sluice's emitters produce is ONE statement — and the
// restore path inlines RECORDED expression bodies (CHECK constraints,
// generated columns, defaults) from a backup manifest verbatim into
// that DDL, then executes it through pgx's simple protocol, which runs
// multi-statement strings. A tampered or corrupted manifest whose
// expression body closes the surrounding syntax and appends `; DROP
// TABLE …; --` therefore executed arbitrary SQL as the restore role,
// at exit 0, with `backup verify` green (observed live by the audit:
// a canary table dropped mid-restore). Signature verification catches
// tampering when it is ENABLED; this door holds regardless, on the
// structural invariant the emitters already guarantee: one statement.
//
// The validator understands the LEXICAL constructs PostgreSQL does, so
// a top-level `;` cannot hide inside one of them: standard single-quoted
// literals ('' doubling; the read boundary and the 74299e34 escaping
// chunk guarantee no backslash-escape spellings reach the IR),
// double-quoted identifiers ("" doubling), dollar-quoted bodies (the
// enum-guard DO $$ … $$ block carries internal semicolons; a `$` that
// merely CONTINUES an identifier — `col$q$` — is not a quote opener, per
// PG's rule that a dollar-quote delimiter must not abut a preceding
// identifier byte), `--` line comments, and `/* … */` block comments
// (which NEST in PostgreSQL). One optional trailing semicolon is
// allowed; anything after it, an unbalanced paren, or an unterminated
// quote/comment refuses with SLUICE-E-DDL-EMIT-MULTI-STATEMENT before
// the statement reaches the server.
//
// # Why comments and the dollar-adjacency guard are load-bearing (C-1)
//
// The 2026-08-14 audit OBSERVED, end-to-end against real PostgreSQL 16,
// two bypasses of the original door: (a) it had no comment case, so an
// apostrophe inside a `/* … */` / `-- …` comment opened a
// validator-only string literal that scanned past an injected top-level
// `;` the server then executed (`… /* ' */ ); DROP TABLE canary; -- '`);
// (b) `dollarQuoteTagAt` treated any `$tag$` as an opener regardless of
// the preceding byte, so `a$q$ … ; … $q$` looked dollar-quoted to the
// validator (hiding the `;`) but is one identifier token to PG. Both
// are closed here. The DERIVED gate — TestSingleStatementDoorMatchesPG,
// which diffs the door's verdict against a real server for a corpus
// spanning every PG lexical form — is what keeps the next unenumerated
// construct from reopening the class; this hand lexer is pinned against
// the server, never against the author's imagination.

// assertSingleDDLStatement reports a non-nil error when stmt is not
// structurally a single SQL statement under PostgreSQL quoting rules.
func assertSingleDDLStatement(stmt string) error {
	depth := 0
	i := 0
	n := len(stmt)
	for i < n {
		c := stmt[i]
		switch c {
		case '\'':
			j := i + 1
			for {
				if j >= n {
					return fmt.Errorf("unterminated string literal opened at byte %d", i)
				}
				if stmt[j] == '\'' {
					if j+1 < n && stmt[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j + 1
		case '"':
			j := i + 1
			for {
				if j >= n {
					return fmt.Errorf("unterminated quoted identifier opened at byte %d", i)
				}
				if stmt[j] == '"' {
					if j+1 < n && stmt[j+1] == '"' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j + 1
		case '$':
			// A '$' that continues an identifier (`col$q$`) is not a
			// dollar-quote opener — PG lexes it as one identifier token
			// (C-1 Family A). Only a '$' NOT abutting a preceding
			// identifier byte can open a quote.
			if i > 0 && isDollarBoundaryIdentByte(stmt[i-1]) {
				i++
				continue
			}
			tag, ok := dollarQuoteTagAt(stmt, i)
			if !ok {
				i++
				continue
			}
			end := strings.Index(stmt[i+len(tag):], tag)
			if end < 0 {
				return fmt.Errorf("unterminated dollar-quoted body (%s) opened at byte %d", tag, i)
			}
			i += len(tag) + end + len(tag)
		case '-':
			// `-- …` line comment (C-1 Family B). A lone '-' (an
			// operator) falls through to default.
			if i+1 < n && stmt[i+1] == '-' {
				j := i + 2
				for j < n && stmt[j] != '\n' {
					j++
				}
				i = j
				continue
			}
			i++
		case '/':
			// `/* … */` block comment, which NESTS in PostgreSQL
			// (C-1 Family B). A lone '/' (division) falls through.
			if i+1 < n && stmt[i+1] == '*' {
				end, ok := blockCommentEnd(stmt, i)
				if !ok {
					return fmt.Errorf("unterminated block comment opened at byte %d", i)
				}
				i = end
				continue
			}
			i++
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced ')' at byte %d", i)
			}
			i++
		case ';':
			if depth != 0 {
				return fmt.Errorf("';' inside an unbalanced paren group at byte %d", i)
			}
			if rest := trimSpaceAndComments(stmt[i+1:]); rest != "" {
				return fmt.Errorf("';' at byte %d is followed by more SQL (%.40q…)", i, rest)
			}
			i = n
		default:
			i++
		}
	}
	if depth != 0 {
		return fmt.Errorf("%d unclosed '(' at end of statement", depth)
	}
	return nil
}

// isDollarBoundaryIdentByte reports whether c is a byte that, when it
// immediately precedes a '$', makes that '$' a continuation of an
// identifier rather than a dollar-quote opener (C-1 Family A). PG
// identifier bytes are letters, digits, and '_'; '$' itself is
// deliberately EXCLUDED so the untagged `$$` opener (preceded by its own
// first '$' in the pair) is still recognised — the guard fires on the
// FIRST '$' via the byte before the pair, not on the second.
func isDollarBoundaryIdentByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// blockCommentEnd returns the index just past the `*/` that closes the
// block comment opening at stmt[i] (`/*`), honouring PostgreSQL's
// NESTING rule (`/* a /* b */ c */` is one comment), or ok=false when
// the comment is unterminated.
func blockCommentEnd(stmt string, i int) (end int, ok bool) {
	depth := 0
	j := i
	n := len(stmt)
	for j+1 < n {
		switch {
		case stmt[j] == '/' && stmt[j+1] == '*':
			depth++
			j += 2
		case stmt[j] == '*' && stmt[j+1] == '/':
			depth--
			j += 2
			if depth == 0 {
				return j, true
			}
		default:
			j++
		}
	}
	return 0, false
}

// trimSpaceAndComments strips leading whitespace and complete comments
// from s so a trailing `; -- note` or `; /* note */` after the single
// statement's terminating semicolon does not read as "more SQL". An
// unterminated block comment in the tail is returned as-is (non-empty →
// the caller refuses, the loud direction for a malformed tail).
func trimSpaceAndComments(s string) string {
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < n && s[i+1] == '-':
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			end, ok := blockCommentEnd(s, i)
			if !ok {
				return s[i:] // unterminated block comment — refuse upstream
			}
			i = end
		default:
			return s[i:]
		}
	}
	return ""
}

// dollarQuoteTagAt reports the full `$tag$` opener starting at stmt[i]
// (`$$` for the untagged form), or ok=false when the '$' does not open
// a dollar quote (e.g. a bare '$' in an emitted operator). PG tags
// follow identifier rules — the first tag byte cannot be a digit
// (that spelling is a positional parameter, which emitted DDL never
// carries).
func dollarQuoteTagAt(stmt string, i int) (tag string, ok bool) {
	j := i + 1
	for j < len(stmt) {
		c := stmt[j]
		if c == '$' {
			return stmt[i : j+1], true
		}
		isIdent := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9' && j > i+1)
		if !isIdent {
			return "", false
		}
		j++
	}
	return "", false
}

// ddlExecer is the slice of *sql.DB / *sql.Conn / *sql.Tx the emitted-
// DDL sites execute through.
type ddlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// execEmittedDDL is the ONE road from an emitted DDL string to
// ExecContext in the schema-writing surface (held there by
// TestSchemaWriterEmittedDDLGoesThroughTheSingleStatementDoor). It
// refuses a structurally multi-statement string with the coded error
// BEFORE the server sees it, and otherwise executes it unchanged.
func execEmittedDDL(ctx context.Context, ex ddlExecer, stmt string) error {
	if verr := assertSingleDDLStatement(stmt); verr != nil {
		return sluicecode.Wrap(sluicecode.CodeDDLEmitMultiStatement,
			"the emitted DDL came from recorded schema content (a backup manifest or a read source schema); "+
				"if this is a restore, the chain's recorded schema is corrupt or tampered — verify the chain's "+
				"signature (`backup verify --verify-key …`) and take a fresh `backup full` of the live source; "+
				"nothing was executed",
			fmt.Errorf("postgres: refusing to execute emitted DDL that is not a single statement (%v); "+
				"sluice's emitters only ever produce one statement, so this content did not come from a "+
				"faithful emit: %.120q", verr, stmt))
	}
	_, err := ex.ExecContext(ctx, stmt)
	return err
}
