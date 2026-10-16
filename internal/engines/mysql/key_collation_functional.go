// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// The key-collation guard through a FUNCTIONAL key part
// (KEY-COLLATION-FUNCTIONAL-KEY-PART; operator's design, 2026-10-15: check
// through the expression).
//
// # The harm
//
// A Postgres `UNIQUE (lower(email))` reaches MySQL as the functional key
// part `((lower(cast(email as char charset utf8mb4))))`. Its comparison is
// the CAST's — the character set's default, `utf8mb4_0900_ai_ci` — which
// equates lower('e') and lower('é'), two values Postgres keeps apart. The
// guard read the target's unique keys with every functional part skipped,
// so it saw no key holding the armed column, and the second source row
// overwrote the first through ON DUPLICATE KEY UPDATE at exit 0 (measured
// 2026-10-15 on MySQL 8.4, CDC apply and every idempotent copy path; a fresh
// copy meets the pair as a loud 1062 when the index is built after it).
//
// # The rule
//
// A functional part the guard can read — one column passed through LOWER,
// UPPER, TRIM, LTRIM, RTRIM, a CAST to a character type, or COLLATE, in any
// nesting ([parseFunctionalKeyPart]) — is compared the way the index
// compares it and the way the source can be trusted to:
//
//   - the TARGET side is the expression itself, rendered over the existing
//     and the incoming row, under the collation the index was created
//     with. That collation is read off the index's hidden column, because
//     it is not the expression's collation now: measured on MySQL 8.4, an
//     ALTER of the column's collation leaves the functional key's hidden
//     column on the old one, so re-deriving it would compare 'e'/'é' under
//     `utf8mb4_bin` after an operator altered the column — while the index
//     still merges them under `_ai_ci`. Where the hidden column cannot be
//     read (vtgate does not parse SHOW EXTENDED), the target side is TRUE:
//     the guard assumes the part may match, which can only refuse more.
//   - the SOURCE side is the column's BYTES. Two rows whose column is byte
//     for byte equal have equal expressions under any deterministic
//     function, so "the same source key" is never claimed falsely; no
//     premise about how the source's own function respects its collation
//     is needed (one would be false: TRIM does not respect a collation
//     with ignorable characters). The cost is stated: a conflict between
//     byte-different values the source equates refuses loudly — which a
//     source unique key admits only for one row re-delivered under a new
//     spelling, and the primary-key arm recognises that whenever the table
//     has a primary key.
//
// The part arms its key when its column is armed and its value is a
// collated string. A part whose value is not a string (`(j * 2)`, a binary
// CAST) compares the same everywhere and is left out of the comparison, as
// before; it only ever narrows a key, so leaving it out can refuse more,
// never less. A part the guard cannot read refuses on an armed table
// (KEY-COLLATION-GUARD-UNSUPPORTED) rather than being skipped: it may
// compare a column under a collation the source does not share, and
// nothing about it can be checked.
//
// Residual, stated: a table with NO armed column runs no guard at all, so an
// unreadable functional part there (a JSON path over a table with no
// character column) is not seen (KEY-COLLATION-FUNCTIONAL-UNARMED-TABLE).

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// functionalStep is one function the guard can see through, applied to the
// value inside it.
type functionalStep struct {
	// kind is "fn" (fn names LOWER, UPPER, TRIM, LTRIM or RTRIM), "cast"
	// (a CAST to CHAR(length) CHARACTER SET charset; length 0 is no
	// length) or "collate" (charset holds the collation).
	kind, fn, charset string
	length            int
}

// functionalKeyPart is a functional key part the guard reads through: the
// one column it reads and the steps applied to it, innermost first.
type functionalKeyPart struct {
	index, expr string // the key's name and the server's EXPRESSION text
	seq         int    // the part's position in its key, from 0
	alone       bool   // the key's only part
	column      string
	steps       []functionalStep
	// collation is the index's comparison collation (its hidden column's),
	// "" when the value is not a collated string; known is false when the
	// hidden column could not be read.
	collation string
	known     bool
}

// collated reports whether the part's value may be a collated string, the
// only kind a collation can merge.
func (f *functionalKeyPart) collated() bool { return !f.known || f.collation != "" }

// render applies the part's steps to x.
func (f *functionalKeyPart) render(x string) string {
	for _, s := range f.steps {
		switch s.kind {
		case "fn":
			x = s.fn + "(" + x + ")"
		case "cast":
			length := ""
			if s.length > 0 {
				length = "(" + strconv.Itoa(s.length) + ")"
			}
			x = "CAST(" + x + " AS CHAR" + length + " CHARACTER SET " + s.charset + ")"
		case "collate":
			x = "(" + x + " COLLATE " + s.charset + ")"
		}
	}
	return x
}

// targetEq is the part's comparison as the index makes it, between two
// values of its column: the expression under the index's own collation, or
// TRUE when that collation is not known.
func (f *functionalKeyPart) targetEq(existing, incoming string) string {
	if !f.known {
		return "TRUE"
	}
	side := func(x string) string {
		if f.collation == "" {
			return f.render(x)
		}
		return "(" + f.render(x) + " COLLATE " + f.collation + ")"
	}
	return side(existing) + " <=> " + side(incoming)
}

// opaqueKeyPart is a functional key part the guard cannot read.
type opaqueKeyPart struct {
	index, expr string
	seq         int
	// collation and known as on [functionalKeyPart]; only a part that may be
	// a collated string refuses.
	collation string
	known     bool
}

// functionalLeaf names the functions the guard reads through. lcase and
// ucase are MySQL's synonyms; the server stores them as lower and upper.
var functionalLeaf = map[string]string{
	"lower": "LOWER", "lcase": "LOWER", "upper": "UPPER", "ucase": "UPPER",
	"trim": "TRIM", "ltrim": "LTRIM", "rtrim": "RTRIM",
}

// parseFunctionalKeyPart reads the server's EXPRESSION text for a
// functional key part as one column passed through the steps the guard can
// render. The grammar is the server's own normalized spelling (measured on
// MySQL 8.4: lowercase function names, backquoted identifiers, `cast(x as
// char(n) charset cs)`, `(x collate c)`):
//
//	part    := ident | fn '(' part ')' | 'cast' '(' part 'as' 'char' ['(' n ')'] 'charset' word ')'
//	         | '(' part 'collate' word ')'
//
// Anything else — another function, a second column, a literal, a TRIM
// with a remove-string — is not read: ok is false and the caller treats the
// part as opaque. A non-ASCII column name the catalog mangled (Bug 288)
// is not read either ([functionalParser.ident]).
func parseFunctionalKeyPart(expr string) (column string, steps []functionalStep, ok bool) {
	p := &functionalParser{s: expr}
	column, steps, ok = p.part()
	p.space()
	if !ok || p.i != len(p.s) {
		return "", nil, false
	}
	return column, steps, true
}

type functionalParser struct {
	s string
	i int
}

func (p *functionalParser) space() {
	for p.i < len(p.s) && p.s[p.i] == ' ' {
		p.i++
	}
}

// word reads an ASCII identifier-like word (a function, keyword, charset or
// collation name), lowercased.
func (p *functionalParser) word() string {
	p.space()
	start := p.i
	for p.i < len(p.s) {
		b := p.s[p.i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' {
			p.i++
			continue
		}
		break
	}
	return strings.ToLower(p.s[start:p.i])
}

// punct consumes c after optional spaces.
func (p *functionalParser) punct(c byte) bool {
	p.space()
	if p.i < len(p.s) && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

// keyword consumes the word w.
func (p *functionalParser) keyword(w string) bool {
	save := p.i
	if p.word() == w {
		return true
	}
	p.i = save
	return false
}

func (p *functionalParser) part() (string, []functionalStep, bool) {
	p.space()
	if p.i >= len(p.s) {
		return "", nil, false
	}
	switch p.s[p.i] {
	case '`':
		return p.ident()
	case '(':
		p.i++
		col, steps, ok := p.part()
		if !ok || !p.keyword("collate") {
			return "", nil, false
		}
		coll := p.word()
		if coll == "" || !p.punct(')') {
			return "", nil, false
		}
		return col, append(steps, functionalStep{kind: "collate", charset: coll}), true
	}
	name := p.word()
	if name == "" || !p.punct('(') {
		return "", nil, false
	}
	col, steps, ok := p.part()
	if !ok {
		return "", nil, false
	}
	if fn, leaf := functionalLeaf[name]; leaf {
		if !p.punct(')') {
			return "", nil, false
		}
		return col, append(steps, functionalStep{kind: "fn", fn: fn}), true
	}
	if name != "cast" || !p.keyword("as") || !p.keyword("char") {
		return "", nil, false
	}
	length := 0
	if p.punct('(') {
		start := p.i
		for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
			p.i++
		}
		n, err := strconv.Atoi(p.s[start:p.i])
		if err != nil || n <= 0 || !p.punct(')') {
			return "", nil, false
		}
		length = n
	}
	if !p.keyword("charset") {
		return "", nil, false
	}
	cs := p.word()
	if cs == "" || !p.punct(')') {
		return "", nil, false
	}
	return col, append(steps, functionalStep{kind: "cast", charset: cs, length: length}), true
}

// ident reads a backquoted identifier (a doubled backquote is one). A
// non-ASCII name is not read: the catalog's EXPRESSION text can carry it
// mangled (Bug 288), so it may not name the column it looks like.
func (p *functionalParser) ident() (string, []functionalStep, bool) {
	var b strings.Builder
	for p.i++; p.i < len(p.s); p.i++ {
		if p.s[p.i] >= 0x80 {
			return "", nil, false
		}
		if p.s[p.i] != '`' {
			b.WriteByte(p.s[p.i])
			continue
		}
		if p.i+1 < len(p.s) && p.s[p.i+1] == '`' {
			b.WriteByte('`')
			p.i++
			continue
		}
		p.i++
		if b.Len() == 0 {
			return "", nil, false
		}
		return b.String(), nil, true
	}
	return "", nil, false
}

// functionalKeyCollations reads each functional key part's comparison
// collation off its hidden generated column (`!hidden!<index>!<part>!<n>`,
// part counted from 0), keyed "<index>!<part>"; the value is "" for a part
// whose value is not a collated string. ok is false when the columns cannot
// be read: vtgate does not parse SHOW EXTENDED, and the Vitess flavors are
// not asked.
func functionalKeyCollations(ctx context.Context, q queryRower, flavor Flavor, schema, table string) (map[string]string, bool) {
	if flavor != FlavorVanilla {
		return nil, false
	}
	from := quoteIdent(table)
	if schema != "" {
		from = quoteIdent(schema) + "." + from
	}
	rows, err := q.QueryContext(ctx, "SHOW EXTENDED FULL COLUMNS FROM "+from)
	if err != nil {
		return nil, false
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, false
	}
	field, coll := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "field":
			field = i
		case "collation":
			coll = i
		}
	}
	if field < 0 || coll < 0 {
		return nil, false
	}
	out := map[string]string{}
	vals := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range vals {
		dest[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, false
		}
		name, ok := strings.CutPrefix(vals[field].String, "!hidden!")
		if !ok {
			continue
		}
		// From the right: an index name may itself hold '!'.
		rest, _, ok1 := cutLast(name, "!")
		index, part, ok2 := cutLast(rest, "!")
		if !ok1 || !ok2 {
			continue
		}
		key := index + "!" + part
		if _, dup := out[key]; dup {
			out[key] = "\x00" // two hidden columns for one part: not known
			continue
		}
		out[key] = vals[coll].String
	}
	if rows.Err() != nil {
		return nil, false
	}
	return out, true
}

// cutLast is strings.Cut at the last sep.
func cutLast(s, sep string) (before, after string, found bool) {
	if i := strings.LastIndex(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return s, "", false
}

// resolveFunctionalCollations records, on every functional part of keys,
// the collation its index compares it under, from [functionalKeyCollations].
// seq is each part's position in its index, keyed like that map.
func (k *targetUniqueKeys) resolveFunctionalCollations(collations map[string]string, known bool) {
	lookup := func(index string, seq int) (string, bool) {
		if !known {
			return "", false
		}
		c, ok := collations[index+"!"+strconv.Itoa(seq)]
		if !ok || c == "\x00" {
			return "", false
		}
		return c, true
	}
	for _, key := range k.uniqueFns {
		for _, f := range key {
			if f != nil {
				f.collation, f.known = lookup(f.index, f.seq)
			}
		}
	}
	for i := range k.opaque {
		o := &k.opaque[i]
		o.collation, o.known = lookup(o.index, o.seq)
	}
}

// refusesOpaque returns the opaque parts that refuse: those whose value may
// be a collated string.
func (k *targetUniqueKeys) refusesOpaque() []opaqueKeyPart {
	var out []opaqueKeyPart
	for _, o := range k.opaque {
		if !o.known || o.collation != "" {
			out = append(out, o)
		}
	}
	return out
}

// keyCollationFunctionalUnsupported is the GUARD-UNSUPPORTED refusal for a
// table whose columns the guard is armed on and whose unique key has a
// functional part it cannot read (KEY-COLLATION-FUNCTIONAL-KEY-PART).
func keyCollationFunctionalUnsupported(qn string, opaque []opaqueKeyPart) error {
	parts := make([]string, len(opaque))
	for i, o := range opaque {
		parts[i] = fmt.Sprintf("%s (%s)", quoteIdent(o.index), o.expr)
	}
	return &terminalMySQLError{err: fmt.Errorf("%w: %s: mysql: %s: the unique key(s) %s compare an expression "+
		"sluice cannot read, on a table whose source declares a collation the target does not share, so sluice "+
		"cannot check that an upsert does not merge two source rows that expression equates. Refusing before any "+
		"row of the table is written. Replace the key with one over plain columns, or over one column passed "+
		"through LOWER, UPPER, TRIM, a character-set CAST or COLLATE (which sluice compares through), or exclude "+
		"the table",
		ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, strings.Join(parts, ", "))}
}

// armsFunctional reports whether an armed functional part is among the
// plan's keys: the remedies then name the key to recreate, not a column to
// alter.
func (p *keyCollationPlan) armsFunctional() bool {
	for _, fns := range p.fns {
		for _, f := range fns {
			if f == nil || !f.collated() {
				continue
			}
			if _, ok := p.armed[f.column]; ok {
				return true
			}
		}
	}
	return false
}

// functionalKeyNeed names what an armed functional part's key must be
// recreated as for its comparison to keep the source's values apart: its
// expression under the binary collation of its character set. An ALTER of
// the column does not reach it — MySQL keeps the collation a functional
// key was created with (measured) — and sluice recreates the key from the
// source's expression, under the target's default, so a re-copy does not
// converge either. When the part is its key's only part the statement to
// recreate it is spelled out; otherwise the text names the part and leaves
// the key's other parts as they are.
func functionalKeyNeed(f *functionalKeyPart) string {
	bin := "utf8mb4_bin"
	if cs, _, ok := strings.Cut(f.collation, "_"); ok && f.known {
		bin = cs + "_bin"
	}
	const why = "altering the column does not change a functional key, which keeps the collation it was created with"
	if f.alone {
		return fmt.Sprintf("the functional key %s recreated with its expression under a binary collation "+
			"(`ALTER TABLE … DROP INDEX %s, ADD UNIQUE KEY %s ((%s COLLATE %s))`; %s)",
			quoteIdent(f.index), quoteIdent(f.index), quoteIdent(f.index), f.expr, bin, why)
	}
	return fmt.Sprintf("the functional key %s recreated with its part %s as `(%s COLLATE %s)` and its other "+
		"parts unchanged (%s)", quoteIdent(f.index), f.expr, f.expr, bin, why)
}
