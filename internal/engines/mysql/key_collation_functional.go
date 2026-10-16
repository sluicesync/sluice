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
// Every functional part is compared per ROW, the way the index compares it
// on the target and the way the source can be trusted to:
//
//   - the SOURCE side is the BYTES of every column the expression reads.
//     Two rows whose columns are byte for byte equal have equal expressions
//     under any deterministic function, so "the same source key" is never
//     claimed falsely, and no premise about how the source's own function
//     respects its collation is needed (one would be false: TRIM does not
//     respect a collation with ignorable characters). The cost is stated:
//     a conflict between byte-different values the source equates refuses
//     loudly — which a source unique key admits only for one row
//     re-delivered under a new spelling, and the identity arm (the primary
//     key, or the applier's upsert key) recognises that.
//   - the TARGET side, for a part the guard can READ — one column passed
//     through LOWER, UPPER, TRIM, LTRIM, RTRIM, a CAST to a character type,
//     or COLLATE, in any nesting ([parseFunctionalKeyPart]) — is the
//     expression itself over the existing and the incoming row, under the
//     collation the index was created with. That collation is read off the
//     index's hidden column, because it is not the expression's collation
//     now: an ALTER of the column's collation leaves a functional key's
//     hidden column on the old one (measured on MySQL 8.0 and 8.4), so
//     re-deriving it would compare 'e'/'é' under `utf8mb4_bin` while the
//     index still merges them under `_ai_ci` (pin kfe).
//   - the TARGET side is TRUE — the part is assumed to match — for a part
//     the guard cannot read (`md5(k)`, a JSON path, two columns combined),
//     or whose hidden column cannot be read (vtgate does not parse SHOW
//     EXTENDED; an ambiguous truncated hidden-column name). TRUE can only
//     refuse more, and with the bytes source side it refuses only a real
//     respelling: two rows the target matched although their columns differ.
//     Review 2026-10-15 F4: the first cut refused the whole TABLE for an
//     unreadable part, which turned the common Postgres idiom `UNIQUE
//     (md5(body))` into a refusal of every apply and copy, protecting
//     nothing.
//
// A part ARMS its key — puts it under the guard — when its value may be a
// collated string and the source's comparison of it can differ from the
// index's ([functionalPartArms]): a column it reads is armed, the source
// declares one under a collation other than the index's (review F2: a column
// both sides declare `utf8mb4_bin` still merges 'e' and 'é' under a key
// whose CAST compares `_ai_ci`), or the index's collation is not known.
// Where the target compares exactly as the source does — a MySQL source's
// own key — it does not arm (re-review finding 2: arming there refused stale
// respellings ON DUPLICATE KEY UPDATE converges). So the keys are read for
// every table, armed column or not: one catalog read per table per writer,
// cached, and two more (its columns and its hidden columns) for a table with
// a functional part. A part KNOWN not to be a string (`(j * 2)`, a binary
// CAST) compares by value everywhere and arms nothing; inside a key armed by
// another part it is left out (TRUE on both sides), which can only widen
// the key's match, never hide one. Where the hidden column cannot be read
// (vtgate) the guard cannot know that, so such a part compares as a string
// would — TRUE on the target, its columns' bytes on the source.
//
// A part over a GENERATED column compares through that column's base
// columns, with TRUE on the target side ([targetUniqueKeys.
// resolveFunctionalColumns]): no insert carries a generated value.
//
// The catalog keeps the expression's identifiers as the index DDL spelled
// them (`lower(EMAIL)` over column `Email`), so every column a part reads
// is resolved against the table's columns case-insensitively (review F1:
// an exact-case lookup missed and ran no guard). A name that resolves to no
// column — a non-ASCII name the catalog mangled (Bug 288) — refuses
// KEY-COLLATION-GUARD-UNSUPPORTED: there is nothing to compare its bytes by.

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
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

// functionalKeyPart is a functional key part: the columns it reads and, for
// a part the guard can read ([parseFunctionalKeyPart]), the steps applied to
// its one column, innermost first.
type functionalKeyPart struct {
	index, expr string // the key's name and the server's EXPRESSION text
	seq         int    // the part's position in its key, from 0
	alone       bool   // the key's only part
	// columns are the stored columns the expression reads, as the table
	// spells them once resolved ([targetUniqueKeys.resolveFunctionalColumns]),
	// a generated column replaced by its base columns; named are the columns
	// the expression names, generated ones included, which arming compares.
	columns, named []string
	// arms: the part puts its key under the guard ([markFunctionalArming]).
	arms bool
	// readable: steps render the expression over columns[0].
	readable bool
	steps    []functionalStep
	// collation is the index's comparison collation (its hidden column's),
	// "" when the value is not a collated string; known is false when the
	// hidden column could not be read.
	collation string
	known     bool
}

// collated reports whether the part's value may be a collated string, the
// only kind a collation can merge.
func (f *functionalKeyPart) collated() bool { return !f.known || f.collation != "" }

// sourceEq is the part's source-side comparison: the bytes of every column
// it reads (bytes renders one column's), or TRUE for a part that is not a
// collated string, which compares by value on both sides and is left out.
func (f *functionalKeyPart) sourceEq(bytes func(column string) string) string {
	if !f.collated() || len(f.columns) == 0 {
		return "TRUE"
	}
	parts := make([]string, len(f.columns))
	for i, c := range f.columns {
		parts[i] = bytes(c)
	}
	return "(" + strings.Join(parts, " AND ") + ")"
}

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

// targetEq is the part's comparison as the index makes it, between the
// existing and the incoming value of its column: the expression under the
// index's own collation, or TRUE for a part the guard cannot read, whose
// collation is not known, or that is not a collated string.
func (f *functionalKeyPart) targetEq(existing, incoming string) string {
	if !f.readable || !f.known || f.collation == "" {
		return "TRUE"
	}
	side := func(x string) string { return "(" + f.render(x) + " COLLATE " + f.collation + ")" }
	return side(existing) + " <=> " + side(incoming)
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

// expressionColumns lists the backquoted identifiers of the server's
// EXPRESSION text — the columns an expression the guard cannot read reads.
// The server backquotes every column reference in that text. String
// literals are skipped (re-review 2026-10-15, finding 6: a backquote inside
// one read as a column): the catalog stores them with escaped delimiters,
// `_utf8mb4\'a`b\'`, and a plain `'…'` (doubled quote, backslash escape) is
// skipped the same way. A name that still resolves to no column of the
// table refuses at resolution, loudly.
func expressionColumns(expr string) []string {
	var out []string
	for i := 0; i < len(expr); i++ {
		switch {
		case expr[i] == '\\' && i+1 < len(expr) && expr[i+1] == '\'':
			// A literal with escaped delimiters: to the next \'.
			for i += 2; i < len(expr); i++ {
				if expr[i] == '\\' && i+1 < len(expr) {
					if expr[i+1] == '\'' {
						i++
						break
					}
					i++
				}
			}
		case expr[i] == '\'':
			for i++; i < len(expr); i++ {
				if expr[i] == '\\' {
					i++
					continue
				}
				if expr[i] == '\'' {
					if i+1 < len(expr) && expr[i+1] == '\'' {
						i++
						continue
					}
					break
				}
			}
		case expr[i] == '`':
			var b strings.Builder
			for i++; i < len(expr); i++ {
				if expr[i] == '`' {
					if i+1 < len(expr) && expr[i+1] == '`' {
						b.WriteByte('`')
						i++
						continue
					}
					break
				}
				b.WriteByte(expr[i])
			}
			out = append(out, b.String())
		}
	}
	return out
}

// hiddenKeyColumn is one functional key part's hidden generated column:
// `!hidden!<index>!<part>!<n>`, part counted from 0. truncated: the name is
// 64 characters long, where the server shortens the index name to fit
// (measured: a 54-character index name keeps 52), so index may be a prefix
// of the key's name.
type hiddenKeyColumn struct {
	index     string
	part      int
	collation string
	truncated bool
}

// functionalKeyCollations reads every functional key part's hidden column,
// its collation "" for a part whose value is not a collated string. ok is
// false when the columns cannot be read: vtgate does not parse SHOW
// EXTENDED, and the Vitess flavors are not asked.
func functionalKeyCollations(ctx context.Context, q queryRower, flavor Flavor, schema, table string) ([]hiddenKeyColumn, bool) {
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
	var out []hiddenKeyColumn
	vals := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range vals {
		dest[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, false
		}
		full := vals[field].String
		name, ok := strings.CutPrefix(full, "!hidden!")
		if !ok {
			continue
		}
		// From the right: an index name may itself hold '!'.
		rest, _, ok1 := cutLast(name, "!")
		index, part, ok2 := cutLast(rest, "!")
		n, err := strconv.Atoi(part)
		if !ok1 || !ok2 || err != nil {
			continue
		}
		out = append(out, hiddenKeyColumn{index: index, part: n, collation: vals[coll].String, truncated: utf8.RuneCountInString(full) >= 64})
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
// the collation its index compares it under, from the hidden columns
// [functionalKeyCollations] read: the one hidden column for the part's key
// and position, matched by the full key name or — for a shortened name —
// by prefix. None, or more than one (two long names sharing their first 52
// characters), leaves the collation unknown, and the part compares TRUE on
// the target side rather than refusing.
func (k *targetUniqueKeys) resolveFunctionalCollations(hidden []hiddenKeyColumn, known bool) {
	for _, f := range k.functionalParts() {
		f.collation, f.known = "", false
		if !known {
			continue
		}
		matches := 0
		for _, h := range hidden {
			if h.part == f.seq && (h.index == f.index || (h.truncated && strings.HasPrefix(f.index, h.index))) {
				matches++
				f.collation = h.collation
			}
		}
		if matches == 1 {
			f.known = true
		} else {
			f.collation = ""
		}
	}
}

// functionalParts is every functional part of the keys.
func (k *targetUniqueKeys) functionalParts() []*functionalKeyPart {
	var out []*functionalKeyPart
	for _, key := range k.uniqueFns {
		for _, f := range key {
			if f != nil {
				out = append(out, f)
			}
		}
	}
	return out
}

// resolveFunctionalColumns rewrites every column a functional part reads to
// the table's own spelling of it, matched case-insensitively (a MySQL column
// name is), and returns the parts naming a column the table does not have.
// The key entry a part stands in is rewritten with it.
//
// A GENERATED column is never in an insert's column list, so the guard
// cannot reference its incoming value (re-review 2026-10-15, finding 1: a
// key over one refused the whole table, same-engine included). A part
// reading one compares through the generated column's own base columns
// instead — the columns its generation expression names, expanded through
// any generated column among them — and its target side becomes TRUE, since
// the expression over the generated value cannot be rendered. Equal base
// bytes give an equal generated value, so the source side stays sound.
func (k *targetUniqueKeys) resolveFunctionalColumns(tableColumns []tableColumn) (unresolved []*functionalKeyPart) {
	byFold := make(map[string]tableColumn, len(tableColumns))
	for _, c := range tableColumns {
		byFold[strings.ToLower(c.name)] = c
	}
	for i, key := range k.uniqueFns {
		for j, f := range key {
			if f == nil {
				continue
			}
			ok := true
			f.named = f.named[:0]
			var base []string
			seen := map[string]bool{}
			var expand func(name string, depth int)
			expand = func(name string, depth int) {
				c, found := byFold[strings.ToLower(name)]
				if !found || depth > 8 {
					ok = false
					return
				}
				if c.generation == "" {
					if !seen[c.name] {
						seen[c.name] = true
						base = append(base, c.name)
					}
					return
				}
				f.readable = false // the generated value is not in the insert
				for _, ref := range expressionColumns(c.generation) {
					expand(ref, depth+1)
				}
			}
			for _, c := range f.columns {
				if tc, found := byFold[strings.ToLower(c)]; found {
					f.named = append(f.named, tc.name)
				}
				expand(c, 0)
			}
			if !ok {
				unresolved = append(unresolved, f)
				continue
			}
			f.columns = base
			k.uniques[i][j] = ""
			if len(f.columns) > 0 {
				k.uniques[i][j] = f.columns[0]
			}
		}
	}
	return unresolved
}

// tableColumn is one column of the target table as the catalog spells it,
// with its generation expression ("" for a stored value).
type tableColumn struct{ name, generation string }

// keyCollationFunctionalUnsupported is the GUARD-UNSUPPORTED refusal for a
// functional key part naming a column the table does not have — a non-ASCII
// name the catalog's EXPRESSION text mangled (Bug 288): there is no column
// to compare its bytes by (KEY-COLLATION-FUNCTIONAL-KEY-PART).
func keyCollationFunctionalUnsupported(qn string, parts []*functionalKeyPart) error {
	names := make([]string, len(parts))
	for i, f := range parts {
		names[i] = fmt.Sprintf("%s (%s)", quoteIdent(f.index), f.expr)
	}
	return &terminalMySQLError{err: fmt.Errorf("%w: %s: mysql: %s: the unique key(s) %s read a column sluice cannot "+
		"find in the table (the catalog's text of the expression does not name it as the table does), so sluice "+
		"cannot check that an upsert does not merge two source rows that key equates. Refusing before any row of "+
		"the table is written. Recreate the key over columns named in ASCII, or exclude the table",
		ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, strings.Join(names, ", "))}
}

// markFunctionalArming decides, for every functional part of keys, whether
// it arms its key ([functionalPartArms]).
func (k *targetUniqueKeys) markFunctionalArming(source *ir.Table, armed map[string]armedCollation, compareAs map[string]string) {
	for _, f := range k.functionalParts() {
		f.arms = functionalPartArms(f, source, armed, compareAs)
	}
}

// functionalPartArms reports whether a functional part puts its key under
// the guard: when its value may be a collated string AND the source's own
// comparison of it can differ from the index's. That is so when
//
//   - the index's collation is not known (vtgate; an ambiguous shortened
//     hidden-column name): nothing shows the two alike;
//   - a column it names is armed — the source declares a collation the
//     target column does not share, which every Postgres-source character
//     column is (the cross-family case);
//   - a column it names is declared by the source under a collation other
//     than the one the index compares under (review F2: a column both sides
//     declare `utf8mb4_bin` under a key whose CAST compares `_ai_ci`), or
//     the source declares none, or lacks the column.
//
// Otherwise the target compares the part exactly as the source compares its
// columns, and arming it would only refuse what converges: re-review
// 2026-10-15 finding 2 measured a MySQL → MySQL copy, with the source's own
// key, refusing a stale respelled row (target row 3 'A@y', the source's row
// 4 'a@y') that ON DUPLICATE KEY UPDATE converges. A part whose value is not
// a string never arms. With no source shape (nothing known of the source)
// only an unknown index collation arms, as for a plain column.
func functionalPartArms(f *functionalKeyPart, source *ir.Table, armed map[string]armedCollation, compareAs map[string]string) bool {
	switch {
	case !f.collated():
		return false
	case !f.known:
		return true
	case source == nil:
		return false
	}
	srcCols := make(map[string]*ir.Column, len(source.Columns))
	for _, c := range source.Columns {
		if c != nil {
			srcCols[strings.ToLower(c.Name)] = c
		}
	}
	armedFold := make(map[string]bool, len(armed))
	for name := range armed {
		armedFold[strings.ToLower(name)] = true
	}
	for _, name := range f.named {
		if armedFold[strings.ToLower(name)] {
			return true
		}
		c := srcCols[strings.ToLower(name)]
		if c == nil {
			return true
		}
		_, coll := translate.ColumnCollation(c.Type)
		coll = strings.ToLower(coll)
		if mapped, ok := compareAs[coll]; ok {
			coll = mapped
		}
		if coll == "" || !strings.EqualFold(coll, f.collation) {
			return true
		}
	}
	return false
}

// armsFunctional reports whether an arming functional part is among the
// plan's keys: the remedies then name the key to recreate, not a column to
// alter.
func (p *keyCollationPlan) armsFunctional() bool {
	for _, fns := range p.fns {
		for _, f := range fns {
			if f != nil && f.arms {
				return true
			}
		}
	}
	return false
}

// functionalKeyNeed names what a collated functional part's key must be
// recreated as for its comparison to keep the source's values apart: its
// expression compared byte for byte, with no padding. An ALTER of the
// column does not reach it — MySQL keeps the collation a functional key was
// created with (measured) — and sluice recreates the key from the source's
// expression, under the target's default, so a re-copy does not converge
// either. When the part is its key's only part the statement to recreate it
// is spelled out; otherwise the text names the part and leaves the key's
// other parts as they are.
//
// The comparison named is `utf8mb4_0900_bin` for a value known to be
// utf8mb4 and a binary CAST for any other or an unknown one (re-review
// finding 4: on vtgate a utf8mb4 collation over a latin1 expression errors):
// review F5 measured that `<charset>_bin` is PAD SPACE, so 'e' and 'e ' still
// collided after an operator applied it.
func functionalKeyNeed(f *functionalKeyPart) string {
	exact := "(" + f.expr + ") COLLATE utf8mb4_0900_bin"
	if cs, _, _ := strings.Cut(f.collation, "_"); !f.known || cs != "utf8mb4" {
		exact = "CAST(" + f.expr + " AS BINARY)"
	}
	const why = "altering the column does not change a functional key, which keeps the collation it was created with"
	if f.alone {
		return fmt.Sprintf("the functional key %s recreated comparing its expression byte for byte "+
			"(`ALTER TABLE … DROP INDEX %s, ADD UNIQUE KEY %s ((%s))`; %s)",
			quoteIdent(f.index), quoteIdent(f.index), quoteIdent(f.index), exact, why)
	}
	return fmt.Sprintf("the functional key %s recreated with its part %s as `(%s)` and its other "+
		"parts unchanged (%s)", quoteIdent(f.index), f.expr, exact, why)
}
