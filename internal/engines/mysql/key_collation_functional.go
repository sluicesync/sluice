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
// the CAST's — with an explicit CHARACTER SET, the server's default for the
// set, `utf8mb4_0900_ai_ci` (only a charset-less CAST follows the creating
// session's collation_connection) — which equates lower('e') and lower('é'),
// two values Postgres keeps apart. The
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
// index's ([functionalPartArms]). For a MySQL-family source that is READ:
// the reader records the source key's hidden-column collation on the IR
// ([ir.IndexColumn.HiddenCollation]; FUNCTIONAL-CAST-HIDDEN-COLLATION) and
// the part arms when it is unknown or differs from the target's — no
// inference from the column's declared collation, which five review rounds
// each found wrong in another shape (a session CAST, an ALTER … MODIFY that
// left the hidden column behind, a string literal under an INT column). The
// writer pins that collation on the keys it creates, so a migrated target
// compares as its source and does not arm. For a Postgres or SQLite source
// (no hidden column) the column routes stay: an armed column, or a declared
// collation other than the index's or none. Always: an unknown index
// collation, or no source shape at all (a live trigger-CDC stream), arms. So the keys are read for
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
// an exact-case lookup missed and ran no guard).
//
// The table-level refusals that remain, each KEY-COLLATION-GUARD-UNSUPPORTED
// and each loud before any row: a name that resolves to no column (a
// non-ASCII name the catalog mangled, Bug 288; an unscannable literal); a
// generated column nested more than eight deep; and an armed key whose
// resolved columns are still not all in the insert (renderFor). A collated
// part that resolves to no column at all compares FALSE on the source side,
// so a conflict through it refuses that row.

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
	// unresolved is why its columns could not be resolved ("" when they
	// were): one of unresolvedColumn, unresolvedLiteral, unresolvedNesting.
	unresolved string
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
// A collated part that reads NO column compares FALSE — never the same
// source key — so any conflict through it refuses: a constant expression, or
// a column list a scan lost, must be loud, never a silent "same" (third
// review 2026-10-15, finding 2: TRUE here let a part whose columns were
// dropped pass every merge).
func (f *functionalKeyPart) sourceEq(bytes func(column string) string) string {
	if !f.collated() {
		return "TRUE"
	}
	if len(f.columns) == 0 {
		return "FALSE"
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

// decodeCatalogExpression undoes information_schema's escaping layer over an
// expression's SQL spelling for the flavor that wrote it, as
// [normalizeExprForFlavor] does for the schema reader: MySQL (and vtgate,
// which serves MySQL's catalog) escapes `\` and `'` once more; MariaDB
// stores the SHOW CREATE spelling with no extra layer, where `\'` inside a
// literal is the value's own apostrophe and decoding it would corrupt the
// scan (fourth review, finding 4 — unreachable today, MariaDB having no
// functional key parts, but a generated column's text is read on any flavor
// that reaches here).
func decodeCatalogExpression(flavor Flavor, s string) string {
	if flavor == FlavorMariaDB {
		return s
	}
	return decodeISExpressionEscapes(s)
}

// unreadableLiteral stands in [expressionColumns]'s result for a string
// literal it could not scan to its end: a name no table has, so the part
// refuses at resolution rather than being read with columns missing.
const unreadableLiteral = "\x00unterminated literal"

// expressionColumns lists the backquoted identifiers of an expression in
// SQL spelling — the columns an expression the guard cannot read reads. The
// server backquotes every column reference. The text must already be
// decoded from the information_schema layer ([decodeISExpressionEscapes],
// done where the catalog is read): that layer escapes `\` and `'` once more,
// so `'it”s'` is stored as `_utf8mb4\'it\\\'s\'`, and a scan of the raw
// text took the inner `\'` for the close and lost every column after the
// literal — `concat(k, 'it”s', j)` read as k alone, and 'e'/'é' in j
// merged at exit 0 (third review 2026-10-15, measured on MySQL 8.4). String
// literals are skipped with the package's MySQL literal scanner
// ([quotedLiteralClose]: backslash escapes and doubled quotes, either
// delimiter), so a backquote inside one is not read as a column. A literal
// that does not scan yields [unreadableLiteral], which refuses.
func expressionColumns(expr string) []string {
	var out []string
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '\'', '"':
			closeIdx, _, ok := quotedLiteralClose(expr[i:], expr[i])
			if !ok {
				return append(out, unreadableLiteral)
			}
			i += closeIdx
		case '`':
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
		if known {
			f.collation, f.known = hiddenCollationOf(hidden, f.index, f.seq)
		}
	}
}

// hiddenCollationOf is the collation of the one hidden column for a key's
// functional part at position seq, matched by the full key name or — for a
// shortened name — by prefix. known is false when none or more than one
// matches. Shared by the guard's target side and the schema reader's
// source side ([SchemaReader.populateHiddenCollations]).
func hiddenCollationOf(hidden []hiddenKeyColumn, index string, seq int) (collation string, known bool) {
	matches := 0
	for _, h := range hidden {
		if h.part == seq && (h.index == index || (h.truncated && strings.HasPrefix(index, h.index))) {
			matches++
			collation = h.collation
		}
	}
	if matches != 1 {
		return "", false
	}
	return collation, true
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
			f.named, f.unresolved = f.named[:0], ""
			var base []string
			seen := map[string]bool{}
			var expand func(name string, depth int)
			expand = func(name string, depth int) {
				c, found := byFold[strings.ToLower(name)]
				switch {
				case name == unreadableLiteral:
					ok, f.unresolved = false, unresolvedLiteral
					return
				case !found:
					ok, f.unresolved = false, unresolvedColumn
					return
				case depth > 8:
					ok, f.unresolved = false, unresolvedNesting
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

// The causes a functional part's columns could not be resolved
// ([targetUniqueKeys.resolveFunctionalColumns]), each with its own remedy
// (fourth review, finding 4: one "name the columns in ASCII" remedy was
// wrong for the other two).
const (
	// unresolvedColumn: the expression names a column the table does not
	// have — a non-ASCII name the catalog's text mangled (Bug 288).
	unresolvedColumn = "names a column sluice cannot find in the table (the catalog's text of the expression does " +
		"not name it as the table does); recreate the key over columns named in ASCII"
	// unresolvedLiteral: a string literal in the expression did not scan.
	unresolvedLiteral = "holds a string literal sluice cannot read to its end, so the columns after it are not " +
		"known; recreate the key with the literal written plainly, or move the expression into a stored generated " +
		"column and key that column"
	// unresolvedNesting: generated columns nested past the expansion limit.
	unresolvedNesting = "reads a generated column built on generated columns more than eight deep; key a " +
		"shallower expression"
)

// keyCollationFunctionalUnsupported is the GUARD-UNSUPPORTED refusal for
// functional key parts whose columns could not be resolved, each named with
// its cause and remedy: there is no set of columns to compare their bytes by
// (KEY-COLLATION-FUNCTIONAL-KEY-PART).
func keyCollationFunctionalUnsupported(qn string, parts []*functionalKeyPart) error {
	names := make([]string, len(parts))
	for i, f := range parts {
		why := f.unresolved
		if why == "" {
			why = unresolvedColumn
		}
		names[i] = fmt.Sprintf("%s (%s) %s", quoteIdent(f.index), f.expr, why)
	}
	return &terminalMySQLError{err: fmt.Errorf("%w: %s: mysql: %s: sluice cannot tell which columns a unique key "+
		"compares, so it cannot check that an upsert does not merge two source rows that key equates. Refusing "+
		"before any row of the table is written. The key %s. Or exclude the table",
		ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, strings.Join(names, "; the key "))}
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
// comparison of it can differ from the index's.
//
// For a MySQL-family source the source's comparison is READ, not inferred
// (FUNCTIONAL-CAST-HIDDEN-COLLATION, the class fix): the MySQL reader
// records each functional part's hidden-column collation on the source IR
// ([ir.IndexColumn.HiddenCollation]), and the part arms when that is
// unknown or differs from the target index's — or when a column it reads
// does not compare on the target as on the source, since the VALUE is
// computed under the target column's collation ([functionalColumnsArm];
// sixth review F1). Five review rounds each
// found another shape where inferring the source key's comparison from its
// columns' declared collation was wrong and silent: a CAST takes the
// creating session's collation (a CAST with an explicit CHARACTER SET takes
// the server's default for the set; only a charset-less CAST follows the
// session's collation_connection), a literal takes the session's too
// (`if(f,'A','a')` over an INT f), and an ALTER … MODIFY of the column
// leaves the hidden column on its OLD collation (`trim(k)` after k moved
// `_bin` → `_0900_ai_ci`). The column says nothing reliable; the hidden
// column is the key. Unknown — a Vitess/PlanetScale source (vtgate does not
// serve SHOW EXTENDED), a manifest an older binary wrote, a CDC projection
// that carries no secondary indexes, a source key of another name — arms.
//
// For a Postgres or SQLite source there is no hidden column; the routes stay
// those the earlier rounds measured: a column it reads is armed (the source
// declares a collation the target column does not share), or the source
// declares a string column under a collation other than the index's or none
// at all (every Postgres-source character column does one or the other).
// A non-string column (INT, JSON) brings no collation and arms nothing.
//
// Always, before either route: the index's collation not known (vtgate; an
// ambiguous shortened hidden-column name) arms, and so does having no source
// shape at all (a live sqlite-trigger or d1-trigger stream, whose readers
// emit no SchemaSnapshot; third review, finding 3) — a functional part's
// source side is bytes, which needs nothing from the source. A part whose
// value is not a string never arms.
//
// Arming refuses a row only when the target matched two rows whose columns
// differ byte for byte, so where the source's key compares as the target's,
// arming costs only a refusal of a stale respelling ON DUPLICATE KEY UPDATE
// would converge (re-review finding 2) — which is why a read-and-equal part
// does not arm.
func functionalPartArms(f *functionalKeyPart, source *ir.Table, armed map[string]armedCollation, compareAs map[string]string) bool {
	switch {
	case !f.collated():
		return false
	case !f.known, source == nil:
		return true
	}
	if !sourceIsNonMySQL(source) {
		src := sourceFunctionalPart(source, f)
		if src == nil || src.HiddenCollation == "" {
			return true
		}
		coll := strings.ToLower(src.HiddenCollation)
		if mapped, ok := compareAs[coll]; ok {
			coll = mapped
		}
		if !strings.EqualFold(coll, f.collation) {
			return true
		}
		// Sixth review, F1: an equal hidden collation fixes how the key
		// COMPARES its value, not how the value is COMPUTED. The expression
		// evaluates over the target's columns under the target column's
		// collation — `k = 'a'`, FIELD, LOCATE, NULLIF, CASE, the case tables
		// of LOWER/UPPER — so `if(k = 'a', 'same', k)` over a source
		// `utf8mb4_bin` k and a target `utf8mb4_0900_ai_ci` k computes 'same'
		// for 'A' on the target alone, and the pinned `_bin` key merged
		// (2,'A') onto (1,'a') at exit 0 (measured). So the columns are
		// compared too: one whose collation the target does not share, or
		// whose source collation is not known, arms.
		return functionalColumnsArm(f, source, armed, nil)
	}
	return functionalColumnsArm(f, source, armed, compareAs)
}

// functionalColumnsArm reports whether a column a functional part reads puts
// its key under the guard: a column the plan already armed (its source
// collation is one the target column does not share), a column the source
// does not have, or a string column whose source collation is not known. A
// non-string column (INT, JSON) brings no collation and arms nothing (fourth
// review, finding 3).
//
// With compareAs non-nil — the Postgres/SQLite route, which has no hidden
// column to read — the column's own collation must also BE the index's,
// since nothing else says what the source compares the value under.
func functionalColumnsArm(f *functionalKeyPart, source *ir.Table, armed map[string]armedCollation, compareAs map[string]string) bool {
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
		if !isStringType(c.Type) {
			continue
		}
		_, coll := translate.ColumnCollation(c.Type)
		if coll == "" {
			return true
		}
		if compareAs == nil {
			continue
		}
		coll = strings.ToLower(coll)
		if mapped, ok := compareAs[coll]; ok {
			coll = mapped
		}
		if !strings.EqualFold(coll, f.collation) {
			return true
		}
	}
	return false
}

// sourceIsNonMySQL reports positive evidence that the source shape is not a
// MySQL-family table: a string column carrying a collation no MySQL-family
// server names, or a stamped Postgres collation determinism. Without that
// evidence the source is treated as MySQL-family, whose functional parts
// compare by their read hidden collation and their columns, or — unknown —
// arm. That route arms wherever the column-only route does, so a wrong
// "MySQL" answer costs only refusals.
//
// The test is the collation NAME, not [translate.CollationDialect]'s
// charset-presence rule (sixth review, F5): a MySQL collation carried
// without its charset — the shape the mydumper reader produced before
// MYDUMPER-COLLATION-NO-CHARSET, which an old backup manifest still carries
// — reads as Postgres there, and routed the part onto the column-only
// comparison, which ignores the source's hidden column.
func sourceIsNonMySQL(source *ir.Table) bool {
	for _, c := range source.Columns {
		if c == nil || !isStringType(c.Type) {
			continue
		}
		if collationDeterminismOf(c.Type) != ir.CollationDeterminismUnknown {
			return true
		}
		if _, coll := translate.ColumnCollation(c.Type); coll != "" && !mysqlCollationName(coll) {
			return true
		}
	}
	return false
}

// mysqlCollationName reports whether name is a MySQL- or MariaDB-family
// collation: one Vitess's MySQL 8 collation environment knows, or one whose
// leading word is a MySQL character set (MariaDB's `utf8mb4_uca1400_ai_ci`,
// which that environment does not carry). Postgres's "C", "POSIX",
// "en_US.utf8" and ICU's "und-x-icu" are none of these.
func mysqlCollationName(name string) bool {
	if charsetForCollationName(name) != "" {
		return true
	}
	cs, _, found := strings.Cut(strings.ToLower(name), "_")
	if cs == "utf8" {
		cs = "utf8mb3"
	}
	_, known := charsetMaxBytes[cs]
	return found && known
}

// sourceFunctionalPart is the source's own functional part matching f — the
// source index of f's name (sluice keeps a MySQL key's name) whose entry at
// f's position is an expression — or nil.
func sourceFunctionalPart(source *ir.Table, f *functionalKeyPart) *ir.IndexColumn {
	for _, idx := range append([]*ir.Index{source.PrimaryKey}, source.Indexes...) {
		if idx == nil || !strings.EqualFold(idx.Name, f.index) || f.seq >= len(idx.Columns) {
			continue
		}
		if c := &idx.Columns[f.seq]; c.Expression != "" {
			return c
		}
	}
	return nil
}

// isStringType reports whether a source column type carries a collation
// into an expression over it: the character types, ENUM and SET, through a
// domain. On the Postgres/SQLite route an ENUM or SET (no collation in the
// IR) counts as declaring none and arms — safe, and moot for MySQL-family
// sources, which compare by the read hidden collation instead.
func isStringType(t ir.Type) bool {
	switch v := t.(type) {
	case ir.Char, ir.Varchar, ir.Text, ir.Enum, ir.Set:
		return true
	case ir.Domain:
		return isStringType(v.BaseType)
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
