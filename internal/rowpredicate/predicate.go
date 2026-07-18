// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package rowpredicate is the client-side row-predicate evaluator behind
// ADR-0173 Phase 2 (continuous *filtered* sync). Unlike the migrate leg
// (Phase 1), which pushes a native-SQL `--where` predicate down into the
// source read, a filtered CDC stream has no source-side filter — every
// binlog/slot/VStream change arrives — so sluice must evaluate the
// predicate CLIENT-SIDE, per [ir.Row], over the decoded before/after
// images.
//
// Evaluating arbitrary source SQL client-side would silently diverge from
// the source's own evaluation (functions, subqueries, collation-sensitive
// comparisons), which is a silent-correctness bug — the exact class the
// loud-failure tenet exists to kill. So this package deliberately accepts
// only a RESTRICTED grammar it can evaluate FAITHFULLY, and refuses
// everything else LOUDLY at compile time (sync-start), with a coded
// refusal ([sluicecode.CodeWhereCDCUnsupportedPredicate]):
//
//	predicate := orExpr
//	orExpr    := andExpr ('OR' andExpr)*
//	andExpr   := notExpr ('AND' notExpr)*
//	notExpr   := 'NOT' notExpr | primary
//	primary   := '(' orExpr ')' | comparison
//	comparison:= column 'IS' ['NOT'] 'NULL'
//	           | column ('=' | '!=' | '<>' | '<' | '<=' | '>' | '>=') literal
//	           | column ['NOT'] 'IN' '(' literal (',' literal)* ')'
//	literal   := number | 'string' | TRUE | FALSE | NULL
//
// Fidelity gates applied at compile time (each a loud refusal, never a
// silent mis-evaluation):
//
//   - Unknown column, unknown token, function call, subquery, arithmetic,
//     LIKE, or any construct the grammar can't parse.
//   - An ORDERING comparison (`<`,`<=`,`>`,`>=`) on a string/binary/temporal-
//     of-day column — ordering is collation- (strings) or format- (time-of-
//     day) dependent, so a byte/lexical compare can diverge.
//   - Any string comparison on a column whose collation is not provably
//     case- and accent-sensitive (a `*_ci`/`*_ai` MySQL collation, or an
//     unknown collation on a MySQL-family source whose default collations
//     are case-insensitive): a byte-exact client compare would diverge from
//     the source's collation-aware `=`.
//   - A comparison against a literal whose kind does not match the column
//     family (e.g. `age = 'x'` where age is numeric).
//   - A timezone-aware temporal comparison (the row value is a UTC instant
//     but the source would interpret the literal in its session timezone).
//
// Evaluation uses SQL three-valued logic (TRUE / FALSE / UNKNOWN). A row
// is "in scope" only when the predicate evaluates to definitely TRUE;
// UNKNOWN (a NULL-involving comparison) is treated as NOT matching, which
// is the same in/out decision `WHERE p` makes on the source (a row where
// `p` is UNKNOWN is not selected).
package rowpredicate

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Predicate is a compiled, client-side-evaluable row predicate. Build one
// with [Compile]; evaluate rows with [Predicate.Eval]. A nil *Predicate is
// a valid "matches everything" (used for tables with no filter).
type Predicate struct {
	root node
	// text is the original predicate string, for diagnostics.
	text string
}

// Eval reports whether row is IN SCOPE for this predicate under SQL
// three-valued logic: it returns true only when the predicate is
// definitely TRUE. A NULL-involving comparison (UNKNOWN) returns false,
// matching `WHERE p`'s row-selection semantics. A nil predicate matches
// every row.
func (p *Predicate) Eval(row ir.Row) bool {
	if p == nil || p.root == nil {
		return true
	}
	return p.root.eval(row) == truthTrue
}

// String returns the original predicate text, for logging.
func (p *Predicate) String() string {
	if p == nil {
		return ""
	}
	return p.text
}

// Family is the value family a column belongs to, for fidelity gating.
type Family uint8

// The value families the evaluator distinguishes. They mirror the
// docs/value-types.md contract, collapsed to the granularity the
// comparison logic needs.
const (
	// FamilyUnsupported is any column the evaluator cannot compare
	// faithfully (Array, Set, Geometry, …) — a comparison against it is
	// refused at compile time.
	FamilyUnsupported Family = iota
	// FamilyNumeric is Integer / Decimal / Float. Row values are int64,
	// uint64, float64, or a decimal string; compared numerically.
	FamilyNumeric
	// FamilyBool is Boolean. Row value is a Go bool.
	FamilyBool
	// FamilyString is a text-like value compared byte-exact: Char /
	// Varchar / Text (only when case+accent sensitive), Enum, UUID, Inet,
	// Cidr, Macaddr, and Time-of-day (stored as a string). Ordering is
	// refused for this family (collation/format dependent).
	FamilyString
	// FamilyBinary is Binary / Varbinary / Blob / JSON. Row value is
	// []byte; compared byte-exact (equality only).
	FamilyBinary
	// FamilyTemporal is Date / DateTime / Timestamp-without-tz. Row value
	// is a time.Time; compared chronologically.
	FamilyTemporal
)

// ColumnInfo is the fidelity-relevant description of one column: which
// value family it holds and, for strings, whether the source's own
// equality is byte-exact (case+accent sensitive) so a client-side byte
// compare is faithful.
type ColumnInfo struct {
	Family Family
	// CaseSensitive is meaningful only for [FamilyString]: true when the
	// column's collation makes the source's `=` byte-exact, so a client
	// compare cannot diverge.
	CaseSensitive bool
}

// ColumnInfosFromIR builds the per-column fidelity map the evaluator
// needs from a table's IR columns, keyed by lower-cased column name.
// engineName drives the default-collation decision for a string column
// that carries no explicit collation (MySQL-family defaults are
// case-insensitive; Postgres/SQLite `=` is byte-exact).
func ColumnInfosFromIR(engineName string, cols []*ir.Column) map[string]ColumnInfo {
	out := make(map[string]ColumnInfo, len(cols))
	for _, c := range cols {
		if c == nil {
			continue
		}
		out[strings.ToLower(c.Name)] = columnInfoFor(engineName, c)
	}
	return out
}

func columnInfoFor(engineName string, c *ir.Column) ColumnInfo {
	switch t := c.Type.(type) {
	case ir.Integer, ir.Decimal, ir.Float:
		return ColumnInfo{Family: FamilyNumeric}
	case ir.Boolean:
		return ColumnInfo{Family: FamilyBool}
	case ir.Char:
		return ColumnInfo{Family: FamilyString, CaseSensitive: collationCaseSensitive(engineName, t.Collation)}
	case ir.Varchar:
		return ColumnInfo{Family: FamilyString, CaseSensitive: collationCaseSensitive(engineName, t.Collation)}
	case ir.Text:
		return ColumnInfo{Family: FamilyString, CaseSensitive: collationCaseSensitive(engineName, t.Collation)}
	case ir.Enum, ir.UUID, ir.Inet, ir.Cidr, ir.Macaddr:
		// Canonical/identifier-shaped ASCII values: the source's `=` is
		// exact, so a byte compare is faithful.
		return ColumnInfo{Family: FamilyString, CaseSensitive: true}
	case ir.Time:
		// Stored as a fixed-width string ("08:30:00[.ffffff]"); equality is
		// byte-exact. Ordering is refused (over-24h / fractional edge
		// cases), so no chronological parse is attempted.
		return ColumnInfo{Family: FamilyString, CaseSensitive: true}
	case ir.Binary, ir.Varbinary, ir.Blob, ir.JSON:
		return ColumnInfo{Family: FamilyBinary}
	case ir.Date, ir.DateTime:
		return ColumnInfo{Family: FamilyTemporal}
	case ir.Timestamp:
		if t.WithTimeZone {
			// A tz-aware instant: the source interprets a bare literal in
			// its session timezone, which the client cannot reproduce
			// faithfully. Leave it unsupported so a comparison refuses.
			return ColumnInfo{Family: FamilyUnsupported}
		}
		return ColumnInfo{Family: FamilyTemporal}
	default:
		return ColumnInfo{Family: FamilyUnsupported}
	}
}

// collationCaseSensitive reports whether the source's `=` on a string
// column with this collation is byte-exact (so a client-side byte compare
// is faithful). A `*_ci` (case-insensitive) or `*_ai` (accent-insensitive)
// collation is not; an unknown/empty collation is case-sensitive on
// Postgres/SQLite (deterministic-collation `=` is byte-equality) but
// NOT provably so on a MySQL-family source (whose platform-default
// collations are case-insensitive).
func collationCaseSensitive(engineName, collation string) bool {
	lc := strings.ToLower(strings.TrimSpace(collation))
	if lc == "" {
		return !isMySQLFamily(engineName)
	}
	if strings.Contains(lc, "_ci") || strings.Contains(lc, "_ai") {
		return false
	}
	return true
}

func isMySQLFamily(engineName string) bool {
	switch strings.ToLower(engineName) {
	case "mysql", "planetscale", "vitess", "mariadb":
		return true
	default:
		return false
	}
}

// Compile parses predicate into a client-side-evaluable [Predicate],
// resolving each referenced column against infos (built by
// [ColumnInfosFromIR]) so type/collation fidelity is checked UP FRONT.
// Any construct the grammar can't faithfully evaluate is refused loudly
// with a [sluicecode.CodeWhereCDCUnsupportedPredicate] coded error naming
// the table and the reason.
func Compile(table, predicate string, infos map[string]ColumnInfo) (*Predicate, error) {
	toks, err := tokenize(predicate)
	if err != nil {
		return nil, refuse(table, predicate, err.Error())
	}
	p := &parser{toks: toks, infos: infos}
	root, err := p.parseOr()
	if err != nil {
		return nil, refuse(table, predicate, err.Error())
	}
	if !p.atEnd() {
		return nil, refuse(table, predicate, fmt.Sprintf("unexpected %q after a complete expression", p.peek().text))
	}
	return &Predicate{root: root, text: predicate}, nil
}

func refuse(table, predicate, reason string) error {
	return sluicecode.Wrap(
		sluicecode.CodeWhereCDCUnsupportedPredicate,
		"rewrite the predicate within the supported grammar (column =/!=/</<=/>/>= literal, IN, IS [NOT] NULL, AND/OR/NOT, parentheses), "+
			"or use `sluice migrate --where` for a one-shot source-evaluated filter",
		fmt.Errorf(
			"continuous filtered sync: --where %s=%q cannot be evaluated client-side per CDC change: %s "+
				"(a filtered CDC stream evaluates the predicate over the decoded row, so only a restricted, "+
				"faithfully-evaluable grammar is accepted — anything else could silently diverge from the source's "+
				"own evaluation and leak or drop rows)",
			table, predicate, reason,
		),
	)
}

// -------------------- three-valued logic --------------------

type truth uint8

const (
	truthFalse truth = iota
	truthTrue
	truthUnknown
)

func (t truth) not() truth {
	switch t {
	case truthTrue:
		return truthFalse
	case truthFalse:
		return truthTrue
	default:
		return truthUnknown
	}
}

// -------------------- AST --------------------

type node interface {
	eval(row ir.Row) truth
}

type andNode struct{ kids []node }

func (n andNode) eval(row ir.Row) truth {
	res := truthTrue
	for _, k := range n.kids {
		switch k.eval(row) {
		case truthFalse:
			return truthFalse
		case truthUnknown:
			res = truthUnknown
		}
	}
	return res
}

type orNode struct{ kids []node }

func (n orNode) eval(row ir.Row) truth {
	res := truthFalse
	for _, k := range n.kids {
		switch k.eval(row) {
		case truthTrue:
			return truthTrue
		case truthUnknown:
			res = truthUnknown
		}
	}
	return res
}

type notNode struct{ kid node }

func (n notNode) eval(row ir.Row) truth { return n.kid.eval(row).not() }

// isNullNode is `column IS [NOT] NULL`. It is never UNKNOWN.
type isNullNode struct {
	column  string
	negated bool
}

func (n isNullNode) eval(row ir.Row) truth {
	isNull := row[n.column] == nil
	if n.negated {
		isNull = !isNull
	}
	if isNull {
		return truthTrue
	}
	return truthFalse
}

// cmpNode is `column op literal`.
type cmpNode struct {
	column string
	op     cmpOp
	fam    Family
	lit    literal
}

func (n cmpNode) eval(row ir.Row) truth {
	v, ok := row[n.column]
	if !ok || v == nil {
		return truthUnknown // NULL operand → UNKNOWN
	}
	return compareValue(n.fam, v, n.op, n.lit)
}

// inNode is `column [NOT] IN (lit, ...)`, desugared to OR-of-equalities
// with SQL NULL semantics.
type inNode struct {
	column  string
	fam     Family
	lits    []literal
	negated bool
}

func (n inNode) eval(row ir.Row) truth {
	v, ok := row[n.column]
	if !ok || v == nil {
		return truthUnknown
	}
	res := truthFalse
	for _, l := range n.lits {
		switch compareValue(n.fam, v, opEq, l) {
		case truthTrue:
			res = truthTrue
		case truthUnknown:
			if res != truthTrue {
				res = truthUnknown
			}
		}
	}
	if n.negated {
		return res.not()
	}
	return res
}

// -------------------- tokenizer --------------------

type tokKind uint8

const (
	tkIdent tokKind = iota // bare or quoted column identifier
	tkNumber
	tkString // single-quoted literal
	tkKeyword
	tkOp     // = != <> < <= > >=
	tkLParen // (
	tkRParen // )
	tkComma  // ,
)

type token struct {
	kind tokKind
	text string // canonical text; keywords are upper-cased
}

var keywords = map[string]bool{
	"AND": true, "OR": true, "NOT": true, "IS": true,
	"NULL": true, "IN": true, "TRUE": true, "FALSE": true,
}

func tokenize(s string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, token{tkLParen, "("})
			i++
		case c == ')':
			toks = append(toks, token{tkRParen, ")"})
			i++
		case c == ',':
			toks = append(toks, token{tkComma, ","})
			i++
		case c == '\'':
			lit, next, err := lexString(s, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{tkString, lit})
			i = next
		case c == '"' || c == '`':
			ident, next, err := lexQuotedIdent(s, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{tkIdent, ident})
			i = next
		case c == '=' || c == '!' || c == '<' || c == '>':
			op, next, err := lexOp(s, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{tkOp, op})
			i = next
		case c == '-' || c == '+' || (c >= '0' && c <= '9'):
			num, next := lexNumber(s, i)
			toks = append(toks, token{tkNumber, num})
			i = next
		case isIdentStart(c):
			word, next := lexBareWord(s, i)
			if keywords[strings.ToUpper(word)] {
				toks = append(toks, token{tkKeyword, strings.ToUpper(word)})
			} else {
				toks = append(toks, token{tkIdent, word})
			}
			i = next
		default:
			return nil, fmt.Errorf("unexpected character %q", string(c))
		}
	}
	return toks, nil
}

func lexString(s string, i int) (lit string, next int, err error) {
	// s[i] == '\''. SQL doubles a quote to escape it.
	var b strings.Builder
	j := i + 1
	for j < len(s) {
		if s[j] == '\'' {
			if j+1 < len(s) && s[j+1] == '\'' {
				b.WriteByte('\'')
				j += 2
				continue
			}
			return b.String(), j + 1, nil
		}
		if s[j] == '\\' {
			// Backslash escapes are MySQL-dialect-specific and their
			// meaning is sql_mode-dependent; refuse rather than guess.
			return "", 0, fmt.Errorf("string literal contains a backslash escape, whose meaning is dialect/sql_mode-dependent")
		}
		b.WriteByte(s[j])
		j++
	}
	return "", 0, fmt.Errorf("unterminated string literal")
}

func lexQuotedIdent(s string, i int) (ident string, next int, err error) {
	q := s[i]
	var b strings.Builder
	j := i + 1
	for j < len(s) {
		if s[j] == q {
			return b.String(), j + 1, nil
		}
		b.WriteByte(s[j])
		j++
	}
	return "", 0, fmt.Errorf("unterminated quoted identifier")
}

func lexOp(s string, i int) (op string, next int, err error) {
	switch s[i] {
	case '=':
		return "=", i + 1, nil
	case '!':
		if i+1 < len(s) && s[i+1] == '=' {
			return "!=", i + 2, nil
		}
		return "", 0, fmt.Errorf("unexpected %q (did you mean !=?)", "!")
	case '<':
		if i+1 < len(s) {
			switch s[i+1] {
			case '=':
				return "<=", i + 2, nil
			case '>':
				return "<>", i + 2, nil
			}
		}
		return "<", i + 1, nil
	case '>':
		if i+1 < len(s) && s[i+1] == '=' {
			return ">=", i + 2, nil
		}
		return ">", i + 1, nil
	}
	return "", 0, fmt.Errorf("unexpected operator character %q", string(s[i]))
}

func lexNumber(s string, i int) (num string, next int) {
	j := i
	if s[j] == '-' || s[j] == '+' {
		j++
	}
	for j < len(s) {
		c := s[j]
		if (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-' {
			j++
			continue
		}
		break
	}
	return s[i:j], j
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}

func lexBareWord(s string, i int) (word string, next int) {
	j := i
	for j < len(s) && isIdentPart(s[j]) {
		j++
	}
	return s[i:j], j
}

// -------------------- parser --------------------

type parser struct {
	toks  []token
	pos   int
	infos map[string]ColumnInfo
}

func (p *parser) atEnd() bool   { return p.pos >= len(p.toks) }
func (p *parser) peek() token   { return p.toks[p.pos] }
func (p *parser) next() token   { t := p.toks[p.pos]; p.pos++; return t }
func (p *parser) hasMore() bool { return p.pos < len(p.toks) }

func (p *parser) isKeyword(kw string) bool {
	return p.hasMore() && p.peek().kind == tkKeyword && p.peek().text == kw
}

func (p *parser) parseOr() (node, error) {
	first, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	kids := []node{first}
	for p.isKeyword("OR") {
		p.next()
		n, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		kids = append(kids, n)
	}
	if len(kids) == 1 {
		return kids[0], nil
	}
	return orNode{kids: kids}, nil
}

func (p *parser) parseAnd() (node, error) {
	first, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	kids := []node{first}
	for p.isKeyword("AND") {
		p.next()
		n, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		kids = append(kids, n)
	}
	if len(kids) == 1 {
		return kids[0], nil
	}
	return andNode{kids: kids}, nil
}

func (p *parser) parseNot() (node, error) {
	if p.isKeyword("NOT") {
		p.next()
		kid, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notNode{kid: kid}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (node, error) {
	if !p.hasMore() {
		return nil, fmt.Errorf("expected a column comparison or '(' but the predicate ended")
	}
	if p.peek().kind == tkLParen {
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if !p.hasMore() || p.peek().kind != tkRParen {
			return nil, fmt.Errorf("missing closing ')'")
		}
		p.next()
		return inner, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (node, error) {
	if p.peek().kind != tkIdent {
		return nil, fmt.Errorf("expected a column name, got %q", p.peek().text)
	}
	colTok := p.next()
	col := colTok.text
	info, ok := p.infos[strings.ToLower(col)]
	if !ok {
		return nil, fmt.Errorf("unknown column %q (the predicate must reference columns of the filtered table)", col)
	}
	if !p.hasMore() {
		return nil, fmt.Errorf("column %q is not followed by a comparison", col)
	}
	// column IS [NOT] NULL
	if p.isKeyword("IS") {
		p.next()
		negated := false
		if p.isKeyword("NOT") {
			p.next()
			negated = true
		}
		if !p.isKeyword("NULL") {
			return nil, fmt.Errorf("expected NULL after IS on column %q", col)
		}
		p.next()
		return isNullNode{column: strings.ToLower(col), negated: negated}, nil
	}
	// column [NOT] IN (...)
	negatedIn := false
	if p.isKeyword("NOT") {
		p.next()
		if !p.isKeyword("IN") {
			return nil, fmt.Errorf("expected IN after NOT on column %q", col)
		}
		negatedIn = true
	}
	if negatedIn || p.isKeyword("IN") {
		p.next() // consume IN
		return p.parseIn(col, info, negatedIn)
	}
	// column op literal
	if p.peek().kind != tkOp {
		return nil, fmt.Errorf("expected a comparison operator after column %q, got %q", col, p.peek().text)
	}
	opTok := p.next()
	op, err := parseOp(opTok.text)
	if err != nil {
		return nil, err
	}
	if !p.hasMore() {
		return nil, fmt.Errorf("comparison on %q is missing its right-hand literal", col)
	}
	lit, err := p.parseLiteral()
	if err != nil {
		return nil, err
	}
	if err := checkComparable(col, info, op, lit); err != nil {
		return nil, err
	}
	return cmpNode{column: strings.ToLower(col), op: op, fam: info.Family, lit: lit}, nil
}

func (p *parser) parseIn(col string, info ColumnInfo, negated bool) (node, error) {
	if !p.hasMore() || p.peek().kind != tkLParen {
		return nil, fmt.Errorf("expected '(' after IN on column %q", col)
	}
	p.next()
	var lits []literal
	for {
		if !p.hasMore() {
			return nil, fmt.Errorf("unterminated IN list on column %q", col)
		}
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		if err := checkComparable(col, info, opEq, lit); err != nil {
			return nil, err
		}
		lits = append(lits, lit)
		if !p.hasMore() {
			return nil, fmt.Errorf("unterminated IN list on column %q", col)
		}
		switch p.peek().kind {
		case tkComma:
			p.next()
			continue
		case tkRParen:
			p.next()
			if len(lits) == 0 {
				return nil, fmt.Errorf("empty IN list on column %q", col)
			}
			return inNode{column: strings.ToLower(col), fam: info.Family, lits: lits, negated: negated}, nil
		default:
			return nil, fmt.Errorf("expected ',' or ')' in IN list on column %q, got %q", col, p.peek().text)
		}
	}
}

func (p *parser) parseLiteral() (literal, error) {
	if !p.hasMore() {
		return literal{}, fmt.Errorf("expected a literal but the predicate ended")
	}
	t := p.next()
	switch t.kind {
	case tkNumber:
		r := new(big.Rat)
		if _, ok := r.SetString(t.text); !ok {
			return literal{}, fmt.Errorf("malformed numeric literal %q", t.text)
		}
		return literal{kind: litNumber, num: r, text: t.text}, nil
	case tkString:
		return literal{kind: litString, str: t.text}, nil
	case tkKeyword:
		switch t.text {
		case "TRUE":
			return literal{kind: litBool, b: true}, nil
		case "FALSE":
			return literal{kind: litBool, b: false}, nil
		case "NULL":
			return literal{kind: litNull}, nil
		}
		return literal{}, fmt.Errorf("unexpected keyword %q where a literal was expected", t.text)
	default:
		return literal{}, fmt.Errorf("expected a literal (number, 'string', TRUE, FALSE, NULL), got %q", t.text)
	}
}

// -------------------- literals & comparison --------------------

type litKind uint8

const (
	litNumber litKind = iota
	litString
	litBool
	litNull
)

type literal struct {
	kind litKind
	num  *big.Rat
	str  string
	b    bool
	text string
}

type cmpOp uint8

const (
	opEq cmpOp = iota
	opNe
	opLt
	opLe
	opGt
	opGe
)

func parseOp(s string) (cmpOp, error) {
	switch s {
	case "=":
		return opEq, nil
	case "!=", "<>":
		return opNe, nil
	case "<":
		return opLt, nil
	case "<=":
		return opLe, nil
	case ">":
		return opGt, nil
	case ">=":
		return opGe, nil
	}
	return 0, fmt.Errorf("unsupported operator %q", s)
}

func (o cmpOp) isOrdering() bool { return o != opEq && o != opNe }

// checkComparable is the compile-time fidelity gate for one comparison:
// it refuses (returns an error) when the column family, the operator, and
// the literal kind do not combine into a faithfully-evaluable comparison.
func checkComparable(col string, info ColumnInfo, op cmpOp, lit literal) error {
	if lit.kind == litNull {
		return fmt.Errorf("comparison to NULL on %q must be written `IS NULL` / `IS NOT NULL` (a `= NULL` comparison is always UNKNOWN in SQL)", col)
	}
	switch info.Family {
	case FamilyNumeric:
		if lit.kind != litNumber {
			return fmt.Errorf("numeric column %q compared to a non-numeric literal", col)
		}
		return nil
	case FamilyBool:
		if op.isOrdering() {
			return fmt.Errorf("ordering comparison on boolean column %q is not supported (use = / !=)", col)
		}
		if lit.kind == litBool {
			return nil
		}
		if lit.kind == litNumber && (lit.text == "0" || lit.text == "1") {
			return nil
		}
		return fmt.Errorf("boolean column %q must be compared to TRUE / FALSE (or 0 / 1)", col)
	case FamilyString:
		if op.isOrdering() {
			return fmt.Errorf("ordering comparison (<, <=, >, >=) on string column %q is collation-dependent and cannot be evaluated faithfully client-side; use =, !=, IN, or IS [NOT] NULL", col)
		}
		if lit.kind != litString {
			return fmt.Errorf("string column %q compared to a non-string literal", col)
		}
		if !info.CaseSensitive {
			return fmt.Errorf("string column %q has a case/accent-insensitive (or unknown) collation, so a client-side byte comparison could diverge from the source's own evaluation; normalize the value on the source and filter on that", col)
		}
		return nil
	case FamilyBinary:
		if op.isOrdering() {
			return fmt.Errorf("ordering comparison on binary/JSON column %q is not supported; use =, !=, or IS [NOT] NULL", col)
		}
		if lit.kind != litString {
			return fmt.Errorf("binary/JSON column %q can only be compared to a string literal for equality", col)
		}
		return nil
	case FamilyTemporal:
		if lit.kind != litString {
			return fmt.Errorf("temporal column %q must be compared to a quoted date/time literal", col)
		}
		if _, ok := parseTemporalLiteral(lit.str); !ok {
			return fmt.Errorf("temporal column %q compared to %q, which is not a recognized date/time literal", col, lit.str)
		}
		return nil
	default:
		return fmt.Errorf("column %q has a type the client-side --where evaluator cannot compare (arrays, sets, geometry, tz-aware timestamps, …)", col)
	}
}

// compareValue evaluates `value op lit` for a non-NULL value under the
// column's family. Any value that does not match the contract for its
// family (a bug upstream, or a NaN/Inf float) yields UNKNOWN rather than a
// guessed answer.
func compareValue(fam Family, value any, op cmpOp, lit literal) truth {
	switch fam {
	case FamilyNumeric:
		return compareNumeric(value, op, lit)
	case FamilyBool:
		return compareBool(value, op, lit)
	case FamilyString:
		return compareString(value, op, lit)
	case FamilyBinary:
		return compareBinary(value, op, lit)
	case FamilyTemporal:
		return compareTemporal(value, op, lit)
	default:
		return truthUnknown
	}
}

func compareNumeric(value any, op cmpOp, lit literal) truth {
	left, ok := numericToRat(value)
	if !ok {
		return truthUnknown
	}
	return orderToTruth(left.Cmp(lit.num), op)
}

func numericToRat(value any) (*big.Rat, bool) {
	switch v := value.(type) {
	case int64:
		return new(big.Rat).SetInt64(v), true
	case uint64:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(v)), true
	case int:
		return new(big.Rat).SetInt64(int64(v)), true
	case float64:
		r := new(big.Rat)
		if _, ok := r.SetString(strconv.FormatFloat(v, 'g', -1, 64)); !ok {
			return nil, false // NaN / ±Inf have no rational form
		}
		return r, true
	case string:
		r := new(big.Rat)
		if _, ok := r.SetString(strings.TrimSpace(v)); !ok {
			return nil, false
		}
		return r, true
	default:
		return nil, false
	}
}

func compareBool(value any, op cmpOp, lit literal) truth {
	b, ok := value.(bool)
	if !ok {
		return truthUnknown
	}
	var want bool
	switch lit.kind {
	case litBool:
		want = lit.b
	case litNumber:
		want = lit.text == "1"
	default:
		return truthUnknown
	}
	equal := b == want
	switch op {
	case opEq:
		return boolToTruth(equal)
	case opNe:
		return boolToTruth(!equal)
	default:
		return truthUnknown
	}
}

func compareString(value any, op cmpOp, lit literal) truth {
	s, ok := value.(string)
	if !ok {
		return truthUnknown
	}
	equal := s == lit.str
	switch op {
	case opEq:
		return boolToTruth(equal)
	case opNe:
		return boolToTruth(!equal)
	default:
		return truthUnknown
	}
}

func compareBinary(value any, op cmpOp, lit literal) truth {
	b, ok := value.([]byte)
	if !ok {
		return truthUnknown
	}
	equal := bytes.Equal(b, []byte(lit.str))
	switch op {
	case opEq:
		return boolToTruth(equal)
	case opNe:
		return boolToTruth(!equal)
	default:
		return truthUnknown
	}
}

func compareTemporal(value any, op cmpOp, lit literal) truth {
	t, ok := value.(time.Time)
	if !ok {
		return truthUnknown
	}
	want, ok := parseTemporalLiteral(lit.str)
	if !ok {
		return truthUnknown
	}
	// Compare wall-clock instants in UTC (the row value is UTC per the
	// value contract; the literal is parsed as UTC wall-clock).
	var order int
	switch {
	case t.Before(want):
		order = -1
	case t.After(want):
		order = 1
	default:
		order = 0
	}
	return orderToTruth(order, op)
}

// temporalLayouts are the date/time literal forms the evaluator accepts,
// parsed in UTC. Ordered most-specific first.
var temporalLayouts = []string{
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
}

func parseTemporalLiteral(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range temporalLayouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// orderToTruth maps a three-way comparison result (cmp: -1/0/1) and an
// operator into a truth value.
func orderToTruth(cmp int, op cmpOp) truth {
	var b bool
	switch op {
	case opEq:
		b = cmp == 0
	case opNe:
		b = cmp != 0
	case opLt:
		b = cmp < 0
	case opLe:
		b = cmp <= 0
	case opGt:
		b = cmp > 0
	case opGe:
		b = cmp >= 0
	}
	return boolToTruth(b)
}

func boolToTruth(b bool) truth {
	if b {
		return truthTrue
	}
	return truthFalse
}
