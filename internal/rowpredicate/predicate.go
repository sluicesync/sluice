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
// One gate NORMALIZES instead of refusing: a temporal literal finer-grained
// than the column (a time-of-day on a DATE column; >6 fractional-second
// digits) is rewritten at compile time to the SOURCE engine's own coercion
// of it ([ir.TemporalLiteralSemantics], supplied by the engine's resolver),
// so the client evaluator classifies exactly as the engine's snapshot
// SELECT / pushed stream filter does — the "filtered replicas follow the
// source engine's comparison semantics" contract (audit 2026-07-23 D0-5).
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
	"math"
	"math/big"
	"sort"
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

// Columns returns the sorted set of column names (lower-cased, the same
// normalization [Compile] applies) the predicate references. A nil
// predicate references nothing. Callers use it to verify a decoded row
// carries every column the evaluation will read — a filtered CDC
// before-image that omits a referenced column must be refused, not
// evaluated (a missing column reads as UNKNOWN and would silently
// mis-classify a row-move).
func (p *Predicate) Columns() []string {
	if p == nil || p.root == nil {
		return nil
	}
	set := map[string]bool{}
	p.root.columns(set)
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// ValueComparedColumns returns the lower-cased columns this predicate references
// in a VALUE comparison — a cmpNode (`col op literal`) or an IN membership —
// EXCLUDING columns that appear ONLY in `col IS [NOT] NULL` (a presence test
// whose result cannot depend on the stored value's exact bits).
//
// It is a strict subset of [Predicate.Columns]. The SL1 A0-fallback guard
// (pipeline/where_cdc_filter.go) uses it so a single-precision FLOAT referenced
// only in a display-round-INSENSITIVE `IS NULL` is not mistaken for a lossy
// ordering term and wrongly refused (audit 2026-07-19c F-WR-1).
func (p *Predicate) ValueComparedColumns() []string {
	if p == nil || p.root == nil {
		return nil
	}
	set := map[string]bool{}
	valueComparedColumns(p.root, set)
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// valueComparedColumns walks n, adding the column of every cmpNode / inNode
// (value comparisons) and skipping isNullNode (presence-only). Node types are
// value receivers stored as node values, so the switch matches on the concrete
// value types.
func valueComparedColumns(n node, set map[string]bool) {
	switch t := n.(type) {
	case andNode:
		for _, k := range t.kids {
			valueComparedColumns(k, set)
		}
	case orNode:
		for _, k := range t.kids {
			valueComparedColumns(k, set)
		}
	case notNode:
		valueComparedColumns(t.kid, set)
	case cmpNode:
		set[t.column] = true
	case inNode:
		set[t.column] = true
	case isNullNode:
		// presence test — display-round-insensitive; intentionally NOT recorded.
	}
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
	// FamilyNumeric is Integer / Decimal (EXACT numerics). Row values are
	// int64, uint64, or a decimal string; compared numerically via exact
	// big.Rat, which reproduces the source's integer/decimal `=` faithfully.
	FamilyNumeric
	// FamilyFloat is Float / Double (IEEE-754). Row values are float64. It is
	// split from FamilyNumeric because of an exact-vs-approximate divergence:
	// the client compares the literal EXACTLY (big.Rat) while the source
	// coerces the same literal to a 64-bit double, so a high-precision literal
	// (e.g. `0.10000000000000001` against a stored `0.1`) is classified out of
	// scope client-side but in scope by the source's `=`. Equality
	// (`=`/`!=`/`IN`) is therefore REFUSED at compile time; ordering
	// (`<`/`<=`/`>`/`>=`) is allowed (audit F0-5).
	FamilyFloat
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
// value family it holds and, for strings, the engine-resolved policy for
// reproducing the source's own equality client-side.
type ColumnInfo struct {
	Family Family
	// Faithful is meaningful only for [FamilyString]: true when the source's
	// `=` can be reproduced client-side (either byte-exact or via a resolved
	// collation-aware comparator). False means the comparison is REFUSED at
	// compile time (an unresolvable / non-UTF-8 / non-deterministic collation,
	// or --where-strict-collation on a fold path) rather than compared under a
	// guessed collation. The engine's [ir.CollationResolver] decides it.
	Faithful bool
	// Compare is meaningful only for [FamilyString] when Faithful: a non-nil
	// collation-aware comparator (MySQL ci/ai via the engine's Vitess-backed
	// resolver) that reproduces the source's `=`. nil means BYTE-EXACT (the
	// evaluator uses `==`). Threaded from [ir.StringEquality.Compare].
	Compare func(a, b string) bool
	// PadSpace is meaningful only for [FamilyString]: true when the column's
	// collation uses PAD SPACE comparison (trailing spaces ignored in `=`, the
	// MySQL legacy default). The evaluator right-trims ASCII spaces from both
	// operands before comparing so `'EU'` matches a stored `'EU '` as the
	// source's own `=` does (audit F0-1/F0-2). False on NO-PAD collations and
	// on byte-exact (Postgres) sources. Threaded from [ir.StringEquality.PadSpace].
	PadSpace bool

	// TemporalSemantics is meaningful only for [FamilyTemporal]: the SOURCE
	// engine's coercion rule for a temporal literal FINER-grained than the
	// column (a time-bearing literal on a DATE column; more fractional-second
	// digits than the engine's µs resolution), resolved from the engine's
	// [ir.TemporalLiteralResolver] by [ColumnInfosFromIR]. Compile normalizes
	// each temporal literal under this rule so the client evaluator reproduces
	// the source's own comparison — the "filtered replicas follow the source
	// engine's comparison semantics" contract (audit 2026-07-23 D0-5 / Q1).
	// The zero value ([ir.TemporalLiteralClientExact]) applies no
	// normalization: the pre-Q1 full-precision compare, which the push-down
	// classifier keeps out of any server-side envelope as its fail-closed belt.
	TemporalSemantics ir.TemporalLiteralSemantics
	// TemporalDateOnly is meaningful only for [FamilyTemporal]: true for a
	// DATE column, whose literal [ir.TemporalLiteralCastToColumn] (Postgres)
	// truncates to the date, while the promote engines (MySQL family) compare
	// the full instant against the date-at-midnight value.
	TemporalDateOnly bool
}

// faithfulString reports whether a string-column comparison can be evaluated
// client-side without diverging from the source, as resolved by the engine's
// [ir.CollationResolver]. When false, a string equality is refused.
func (ci ColumnInfo) faithfulString() bool {
	return ci.Faithful
}

// ColumnInfosFromIR builds the per-column fidelity map the evaluator
// needs from a table's IR columns, keyed by lower-cased column name.
// resolver is the SOURCE engine's [ir.CollationResolver]: it decides, for
// each string column, whether the source's `=` is reproducible client-side
// (byte-exact or via a collation-aware comparator) or must be refused —
// keeping the engine-neutral evaluator free of any collation-library
// dependency (audit 2026-07-18 M2.1).
// strict, when true, disables ADR-0174 Piece 1's faithful case/accent-
// insensitive comparison: a string column whose `=` is not byte-exact is
// left unreproducible (refused), the pre-0174 strict behavior. Wired from
// the operator's --where-strict-collation opt-out, forwarded to the resolver.
func ColumnInfosFromIR(resolver ir.CollationResolver, cols []*ir.Column, strict bool) map[string]ColumnInfo {
	// The temporal-literal lens is the resolver's optional companion surface
	// (audit 2026-07-23 D0-5 / Q1): the SOURCE engine names how it coerces a
	// finer-than-column temporal literal, and Compile normalizes literals to
	// match. A resolver without the surface leaves the zero value
	// (ClientExact — no normalization), which the push-down classifiers keep
	// out of their envelopes as the fail-closed belt.
	var temporal ir.TemporalLiteralSemantics
	if tr, ok := resolver.(ir.TemporalLiteralResolver); ok {
		temporal = tr.ResolveTemporalLiteralSemantics()
	}
	out := make(map[string]ColumnInfo, len(cols))
	for _, c := range cols {
		if c == nil {
			continue
		}
		out[strings.ToLower(c.Name)] = columnInfoFor(resolver, c, strict, temporal)
	}
	return out
}

func columnInfoFor(resolver ir.CollationResolver, c *ir.Column, strict bool, temporal ir.TemporalLiteralSemantics) ColumnInfo {
	switch t := c.Type.(type) {
	case ir.Integer, ir.Decimal:
		return ColumnInfo{Family: FamilyNumeric}
	case ir.Float:
		// Split from FamilyNumeric so `=`/`!=`/`IN` refuse (exact big.Rat vs
		// the source's IEEE-754 coercion diverge on high-precision literals);
		// ordering stays allowed (audit F0-5).
		return ColumnInfo{Family: FamilyFloat}
	case ir.Boolean:
		return ColumnInfo{Family: FamilyBool}
	case ir.Char:
		// fixedChar=true: a fixed-length CHAR/bpchar may pad (Postgres bpchar
		// `=` is PAD SPACE regardless of collation — audit 2026-07-19 A2).
		return stringColumnInfo(resolver, t.Collation, t.Determinism, strict, true)
	case ir.Varchar:
		return stringColumnInfo(resolver, t.Collation, t.Determinism, strict, false)
	case ir.Text:
		return stringColumnInfo(resolver, t.Collation, t.Determinism, strict, false)
	case ir.Enum:
		// A MySQL ENUM compares a value against a string literal under the
		// column's collation (a ci/ai enum matches `active` ↔ `Active`), so
		// route it through the resolver like varchar (audit 2026-07-19 M1-5) —
		// a byte-exact client compare would mis-classify the row-move. A PG enum
		// carries no collation (empty → the resolver's byte-exact path, matching
		// its exact-label `=`); a MySQL enum under an unreproducible collation
		// (or --where-strict-collation) refuses loudly, like any string column.
		return stringColumnInfo(resolver, t.Collation, ir.CollationDeterminismUnknown, strict, false)
	case ir.UUID, ir.Inet, ir.Cidr, ir.Macaddr:
		// Canonical/identifier-shaped ASCII values: the source's `=` is
		// exact, so a byte compare is faithful (no collation resolution).
		return ColumnInfo{Family: FamilyString, Faithful: true}
	case ir.Time:
		// Stored as a fixed-width string ("08:30:00[.ffffff]"); equality is
		// byte-exact. Ordering is refused (over-24h / fractional edge
		// cases), so no chronological parse is attempted.
		return ColumnInfo{Family: FamilyString, Faithful: true}
	case ir.Binary, ir.Varbinary, ir.Blob, ir.JSON:
		return ColumnInfo{Family: FamilyBinary}
	case ir.Date:
		return ColumnInfo{Family: FamilyTemporal, TemporalSemantics: temporal, TemporalDateOnly: true}
	case ir.DateTime:
		return ColumnInfo{Family: FamilyTemporal, TemporalSemantics: temporal}
	case ir.Timestamp:
		if t.WithTimeZone {
			// A tz-aware instant: the source interprets a bare literal in
			// its session timezone, which the client cannot reproduce
			// faithfully. Leave it unsupported so a comparison refuses.
			return ColumnInfo{Family: FamilyUnsupported}
		}
		return ColumnInfo{Family: FamilyTemporal, TemporalSemantics: temporal}
	default:
		return ColumnInfo{Family: FamilyUnsupported}
	}
}

// stringColumnInfo builds the ColumnInfo for a text-like column by asking the
// SOURCE engine's [ir.CollationResolver] how the column's `=` must be
// reproduced client-side. The resolver owns ALL collation reasoning (MySQL
// charset/PAD/ci-ai name rules + the Vitess-backed fold comparator; Postgres/
// SQLite determinism-driven byte-exact-or-refuse) so this package carries no
// collation-library dependency (audit 2026-07-18 M2.1). A non-faithful result
// leaves the column unreproducible, so its string comparisons refuse loudly at
// compile time.
func stringColumnInfo(resolver ir.CollationResolver, collation string, determinism ir.CollationDeterminism, strict, fixedChar bool) ColumnInfo {
	eq := resolver.ResolveStringEquality(collation, determinism, strict, fixedChar)
	if !eq.Faithful {
		return ColumnInfo{Family: FamilyString} // refuse
	}
	return ColumnInfo{Family: FamilyString, Faithful: true, Compare: eq.Compare, PadSpace: eq.PadSpace}
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
	// columns adds every column name this node (and its children)
	// references to set. Names are already lower-cased at parse time.
	columns(set map[string]bool)
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

func (n andNode) columns(set map[string]bool) {
	for _, k := range n.kids {
		k.columns(set)
	}
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

func (n orNode) columns(set map[string]bool) {
	for _, k := range n.kids {
		k.columns(set)
	}
}

type notNode struct{ kid node }

func (n notNode) eval(row ir.Row) truth { return n.kid.eval(row).not() }

func (n notNode) columns(set map[string]bool) { n.kid.columns(set) }

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

func (n isNullNode) columns(set map[string]bool) { set[n.column] = true }

// cmpNode is `column op literal`.
type cmpNode struct {
	column string
	op     cmpOp
	fam    Family
	lit    literal
	// compare, when non-nil, makes a FamilyString comparison use the source's
	// collation-aware comparator (ADR-0174 Piece 1, engine-supplied) instead of
	// a byte compare. nil means byte-exact.
	compare func(a, b string) bool
	// padSpace right-trims trailing ASCII spaces from a FamilyString compare
	// so a PAD SPACE collation's `=` is reproduced (audit F0-1/F0-2).
	padSpace bool
}

func (n cmpNode) eval(row ir.Row) truth {
	v, ok := row[n.column]
	if !ok || v == nil {
		return truthUnknown // NULL operand → UNKNOWN
	}
	return compareValue(n.fam, v, n.op, n.lit, n.compare, n.padSpace)
}

func (n cmpNode) columns(set map[string]bool) { set[n.column] = true }

// inNode is `column [NOT] IN (lit, ...)`, desugared to OR-of-equalities
// with SQL NULL semantics.
type inNode struct {
	column   string
	fam      Family
	lits     []literal
	negated  bool
	compare  func(a, b string) bool
	padSpace bool
}

func (n inNode) eval(row ir.Row) truth {
	v, ok := row[n.column]
	if !ok || v == nil {
		return truthUnknown
	}
	res := truthFalse
	for _, l := range n.lits {
		switch compareValue(n.fam, v, opEq, l, n.compare, n.padSpace) {
		case truthTrue:
			// audit F-P2: an IN list desugars to OR-of-equalities, where TRUE
			// dominates (an unmatched-but-NULL later member cannot demote a
			// definite match to UNKNOWN). So the first TRUE fixes the result —
			// break instead of paying compareValue (up to ~130ns + 2 allocs on
			// the collation-fold path) for every remaining member. NOT IN
			// (negated) is res.not(): res=TRUE → FALSE, equally final, so the
			// same short-circuit is correct there too.
			return n.maybeNegate(truthTrue)
		case truthUnknown:
			if res != truthTrue {
				res = truthUnknown
			}
		}
	}
	return n.maybeNegate(res)
}

// maybeNegate applies NOT-IN's negation to a computed IN result.
func (n inNode) maybeNegate(res truth) truth {
	if n.negated {
		return res.not()
	}
	return res
}

func (n inNode) columns(set map[string]bool) { set[n.column] = true }

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
	// Q1 (audit 2026-07-23 D0-5): a temporal literal finer-grained than the
	// column is rewritten to the SOURCE engine's own coercion of it, so the
	// client evaluator agrees with the engine-evaluated legs by construction.
	lit = normalizeTemporalLiteral(info, lit)
	return cmpNode{column: strings.ToLower(col), op: op, fam: info.Family, lit: lit, compare: info.Compare, padSpace: info.PadSpace}, nil
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
		// Q1 normalization, per IN member (see parseComparison).
		lits = append(lits, normalizeTemporalLiteral(info, lit))
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
			return inNode{column: strings.ToLower(col), fam: info.Family, lits: lits, negated: negated, compare: info.Compare, padSpace: info.PadSpace}, nil
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
	// normalized marks a temporal literal [normalizeTemporalLiteral]
	// REWROTE to the source engine's coercion (a truncated date, a
	// µs-rounded fraction). Surfaced as
	// [PushdownTerm.TemporalLiteralNormalized] so a server-side push-down
	// site can tell that the RAW predicate text carries finer granularity
	// than the engine the client now mirrors — the VStream A0-fallback
	// trigger (the vtgate evalengine's own coercion is unverified).
	normalized bool
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
	case FamilyFloat:
		if lit.kind != litNumber {
			return fmt.Errorf("floating-point column %q compared to a non-numeric literal", col)
		}
		if !op.isOrdering() {
			// opEq / opNe (and IN, which checks each member with opEq) on a
			// float column cannot be reproduced faithfully client-side: the
			// evaluator compares the literal EXACTLY (big.Rat) while the source
			// coerces it to a 64-bit double, so a high-precision literal like
			// 0.10000000000000001 diverges from a stored 0.1 (audit F0-5).
			return fmt.Errorf("equality (=, !=, IN) on floating-point column %q cannot be evaluated faithfully client-side: "+
				"the client compares the literal exactly while the source coerces it to an IEEE-754 double, so a high-precision "+
				"literal can diverge (0.10000000000000001 vs a stored 0.1) — use an ordering comparison (<, <=, >, >=) or a range, "+
				"or filter on a non-float column", col)
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
		if !info.faithfulString() {
			return fmt.Errorf("string column %q has a case/accent-insensitive or unknown collation whose `=` sluice cannot reproduce faithfully client-side (an unrecognized or absent collation, or --where-strict-collation is set), so a comparison could diverge from the source's own evaluation; use a recognized collation, normalize the value on the source and filter on that, or drop --where-strict-collation", col)
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
		// Review F4: a literal FINER-grained than the column can only be
		// compared faithfully by reproducing the source engine's coercion
		// of it, and under the ClientExact zero value there IS no engine
		// lens — the engines resolve it three different ways (see
		// [ir.TemporalLiteralSemantics]), so a full-precision compare is a
		// guess. Refuse, like every other unfaithful comparison; with an
		// engine lens the literal is normalized instead (below). This makes
		// "Compile output is always engine-granular or engine-normalized"
		// an invariant of the code, not a comment.
		if info.TemporalSemantics == ir.TemporalLiteralClientExact {
			timeBearing, subMicro := temporalLiteralGranularity(lit.str)
			if info.TemporalDateOnly && timeBearing {
				return fmt.Errorf("date column %q is compared to the time-bearing literal %q, and no source-engine temporal-literal semantics are available to resolve the granularity mismatch faithfully (engines disagree: Postgres truncates the literal to the date, MySQL/MariaDB compare the full instant)", col, lit.str)
			}
			if subMicro {
				return fmt.Errorf("temporal column %q is compared to %q, which has more than 6 fractional-second digits, and no source-engine temporal-literal semantics are available to resolve the sub-microsecond precision faithfully (engines disagree: Postgres and MySQL round differently, MariaDB truncates)", col, lit.str)
			}
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
func compareValue(fam Family, value any, op cmpOp, lit literal, compare func(a, b string) bool, padSpace bool) truth {
	switch fam {
	case FamilyNumeric:
		return compareNumeric(value, op, lit)
	case FamilyFloat:
		// FamilyFloat only reaches here for ordering ops (equality is refused
		// at compile time). Compare as float64 to match the source's IEEE-754
		// coercion of the literal: a big.Rat compare treats a high-precision
		// literal (0.10000000000000001) as distinct from a stored 0.1, but the
		// source coerces both to the same double, so `d >= 0.10000000000000001`
		// with a stored 0.1 diverges (audit 2026-07-19 B1).
		return compareFloat(value, op, lit)
	case FamilyBool:
		return compareBool(value, op, lit)
	case FamilyString:
		return compareString(value, op, lit, compare, padSpace)
	case FamilyBinary:
		return compareBinary(value, op, lit)
	case FamilyTemporal:
		return compareTemporal(value, op, lit)
	default:
		return truthUnknown
	}
}

// compareNumeric evaluates an EXACT-numeric (integer/decimal) comparison via
// big.Rat. A value with no rational form is first checked for the non-finite
// specials PG's NUMERIC can store — NaN, and (PG 14+) ±Infinity, which
// travel as strings per the ir.Decimal value contract — and ordered with
// PG's numeric total order: NaN above EVERYTHING (Infinity included),
// -Infinity below everything (audit 2026-07-23 review F2, the FamilyNumeric
// sibling of compareFloat's Q4 fix; PG is the only supported source whose
// exact-numeric type stores non-finite values, and ir.Decimal is INSIDE the
// push-down envelope, so mapping these to UNKNOWN silently dropped a
// server-delivered NaN row's changes under the benign-direction belt log).
// The grammar cannot spell a non-finite literal, so the literal side is
// always a finite big.Rat. Anything else unparseable stays UNKNOWN.
func compareNumeric(value any, op cmpOp, lit literal) truth {
	left, ok := numericToRat(value)
	if !ok {
		if cmp, nonFinite := numericNonFiniteOrder(value); nonFinite {
			return orderToTruth(cmp, op)
		}
		return truthUnknown
	}
	return orderToTruth(left.Cmp(lit.num), op)
}

// numericNonFiniteOrder classifies a numeric value with no rational form
// against any FINITE literal under PG's numeric total order: NaN and
// +Infinity order above (+1), -Infinity below (-1). Strings are recognized
// through strconv.ParseFloat (which accepts PG's "NaN"/"Infinity"/
// "-Infinity" spellings and Go's own renderings); a string that ParseFloat
// resolves to a FINITE value is NOT claimed here — the exact big.Rat path
// already rejected it, so it stays UNKNOWN rather than acquiring a lossy
// float verdict.
func numericNonFiniteOrder(value any) (cmp int, nonFinite bool) {
	var f float64
	switch v := value.(type) {
	case float64:
		f = v
	case float32:
		f = float64(v)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		f = parsed
	default:
		return 0, false
	}
	switch {
	case math.IsNaN(f):
		return 1, true // NaN sorts last, above Infinity
	case math.IsInf(f, 1):
		return 1, true
	case math.IsInf(f, -1):
		return -1, true
	}
	return 0, false
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

// compareFloat evaluates an ordering comparison on a FamilyFloat column as a
// float64 comparison, reproducing the source's IEEE-754 coercion of the literal
// (audit 2026-07-19 B1). Equality is refused at compile time, so only ordering
// (<, <=, >, >=) reaches here.
//
// NaN is ordered with Postgres's float TOTAL order — NaN greater than every
// non-NaN value, NaN equal to NaN (observed PG 16.14, 2026-07-23:
// `'NaN'::float8 > 0.1` → true) — audit 2026-07-23 D0-6 / owner call Q4.
// Mapping NaN to UNKNOWN made the two legs of one sync disagree on the same
// row: the snapshot SELECT (server-evaluated WHERE) included a NaN row, then
// the client CDC evaluator dropped its every UPDATE (stale target row) and
// swallowed its DELETE (orphan forever), at exit 0. Postgres is the only
// supported source that can deliver a float NaN (MySQL/MariaDB cannot store
// one; SQLite stores NaN as NULL), so the engine-neutral comparator applies
// the total order universally — no other engine can ever present the case.
// The literal side can only be NaN through a direct call (the grammar cannot
// spell it); it is handled anyway so the order is total, not conditional.
func compareFloat(value any, op cmpOp, lit literal) truth {
	left, ok := toFloat64(value)
	if !ok {
		return truthUnknown
	}
	right, err := strconv.ParseFloat(strings.TrimSpace(lit.text), 64)
	if err != nil {
		return truthUnknown
	}
	switch {
	case math.IsNaN(left) && math.IsNaN(right):
		return orderToTruth(0, op)
	case math.IsNaN(left):
		return orderToTruth(1, op) // NaN sorts last: greater than any non-NaN
	case math.IsNaN(right):
		return orderToTruth(-1, op)
	case left < right:
		return orderToTruth(-1, op)
	case left > right:
		return orderToTruth(1, op)
	default:
		return orderToTruth(0, op)
	}
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case int:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
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

func compareString(value any, op cmpOp, lit literal, compare func(a, b string) bool, padSpace bool) truth {
	s, ok := value.(string)
	if !ok {
		return truthUnknown
	}
	rhs := lit.str
	// audit F0-1/F0-2: a PAD SPACE collation ignores TRAILING spaces in `=`
	// (the engine comparator does not), so reproduce it by right-trimming ASCII
	// spaces from both operands before comparing — `'EU'` then matches a stored
	// `'EU '` exactly as the source's own `=` does. NO-PAD collations skip this.
	if padSpace {
		s = strings.TrimRight(s, " ")
		rhs = strings.TrimRight(rhs, " ")
	}
	// ADR-0174 Piece 1: under a resolved case/accent-insensitive collation,
	// reproduce the source's `=` via the engine's own comparator; otherwise the
	// column's `=` is byte-exact (the compile gate proved one of these).
	var equal bool
	if compare != nil {
		equal = compare(s, rhs)
	} else {
		equal = s == rhs
	}
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

// normalizeTemporalLiteral rewrites a FamilyTemporal literal to the value the
// SOURCE engine actually compares, per the engine's declared
// [ir.TemporalLiteralSemantics] (audit 2026-07-23 D0-5, owner call Q1:
// filtered replicas follow the source engine's comparison semantics — the
// snapshot SELECT and any server-side stream filter evaluate the RAW literal
// under these rules, so the client evaluator must reproduce them or the two
// legs of one sync classify the same row differently). Ground truth, observed
// 2026-07-23 on real servers:
//
//   - Postgres 16.14 (CastToColumn): the unknown-typed literal is cast to the
//     COLUMN's type — a DATE column TRUNCATES the time-of-day
//     (`d < '2026-01-15 12:00'` plans as `d < '2026-01-15'::date`); a
//     timestamp column rounds fractional seconds to µs by PG's
//     DOUBLE-MEDIATED rule (see [pgFractionMicros] — '.1234565' → .123456,
//     '.0001255' → .000125, '.0001265' → .000127, '.9999995' carries +1s).
//     A typmod column (timestamp(0)) does NOT truncate the literal.
//   - MySQL 8.0.46 (PromoteRoundHalfUp): the DATE column is PROMOTED to
//     datetime and the full instant compared (`d = '2026-01-15 08:30:00'` is
//     FALSE); fractional seconds beyond 6 digits round HALF-UP on the exact
//     decimal digits (away from zero: '.1234565' → .123457) and carry.
//   - MariaDB 11.8.8 (PromoteTruncate): promotes like MySQL, but fractional
//     seconds beyond 6 digits are TRUNCATED ('.1234565' → .123456, no carry).
//
// The rewrite keys on the literal's TEXT granularity (the same
// [temporalLiteralGranularity] facts the push-down classifier reads):
// literals already at engine granularity pass through byte-identical, so
// diagnostics keep the operator's spelling wherever it is already faithful.
// ClientExact (the zero value — no engine lens) never rewrites. Pinned
// against all three real servers by the temporal ground-truth matrix
// (temporal_realdb_integration_test.go).
func normalizeTemporalLiteral(info ColumnInfo, lit literal) literal {
	if info.Family != FamilyTemporal || lit.kind != litString ||
		info.TemporalSemantics == ir.TemporalLiteralClientExact {
		return lit
	}
	t, ok := parseTemporalLiteral(lit.str)
	if !ok {
		return lit // unreachable: checkComparable already refused unparseable literals
	}
	timeBearing, subMicro := temporalLiteralGranularity(lit.str)
	if info.TemporalSemantics == ir.TemporalLiteralCastToColumn && info.TemporalDateOnly {
		// Postgres DATE: the literal is cast to date — time-of-day truncated.
		if timeBearing {
			lit.str = t.Format("2006-01-02")
			lit.normalized = true
		}
		return lit
	}
	if !subMicro {
		return lit // already at (or coarser than) the µs engine resolution
	}
	switch info.TemporalSemantics {
	case ir.TemporalLiteralCastToColumn:
		// PG: replace the whole fraction with the server's double-mediated
		// micros (may carry into the seconds via Add).
		base := t.Add(-time.Duration(t.Nanosecond()))
		t = base.Add(time.Duration(pgFractionMicros(temporalFractionDigits(lit.str))) * time.Microsecond)
	case ir.TemporalLiteralPromoteRoundHalfUp:
		t = roundToMicrosHalfUp(t) // MySQL: half-up (away from zero), exact decimal
	case ir.TemporalLiteralPromoteTruncate:
		t = t.Truncate(time.Microsecond) // MariaDB: truncate, no carry
	}
	lit.str = t.Format("2006-01-02 15:04:05.999999")
	lit.normalized = true
	return lit
}

// pgFractionMicros reproduces Postgres's fractional-second parse EXACTLY:
// `fsec = rint(strtod(fraction) * 1000000)` (datetime.c, unchanged through
// PG 16/17) — the fraction goes THROUGH A C DOUBLE before rounding.
// strconv.ParseFloat is a correctly-rounded IEEE-754 parse (== strtod) and
// math.RoundToEven is rint under the default rounding mode, so this is
// byte-equivalent. The double mediation matters: the rule is NOMINALLY
// half-even, but an exact-decimal half rounds the way the BINARY double of
// the fraction lands (~0.1% of 7-digit fractions land on the other side of
// .5 than exact decimal arithmetic — '.0001255' → 125 where exact half-even
// gives 126, '.0001265' → 127; OBSERVED live on PG 16.14, 2026-07-23, and
// re-ground-truthed by the randomized fraction sweep in
// temporal_realdb_integration_test.go). An exact-decimal implementation
// here was the audit-review F1 CRITICAL: it agreed on every hand-picked
// boundary and silently diverged on the class.
func pgFractionMicros(fracDigits string) int64 {
	f, err := strconv.ParseFloat("0."+fracDigits, 64)
	if err != nil {
		return 0 // unreachable: the caller extracted digits-only text
	}
	return int64(math.RoundToEven(f * 1e6))
}

// temporalFractionDigits returns the fractional-second digit run of a
// temporal literal ("" when it has none). The caller has already vetted the
// literal through parseTemporalLiteral, so anything after the last '.' is
// the digit run.
func temporalFractionDigits(lit string) string {
	if i := strings.LastIndexByte(strings.TrimSpace(lit), '.'); i >= 0 {
		return strings.TrimSpace(lit)[i+1:]
	}
	return ""
}

// roundToMicrosHalfUp rounds t's sub-microsecond nanoseconds to the
// microsecond, half-up (away from zero — MySQL's rule, applied to the exact
// decimal digits), carrying into the seconds when the fraction rounds to
// 10⁶ µs. Literal fractions are at most 9 digits (Go's parser refuses
// more), so the parsed nanoseconds are exact and no float is involved —
// unlike Postgres's double-mediated rule ([pgFractionMicros]).
func roundToMicrosHalfUp(t time.Time) time.Time {
	ns := t.Nanosecond()
	rem := ns % 1000
	if rem == 0 {
		return t
	}
	micro := ns / 1000
	if rem >= 500 {
		micro++
	}
	return t.Add(time.Duration(micro*1000 - ns))
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
