// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"fmt"
	"strings"
	"time"
)

// PushdownTerm describes one leaf comparison of a compiled predicate — the
// facts an engine-side push-down eligibility classifier needs that the
// column schema alone cannot supply (ADR-0176, the Postgres publication
// row-filter push-down). The AST stays private to this package; classifiers
// consume these flattened terms instead.
//
// One entry is emitted per leaf (cmp / IN / IS NULL) in walk order, so a
// column referenced twice appears twice — classifiers iterate every term
// rather than deduplicating, because two comparisons on one column can have
// different literal kinds.
type PushdownTerm struct {
	// Column is the lower-cased referenced column name (the same
	// normalization [Compile] applies).
	Column string
	// Op is the leaf's canonical operator spelling, one of
	// [GrammarLeafOps] ("<>" folds to "!="). Consumers use it to prove
	// per-operator coverage against the grammar's inventory — the
	// push-down oracle's op-completeness gate (audit 2026-07-23 TEST-2 /
	// G-12) compiles every oracle cell and asserts the union of emitted
	// Ops covers GrammarLeafOps, so a new grammar operator cannot ship
	// without oracle cells. Empty only on an Unrecognized term.
	Op string
	// BoolNumericLiteral marks a BOOLEAN column compared against a numeric
	// 0/1 literal. The client grammar accepts it (the MySQL idiom `flag = 1`)
	// but it is not valid Postgres SQL — `boolean = integer` has no operator —
	// so a push-down site must classify the predicate ineligible rather than
	// emit SQL the server would reject.
	BoolNumericLiteral bool

	// TemporalLiteralTimeBearing marks a temporal-family comparison whose
	// literal carries a time-of-day component (anything beyond a bare
	// `YYYY-MM-DD`). Postgres coerces such a literal to a DATE column by
	// TRUNCATING the time of day (`d < '2024-01-01 12:00'` is stored as
	// `d < '2024-01-01'::date`), so an UN-normalized client compare of the
	// full instant provably disagrees with the server (audit 2026-07-23
	// D0-5). Compile normalizes temporal literals to the source engine's
	// own semantics (Q1), which rewrites exactly these literals — so a
	// predicate compiled through a [ir.TemporalLiteralCastToColumn]
	// resolver can never carry this flag on a DATE column, and the
	// ClientExact zero value refuses the shape at compile (review F4). It
	// remains the push-down classifier's fail-closed BELT for a term that
	// reached the classifier without Compile's gates — and, under the
	// promote lenses (MySQL family), the LIVE signal a VStream push-down
	// site combines with the column type for the F3 temporal-coercion
	// fallback (a promote engine compares the full instant, so the literal
	// is NOT rewritten and the flag stays).
	TemporalLiteralTimeBearing bool

	// TemporalLiteralSubMicrosecond marks a temporal-family comparison
	// whose literal carries MORE than 6 fractional-second digits — finer
	// than every supported engine's µs resolution (Postgres rounds by its
	// double-mediated rint rule, MySQL rounds half-up, MariaDB truncates;
	// ground truth in
	// [ir.TemporalLiteralSemantics]) while an un-normalized client compare
	// keeps Go's nanosecond precision (audit 2026-07-23 D0-5). Compile can
	// never emit this flag: an engine lens rewrites the literal to the
	// engine's µs value, and the ClientExact zero value refuses it at
	// compile (review F4) — so like TemporalLiteralTimeBearing it is the
	// classifier's fail-closed belt for a term that bypassed Compile.
	// Implies TemporalLiteralTimeBearing (a fractional second requires a
	// time component).
	TemporalLiteralSubMicrosecond bool

	// TemporalLiteralNormalized marks a temporal-family comparison whose
	// literal Compile REWROTE to the source engine's coercion (a truncated
	// date under CastToColumn; a µs-rounded/truncated fraction under any
	// lens). The CLIENT evaluator is engine-faithful for such a term — but
	// the RAW predicate text still carries the finer-grained literal, so a
	// server-side push-down site whose OWN evaluator is not the source
	// engine (the VStream filter runs in vtgate's evalengine, whose
	// sub-µs/date coercion is unverified — ADR-0174 residuals) must route
	// the table through the A0-style client-side fallback instead of
	// pushing (review F3).
	TemporalLiteralNormalized bool

	// Unrecognized names the concrete AST node type of a leaf the walker
	// did not recognize (audit 2026-07-23 ARCH-1). Empty for every node the
	// grammar can produce today. A future grammar node (BETWEEN, LIKE, a
	// function call) that misses a case in [collectPushdownTerms] surfaces
	// here instead of silently contributing ZERO terms — which would let an
	// unproven construct push down — and every push-down classifier must
	// treat a non-empty Unrecognized as ineligible: the walker fails CLOSED
	// (the CLAUDE.md "no skip-branch without proof" class).
	Unrecognized string
}

// GrammarLeafOps is the canonical inventory of leaf operators the
// rowpredicate grammar compiles — one spelling per operator ("<>" folds
// to "!=", per [cmpOp.spelling]). KEEP IN LOCKSTEP with [parseOp] and
// the leaf node types (cmpNode / inNode / isNullNode): this is the
// enumeration source coverage gates assert against (the push-down
// oracle's op-completeness pin, audit 2026-07-23 TEST-2 / G-12), so a
// new operator added to the grammar without a matching entry here — or
// an entry without a compiling operator — fails
// TestGrammarLeafOps_EveryOpCompilesAndEmits before it can ship
// untested. AND/OR/NOT are compositions, not leaves; coverage gates pin
// them separately.
func GrammarLeafOps() []string {
	return []string{"=", "!=", "<", "<=", ">", ">=", "IN", "NOT IN", "IS NULL", "IS NOT NULL"}
}

// spelling renders a cmpOp as its canonical [GrammarLeafOps] token.
func (o cmpOp) spelling() string {
	switch o {
	case opEq:
		return "="
	case opNe:
		return "!="
	case opLt:
		return "<"
	case opLe:
		return "<="
	case opGt:
		return ">"
	case opGe:
		return ">="
	}
	return fmt.Sprintf("cmpOp(%d)", o)
}

// PushdownTerms returns the predicate's leaf comparisons for push-down
// eligibility classification. A nil predicate has no terms.
func (p *Predicate) PushdownTerms() []PushdownTerm {
	if p == nil || p.root == nil {
		return nil
	}
	var terms []PushdownTerm
	collectPushdownTerms(p.root, &terms)
	return terms
}

// collectPushdownTerms walks n in AST order, appending one PushdownTerm per
// leaf. Node types are value receivers stored as node values (the same shape
// [valueComparedColumns] switches on).
func collectPushdownTerms(n node, terms *[]PushdownTerm) {
	switch t := n.(type) {
	case andNode:
		for _, k := range t.kids {
			collectPushdownTerms(k, terms)
		}
	case orNode:
		for _, k := range t.kids {
			collectPushdownTerms(k, terms)
		}
	case notNode:
		collectPushdownTerms(t.kid, terms)
	case cmpNode:
		term := PushdownTerm{
			Column:             t.column,
			Op:                 t.op.spelling(),
			BoolNumericLiteral: t.fam == FamilyBool && t.lit.kind == litNumber,
		}
		if t.fam == FamilyTemporal && t.lit.kind == litString {
			term.TemporalLiteralTimeBearing, term.TemporalLiteralSubMicrosecond = temporalLiteralGranularity(t.lit.str)
			term.TemporalLiteralNormalized = t.lit.normalized
		}
		*terms = append(*terms, term)
	case inNode:
		op := "IN"
		if t.negated {
			op = "NOT IN"
		}
		term := PushdownTerm{Column: t.column, Op: op}
		for _, l := range t.lits {
			if t.fam == FamilyBool && l.kind == litNumber {
				term.BoolNumericLiteral = true
			}
			if t.fam == FamilyTemporal && l.kind == litString {
				tb, sm := temporalLiteralGranularity(l.str)
				term.TemporalLiteralTimeBearing = term.TemporalLiteralTimeBearing || tb
				term.TemporalLiteralSubMicrosecond = term.TemporalLiteralSubMicrosecond || sm
				term.TemporalLiteralNormalized = term.TemporalLiteralNormalized || l.normalized
			}
		}
		*terms = append(*terms, term)
	case isNullNode:
		op := "IS NULL"
		if t.negated {
			op = "IS NOT NULL"
		}
		*terms = append(*terms, PushdownTerm{Column: t.column, Op: op})
	default:
		// ARCH-1 (audit 2026-07-23): a node type this walk does not know
		// contributes an Unrecognized term rather than NOTHING — silence
		// here would let a future grammar construct push down unproven.
		// See PushdownTerm.Unrecognized.
		*terms = append(*terms, PushdownTerm{Unrecognized: fmt.Sprintf("%T", n)})
	}
}

// temporalLiteralGranularity classifies a temporal literal's TEXT
// granularity — consumed by both Compile's engine-semantics normalization
// (which rewrites exactly the finer-than-engine texts) and the push-down
// term flags above (audit 2026-07-23 D0-5). A literal
// that parses as a bare date (`YYYY-MM-DD`) carries neither flag; anything
// else the temporal grammar accepted is time-bearing, and a fractional-
// second part longer than 6 digits is additionally sub-microsecond. The
// caller guarantees the literal already passed [checkComparable]'s
// parseTemporalLiteral gate, so this never sees unparseable text (Go's
// parser caps fractional seconds at 9 digits — more never compiles).
func temporalLiteralGranularity(lit string) (timeBearing, subMicro bool) {
	lit = strings.TrimSpace(lit)
	if _, err := time.ParseInLocation("2006-01-02", lit, time.UTC); err == nil {
		return false, false
	}
	timeBearing = true
	if i := strings.LastIndexByte(lit, '.'); i >= 0 {
		frac := 0
		for _, r := range lit[i+1:] {
			if r < '0' || r > '9' {
				frac = 0
				break
			}
			frac++
		}
		subMicro = frac > 6
	}
	return timeBearing, subMicro
}
