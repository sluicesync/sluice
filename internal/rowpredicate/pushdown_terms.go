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
	// resolver can never carry this flag on a DATE column. It remains the
	// push-down classifier's fail-closed BELT: a DATE-column term still
	// carrying it was compiled WITHOUT the engine lens and must classify
	// out of the envelope.
	TemporalLiteralTimeBearing bool

	// TemporalLiteralSubMicrosecond marks a temporal-family comparison
	// whose literal carries MORE than 6 fractional-second digits — finer
	// than every supported engine's µs resolution (Postgres rounds
	// half-even, MySQL rounds half-up, MariaDB truncates; ground truth in
	// [ir.TemporalLiteralSemantics]) while an un-normalized client compare
	// keeps Go's nanosecond precision (audit 2026-07-23 D0-5). Compile's
	// Q1 normalization rewrites such literals to the engine's µs value, so
	// like TemporalLiteralTimeBearing this is the classifier's fail-closed
	// belt for a compile that missed the engine lens. Implies
	// TemporalLiteralTimeBearing (a fractional second requires a time
	// component).
	TemporalLiteralSubMicrosecond bool

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
			BoolNumericLiteral: t.fam == FamilyBool && t.lit.kind == litNumber,
		}
		if t.fam == FamilyTemporal && t.lit.kind == litString {
			term.TemporalLiteralTimeBearing, term.TemporalLiteralSubMicrosecond = temporalLiteralGranularity(t.lit.str)
		}
		*terms = append(*terms, term)
	case inNode:
		term := PushdownTerm{Column: t.column}
		for _, l := range t.lits {
			if t.fam == FamilyBool && l.kind == litNumber {
				term.BoolNumericLiteral = true
			}
			if t.fam == FamilyTemporal && l.kind == litString {
				tb, sm := temporalLiteralGranularity(l.str)
				term.TemporalLiteralTimeBearing = term.TemporalLiteralTimeBearing || tb
				term.TemporalLiteralSubMicrosecond = term.TemporalLiteralSubMicrosecond || sm
			}
		}
		*terms = append(*terms, term)
	case isNullNode:
		*terms = append(*terms, PushdownTerm{Column: t.column})
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
