// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

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
		*terms = append(*terms, PushdownTerm{
			Column:             t.column,
			BoolNumericLiteral: t.fam == FamilyBool && t.lit.kind == litNumber,
		})
	case inNode:
		term := PushdownTerm{Column: t.column}
		if t.fam == FamilyBool {
			for _, l := range t.lits {
				if l.kind == litNumber {
					term.BoolNumericLiteral = true
					break
				}
			}
		}
		*terms = append(*terms, term)
	case isNullNode:
		*terms = append(*terms, PushdownTerm{Column: t.column})
	}
}
