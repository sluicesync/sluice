// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// domainOver wraps base in an ir.Domain — the shape the PG SchemaReader produces
// for a `CREATE DOMAIN … AS <base>` column (Bug 113).
func domainOver(base ir.Type) ir.Type {
	return ir.Domain{Name: "d_over_base", BaseType: base}
}

// TestPin_DomainColumnFamily_MatchesBase pins the Bug 233 fix in columnInfoFor:
// a Postgres DOMAIN is a constraint wrapper, not a storage one, so its `=`
// semantics are the base type's. Without the unwrap a domain-over-anything
// column matched no arm and fell to FamilyUnsupported, which REFUSES an
// otherwise faithfully-evaluable `--where` predicate. Spread across families so
// it is not a per-representative pin (Bug 74 discipline).
//
// Mutation check: revert the ir.UnwrapDomain in columnInfoFor's switch and every
// domain row below collapses to FamilyUnsupported, reddening this pin.
func TestPin_DomainColumnFamily_MatchesBase(t *testing.T) {
	cases := []struct {
		name string
		base ir.Type
		want Family
	}{
		{"domain-over-int", ir.Integer{}, FamilyNumeric},
		{"domain-over-decimal", ir.Decimal{}, FamilyNumeric},
		{"domain-over-bool", ir.Boolean{}, FamilyBool},
		{"domain-over-date", ir.Date{}, FamilyTemporal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := []*ir.Column{
				{Name: "bare", Type: tc.base},
				{Name: "wrapped", Type: domainOver(tc.base)},
			}
			infos := ColumnInfosFromIR(testPGResolver, cols, false)
			// Baseline: the bare column classifies as the family (mutation-test
			// anchor — true with or without the fix).
			if infos["bare"].Family != tc.want {
				t.Fatalf("bare %s: Family = %v; want %v — scaffold wrong", tc.name, infos["bare"].Family, tc.want)
			}
			// The fix: the domain-wrapped column classifies IDENTICALLY, rather
			// than falling to FamilyUnsupported and refusing the predicate.
			if infos["wrapped"].Family != tc.want {
				t.Errorf("%s: Family = %v; want %v — a domain-wrapped column matched no arm and fell to "+
					"FamilyUnsupported, refusing an otherwise faithful --where predicate (Bug 233)",
					tc.name, infos["wrapped"].Family, tc.want)
			}
		})
	}
}

// TestPin_DomainOverCidr_NetworkRendering pins the second Bug 233 fix: the
// isCidr read in ColumnInfosFromIR that selects cidr-vs-inet rendering must
// unwrap too, or a DOMAIN-over-cidr column renders as inet (the wrong prefix
// semantics) even though columnInfoFor already routed it to identifierNetwork.
//
// Mutation check: revert the ir.UnwrapDomain in the isCidr read and the
// domain-over-cidr column's NetworkRendering flips to the inet rendering,
// reddening this pin.
func TestPin_DomainOverCidr_NetworkRendering(t *testing.T) {
	cols := []*ir.Column{
		{Name: "bare_inet", Type: ir.Inet{}},
		{Name: "bare_cidr", Type: ir.Cidr{}},
		{Name: "dom_cidr", Type: domainOver(ir.Cidr{})},
	}
	infos := ColumnInfosFromIR(testPGResolver, cols, false)

	// Anti-vacuity: the two bare renderings must actually differ, or the isCidr
	// read is decorative and reverting it would not be observable.
	if infos["bare_cidr"].NetworkRendering == infos["bare_inet"].NetworkRendering {
		t.Fatalf("bare cidr and inet render identically under the PG resolver (%v) — this pin cannot see the "+
			"isCidr read; re-point it", infos["bare_cidr"].NetworkRendering)
	}
	if infos["dom_cidr"].Identifier != identifierNetwork {
		t.Fatalf("domain-over-cidr Identifier = %v; want identifierNetwork — columnInfoFor did not route it",
			infos["dom_cidr"].Identifier)
	}
	if infos["dom_cidr"].NetworkRendering != infos["bare_cidr"].NetworkRendering {
		t.Errorf("domain-over-cidr NetworkRendering = %v; want the bare cidr rendering %v — the isCidr read did "+
			"not unwrap, so a domain-over-cidr column renders with inet prefix semantics (Bug 233)",
			infos["dom_cidr"].NetworkRendering, infos["bare_cidr"].NetworkRendering)
	}
}
