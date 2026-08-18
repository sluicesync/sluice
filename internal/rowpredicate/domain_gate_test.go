// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_RowpredicateDispatchRoster is this package's
// instantiation of the Bug 233 gate (audit A-3): every column-type dispatch
// either reads the STORAGE type through ir.UnwrapDomain or carries a written,
// code-verified reason.
//
// Both of the package's dispatch sites were REAL Bug 233 instances in the
// `--where` filtered-sync fidelity path: columnInfoFor's family switch (a
// DOMAIN-over-anything matched no arm and fell to FamilyUnsupported, REFUSING an
// otherwise faithfully-evaluable predicate) and ColumnInfosFromIR's isCidr read
// (a DOMAIN-over-cidr would render as inet). Both now read the STORAGE type, so
// the roster is empty; the correctness proof is in domain_gate_pin_test.go.
//
// See the domaingate package doc for what a pass proves and what it does not.
func TestDomainTransparency_RowpredicateDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "rowpredicate",
		// 2 dispatch sites, now reading storage via UnwrapDomain (still count
		// toward the floor); this holds the shape against a refactor.
		MinSites: 1,
		Allowed:  map[string]string{},
	})
}
