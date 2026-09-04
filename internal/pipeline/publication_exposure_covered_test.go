// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "testing"

// A2-4b's coverage predicate, graded directly.
//
// WHY THIS IS A SEPARATE TEST. The integration pin
// (postgres.TestAuditPublicationExposure_MatchesRealPublicationCoverage)
// hands the auditor a hand-written `covered` and asserts what the auditor
// does with it. It therefore cannot see a WRONG predicate built in
// production code — and a mutation proved exactly that: widening this back
// to "any table in a graded namespace", which is the original bug, left the
// entire suite green.
//
// WHAT "WRONG" MEANS HERE, because the two directions are not symmetric.
// Over-covering is SILENT: a table nothing refuses over is also never
// warned about, and its writes break with nothing said, which is the whole
// defect this surface exists to prevent. Under-covering is merely noisy: an
// operator is warned about a table the refusal already handles. So every
// cell below that widens the predicate must fail.
func TestPublicationExposureCovered_IsExactMembership(t *testing.T) {
	graded := map[string]bool{
		"app.users":  true,
		"app.orders": true,
		"other.logs": true,
	}
	covered := publicationExposureCovered(graded)

	for _, tc := range []struct {
		name      string
		namespace string
		table     string
		want      bool
		why       string
	}{
		{"a graded table is covered", "app", "users", true, "the refusal was handed this one"},
		{"a second graded table in the same namespace", "app", "orders", true, ""},
		{"a graded table in another namespace", "other", "logs", true, ""},

		{
			// THE THIRD CELL, and the reason this surface was reshaped. The
			// refusing preflight applies the operator's table filter before
			// grading, so an --exclude-table'd table in a SELECTED namespace
			// is never handed to it — and must therefore be warned about.
			"an EXCLUDED table inside a graded namespace is NOT covered",
			"app", "audit_trail", false,
			"filtered out of the refusal, so nothing else grades it",
		},
		{
			// A leaf partition is the concrete case that defeated the
			// second cut: Filter.Allows-true, but the source's ReadSchema
			// does not surface it as a Table, so the refusal never saw it.
			"a relation the schema read did not surface is NOT covered",
			"app", "events_p2026_01", false,
			"never handed to the refusal, so the warning owns it",
		},
		{"an unselected namespace is not covered", "unrelated", "nokey", false, ""},
		{
			// Guards against a prefix or pattern match creeping in: these
			// two would both be "covered" under any namespace-level rule.
			"a same-prefixed namespace is not covered", "app_staging", "users", false,
			"exact membership, not a prefix",
		},
		{"an empty namespace is not covered", "", "users", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := covered(tc.namespace, tc.table); got != tc.want {
				t.Fatalf("covered(%q, %q) = %v, want %v — %s", tc.namespace, tc.table, got, tc.want, tc.why)
			}
		})
	}

	// Anti-vacuity: an empty graded set would satisfy every "not covered"
	// cell above by covering nothing at all.
	if !covered("app", "users") {
		t.Fatal("the graded set is not reaching the predicate; every negative cell above passes for the wrong reason")
	}

	// A nil set must cover NOTHING rather than everything. A caller that
	// forgot to collect the graded names should get a noisy over-warning,
	// never a silent under-warning.
	if publicationExposureCovered(nil)("app", "users") {
		t.Fatal("a nil graded set covered a table; an absent set must warn about everything, not nothing")
	}
}
