// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDiffRemediation_CarriesNotValid pins the fix for the UPR-1c gap the
// pre-tag value-fidelity review found (2026-09-05): the diff lane learned to
// REPORT a validity divergence in that same change, and kept emitting
// remediation DDL that re-validates.
//
// Why that is a defect and not a cosmetic one: `sluice diff` prints SQL the
// operator is expected to RUN. Suggesting a validating constraint where the
// source carries an unvalidated one fails in both directions —
//
//   - the target holds rows the source tolerates → the ADD CONSTRAINT dies
//     mid-remediation with a constraint violation, from the tool's own
//     output;
//   - the target happens to comply → it now carries a constraint STRICTER
//     than the source, which kills the CDC apply on the first replicated row
//     the source accepts.
//
// All three renderers are graded, because they are siblings and the review
// found the gap in all three at once: the missing-CHECK suggestion, the
// mismatched-CHECK drop+add, and the FK clause (which already carried MATCH
// FULL, ON DELETE/UPDATE and DEFERRABLE, and whose own doc claimed it carried
// every load-bearing attribute).
func TestDiffRemediation_CarriesNotValid(t *testing.T) {
	quote := func(s string) string { return `"` + s + `"` }

	t.Run("a missing NOT VALID CHECK is suggested NOT VALID", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{{
			Name: "orders",
			CheckConstraints: []*ir.CheckConstraint{
				{Name: "orders_total_chk", Expr: "total >= 0", NotValid: true},
				{Name: "orders_qty_chk", Expr: "qty > 0"},
			},
		}}}

		var sb strings.Builder
		renderMissingCheck(&sb, "orders", "orders_total_chk", quote, expected)
		got := sb.String()
		if !strings.Contains(got, "CHECK (total >= 0) NOT VALID;") {
			t.Errorf("unvalidated source CHECK suggested back as validating — running this output "+
				"against a target holding a legacy row the source tolerates fails mid-remediation.\ngot: %s", got)
		}

		// The control, without which the assertion above would pass against a
		// renderer that appended NOT VALID unconditionally.
		sb.Reset()
		renderMissingCheck(&sb, "orders", "orders_qty_chk", quote, expected)
		if got := sb.String(); strings.Contains(got, "NOT VALID") {
			t.Errorf("a VALIDATED source CHECK was suggested NOT VALID — the suggestion is now weaker "+
				"than the source, which is the same defect mirrored.\ngot: %s", got)
		}
	})

	t.Run("the FK clause carries NOT VALID last, after DEFERRABLE", func(t *testing.T) {
		fk := &ir.ForeignKey{
			Name:              "orders_customer_fk",
			Columns:           []string{"customer_id"},
			ReferencedTable:   "customers",
			ReferencedColumns: []string{"id"},
			OnDelete:          ir.FKActionCascade,
			Deferrable:        true,
			InitiallyDeferred: true,
			NotValid:          true,
		}
		got := renderFKClause(fk, quote)
		if !strings.HasSuffix(got, " NOT VALID") {
			t.Errorf("FK suggestion does not end in NOT VALID; Postgres accepts it only in final "+
				"position, so ordering is part of the assertion.\ngot: %s", got)
		}
		// Position, not just presence: DEFERRABLE must precede it.
		defIdx, nvIdx := strings.Index(got, "DEFERRABLE"), strings.Index(got, "NOT VALID")
		if defIdx < 0 || nvIdx < defIdx {
			t.Errorf("NOT VALID must follow the DEFERRABLE clause; got: %s", got)
		}

		fk.NotValid = false
		if got := renderFKClause(fk, quote); strings.Contains(got, "NOT VALID") {
			t.Errorf("a validated FK was suggested NOT VALID: %s", got)
		}
	})

	t.Run("the suffix helper is the one decision point", func(t *testing.T) {
		if checkNotValidSuffix(true) != " NOT VALID" || checkNotValidSuffix(false) != "" {
			t.Fatal("checkNotValidSuffix does not render the two states it exists to render")
		}
	})
}
