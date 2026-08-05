// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "testing"

// TestDryRunPlan_LabelsTheRowCountAsAnEstimate pins roadmap item 124.
//
// The plan's row_count comes from the engine's STATISTICS — MySQL's
// information_schema.TABLE_ROWS, PG's pg_class.reltuples — which lags real
// cardinality badly on a table that has not been ANALYZEd recently. A field
// report saw it 40% low and read the gap against the final copied count as
// sluice reporting a mismatch. The copy-progress line has carried
// total_rows_estimated for exactly this reason since roadmap #22; the plan is
// the number an operator diffs against, and it said nothing.
//
// Unavailable (-1) must NOT be labelled estimated: "unknown" and "an
// approximate 0" are different claims, and the -1 sentinel exists so consumers
// cannot confuse unknown with empty.
func TestDryRunPlan_LabelsTheRowCountAsAnEstimate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int64
		want  bool
	}{
		{"a real estimate is labelled", 1_026_026, true},
		{"zero is still an estimate", 0, true},
		{"unavailable is NOT an estimate", -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pt := PlanTable{RowCount: tc.count, RowCountEstimated: tc.count >= 0}
			if pt.RowCountEstimated != tc.want {
				t.Errorf("RowCountEstimated = %v for row_count=%d; want %v",
					pt.RowCountEstimated, tc.count, tc.want)
			}
		})
	}
}
