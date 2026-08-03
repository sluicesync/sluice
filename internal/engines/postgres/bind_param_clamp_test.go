// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Postgres bind-parameter clamp (audit 2026-08-01 Q3).
//
// A multi-row INSERT binds rowCount × columnCount parameters, and Postgres's
// extended protocol carries the parameter count as a signed 16-bit integer —
// 65535 is a wire limit, not a tunable. MySQL and SQLite both already bounded
// their batches against their own limits; Postgres was the one engine of the
// three that did not, so a wide table hard-failed on the batch write paths.
//
// The boundary is exact and worth pinning as a NUMBER rather than a property:
// at the default 500 rows per batch, 65535/500 = 131.07, so 131 columns fit
// (65,500 params) and 132 do not (66,000). That is the audit's "≥132 columns"
// claim, and it is the arithmetic a future change to defaultMaxRowsPerBatch
// would silently move.

package postgres

import (
	"strconv"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestClampRowsToBindLimit(t *testing.T) {
	cases := []struct {
		name string
		rows int
		cols int
		want int
	}{
		// The exact boundary at the shipped default batch size.
		{"131 columns fit at the default 500 rows", 500, 131, 500},
		{"132 columns do not — clamped", 500, 132, maxBindParamsPerStmt / 132},
		// Ordinary shapes are untouched: the clamp must not silently shrink
		// batches it has no reason to.
		{"narrow table untouched", 500, 10, 500},
		{"single column untouched", 500, 1, 500},
		{"exactly at the ceiling", 65535, 1, 65535},
		{"one over the ceiling", 65536, 1, 65535},
		// Degenerate inputs stay total.
		{"zero columns", 500, 0, 500},
		{"absurdly wide row still yields at least one", 500, 70000, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := clampRowsToBindLimit(tc.rows, tc.cols)
			if got != tc.want {
				t.Errorf("clampRowsToBindLimit(%d, %d) = %d, want %d", tc.rows, tc.cols, got, tc.want)
			}
			if got*tc.cols > maxBindParamsPerStmt && tc.cols <= maxBindParamsPerStmt {
				t.Errorf("clamped to %d rows × %d cols = %d bound parameters, over the %d ceiling — "+
					"the statement would be rejected by the server",
					got, tc.cols, got*tc.cols, maxBindParamsPerStmt)
			}
		})
	}
}

// TestBuildBatchInsert_StaysUnderTheBindCeiling is the end-to-end arithmetic:
// the statement the writer actually builds, for the clamped row count, must
// bind no more than the protocol allows. It walks column counts across the
// boundary rather than pinning one representative, because the failure is a
// product of two numbers and only shows up past their crossing point.
func TestBuildBatchInsert_StaysUnderTheBindCeiling(t *testing.T) {
	for _, cols := range []int{1, 10, 131, 132, 200, 1000, 1600} {
		table := &ir.Table{Schema: "public", Name: "wide"}
		for i := range cols {
			table.Columns = append(table.Columns, &ir.Column{
				Name: "c" + strconv.Itoa(i),
				Type: ir.Integer{Width: 64},
			})
		}
		rows := clampRowsToBindLimit(defaultMaxRowsPerBatch, len(nonGeneratedColumns(table.Columns)))
		if rows < 1 {
			t.Fatalf("cols=%d: clamped to %d rows — the writer would make no progress", cols, rows)
		}
		if params := rows * cols; params > maxBindParamsPerStmt {
			t.Errorf("cols=%d: %d rows × %d cols = %d bound parameters, over the %d ceiling",
				cols, rows, cols, params, maxBindParamsPerStmt)
		}
		// And the statement must still be built for that row count without
		// panicking or emitting a degenerate VALUES list.
		if q := buildBatchInsert("public", table, rows); q == "" {
			t.Errorf("cols=%d: buildBatchInsert returned an empty statement", cols)
		}
	}
}
