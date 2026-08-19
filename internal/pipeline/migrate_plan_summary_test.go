// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Consumer-contract pins for the pre-copy plan/gotcha report's largest-table
// lookup ([largestEstimatedTable]) and the ADR-0182 query-timeout size gate
// ([largestEstimatedRows]). Both type-assert ONLY [ir.RowCountEstimator] and
// deliberately do NOT fall back to [ir.RowCounter] — a plain COUNT(*) pre-copy
// is exactly the full scan the estimator surface exists to avoid on the engines
// (PG) where the count is exact.
//
// The load-bearing consequence, and Bug 256's root: a source that implements a
// cheap RowCounter but NOT the estimator is INVISIBLE to both consumers. MySQL
// was exactly that shape (its CountRows already reads the approximate
// TABLE_ROWS), so a PlanetScale→PlanetScale region move — the case v0.129.0's
// report targets — named no largest table and the nudge never fired. The fix
// made the MySQL reader implement RowCountEstimator; these pins hold the
// contract that made the omission matter, so a regression re-surfaces here.

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// perTableEstimator implements ir.RowCountEstimator with per-table counts — the
// shape every source RowReader MUST have to appear in the plan report / nudge.
type perTableEstimator struct {
	recordingRowReader
	counts map[string]int64
}

func (r *perTableEstimator) EstimateRowCount(_ context.Context, t *ir.Table) (int64, error) {
	return r.counts[t.Name], nil
}

// counterOnlyReader implements ir.RowCounter but NOT ir.RowCountEstimator — the
// exact pre-fix MySQL shape. Present to prove the consumers are blind to it.
type counterOnlyReader struct {
	recordingRowReader
	counts map[string]int64
}

func (r *counterOnlyReader) CountRows(_ context.Context, t *ir.Table) (int64, error) {
	return r.counts[t.Name], nil
}

func planSummarySchema() *ir.Schema {
	tbl := func(name string) *ir.Table {
		return &ir.Table{Name: name, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	}
	return &ir.Schema{Tables: []*ir.Table{tbl("small"), tbl("big"), tbl("mid")}}
}

// TestLargestEstimatedTable_PicksLargestViaEstimator pins that both consumers
// select the largest table by the estimator's per-table counts.
func TestLargestEstimatedTable_PicksLargestViaEstimator(t *testing.T) {
	ctx := context.Background()
	schema := planSummarySchema()
	rr := &perTableEstimator{counts: map[string]int64{"small": 10, "big": 524000, "mid": 4200}}

	name, rows, known := largestEstimatedTable(ctx, rr, schema)
	if !known || name != "big" || rows != 524000 {
		t.Fatalf("largestEstimatedTable = (%q, %d, %v); want (\"big\", 524000, true)", name, rows, known)
	}

	// The nudge gate reads the same surface and must agree on the max.
	largest, ok := largestEstimatedRows(ctx, rr, schema)
	if !ok || largest != 524000 {
		t.Fatalf("largestEstimatedRows = (%d, %v); want (524000, true)", largest, ok)
	}
}

// TestLargestEstimatedTable_BlindToCounterOnlyReader documents Bug 256's root:
// a RowCounter-only source (no estimator) is invisible to BOTH consumers. This
// is the contract the fix depends on — it is WHY every source engine's reader
// must implement ir.RowCountEstimator, not merely ir.RowCounter. If a future
// change makes the consumers fall back to CountRows, this pin flips and forces
// a deliberate re-decision (a pre-copy COUNT(*) on every table is the cost that
// fallback would silently add).
func TestLargestEstimatedTable_BlindToCounterOnlyReader(t *testing.T) {
	ctx := context.Background()
	schema := planSummarySchema()
	rr := &counterOnlyReader{counts: map[string]int64{"small": 10, "big": 524000, "mid": 4200}}

	if _, ok := any(rr).(ir.RowCountEstimator); ok {
		t.Fatal("counterOnlyReader must NOT implement ir.RowCountEstimator (test setup invariant)")
	}
	if _, ok := any(rr).(ir.RowCounter); !ok {
		t.Fatal("counterOnlyReader must implement ir.RowCounter (test setup invariant)")
	}

	if name, rows, known := largestEstimatedTable(ctx, rr, schema); known || name != "" || rows != 0 {
		t.Fatalf("largestEstimatedTable(counter-only) = (%q, %d, %v); want (\"\", 0, false) — the report is blind to a RowCounter-only source", name, rows, known)
	}
	if largest, ok := largestEstimatedRows(ctx, rr, schema); ok || largest != 0 {
		t.Fatalf("largestEstimatedRows(counter-only) = (%d, %v); want (0, false)", largest, ok)
	}
}
