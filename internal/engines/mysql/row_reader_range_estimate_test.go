// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pin for the Bug 256 fix: the MySQL RowReader implements
// [ir.RowCountEstimator] so the pre-copy plan/gotcha report and the ADR-0182
// query-timeout size gate — both of which type-assert that surface with NO
// RowCounter fallback — can see a MySQL source (including
// --source-driver=planetscale, which shares this reader). For MySQL the
// estimate IS the information_schema TABLE_ROWS count, so EstimateRowCount must
// return exactly what CountRows returns; the pinned/no-schema degrade posture
// is shared too.

package mysql

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// compile pin: the reader must satisfy the estimator surface the plan report +
// nudge consume. Its absence WAS Bug 256.
var _ ir.RowCountEstimator = (*RowReader)(nil)

func TestRowReader_EstimateRowCount(t *testing.T) {
	const table = "audio_plays_daily"
	rec := &fbRecorder{tableRows: map[string]int64{table: 524000}}
	db := newFBFakeDB(t, rec)
	tbl := &ir.Table{Name: table}
	ctx := context.Background()

	// Non-pinned reader with a schema: EstimateRowCount reads TABLE_ROWS and
	// returns it, byte-identical to CountRows (the delegation contract).
	r := &RowReader{q: db, schema: "testdb", closer: db}
	est, err := r.EstimateRowCount(ctx, tbl)
	if err != nil {
		t.Fatalf("EstimateRowCount: %v", err)
	}
	if est != 524000 {
		t.Fatalf("EstimateRowCount = %d; want 524000 (the canned TABLE_ROWS)", est)
	}
	cnt, err := r.CountRows(ctx, tbl)
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if est != cnt {
		t.Fatalf("EstimateRowCount (%d) != CountRows (%d); for MySQL they MUST be the same TABLE_ROWS read", est, cnt)
	}

	// Snapshot-pinned reader (closer == nil): a concurrent catalog query would
	// conflict with the in-flight stream on the pinned conn, so both surfaces
	// degrade to (0, nil) — "no estimate → single-stream", never an error.
	pinned := &RowReader{q: db, schema: "testdb", closer: nil}
	if n, err := pinned.EstimateRowCount(ctx, tbl); err != nil || n != 0 {
		t.Fatalf("EstimateRowCount(pinned) = (%d, %v); want (0, nil)", n, err)
	}

	// No schema name: information_schema can't disambiguate same-named tables
	// across databases, so it reports an honest "no estimate" rather than a
	// wrong-table count.
	noSchema := &RowReader{q: db, schema: "", closer: db}
	if n, err := noSchema.EstimateRowCount(ctx, tbl); err != nil || n != 0 {
		t.Fatalf("EstimateRowCount(no-schema) = (%d, %v); want (0, nil)", n, err)
	}

	// A nil table is a caller bug, surfaced loudly (matches CountRows).
	if _, err := r.EstimateRowCount(ctx, nil); err == nil {
		t.Fatal("EstimateRowCount(nil table): want an error, got nil")
	}
}
