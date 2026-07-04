//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestExportSnapshot_EstimateRowCount_NeverAnalyzed pins the migrate
// chunk-decision contract on the shared-exported-snapshot primary
// reader (the TestRawCopy_ChunkedZeroLoss regression): a freshly-loaded
// NEVER-ANALYZEd table (pg_class.reltuples = the -1 sentinel) must
// resolve to the exact row count via COUNT(*) on the fresh off-snapshot
// estimator connection (ADR-0042 N1), NOT report 0 and silently route
// every such table to the single-reader path. The perf chunk that made
// the exporting transaction's pinned reader the migrate primary
// (perf-parity row 15) hit the ADR-0079 v1.1 "pinned readers decline
// the exact count" branch, which was designed for the sync cold-start's
// SnapshotImporter readers — the second half of this test pins that
// those importer readers STILL decline (return 0), so the v1.1 cost
// decision is unchanged.
func TestExportSnapshot_EstimateRowCount_NeverAnalyzed(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "estimate_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE est_events (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	const rows = 137
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO est_events SELECT g, 'v-' || g FROM generate_series(1, %d) g`, rows,
	)); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	// Deliberately NO ANALYZE: reltuples stays the never-analyzed
	// sentinel, which is the normal migrate cold-start shape (load a
	// source, migrate immediately, autovacuum hasn't run).

	eng := Engine{}
	snap, err := eng.ExportSnapshot(ctx, dsn)
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}
	defer func() { _ = snap.Close() }()

	table := &ir.Table{Name: "est_events"}

	est, ok := snap.Rows.(ir.RowCountEstimator)
	if !ok {
		t.Fatalf("exported snapshot Rows (%T) does not implement ir.RowCountEstimator", snap.Rows)
	}
	got, err := est.EstimateRowCount(ctx, table)
	if err != nil {
		t.Fatalf("EstimateRowCount on exported-snapshot primary: %v", err)
	}
	if got != rows {
		t.Errorf("EstimateRowCount on exported-snapshot primary = %d; want exact %d "+
			"(ADR-0042 N1: never-ANALYZEd must resolve via COUNT(*) on the fresh estimator conn, "+
			"or every fresh-loaded table silently loses within-table chunking)", got, rows)
	}

	// Contrast pin: a sync-import reader minted against the SAME
	// snapshot keeps the ADR-0079 v1.1 behaviour — decline the exact
	// count, report 0 (single-stream) on the never-analyzed sentinel.
	importer, err := eng.OpenSnapshotImporter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotImporter: %v", err)
	}
	defer func() {
		if c, ok := importer.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	readers, err := importer.ImportSnapshot(ctx, snap.Name, 1)
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}
	defer func() {
		if c, ok := readers[0].(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	impEst, ok := readers[0].(ir.RowCountEstimator)
	if !ok {
		t.Fatalf("imported reader (%T) does not implement ir.RowCountEstimator", readers[0])
	}
	impGot, err := impEst.EstimateRowCount(ctx, table)
	if err != nil {
		t.Fatalf("EstimateRowCount on imported reader: %v", err)
	}
	if impGot != 0 {
		t.Errorf("EstimateRowCount on sync-import reader = %d; want 0 "+
			"(ADR-0079 v1.1: importer readers decline the preflight COUNT(*) — unchanged)", impGot)
	}
}
