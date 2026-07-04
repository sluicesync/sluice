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
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestOpenBackupSnapshot_EstimateRowCount_NeverAnalyzed is the backup
// counterpart of TestExportSnapshot_EstimateRowCount_NeverAnalyzed
// (the 59c55e27 trap, ADR-0149): the within-table chunk DECISION for a
// backup runs EstimateRowCount on the snapshot's pinned primary reader
// and on importer-minted range readers. On a freshly-loaded,
// NEVER-ANALYZEd source (pg_class.reltuples = the -1 sentinel) both
// must resolve to the exact row count via COUNT(*) on the fresh
// off-snapshot estimator connection — a 0 here silently routes every
// large table to the single-stream sweep, exactly the class the
// migrate shared-snapshot fix closed. The importer-minted leg needs
// the explicit [ir.ExactCountEstimateOptIn] the backup factory
// applies; without the opt-in the same reader keeps the ADR-0079 v1.1
// sync-import decline (pinned by the exporter test, unchanged).
func TestOpenBackupSnapshot_EstimateRowCount_NeverAnalyzed(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "backup_estimate_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE bk_est_events (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	const rows = 149
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO bk_est_events SELECT g, 'v-' || g FROM generate_series(1, %d) g`, rows,
	)); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	// Deliberately NO ANALYZE: reltuples stays the never-analyzed
	// sentinel — the normal "load a source, back it up immediately"
	// shape (autovacuum hasn't run).

	eng := Engine{}
	snap, err := eng.OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}
	defer func() { _ = snap.Close() }()

	table := &ir.Table{Name: "bk_est_events"}

	est, ok := snap.Rows.(ir.RowCountEstimator)
	if !ok {
		t.Fatalf("backup snapshot Rows (%T) does not implement ir.RowCountEstimator", snap.Rows)
	}
	got, err := est.EstimateRowCount(ctx, table)
	if err != nil {
		t.Fatalf("EstimateRowCount on backup-snapshot primary: %v", err)
	}
	if got != rows {
		t.Errorf("EstimateRowCount on backup-snapshot primary = %d; want exact %d "+
			"(ADR-0149: never-ANALYZEd must resolve via COUNT(*) on the fresh estimator conn, "+
			"or every fresh-loaded table silently loses within-table backup chunking)", got, rows)
	}

	// Importer-minted range-reader leg: the reader declines the exact
	// count by default (ADR-0079 v1.1) and resolves it only after the
	// [ir.ExactCountEstimateOptIn] the backup factory applies.
	importer, err := eng.OpenSnapshotImporter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotImporter: %v", err)
	}
	defer func() {
		if c, ok := importer.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	readers, err := importer.ImportSnapshot(ctx, snap.SnapshotName, 1)
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}
	defer func() {
		if c, ok := readers[0].(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	optIn, ok := readers[0].(ir.ExactCountEstimateOptIn)
	if !ok {
		t.Fatalf("imported reader (%T) does not implement ir.ExactCountEstimateOptIn", readers[0])
	}
	optIn.EnableExactCountEstimate()
	impEst := readers[0].(ir.RowCountEstimator)
	impGot, err := impEst.EstimateRowCount(ctx, table)
	if err != nil {
		t.Fatalf("EstimateRowCount on opted-in imported reader: %v", err)
	}
	if impGot != rows {
		t.Errorf("EstimateRowCount on opted-in imported reader = %d; want exact %d "+
			"(the backup factory's exact-count opt-in must reach importer-minted range readers)", impGot, rows)
	}
}
