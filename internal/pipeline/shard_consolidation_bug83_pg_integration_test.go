//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 Bug 83 end-to-end pin — PG → PG.
//
// Reproduces v0.73.0's "live coordination is non-functional" path: cold
// start completes, then source DDL lands BEFORE the first CDC row event,
// and the next CDC row crashes the applier with `column "<new>" does
// not exist`. Pre-fix (v0.73.0) the intercept's table cache starts
// empty and treats the first CDC SchemaSnapshot as the cold-start
// anchor — the post-DDL schema becomes the cached "pre", no boundary
// routes, and the applier sees Insert rows referencing a column that
// doesn't exist on the target.
//
// Post-fix (v0.73.1) the intercept is pre-seeded with the pre-Shape-A-
// rewrite source IR at cold-start completion, so the first CDC
// SchemaSnapshot is classified as a true boundary and the lease/router
// path applies the DDL exactly once.
//
// "Validate end-to-end before building more" — this is the cross-engine
// pin the v0.73.0 unit-only intercept tests were missing.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_Bug83_PG_LiveCoordination_AddColumn pins the canonical
// Bug 83 repro on PG: streamer runs cold-start → CDC; source ALTER
// adds a column; subsequent INSERT on the source replicates without
// the applier crashing, and the lease table's row reflects an applied
// boundary.
func TestStreamer_Bug83_PG_LiveCoordination_AddColumn(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "test-bug83-pg"
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: "shard_a",
		},
		// Engage live coordination — this is what the CLI default
		// (--no-coordinate-live-ddl absent) produces.
		CoordinateLiveDDL: true,
		ShardCoordinationLease: LeaseConfig{
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 20 * time.Second,
			RetryPeriod:   5 * time.Second,
		},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for bulk-copy to land the 2 seed rows on the target.
	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows (cold-start path stalled before CDC)")
	}

	// Apply source DDL + INSERT BEFORE any other CDC events flow. This
	// is the precise Bug 83 timing: source schema changes between cold-
	// start completion and the first row event the CDC reader has to
	// surface as a SchemaSnapshot.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN price NUMERIC(10,2);
		INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);
	`)

	// Wait for the post-DDL row to land. Pre-fix this never happens —
	// the applier crashes with "column price does not exist" on the
	// Insert dispatch and the row stays at 2.
	if !waitForPGRowCount(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-DDL row never landed — Bug 83 regression " +
			"(intercept treated the first CDC SchemaSnapshot as the cold-start anchor " +
			"instead of as a real boundary; lease never recorded, applier crashed on " +
			"the INSERT referencing the new column)")
	}

	// Assert the target schema reflects the added column.
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var hasPrice int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'price'
	`).Scan(&hasPrice); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if hasPrice != 1 {
		t.Errorf("target widgets.price column missing — boundary apply didn't fire")
	}

	// Assert the lease row reflects the applied state. The Bug 83 root
	// cause was that this row stayed empty (intercept never invoked
	// RouteBoundary); post-fix it has the recorded version + checksum.
	const leaseQ = `SELECT applied_at IS NOT NULL, applied_schema_version
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`
	var (
		applied bool
		version int64
	)
	if err := tgtDB.QueryRowContext(ctx, leaseQ, "public.widgets").Scan(&applied, &version); err != nil {
		t.Fatalf("scan lease row: %v (a missing row is the load-bearing Bug 83 symptom — "+
			"the intercept never routed the boundary, so the lease table stayed empty)", err)
	}
	if !applied {
		t.Error("lease.applied_at should be set after the routed boundary")
	}
	if version < 1 {
		t.Errorf("lease.applied_schema_version = %d; want >= 1", version)
	}

	// Confirm the gamma row landed with the price.
	var (
		gotName  string
		gotPrice sql.NullString
	)
	if err := tgtDB.QueryRowContext(ctx,
		"SELECT name, price::text FROM widgets WHERE id = 3").Scan(&gotName, &gotPrice); err != nil {
		t.Fatalf("scan gamma: %v", err)
	}
	if gotName != "gamma" {
		t.Errorf("widgets.name = %q; want gamma", gotName)
	}
	if !gotPrice.Valid || gotPrice.String != "3.75" {
		t.Errorf("widgets.price = %v; want 3.75", gotPrice)
	}

	// Clean shutdown.
	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

func waitForPGRowCount(t *testing.T, dsn, table string, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollPGRowCount(dsn, table) >= n {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func pollPGRowCount(dsn, table string) int {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		return 0
	}
	return n
}
