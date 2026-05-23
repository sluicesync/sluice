//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 v0.78.0 task #22 — end-to-end RENAME COLUMN pin (PG → PG).
//
// Mirrors the Bug 83 PG end-to-end pattern (cold-start → CDC → in-
// flight DDL → assert target). Drives a live PG → PG streamer with
// live coordination engaged; the source issues an
// `ALTER TABLE ... RENAME COLUMN ... TO ...`; assertions verify the
// target schema reflects the rename, the data under the renamed
// column is preserved, and the lease row's applied state is
// recorded.
//
// "Validate end-to-end before building more" — task #22 closes one
// of the three sub-shapes ADR-0054's v1 catalog explicitly named as
// v1-deferred (the other two — CHECK constraint changes, generated-
// column changes — stay deferred). The pin is the real-engine wire-
// up the v1 catalog never had for RENAME.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_RenameColumn_PG_LiveCoordination drives a PG → PG
// streamer through cold-start, a live RENAME COLUMN on the source,
// and a follow-up INSERT under the new column name. Asserts the
// target schema landed the rename, the row data flowed under the
// renamed column, and the lease table recorded the applied state.
func TestStreamer_RenameColumn_PG_LiveCoordination(t *testing.T) {
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

	const streamID = "test-rename-pg"
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

	// Wait for bulk-copy.
	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	// Source RENAME + INSERT under the new column name.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets RENAME COLUMN name TO product_name;
		INSERT INTO widgets (id, product_name) VALUES (3, 'gamma');
	`)

	// Wait for the post-DDL row to land. Without RENAME-shape
	// recognition the boundary would refuse loudly (combo
	// added=1+dropped=1 → Unrecognized), the apply would never
	// fire, and the gamma INSERT would crash the applier with
	// `column "product_name" does not exist`.
	if !waitForPGRowCount(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-RENAME row never landed — RENAME shape may not be classified " +
			"as ShapeKindRenameColumn, or AlterRenameColumn failed to apply")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Target schema reflects the rename: product_name present, name absent.
	var hasNew, hasOld int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'product_name'`).Scan(&hasNew); err != nil {
		t.Fatalf("check product_name: %v", err)
	}
	if hasNew != 1 {
		t.Error("target widgets.product_name column missing — RENAME apply didn't fire")
	}
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'name'`).Scan(&hasOld); err != nil {
		t.Fatalf("check name: %v", err)
	}
	if hasOld != 0 {
		t.Error("target widgets.name column still present — RENAME left both columns")
	}

	// Original rows preserved under renamed column.
	var alpha, beta string
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 1").Scan(&alpha); err != nil {
		t.Fatalf("read product_name @ id=1: %v", err)
	}
	if alpha != "alpha" {
		t.Errorf("widgets.product_name @ id=1 = %q, want alpha (data should survive rename)", alpha)
	}
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 2").Scan(&beta); err != nil {
		t.Fatalf("read product_name @ id=2: %v", err)
	}
	if beta != "beta" {
		t.Errorf("widgets.product_name @ id=2 = %q, want beta", beta)
	}

	// Gamma row landed via the post-RENAME INSERT.
	var gamma string
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 3").Scan(&gamma); err != nil {
		t.Fatalf("read product_name @ id=3: %v", err)
	}
	if gamma != "gamma" {
		t.Errorf("widgets.product_name @ id=3 = %q, want gamma", gamma)
	}

	// Lease row reflects applied state.
	const leaseQ = `SELECT applied_at IS NOT NULL, applied_schema_version
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`
	var (
		applied bool
		version int64
	)
	if err := tgtDB.QueryRowContext(ctx, leaseQ, "public.widgets").Scan(&applied, &version); err != nil {
		t.Fatalf("scan lease row: %v (a missing row means the RENAME boundary never routed)", err)
	}
	if !applied {
		t.Error("lease.applied_at should be set after the routed RENAME boundary")
	}
	if version < 1 {
		t.Errorf("lease.applied_schema_version = %d; want >= 1", version)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
