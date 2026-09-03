//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 262, the same-engine sibling: a PG→PG Shape A stream with
// --target-schema. The cold start CREATEs the table in the operator's
// target schema (the writer's bound namespace); the PG CDC snapshot
// carries the SOURCE's schema (`public`), and an unscrubbed post made
// the router address `"public"."widgets"` on the target — a relation
// that does not exist there. Every existing PG→PG Shape A pin ran with
// source and target both in `public`, which is exactly the configuration
// where the source-vs-bound namespace mismatch is invisible.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestStreamer_Bug262_ShapeA_PGToPG_TargetSchema_ForwardsDDLIntoTheBoundSchema(t *testing.T) {
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

	const targetSchema = "consolidated"
	streamer := &Streamer{
		Source:       pgEng,
		Target:       pgEng,
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		StreamID:     "test-bug262-pgpg-ts",
		TargetSchema: targetSchema,
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: "shard_a",
		},
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

	const qualified = targetSchema + ".widgets"
	if !waitForPGRowCount(t, targetDSN, qualified, 2, 60*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed the seed rows in %s", qualified)
	}

	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN price NUMERIC(10,2);
		INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);
	`)
	if !waitForPGRowCountOrStreamExit(t, targetDSN, qualified, 3, runErr, 60*time.Second) {
		t.Fatalf("phase B: the post-ADD-COLUMN row never landed in %s", qualified)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The column landed in the bound target schema, and nothing named
	// widgets was created in the source's schema on the target.
	assertPGColumnPresence(t, ctx, tgtDB, targetSchema, "widgets", "price", true)
	var publicWidgets int
	if err := tgtDB.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'widgets'`,
	).Scan(&publicWidgets); err != nil {
		t.Fatalf("tables probe: %v", err)
	}
	if publicWidgets != 0 {
		t.Errorf("public.widgets exists on the target; the DDL was routed by the source's schema, not the bound one")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
