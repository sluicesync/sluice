//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7a) — default-on schema-change forwarding for the single-
// stream CDC path. DROP COLUMN end-to-end on PG → PG live CDC.
//
// THE PRIME-THEN-MUTATE PATTERN (seed-guard, ADR-0091 §5b):
//
// A DROP COLUMN is a DESTRUCTIVE shape. The seed-guard NEVER forwards a
// destructive shape when it is classified against the cold-start SEED
// (the first post-cold-start boundary), because the seed's IR fidelity
// differs from the CDC projection's — a residual fidelity gap could
// otherwise forward a phantom destructive DDL and silently corrupt the
// target. So a test that cold-starts and then immediately DROPs would
// see the DROP SKIPPED (correct, not a bug).
//
// To exercise a forwarded DROP we first establish a CDC→CDC boundary:
//
//  1. Cold-start the table (carrying the column we will drop).
//  2. PRIME: ADD a throwaway column. ADD is additive (not seed-guarded),
//     so it forwards AND flips the table's cache entry from
//     seed-sourced to CDC-sourced. We poll until the prime column lands
//     on the target so we KNOW the boundary was processed.
//  3. DROP the real column. This is now a genuine CDC→CDC boundary and
//     forwards.
//  4. Post-DROP INSERT lands AND the dropped column is gone on target.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_SchemaForward_DropColumn_PG pins the forwarded DROP COLUMN
// shape on PG → PG via the prime-then-mutate pattern.
func TestStreamer_SchemaForward_DropColumn_PG(t *testing.T) {
	// ADR-0091 F7a GAP #1 (fixed): the PG CDC reader's checkSchemaRace now
	// learns --schema-changes=forward via SetSchemaForward and lets DROP
	// COLUMN pass through to the forward intercept as a SchemaSnapshot
	// (cdc_relations.go). The GAP #3 applier-cache invalidation keeps the
	// post-DROP decode correct (no Bug 119 drift).
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			doomed TEXT
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, doomed) VALUES (1, 'alpha', 'x'), (2, 'beta', 'y');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Default config — forwarding is on by default (ADR-0091). No flags.
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-drop-pg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// PRIME: an ADD COLUMN to flip the cache entry seed→CDC. ADD is not
	// seed-guarded, so it forwards. The follow-on INSERT is REQUIRED: a
	// PG pure-DDL emits no logical-replication message by itself; the
	// schema change is only surfaced via the Relation that precedes the
	// next DML on the table, so the prime row is what pushes the boundary
	// through. Wait until the prime column lands so the seed→CDC boundary
	// is provably processed before the DROP.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN _prime_col INT;
		INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);
	`)
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	// Now the real DROP on a genuine CDC→CDC boundary, plus a post-DROP
	// INSERT to prove the column is gone and DML still flows.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets DROP COLUMN doomed;
		INSERT INTO widgets (id, name) VALUES (3, 'gamma');
	`)

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-DROP row never landed — DROP forwarding broken")
	}

	if !waitForPGColumn(t, tgtDB, "widgets", "doomed", false, 60*time.Second) {
		t.Errorf("target widgets.doomed still present — DROP did not forward")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
