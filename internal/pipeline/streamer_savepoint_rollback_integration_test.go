//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for SAVEPOINT / ROLLBACK TO SAVEPOINT within a
// captured transaction (broader-mining gap #6 —
// docs/dev/notes/test-gap-mining-broader.md).
//
// PG's pgoutput correctly suppresses rolled-back sub-transaction
// changes from the decoded change stream when the SUBSCRIPTION /
// pg_create_logical_replication_slot's decoding mode is configured
// in the standard way (which is sluice's default). The applier
// therefore should only see writes that commit. But the loud-failure
// tenet treats "rolled-back writes applied at target" as the
// worst-class silent-loss bug, so this pin asserts it can't happen
// even if a future change to the reader's decoded-state machine
// introduced a regression.
//
// Scenario:
//
//	BEGIN;
//	  INSERT INTO t (id, status) VALUES (1, 'pending');
//	  SAVEPOINT sp1;
//	    UPDATE t SET status = 'CORRUPTED' WHERE id = 1;
//	  ROLLBACK TO SAVEPOINT sp1;
//	  -- if the ROLLBACK TO is honoured, the row is now 'pending' again
//	  UPDATE t SET status = 'committed' WHERE id = 1;
//	COMMIT;
//
// After the transaction commits, the target's row must have
// status='committed'. NOT 'CORRUPTED' (the rolled-back write must not
// be applied) and not 'pending' (the post-savepoint write must be
// applied). A target landing as 'CORRUPTED' is the silent-loss class.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_PostgresToPostgres_SavepointRollbackTo_RolledBackWritesSuppressed
// is the gap #6 regression pin: pgoutput must not deliver writes
// that were rolled back by a `ROLLBACK TO SAVEPOINT`, and sluice's
// applier must respect that contract end-to-end.
func TestStreamer_PostgresToPostgres_SavepointRollbackTo_RolledBackWritesSuppressed(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE orders (
			id     BIGINT PRIMARY KEY,
			status VARCHAR(32) NOT NULL
		);
		ALTER TABLE orders REPLICA IDENTITY FULL;

		INSERT INTO orders (id, status) VALUES (100, 'pre-stream-control');
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "gap6-savepoint-rollback"
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	t.Cleanup(func() {
		streamCancel()
		select {
		case err := <-runErr:
			if err != nil && err != context.Canceled {
				t.Logf("streamer exit err: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Log("streamer did not exit within 15s after cancel")
		}
	})

	// Wait for the bulk-copy seed row.
	if !waitForRowCount(t, targetDSN, "orders", 1, 60*time.Second) {
		t.Fatalf("bulk copy did not deliver the pre-stream seed row within 60s")
	}

	// Now execute the SAVEPOINT/ROLLBACK TO scenario in a single
	// transaction on the source. Use a dedicated connection so the
	// statements are NOT split across implicit txns — they MUST land
	// in one source transaction so pgoutput emits a single BEGIN/COMMIT
	// pair around the {INSERT, savepoint scope, UPDATE} chain.
	source, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("sql.Open source: %v", err)
	}
	defer func() { _ = source.Close() }()

	tx, err := source.BeginTx(streamCtx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(streamCtx, `INSERT INTO orders (id, status) VALUES (1, 'pending')`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := tx.ExecContext(streamCtx, `SAVEPOINT sp1`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SAVEPOINT sp1: %v", err)
	}
	if _, err := tx.ExecContext(streamCtx, `UPDATE orders SET status = 'CORRUPTED' WHERE id = 1`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("UPDATE inside savepoint: %v", err)
	}
	if _, err := tx.ExecContext(streamCtx, `ROLLBACK TO SAVEPOINT sp1`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("ROLLBACK TO SAVEPOINT: %v", err)
	}
	if _, err := tx.ExecContext(streamCtx, `UPDATE orders SET status = 'committed' WHERE id = 1`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("UPDATE post-savepoint: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Wait for CDC to land the COMMIT-ed state on the target.
	target, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open target: %v", err)
	}
	defer func() { _ = target.Close() }()

	// Poll for row id=1 to appear with the committed status. Either:
	//   - target gets status='committed' → the test passes (pgoutput
	//     suppressed the rolled-back UPDATE).
	//   - target gets status='CORRUPTED' → SILENT-LOSS CLASS. Fail
	//     loudly with the corrupt value named.
	//   - target row id=1 never appears within 30s → also a failure,
	//     surface for triage (might mean the streamer halted, in
	//     which case the next assertion catches it).
	deadline := time.Now().Add(30 * time.Second)
	var gotStatus string
	var found bool
	for time.Now().Before(deadline) {
		err := target.QueryRowContext(
			streamCtx,
			`SELECT status FROM orders WHERE id = 1`,
		).Scan(&gotStatus)
		if err == nil {
			found = true
			break
		}
		if err != sql.ErrNoRows {
			// Real DB error — surface immediately.
			t.Fatalf("read target orders id=1: %v", err)
		}
		// sql.ErrNoRows — keep polling.
		time.Sleep(200 * time.Millisecond)
	}

	if !found {
		// Surface streamer state for triage. A halted streamer would
		// have written to runErr.
		select {
		case err := <-runErr:
			t.Fatalf("target row id=1 never appeared within 30s and the streamer exited with err=%v", err)
		default:
		}
		t.Fatalf("target row id=1 never appeared within 30s; streamer still running but no apply landed " +
			"(possible silent-skip-of-savepoint-COMMIT — investigate)")
	}

	switch gotStatus {
	case "committed":
		// Correctness baseline — pgoutput suppressed the rolled-back
		// UPDATE, applier saw only the post-savepoint UPDATE, target
		// landed clean. The forward regression guard is in place.
		t.Logf("SAVEPOINT/ROLLBACK TO behaviour correct: target landed with status=%q (the post-savepoint COMMIT-ed value)",
			gotStatus)
	case "CORRUPTED":
		t.Fatalf("SILENT-LOSS CLASS: target row id=1 landed with status='CORRUPTED' — " +
			"the rolled-back write inside SAVEPOINT sp1 was applied at the target. " +
			"This is the worst-class silent-loss bug per the loud-failure tenet (rolled-back writes " +
			"must not be applied).")
	case "pending":
		t.Errorf("target row id=1 landed with status='pending' — the post-savepoint UPDATE " +
			"that set status='committed' was NOT applied. Possibly the streamer treated the " +
			"ROLLBACK TO as a full transaction rollback (it isn't — the outer COMMIT must still " +
			"land the post-savepoint writes).")
	default:
		t.Errorf("target row id=1 landed with unexpected status=%q (want 'committed')", gotStatus)
	}
}
