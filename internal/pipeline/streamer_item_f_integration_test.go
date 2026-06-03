//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Item F (slot-missing fall-through to cold-start,
// ADR-0022).
//
// Pre-fix shape: when an operator dropped a PG replication slot
// (typically after sluice surfaced wal_status='lost'), the next
// `sluice sync start` errored out with "replication slot ... no
// longer exists; cannot resume from supplied LSN". The persisted
// position was unrecoverable and there was no flag to clear it —
// operators had to DELETE the sluice_cdc_state row by hand and then
// re-run.
//
// Post-fix: the CDC reader's slot-missing branch wraps its error
// with [ir.ErrPositionInvalid]; the streamer detects this via
// errors.Is, logs a WARN, and falls through to coldStart with the
// same lsnTracker. Bug 9's pre-flight refusal still gates populated
// dest — this test drops dest tables before the fall-through to
// exercise the recovery path itself.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_PostgresToPostgres_SlotMissingFallsThroughToColdStart
// exercises the Option B recovery path:
//
//  1. Cold-start sluice on PG → PG; let bulk-copy + a CDC change land
//     so `sluice_cdc_state` has a real persisted position.
//  2. Cancel the streamer.
//  3. Drop the slot directly via pg_drop_replication_slot (simulates
//     the operator's `sluice slot drop` after wal_status='lost').
//  4. Drop dest tables (Bug 9 pre-flight gate; the test exercises
//     fall-through, not the populated-dest refusal).
//  5. Re-run `sync start`.
//  6. Assert: cold-start runs (bulk-copied seed rows reappear),
//     a new slot is created, the persisted position is overwritten
//     with a fresh value.
//
// Pre-fix, step 5 errors out at "replication slot ... no longer
// exists." Post-fix, the WARN fires and cold-start completes.
func TestStreamer_PostgresToPostgres_SlotMissingFallsThroughToColdStart(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE itemf (
			id      BIGSERIAL PRIMARY KEY,
			payload TEXT NOT NULL
		);
		ALTER TABLE itemf REPLICA IDENTITY FULL;
		INSERT INTO itemf (payload)
			SELECT 'seed-' || g FROM generate_series(1, 25) g;
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "itemf-slot-missing"

	// ---- Phase 1: cold-start, drive a CDC change so the persisted
	// position is non-empty. ----
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

	// Wait for bulk-copy to deliver the seed rows.
	if !waitForRowCount(t, targetDSN, "itemf", 25, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("bulk copy did not deliver seed rows")
	}

	// Drive a CDC change so the persisted position advances past
	// cold-start's initial write.
	applyDDL(t, sourceDSN, "INSERT INTO itemf (payload) VALUES ('cdc-1')")
	if !waitForRowCount(t, targetDSN, "itemf", 26, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC did not advance after first writer event")
	}

	// Read back the persisted position so we can assert it gets
	// overwritten by cold-start in phase 4.
	persistedBefore := readPersistedPosition(t, targetDSN, streamID)
	if persistedBefore == "" {
		streamCancel()
		<-runErr
		t.Fatal("persisted position is empty after CDC change")
	}

	// ---- Phase 2: cancel the streamer + give the pump a moment to
	// release the replication connection. Same shape as the bug 15
	// test — without the wait, the next slot-drop attempt races with
	// the pump's release. ----
	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Streamer.Run returned err on cancel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}
	time.Sleep(2 * time.Second)

	// ---- Phase 3: drop the slot directly. Simulates the operator's
	// `sluice slot drop` after wal_status='lost'. ----
	dropSlotDirect(t, sourceDSN, "sluice_slot")

	// ---- Phase 4: drop dest tables (Bug 9 pre-flight gate). The
	// test exercises the position fall-through; the dest-data step
	// is orthogonal and operators handle it via --force-cold-start
	// or manual drops. ----
	applyDDL(t, targetDSN, "DROP TABLE itemf")

	// ---- Phase 5: re-run sync start; the persisted position
	// references the dropped slot, so the streamer should detect
	// ir.ErrPositionInvalid, log the WARN, and fall through to
	// coldStart. ----
	resumeStreamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- resumeStreamer.Run(resumeCtx) }()

	// ---- Phase 6: assert cold-start ran. The dropped table is
	// recreated and seeded with all 26 rows from the source. ----
	if !waitForRowCount(t, targetDSN, "itemf", 26, 60*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("after slot-missing fall-through, dst was not re-seeded by cold-start (got %d rows)",
			pollRowCount(targetDSN, "itemf"))
	}

	// Drive one more CDC change so the resumed streamer writes a
	// fresh position over the stale one.
	applyDDL(t, sourceDSN, "INSERT INTO itemf (payload) VALUES ('cdc-after-fallthrough')")
	if !waitForRowCount(t, targetDSN, "itemf", 27, 30*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("CDC did not advance after fall-through cold-start")
	}

	persistedAfter := readPersistedPosition(t, targetDSN, streamID)
	if persistedAfter == "" {
		resumeCancel()
		<-resumeErr
		t.Fatal("persisted position is empty after fall-through cold-start")
	}
	if persistedAfter == persistedBefore {
		resumeCancel()
		<-resumeErr
		t.Fatalf("persisted position was not refreshed by fall-through cold-start: before=%q after=%q",
			persistedBefore, persistedAfter)
	}

	// Tear down cleanly.
	resumeCancel()
	select {
	case err := <-resumeErr:
		if err != nil {
			t.Errorf("resume Streamer.Run returned err: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("resume Streamer.Run did not return after ctx cancel")
	}
}

// dropSlotDirect drops the named replication slot via
// pg_drop_replication_slot. Simulates the operator running
// `sluice slot drop <name>` outside of the streamer.
func dropSlotDirect(t *testing.T, dsn, slotName string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", slotName); err != nil {
		t.Fatalf("drop slot %q: %v", slotName, err)
	}
}
