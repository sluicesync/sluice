//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for --reset-target-data on `sluice sync start`
// (ADR-0023). Combines with the ADR-0022 slot-missing fall-through
// to validate the one-command recovery path: after the operator drops
// a slot and the dest is left populated, a single re-run with
// --reset-target-data wipes the dest cleanly and re-streams.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// waitForPersistedPositionChanged polls until source_position for
// the named stream is non-empty AND differs from `before`, or until
// timeout. Returns the new position on success, "" on timeout.
//
// The "position changed from a known prior value" signal is the
// load-bearing way to confirm that a reset (or any operation that
// clears + re-populates the cdc-state row) actually fired. The
// alternative "wait for row to go missing" is brittle as of v0.40.1
// because cold-start persists the snapshot anchor as the cdc-state
// row immediately after bulk-copy completes (closing GitHub issue
// #15), shrinking the "row absent" window to roughly the bulk-copy
// duration. "Position changed" is strictly stronger — it requires
// both that the reset cleared the original row AND that the new run
// wrote a fresh row under a different snapshot/CDC position.
func waitForPersistedPositionChanged(t *testing.T, dsn, streamID, before string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := readPersistedPositionTolerant(dsn, streamID)
		if got != "" && got != before {
			return got
		}
		time.Sleep(200 * time.Millisecond)
	}
	return ""
}

// waitForPersistedPosition polls until source_position for streamID
// is non-empty on the target, or until timeout. Returns the final
// observed token (empty on timeout).
func waitForPersistedPosition(t *testing.T, dsn, streamID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := readPersistedPositionTolerant(dsn, streamID)
		if got != "" {
			return got
		}
		time.Sleep(200 * time.Millisecond)
	}
	return ""
}

// readPersistedPositionTolerant is the no-fail variant of
// readPersistedPosition: returns "" on missing row, missing table, or
// any other error, instead of t.Fatalf'ing. Suited for poll loops.
func readPersistedPositionTolerant(dsn, streamID string) string {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var token string
	err = db.QueryRowContext(ctx,
		`SELECT source_position FROM "public"."sluice_cdc_state" WHERE stream_id = $1`,
		streamID).Scan(&token)
	if err != nil {
		// Tolerate undefined-relation and no-rows uniformly.
		if errMsg := err.Error(); strings.Contains(errMsg, "does not exist") {
			return ""
		}
		return ""
	}
	return token
}

// TestStreamer_ResetTargetData_RecoversFromSlotMissing combines
// ADR-0022 (slot-missing fall-through) and ADR-0023 (reset-target-
// data) end-to-end:
//
//  1. Cold-start sluice on PG → PG; let bulk-copy + a CDC change land
//     so `sluice_cdc_state` has a real persisted position.
//  2. Cancel the streamer.
//  3. Drop the slot directly (simulates `sluice slot drop` after
//     wal_status='lost').
//  4. Re-run `sync start` with --reset-target-data --yes (no
//     manual DROP TABLE step).
//  5. Assert: the dest is wiped + re-bulk-copied; the cdc-state row
//     is overwritten; CDC continues to advance after the reset.
//
// Pre-fix shape (without --reset-target-data): the operator had to
// manually DROP TABLE every dest table after `slot drop`, then
// re-run. Post-fix: the single command above suffices.
func TestStreamer_ResetTargetData_RecoversFromSlotMissing(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE reset_recovery (
			id      BIGSERIAL PRIMARY KEY,
			payload TEXT NOT NULL
		);
		ALTER TABLE reset_recovery REPLICA IDENTITY FULL;
		INSERT INTO reset_recovery (payload)
			SELECT 'seed-' || g FROM generate_series(1, 20) g;
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "reset-target-data-recovery"

	// ---- Phase 1: cold-start, drive a CDC change so position advances. ----
	streamer := &Streamer{
		Source: pgEng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		StreamID: streamID,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCount(t, targetDSN, "reset_recovery", 20, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("bulk copy did not deliver seed rows")
	}
	applyDDL(t, sourceDSN, "INSERT INTO reset_recovery (payload) VALUES ('cdc-1')")
	if !waitForRowCount(t, targetDSN, "reset_recovery", 21, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC did not advance after first writer event")
	}

	persistedBefore := readPersistedPosition(t, targetDSN, streamID)
	if persistedBefore == "" {
		streamCancel()
		<-runErr
		t.Fatal("persisted position is empty after CDC change")
	}

	// ---- Phase 2: cancel the streamer + give the pump a moment. ----
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

	// ---- Phase 3: drop the slot. ----
	dropSlotDirect(t, sourceDSN, "sluice_slot")

	// ---- Phase 4: re-run with --reset-target-data --yes. The dest
	// is still populated from phase 1; without the reset flag, the
	// pre-flight refusal would fire after the slot-missing fall-
	// through. With the flag, the cdc-state row is cleared, the
	// dest tables are dropped, and cold-start runs. ----
	resetStreamer := &Streamer{
		Source: pgEng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		StreamID:        streamID,
		ResetTargetData: true,
	}
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- resetStreamer.Run(resumeCtx) }()

	// ---- Phase 5: wait for the cdc-state row to advance past the
	// pre-reset position. The reset path deletes the row, then
	// cold-start re-creates it: as of v0.40.1 (GitHub issue #15)
	// cold-start persists the snapshot anchor before the first CDC
	// apply, so "row absent" is too brief to reliably observe with a
	// poll loop. "Position changed from persistedBefore" is the
	// stronger invariant — it proves both that the reset cleared
	// the original row AND that the new cold-start wrote a fresh
	// anchor under a different snapshot. ----
	if got := waitForPersistedPositionChanged(t, targetDSN, streamID, persistedBefore, 30*time.Second); got == "" {
		resumeCancel()
		<-resumeErr
		t.Fatalf("reset did not advance the cdc-state row past the pre-reset position within timeout (still %q)", persistedBefore)
	}

	// Now wait for bulk-copy to deliver the 21 rows in source. The
	// stale phase-1 row count of 21 has already been wiped by the
	// drop in the line above's preceding step, so pollRowCount sees
	// 0 → 21 monotonically.
	if !waitForRowCount(t, targetDSN, "reset_recovery", 21, 60*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("after reset: dst was not re-seeded by cold-start (got %d rows)",
			pollRowCount(targetDSN, "reset_recovery"))
	}

	// Drive a CDC change AFTER bulk-copy completes so the resumed
	// streamer writes a fresh cdc-state row when the change applies.
	applyDDL(t, sourceDSN, "INSERT INTO reset_recovery (payload) VALUES ('cdc-after-reset')")
	if !waitForRowCount(t, targetDSN, "reset_recovery", 22, 30*time.Second) {
		resumeCancel()
		<-resumeErr
		t.Fatalf("CDC did not advance after reset cold-start")
	}

	persistedAfter := waitForPersistedPosition(t, targetDSN, streamID, 30*time.Second)
	if persistedAfter == "" {
		resumeCancel()
		<-resumeErr
		t.Fatal("persisted position is empty after reset")
	}
	if persistedAfter == persistedBefore {
		resumeCancel()
		<-resumeErr
		t.Fatalf("persisted position was not refreshed by reset: before=%q after=%q",
			persistedBefore, persistedAfter)
	}

	resumeCancel()
	select {
	case err := <-resumeErr:
		if err != nil {
			t.Errorf("reset Streamer.Run returned err: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("reset Streamer.Run did not return after ctx cancel")
	}
}
