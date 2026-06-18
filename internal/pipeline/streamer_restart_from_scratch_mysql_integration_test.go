//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for the #244 value-fidelity-review bug:
// --restart-from-scratch (and the ADR-0093 auto-resnapshot recovery,
// which sets RestartFromScratch internally) onto a NON-idempotent
// native-MySQL source must land cleanly on a POPULATED target.
//
// Pre-fix: the dispatch forced a fresh cold-start onto the NON-dropped
// target while skipping the Bug-9 populated-target preflight, trusting
// that "the idempotent copy absorbs the overlap". But the native MySQL
// binlog snapshot reader does NOT implement ir.IdempotentCopyReader, so
// its cold-copy runs PLAIN INSERT — which dup-key ERRORS (MySQL Error
// 1062) on the leftover rows. The misleading hint promised idempotency
// that the native path never had.
//
// Post-fix: coldStartGatePreflight detects the non-idempotent reader and
// drops + recreates the in-scope target tables before the fresh cold-copy
// (resetTargetTablesForRestart), so the plain-INSERT copy starts clean.
// The cdc-state row is preserved.
//
// This is the integration counterpart to the unit pins in
// restart_from_scratch_reset_test.go. It does the SAME thing the
// existing binlog-purged test does EXCEPT it deliberately does NOT drop
// the target before the re-run — the whole point is that restart-from-
// scratch must clean the target itself.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestStreamer_MySQLToMySQL_RestartFromScratch_PopulatedTarget pins that
// a restart-from-scratch onto a populated native-MySQL target recovers
// cleanly (no Error 1062), by dropping + re-copying rather than
// plain-INSERTing onto the leftover rows.
func TestStreamer_MySQLToMySQL_RestartFromScratch_PopulatedTarget(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)

	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE restart_scratch (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO restart_scratch (payload) VALUES
			('seed-1'), ('seed-2'), ('seed-3'), ('seed-4'), ('seed-5');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "mysql-restart-from-scratch"

	// ---- Phase 1: cold-start, drive a CDC change so the target is
	// populated (5 seeds + 1 CDC) and a position is persisted. ----
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "restart_scratch", 5, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("bulk copy did not deliver seed rows")
	}
	applyDDLMySQL(t, sourceDSN, "INSERT INTO restart_scratch (payload) VALUES ('cdc-1')")
	if !waitForRowCountMySQL(t, targetDSN, "restart_scratch", 6, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("CDC did not advance after first writer event")
	}

	persistedBefore := readPersistedPositionMySQL(t, targetDSN, streamID)
	if persistedBefore == "" {
		streamCancel()
		<-runErr
		t.Fatal("persisted position is empty after CDC change")
	}

	// ---- Phase 2: cancel the streamer. The target now HOLDS 6 rows and
	// retains its schema (PK). We deliberately do NOT drop it. ----
	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Streamer.Run returned err on cancel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}

	if got := pollRowCountMySQL(targetDSN, "restart_scratch"); got != 6 {
		t.Fatalf("precondition: target should hold 6 rows before restart; got %d", got)
	}

	// ---- Phase 3: re-run with --restart-from-scratch onto the populated
	// target. Pre-fix this dup-key-errors (Error 1062) because the native
	// cold-copy plain-INSERTs the re-copied seeds onto the leftover rows.
	// Post-fix the gate drops + recreates the table, then re-copies. ----
	restartStreamer := &Streamer{
		Source:             mysqlEng,
		Target:             mysqlEng,
		SourceDSN:          sourceDSN,
		TargetDSN:          targetDSN,
		StreamID:           streamID,
		RestartFromScratch: true,
	}
	restartCtx, restartCancel := context.WithCancel(context.Background())
	defer restartCancel()
	restartErr := make(chan error, 1)
	go func() { restartErr <- restartStreamer.Run(restartCtx) }()

	// The re-copy must land all 6 existing source rows cleanly. If the
	// pre-fix plain-INSERT-onto-populated bug were present, Run would have
	// errored on the first re-copied seed (id=1 collides). A stable count
	// of exactly 6 (not 12, not an error) proves a clean from-scratch copy
	// onto a freshly-cleaned target.
	if !waitForRowCountMySQL(t, targetDSN, "restart_scratch", 6, 60*time.Second) {
		select {
		case err := <-restartErr:
			restartCancel()
			t.Fatalf("restart-from-scratch onto populated target failed (this is the #244 dup-key bug if it mentions Error 1062): %v", err)
		default:
		}
		restartCancel()
		<-restartErr
		t.Fatalf("restart-from-scratch did not re-seed cleanly; got %d rows", pollRowCountMySQL(targetDSN, "restart_scratch"))
	}

	// A late Run error would mean the copy started but collided partway.
	select {
	case err := <-restartErr:
		restartCancel()
		t.Fatalf("restart-from-scratch Run errored after starting copy: %v", err)
	case <-time.After(1 * time.Second):
		// no error — healthy
	}

	// ---- Phase 4: confirm CDC still flows after the restart cold-start
	// and a fresh position is written (the cdc-state row was preserved,
	// only the position discarded). ----
	applyDDLMySQL(t, sourceDSN, "INSERT INTO restart_scratch (payload) VALUES ('cdc-after-restart')")
	if !waitForRowCountMySQL(t, targetDSN, "restart_scratch", 7, 30*time.Second) {
		restartCancel()
		<-restartErr
		t.Fatalf("CDC did not advance after restart-from-scratch cold-start")
	}

	persistedAfter := readPersistedPositionMySQL(t, targetDSN, streamID)
	if persistedAfter == "" {
		restartCancel()
		<-restartErr
		t.Fatal("persisted position is empty after restart-from-scratch")
	}

	restartCancel()
	select {
	case err := <-restartErr:
		if err != nil {
			t.Errorf("restart Streamer.Run returned err: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("restart Streamer.Run did not return after ctx cancel")
	}
}
