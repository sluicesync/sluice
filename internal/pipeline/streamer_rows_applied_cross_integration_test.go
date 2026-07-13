//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end pin for the ADR-0156 phase-2 cumulative
// rows_applied counter (surfaced in `sync start`'s live panel). A MySQL
// (binlog) source streams into a Postgres target; the test drives a KNOWN
// number of row-level DML changes and asserts the target control table's
// StreamStatus.RowsApplied equals the exact count — then STOPS the streamer,
// WARM-RESUMES a fresh streamer over the same stream, drives more DML, and
// asserts the counter CONTINUES from the persisted total (never resets to 0).
//
// Only INSERT/UPDATE/DELETE count; the schema-only cold-copy applies zero rows
// (so the snapshot phase doesn't perturb the CDC counter). Prefixed
// TestStreamer_ to ride the streamer CI shard.
package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// rowsAppliedForStream opens a fresh target applier and returns the persisted
// cumulative rows_applied for streamID (0 if the stream row is absent).
func rowsAppliedForStream(t *testing.T, pgEng ir.Engine, targetDSN, streamID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	applier, err := pgEng.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier(target): %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		t.Fatalf("ListStreams: %v", err)
	}
	for _, s := range streams {
		if s.StreamID == streamID {
			return s.RowsApplied
		}
	}
	return 0
}

// waitRowsApplied polls the target control table until rows_applied for
// streamID reaches at least want, returning the observed value (or the last
// seen on timeout, which the caller asserts against).
func waitRowsApplied(t *testing.T, pgEng ir.Engine, targetDSN, streamID string, want int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int64
	for {
		got = rowsAppliedForStream(t, pgEng, targetDSN, streamID)
		if got >= want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestStreamer_RowsAppliedCounter_CrossEngineWarmResume(t *testing.T) {
	mySrc, _, myCleanup := startMySQLBinlog(t)
	defer myCleanup()
	_, pgDst, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const table = "rows_applied_t"
	const streamID = "rows-applied-cross"

	// Schema only (no seed rows) so cold-copy applies ZERO rows and the CDC
	// counter reflects only the DML the test drives.
	applyDDLMySQL(t, mySrc, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, val INT NOT NULL)")

	srcDB, err := sql.Open("mysql", mySrc)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	execSrc := func(q string, args ...any) {
		t.Helper()
		if _, err := srcDB.Exec(q, args...); err != nil {
			t.Fatalf("source exec %q: %v", q, err)
		}
	}

	// --- Phase 1: cold streamer + a known DML burst ---
	startStreamer := func() (cancel context.CancelFunc, done chan error) {
		s := &Streamer{
			Source:    myEng,
			Target:    pgEng,
			SourceDSN: mySrc,
			TargetDSN: pgDst,
			StreamID:  streamID,
		}
		ctx, cxl := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- s.Run(ctx) }()
		return cxl, errCh
	}

	cancel1, done1 := startStreamer()

	// 5 inserts, 2 updates, 1 delete = 8 row-level DML. Net target rows = 4
	// (ids 1,2,3,4 survive; id 5 deleted). Each statement auto-commits as its
	// own source transaction.
	for i := 1; i <= 5; i++ {
		execSrc("INSERT INTO "+table+" (id, val) VALUES (?, ?)", i, i*10)
	}
	execSrc("UPDATE "+table+" SET val = ? WHERE id = ?", 111, 1)
	execSrc("UPDATE "+table+" SET val = ? WHERE id = ?", 222, 2)
	execSrc("DELETE FROM "+table+" WHERE id = ?", 5)
	const phase1DML = 8

	if ok := waitForRowCount(t, pgDst, table, 4, 60*time.Second); !ok {
		t.Fatalf("target never reached 4 rows after phase-1 DML (got %d)", countRows(t, pgDst, table))
	}
	got := waitRowsApplied(t, pgEng, pgDst, streamID, phase1DML, 30*time.Second)
	if got != phase1DML {
		t.Fatalf("phase-1 rows_applied = %d; want exactly %d (5 inserts + 2 updates + 1 delete, no resends within a run)", got, phase1DML)
	}

	// Stop the first streamer cleanly.
	cancel1()
	select {
	case err := <-done1:
		if err != nil {
			t.Errorf("phase-1 Streamer.Run: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("phase-1 streamer did not exit within 20s of cancel")
	}

	// --- Phase 2: WARM RESUME + more DML ---
	// A fresh streamer over the same stream must resume from the persisted
	// position (the target table already holds the data) and the counter must
	// CONTINUE from phase1DML — never restart at 0.
	cancel2, done2 := startStreamer()
	defer func() {
		cancel2()
		select {
		case <-done2:
		case <-time.After(20 * time.Second):
			t.Error("phase-2 streamer did not exit within 20s of cancel")
		}
	}()

	for i := 6; i <= 8; i++ {
		execSrc("INSERT INTO "+table+" (id, val) VALUES (?, ?)", i, i*10)
	}
	const phase2NewDML = 3
	const wantAtLeast = phase1DML + phase2NewDML // 11

	if ok := waitForRowCount(t, pgDst, table, 7, 60*time.Second); !ok {
		t.Fatalf("target never reached 7 rows after phase-2 DML (got %d)", countRows(t, pgDst, table))
	}
	got = waitRowsApplied(t, pgEng, pgDst, streamID, wantAtLeast, 30*time.Second)
	// >= (not ==): a warm resume may redeliver a bounded tail of already-
	// applied changes (at-least-once), which the counter re-counts — honest
	// per StreamStatus.RowsApplied's contract. The load-bearing assertion is
	// that the counter CONTINUED from the persisted total (>= 11), proving it
	// did not reset to 0 (which would leave it at ~3).
	if got < wantAtLeast {
		t.Fatalf("phase-2 rows_applied = %d; want >= %d (counter must continue from the persisted %d, not reset)",
			got, wantAtLeast, phase1DML)
	}
	t.Logf("rows_applied: phase-1 = %d, after warm-resume + %d DML = %d", phase1DML, phase2NewDML, got)
}
