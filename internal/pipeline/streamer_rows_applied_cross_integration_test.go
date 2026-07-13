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
		// Transient during streamer startup: the per-target control table may
		// be mid-creation (created, but an additive ALTER — slot_name /
		// rows_applied — not yet applied). Production's panel poller tolerates
		// this identically (pollLiveStatus treats a ListStreams error as
		// "reconnecting", never fatal), and this is a polling helper, so treat
		// it as "no rows counted yet" and let the caller's timeout govern.
		return 0
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

	// Defeat the cold-copy/CDC snapshot race: a row inserted before the
	// streamer locks its CDC start position is bulk-copied into the initial
	// snapshot, NOT applied through CDC — and rows_applied is the CDC-apply
	// counter, so snapshot rows are (correctly) not counted. Driving the
	// measured DML immediately after startStreamer would race that boundary
	// nondeterministically. Instead drive throwaway sentinel rows (ids from
	// 1000, disjoint from the measured ids 1..8) until the counter moves,
	// waiting for each sentinel's result before the next so none are left in
	// flight — proving CDC is engaged and counting. Every DML after this is a
	// CDC change.
	sentinelID := 1000
	waitCDCCounting := func() {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			before := rowsAppliedForStream(t, pgEng, pgDst, streamID)
			execSrc("INSERT INTO "+table+" (id, val) VALUES (?, 0)", sentinelID)
			sentinelID++
			if waitRowsApplied(t, pgEng, pgDst, streamID, before+1, 5*time.Second) > before {
				return
			}
		}
		t.Fatal("CDC never engaged / counted a sentinel within 90s (cold-copy handoff never completed)")
	}
	// stableBaseline reads rows_applied after it stops moving, so any
	// still-in-flight sentinel apply is fully settled before the measured
	// burst below asserts an EXACT delta (no late count inflating it).
	stableBaseline := func() int64 {
		t.Helper()
		last := rowsAppliedForStream(t, pgEng, pgDst, streamID)
		for {
			time.Sleep(1500 * time.Millisecond)
			cur := rowsAppliedForStream(t, pgEng, pgDst, streamID)
			if cur == last {
				return cur
			}
			last = cur
		}
	}
	waitCDCCounting()
	base1 := stableBaseline()

	// 5 inserts, 2 updates, 1 delete = 8 row-level DML, all CDC changes now
	// (driven strictly after CDC engaged). Each statement auto-commits as its
	// own source transaction.
	for i := 1; i <= 5; i++ {
		execSrc("INSERT INTO "+table+" (id, val) VALUES (?, ?)", i, i*10)
	}
	execSrc("UPDATE "+table+" SET val = ? WHERE id = ?", 111, 1)
	execSrc("UPDATE "+table+" SET val = ? WHERE id = ?", 222, 2)
	execSrc("DELETE FROM "+table+" WHERE id = ?", 5)
	const phase1DML = 8

	want1 := base1 + phase1DML
	got := waitRowsApplied(t, pgEng, pgDst, streamID, want1, 60*time.Second)
	if got != want1 {
		t.Fatalf("phase-1 rows_applied = %d; want exactly %d (baseline %d + 8 CDC DML: 5 inserts + 2 updates + 1 delete)", got, want1, base1)
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

	// The counter must have PERSISTED across the restart — never reset to 0.
	// (A warm resume may redeliver a bounded already-applied tail that is
	// re-counted, so the resumed total is >= the phase-1 total, never less.)
	base2 := waitRowsApplied(t, pgEng, pgDst, streamID, want1, 30*time.Second)
	if base2 < want1 {
		t.Fatalf("warm-resume rows_applied = %d; counter must persist at >= the phase-1 total %d, not reset", base2, want1)
	}

	// Re-confirm CDC engaged on the resumed stream (same snapshot-race guard),
	// then drive 3 new CDC DML and assert the counter CONTINUES upward. Uses
	// >= because a warm resume may redeliver an already-applied tail (honest
	// per StreamStatus.RowsApplied's at-least-once contract).
	waitCDCCounting()
	base3 := stableBaseline()
	for i := 6; i <= 8; i++ {
		execSrc("INSERT INTO "+table+" (id, val) VALUES (?, ?)", i, i*10)
	}
	const phase2NewDML = 3
	wantAtLeast := base3 + phase2NewDML
	got = waitRowsApplied(t, pgEng, pgDst, streamID, wantAtLeast, 60*time.Second)
	if got < wantAtLeast {
		t.Fatalf("phase-2 rows_applied = %d; want >= %d (persisted %d + 3 new CDC DML; counter must continue upward)", got, wantAtLeast, base3)
	}
	t.Logf("rows_applied: phase-1 total = %d, warm-resume baseline = %d, after +3 DML = %d", want1, base2, got)
}
