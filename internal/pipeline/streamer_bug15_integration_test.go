//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 15 (slot-ack-after-apply, ADR-0020).
//
// The pre-fix shape: sluice's PG CDC reader advanced the slot's
// confirmed_flush_lsn as soon as the pump parsed a Commit message
// off the WAL stream — which is BEFORE the apply batch commits to
// dst. When `sync stop` arrives mid-batch, sluice exits cleanly,
// the buffered events are dropped from memory, and on warm-resume
// the slot streams from a position past the un-applied batch. The
// 43-row gap in the v0.4.0 night soak was exactly this surface.
//
// The fix: only ack to the slot the LSN whose data has been
// committed to dst. The applier reports applied LSNs to a tracker
// the keepalive routine reads from; until the applier reports its
// first commit, the keepalive falls back to the streamed-LSN so
// the slot stays alive on idle streams. See [lsnTracker] and
// [CDCReader.ackLSN] for the implementation.
//
// This test mirrors workspace/bug15_repro.sh from the testing
// repo: cold-start, sustained writer, mid-stream RequestStop,
// restart, assert no row gap (the load-bearing post-restart
// invariant — the in-flight buffer at stop time must replay).

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_PostgresToPostgres_StopRestartNoLoss is the canonical
// confirmation that Bug 15's slot-ack-after-apply fix works. The
// shape:
//
//  1. Cold-start sluice on PG → PG with --apply-batch-size=50.
//  2. Drive a sustained writer on the source (10 ops/sec, 60 sec).
//  3. Mid-stream (~25 sec in), RequestStop on the target.
//  4. Streamer drains and exits.
//  5. Restart sluice; warm resume from persisted position.
//  6. Wait for tail of writer events to land on dst.
//  7. Assert MAX(id) == COUNT(*) — no row gaps.
//
// Pre-fix, this test would consistently fail with two distinct
// gaps: an in-flight gap of 20-50 rows around the stop time, and
// a tail gap of post-restart rows the wedged apply path never
// committed. Post-fix, no gap.
func TestStreamer_PostgresToPostgres_StopRestartNoLoss(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)
	// Bound the graceful-drain watchdog (production default 30s) WELL below
	// this test's 30s RequestStop wait. Root cause of a -race CI flake
	// ("Streamer.Run did not return after RequestStop"): the stop path is
	// poll-detect (~200ms here) + a graceful drain that, if it wedges on a
	// resource-pressured host (the PG replication pump can be slow to release
	// the change channel under load), is rescued by the hard-cancel watchdog
	// at observation+30s ≈ RequestStop+30.2s — JUST after the test's 30s
	// window gave up. Tuning the watchdog to 15s makes the hard-cancel return
	// Run at ~15s, comfortably inside the 30s wait, mirroring the already-
	// tuned poll cadence. Hard-cancel is still zero-loss (positions commit
	// atomically per change; ADR-0007/0025), so this doesn't weaken the
	// no-loss assertion.
	setDrainTimeoutForTest(t, 15*time.Second)

	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE bug15 (
			id      BIGSERIAL PRIMARY KEY,
			payload TEXT NOT NULL
		);
		ALTER TABLE bug15 REPLICA IDENTITY FULL;
		INSERT INTO bug15 (payload)
			SELECT 'seed-' || g FROM generate_series(1, 50) g;
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "bug15-stop-restart"

	// Phase-A instrumentation for the recurring resume-stall flake
	// (CI run 27266012795: after warm resume, dst sat at EXACTLY the
	// pre-stop count for the full 120s window — zero CDC applied —
	// then a rerun passed). Under the shard's non-verbose `go test`,
	// the streamer's slog output is unrecoverable post-mortem (the
	// failed run's log contained ZERO lines for this stream id), so
	// capture it at DEBUG and dump it only on failure. The discarded
	// resume-Run error below gets the same treatment.
	logs := captureSlog(t)

	// ---- Phase 1: cold-start with batched apply ----
	streamer := &Streamer{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      sourceDSN,
		TargetDSN:      targetDSN,
		StreamID:       streamID,
		ApplyBatchSize: 50,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for bulk-copy to deliver the seed rows.
	if !waitForRowCount(t, targetDSN, "bug15", 50, 30*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("bulk copy did not deliver seed rows")
	}

	// ---- Phase 2: sustained writer ----
	// Generate ~600 rows over ~60 seconds. Stop is issued at ~25s
	// (250 ops in), so the in-flight buffer at stop time will be a
	// realistic chunk of the 50-batch.
	writerCtx, writerCancel := context.WithCancel(context.Background())
	defer writerCancel()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		runWriter(t, writerCtx, sourceDSN)
	}()

	// Let the writer get a head start so events accumulate.
	time.Sleep(2 * time.Second)

	// Wait until ~50 rows have flowed through CDC, then ~more time
	// to ensure batched apply has been buffering. Rough timing:
	// writer adds 10/sec; we wait for ~150 rows past the seed.
	if !waitForRowCount(t, targetDSN, "bug15", 50+50, 30*time.Second) {
		writerCancel()
		streamCancel()
		<-runErr
		<-writerDone
		t.Fatalf("CDC did not advance after writer started")
	}

	// ---- Phase 3: RequestStop mid-stream ----
	applierCtx, applierCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer applierCancel()
	stopApplier, err := pgEng.OpenChangeApplier(applierCtx, targetDSN)
	if err != nil {
		writerCancel()
		streamCancel()
		<-runErr
		<-writerDone
		t.Fatalf("OpenChangeApplier (for stop): %v", err)
	}
	if err := stopApplier.RequestStop(applierCtx, streamID); err != nil {
		_ = closeApplier(stopApplier)
		writerCancel()
		streamCancel()
		<-runErr
		<-writerDone
		t.Fatalf("RequestStop: %v", err)
	}
	_ = closeApplier(stopApplier)

	select {
	case err := <-runErr:
		if err != nil {
			writerCancel()
			<-writerDone
			t.Fatalf("Streamer.Run returned err on stop: %v", err)
		}
	case <-time.After(30 * time.Second):
		writerCancel()
		streamCancel()
		<-writerDone
		t.Fatal("Streamer.Run did not return after RequestStop")
	}

	// Tear down the first streamer's pump goroutine (and its
	// replication connection) before opening a second one. RequestStop
	// only cancels the apply loop; the CDC reader's pump is bound to
	// streamCtx and would otherwise hold the slot active until the
	// deferred streamCancel runs at test-end. Without this, the warm-
	// resume's StartReplication races with the previous connection's
	// release and the slot reports "active for PID N".
	streamCancel()
	// No release-grace sleep: the warm resume's START_REPLICATION now
	// absorbs the prior pump's slot-release window itself via the
	// bounded slot-active retry in the PG CDC reader (SQLSTATE 55006,
	// see startReplicationWithSlotActiveRetry). Racing the resume
	// directly against the release is deliberate — this test now
	// EXERCISES that retry path instead of papering over the race
	// with a 5s sleep that still lost under CI contention.

	// Streamer is now stopped. The writer continues producing rows
	// on the source — those events will accumulate in WAL until the
	// next streamer comes online. Pre-fix, the in-flight batch
	// buffered at stop time was dropped, AND the slot had been
	// ack'd past it; post-fix, the slot retains the WAL because
	// the applier never reported those LSNs as applied.

	// ---- Phase 4: warm resume ----
	t.Logf("phase 4: resuming; src=%d dst=%d at resume start",
		readSrcRowCount(t, sourceDSN, "bug15"), pollRowCount(targetDSN, "bug15"))
	resumeStreamer := &Streamer{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      sourceDSN,
		TargetDSN:      targetDSN,
		StreamID:       streamID,
		ApplyBatchSize: 50,
	}
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	defer resumeCancel()
	resumeErr := make(chan error, 1)
	go func() { resumeErr <- resumeStreamer.Run(resumeCtx) }()

	// Let the writer finish, then drain the stream.
	<-writerDone

	// Give the resumed streamer time to apply the tail events.
	// Source row count is the target. Use a forgiving threshold —
	// the load-bearing invariant is MAX(id) == COUNT(*) (no row
	// gaps), not "perfectly caught up to source". With
	// --apply-batch-size=50, a single trailing row past the
	// last full batch can sit in the in-flight buffer until the
	// next batch fills or the stream closes; that's a separate
	// throughput knob, not a Bug 15 regression. We wait until
	// dst is within 50 rows of src (one batch's worth).
	srcCount := readSrcRowCount(t, sourceDSN, "bug15")
	threshold := srcCount - 50
	if threshold < 50 {
		threshold = 50
	}
	// 120s (not 60s): after the warm resume reconnects, the tail events drain
	// at the per-change/batched CDC apply rate, which under the -race scheduler
	// + self-hosted disk-I/O contention is occasionally slow enough that 60s
	// was a flaky deadline (the recurring "bug15 catch-up" rerun). The
	// load-bearing assertion (no gaps, MAX(id)==COUNT(*)) is unchanged; this
	// only widens the catch-up window so a slow-but-correct drain isn't a
	// false failure.
	// The wait watches resumeErr concurrently: if the resume Run
	// returns EARLY (slot still active, retries exhausted, …) the old
	// shape sat blind in the row-count poll for the full window and
	// then DISCARDED the error (`<-resumeErr` without capturing) — the
	// stall's diagnosis was thrown away on every occurrence. Now an
	// early return fails immediately with the actual error, and the
	// timeout path dumps the error + the captured streamer log.
	catchupDeadline := time.Now().Add(120 * time.Second)
	caughtUp := false
	for time.Now().Before(catchupDeadline) {
		if pollRowCount(targetDSN, "bug15") >= threshold {
			caughtUp = true
			break
		}
		select {
		case err := <-resumeErr:
			t.Logf("captured streamer log:\n%s", logs.String())
			t.Fatalf("resume Streamer.Run returned EARLY during catch-up (src=%d, dst=%d, threshold=%d): %v",
				srcCount, pollRowCount(targetDSN, "bug15"), threshold, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !caughtUp {
		resumeCancel()
		var runResult error
		select {
		case runResult = <-resumeErr:
		case <-time.After(15 * time.Second):
			runResult = fmt.Errorf("(resume Run did not return within 15s of cancel)")
		}
		t.Logf("captured streamer log:\n%s", logs.String())
		t.Fatalf("after resume, dst rows did not catch up to threshold (src=%d, dst=%d, threshold=%d); resume Run returned: %v",
			srcCount, pollRowCount(targetDSN, "bug15"), threshold, runResult)
	}

	// ---- Phase 5: assert no gaps ----
	// MAX(id) == COUNT(*) for a contiguous BIGSERIAL is the
	// canonical "no rows lost" invariant from the bug catalog.
	// This is the load-bearing assertion: pre-fix, the gap of
	// dropped in-flight events would show up as MAX > COUNT.
	maxID, count := readMaxAndCount(t, targetDSN, "bug15")
	if maxID != count {
		t.Errorf("dst row gap: MAX(id)=%d, COUNT(*)=%d (delta=%d) — slot-ack-after-apply did not preserve in-flight buffer",
			maxID, count, maxID-count)
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

// runWriter drives a sustained INSERT loop on the source: 10
// ops/sec, until ctx is cancelled or 60s elapses (whichever
// first).
func runWriter(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Logf("writer: open: %v", err)
		return
	}
	defer func() { _ = db.Close() }()

	timeout := time.NewTimer(60 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for i := 1; i < 6000; i++ {
		select {
		case <-ctx.Done():
			return
		case <-timeout.C:
			return
		case <-tick.C:
			if _, err := db.ExecContext(ctx, "INSERT INTO bug15 (payload) VALUES ($1)",
				fmt.Sprintf("continuous-%d", i)); err != nil {
				if ctx.Err() != nil {
					return
				}
				// Source-side errors (deadlines, connection drops)
				// happen when the test tears down. Don't escalate.
				return
			}
		}
	}
}

// readMaxAndCount returns MAX(id) and COUNT(*) for the table. The
// gap-detection invariant for a contiguous BIGSERIAL is MAX = COUNT.
func readMaxAndCount(t *testing.T, dsn, table string) (max, count int64) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q := fmt.Sprintf("SELECT COALESCE(MAX(id), 0), COUNT(*) FROM %s", table)
	if err := db.QueryRowContext(ctx, q).Scan(&max, &count); err != nil {
		t.Fatalf("max+count: %v", err)
	}
	return max, count
}

// readSrcRowCount returns the source-side COUNT(*) for the named
// table. Used to compute the target the resumed streamer must
// catch up to before the gap assertion runs.
func readSrcRowCount(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count src: %v", err)
	}
	return n
}

// closeApplier is a small helper to close an applier obtained from
// the engine factory in tests; the public IR doesn't expose Close
// on the interface, but every shipping engine's applier embeds it.
func closeApplier(a any) error {
	if c, ok := a.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}
