//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration test for the batched-apply path of the
// snapshot+CDC orchestrator. Same-engine Postgres → Postgres: a
// source-side transaction with N INSERTs is bulk-applied to the
// dest, and the test asserts:
//
//   - All N rows land on the dest (correctness).
//   - The batching MECHANISM engaged: every row flowed through a
//     coalesced multi-row flush, observed via the applier's
//     [ir.BatchObserver] seam (batchApplyObserverForTest) — per-flush
//     row counts, not timing artifacts.
//
// An earlier version asserted a dest-side pg_stat_database commit
// delta <= a constant tolerance instead; commit counts depend on
// change-arrival timing against the coalescing window (plus control-
// table and stat-collector noise), so a loaded runner legitimately
// produced a few extra flushes and flaked the constant (2026-07-09,
// `-race` runner: 55 commits vs the pinned 52). The flush observer
// counts the mechanism itself, which is what the test exists to pin:
// per-change apply produces one 1-row flush per change (mean rows/
// flush == 1) and fails the floor below by an order of magnitude.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// flushRecorder is a test [ir.BatchObserver] that records the row count
// of every successful coalesced flush. Guarded by a mutex: the applier
// calls ObserveBatch from the apply goroutine while the test polls from
// its own.
type flushRecorder struct {
	mu      sync.Mutex
	flushes []int
}

var _ ir.BatchObserver = (*flushRecorder)(nil)

func (r *flushRecorder) ObserveBatch(_ context.Context, _ time.Duration, rows int, err error) {
	if err != nil || rows <= 0 {
		return
	}
	r.mu.Lock()
	r.flushes = append(r.flushes, rows)
	r.mu.Unlock()
}

// snapshot returns a copy of the recorded per-flush row counts.
func (r *flushRecorder) snapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.flushes))
	copy(out, r.flushes)
	return out
}

// TestStreamer_PostgresToPostgres_BatchedApply drives the whole
// streamer flow with ApplyBatchSize > 1 and asserts multi-row
// coalescing via the applier's flush observer.
func TestStreamer_PostgresToPostgres_BatchedApply(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	// Seed schema only (no rows) so the bulk-copy phase is
	// near-zero work; the test focuses on CDC-side batched apply.
	const seedDDL = `
		CREATE TABLE bulk_users (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE bulk_users REPLICA IDENTITY FULL;
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const batchSize = 100
	const totalRows = 200

	// Observe every coalesced flush through the applier's BatchObserver
	// seam. The seam takes the observer seat only when the AIMD
	// controller doesn't (AutoTune is off here — the Go zero value), and
	// it watches the SERIAL batch loop, so ApplyConcurrency is pinned to
	// 1 (the ADR-0104 lane path reports to per-lane controllers instead;
	// its coverage lives in the apply-concurrency suites).
	rec := &flushRecorder{}
	batchApplyObserverForTest = rec
	defer func() { batchApplyObserverForTest = nil }()

	streamer := &Streamer{
		Source:           pgEng,
		Target:           pgEng,
		SourceDSN:        sourceDSN,
		TargetDSN:        targetDSN,
		ApplyBatchSize:   batchSize,
		ApplyConcurrency: 1,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for the replication slot to exist before committing the
	// finite source burst below — a commit that lands BEFORE the slot is
	// created is captured by neither the snapshot nor CDC (the AIMD
	// "0/250" flake class; see [waitForSourceSlot]).
	waitForSourceSlot(t, sourceDSN, 60*time.Second)

	// Drive a single source-side transaction with N inserts.
	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	tx, err := srcDB.BeginTx(streamCtx, nil)
	if err != nil {
		t.Fatalf("source begin: %v", err)
	}
	for i := 1; i <= totalRows; i++ {
		_, err := tx.ExecContext(
			streamCtx,
			"INSERT INTO bulk_users (id, email) VALUES ($1, $2)",
			i, fmt.Sprintf("user%d@example.com", i),
		)
		if err != nil {
			t.Fatalf("source insert: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("source commit: %v", err)
	}

	// Wait for the dest to reach totalRows.
	deadline := time.Now().Add(60 * time.Second)
	for {
		got := countRows(t, targetDSN, "bulk_users")
		if got >= totalRows {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dest only saw %d/%d rows after timeout", got, totalRows)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Mechanism assertions. Every insert must have flowed through an
	// observed coalesced flush, and the flushes must be genuinely
	// multi-row: per-change apply (the regression this test guards)
	// yields totalRows flushes of 1 row — mean rows/flush 1 — an order
	// of magnitude under the floor. The floor itself is generous: the
	// ideal is ceil(totalRows/batchSize) = 2 flushes of 100; a loaded
	// runner splitting the burst into a handful of smaller flushes
	// still clears mean >= 10 easily, because flush shape depends only
	// on change-arrival coalescing, not on dest-side commit noise.
	flushes := rec.snapshot()
	var flushedRows int
	for _, n := range flushes {
		flushedRows += n
	}
	if flushedRows < totalRows {
		t.Errorf("observed %d rows across %d coalesced flushes; want >= %d (per-change Apply bypasses the batch loop and its observer entirely)",
			flushedRows, len(flushes), totalRows)
	}
	const minMeanRowsPerFlush = 10
	if maxFlushes := totalRows / minMeanRowsPerFlush; len(flushes) > maxFlushes {
		t.Errorf("%d rows landed in %d flushes (mean %.1f rows/flush); want <= %d flushes (mean >= %d) — batching is not coalescing",
			flushedRows, len(flushes), float64(flushedRows)/float64(len(flushes)), maxFlushes, minMeanRowsPerFlush)
	}
	t.Logf("batched apply: %d source rows landed in %d coalesced flushes %v (batchSize=%d)",
		flushedRows, len(flushes), flushes, batchSize)

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
