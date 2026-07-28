//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-Postgres pin for the cold-start handoff stop rule: stopping a
// stream in the window between "bulk copy committed on the target" and
// "CDC started" must leave the source's replication slot ALIVE and the
// copy RESUMABLE.
//
// The regression this pins was a silent one-way door. The cold-start CDC
// anchor write (GitHub issue #15) rode the caller's ctx, so an operator
// stop inside that window failed the write with context.Canceled — and
// that error path is [ir.SnapshotStream.Abandon], which DROPS the
// just-created slot and the per-stream publication. The target kept the
// copied rows but had no cdc-state row, so neither resume path worked:
// warm resume has no position to resume from, and cold start refuses on
// the populated target. The only escape was `--reset-target-data`, i.e.
// re-copying the whole database.
//
// This also closes the long-running flake in
// TestStreamer_WarmResume_PG_FullSlots_NeverProbesRefuses, whose
// "walsender never released sluice_slot" failure was really "the slot no
// longer exists" — a wait that could never be satisfied, which is why two
// rounds of raising its timeout did not help.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

// coldStartStopAttempts is how many independent stop-at-copy-completion
// rounds the pin runs. The stop's landing point is timing-dependent
// (pre-fix, roughly two thirds of tight-poll stops landed in the anchor
// window and the rest just after it), so a single round would miss the
// regression about a third of the time. Every round asserts the
// invariant, which drives that miss rate into the noise; the
// deterministic counterpart — coldStartBeginCDC's dispatch under an
// already-cancelled ctx — is pinned as a unit test in
// streamer_coldstart_anchor_cancel_test.go.
const coldStartStopAttempts = 4

// TestStreamer_ColdStartStopInHandoff_PG_KeepsSlotAndResumes stops a cold
// start the instant its copy lands (a 1ms poll, vs the 200ms poll other
// tests use, so the stop lands INSIDE the handoff rather than after it)
// and requires that the stream is still resumable afterwards.
func TestStreamer_ColdStartStopInHandoff_PG_KeepsSlotAndResumes(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	interrupted := 0
	resumeProven := false
	for attempt := 1; attempt <= coldStartStopAttempts; attempt++ {
		func() {
			src, tgt, cleanup := startPostgresLogical(t)
			defer cleanup()

			applyDDL(t, src, `CREATE TABLE handoff_t (id BIGINT PRIMARY KEY, v INT);
				INSERT INTO handoff_t (id, v) SELECT g, g FROM generate_series(1, 50) g;`)

			newStream := func() *Streamer {
				return &Streamer{
					Source:    pgEng,
					Target:    pgEng,
					SourceDSN: src,
					TargetDSN: tgt,
					StreamID:  "coldstart-handoff-stop",
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- newStream().Run(ctx) }()

			// Tight poll: stop as close to the copy's commit as possible.
			deadline := time.Now().Add(2 * time.Minute)
			copied := false
			for time.Now().Before(deadline) {
				if pollRowCount(tgt, "handoff_t") == 50 {
					copied = true
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !copied {
				t.Fatalf("attempt %d: cold start never delivered 50 rows (got %d)",
					attempt, pollRowCount(tgt, "handoff_t"))
			}
			cancel()

			var runErr error
			select {
			case runErr = <-errCh:
			case <-time.After(30 * time.Second):
				t.Fatalf("attempt %d: cold-start Run did not return after ctx cancel", attempt)
			}
			if runErr != nil {
				interrupted++
			}
			t.Logf("attempt %d: cold-start Run returned err=%v", attempt, runErr)

			// THE INVARIANT: the durable resume anchor survives the stop,
			// wherever in the handoff the stop happened to land.
			if !pgSlotExists(t, src, "sluice_slot") {
				t.Fatalf("attempt %d: a stop during the cold-start handoff DROPPED sluice_slot; the "+
					"completed copy on the target is now unresumable (warm resume has no position, "+
					"cold start refuses on the populated target). Run error was: %v", attempt, runErr)
			}
			if resumeProven || runErr == nil {
				return
			}

			// And it is a real anchor, not debris: a warm resume must pick
			// it up and apply a live change. Were the cdc-state row missing,
			// this second Run would refuse on the populated target instead.
			resumeCtx, resumeCancel := context.WithCancel(context.Background())
			defer resumeCancel()
			resumeErrCh := make(chan error, 1)
			go func() { resumeErrCh <- newStream().Run(resumeCtx) }()

			applyDDL(t, src, `INSERT INTO handoff_t (id, v) VALUES (51, 51);`)
			if !waitForExactRowCount(tgt, "handoff_t", 51, 2*time.Minute) {
				select {
				case err := <-resumeErrCh:
					t.Fatalf("attempt %d: warm resume after the interrupted handoff exited: %v", attempt, err)
				default:
					t.Fatalf("attempt %d: warm resume after the interrupted handoff never applied the "+
						"live change (got %d rows)", attempt, pollRowCount(tgt, "handoff_t"))
				}
			}
			resumeCancel()
			select {
			case <-resumeErrCh:
			case <-time.After(30 * time.Second):
				t.Fatalf("attempt %d: warm-resume Run did not return after ctx cancel", attempt)
			}
			resumeProven = true
		}()
	}

	// Vacuous-green guard: a run in which every stop landed after CDC was
	// already live never entered the window this pin exists to cover.
	if interrupted == 0 {
		t.Fatalf("no attempt out of %d stopped inside the cold-start handoff window; the pin never "+
			"exercised what it exists to cover (widen the window or revisit the poll)",
			coldStartStopAttempts)
	}
	if !resumeProven {
		t.Fatal("the interrupted-handoff warm resume was never exercised")
	}
}
