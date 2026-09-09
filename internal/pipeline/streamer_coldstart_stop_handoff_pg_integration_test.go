//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A stop that lands after the bulk copy has committed must not destroy the
// resume anchor (the replication slot) — and this pin says, per window,
// what the kept slot actually buys.
//
// Two windows exist after the copy commits, and they are NOT the same:
//
//   - The ANCHOR window: coldStartBeginCDC writes the persisted position on
//     an uncancellable context (the v0.116-era fix, c26708f4). A stop that
//     lands here still writes the anchor; a re-run WARM-RESUMES from the
//     kept slot and applies live changes.
//   - The PRE-ANCHOR window (index build, constraints, FLOAT re-read): the
//     v0.148.0 door abandonUnlessStopped keeps the slot, but no position has
//     been written. A re-run finds no position, cold-starts, and REFUSES on
//     the existing slot. The slot is kept for the handoff-resume path that
//     does not exist yet (audit 2026-09-09 A0909-STOP-1); until it ships the
//     way out is `slot drop` + `--reset-target-data`, and the STOPPED-SLOT-KEPT
//     WARN says so.
//
// The first cut of this pin (v0.148.0) asserted "resumes" for BOTH windows
// and passed only when its 1 ms poll happened to land the stop in the
// anchor window — a gate green by race, red on the v0.148.1 release commit,
// which is how the pre-anchor window's non-resumability was found. Each
// attempt now grades the window it actually landed in, decided by the one
// fact that separates them: whether the cdc-state row exists on the target.
//
// This also closes the long-running flake in
// TestStreamer_WarmResume_PG_FullSlots_NeverProbesRefuses, whose
// "walsender never released sluice_slot" failure was really "the slot no
// longer exists" — a wait that could never be satisfied, which is why two
// rounds of raising its timeout did not help.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

// coldStartStopAttempts is how many independent stop-at-copy-completion
// rounds the pin runs. The stop's landing point is timing-dependent, so a
// single round would cover one window at most; every round asserts the
// slot invariant and grades its own window. The deterministic counterpart
// — coldStartBeginCDC's dispatch under an already-cancelled ctx — is
// pinned as a unit test in streamer_coldstart_anchor_cancel_test.go.
const coldStartStopAttempts = 4

// pgCDCStateRowExists reports whether the target holds a persisted position
// for streamID — the fact that decides which post-copy window a stop landed
// in. A target with no control table at all has no row.
func pgCDCStateRowExists(t *testing.T, dsn, streamID string) bool {
	t.Helper()
	if !pgQueryOne[bool](t, dsn, "SELECT to_regclass('sluice_cdc_state') IS NOT NULL") {
		return false
	}
	return pgQueryOne[bool](t, dsn, "SELECT EXISTS (SELECT 1 FROM sluice_cdc_state WHERE stream_id = $1)", streamID)
}

// TestStreamer_ColdStartStopInHandoff_PG_KeepsSlotAndResumes stops a cold
// start the instant its copy lands (a 1ms poll, vs the 200ms poll other
// tests use, so the stop lands INSIDE the handoff rather than after it),
// requires that the slot survives wherever the stop landed, and then
// requires the honest behaviour of the window it landed in.
func TestStreamer_ColdStartStopInHandoff_PG_KeepsSlotAndResumes(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	const streamID = "coldstart-handoff-stop"
	interrupted := 0
	anchoredResumeProven := false
	preAnchorRefusalProven := false
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
					StreamID:  streamID,
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
			anchored := pgCDCStateRowExists(t, tgt, streamID)
			t.Logf("attempt %d: cold-start Run returned err=%v; anchor row on target=%v", attempt, runErr, anchored)
			// THE INVARIANT: the slot survives the stop, wherever in the
			// handoff the stop happened to land.
			if !pgSlotExists(t, src, "sluice_slot") {
				t.Fatalf("attempt %d: a stop during the cold-start handoff DROPPED sluice_slot; the "+
					"completed copy on the target is now unresumable (warm resume has no position, "+
					"cold start refuses on the populated target). Run error was: %v", attempt, runErr)
			}
			if runErr == nil {
				return // the stop landed after CDC was live; nothing to grade here
			}

			resumeCtx, resumeCancel := context.WithCancel(context.Background())
			defer resumeCancel()
			resumeErrCh := make(chan error, 1)
			go func() { resumeErrCh <- newStream().Run(resumeCtx) }()

			if !anchored {
				// PRE-ANCHOR window: the honest behaviour today is a LOUD
				// refusal naming the slot — not a resume, and not a silent
				// re-copy over the populated target either.
				select {
				case err := <-resumeErrCh:
					if err == nil || !strings.Contains(err.Error(), "sluice_slot") {
						t.Fatalf("attempt %d: re-run after a pre-anchor stop returned %v; want a loud refusal "+
							"naming sluice_slot (this release cannot resume a copy stopped before the anchor — "+
							"A0909-STOP-1)", attempt, err)
					}
					t.Logf("attempt %d: pre-anchor re-run refused as documented: %v", attempt, err)
					preAnchorRefusalProven = true
				case <-time.After(2 * time.Minute):
					t.Fatalf("attempt %d: re-run after a pre-anchor stop neither refused nor returned in 2m "+
						"(rows now %d); a copy without an anchor must not silently proceed", attempt, pollRowCount(tgt, "handoff_t"))
				}
				return
			}

			// ANCHOR window: the anchor was written (uncancellable), so a
			// warm resume must pick the slot up and apply a live change.
			applyDDL(t, src, `INSERT INTO handoff_t (id, v) VALUES (51, 51);`)
			if !waitForExactRowCount(tgt, "handoff_t", 51, 2*time.Minute) {
				select {
				case err := <-resumeErrCh:
					t.Fatalf("attempt %d: warm resume after the anchored stop exited: %v", attempt, err)
				default:
					t.Fatalf("attempt %d: warm resume after the anchored stop never applied the "+
						"live change (got %d rows)", attempt, pollRowCount(tgt, "handoff_t"))
				}
			}
			resumeCancel()
			select {
			case <-resumeErrCh:
			case <-time.After(30 * time.Second):
				t.Fatalf("attempt %d: warm-resume Run did not return after ctx cancel", attempt)
			}
			anchoredResumeProven = true
		}()
	}

	// Vacuous-green guard: a run in which every stop landed after CDC was
	// already live never entered the window this pin exists to cover.
	if interrupted == 0 {
		t.Fatalf("no attempt out of %d stopped inside the cold-start handoff window; the pin never "+
			"exercised what it exists to cover (widen the window or revisit the poll)",
			coldStartStopAttempts)
	}
	if !anchoredResumeProven && !preAnchorRefusalProven {
		t.Fatal("an interrupted handoff was observed but neither window's behaviour was graded")
	}
	t.Logf("windows graded: anchored-resume=%v pre-anchor-refusal=%v", anchoredResumeProven, preAnchorRefusalProven)
}
