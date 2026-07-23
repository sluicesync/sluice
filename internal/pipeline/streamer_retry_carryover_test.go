// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 202 pins: the apply-retry consecutive-failure budget must NOT carry
// over across ridden-out outages when the position store (the TARGET) is
// unreachable at the next failure. The only pre-fix reset path was
// `progressed` — both boundary position reads succeeding AND the token
// advancing — but a target outage fails the after-read by construction, so
// progress applied BETWEEN outages was never credited precisely in the
// outage class ride-out exists for: outage #1 burned attempts 1..k, the
// stream converged in-process and applied hours of CDC, and outage #2's
// FIRST failure inherited k and exited "apply retry budget exhausted".
//
// The fix is an in-memory progress ledger at the retry-loop layer: every
// SUCCESSFUL anchor-bearing position read — the boundary reads plus a
// mid-attempt sentinel poll — is compared against the token last observed at
// the previous counted failure; a different token proves durable commits
// happened since (position writes ride the batch tx, ADR-0007) and grants a
// fresh budget. A FAILED read stays non-evidence in both directions (the
// D0-4 discipline), so the two loud-failure floors pinned below survive: a
// genuinely stuck batch (token never advances) and a never-reachable target
// (no successful reads at all) both still exhaust in exactly `attempts`.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// flakyPosStore is a mutex-guarded position store whose availability (down ⇒
// reads FAIL, modelling a target outage) and persisted token are driven by
// the test's runOnceFn state machine. Mutex-guarded because the mid-attempt
// progress sentinel reads it concurrently with the attempt mutating it.
type flakyPosStore struct {
	mu    sync.Mutex
	token string
	found bool
	down  bool
}

func (s *flakyPosStore) set(down bool, token string, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down, s.token, s.found = down, token, found
}

func (s *flakyPosStore) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		// Both engines surface a failed read as (zero, false, err) — the
		// found=false accompanying an error is exactly the D0-4 trap shape.
		return ir.Position{}, false, errors.New("read position: dial tcp 127.0.0.1:5432: connect: connection refused")
	}
	return ir.Position{Engine: "postgres", Token: s.token}, s.found, nil
}

func (*flakyPosStore) EnsureControlTable(context.Context) error               { return nil }
func (*flakyPosStore) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (*flakyPosStore) RequestStop(context.Context, string) error              { return nil }
func (*flakyPosStore) ClearStopRequested(context.Context, string) error       { return nil }
func (*flakyPosStore) Apply(context.Context, string, <-chan ir.Change) error  { return nil }

// flakyPosEngine hands runWithRetry the flaky store as its side-channel
// position reader; every other engine surface is unused by the retry seam.
type flakyPosEngine struct {
	scriptedPosEngine
	store *flakyPosStore
}

func (e flakyPosEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return e.store, nil
}

// carryoverStreamer wires runWithRetry onto the flaky store with negligible
// backoff and a fast mid-attempt sentinel cadence.
func carryoverStreamer(store *flakyPosStore, attempts int) *Streamer {
	return &Streamer{
		StreamID:              "test-stream",
		Target:                flakyPosEngine{store: store},
		TargetDSN:             "tgt",
		ApplyRetryAttempts:    attempts,
		ApplyRetryBackoffBase: 1, // 1ns → effectively no wait
		ApplyRetryBackoffCap:  1,
		// Fast sentinel so a short simulated "healthy stretch" inside one
		// attempt is reliably observed (production default: 5s).
		applyProgressPollInterval: 2 * time.Millisecond,
	}
}

// TestRunWithRetry_BudgetResetsAcrossRiddenOutOutages is the Bug 202 core
// pin (RED pre-fix). Sequence, mirroring bugA_carryover.sh:
//
//	outage #1: attempts 1 and 2 fail with the store down (reads fail);
//	recovery: attempt 3's runOnce brings the store back, "applies" (the
//	  persisted token advances t0→t1, observed only by the mid-attempt
//	  sentinel — the boundary before-read ran while the store was still
//	  down), then outage #2 hits and the attempt fails with the store down.
//
// With attempts=3, pre-fix the third failure inherited the count (3 >= 3 →
// budget exhausted despite verified progress in between). Post-fix the
// ledger credits the observed t0→t1 advance, grants a fresh budget, and the
// fourth attempt succeeds.
func TestRunWithRetry_BudgetResetsAcrossRiddenOutOutages(t *testing.T) {
	store := &flakyPosStore{token: "t0", found: true} // up at Run start
	s := carryoverStreamer(store, 3)

	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		switch calls {
		case 1:
			// Outage #1 begins mid-attempt: the store goes down, the apply
			// fails retriable. (This attempt's before-read was UP and
			// observed t0 — the ledger baseline.)
			store.set(true, "t0", true)
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		case 2:
			// Outage #1 persists: reads down, apply fails again.
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		case 3:
			// Target recovered mid-attempt; the stream applies real progress
			// (persisted token advances). Only the sentinel can see it: this
			// attempt's boundary before-read already failed (store was down),
			// and the after-read will fail (outage #2). Then outage #2 hits.
			store.set(false, "t1", true)
			time.Sleep(60 * time.Millisecond) // several sentinel polls at 2ms
			store.set(true, "t1", true)
			return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
		default:
			// Outage #2 rode out on the fresh budget.
			return nil
		}
	}

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v; want nil — the second outage's first failure exhausted a budget that verified progress should have reset (Bug 202 carry-over)", err)
	}
	if calls != 4 {
		t.Fatalf("runOnce called %d times; want 4 (three failures + the recovery attempt)", calls)
	}
}

// TestRunWithRetry_StuckBatchStillExhaustsBudget pins loud-failure floor (b):
// a genuinely stuck batch — the target is UP, reads succeed, but the
// persisted token NEVER advances between failures — must still exhaust the
// budget in exactly `attempts`. The ledger must credit only an OBSERVED
// token advance, never mere target reachability or elapsed attempt time.
func TestRunWithRetry_StuckBatchStillExhaustsBudget(t *testing.T) {
	store := &flakyPosStore{token: "t0", found: true} // up, token frozen
	s := carryoverStreamer(store, 3)

	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		// Long enough that the sentinel observes the (unchanged) token every
		// attempt — reachability alone must not reset the counter.
		time.Sleep(10 * time.Millisecond)
		return &retriableWrapper{err: errors.New("postgres: applier: exec: deadlock detected")}
	}

	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run = nil; want budget exhaustion — a stuck batch with a frozen position must stay loud (the fix must not turn the budget into never-exhausting)")
	}
	if !strings.Contains(err.Error(), "apply retry budget exhausted after 3") {
		t.Fatalf("Run error = %v; want the 3-attempt budget-exhausted shape", err)
	}
	if calls != 3 {
		t.Fatalf("runOnce called %d times; want exactly 3", calls)
	}
}

// TestRunWithRetry_TargetDownThroughoutStillExhaustsBudget pins loud-failure
// floor (a): a target that never comes back — every read fails, so the
// ledger accumulates NO evidence — exhausts the budget in exactly `attempts`
// (the ADR-0038 bound; a failed read proves nothing in either direction).
func TestRunWithRetry_TargetDownThroughoutStillExhaustsBudget(t *testing.T) {
	store := &flakyPosStore{down: true}
	s := carryoverStreamer(store, 3)

	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		time.Sleep(10 * time.Millisecond) // sentinel polls fail throughout
		return &retriableWrapper{err: errors.New("postgres: applier: exec: connection refused")}
	}

	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run = nil; want budget exhaustion — an unreachable target must stay loud")
	}
	if !strings.Contains(err.Error(), "apply retry budget exhausted after 3") {
		t.Fatalf("Run error = %v; want the 3-attempt budget-exhausted shape", err)
	}
	if calls != 3 {
		t.Fatalf("runOnce called %d times; want exactly 3", calls)
	}
}
