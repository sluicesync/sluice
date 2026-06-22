// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withFastGrowGate shrinks the gate's time envelope to near-zero for the
// duration of a test so the FSM cycles fast and deterministically, while
// the STRUCTURE (open/closed transitions, coalescing, max-hold bound) is
// exactly as production. Restores the production values on cleanup. Mirrors
// withFastSourceReadBackoff.
func withFastGrowGate(t *testing.T) {
	t.Helper()
	base, capDur, maxHold := growGateBackoffBase, growGateBackoffCap, growGateMaxHold
	growGateBackoffBase = time.Millisecond
	growGateBackoffCap = time.Millisecond
	growGateMaxHold = 5 * time.Second
	t.Cleanup(func() {
		growGateBackoffBase = base
		growGateBackoffCap = capDur
		growGateMaxHold = maxHold
	})
}

// TestGrowGate_OpenAwaitIsInstant pins the hot-path fast read: an
// un-tripped gate returns from Await immediately (no block).
func TestGrowGate_OpenAwaitIsInstant(t *testing.T) {
	g := newGrowGate(context.Background(), nil)
	done := make(chan error, 1)
	go func() { done <- g.Await(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("open Await returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("open Await blocked; want an instant return")
	}
}

// TestGrowGate_NilGateHelpersArePreADRNoOps pins the byte-for-byte
// pre-ADR-0110 behaviour: a nil ir.GrowGate (via the package helpers) makes
// Await an instant nil and Trip a no-op — no panic, no block.
func TestGrowGate_NilGateHelpersArePreADRNoOps(t *testing.T) {
	if err := awaitGrowGate(context.Background(), nil); err != nil {
		t.Fatalf("awaitGrowGate(nil) = %v, want nil", err)
	}
	tripGrowGate(nil, "no-op") // must not panic
	// growGateOrNil(nil) must be a TRUE nil interface (no typed-nil trap).
	if got := growGateOrNil(nil); got != nil {
		t.Fatal("growGateOrNil(nil) must be a true nil interface")
	}
	// And a non-nil concrete gate round-trips to a non-nil interface.
	if got := growGateOrNil(newGrowGate(context.Background(), nil)); got == nil {
		t.Fatal("growGateOrNil(non-nil) must yield a non-nil interface")
	}
}

// TestGrowGate_TripPausesThenReopens pins the core close→reopen cycle: a
// Trip closes the gate (Await blocks), and once the quiet backoff cycle
// elapses the gate reopens and the parked Await returns nil.
func TestGrowGate_TripPausesThenReopens(t *testing.T) {
	captureSlog(t)
	withFastGrowGate(t)

	g := newGrowGate(context.Background(), nil)
	g.Trip("storage grow")

	done := make(chan error, 1)
	go func() { done <- g.Await(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Await after reopen returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gate never reopened: parked Await did not return")
	}
}

// TestGrowGate_ReopenWakesAllParkedAwaiters pins the closed-channel
// broadcast: N goroutines parked on one pause window all unblock when the
// gate reopens (no per-waiter signal lost).
func TestGrowGate_ReopenWakesAllParkedAwaiters(t *testing.T) {
	captureSlog(t)
	withFastGrowGate(t)

	g := newGrowGate(context.Background(), nil)
	g.Trip("grow")

	const n = 32
	var wg sync.WaitGroup
	var failed atomic.Int32
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			if err := g.Await(context.Background()); err != nil {
				failed.Add(1)
			}
		}()
	}

	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()
	select {
	case <-allDone:
		if f := failed.Load(); f != 0 {
			t.Fatalf("%d/%d parked Awaiters returned an error; want all nil", f, n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("not all parked Awaiters woke on reopen (broadcast lost a waiter)")
	}
}

// TestGrowGate_ConcurrentTripsCoalesceOneWindow pins the coalescing
// contract: many concurrent Trips (the ~W×D lanes hitting the transient at
// once + the telemetry sidecar) collapse into ONE pause window served by a
// SINGLE owner goroutine — not N windows / N owners. We assert exactly one
// reopen broadcast fires for the burst by counting how many distinct
// reopenCh closes a long-lived Awaiter observes via the owner-count seam.
func TestGrowGate_ConcurrentTripsCoalesceOneWindow(t *testing.T) {
	captureSlog(t)
	withFastGrowGate(t)
	// Hold long enough that the whole concurrent burst lands while closed.
	growGateBackoffBase = 50 * time.Millisecond
	growGateBackoffCap = 50 * time.Millisecond

	g := newGrowGate(context.Background(), nil)
	var owners atomic.Int32
	g.onOwnerStart = func() { owners.Add(1) }

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			g.Trip("lane trip")
			_ = i
		}(i)
	}
	wg.Wait()

	// Let the (single) window run to completion.
	if err := g.Await(context.Background()); err != nil {
		t.Fatalf("Await returned %v", err)
	}
	if got := owners.Load(); got != 1 {
		t.Fatalf("owner goroutines spawned = %d, want 1 (the concurrent burst must coalesce into one window)", got)
	}
}

// TestGrowGate_CtxCancelUnwindsAllParked is the ADR-0099 shutdown-hang
// lesson, applied to the gate: park N Awaiters on a pause window, then
// cancel the AWAITERS' ctx — every one must return ctx.Err() PROMPTLY (no
// hang), proving Await always selects on ctx.Done(). This is the
// load-bearing no-deadlock contract.
func TestGrowGate_CtxCancelUnwindsAllParked(t *testing.T) {
	captureSlog(t)
	// A long hold so the gate stays closed for the whole test — the only
	// thing that should release the Awaiters is the ctx cancel.
	base, capDur, maxHold := growGateBackoffBase, growGateBackoffCap, growGateMaxHold
	growGateBackoffBase = time.Hour
	growGateBackoffCap = time.Hour
	growGateMaxHold = time.Hour
	t.Cleanup(func() {
		growGateBackoffBase = base
		growGateBackoffCap = capDur
		growGateMaxHold = maxHold
	})

	g := newGrowGate(context.Background(), nil)
	g.Trip("grow that won't lift on its own")

	ctx, cancel := context.WithCancel(context.Background())
	const n = 16
	done := make(chan error, n)
	for range n {
		go func() { done <- g.Await(ctx) }()
	}
	// Give them a beat to park, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("parked Await returned %v, want context.Canceled", err)
			}
		case <-deadline:
			t.Fatalf("only %d/%d parked Awaiters unwound on ctx cancel; the gate HUNG", i, n)
		}
	}
}

// TestGrowGate_OwnerExitsOnRunCtxCancel pins that the owner goroutine
// exits (no leak) when the RUN ctx — the one the gate was constructed with —
// is cancelled mid-pause, and that it reopens the gate on its way out so a
// subsequently-arriving Awaiter doesn't hang.
func TestGrowGate_OwnerExitsOnRunCtxCancel(t *testing.T) {
	captureSlog(t)
	// Long holds so only the run-ctx cancel can end the window.
	base, capDur, maxHold := growGateBackoffBase, growGateBackoffCap, growGateMaxHold
	growGateBackoffBase = time.Hour
	growGateBackoffCap = time.Hour
	growGateMaxHold = time.Hour
	t.Cleanup(func() {
		growGateBackoffBase = base
		growGateBackoffCap = capDur
		growGateMaxHold = maxHold
	})

	runCtx, cancelRun := context.WithCancel(context.Background())
	g := newGrowGate(runCtx, nil)
	g.Trip("grow")

	// Park one Awaiter on its own ctx (not the run ctx) so the ONLY thing
	// that can release it is the owner reopening the gate on run-ctx cancel.
	done := make(chan error, 1)
	go func() { done <- g.Await(context.Background()) }()
	time.Sleep(20 * time.Millisecond)

	cancelRun()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Await returned %v; want nil (owner reopened on run-ctx cancel)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("owner did not reopen the gate on run-ctx cancel: Awaiter hung")
	}
}

// TestGrowGate_MaxHoldBoundsAGenuinelyDeadTarget pins the loud-floor
// guarantee: a target that NEVER recovers (lanes keep re-tripping) does not
// pause forever — the gate force-reopens at max-hold so each lane's own
// bounded retry budget surfaces the dead target loudly. We re-trip on a
// timer faster than the backoff cycle, set a tiny max-hold, and assert the
// gate reopens within a bounded multiple of it.
func TestGrowGate_MaxHoldBoundsAGenuinelyDeadTarget(t *testing.T) {
	captureSlog(t)
	base, capDur, maxHold := growGateBackoffBase, growGateBackoffCap, growGateMaxHold
	growGateBackoffBase = time.Millisecond
	growGateBackoffCap = time.Millisecond
	growGateMaxHold = 50 * time.Millisecond
	t.Cleanup(func() {
		growGateBackoffBase = base
		growGateBackoffCap = capDur
		growGateMaxHold = maxHold
	})

	g := newGrowGate(context.Background(), nil)
	g.Trip("dead target")

	// Hammer re-trips so the window keeps trying to extend; max-hold must
	// still force a reopen.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				g.Trip("still dead")
			}
		}
	}()
	defer close(stop)

	done := make(chan error, 1)
	go func() { done <- g.Await(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Await returned %v, want nil at max-hold reopen", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("max-hold did not bound a forever-re-tripping window: the gate hung past max-hold")
	}
}

// TestGrowGate_ProactiveReleasesOnRecovery pins the telemetry path: a
// proactive pause (recovered probe non-nil) reopens as soon as recovered()
// reports headroom restored, EARLIER than its (long) max-hold, even with no
// lane error.
func TestGrowGate_ProactiveReleasesOnRecovery(t *testing.T) {
	captureSlog(t)
	base, capDur, maxHold := growGateBackoffBase, growGateBackoffCap, growGateMaxHold
	growGateBackoffBase = 5 * time.Millisecond
	growGateBackoffCap = 5 * time.Millisecond
	growGateMaxHold = time.Hour // far away — recovery must be what reopens
	t.Cleanup(func() {
		growGateBackoffBase = base
		growGateBackoffCap = capDur
		growGateMaxHold = maxHold
	})

	var healthy atomic.Bool // starts false (not recovered)
	g := newGrowGate(context.Background(), healthy.Load)
	g.Trip("proactive: storage near boundary")

	// Flip to recovered shortly after the pause begins.
	go func() {
		time.Sleep(30 * time.Millisecond)
		healthy.Store(true)
	}()

	done := make(chan error, 1)
	go func() { done <- g.Await(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Await returned %v, want nil on recovery release", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proactive pause never released on recovery (it should reopen the moment headroom recovers)")
	}
}

// TestGrowGate_BackoffShape pins the exponential-doubling envelope (same
// shape as ADR-0108/0109): 100ms base doubling to the 30s cap.
func TestGrowGate_BackoffShape(t *testing.T) {
	// Drive the per-instance backoff method directly (the production default
	// envelope, snapshotted at construction) — no package-global mutation,
	// so nothing can race a still-running owner from another test.
	g := newGrowGate(context.Background(), nil)
	g.backoffBase = 100 * time.Millisecond
	g.backoffCap = 30 * time.Second

	want := []time.Duration{
		100 * time.Millisecond,  // cycle 1
		200 * time.Millisecond,  // 2
		400 * time.Millisecond,  // 3
		800 * time.Millisecond,  // 4
		1600 * time.Millisecond, // 5
		3200 * time.Millisecond, // 6
		6400 * time.Millisecond, // 7
		12800 * time.Millisecond,
		25600 * time.Millisecond,
		30 * time.Second, // 10: capped
		30 * time.Second, // 11: stays capped
	}
	for i, w := range want {
		if got := g.backoff(i + 1); got != w {
			t.Errorf("g.backoff(%d) = %v, want %v", i+1, got, w)
		}
	}
}

// TestGrowGate_ReTripAfterReopenStartsFreshWindow pins that once a window
// has ended (gate reopened, owner exited), a NEW Trip starts a fresh window
// with its own owner — the "reopen lets lanes probe; if still bad the first
// re-trip opens a new window" loop, observed as a second owner.
func TestGrowGate_ReTripAfterReopenStartsFreshWindow(t *testing.T) {
	captureSlog(t)
	withFastGrowGate(t)

	g := newGrowGate(context.Background(), nil)
	var owners atomic.Int32
	g.onOwnerStart = func() { owners.Add(1) }

	// First window.
	g.Trip("grow step 1")
	if err := g.Await(context.Background()); err != nil {
		t.Fatalf("Await(1) = %v", err)
	}
	// A small settle so the first owner fully tears down (extend nulled).
	time.Sleep(20 * time.Millisecond)
	// Second window (target still bad after probe).
	g.Trip("grow step 2")
	if err := g.Await(context.Background()); err != nil {
		t.Fatalf("Await(2) = %v", err)
	}
	// Allow the second owner to start counting.
	time.Sleep(20 * time.Millisecond)
	if got := owners.Load(); got != 2 {
		t.Fatalf("owner goroutines = %d, want 2 (a re-trip after a window ends opens a fresh window)", got)
	}
}
