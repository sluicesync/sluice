// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// trackRunner is a SyncRunner that records how many times it was started
// and whether it is currently running, blocking until its ctx is
// cancelled. It lets the reconcile tests assert add/remove/restart/no-op
// purely (no real pipeline boot, no signals — so they run on every OS).
type trackRunner struct {
	starts  atomic.Int64
	running atomic.Bool
}

func (r *trackRunner) Run(ctx context.Context) error {
	r.starts.Add(1)
	r.running.Store(true)
	defer r.running.Store(false)
	<-ctx.Done()
	return ctx.Err()
}

// waitRunning polls a trackRunner until its running flag matches want, or
// the deadline fires.
func waitRunning(t *testing.T, r *trackRunner, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.running.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("trackRunner running=%v; want %v (starts=%d)", r.running.Load(), want, r.starts.Load())
}

// startSupervisor runs sup in a background goroutine under a cancellable
// ctx and returns the cancel + a channel carrying Run's return value.
func startSupervisor(t *testing.T, sup *Supervisor) (cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- sup.Run(ctx) }()
	return cancel, ch
}

// TestSupervisor_ReconcileAddRemoveChangeNoop pins the four diff
// branches: a new sync is STARTED, a dropped sync is STOPPED (graceful
// drain), a sync whose fingerprint changed is RESTARTED, and an unchanged
// sync is left untouched (NOT restarted).
func TestSupervisor_ReconcileAddRemoveChangeNoop(t *testing.T) {
	a, b, c := &trackRunner{}, &trackRunner{}, &trackRunner{}
	sup := NewSupervisor([]SupervisedSync{
		{ID: "a", Runner: a, Fingerprint: "a1"},
		{ID: "b", Runner: b, Fingerprint: "b1"},
	}, fastTestPolicy(0))
	cancel, done := startSupervisor(t, sup)
	defer func() { cancel(); <-done }()

	waitForState(t, sup, "a", SyncRunning, 5*time.Second)
	waitForState(t, sup, "b", SyncRunning, 5*time.Second)

	// Reconcile: keep a (same fp → no-op), add c (new), drop b (stop).
	res, err := sup.Reconcile([]SupervisedSync{
		{ID: "a", Runner: a, Fingerprint: "a1"},
		{ID: "c", Runner: c, Fingerprint: "c1"},
	})
	if err != nil {
		t.Fatalf("Reconcile = %v; want nil", err)
	}
	if got := res.Started; len(got) != 1 || got[0] != "c" {
		t.Errorf("Started = %v; want [c]", got)
	}
	if got := res.Stopped; len(got) != 1 || got[0] != "b" {
		t.Errorf("Stopped = %v; want [b]", got)
	}
	if got := res.Unchanged; len(got) != 1 || got[0] != "a" {
		t.Errorf("Unchanged = %v; want [a]", got)
	}
	if len(res.Restarted) != 0 {
		t.Errorf("Restarted = %v; want []", res.Restarted)
	}

	// b drained and is gone from the fleet view; a untouched (started once,
	// still running); c started and running.
	waitRunning(t, b, false, 5*time.Second)
	if _, ok := snapshotFor(sup, "b"); ok {
		t.Error("removed sync \"b\" still present in snapshot; want dropped")
	}
	waitForState(t, sup, "c", SyncRunning, 5*time.Second)
	waitRunning(t, c, true, 5*time.Second)
	if got := a.starts.Load(); got != 1 {
		t.Errorf("unchanged sync \"a\" started %d times; want 1 (not restarted)", got)
	}
	if !a.running.Load() {
		t.Error("unchanged sync \"a\" no longer running; want untouched")
	}

	// Now CHANGE a's fingerprint → it must be stopped and restarted.
	res, err = sup.Reconcile([]SupervisedSync{
		{ID: "a", Runner: a, Fingerprint: "a2"},
		{ID: "c", Runner: c, Fingerprint: "c1"},
	})
	if err != nil {
		t.Fatalf("Reconcile (change) = %v; want nil", err)
	}
	if got := res.Restarted; len(got) != 1 || got[0] != "a" {
		t.Errorf("Restarted = %v; want [a]", got)
	}
	waitForState(t, sup, "a", SyncRunning, 5*time.Second)
	if got := a.starts.Load(); got != 2 {
		t.Errorf("changed sync \"a\" started %d times; want 2 (restarted)", got)
	}
	if got := c.starts.Load(); got != 1 {
		t.Errorf("unchanged sync \"c\" started %d times; want 1 (not restarted)", got)
	}

	// Idempotent reconcile: nothing changes.
	res, err = sup.Reconcile([]SupervisedSync{
		{ID: "a", Runner: a, Fingerprint: "a2"},
		{ID: "c", Runner: c, Fingerprint: "c1"},
	})
	if err != nil {
		t.Fatalf("Reconcile (noop) = %v; want nil", err)
	}
	if res.changed() {
		t.Errorf("idempotent reconcile reported changes: %+v", res)
	}
	if len(res.Unchanged) != 2 {
		t.Errorf("Unchanged = %v; want both", res.Unchanged)
	}
}

// TestSupervisor_ReconcileRejectsBadReloadKeepsFleetRunning is THE
// load-bearing pin (ADR-0122 §3): a Reconcile with an invalid new set
// (here, a duplicate stream-id) returns an error and leaves the live
// fleet running EXACTLY as it was — not a single sync stopped, restarted,
// or started. A malformed reloaded config must never take down or corrupt
// the live fleet.
func TestSupervisor_ReconcileRejectsBadReloadKeepsFleetRunning(t *testing.T) {
	a, b := &trackRunner{}, &trackRunner{}
	sup := NewSupervisor([]SupervisedSync{
		{ID: "a", Runner: a, Fingerprint: "a1"},
		{ID: "b", Runner: b, Fingerprint: "b1"},
	}, fastTestPolicy(0))
	cancel, done := startSupervisor(t, sup)
	defer func() { cancel(); <-done }()

	waitForState(t, sup, "a", SyncRunning, 5*time.Second)
	waitForState(t, sup, "b", SyncRunning, 5*time.Second)

	bad := &trackRunner{}
	// The new set is invalid: stream-id "a" appears twice. (This is the
	// supervisor's own defense-in-depth invariant; the CLI also re-runs the
	// full slot-name + stream-id config validators before calling here.)
	_, err := sup.Reconcile([]SupervisedSync{
		{ID: "a", Runner: bad, Fingerprint: "a-new"},
		{ID: "a", Runner: bad, Fingerprint: "a-dup"},
		{ID: "z", Runner: bad, Fingerprint: "z1"},
	})
	if err == nil {
		t.Fatal("Reconcile with a duplicate stream-id = nil; want a loud refusal")
	}

	// The live fleet is UNCHANGED: a and b each started exactly once and
	// are still running; the bad set's "z" was never started; the bad
	// runner was never invoked.
	if got := a.starts.Load(); got != 1 || !a.running.Load() {
		t.Errorf("sync \"a\" disturbed by bad reload: starts=%d running=%v; want 1/true", got, a.running.Load())
	}
	if got := b.starts.Load(); got != 1 || !b.running.Load() {
		t.Errorf("sync \"b\" disturbed by bad reload: starts=%d running=%v; want 1/true", got, b.running.Load())
	}
	if got := bad.starts.Load(); got != 0 {
		t.Errorf("rejected set's runner started %d times; want 0 (nothing applied)", got)
	}
	if _, ok := snapshotFor(sup, "z"); ok {
		t.Error("rejected set's sync \"z\" leaked into the live fleet")
	}
	// Snapshot still shows exactly the original two syncs running.
	snaps := sup.Snapshot()
	if len(snaps) != 2 {
		t.Fatalf("fleet has %d syncs after a rejected reload; want the original 2 (%+v)", len(snaps), snaps)
	}

	// And a SUBSEQUENT good reload still works — the supervisor wasn't
	// wedged by the refusal.
	if _, err := sup.Reconcile([]SupervisedSync{
		{ID: "a", Runner: a, Fingerprint: "a1"},
		{ID: "b", Runner: b, Fingerprint: "b1"},
	}); err != nil {
		t.Fatalf("good reload after a refusal = %v; want nil (supervisor not wedged)", err)
	}
}

// TestSupervisor_ReconcileEmptyStreamIDRejected pins the other invalid-set
// shape (an empty stream-id) is refused without mutating the live fleet.
func TestSupervisor_ReconcileEmptyStreamIDRejected(t *testing.T) {
	a := &trackRunner{}
	sup := NewSupervisor([]SupervisedSync{{ID: "a", Runner: a, Fingerprint: "a1"}}, fastTestPolicy(0))
	cancel, done := startSupervisor(t, sup)
	defer func() { cancel(); <-done }()
	waitForState(t, sup, "a", SyncRunning, 5*time.Second)

	if _, err := sup.Reconcile([]SupervisedSync{{ID: "", Runner: &trackRunner{}}}); err == nil {
		t.Fatal("Reconcile with an empty stream-id = nil; want a refusal")
	}
	if got := a.starts.Load(); got != 1 || !a.running.Load() {
		t.Errorf("live sync disturbed by rejected reload: starts=%d running=%v", got, a.running.Load())
	}
}

// TestSupervisor_ReconcileBeforeRun pins that Reconcile refuses before the
// supervisor's Run has set the fleet context (nothing to reconcile yet).
func TestSupervisor_ReconcileBeforeRun(t *testing.T) {
	sup := NewSupervisor([]SupervisedSync{{ID: "a", Runner: &trackRunner{}, Fingerprint: "a1"}}, fastTestPolicy(0))
	if _, err := sup.Reconcile([]SupervisedSync{{ID: "a", Runner: &trackRunner{}, Fingerprint: "a1"}}); err == nil {
		t.Fatal("Reconcile before Run = nil; want an error")
	}
}

// TestSupervisor_ReconcileGracefulStopWaitsForDrain pins that removing a
// sync drains it cleanly — its goroutine has fully exited (running flag
// false) by the time Reconcile returns, so no half-dead stream leaks and
// a restarted PG sync's predecessor has released its slot before the
// replacement starts.
func TestSupervisor_ReconcileGracefulStopWaitsForDrain(t *testing.T) {
	a, b := &trackRunner{}, &trackRunner{}
	sup := NewSupervisor([]SupervisedSync{
		{ID: "a", Runner: a, Fingerprint: "a1"},
		{ID: "b", Runner: b, Fingerprint: "b1"},
	}, fastTestPolicy(0))
	cancel, done := startSupervisor(t, sup)
	defer func() { cancel(); <-done }()
	waitForState(t, sup, "a", SyncRunning, 5*time.Second)
	waitForState(t, sup, "b", SyncRunning, 5*time.Second)

	if _, err := sup.Reconcile([]SupervisedSync{{ID: "a", Runner: a, Fingerprint: "a1"}}); err != nil {
		t.Fatalf("Reconcile = %v; want nil", err)
	}
	// Synchronously drained: Reconcile does not return until the removed
	// sync's goroutine has exited.
	if b.running.Load() {
		t.Error("removed sync \"b\" still running when Reconcile returned; want fully drained")
	}
	if a.starts.Load() != 1 || !a.running.Load() {
		t.Error("peer \"a\" disturbed by the removal; want untouched")
	}
}
