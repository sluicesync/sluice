// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// runnerFunc adapts a func to the SyncRunner interface.
type runnerFunc func(ctx context.Context) error

func (f runnerFunc) Run(ctx context.Context) error { return f(ctx) }

// fastTestPolicy is a tiny-backoff policy so the failure-isolation
// tests don't sleep for seconds. MaxConsecutiveFailures bounds the
// failing sync so the test is deterministic.
func fastTestPolicy(maxFailures int) RestartPolicy {
	return RestartPolicy{
		BackoffBase:            time.Millisecond,
		BackoffCap:             2 * time.Millisecond,
		HealthyRunThreshold:    time.Hour, // never reset within a test
		MaxConsecutiveFailures: maxFailures,
	}
}

// waitForState polls the supervisor snapshot until the named sync
// reaches want, or the deadline fires. Returns the final snapshot for
// the sync (zero value if never seen).
func waitForState(t *testing.T, sup *Supervisor, id string, want SyncState, timeout time.Duration) SyncStatusSnapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, snap := range sup.Snapshot() {
			if snap.ID == id && snap.State == want {
				return snap
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("sync %q never reached state %q within %s; snapshot=%+v", id, want, timeout, sup.Snapshot())
	return SyncStatusSnapshot{}
}

func snapshotFor(sup *Supervisor, id string) (SyncStatusSnapshot, bool) {
	for _, snap := range sup.Snapshot() {
		if snap.ID == id {
			return snap, true
		}
	}
	return SyncStatusSnapshot{}, false
}

// TestSupervisor_FailureIsolation is THE load-bearing pin (ADR-0122 §1,
// roadmap item 47 gotcha 1): a supervisor with two syncs where one is
// configured to fail repeatedly must keep the other running. The failing
// sync hits the MaxConsecutiveFailures cap and is isolated to the
// `failed` state; the healthy sync stays `running` throughout.
func TestSupervisor_FailureIsolation(t *testing.T) {
	var failCalls atomic.Int64
	failing := SupervisedSync{
		ID: "failing",
		Runner: runnerFunc(func(_ context.Context) error {
			failCalls.Add(1)
			return errors.New("boom: bad DSN")
		}),
	}

	healthyRunning := make(chan struct{}, 1)
	healthy := SupervisedSync{
		ID: "healthy",
		Runner: runnerFunc(func(ctx context.Context) error {
			select {
			case healthyRunning <- struct{}{}:
			default:
			}
			<-ctx.Done() // run until the fleet is cancelled
			return ctx.Err()
		}),
	}

	sup := NewSupervisor([]SupervisedSync{failing, healthy}, fastTestPolicy(3))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	// The healthy sync must actually be running.
	select {
	case <-healthyRunning:
	case <-time.After(5 * time.Second):
		t.Fatal("healthy sync never started")
	}

	// The failing sync must give up after exactly MaxConsecutiveFailures.
	failed := waitForState(t, sup, "failing", SyncFailed, 5*time.Second)
	if failed.ConsecutiveFailures != 3 {
		t.Errorf("failing sync ConsecutiveFailures = %d; want 3", failed.ConsecutiveFailures)
	}
	if got := failCalls.Load(); got != 3 {
		t.Errorf("failing runner invoked %d times; want 3", got)
	}

	// THE isolation assertion: the healthy sync is unaffected — still
	// running while its peer is permanently failed.
	if hsnap, ok := snapshotFor(sup, "healthy"); !ok || hsnap.State != SyncRunning {
		t.Fatalf("healthy sync state = %q (ok=%v); want %q — peer failure was NOT isolated", hsnap.State, ok, SyncRunning)
	}

	cancel()
	select {
	case err := <-done:
		// ctx-cancelled fleet returns nil even though a peer failed.
		if err != nil {
			t.Errorf("Supervisor.Run returned %v; want nil on ctx cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Supervisor.Run did not return after cancel")
	}

	// The failing runner must not have been hammered after it was
	// isolated (no hot-loop, no further restarts).
	if got := failCalls.Load(); got != 3 {
		t.Errorf("failing runner invoked %d times after isolation; want 3 (no hot-loop)", got)
	}
}

// TestSupervisor_PanicIsolation pins that a panicking sync is recovered
// and isolated — it must never crash the process or take down a peer.
func TestSupervisor_PanicIsolation(t *testing.T) {
	panicking := SupervisedSync{
		ID: "panicking",
		Runner: runnerFunc(func(_ context.Context) error {
			panic("kaboom")
		}),
	}
	healthyRunning := make(chan struct{}, 1)
	healthy := SupervisedSync{
		ID: "healthy",
		Runner: runnerFunc(func(ctx context.Context) error {
			select {
			case healthyRunning <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}),
	}

	sup := NewSupervisor([]SupervisedSync{panicking, healthy}, fastTestPolicy(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	select {
	case <-healthyRunning:
	case <-time.After(5 * time.Second):
		t.Fatal("healthy sync never started")
	}

	failed := waitForState(t, sup, "panicking", SyncFailed, 5*time.Second)
	if failed.LastError == "" {
		t.Error("panicking sync LastError is empty; want the recovered panic value")
	}
	if hsnap, ok := snapshotFor(sup, "healthy"); !ok || hsnap.State != SyncRunning {
		t.Fatalf("healthy sync state = %q; want running — panic was NOT isolated", hsnap.State)
	}

	cancel()
	<-done
}

// TestSupervisor_CleanDrainNotRestarted pins that a runner returning nil
// (a graceful drain via `sync stop`) is marked stopped and NOT restarted.
func TestSupervisor_CleanDrainNotRestarted(t *testing.T) {
	var calls atomic.Int64
	draining := SupervisedSync{
		ID: "draining",
		Runner: runnerFunc(func(_ context.Context) error {
			calls.Add(1)
			return nil // graceful drain
		}),
	}
	sup := NewSupervisor([]SupervisedSync{draining}, fastTestPolicy(0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sup.Run(ctx); err != nil {
		t.Errorf("Supervisor.Run = %v; want nil (clean drain, no failures)", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("drained runner invoked %d times; want 1 (no restart on nil)", got)
	}
	if snap, _ := snapshotFor(sup, "draining"); snap.State != SyncStopped {
		t.Errorf("drained sync state = %q; want %q", snap.State, SyncStopped)
	}
}

// TestSupervisor_SingleSyncFailureSurfaces pins that when ctx is still
// live and the only sync permanently fails, Run returns the aggregated
// error (so a one-sync fleet exits non-zero rather than reporting clean).
func TestSupervisor_SingleSyncFailureSurfaces(t *testing.T) {
	sentinel := errors.New("permanent: schema mismatch")
	bad := SupervisedSync{
		ID:     "bad",
		Runner: runnerFunc(func(_ context.Context) error { return sentinel }),
	}
	sup := NewSupervisor([]SupervisedSync{bad}, fastTestPolicy(2))
	err := sup.Run(context.Background())
	if err == nil {
		t.Fatal("Supervisor.Run = nil; want the failed sync's error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("Supervisor.Run error = %v; want it to wrap %v", err, sentinel)
	}
}

// TestSupervisor_NoSyncs pins the empty-fleet refusal.
func TestSupervisor_NoSyncs(t *testing.T) {
	sup := NewSupervisor(nil, RestartPolicy{})
	if err := sup.Run(context.Background()); err == nil {
		t.Fatal("Supervisor.Run with no syncs = nil; want an error")
	}
}

// TestBackoffFor pins the exponential-with-cap schedule.
func TestBackoffFor(t *testing.T) {
	p := RestartPolicy{BackoffBase: 100 * time.Millisecond, BackoffCap: 30 * time.Second}.withDefaults()
	cases := []struct {
		consecutive int
		want        time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{9, 25600 * time.Millisecond},
		{10, 30 * time.Second}, // capped
		{50, 30 * time.Second}, // still capped, no overflow
	}
	for _, c := range cases {
		if got := backoffFor(c.consecutive, p); got != c.want {
			t.Errorf("backoffFor(%d) = %s; want %s", c.consecutive, got, c.want)
		}
	}
}

// TestRecordFailure_ResetsOnHealthyRun pins the reset-on-progress
// semantics: a healthy run before the failure zeroes the consecutive
// counter, so a long-lived sync that finally dies doesn't carry debt.
func TestRecordFailure_ResetsOnHealthyRun(t *testing.T) {
	sup := NewSupervisor([]SupervisedSync{{ID: "s"}}, RestartPolicy{})
	if got := sup.recordFailure("s", errors.New("a"), false); got != 1 {
		t.Fatalf("first failure consecutive = %d; want 1", got)
	}
	if got := sup.recordFailure("s", errors.New("b"), false); got != 2 {
		t.Fatalf("second failure consecutive = %d; want 2", got)
	}
	// Healthy run before the third failure resets the counter to 0,
	// then this failure counts as 1.
	if got := sup.recordFailure("s", errors.New("c"), true); got != 1 {
		t.Fatalf("post-healthy failure consecutive = %d; want 1 (reset)", got)
	}
	// restarts accumulate regardless of resets.
	if snap, _ := snapshotFor(sup, "s"); snap.Restarts != 3 {
		t.Errorf("Restarts = %d; want 3", snap.Restarts)
	}
}
