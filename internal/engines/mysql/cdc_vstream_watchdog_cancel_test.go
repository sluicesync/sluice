// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests pin the audit 2026-08-26 C-1 fix, both halves:
//
//  1. [vstreamLiveness.stop] JOINS the watchdog goroutine, so a timer fire the
//     goroutine has committed to can never run its callback after the pump has
//     torn the watchdog down.
//  2. The snapshot CDC pump's watchdog callbacks cancel the per-stream cancel
//     CAPTURED at launch ([launchCDCPump]), never the dynamic s.grpcCancel a
//     reshard reopen may have re-pointed at a FRESH stream by fire time.
//
// Either half alone closes the silent-exit-0 interleaving (stale Phase-2 fire
// races the reshard JOURNAL -> cancels the fresh reopened stream -> Canceled
// Recv reads as a clean stop -> first-wins setErr + the reopen's err-clear
// leave Err()==nil -> the continuous sync exits 0 mid-stream); both are fixed
// to match the file's own conventions. The race is pure ORDERING — every
// access was already correctly locked — so -race cannot grade it; these
// deterministic pins are the gate.

// TestVStreamLiveness_StopJoinsWatchdogGoroutine pins the join: stop() must
// not return while the watchdog goroutine is still running its callback. The
// fake-timer seam injects a fire, the callback parks on a test-held gate, and
// stop() is asserted to block until the gate releases (mutation: removing the
// join makes stop return while the callback still runs — the first select
// fails the test).
func TestVStreamLiveness_StopJoinsWatchdogGoroutine(t *testing.T) {
	ft := newFakeLivenessTimer()
	entered := make(chan struct{})
	release := make(chan struct{})
	live := startVStreamLivenessWithTimer(context.Background(), time.Minute, time.Second, 0,
		func() { close(entered); <-release },
		failingTimeout(t, "phase-2"),
		nil,
		ft.factory())

	// Commit the watchdog to the Phase-1 callback, mid-flight.
	ft.fire <- time.Now()
	<-entered

	stopped := make(chan struct{})
	go func() {
		live.stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("stop() returned while the watchdog callback was still running — the join is gone (audit 2026-08-26 C-1)")
	case <-time.After(50 * time.Millisecond):
		// Still joined on the in-flight callback: correct.
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop() never returned after the watchdog goroutine exited")
	}

	// Idempotent second stop still returns (and still upholds the join).
	done := make(chan struct{})
	go func() {
		live.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second stop() wedged")
	}
}

// TestVStreamSnapshotCDCPump_WatchdogCancelsArmedStreamNotFreshOne pins the
// captured-per-stream cancel: the pump's watchdog is armed for ONE stream, and
// its fire must cancel THAT stream even when s.grpcCancel has since been
// re-pointed at a freshly-installed stream (the reshard-reopen field swap,
// simulated here between launch and fire). Three assertions, each a half of
// the silent-exit-0 chain:
//
//   - the armed stream's pump exits (its own context was cancelled),
//   - the FRESH stream's context is untouched (no Canceled Recv -> no clean
//     stop -> no silent exit 0 on the reopened tail),
//   - the loud watchdog error is recorded (Err() != nil).
//
// With the pre-fix dynamic s.cancelGRPCStream() the fire cancels the fresh
// context and leaves the armed stream parked forever: the first select times
// out and the freshCtx check trips.
func TestVStreamSnapshotCDCPump_WatchdogCancelsArmedStreamNotFreshOne(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer testCancel()

	s := newTestSnapshotStream()
	// Phase-1 fires fast: the armed stream never produces an event. The
	// window is generous enough that the field swap below (a handful of
	// statements) always lands before the fire.
	s.livenessWindow = 100 * time.Millisecond
	s.progressWindow = 0
	s.idleWarnWindow = 0

	// The ARMED stream: parks silently on its own context, like a wedged
	// tail. Its cancel is what launchCDCPump must capture.
	armedCtx, armedCancel := context.WithCancel(context.Background())
	defer armedCancel()
	s.grpcStream = &scriptedStream{ctx: armedCtx}
	s.grpcCancel = armedCancel

	out := make(chan ir.Change)
	s.launchCDCPump(testCtx, out)

	// Simulate the reshard reopen's cancel swap racing a committed fire: by
	// the time the watchdog fires, the dynamic s.grpcCancel points at the
	// FRESH stream's cancel.
	freshCtx, freshCancel := context.WithCancel(context.Background())
	defer freshCancel()
	s.mu.Lock()
	s.grpcCancel = freshCancel
	s.mu.Unlock()

	// The watchdog must cancel the stream it was ARMED for, unblocking the
	// parked Recv so the pump exits and closes out.
	select {
	case c, ok := <-out:
		if ok {
			t.Fatalf("pump delivered an unexpected change: %#v", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pump never exited — the watchdog fire did not cancel the armed stream (it cancelled the dynamic s.grpcCancel instead; audit 2026-08-26 C-1)")
	}
	select {
	case <-armedCtx.Done():
	default:
		t.Error("the armed stream's context was never cancelled")
	}

	// ...and must NOT have touched the freshly-installed stream's context — a
	// cancelled fresh tail reads its next Recv as context.Canceled, returns
	// with no error, and the sync exits 0 mid-stream.
	select {
	case <-freshCtx.Done():
		t.Fatal("the watchdog cancelled the FRESHLY-installed stream's context — the silent-exit-0 half of audit 2026-08-26 C-1")
	default:
	}

	// The fire must be LOUD.
	if err := s.Err(); err == nil {
		t.Fatal("watchdog fired but Err() is nil — the timeout was recorded nowhere")
	}
}
