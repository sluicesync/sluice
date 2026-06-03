// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Tests for the [slotHealthProbeLoop] goroutine — that the ticker
// drives probes, ctx cancellation tears down cleanly, and probe
// errors fall through to the next tick instead of killing the loop.
// Threshold-emit logic is covered separately by slot_health_test.go;
// this file is about the goroutine's lifecycle.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// stubSlotHealthReporter is a controllable reporter for the loop tests.
// Threadsafe; the loop reads from one goroutine, the test from another.
type stubSlotHealthReporter struct {
	mu      sync.Mutex
	calls   int32
	nextErr error
	snap    ir.SlotHealth
	snapOK  bool
}

func (s *stubSlotHealthReporter) SlotHealth(_ context.Context, _ string) (ir.SlotHealth, bool, error) {
	atomic.AddInt32(&s.calls, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextErr != nil {
		return ir.SlotHealth{}, false, s.nextErr
	}
	return s.snap, s.snapOK, nil
}

func (s *stubSlotHealthReporter) setSnap(snap ir.SlotHealth, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = snap
	s.snapOK = ok
	s.nextErr = nil
}

func (s *stubSlotHealthReporter) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextErr = err
}

// TestSlotHealthProbeLoop_TicksAndCancels pins the basic lifecycle: a
// short-interval loop calls the reporter on every tick and exits
// cleanly when ctx cancels. The loop must not panic on a
// nil-snapshot probe (ok=false) — that's the cold-start race where
// the slot row doesn't exist yet.
func TestSlotHealthProbeLoop_TicksAndCancels(t *testing.T) {
	r := &stubSlotHealthReporter{}
	r.setSnap(ir.SlotHealth{SlotName: "sluice_slot"}, false) // ok=false

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		slotHealthProbeLoop(ctx, r, "sluice_slot", "stream-x", DefaultSlotHealthThresholds(), 20*time.Millisecond)
		close(done)
	}()

	// Wait long enough for ~5 ticks.
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not exit within 2s of ctx cancel")
	}

	if got := atomic.LoadInt32(&r.calls); got < 3 {
		t.Errorf("expected >=3 probe calls in 150ms with 20ms tick; got %d", got)
	}
}

// TestSlotHealthProbeLoop_ProbeErrorDoesNotKillLoop pins resilience:
// a probe error logs at DEBUG and the loop continues. We can't easily
// assert the log line without a custom slog handler, but we can
// observe that the call count keeps incrementing.
func TestSlotHealthProbeLoop_ProbeErrorDoesNotKillLoop(t *testing.T) {
	r := &stubSlotHealthReporter{}
	r.setErr(errors.New("transient PG error"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		slotHealthProbeLoop(ctx, r, "sluice_slot", "stream-y", DefaultSlotHealthThresholds(), 20*time.Millisecond)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if got := atomic.LoadInt32(&r.calls); got < 3 {
		t.Errorf("loop should continue past probe errors; got %d calls", got)
	}
}

// TestSlotHealthProbeAttachmentCloseIsIdempotent pins the cleanup
// shape: Close() can be called twice without panicking.
func TestSlotHealthProbeAttachmentCloseIsIdempotent(t *testing.T) {
	var closed int32
	att := &slotHealthProbeAttachment{
		cancel: func() {},
		close:  func() { atomic.AddInt32(&closed, 1) },
	}
	att.Close()
	att.Close()
	if got := atomic.LoadInt32(&closed); got != 1 {
		t.Errorf("close fn should fire exactly once across two Close() calls; fired %d times", got)
	}
}

// TestSlotHealthProbeAttachmentZeroValueCloseIsSafe pins the noop
// path: attachSlotHealthProbe returns a zero-value attachment when
// the source is missing / engine doesn't implement the reporter.
// Close on that must not panic.
func TestSlotHealthProbeAttachmentZeroValueCloseIsSafe(_ *testing.T) {
	att := &slotHealthProbeAttachment{}
	att.Close() // must not panic
}
