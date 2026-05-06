package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStopFlagReader is a minimal stopFlagReader stub used by the
// poll-loop unit tests.
type fakeStopFlagReader struct {
	mu        sync.Mutex
	calls     int
	returnVal bool // returned by readStopRequested when err is nil
	returnErr error
}

func (r *fakeStopFlagReader) setStopRequested(v bool) {
	r.mu.Lock()
	r.returnVal = v
	r.mu.Unlock()
}

func (r *fakeStopFlagReader) ReadStopRequested(_ context.Context, _ string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return r.returnVal, r.returnErr
}

func (r *fakeStopFlagReader) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// withFastPollInterval replaces stopSignalPollInterval for the test
// duration so the goroutine ticks frequently.
func withFastPollInterval(t *testing.T) {
	t.Helper()
	prev := pollIntervalForTest
	pollIntervalForTest = 10 * time.Millisecond
	t.Cleanup(func() { pollIntervalForTest = prev })
}

// withFastDrainTimeout shrinks the graceful-drain hard-timeout window
// so the watchdog tests run in milliseconds rather than 30 seconds.
func withFastDrainTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := drainTimeoutForTest
	drainTimeoutForTest = d
	t.Cleanup(func() { drainTimeoutForTest = prev })
}

// TestPollStopSignal_CancelsStreamOnFlag verifies the load-bearing
// shape: when the reader reports stop_requested = true, the poll
// loop calls cancelStream (graceful drain) and returns. cancelApply
// only fires later as a hard-timeout fallback.
func TestPollStopSignal_CancelsStreamOnFlag(t *testing.T) {
	withFastPollInterval(t)
	withFastDrainTimeout(t, 10*time.Second) // long enough we won't see it

	reader := &fakeStopFlagReader{}
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	var streamCancelled atomic.Bool
	var applyCancelled atomic.Bool
	cancelStream := func() { streamCancelled.Store(true) }
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelStream, cancelApply, nil)
		close(done)
	}()

	// Let the loop tick a couple of times with the flag still off.
	time.Sleep(50 * time.Millisecond)
	if streamCancelled.Load() {
		t.Fatal("streamCancelled fired with flag off")
	}

	// Flip the flag; the next tick should fire cancelStream.
	reader.setStopRequested(true)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not return after stop flag was observed")
	}
	if !streamCancelled.Load() {
		t.Fatal("cancelStream was not called when flag flipped to true")
	}
	// cancelApply must NOT fire on graceful drain — that's the
	// hard-timeout-only path.
	if applyCancelled.Load() {
		t.Error("cancelApply fired on graceful-drain path; should only fire after drain timeout")
	}
	if reader.callCount() < 2 {
		t.Errorf("expected ≥2 polls before observing flag; got %d", reader.callCount())
	}
}

// TestPollStopSignal_SetsObservedOnFlag verifies the v0.9.0 hook for
// `sync stop --wait`: the optional observed *atomic.Bool is set the
// moment pollStopSignal first sees the flag, before cancelStream
// fires. Streamer.Run reads it after dispatchApply returns to decide
// whether to clear stop_requested_at — only stop-signal-driven
// graceful drains clear, not Ctrl-C / outer-ctx cancels.
func TestPollStopSignal_SetsObservedOnFlag(t *testing.T) {
	withFastPollInterval(t)
	withFastDrainTimeout(t, 10*time.Second)

	reader := &fakeStopFlagReader{}
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	var observed atomic.Bool
	var streamCancelled atomic.Bool
	cancelStream := func() { streamCancelled.Store(true) }
	cancelApply := func() {}

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelStream, cancelApply, &observed)
		close(done)
	}()

	if observed.Load() {
		t.Fatal("observed set before flag was raised")
	}
	reader.setStopRequested(true)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not return after stop flag was observed")
	}
	if !observed.Load() {
		t.Error("observed was not set when stop flag was observed")
	}
	if !streamCancelled.Load() {
		t.Error("cancelStream did not fire after stop flag was observed")
	}
}

// TestPollStopSignal_HardCancelsApplyOnDrainTimeout verifies the
// graceful-drain watchdog: when pollCtx (= applyCtx) doesn't cancel
// within drainTimeoutForTest, cancelApply fires as the fallback.
func TestPollStopSignal_HardCancelsApplyOnDrainTimeout(t *testing.T) {
	withFastPollInterval(t)
	withFastDrainTimeout(t, 100*time.Millisecond)

	reader := &fakeStopFlagReader{}
	reader.setStopRequested(true)
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	var streamCancelled atomic.Bool
	var applyCancelled atomic.Bool
	cancelStream := func() { streamCancelled.Store(true) }
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelStream, cancelApply, nil)
		close(done)
	}()

	// Wait for the poll loop to return (cancelStream fires + watchdog
	// goroutine launches).
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not return after stop flag was observed")
	}
	if !streamCancelled.Load() {
		t.Fatal("cancelStream was not called")
	}

	// cancelApply hasn't fired yet — drain timeout hasn't elapsed.
	if applyCancelled.Load() {
		t.Fatal("cancelApply fired before drain timeout elapsed")
	}

	// Wait past the drain timeout. Since pollCtx is still alive,
	// the watchdog hits the timeout branch and calls cancelApply.
	deadline := time.Now().Add(2 * time.Second)
	for !applyCancelled.Load() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !applyCancelled.Load() {
		t.Fatal("cancelApply did not fire after drain timeout elapsed")
	}
}

// TestPollStopSignal_WatchdogExitsCleanlyOnApplyDone verifies the
// graceful-drain watchdog goroutine exits cleanly when pollCtx
// cancels first (= apply finished naturally), without firing
// cancelApply.
func TestPollStopSignal_WatchdogExitsCleanlyOnApplyDone(t *testing.T) {
	withFastPollInterval(t)
	withFastDrainTimeout(t, 5*time.Second) // long; we cancel pollCtx first

	reader := &fakeStopFlagReader{}
	reader.setStopRequested(true)
	pollCtx, cancelPoll := context.WithCancel(context.Background())

	var streamCancelled atomic.Bool
	var applyCancelled atomic.Bool
	cancelStream := func() { streamCancelled.Store(true) }
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelStream, cancelApply, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not return after stop flag was observed")
	}
	if !streamCancelled.Load() {
		t.Fatal("cancelStream was not called")
	}

	// Simulate apply finishing naturally — cancel pollCtx (analogous
	// to defer cancelApply running in Streamer.Run after dispatchApply
	// returns nil on channel close).
	cancelPoll()

	// Give the watchdog goroutine a moment to observe pollCtx.Done.
	time.Sleep(100 * time.Millisecond)
	if applyCancelled.Load() {
		t.Error("cancelApply fired despite apply finishing naturally")
	}
}

// TestPollStopSignal_ExitsOnPollCtxCancel verifies the goroutine
// doesn't outlive its parent context — important so the goroutine
// doesn't leak after Streamer.Run returns.
func TestPollStopSignal_ExitsOnPollCtxCancel(t *testing.T) {
	withFastPollInterval(t)

	reader := &fakeStopFlagReader{} // flag stays off
	pollCtx, cancelPoll := context.WithCancel(context.Background())

	var streamCancelled atomic.Bool
	var applyCancelled atomic.Bool
	cancelStream := func() { streamCancelled.Store(true) }
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelStream, cancelApply, nil)
		close(done)
	}()

	// Cancel the poll's own context; the goroutine should return
	// without firing either cancel func (no stop flag was ever set).
	cancelPoll()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not return after pollCtx was cancelled")
	}
	if streamCancelled.Load() {
		t.Error("cancelStream was called on plain ctx-cancel; should only fire on flag-true")
	}
	if applyCancelled.Load() {
		t.Error("cancelApply was called on plain ctx-cancel; should only fire after drain timeout")
	}
}

// TestPollStopSignal_TolerantOfTransientErrors verifies the loop
// keeps polling when the reader returns an error — the read might
// fail on a transient connection blip, and we don't want a single
// failed query to disable the stop-signal channel for the rest of
// the streamer's lifetime.
func TestPollStopSignal_TolerantOfTransientErrors(t *testing.T) {
	withFastPollInterval(t)
	withFastDrainTimeout(t, 10*time.Second)

	reader := &fakeStopFlagReader{returnErr: errors.New("transient blip")}
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	var streamCancelled atomic.Bool
	var applyCancelled atomic.Bool
	cancelStream := func() { streamCancelled.Store(true) }
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelStream, cancelApply, nil)
		close(done)
	}()

	// Let the loop tick several times despite errors.
	time.Sleep(80 * time.Millisecond)
	if streamCancelled.Load() {
		t.Fatal("cancelStream fired during error-only ticks")
	}

	// Recover and observe the flag — poll should still see it.
	reader.mu.Lock()
	reader.returnErr = nil
	reader.returnVal = true
	reader.mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not recover from transient errors")
	}
	if !streamCancelled.Load() {
		t.Fatal("cancelStream was not called after transient errors cleared")
	}
	if applyCancelled.Load() {
		t.Error("cancelApply fired on graceful-drain path")
	}
}
