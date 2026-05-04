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
// duration so the goroutine ticks frequently. The constant restore
// runs as a Cleanup; tests can run in parallel as long as they all
// adopt the fast cadence.
//
// We toggle a package-level variable rather than the constant — the
// constant stays the production default, the override is for tests.
func withFastPollInterval(t *testing.T) {
	t.Helper()
	prev := pollIntervalForTest
	pollIntervalForTest = 10 * time.Millisecond
	t.Cleanup(func() { pollIntervalForTest = prev })
}

// TestPollStopSignal_CancelsApplyOnFlag verifies the load-bearing
// shape: when the reader reports stop_requested = true, the poll
// loop calls cancelApply and returns.
func TestPollStopSignal_CancelsApplyOnFlag(t *testing.T) {
	withFastPollInterval(t)

	reader := &fakeStopFlagReader{}
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	var applyCancelled atomic.Bool
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelApply)
		close(done)
	}()

	// Let the loop tick a couple of times with the flag still off.
	time.Sleep(50 * time.Millisecond)
	if applyCancelled.Load() {
		t.Fatal("applyCancelled fired with flag off")
	}

	// Flip the flag; the next tick should fire cancelApply.
	reader.setStopRequested(true)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not return after stop flag was observed")
	}
	if !applyCancelled.Load() {
		t.Fatal("cancelApply was not called when flag flipped to true")
	}
	if reader.callCount() < 2 {
		t.Errorf("expected ≥2 polls before observing flag; got %d", reader.callCount())
	}
}

// TestPollStopSignal_ExitsOnPollCtxCancel verifies the goroutine
// doesn't outlive its parent context — important so the goroutine
// doesn't leak after Streamer.Run returns.
func TestPollStopSignal_ExitsOnPollCtxCancel(t *testing.T) {
	withFastPollInterval(t)

	reader := &fakeStopFlagReader{} // flag stays off
	pollCtx, cancelPoll := context.WithCancel(context.Background())

	var applyCancelled atomic.Bool
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelApply)
		close(done)
	}()

	// Cancel the poll's own context; the goroutine should return
	// without firing cancelApply (no stop flag was ever set).
	cancelPoll()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll loop did not return after pollCtx was cancelled")
	}
	if applyCancelled.Load() {
		t.Error("cancelApply was called on plain ctx-cancel; should only fire on flag-true")
	}
}

// TestPollStopSignal_TolerantOfTransientErrors verifies the loop
// keeps polling when the reader returns an error — the read might
// fail on a transient connection blip, and we don't want a single
// failed query to disable the stop-signal channel for the rest of
// the streamer's lifetime.
func TestPollStopSignal_TolerantOfTransientErrors(t *testing.T) {
	withFastPollInterval(t)

	reader := &fakeStopFlagReader{returnErr: errors.New("transient blip")}
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	var applyCancelled atomic.Bool
	cancelApply := func() { applyCancelled.Store(true) }

	done := make(chan struct{})
	go func() {
		pollStopSignal(pollCtx, reader, "stream-1", cancelApply)
		close(done)
	}()

	// Let the loop tick several times despite errors.
	time.Sleep(80 * time.Millisecond)
	if applyCancelled.Load() {
		t.Fatal("cancelApply fired during error-only ticks")
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
	if !applyCancelled.Load() {
		t.Fatal("cancelApply was not called after transient errors cleared")
	}
}
