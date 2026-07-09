package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestIsStreamNotFoundErr mirrors TestIsSlotNotFoundErr: the helper
// substring-matches a wrapped engine sentinel so the CLI can branch
// to a friendly "no stream X on target" message instead of bleeding
// engine-specific error text to the operator.
func TestIsStreamNotFoundErr(t *testing.T) {
	if isStreamNotFoundErr(nil) {
		t.Error("nil error should not be stream-not-found")
	}
	if !isStreamNotFoundErr(fmt.Errorf("postgres: stream not found: %q", "x")) {
		t.Error("wrapped postgres stream-not-found should match")
	}
	if !isStreamNotFoundErr(fmt.Errorf("mysql: stream not found: %q", "x")) {
		t.Error("wrapped mysql stream-not-found should match")
	}
	if isStreamNotFoundErr(errors.New("permission denied")) {
		t.Error("unrelated error should not match")
	}
}

// fakeApplier is a minimal ir.ChangeApplier + stopFlagReader used by
// the waitForStopComplete tests. Returns whatever the test sets on
// stopRequested; production appliers (mysql, postgres) return the
// real control-row value.
type fakeApplier struct {
	stopRequested atomic.Bool
	readErr       atomic.Value // error
	readCalls     atomic.Int32
}

func (f *fakeApplier) ReadStopRequested(_ context.Context, _ string) (bool, error) {
	f.readCalls.Add(1)
	if e := f.readErr.Load(); e != nil {
		if err, ok := e.(error); ok {
			return false, err
		}
	}
	return f.stopRequested.Load(), nil
}

// Stub the rest of the ir.ChangeApplier surface — the wait helper
// only touches ReadStopRequested.
func (*fakeApplier) EnsureControlTable(context.Context) error { return nil }

func (*fakeApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (*fakeApplier) Apply(context.Context, string, <-chan ir.Change) error { return nil }
func (*fakeApplier) RequestStop(context.Context, string) error             { return nil }
func (*fakeApplier) ClearStopRequested(context.Context, string) error      { return nil }
func (*fakeApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

// TestWaitForStopComplete_FlagClears covers the happy path: the
// streamer drains and clears stop_requested_at; --wait observes the
// clear and returns nil.
func TestWaitForStopComplete_FlagClears(t *testing.T) {
	app := &fakeApplier{}
	app.stopRequested.Store(true)

	// Simulate the streamer clearing the flag after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		app.stopRequested.Store(false)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := waitForStopComplete(ctx, app, "stream-1", 2*time.Second); err != nil {
		t.Errorf("waitForStopComplete returned err = %v; want nil", err)
	}
	if app.readCalls.Load() < 1 {
		t.Errorf("expected ≥1 read call; got %d", app.readCalls.Load())
	}
}

// TestWaitForStopComplete_Timeout covers the bounded-wait path:
// flag never clears within the timeout → CLI exits non-zero with a
// clear message; the request remains in place.
func TestWaitForStopComplete_Timeout(t *testing.T) {
	app := &fakeApplier{}
	app.stopRequested.Store(true) // never clears

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := waitForStopComplete(ctx, app, "stream-1", 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error; got nil")
	}
	if !strings.Contains(err.Error(), "did not complete drain") {
		t.Errorf("err = %v; want a 'did not complete drain' message", err)
	}
	if !strings.Contains(err.Error(), "stream-1") {
		t.Errorf("err message should name the stream; got %v", err)
	}
	// The timeout is a PLAIN error — kong's generic exit 1, per the
	// helper's doc comment (which once promised an exit-2 error the
	// code never produced; 2 is reserved for config-load errors).
	// This pins code and comment staying in agreement.
	if got := exitCodeLikeKong(err); got != 1 {
		t.Errorf("timeout exit code = %d; want the generic 1", got)
	}
}

// TestWaitForStopComplete_ContextCancel verifies the helper exits
// cleanly when the surrounding ctx cancels (e.g. operator Ctrl-C
// while waiting).
func TestWaitForStopComplete_ContextCancel(t *testing.T) {
	app := &fakeApplier{}
	app.stopRequested.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := waitForStopComplete(ctx, app, "stream-1", 5*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v; want context.Canceled", err)
	}
}

// TestWaitForStopComplete_NonPollingApplier covers the graceful-
// degradation path: an applier that can RequestStop but doesn't
// implement ReadStopRequested falls back to fire-and-forget with a
// surfaced message rather than blocking forever.
func TestWaitForStopComplete_NonPollingApplier(t *testing.T) {
	// stubApplier doesn't implement stopFlagReader.
	type stubApplier struct{ ir.ChangeApplier }
	var s stubApplier

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := waitForStopComplete(ctx, &s, "stream-1", 5*time.Second); err != nil {
		t.Errorf("non-polling applier should fall back cleanly; got err = %v", err)
	}
}
