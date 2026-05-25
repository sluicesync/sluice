// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Unit tests for the source-side heartbeat writer (ADR-0061, F17).
// Pins:
//   - the loop's ticker drives WriteHeartbeat at the configured cadence
//     and ctx cancellation tears it down cleanly;
//   - a transient WriteHeartbeat error logs and the loop continues
//     (non-fatal — next tick retries);
//   - a permission-revoked error tears the loop down cleanly so a
//     mid-stream privilege loss doesn't spam WARNs forever;
//   - the prune path fires on its own cadence with pruneWindow > 0 and
//     is skipped when pruneWindow <= 0;
//   - attachSourceHeartbeat returns the noop attachment cleanly on
//     every opt-out branch (interval=0, NoSourceHeartbeat=true, source
//     unset).
//
// Pin-the-class discipline: every observable behaviour of the loop is
// exercised at least once. Production wiring lives in
// attachSourceHeartbeat which is exercised by the integration test.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// stubHeartbeatWriter is a controllable HeartbeatWriter for the loop
// tests. Threadsafe: the loop reads/writes from one goroutine, the
// test from another.
type stubHeartbeatWriter struct {
	mu sync.Mutex

	ensureErr error
	writeErr  error
	pruneErr  error

	writeCalls atomic.Int32
	pruneCalls atomic.Int32
	pruneRows  int64

	lastStreamID  string
	lastTableName string
}

func (s *stubHeartbeatWriter) EnsureHeartbeatTable(_ context.Context, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureErr
}

func (s *stubHeartbeatWriter) WriteHeartbeat(_ context.Context, tableName, streamID string) error {
	s.writeCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastTableName = tableName
	s.lastStreamID = streamID
	return s.writeErr
}

func (s *stubHeartbeatWriter) PruneHeartbeat(_ context.Context, _ string, _ time.Duration) (int64, error) {
	s.pruneCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pruneRows, s.pruneErr
}

func (s *stubHeartbeatWriter) setWriteErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeErr = err
}

// TestSourceHeartbeatLoop_TicksAndCancels pins the basic lifecycle: a
// short-interval loop calls WriteHeartbeat on every tick and exits
// cleanly when ctx cancels.
func TestSourceHeartbeatLoop_TicksAndCancels(t *testing.T) {
	w := &stubHeartbeatWriter{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sourceHeartbeatLoop(ctx, w, "sluice_heartbeat", "stream-a", 20*time.Millisecond, 0)
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

	if got := w.writeCalls.Load(); got < 3 {
		t.Errorf("expected >=3 write calls in 150ms with 20ms tick; got %d", got)
	}
	if w.lastStreamID != "stream-a" {
		t.Errorf("lastStreamID: got %q; want %q", w.lastStreamID, "stream-a")
	}
	if w.lastTableName != "sluice_heartbeat" {
		t.Errorf("lastTableName: got %q; want %q", w.lastTableName, "sluice_heartbeat")
	}
}

// TestSourceHeartbeatLoop_TransientWriteErrorDoesNotKillLoop pins
// resilience: a transient INSERT failure logs and the loop continues.
func TestSourceHeartbeatLoop_TransientWriteErrorDoesNotKillLoop(t *testing.T) {
	w := &stubHeartbeatWriter{}
	w.setWriteErr(errors.New("transient: connection reset"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		sourceHeartbeatLoop(ctx, w, "sluice_heartbeat", "stream-b", 20*time.Millisecond, 0)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if got := w.writeCalls.Load(); got < 3 {
		t.Errorf("loop should continue past transient errors; got %d calls", got)
	}
}

// TestSourceHeartbeatLoop_PermissionErrorTerminatesLoop pins the
// loud-failure path: a permission-revoked error tears the loop down
// cleanly so the streamer doesn't spam WARNs every tick.
func TestSourceHeartbeatLoop_PermissionErrorTerminatesLoop(t *testing.T) {
	w := &stubHeartbeatWriter{}
	w.setWriteErr(fmt.Errorf("%w: PG SQLSTATE 42501", ir.ErrHeartbeatPermission))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		sourceHeartbeatLoop(ctx, w, "sluice_heartbeat", "stream-c", 20*time.Millisecond, 0)
		close(done)
	}()

	// The loop should exit on the first permission-revoked tick — not
	// wait for ctx cancel. Allow generously for goroutine scheduling.
	select {
	case <-done:
		// good — loop exited on its own
	case <-time.After(500 * time.Millisecond):
		t.Fatal("loop did not exit within 500ms of permission-revoked write error")
	}

	if got := w.writeCalls.Load(); got != 1 {
		t.Errorf("expected exactly 1 write call before loop terminates; got %d", got)
	}
}

// TestSourceHeartbeatLoop_PrunePathFires pins the prune branch: with
// pruneWindow > 0 and a short prune cadence, PruneHeartbeat is called.
// We drive sourceHeartbeatPruneCadence down so the test doesn't have
// to wait for the production 1-minute cadence.
func TestSourceHeartbeatLoop_PrunePathFires(t *testing.T) {
	orig := sourceHeartbeatPruneCadence
	sourceHeartbeatPruneCadence = 30 * time.Millisecond
	defer func() { sourceHeartbeatPruneCadence = orig }()

	w := &stubHeartbeatWriter{pruneRows: 7}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		sourceHeartbeatLoop(
			ctx, w, "sluice_heartbeat", "stream-d",
			20*time.Millisecond, // write
			time.Hour,           // pruneWindow: > 0 so prune branch is active
		)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if got := w.pruneCalls.Load(); got < 2 {
		t.Errorf("expected >=2 prune calls with 30ms cadence in 150ms; got %d", got)
	}
}

// TestSourceHeartbeatLoop_PruneDisabled pins the disabled-prune branch:
// pruneWindow=0 skips Prune entirely.
func TestSourceHeartbeatLoop_PruneDisabled(t *testing.T) {
	orig := sourceHeartbeatPruneCadence
	sourceHeartbeatPruneCadence = 30 * time.Millisecond
	defer func() { sourceHeartbeatPruneCadence = orig }()

	w := &stubHeartbeatWriter{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		sourceHeartbeatLoop(
			ctx, w, "sluice_heartbeat", "stream-e",
			20*time.Millisecond, // write
			0,                   // pruneWindow: disabled
		)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if got := w.pruneCalls.Load(); got != 0 {
		t.Errorf("expected 0 prune calls with pruneWindow=0; got %d", got)
	}
	if got := w.writeCalls.Load(); got < 3 {
		t.Errorf("write path should still tick when prune is disabled; got %d", got)
	}
}

// TestSourceHeartbeatLoop_PrunePermissionStopsPruneTicker pins the
// half-permission case: DELETE is revoked but INSERT still works. The
// loop must stop the prune ticker (so we don't keep retrying) while
// the write ticker continues.
func TestSourceHeartbeatLoop_PrunePermissionStopsPruneTicker(t *testing.T) {
	orig := sourceHeartbeatPruneCadence
	sourceHeartbeatPruneCadence = 30 * time.Millisecond
	defer func() { sourceHeartbeatPruneCadence = orig }()

	w := &stubHeartbeatWriter{
		pruneErr: fmt.Errorf("%w: DELETE denied", ir.ErrHeartbeatPermission),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		sourceHeartbeatLoop(
			ctx, w, "sluice_heartbeat", "stream-f",
			20*time.Millisecond,
			time.Hour,
		)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	// One prune call (which trips the permission error and stops the
	// prune ticker), and many write calls (loop continues for INSERT).
	if got := w.pruneCalls.Load(); got != 1 {
		t.Errorf("expected exactly 1 prune call before prune ticker stops; got %d", got)
	}
	if got := w.writeCalls.Load(); got < 3 {
		t.Errorf("write path should continue after prune-permission revocation; got %d writes", got)
	}
}

// TestSourceHeartbeatAttachment_CloseIsIdempotent pins the cleanup
// shape: Close() can be called twice without panicking.
func TestSourceHeartbeatAttachment_CloseIsIdempotent(t *testing.T) {
	var closed int32
	att := &sourceHeartbeatAttachment{
		cancel: func() {},
		close:  func() { atomic.AddInt32(&closed, 1) },
	}
	att.Close()
	att.Close()
	if got := atomic.LoadInt32(&closed); got != 1 {
		t.Errorf("close fn should fire exactly once across two Close() calls; fired %d times", got)
	}
}

// TestSourceHeartbeatAttachment_ZeroValueCloseIsSafe pins the noop path:
// attachSourceHeartbeat returns a zero-value attachment when the source
// is missing / interval=0 / engine doesn't implement the writer. Close
// on that must not panic.
func TestSourceHeartbeatAttachment_ZeroValueCloseIsSafe(_ *testing.T) {
	att := &sourceHeartbeatAttachment{}
	att.Close() // must not panic
}

// TestAttachSourceHeartbeat_OptOutInterval pins that interval=0 short-
// circuits the attach path without opening any source connection.
func TestAttachSourceHeartbeat_OptOutInterval(t *testing.T) {
	s := &Streamer{
		// Source / SourceDSN intentionally non-nil to prove the short-
		// circuit happens before the open-source branch.
		Source:                  fakeEngineForHeartbeatTest{},
		SourceDSN:               "fake://dsn",
		SourceHeartbeatInterval: 0,
	}
	att := s.attachSourceHeartbeat(context.Background(), "stream-z")
	if att == nil {
		t.Fatal("attachSourceHeartbeat should return non-nil even on opt-out")
	}
	// The noop attachment has nil cancel + nil close fns; Close must be
	// idempotent and harmless.
	att.Close()
	att.Close()
}

// TestAttachSourceHeartbeat_OptOutFlag pins that --no-source-heartbeat
// short-circuits even when the interval is set (CLI override of YAML).
func TestAttachSourceHeartbeat_OptOutFlag(t *testing.T) {
	s := &Streamer{
		Source:                  fakeEngineForHeartbeatTest{},
		SourceDSN:               "fake://dsn",
		SourceHeartbeatInterval: 30 * time.Second,
		NoSourceHeartbeat:       true,
	}
	att := s.attachSourceHeartbeat(context.Background(), "stream-z")
	if att == nil {
		t.Fatal("attachSourceHeartbeat should return non-nil even on opt-out")
	}
	att.Close()
}

// fakeEngineForHeartbeatTest is a minimal engine used only to prove the
// opt-out branches don't open a SchemaReader. Every method panics so a
// regression that bypasses the opt-out gate fails loudly.
type fakeEngineForHeartbeatTest struct{}

func (fakeEngineForHeartbeatTest) Name() string                  { return "fake-heartbeat" }
func (fakeEngineForHeartbeatTest) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (fakeEngineForHeartbeatTest) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	panic("attachSourceHeartbeat should not open source on opt-out branch")
}

func (fakeEngineForHeartbeatTest) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	panic("not implemented")
}

func (fakeEngineForHeartbeatTest) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	panic("not implemented")
}

func (fakeEngineForHeartbeatTest) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	panic("not implemented")
}

func (fakeEngineForHeartbeatTest) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	panic("not implemented")
}

func (fakeEngineForHeartbeatTest) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	panic("not implemented")
}

func (fakeEngineForHeartbeatTest) OpenSnapshotStream(_ context.Context, _ string) (*ir.SnapshotStream, error) {
	panic("not implemented")
}
