// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Unit tests for the Streamer's dispatchApply routing — the
// optional-interface probe that picks BatchedChangeApplier when
// available and ApplyBatchSize > 1, and falls through to per-change
// Apply otherwise.
//
// Integration coverage of the actual batched-commit shape lives in
// internal/engines/postgres/change_applier_batch_integration_test.go
// (commit-count assertion via pg_stat_database, idempotency replay,
// truncate-flush, ctx-cancel rollback). These tests are about the
// routing decision only, so they use in-process channel pumps and
// don't need a database.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// applierWithBatch is a stub that implements both Apply and
// ApplyBatch. Each call records which path was used and the
// batch size if any. Used to assert the streamer's
// dispatchApply routing.
type applierWithBatch struct {
	applyCalls int
	batchCalls int
	lastBatchN int
}

func (a *applierWithBatch) EnsureControlTable(_ context.Context) error { return nil }
func (a *applierWithBatch) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *applierWithBatch) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return []ir.StreamStatus{}, nil
}
func (a *applierWithBatch) RequestStop(_ context.Context, _ string) error        { return nil }
func (a *applierWithBatch) ClearStopRequested(_ context.Context, _ string) error { return nil }

func (a *applierWithBatch) Apply(ctx context.Context, _ string, changes <-chan ir.Change) error {
	a.applyCalls++
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *applierWithBatch) ApplyBatch(ctx context.Context, _ string, changes <-chan ir.Change, maxBatchSize int) error {
	a.batchCalls++
	a.lastBatchN = maxBatchSize
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applierApplyOnly only implements the basic ChangeApplier surface;
// no ApplyBatch. The streamer should fall back to Apply when the
// type assertion fails.
type applierApplyOnly struct {
	applyCalls int
}

func (a *applierApplyOnly) EnsureControlTable(_ context.Context) error { return nil }
func (a *applierApplyOnly) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *applierApplyOnly) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return []ir.StreamStatus{}, nil
}
func (a *applierApplyOnly) RequestStop(_ context.Context, _ string) error        { return nil }
func (a *applierApplyOnly) ClearStopRequested(_ context.Context, _ string) error { return nil }

func (a *applierApplyOnly) Apply(ctx context.Context, _ string, changes <-chan ir.Change) error {
	a.applyCalls++
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// TestDispatchApply_BatchedWhenAvailable confirms the routing picks
// the BatchedChangeApplier path when ApplyBatchSize > 1 and the
// applier implements the optional interface.
func TestDispatchApply_BatchedWhenAvailable(t *testing.T) {
	app := &applierWithBatch{}
	s := &Streamer{ApplyBatchSize: 50}

	ch := make(chan ir.Change)
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.dispatchApply(ctx, app, "test-stream", ch); err != nil {
		t.Fatalf("dispatchApply: %v", err)
	}
	if app.batchCalls != 1 {
		t.Errorf("ApplyBatch call count = %d; want 1", app.batchCalls)
	}
	if app.applyCalls != 0 {
		t.Errorf("Apply call count = %d; want 0 (should have routed to batched path)", app.applyCalls)
	}
	if app.lastBatchN != 50 {
		t.Errorf("ApplyBatch maxBatchSize = %d; want 50", app.lastBatchN)
	}
}

// TestDispatchApply_PerChangeOnZero confirms the default (zero or
// one) routes to per-change Apply even when the applier supports
// the batched path.
func TestDispatchApply_PerChangeOnZero(t *testing.T) {
	for _, n := range []int{0, 1} {
		n := n
		t.Run("size="+itoa(n), func(t *testing.T) {
			app := &applierWithBatch{}
			s := &Streamer{ApplyBatchSize: n}

			ch := make(chan ir.Change)
			close(ch)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := s.dispatchApply(ctx, app, "test-stream", ch); err != nil {
				t.Fatalf("dispatchApply: %v", err)
			}
			if app.applyCalls != 1 {
				t.Errorf("Apply call count = %d; want 1 (default should route to per-change)", app.applyCalls)
			}
			if app.batchCalls != 0 {
				t.Errorf("ApplyBatch call count = %d; want 0", app.batchCalls)
			}
		})
	}
}

// TestDispatchApply_FallbackOnApplierWithoutBatch confirms that an
// applier that doesn't implement BatchedChangeApplier falls through
// to per-change Apply even when ApplyBatchSize > 1, with a warning
// log line that operators can spot.
func TestDispatchApply_FallbackOnApplierWithoutBatch(t *testing.T) {
	app := &applierApplyOnly{}
	s := &Streamer{ApplyBatchSize: 100}

	ch := make(chan ir.Change)
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.dispatchApply(ctx, app, "test-stream", ch); err != nil {
		t.Fatalf("dispatchApply: %v", err)
	}
	if app.applyCalls != 1 {
		t.Errorf("Apply call count = %d; want 1 (fallback path)", app.applyCalls)
	}
}

// TestDispatchApply_ProgagatesError confirms the dispatcher returns
// any error from the applier path verbatim. Error wrapping happens
// at the caller in Run; dispatchApply itself just returns whatever
// the applier returned.
func TestDispatchApply_ProgagatesError(t *testing.T) {
	want := errors.New("boom")
	app := &errorApplier{err: want}
	s := &Streamer{ApplyBatchSize: 0}

	ch := make(chan ir.Change)
	close(ch)

	got := s.dispatchApply(context.Background(), app, "test-stream", ch)
	if !errors.Is(got, want) {
		t.Errorf("err = %v; want %v", got, want)
	}
}

// errorApplier is a minimal applier whose Apply method returns
// a fixed error. Used to assert dispatchApply's error
// propagation.
type errorApplier struct {
	err error
}

func (a *errorApplier) EnsureControlTable(_ context.Context) error { return nil }
func (a *errorApplier) ReadPosition(_ context.Context, _ string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *errorApplier) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	return []ir.StreamStatus{}, nil
}
func (a *errorApplier) RequestStop(_ context.Context, _ string) error        { return nil }
func (a *errorApplier) ClearStopRequested(_ context.Context, _ string) error { return nil }

func (a *errorApplier) Apply(_ context.Context, _ string, _ <-chan ir.Change) error {
	return a.err
}

// itoa is a tiny helper for the table-test name above. The standard
// library's strconv.Itoa would do fine; this avoids a single import
// for a single-use site.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + itoa(-n)
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
