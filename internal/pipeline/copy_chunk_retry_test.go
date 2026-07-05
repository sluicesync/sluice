// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// --- minimal RowReader / RowWriter stubs ---

type noopRowReader struct{}

func (noopRowReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}
func (noopRowReader) Err() error { return nil }

type noopRowWriter struct{}

func (noopRowWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

// slotRetryEngine is a fake ir.Engine whose OpenRowReader / OpenRowWriter
// fail the first failOpens times with a configurable error, then succeed.
// It optionally implements ir.ConnectionSlotClassifier so the retry seam
// can be exercised with and without the classifier.
type slotRetryEngine struct {
	stubEngine
	openErr      error // error returned on a failing open
	failOpens    int32 // number of opens to fail before succeeding
	classifySlot bool  // whether IsConnectionSlotExhausted treats openErr as slot exhaustion

	readerOpens int32
	writerOpens int32
}

func (e *slotRetryEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	n := atomic.AddInt32(&e.readerOpens, 1)
	if n <= atomic.LoadInt32(&e.failOpens) {
		return nil, e.openErr
	}
	return noopRowReader{}, nil
}

func (e *slotRetryEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	atomic.AddInt32(&e.writerOpens, 1)
	return noopRowWriter{}, nil
}

func (e *slotRetryEngine) IsConnectionSlotExhausted(err error) bool {
	return e.classifySlot && err != nil && errors.Is(err, e.openErr)
}

// zeroDelayGate builds a gate with a no-wait policy so the retry tests
// never actually sleep, but the retry bound is still enforced.
func zeroDelayGate(parallelism, MaxRetries int) *migcore.CopyParallelismGate {
	return migcore.NewCopyParallelismGate(parallelism, migcore.CopyBackoffPolicy{
		MaxRetries:   MaxRetries,
		BaseDelay:    0,
		MaxDelay:     0,
		MaxTotalWait: 1 << 62, // effectively unbounded for these tests
	})
}

// TestAcquireChunkConn_RetriesOnSlotExhaustionThenSucceeds pins the happy
// retry path: a classified slot-exhaustion on the first two opens backs
// off and retries, and the third open succeeds.
func TestAcquireChunkConn_RetriesOnSlotExhaustionThenSucceeds(t *testing.T) {
	slotErr := errors.New("FATAL: remaining connection slots are reserved (SQLSTATE 53300)")
	eng := &slotRetryEngine{openErr: slotErr, failOpens: 2, classifySlot: true}
	deps := &parallelBulkCopyDeps{source: eng, target: eng}
	gate := zeroDelayGate(4, 10)

	rdr, wr, release, err := acquireChunkConn(context.Background(), deps, gate, eng.IsConnectionSlotExhausted, 1, "t")
	if err != nil {
		t.Fatalf("acquireChunkConn after retries: %v", err)
	}
	if rdr == nil || wr == nil || release == nil {
		t.Fatal("acquireChunkConn returned nil reader/writer/release on success")
	}
	release()

	if got := atomic.LoadInt32(&eng.readerOpens); got != 3 {
		t.Errorf("reader opens = %d, want 3 (2 failed + 1 success)", got)
	}
	// Parallelism shrank twice (4→2→1) across the two retries.
	eff := gate.Effective()
	if eff != 1 {
		t.Errorf("effective parallelism after 2 shrinks = %d, want 1", eff)
	}
}

// TestAcquireChunkConn_NonRetryableFailsLoudly pins the safety property:
// an error the classifier does NOT recognise as slot exhaustion (bad
// DSN, permission denied, a real failure) surfaces immediately without a
// single retry — never masked as backpressure.
func TestAcquireChunkConn_NonRetryableFailsLoudly(t *testing.T) {
	realErr := errors.New("permission denied for table foo")
	// classifySlot=false → IsConnectionSlotExhausted returns false for
	// this error, so it must NOT be retried.
	eng := &slotRetryEngine{openErr: realErr, failOpens: 100, classifySlot: false}
	deps := &parallelBulkCopyDeps{source: eng, target: eng}
	gate := zeroDelayGate(4, 10)

	_, _, _, err := acquireChunkConn(context.Background(), deps, gate, eng.IsConnectionSlotExhausted, 1, "t")
	if err == nil {
		t.Fatal("expected the non-retryable error to surface, got nil")
	}
	if !errors.Is(err, realErr) {
		t.Errorf("error should wrap the real open error; got %v", err)
	}
	if got := atomic.LoadInt32(&eng.readerOpens); got != 1 {
		t.Errorf("reader opens = %d, want 1 (no retry on a non-retryable error)", got)
	}
}

// TestAcquireChunkConn_GivesUpAfterBound pins the loud bounded give-up: a
// permanently slot-exhausted target fails after MaxRetries with
// migcore.ErrCopySlotsExhausted rather than spinning forever.
func TestAcquireChunkConn_GivesUpAfterBound(t *testing.T) {
	slotErr := errors.New("too many clients (SQLSTATE 53300)")
	// Never succeeds.
	eng := &slotRetryEngine{openErr: slotErr, failOpens: 1 << 30, classifySlot: true}
	deps := &parallelBulkCopyDeps{source: eng, target: eng}
	gate := zeroDelayGate(8, 3)

	_, _, _, err := acquireChunkConn(context.Background(), deps, gate, eng.IsConnectionSlotExhausted, 1, "t")
	if err == nil {
		t.Fatal("expected a give-up error on a permanently-exhausted target, got nil")
	}
	if !errors.Is(err, migcore.ErrCopySlotsExhausted) {
		t.Errorf("give-up error should wrap migcore.ErrCopySlotsExhausted; got %v", err)
	}
	// MaxRetries=3 → opens attempted: initial + 3 retries = 4.
	if got := atomic.LoadInt32(&eng.readerOpens); got != 4 {
		t.Errorf("reader opens = %d, want 4 (initial + 3 retries)", got)
	}
}
