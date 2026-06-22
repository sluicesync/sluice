// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeRetriableErr is a test value satisfying ir.RetriableError — the
// SAME engine-neutral surface the MySQL reader's classifyApplierError
// produces for a connection-drop. The source-read retry classifies on
// this interface (errors.As), so the test never needs an engine package.
type fakeRetriableErr struct{ msg string }

func (e fakeRetriableErr) Error() string            { return e.msg }
func (e fakeRetriableErr) Retriable() bool          { return true }
func (e fakeRetriableErr) RetryHint() time.Duration { return 0 }

// withFastSourceReadBackoff shrinks the source-read retry envelope to
// near-zero for the duration of a test so the suite never actually sleeps,
// while the attempt COUNT bound stays exactly as production. Restores the
// production values on cleanup.
func withFastSourceReadBackoff(t *testing.T) {
	t.Helper()
	base, capDur, attempts := coldCopySourceReadBackoffBase, coldCopySourceReadBackoffCap, coldCopySourceReadRetryAttempts
	coldCopySourceReadBackoffBase = time.Microsecond
	coldCopySourceReadBackoffCap = time.Microsecond
	t.Cleanup(func() {
		coldCopySourceReadBackoffBase = base
		coldCopySourceReadBackoffCap = capDur
		coldCopySourceReadRetryAttempts = attempts
	})
}

// TestSourceReadRetry_RecoversAfterTransientDrops pins the happy path: a
// classified-retriable source-read error on the first two attempts opens a
// fresh reader (closing each) and retries; the third attempt succeeds. The
// run never returns an error.
func TestSourceReadRetry_RecoversAfterTransientDrops(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	var attempts, fresh, closes int
	attempt := func(_ context.Context, _ ir.RowReader, _ bool) error {
		attempts++
		if attempts <= 2 {
			return fakeRetriableErr{msg: "mysql: rows iteration: invalid connection"}
		}
		return nil
	}
	freshReader := func(_ context.Context) (ir.RowReader, func(), error) {
		fresh++
		return noopRowReader{}, func() { closes++ }, nil
	}
	truncate := func(_ context.Context) error {
		return errors.New("truncate must not be called on chunk-cursor strategy")
	}

	err := copyTableWithSourceReadRetry(context.Background(), "documents",
		resumeFromChunkCursor, noopRowReader{}, false, attempt, freshReader, truncate, nil)
	if err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (2 dropped + 1 success)", attempts)
	}
	if fresh != 2 || closes != 2 {
		t.Errorf("fresh readers opened=%d closed=%d; want 2/2 (one per retry, each closed)", fresh, closes)
	}
}

// TestSourceReadRetry_FirstAttemptNonRetriableIsTerminal pins the safety
// boundary: a NON-retriable error on the first attempt returns immediately
// — no retry, no fresh reader, no truncate. A real decode/query fault must
// stay terminal exactly as today.
func TestSourceReadRetry_FirstAttemptNonRetriableIsTerminal(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	decodeErr := errors.New("mysql: column \"j\": decode json: invalid")
	var fresh, truncs int
	attempt := func(_ context.Context, _ ir.RowReader, _ bool) error { return decodeErr }
	freshReader := func(_ context.Context) (ir.RowReader, func(), error) {
		fresh++
		return noopRowReader{}, func() {}, nil
	}
	truncate := func(_ context.Context) error { truncs++; return nil }

	err := copyTableWithSourceReadRetry(context.Background(), "t",
		resumeTruncateRestart, noopRowReader{}, false, attempt, freshReader, truncate, nil)
	if !errors.Is(err, decodeErr) {
		t.Fatalf("expected the terminal decode error verbatim, got %v", err)
	}
	if fresh != 0 || truncs != 0 {
		t.Errorf("non-retriable error must not reopen (%d) or truncate (%d)", fresh, truncs)
	}
}

// TestSourceReadRetry_BudgetExhaustionIsLoudTerminal pins the bounded
// give-up: a source that keeps dropping is retried exactly
// coldCopySourceReadRetryAttempts times then surfaces a LOUD terminal
// error wrapping the most recent transient — never silent, never infinite.
func TestSourceReadRetry_BudgetExhaustionIsLoudTerminal(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)
	coldCopySourceReadRetryAttempts = 4 // shrink the count bound for the test

	last := fakeRetriableErr{msg: "mysql: rows iteration: connection reset by peer"}
	var attempts int
	attempt := func(_ context.Context, _ ir.RowReader, _ bool) error {
		attempts++
		return last
	}
	freshReader := func(_ context.Context) (ir.RowReader, func(), error) {
		return noopRowReader{}, func() {}, nil
	}

	err := copyTableWithSourceReadRetry(context.Background(), "events",
		resumeTruncateRestart, noopRowReader{}, false, attempt, freshReader,
		func(_ context.Context) error { return nil }, nil)
	if err == nil {
		t.Fatal("expected a loud terminal error on budget exhaustion, got nil")
	}
	if !errors.As(err, new(fakeRetriableErr)) {
		t.Errorf("terminal error must WRAP the last transient (%%w); got %v", err)
	}
	if !strings.Contains(err.Error(), "events") || !strings.Contains(err.Error(), "retry budget") {
		t.Errorf("terminal error must name the table + budget; got %v", err)
	}
	// initial attempt + (attempts-1) retries = coldCopySourceReadRetryAttempts.
	if attempts != coldCopySourceReadRetryAttempts {
		t.Errorf("attempts = %d, want %d", attempts, coldCopySourceReadRetryAttempts)
	}
}

// TestSourceReadRetry_CtxCancelDuringBackoffUnwinds pins prompt
// cancellation: when the parent ctx is cancelled while the loop is in its
// backoff sleep, the helper returns the ctx error promptly instead of
// finishing the budget.
func TestSourceReadRetry_CtxCancelDuringBackoffUnwinds(t *testing.T) {
	captureSlog(t)
	// A real (non-microsecond) backoff so the ctx-cancel races the sleep.
	base, capDur := coldCopySourceReadBackoffBase, coldCopySourceReadBackoffCap
	coldCopySourceReadBackoffBase = 5 * time.Second
	coldCopySourceReadBackoffCap = 5 * time.Second
	t.Cleanup(func() {
		coldCopySourceReadBackoffBase = base
		coldCopySourceReadBackoffCap = capDur
	})

	ctx, cancel := context.WithCancel(context.Background())
	attempt := func(_ context.Context, _ ir.RowReader, _ bool) error {
		return fakeRetriableErr{msg: "mysql: rows iteration: i/o timeout"}
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- copyTableWithSourceReadRetry(ctx, "t", resumeTruncateRestart,
			noopRowReader{}, false, attempt,
			func(context.Context) (ir.RowReader, func(), error) { return noopRowReader{}, func() {}, nil },
			func(context.Context) error { return nil }, nil)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled on cancel-during-backoff, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retry did not unwind promptly on ctx cancel during backoff")
	}
}

// TestSourceReadRetry_TruncateRestartTruncatesAndForcesRestart pins the
// non-chunkable strategy: each retry truncates the target FIRST, then
// re-copies from a fresh reader. resuming stays at the run's flag (false)
// because the truncate already cleared the target.
func TestSourceReadRetry_TruncateRestartTruncatesAndForcesRestart(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	var truncs int
	var sawResuming []bool
	attempts := 0
	attempt := func(_ context.Context, _ ir.RowReader, resuming bool) error {
		sawResuming = append(sawResuming, resuming)
		attempts++
		if attempts == 1 {
			return fakeRetriableErr{msg: "mysql: rows iteration: broken pipe"}
		}
		return nil
	}
	freshReader := func(context.Context) (ir.RowReader, func(), error) {
		return noopRowReader{}, func() {}, nil
	}
	truncate := func(context.Context) error { truncs++; return nil }

	if err := copyTableWithSourceReadRetry(context.Background(), "logs",
		resumeTruncateRestart, noopRowReader{}, false, attempt, freshReader, truncate, nil); err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
	if truncs != 1 {
		t.Errorf("truncate calls = %d, want 1 (one per retry before re-copy)", truncs)
	}
	// First attempt + one retry; both at resuming=false (truncate-restart
	// never forces resume).
	if len(sawResuming) != 2 || sawResuming[0] || sawResuming[1] {
		t.Errorf("resuming flags = %v; want [false false] for truncate-restart", sawResuming)
	}
}

// TestSourceReadRetry_ChunkCursorForcesResume pins the keyset-chunked
// strategy: the RETRY attempt is invoked with resuming=true so the chunk
// copy continues from chunk.LastPK (WHERE pk > LastPK) — never a truncate.
func TestSourceReadRetry_ChunkCursorForcesResume(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	var sawResuming []bool
	attempts := 0
	attempt := func(_ context.Context, _ ir.RowReader, resuming bool) error {
		sawResuming = append(sawResuming, resuming)
		attempts++
		if attempts == 1 {
			return fakeRetriableErr{msg: "mysql: rows iteration: invalid connection"}
		}
		return nil
	}
	truncate := func(context.Context) error {
		t.Fatal("chunk-cursor strategy must NEVER truncate (it resumes from LastPK)")
		return nil
	}
	if err := copyTableWithSourceReadRetry(context.Background(), "documents",
		resumeFromChunkCursor, noopRowReader{}, false, attempt,
		func(context.Context) (ir.RowReader, func(), error) { return noopRowReader{}, func() {}, nil },
		truncate, nil); err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
	if len(sawResuming) != 2 || sawResuming[0] /* run flag */ || !sawResuming[1] /* forced */ {
		t.Errorf("resuming flags = %v; want [false true] (retry forces resume to read from LastPK)", sawResuming)
	}
}

// TestSourceReadRetry_ReopenFailureRidesBudget pins that a FRESH-reader
// open failure is itself classified on the next iteration: a retriable
// open error rides the same budget (it does not short-circuit), and the
// recovery once the open succeeds still converges.
func TestSourceReadRetry_ReopenFailureRidesBudget(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	attempts := 0
	attempt := func(_ context.Context, _ ir.RowReader, _ bool) error {
		attempts++
		if attempts == 1 {
			return fakeRetriableErr{msg: "mysql: rows iteration: invalid connection"}
		}
		return nil // succeeds once a reader is finally handed in
	}
	opens := 0
	freshReader := func(context.Context) (ir.RowReader, func(), error) {
		opens++
		if opens == 1 {
			// The reconnect itself fails with a retriable shape (source
			// still unreachable) — must ride the budget, not return.
			return nil, nil, fakeRetriableErr{msg: "dial tcp: connection refused"}
		}
		return noopRowReader{}, func() {}, nil
	}
	if err := copyTableWithSourceReadRetry(context.Background(), "t",
		resumeFromChunkCursor, noopRowReader{}, false, attempt, freshReader,
		func(context.Context) error { return nil }, nil); err != nil {
		t.Fatalf("expected eventual recovery after a transient reopen failure, got %v", err)
	}
	if opens != 2 {
		t.Errorf("reader opens = %d, want 2 (first reopen failed-retriable, second succeeded)", opens)
	}
}

// TestIsRetriableSourceReadError_Classification pins the classifier seam:
// only an ir.RetriableError (with Retriable()==true) is retriable; a plain
// error (a decode fault) and nil are not.
func TestIsRetriableSourceReadError_Classification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain decode", errors.New("mysql: column x: decode failed"), false},
		{"retriable", fakeRetriableErr{msg: "invalid connection"}, true},
		{"wrapped retriable", errorsJoin(errors.New("source row stream for table \"t\" failed"), fakeRetriableErr{msg: "EOF"}), true},
	}
	for _, c := range cases {
		if got := isRetriableSourceReadError(c.err); got != c.want {
			t.Errorf("%s: isRetriableSourceReadError = %v, want %v", c.name, got, c.want)
		}
	}
}

// errorsJoin wraps wrapped under outer so errors.As walks to wrapped —
// mirrors readerStreamErr's `%w` wrap of the reader's classified Err().
func errorsJoin(outer, wrapped error) error {
	return wrappedErr{outer: outer, inner: wrapped}
}

type wrappedErr struct {
	outer error
	inner error
}

func (w wrappedErr) Error() string { return w.outer.Error() + ": " + w.inner.Error() }
func (w wrappedErr) Unwrap() error { return w.inner }
