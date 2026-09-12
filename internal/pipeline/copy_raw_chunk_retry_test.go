// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// retryingImporter fails its first failures imports with err, then succeeds,
// counting attempts. The exporter side is the existing fakeRawExporter, which
// writes nothing and returns nil — enough for the pipe to reach EOF.
type retryingImporter struct {
	fakeRawImporter
	failures int32
	err      error
	attempts atomic.Int32
	rows     int64
}

func (r *retryingImporter) ImportRawCopy(_ context.Context, _ *ir.Table, _ ir.RawCopyFormat, src io.Reader) (int64, error) {
	n := r.attempts.Add(1)
	// Drain so the exporter goroutine's writes always complete; the real
	// importer does the same and a test that skipped it could deadlock.
	_, _ = io.Copy(io.Discard, src)
	if n <= r.failures {
		return 0, r.err
	}
	return r.rows, nil
}

// retriableTestError satisfies ir.RetriableError, which is how a real engine
// tells the pipeline that 53100 (storage grow) or 08006 (broken pipe) is worth
// riding out. Using the interface rather than a message shape keeps this test
// grading the pipeline's policy instead of a string.
type retriableTestError struct{ msg string }

func (e *retriableTestError) Error() string            { return e.msg }
func (e *retriableTestError) Retriable() bool          { return true }
func (e *retriableTestError) RetryHint() time.Duration { return 0 }

// withFastRawCopyRetries shrinks the retry envelope so the suite stays fast.
// Production never mutates these (see the vars' doc).
func withFastRawCopyRetries(t *testing.T) {
	t.Helper()
	wall, attempts := rawCopyRetryMaxWall, rawCopyRetryAttempts
	baseBackoff, capBackoff := chunkOpenRetryBackoffBase, chunkOpenRetryBackoffCap
	rawCopyRetryMaxWall = 5 * time.Second
	chunkOpenRetryBackoffBase = time.Millisecond
	chunkOpenRetryBackoffCap = 2 * time.Millisecond
	t.Cleanup(func() {
		rawCopyRetryMaxWall, rawCopyRetryAttempts = wall, attempts
		chunkOpenRetryBackoffBase, chunkOpenRetryBackoffCap = baseBackoff, capBackoff
	})
}

// TestRunRawCopyChunkWithRetry pins the behaviour whose ABSENCE failed two
// live migrations on 2026-09-12.
//
// Copying a 29 GB table into a freshly-created PlanetScale Neki database died
// twice — at 116s with 53100 (`could not extend file … No space left on
// device`, the platform's storage auto-grow being reactive rather than
// predictive) and at 191s with 08006 (`write: broken pipe`, primary at 100%
// CPU). Both SQLSTATEs were ALREADY classified retriable and ADR-0110's
// grow-gate tripped correctly on both runs. The copies died anyway, because
// `copyChunkRaw` called runRawCopyChunk exactly once: classification decides
// whether to retry, and this lane had nothing to retry with.
//
// The typed lane's sibling rode the same transients out via
// postgres.RowWriter.copyChunkWithRetry. This is the missing parity.
func TestRunRawCopyChunkWithRetry(t *testing.T) {
	withFastRawCopyRetries(t)

	table := &ir.Table{Name: "t"}
	ctx := context.Background()

	t.Run("a classified transient is retried until it clears", func(t *testing.T) {
		imp := &retryingImporter{failures: 3, err: &retriableTestError{msg: "could not extend file: No space left on device"}, rows: 42}

		rows, err := runRawCopyChunkWithRetry(ctx, fakeRawExporter{}, imp, table, nil, ir.RawCopyText, -1)
		if err != nil {
			t.Fatalf("a transient that clears on the 4th attempt should succeed, got: %v", err)
		}
		if rows != 42 {
			t.Errorf("rows = %d, want 42 (the successful attempt's count must be returned, not a stale one)", rows)
		}
		if got := imp.attempts.Load(); got != 4 {
			t.Errorf("attempts = %d, want 4 (3 failures + 1 success)", got)
		}
	})

	t.Run("a TERMINAL error is returned immediately and never retried", func(t *testing.T) {
		// The anti-over-match arm. A retry loop that rode out everything
		// would turn a deterministic fault into a 30-minute stall, and would
		// make the cell above pass for the wrong reason.
		sentinel := errors.New("column \"x\" does not exist")
		imp := &retryingImporter{failures: 100, err: sentinel}

		_, err := runRawCopyChunkWithRetry(ctx, fakeRawExporter{}, imp, table, nil, ir.RawCopyText, -1)
		if !errors.Is(err, sentinel) {
			t.Fatalf("a terminal error must be returned unchanged, got: %v", err)
		}
		if got := imp.attempts.Load(); got != 1 {
			t.Errorf("attempts = %d, want 1 — a terminal fault must fail as fast as it did before this retry existed", got)
		}
	})

	t.Run("budget exhaustion fails LOUDLY, wrapping the last transient", func(t *testing.T) {
		sentinel := &retriableTestError{msg: "broken pipe"}
		imp := &retryingImporter{failures: 1 << 30, err: sentinel}

		_, err := runRawCopyChunkWithRetry(ctx, fakeRawExporter{}, imp, table, nil, ir.RawCopyText, -1)
		if err == nil {
			t.Fatal("an endless transient must eventually fail — never retry forever, never exit 0")
		}
		if !errors.Is(err, error(sentinel)) {
			t.Errorf("the exhaustion error must wrap the last transient so the cause stays visible, got: %v", err)
		}
		if !strings.Contains(err.Error(), "retry exhausted") {
			t.Errorf("the exhaustion error should say so plainly, got: %v", err)
		}
		if got := imp.attempts.Load(); got < 2 {
			t.Errorf("attempts = %d, want >1 — it should have actually retried before giving up", got)
		}
	})

	t.Run("a cancelled context stops promptly and does not spend the budget", func(t *testing.T) {
		cctx, cancel := context.WithCancel(context.Background())
		imp := &retryingImporter{failures: 1 << 30, err: &retriableTestError{msg: "broken pipe"}}
		cancel()

		start := time.Now()
		if _, err := runRawCopyChunkWithRetry(cctx, fakeRawExporter{}, imp, table, nil, ir.RawCopyText, -1); err == nil {
			t.Fatal("a cancelled run must return an error")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("cancel took %v to surface — a Ctrl-C must not wait out the retry envelope", elapsed)
		}
		if got := imp.attempts.Load(); got > 1 {
			t.Errorf("attempts = %d, want 1 — a cancelled run must not keep retrying", got)
		}
	})
}
