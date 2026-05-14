// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// retriableWrapper is a test double satisfying [ir.RetriableError]
// that mirrors the engine-side `retriableMySQLError` /
// `retriablePGError` shape — wraps an underlying err via Unwrap and
// reports Retriable()==true. Used to construct the exact error shape
// the applier produces when --apply-exec-timeout fires, without
// importing the engine packages (which would create a cycle).
type retriableWrapper struct {
	err error
}

func (e *retriableWrapper) Error() string            { return e.err.Error() }
func (e *retriableWrapper) Unwrap() error            { return e.err }
func (e *retriableWrapper) Retriable() bool          { return true }
func (e *retriableWrapper) RetryHint() time.Duration { return 0 }

// TestErrIsRetriable_Bug57 pins the load-bearing classification
// contract for the v0.52.2 fix (Bug 57). Both runOnce and runWithRetry
// MUST check the retriable wrapper BEFORE any
// `errors.Is(err, context.DeadlineExceeded)` short-circuit — the
// applier classifier wraps the timeout-driven DeadlineExceeded as a
// retriable error, and `errors.Is`'s Unwrap walk reaches the inner
// DeadlineExceeded, so pre-v0.52.2 the streamer mistook the wrapped
// timeout for a clean ctx-shutdown and exited the retry loop.
//
// This test simultaneously asserts:
//
//  1. The bug-shape: errors.Is matches context.DeadlineExceeded
//     against a wrapped retriable error (the pre-fix trap).
//  2. The fix-shape: errIsRetriable returns true so the streamer's
//     reordered checks route through the retry loop.
func TestErrIsRetriable_Bug57(t *testing.T) {
	// Construct the exact error shape the applier produces when its
	// per-exec timeout fires: classifyApplierError wraps the inner
	// DeadlineExceeded as a retriable wrapper preserving Unwrap chain.
	inner := fmt.Errorf("mysql: applier: commit: %w", context.DeadlineExceeded)
	wrapped := &retriableWrapper{err: inner}

	t.Run("bug-shape: errors.Is sees DeadlineExceeded via Unwrap chain", func(t *testing.T) {
		if !errors.Is(wrapped, context.DeadlineExceeded) {
			t.Fatal("wrapped retriable should match DeadlineExceeded via errors.Is; the test premise is gone")
		}
	})

	t.Run("fix-shape: errIsRetriable returns true", func(t *testing.T) {
		var re ir.RetriableError
		if !errIsRetriable(wrapped, &re) {
			t.Fatal("wrapped retriable not classified as retriable; runWithRetry would exit instead of retrying")
		}
		if re == nil || !re.Retriable() {
			t.Errorf("matched wrapper but Retriable()==false; got %+v", re)
		}
	})

	t.Run("fix order: the streamer must check retriable BEFORE ctx-termination", func(t *testing.T) {
		// Simulate the post-fix order. Pre-fix swapped the if-blocks.
		var re ir.RetriableError
		retriable := errIsRetriable(wrapped, &re)
		ctxTerm := errors.Is(wrapped, context.Canceled) || errors.Is(wrapped, context.DeadlineExceeded)
		if !retriable {
			t.Fatal("test premise: errIsRetriable should fire on wrapped retriable")
		}
		if !ctxTerm {
			t.Fatal("test premise: errors.Is should still see DeadlineExceeded in chain (bug-shape preserved)")
		}
		// The streamer's post-fix logic: when BOTH are true, the
		// retriable branch wins (retry); only when retriable==false
		// AND ctxTerm==true do we short-circuit as clean shutdown.
		// This subtest is documentation-as-test for the reordering.
	})
}

// TestErrIsRetriable_NonRetriable covers the inverse: a bare
// DeadlineExceeded (genuine ctx termination, no retriable wrapper)
// MUST NOT be classified as retriable, so the streamer's clean-
// shutdown short-circuit still fires for the operator-Ctrl-C / sync-
// stop case.
func TestErrIsRetriable_NonRetriable(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"bare DeadlineExceeded", context.DeadlineExceeded},
		{"bare Canceled", context.Canceled},
		{"wrapped DeadlineExceeded (no retriable wrapper)", fmt.Errorf("ctx termination: %w", context.DeadlineExceeded)},
		{"plain error", errors.New("some non-retriable failure")},
		{"nil", nil},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var re ir.RetriableError
			if errIsRetriable(c.err, &re) {
				t.Errorf("%v misclassified as retriable; would loop forever instead of cleanly returning", c.err)
			}
		})
	}
}

// TestComputeRetryBackoff covers the ADR-0038 backoff schedule:
// exponential doubling from base, capped at max, with a hint floor
// that can only raise the value (never lower it) and is itself
// capped at max.
func TestComputeRetryBackoff(t *testing.T) {
	const (
		base       = 100 * time.Millisecond
		maxBackoff = 30 * time.Second
	)

	cases := []struct {
		name    string
		attempt int
		hint    time.Duration
		want    time.Duration
	}{
		{"attempt 1 = base", 1, 0, 100 * time.Millisecond},
		{"attempt 2 = base*2", 2, 0, 200 * time.Millisecond},
		{"attempt 3 = base*4", 3, 0, 400 * time.Millisecond},
		{"attempt 4 = base*8", 4, 0, 800 * time.Millisecond},
		{"attempt 5 = base*16", 5, 0, 1600 * time.Millisecond},
		{"attempt 6 = base*32", 6, 0, 3200 * time.Millisecond},
		{"attempt 7 = base*64", 7, 0, 6400 * time.Millisecond},
		{"attempt 8 = base*128", 8, 0, 12800 * time.Millisecond},
		{"attempt 9 = base*256", 9, 0, 25600 * time.Millisecond},
		{"attempt 10 capped at max", 10, 0, 30 * time.Second},
		{"attempt 50 still capped at max", 50, 0, 30 * time.Second},
		{"hint smaller than computed ignored", 5, 100 * time.Millisecond, 1600 * time.Millisecond},
		{"hint equal to computed ignored", 5, 1600 * time.Millisecond, 1600 * time.Millisecond},
		{"hint larger than computed wins", 3, 5 * time.Second, 5 * time.Second},
		{"hint larger than cap still capped at cap", 1, 60 * time.Second, 30 * time.Second},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := computeRetryBackoff(c.attempt, base, maxBackoff, c.hint)
			if got != c.want {
				t.Errorf("computeRetryBackoff(attempt=%d, hint=%v) = %v; want %v",
					c.attempt, c.hint, got, c.want)
			}
		})
	}
}

// TestComputeRetryBackoff_AttemptsBudget walks the full
// default-attempt schedule (8) and confirms the total wait the
// operator is committing to with default flags. Useful as a regression
// guard if the schedule's defaults are ever bumped — the
// commit log + the docs both promise "~4 minutes worst case" and
// this assertion pins that promise.
func TestComputeRetryBackoff_AttemptsBudget(t *testing.T) {
	const (
		base       = 100 * time.Millisecond
		maxBackoff = 30 * time.Second
		attempts   = 8
	)
	var total time.Duration
	for i := 1; i <= attempts; i++ {
		total += computeRetryBackoff(i, base, maxBackoff, 0)
	}
	// Schedule: 100ms + 200ms + 400ms + 800ms + 1.6s + 3.2s + 6.4s + 12.8s
	want := 25500 * time.Millisecond
	if total != want {
		t.Errorf("8-attempt default budget = %v; want %v (ADR-0038 promise: < 4 min)", total, want)
	}
	// Pin the "well under 4 minutes" property — the ADR's stated
	// upper bound. If a future change makes the worst-case longer,
	// the ADR needs updating too.
	if total > 4*time.Minute {
		t.Errorf("8-attempt budget exceeds 4-minute promise from ADR-0038: %v", total)
	}
}

// TestComputeRetryBackoff_TinyBase covers the "operator sets a very
// small base" edge case. The exponential schedule should still
// start at the chosen base and double cleanly.
func TestComputeRetryBackoff_TinyBase(t *testing.T) {
	const (
		base       = 10 * time.Millisecond
		maxBackoff = time.Second
	)
	got := computeRetryBackoff(1, base, maxBackoff, 0)
	if got != base {
		t.Errorf("attempt 1 = %v; want %v (base)", got, base)
	}
	got = computeRetryBackoff(2, base, maxBackoff, 0)
	if got != 20*time.Millisecond {
		t.Errorf("attempt 2 = %v; want 20ms", got)
	}
}
