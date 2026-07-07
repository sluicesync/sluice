// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
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

// idleProgressWrapper is a test double satisfying BOTH [ir.RetriableError]
// and [ir.LivenessProgressTimeoutError] with IsIdleProgressTimeout()==true —
// the engine-neutral shape a VStream Phase-2 "established then went idle"
// progress timeout (mysql.vstreamProgressTimeoutError) produces, without
// importing the mysql engine package (which would create a cycle).
type idleProgressWrapper struct {
	err error
}

func (e *idleProgressWrapper) Error() string               { return e.err.Error() }
func (e *idleProgressWrapper) Unwrap() error               { return e.err }
func (e *idleProgressWrapper) Retriable() bool             { return true }
func (e *idleProgressWrapper) RetryHint() time.Duration    { return 0 }
func (e *idleProgressWrapper) IsIdleProgressTimeout() bool { return true }

// Compile-time proof the double stays in sync with the marker interface, so a
// signature drift fails the build rather than silently skipping the exemption.
var _ ir.LivenessProgressTimeoutError = (*idleProgressWrapper)(nil)

// idleProgressErr builds the benign, retriable idle-progress-timeout error
// (marker present) an established-then-idle CDC reconnect surfaces.
func idleProgressErr() error {
	return &idleProgressWrapper{err: errors.New(
		"mysql/vstream: stream produced no events for 45s after data had been flowing; reconnecting from the last position",
	)}
}

// neverEstablishedErr builds a NO-MARKER retriable error — the shape a
// Phase-1 liveness timeout / open-or-connection failure surfaces (the stream
// never established). It must still count toward the give-up budget.
func neverEstablishedErr() error {
	return &retriableWrapper{err: errors.New(
		"mysql/vstream: no events within 30s of opening the stream",
	)}
}

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
//  2. The fix-shape: classifyRetriable returns true so the streamer's
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

	t.Run("fix-shape: classifyRetriable returns true", func(t *testing.T) {
		re, retriable := classifyRetriable(wrapped)
		if !retriable {
			t.Fatal("wrapped retriable not classified as retriable; runWithRetry would exit instead of retrying")
		}
		if re == nil || !re.Retriable() {
			t.Errorf("matched wrapper but Retriable()==false; got %+v", re)
		}
	})

	t.Run("fix order: the streamer must check retriable BEFORE ctx-termination", func(t *testing.T) {
		// Simulate the post-fix order. Pre-fix swapped the if-blocks.
		_, retriable := classifyRetriable(wrapped)
		ctxTerm := errors.Is(wrapped, context.Canceled) || errors.Is(wrapped, context.DeadlineExceeded)
		if !retriable {
			t.Fatal("test premise: classifyRetriable should fire on wrapped retriable")
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
			if _, ok := classifyRetriable(c.err); ok {
				t.Errorf("%v misclassified as retriable; would loop forever instead of cleanly returning", c.err)
			}
		})
	}
}

// TestRunWithRetry_IdleProgressTimeoutSurvivesBudget pins loose end 2b (case
// a): a healthy-but-IDLE source whose every reconnect surfaces the Phase-2
// established-then-idle progress timeout (marker present) must survive
// INDEFINITELY — its benign idle reconnects must NOT advance the give-up
// budget. foundPos=true models the CDC phase (a cdc-state row exists with a
// stable token, so the position never advances and the progressed-reset never
// fires); pre-fix this was GUARANTEED to give up after ApplyRetryAttempts idle
// cycles (~6 min against an idle PlanetScale source).
func TestRunWithRetry_IdleProgressTimeoutSurvivesBudget(t *testing.T) {
	const attempts = 8
	const idleCycles = attempts*3 + 1 // far past the budget
	s := fastRetryStreamer(t, true /* CDC phase */, attempts)
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		if calls <= idleCycles {
			return idleProgressErr()
		}
		return nil // the source finally emits an event; the run completes
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error; an idle-but-healthy source must survive %d idle reconnects (> the %d-attempt budget): %v",
			idleCycles, attempts, err)
	}
	if calls != idleCycles+1 {
		t.Fatalf("runOnce called %d times; want %d — idle-progress timeouts must never exhaust the budget", calls, idleCycles+1)
	}
}

// TestRunWithRetry_NeverEstablishedExhaustsBudget pins the loud-failure
// invariant (case b): a stream that can NEVER establish — a Phase-1 liveness
// timeout / open error on every reconnect (NO marker) — must STILL exhaust the
// budget and fail loudly, never loop forever. foundPos=true (CDC phase, stable
// token) so the only thing keeping it alive would be a bogus marker exemption;
// there is none, so it gives up after exactly ApplyRetryAttempts.
func TestRunWithRetry_NeverEstablishedExhaustsBudget(t *testing.T) {
	const attempts = 5
	s := fastRetryStreamer(t, true, attempts)
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		return neverEstablishedErr() // never establishes; no idle marker
	}
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil; a stream that can never establish must fail loudly after the budget")
	}
	if calls != attempts {
		t.Fatalf("runOnce called %d times; a never-establishing stream must give up at the %d-attempt budget", calls, attempts)
	}
	if !strings.Contains(err.Error(), "retry budget exhausted") {
		t.Errorf("want a loud budget-exhaustion error; got %v", err)
	}
}

// TestRunWithRetry_MixedIdleAndRealStillGivesUp pins case (c): real failures
// interspersed with benign idle-progress timeouts still PROGRESS toward the
// cap. The idle timeouts are skipped (neither increment nor reset), so the
// real failures accumulate across them and the budget still exhausts — the
// exemption must not become a loophole that resets a genuine failure streak.
func TestRunWithRetry_MixedIdleAndRealStillGivesUp(t *testing.T) {
	const attempts = 3
	s := fastRetryStreamer(t, true, attempts)
	var calls int
	s.runOnceFn = func(context.Context) error {
		calls++
		// Alternate benign idle (odd calls, skipped) with real failures
		// (even calls, counted): idle,real,idle,real,idle,real — the 3rd real
		// hits the cap on call 6.
		if calls%2 == 1 {
			return idleProgressErr()
		}
		return neverEstablishedErr()
	}
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil; real failures interspersed with benign idles must still exhaust the budget")
	}
	if calls != 6 {
		t.Fatalf("runOnce called %d times; want 6 (3 skipped idles + 3 counted real failures reaching the %d-attempt cap)", calls, attempts)
	}
	if !strings.Contains(err.Error(), "retry budget exhausted") {
		t.Errorf("want a loud budget-exhaustion error; got %v", err)
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
