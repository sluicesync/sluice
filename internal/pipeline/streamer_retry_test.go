// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"
	"time"
)

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
