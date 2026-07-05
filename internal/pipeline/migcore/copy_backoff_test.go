// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"
	"time"
)

// testBackoffPolicy is a small, deterministic policy for the pure-decision
// tests: 3 retries, 10ms base doubling to a 40ms cap, 100ms total-wait.
var testBackoffPolicy = CopyBackoffPolicy{
	MaxRetries:   3,
	BaseDelay:    10 * time.Millisecond,
	MaxDelay:     40 * time.Millisecond,
	MaxTotalWait: 100 * time.Millisecond,
}

// TestNextCopyBackoff_HalvesAndFloors pins the multiplicative-decrease:
// each event halves the effective parallelism, flooring at 1.
func TestNextCopyBackoff_HalvesAndFloors(t *testing.T) {
	cases := []struct {
		current int
		want    int
	}{
		{8, 4},
		{4, 2},
		{3, 1}, // integer halve: 3/2 = 1
		{2, 1},
		{1, 1}, // already at floor, stays at floor
	}
	for _, tc := range cases {
		d := NextCopyBackoff(tc.current, 1, 0, testBackoffPolicy)
		if d.GiveUp {
			t.Fatalf("current=%d attempt=1: unexpected give-up", tc.current)
		}
		if d.NextParallelism != tc.want {
			t.Errorf("NextCopyBackoff(%d) parallelism = %d, want %d", tc.current, d.NextParallelism, tc.want)
		}
	}
}

// TestNextCopyBackoff_ExponentialDelayCapped pins the bounded exponential
// backoff: BaseDelay * 2^(attempt-1), capped at MaxDelay.
func TestNextCopyBackoff_ExponentialDelayCapped(t *testing.T) {
	// Use a high retry/total-wait bound so only the delay shape is under
	// test here, not the give-up triggers.
	p := CopyBackoffPolicy{MaxRetries: 100, BaseDelay: 10 * time.Millisecond, MaxDelay: 40 * time.Millisecond, MaxTotalWait: time.Hour}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond}, // hits the cap
		{4, 40 * time.Millisecond}, // stays at the cap
		{9, 40 * time.Millisecond}, // far past the cap, no overflow
	}
	for _, tc := range cases {
		d := NextCopyBackoff(8, tc.attempt, 0, p)
		if d.GiveUp {
			t.Fatalf("attempt=%d: unexpected give-up", tc.attempt)
		}
		if d.Delay != tc.want {
			t.Errorf("attempt=%d delay = %s, want %s", tc.attempt, d.Delay, tc.want)
		}
	}
}

// TestNextCopyBackoff_GivesUpOnMaxRetries pins the loud bounded give-up:
// once the attempt exceeds MaxRetries, the decision gives up.
func TestNextCopyBackoff_GivesUpOnMaxRetries(t *testing.T) {
	// MaxRetries=3, so attempts 1..3 proceed, attempt 4 gives up.
	for attempt := 1; attempt <= testBackoffPolicy.MaxRetries; attempt++ {
		if d := NextCopyBackoff(8, attempt, 0, testBackoffPolicy); d.GiveUp {
			t.Fatalf("attempt=%d (<= MaxRetries=%d): unexpected give-up", attempt, testBackoffPolicy.MaxRetries)
		}
	}
	if d := NextCopyBackoff(8, testBackoffPolicy.MaxRetries+1, 0, testBackoffPolicy); !d.GiveUp {
		t.Errorf("attempt=%d (> MaxRetries): expected give-up", testBackoffPolicy.MaxRetries+1)
	}
}

// TestNextCopyBackoff_GivesUpOnTotalWaitCeiling pins the second give-up
// trigger: even with retries remaining, once this attempt's delay would
// carry the accumulated wait past MaxTotalWait, give up.
func TestNextCopyBackoff_GivesUpOnTotalWaitCeiling(t *testing.T) {
	// MaxTotalWait=100ms. With 95ms already spent, attempt 1's 10ms delay
	// would reach 105ms > 100ms → give up, despite retries remaining.
	d := NextCopyBackoff(8, 1, 95*time.Millisecond, testBackoffPolicy)
	if !d.GiveUp {
		t.Errorf("prior=95ms + 10ms delay > 100ms cap: expected give-up, got %+v", d)
	}

	// With only 80ms spent, attempt 1's 10ms delay reaches 90ms < 100ms →
	// proceed.
	d = NextCopyBackoff(8, 1, 80*time.Millisecond, testBackoffPolicy)
	if d.GiveUp {
		t.Errorf("prior=80ms + 10ms delay < 100ms cap: expected proceed, got give-up")
	}
}

// TestDefaultCopyBackoffPolicy_IsBounded is a guard that the shipped
// policy can never spin forever: it has a positive retry bound and a
// positive total-wait ceiling.
func TestDefaultCopyBackoffPolicy_IsBounded(t *testing.T) {
	p := DefaultCopyBackoffPolicy
	if p.MaxRetries <= 0 {
		t.Errorf("default policy MaxRetries = %d, must be > 0 (no infinite spin)", p.MaxRetries)
	}
	if p.MaxTotalWait <= 0 {
		t.Errorf("default policy MaxTotalWait = %s, must be > 0 (bounded wall-clock)", p.MaxTotalWait)
	}
	if p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay {
		t.Errorf("default policy delays invalid: base=%s max=%s", p.BaseDelay, p.MaxDelay)
	}
}
