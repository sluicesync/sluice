// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"testing"
	"time"
)

// TestTargetHealthSnapshot_Fresh pins the freshness window boundary
// cases that every ADR-0107 consumer relies on to decide "act vs
// degrade to reactive". A snapshot that is NOT fresh must be treated as
// no-signal — the proactive damp must never act on a possibly-wrong old
// reading.
func TestTargetHealthSnapshot_Fresh(t *testing.T) {
	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	window := 60 * time.Second

	cases := []struct {
		name      string
		sampledAt time.Time
		now       time.Time
		window    time.Duration
		want      bool
	}{
		{
			name:      "zero SampledAt is never fresh",
			sampledAt: time.Time{},
			now:       base,
			window:    window,
			want:      false,
		},
		{
			name:      "zero window is never fresh",
			sampledAt: base,
			now:       base,
			window:    0,
			want:      false,
		},
		{
			name:      "negative window is never fresh",
			sampledAt: base,
			now:       base.Add(time.Second),
			window:    -time.Second,
			want:      false,
		},
		{
			name:      "sampled exactly now is fresh",
			sampledAt: base,
			now:       base,
			window:    window,
			want:      true,
		},
		{
			name:      "within window is fresh",
			sampledAt: base,
			now:       base.Add(30 * time.Second),
			window:    window,
			want:      true,
		},
		{
			name:      "exactly at the window edge is fresh (inclusive)",
			sampledAt: base,
			now:       base.Add(window),
			window:    window,
			want:      true,
		},
		{
			name:      "one nanosecond past the window is stale",
			sampledAt: base,
			now:       base.Add(window + time.Nanosecond),
			window:    window,
			want:      false,
		},
		{
			name:      "a snapshot from the future is fresh (clock skew tolerated)",
			sampledAt: base.Add(time.Second),
			now:       base,
			window:    window,
			want:      true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := TargetHealthSnapshot{SampledAt: tc.sampledAt}
			if got := snap.Fresh(tc.now, tc.window); got != tc.want {
				t.Fatalf("Fresh(%v, %v) = %v; want %v", tc.now, tc.window, got, tc.want)
			}
		})
	}
}
