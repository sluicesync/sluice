// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package appliercontrol implements the AIMD (Additive-Increase /
// Multiplicative-Decrease) controller that governs --apply-batch-size
// on a per-stream basis (ADR-0052). Engine appliers consult the
// controller via the optional [ir.BatchSizeProvider] / [ir.BatchObserver]
// surfaces; the controller is engine-neutral and lives outside the
// engine packages so its math can be exercised in isolation.
//
// The controller's two control inputs (ADR-0052 DP-4 (a) + (b)):
//
//   - Latency p95 over the last N batches (default N=50).
//   - Retriable-error rate over a rolling 1-minute window.
//
// AI by +5 rows when p95 < target AND retry rate ≤ threshold AND not
// in cool-off; MD to size/2 (rounded down, floored at 1) when p95 ≥
// target OR retry rate > threshold. After any MD the controller enters
// a 20-successful-batch cool-off during which AI is suppressed.
package appliercontrol

import (
	"math"
	"sort"
	"time"
)

// LatencyWindow is a bounded sliding window of per-batch latencies
// (the wall-clock duration from "batch begin" to "tx commit returned").
// It exposes a p95 read-out for the controller's AI/MD decision.
//
// Count-based eviction (no time-based expiry): the controller's natural
// cadence is per-batch, so a 50-sample window covers the most recent 50
// batches regardless of how busy or quiet the stream is. A quiet stream
// keeps an older window — which is correct, since the controller has
// no fresher signal to react to than the most recent samples it's seen.
//
// Concurrency: not safe for concurrent use. The controller's design is
// single-goroutine per stream (each applier owns its controller; appliers
// are single-goroutine consumers of their change channels).
type LatencyWindow struct {
	cap     int
	samples []time.Duration
	// head is the index of the oldest sample. When len(samples) == cap
	// the window is full and Observe wraps; when it's less than cap the
	// window is filling and Observe appends.
	head int
}

// NewLatencyWindow returns an empty window with the given capacity. A
// non-positive capacity falls back to the ADR-0052 default of 50.
func NewLatencyWindow(capacity int) *LatencyWindow {
	if capacity <= 0 {
		capacity = 50
	}
	return &LatencyWindow{
		cap:     capacity,
		samples: make([]time.Duration, 0, capacity),
	}
}

// Observe records one batch's latency. Returns the window for fluent
// chaining (rare; the controller's call site discards).
func (w *LatencyWindow) Observe(d time.Duration) *LatencyWindow {
	if d < 0 {
		// Defensive: a negative latency is a clock-skew artefact, not
		// real data. Clamp to zero rather than reject so the window's
		// length matches the controller's batch count.
		d = 0
	}
	if len(w.samples) < w.cap {
		w.samples = append(w.samples, d)
		return w
	}
	w.samples[w.head] = d
	w.head = (w.head + 1) % w.cap
	return w
}

// Len returns the number of samples currently held.
func (w *LatencyWindow) Len() int {
	return len(w.samples)
}

// Cap returns the configured capacity.
func (w *LatencyWindow) Cap() int {
	return w.cap
}

// P95 returns the 95th-percentile latency over the current samples.
// On an empty window returns 0.
//
// Implementation: linear interpolation between the two surrounding
// samples (the "nearest-rank" variant is too coarse on small windows;
// the controller cares about the value, not the rank). The window is
// small enough (≤ 50 typically) that a fresh sort per call is cheaper
// than maintaining a heap-augmented structure.
func (w *LatencyWindow) P95() time.Duration {
	return w.percentile(0.95)
}

// percentile is the shared interpolating-quantile primitive used by
// P95. Returns 0 on an empty window.
func (w *LatencyWindow) percentile(q float64) time.Duration {
	n := len(w.samples)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return w.samples[0]
	}
	sorted := make([]time.Duration, n)
	copy(sorted, w.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	// Linear interpolation between the floor and ceil ranks.
	rank := q * float64(n-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	loV := float64(sorted[lo])
	hiV := float64(sorted[hi])
	frac := rank - float64(lo)
	return time.Duration(loV + (hiV-loV)*frac)
}

// Reset clears all samples. Used by the controller on engine-side
// teardown if it ever wires that; today the controller's lifetime
// matches the applier's so the reset is unused.
//
//nolint:unused // kept for symmetry with the controller's lifecycle
func (w *LatencyWindow) Reset() {
	w.samples = w.samples[:0]
	w.head = 0
}
