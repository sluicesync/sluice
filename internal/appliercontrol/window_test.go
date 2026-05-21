// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliercontrol

import (
	"testing"
	"time"
)

func TestLatencyWindow_EmptyP95IsZero(t *testing.T) {
	w := NewLatencyWindow(10)
	if got := w.P95(); got != 0 {
		t.Fatalf("empty P95 = %v; want 0", got)
	}
}

func TestLatencyWindow_SingleSampleP95EqualsSample(t *testing.T) {
	w := NewLatencyWindow(10)
	w.Observe(7 * time.Second)
	if got := w.P95(); got != 7*time.Second {
		t.Fatalf("single-sample P95 = %v; want 7s", got)
	}
}

func TestLatencyWindow_P95UniformDistribution(t *testing.T) {
	// 20 samples from 1ms..20ms (ascending). p95 sits between the
	// 19th and 20th sample (ranks 18 and 19 with q=0.95 ×19=18.05),
	// linearly interpolated → 19.05ms.
	w := NewLatencyWindow(50)
	for i := 1; i <= 20; i++ {
		w.Observe(time.Duration(i) * time.Millisecond)
	}
	got := w.P95()
	want := 19*time.Millisecond + 50*time.Microsecond
	if got != want {
		t.Fatalf("P95 = %v; want %v", got, want)
	}
}

func TestLatencyWindow_BoundedCapacityWraps(t *testing.T) {
	w := NewLatencyWindow(5)
	// Fill with 1..5, then push 100..104 — the early 1..5 should be
	// evicted and the p95 should reflect the 100..104 distribution.
	for i := 1; i <= 5; i++ {
		w.Observe(time.Duration(i) * time.Millisecond)
	}
	for i := 100; i <= 104; i++ {
		w.Observe(time.Duration(i) * time.Millisecond)
	}
	if w.Len() != 5 {
		t.Fatalf("Len() = %d; want 5", w.Len())
	}
	got := w.P95()
	// With samples [100,101,102,103,104] sorted, rank=0.95×4=3.8, so
	// floor=103ms, ceil=104ms, frac=0.8 → 103.8ms.
	want := 103*time.Millisecond + 800*time.Microsecond
	if got != want {
		t.Fatalf("P95 after wrap = %v; want %v", got, want)
	}
}

func TestLatencyWindow_NegativeSamplesClampToZero(t *testing.T) {
	// Defensive: clock-skew artefacts shouldn't break the window.
	w := NewLatencyWindow(5)
	w.Observe(-1 * time.Second)
	w.Observe(2 * time.Millisecond)
	if w.Len() != 2 {
		t.Fatalf("Len() = %d; want 2", w.Len())
	}
	// Sorted samples: [0, 2ms]; p95 = 0 + 0.95×2ms = 1.9ms.
	if got := w.P95(); got != 1900*time.Microsecond {
		t.Fatalf("P95 with clamped negative = %v; want 1.9ms", got)
	}
}

func TestLatencyWindow_DefaultCapWhenNonPositive(t *testing.T) {
	w := NewLatencyWindow(0)
	if w.Cap() != 50 {
		t.Fatalf("default Cap() = %d; want 50", w.Cap())
	}
	w2 := NewLatencyWindow(-7)
	if w2.Cap() != 50 {
		t.Fatalf("negative-capacity default Cap() = %d; want 50", w2.Cap())
	}
}

func TestLatencyWindow_P95OnLatencyClimb(t *testing.T) {
	// Simulate a controlled climb: 40 samples at 100ms then 10 at 9s.
	// The p95 over 50 samples should hit a 9s region (the top 5% is
	// the high tail).
	w := NewLatencyWindow(50)
	for i := 0; i < 40; i++ {
		w.Observe(100 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		w.Observe(9 * time.Second)
	}
	got := w.P95()
	if got < 9*time.Second-50*time.Millisecond || got > 9*time.Second+50*time.Millisecond {
		t.Fatalf("P95 climbing = %v; want ~9s", got)
	}
}
