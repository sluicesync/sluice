// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// coalesceCountingHandler counts the coalescing-ratio INFO records the
// applier emits so a unit test can assert the rate-limiter's once-per-window
// behaviour. Safe for the W concurrent lanes a stress sub-test drives.
type coalesceCountingHandler struct {
	mu    sync.Mutex
	count int
}

func (h *coalesceCountingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *coalesceCountingHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "mysql: applier: coalescing ratio" {
		h.mu.Lock()
		h.count++
		h.mu.Unlock()
	}
	return nil
}

func (h *coalesceCountingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *coalesceCountingHandler) WithGroup(string) slog.Handler      { return h }

func (h *coalesceCountingHandler) total() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// TestNoteCoalescedFlush_CountsAndRateLimit pins both observability
// properties: the counters accumulate across every flush, and the INFO line
// fires at most once per window (one extra after the window elapses).
func TestNoteCoalescedFlush_CountsAndRateLimit(t *testing.T) {
	prev := slog.Default()
	h := &coalesceCountingHandler{}
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })

	now := time.Unix(1000, 0)
	coalesceClockForTest = func() time.Time { return now }
	t.Cleanup(func() { coalesceClockForTest = nil })

	a := &ChangeApplier{}
	ctx := context.Background()

	// First flush logs (lastCoalesceLogNanos == 0 always fires).
	a.noteCoalescedFlush(ctx, 10)
	// 100 more flushes within the same instant: no further log.
	for range 100 {
		a.noteCoalescedFlush(ctx, 10)
	}
	if got := h.total(); got != 1 {
		t.Fatalf("within window: logged %d times, want 1", got)
	}

	// Advance past the window: exactly one more line.
	now = now.Add(coalesceLogInterval + time.Second)
	a.noteCoalescedFlush(ctx, 10)
	if got := h.total(); got != 2 {
		t.Fatalf("after window: logged %d times, want 2", got)
	}

	// Counters accumulated across all 102 flushes (× 10 rows each).
	if rows := a.coalescedRows.Load(); rows != 1020 {
		t.Errorf("coalescedRows = %d, want 1020", rows)
	}
	if flushes := a.coalescedFlushes.Load(); flushes != 102 {
		t.Errorf("coalescedFlushes = %d, want 102", flushes)
	}
}

// TestNoteCoalescedFlush_IgnoresNonPositive verifies a defensive rows<=0
// flush neither bumps the counters nor fires the line.
func TestNoteCoalescedFlush_IgnoresNonPositive(t *testing.T) {
	prev := slog.Default()
	h := &coalesceCountingHandler{}
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })

	a := &ChangeApplier{}
	a.noteCoalescedFlush(context.Background(), 0)
	a.noteCoalescedFlush(context.Background(), -3)
	if h.total() != 0 {
		t.Errorf("rows<=0 flush logged %d times, want 0", h.total())
	}
	if a.coalescedFlushes.Load() != 0 || a.coalescedRows.Load() != 0 {
		t.Errorf("rows<=0 flush bumped counters: rows=%d flushes=%d",
			a.coalescedRows.Load(), a.coalescedFlushes.Load())
	}
}

func TestCoalescingRatio(t *testing.T) {
	cases := []struct {
		rows, flushes int64
		want          float64
	}{
		{0, 0, 0},
		{100, 0, 0}, // zero flushes guards against divide-by-zero
		{10, 1, 10},
		{248, 1, 248},
		{5, 10, 0.5},
		{1000, 4, 250},
	}
	for _, tc := range cases {
		if got := coalescingRatio(tc.rows, tc.flushes); got != tc.want {
			t.Errorf("coalescingRatio(%d,%d) = %v, want %v", tc.rows, tc.flushes, got, tc.want)
		}
	}
}

func TestShouldLogCoalescing(t *testing.T) {
	iv := int64(coalesceLogInterval)
	if !shouldLogCoalescing(0, 12345, iv) {
		t.Error("first line ever (last==0) should fire")
	}
	if shouldLogCoalescing(1000, 1000+iv-1, iv) {
		t.Error("within window should not fire")
	}
	if !shouldLogCoalescing(1000, 1000+iv, iv) {
		t.Error("at the window boundary should fire")
	}
	if !shouldLogCoalescing(1000, 1000+iv*5, iv) {
		t.Error("well past the window should fire")
	}
}

func TestCoalescingAssessment(t *testing.T) {
	cases := []struct {
		ratio float64
		want  string
	}{
		{248, "good"},
		{10, "good"},
		{5, "moderate"},
		{2, "moderate"},
		{1, "RTT-bound"},
		{0, "RTT-bound"},
	}
	for _, tc := range cases {
		if got := coalescingAssessment(tc.ratio); !strings.Contains(got, tc.want) {
			t.Errorf("coalescingAssessment(%v) = %q, want substring %q", tc.ratio, got, tc.want)
		}
	}
}
