// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliercontrol

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// TestMain silences the controller's slog output during unit tests so
// the INFO/DEBUG decision lines don't drown out the test runner.
// Production code paths log via slog.Default; tests redirect to a
// discard handler so the assertion surface stays scannable.
//
// Don't `defer` the slog.SetDefault restore — os.Exit skips deferred
// calls, so the deferred restore would never run. The handler swap is
// global for the test binary's lifetime and that's fine.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// retriableErr is a minimal stand-in for ir.RetriableError used by
// the controller's isRetriableError walk. The unit tests deliberately
// don't import the ir package — keeping the controller engine-neutral
// is part of ADR-0052's contract.
type retriableErr struct{ inner error }

func (e retriableErr) Error() string            { return e.inner.Error() }
func (e retriableErr) Unwrap() error            { return e.inner }
func (e retriableErr) Retriable() bool          { return true }
func (e retriableErr) RetryHint() time.Duration { return 0 }

func mustController(t *testing.T, cfg Config) *Controller {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestController_DefaultsApplied(t *testing.T) {
	c := mustController(t, Config{StreamID: "s1", InitialSize: 25, Ceiling: 100, TargetLatency: 5 * time.Second})
	if got := c.NextBatchSize(); got != 25 {
		t.Fatalf("initial NextBatchSize = %d; want 25", got)
	}
}

func TestController_AdditiveIncreaseFiresUnderTarget(t *testing.T) {
	c := mustController(t, Config{
		StreamID:      "s",
		Floor:         1,
		Ceiling:       1000,
		InitialSize:   10,
		TargetLatency: 5 * time.Second,
		WindowSize:    10,
		AdditiveStep:  5,
	})
	ctx := context.Background()
	// First two batches are the "warming up" window (len < 3); size is held.
	// From batch 3 onward AI fires by +5 each (len = 3 at the AI check).
	for i := 0; i < 7; i++ {
		c.ObserveBatch(ctx, 100*time.Millisecond, 10, nil)
	}
	// Batches 1-2: warming up (size stays at 10).
	// Batch 3: AI → 15. Batch 4: AI → 20. Batch 5: AI → 25.
	// Batch 6: AI → 30. Batch 7: AI → 35.
	if got := c.NextBatchSize(); got != 35 {
		t.Fatalf("after 7 healthy batches NextBatchSize = %d; want 35", got)
	}
}

func TestController_MultiplicativeDecreaseFiresOverTarget(t *testing.T) {
	c := mustController(t, Config{
		StreamID:      "s",
		Floor:         1,
		Ceiling:       1000,
		InitialSize:   100,
		TargetLatency: 5 * time.Second,
		WindowSize:    10,
	})
	ctx := context.Background()
	// Three slow batches push p95 over the 5s target. The MD fires on
	// the third observation (3 samples ≥ window-minimum threshold).
	for i := 0; i < 3; i++ {
		c.ObserveBatch(ctx, 10*time.Second, 50, nil)
	}
	got := c.NextBatchSize()
	if got != 50 {
		t.Fatalf("after MD NextBatchSize = %d; want 50 (100/2)", got)
	}
	snap := c.Snapshot()
	if !snap.InCoolOff {
		t.Fatalf("Snapshot.InCoolOff = false; want true (post-MD)")
	}
	if snap.DecreasesTotal != 1 {
		t.Fatalf("Snapshot.DecreasesTotal = %d; want 1", snap.DecreasesTotal)
	}
}

func TestController_MDOnRetriableErrorThreshold(t *testing.T) {
	now := time.Now()
	clk := func() time.Time { return now }
	c := mustController(t, Config{
		StreamID:                "s",
		Floor:                   1,
		Ceiling:                 1000,
		InitialSize:             100,
		TargetLatency:           5 * time.Second,
		RetriableErrorThreshold: 3,
		RetriableErrorWindow:    time.Minute,
		Now:                     clk,
	})
	ctx := context.Background()
	err := retriableErr{inner: errors.New("transient")}
	// First three retries don't trip — threshold is "strictly greater
	// than 3 per window". The fourth in the same window does.
	c.ObserveBatch(ctx, time.Millisecond, 0, err)
	c.ObserveBatch(ctx, time.Millisecond, 0, err)
	c.ObserveBatch(ctx, time.Millisecond, 0, err)
	if got := c.NextBatchSize(); got != 100 {
		t.Fatalf("after 3 retries (== threshold) NextBatchSize = %d; want 100", got)
	}
	c.ObserveBatch(ctx, time.Millisecond, 0, err)
	if got := c.NextBatchSize(); got != 50 {
		t.Fatalf("after 4 retries (> threshold) NextBatchSize = %d; want 50", got)
	}
}

func TestController_RetryWindowExpiresOlderTimestamps(t *testing.T) {
	now := time.Now()
	clk := &fakeClock{t: now}
	c := mustController(t, Config{
		StreamID:                "s",
		Floor:                   1,
		Ceiling:                 1000,
		InitialSize:             100,
		TargetLatency:           5 * time.Second,
		RetriableErrorThreshold: 3,
		RetriableErrorWindow:    time.Minute,
		Now:                     clk.Now,
	})
	ctx := context.Background()
	err := retriableErr{inner: errors.New("transient")}
	// Three retries land at t=0.
	for i := 0; i < 3; i++ {
		c.ObserveBatch(ctx, time.Millisecond, 0, err)
	}
	// Advance well past the window — the next retry sees an empty
	// accumulator and the count is 1, no MD.
	clk.advance(2 * time.Minute)
	c.ObserveBatch(ctx, time.Millisecond, 0, err)
	if got := c.NextBatchSize(); got != 100 {
		t.Fatalf("after retry-window expiry NextBatchSize = %d; want 100 (no MD)", got)
	}
}

func TestController_CoolOffSuppressesAI(t *testing.T) {
	// Drive MD via the retry-rate path so the latency window doesn't
	// carry "slow" samples that would re-fire MD during cool-off.
	// Cool-off's contract is specifically about AI suppression — MD
	// remains active even in cool-off, by design.
	now := time.Now()
	clk := &fakeClock{t: now}
	c := mustController(t, Config{
		StreamID:                "s",
		Floor:                   1,
		Ceiling:                 1000,
		InitialSize:             100,
		TargetLatency:           5 * time.Second,
		WindowSize:              10,
		CoolOffBatches:          5,
		AdditiveStep:            5,
		RetriableErrorThreshold: 3,
		RetriableErrorWindow:    time.Minute,
		Now:                     clk.Now,
	})
	ctx := context.Background()
	// Force MD via retry-rate (4 retries within window).
	err := retriableErr{inner: errors.New("transient")}
	for i := 0; i < 4; i++ {
		c.ObserveBatch(ctx, time.Millisecond, 0, err)
	}
	if !c.Snapshot().InCoolOff {
		t.Fatalf("expected cool-off after MD")
	}
	postMD := c.NextBatchSize()
	if postMD != 50 {
		t.Fatalf("post-MD size = %d; want 50", postMD)
	}
	// Feed 5 healthy fast batches. Each decrements the cool-off
	// counter; AI does NOT fire while in cool-off. Size stays at
	// postMD across all 5 batches.
	for i := 0; i < 5; i++ {
		c.ObserveBatch(ctx, 10*time.Millisecond, 10, nil)
		if got := c.NextBatchSize(); got != postMD {
			t.Fatalf("cool-off batch %d: NextBatchSize = %d; want %d (no AI)", i+1, got, postMD)
		}
	}
	if c.Snapshot().InCoolOff {
		t.Fatalf("expected cool-off cleared after 5 successful batches")
	}
	// Post-cool-off: the next healthy batch can AI.
	c.ObserveBatch(ctx, 10*time.Millisecond, 10, nil)
	if got := c.NextBatchSize(); got != postMD+5 {
		t.Fatalf("first post-cool-off batch NextBatchSize = %d; want %d (AI by +5)", got, postMD+5)
	}
}

func TestController_FloorClampsAtOne(t *testing.T) {
	c := mustController(t, Config{
		StreamID:      "s",
		Floor:         1,
		Ceiling:       1000,
		InitialSize:   2,
		TargetLatency: 5 * time.Second,
		WindowSize:    10,
	})
	ctx := context.Background()
	// MD from size=2 → floor(2*0.5)=1 → never below 1.
	for i := 0; i < 3; i++ {
		c.ObserveBatch(ctx, 10*time.Second, 1, nil)
	}
	if got := c.NextBatchSize(); got != 1 {
		t.Fatalf("after MD-from-2 NextBatchSize = %d; want 1 (floor)", got)
	}
}

func TestController_CeilingClampsAI(t *testing.T) {
	c := mustController(t, Config{
		StreamID:      "s",
		Floor:         1,
		Ceiling:       12,
		InitialSize:   10,
		TargetLatency: 5 * time.Second,
		WindowSize:    10,
		AdditiveStep:  5,
	})
	ctx := context.Background()
	// Warm up the window (3 batches, size held).
	for i := 0; i < 3; i++ {
		c.ObserveBatch(ctx, 100*time.Millisecond, 10, nil)
	}
	// Now AI is unlocked. First AI: 10 → 15 → clamped to 12.
	c.ObserveBatch(ctx, 100*time.Millisecond, 10, nil)
	if got := c.NextBatchSize(); got != 12 {
		t.Fatalf("AI clamped NextBatchSize = %d; want 12 (ceiling)", got)
	}
	// Repeated healthy batches stay at the ceiling.
	for i := 0; i < 5; i++ {
		c.ObserveBatch(ctx, 100*time.Millisecond, 12, nil)
	}
	if got := c.NextBatchSize(); got != 12 {
		t.Fatalf("AI saturated NextBatchSize = %d; want 12 (ceiling)", got)
	}
}

func TestController_PerStreamIsolation(t *testing.T) {
	c1 := mustController(t, Config{StreamID: "a", Floor: 1, Ceiling: 100, InitialSize: 50, TargetLatency: 5 * time.Second})
	c2 := mustController(t, Config{StreamID: "b", Floor: 1, Ceiling: 100, InitialSize: 50, TargetLatency: 5 * time.Second})
	ctx := context.Background()
	// Force c1 into MD via slow batches.
	for i := 0; i < 3; i++ {
		c1.ObserveBatch(ctx, 10*time.Second, 50, nil)
	}
	if got := c1.NextBatchSize(); got != 25 {
		t.Fatalf("c1 NextBatchSize = %d; want 25 (post-MD)", got)
	}
	// c2 stays untouched.
	if got := c2.NextBatchSize(); got != 50 {
		t.Fatalf("c2 NextBatchSize = %d; want 50 (untouched)", got)
	}
	// AI on c2 alone shouldn't affect c1. Warm up c2's window first
	// (2 samples), then the third triggers AI.
	for i := 0; i < 3; i++ {
		c2.ObserveBatch(ctx, 100*time.Millisecond, 50, nil)
	}
	if got := c2.NextBatchSize(); got != 55 {
		t.Fatalf("c2 NextBatchSize after AI = %d; want 55", got)
	}
	if got := c1.NextBatchSize(); got != 25 {
		t.Fatalf("c1 NextBatchSize after c2-AI = %d; want still 25", got)
	}
}

func TestController_SnapshotReflectsState(t *testing.T) {
	c := mustController(t, Config{StreamID: "s", Floor: 1, Ceiling: 100, InitialSize: 25, TargetLatency: 5 * time.Second, WindowSize: 10})
	ctx := context.Background()
	c.ObserveBatch(ctx, 100*time.Millisecond, 10, nil)
	snap := c.Snapshot()
	if snap.StreamID != "s" {
		t.Fatalf("Snapshot.StreamID = %q; want %q", snap.StreamID, "s")
	}
	if snap.CurrentSize <= 0 {
		t.Fatalf("Snapshot.CurrentSize = %d; want > 0", snap.CurrentSize)
	}
	if snap.P95 != 100*time.Millisecond {
		t.Fatalf("Snapshot.P95 = %v; want 100ms (single observation)", snap.P95)
	}
}

func TestController_RejectsNegativeTargetLatency(t *testing.T) {
	if _, err := New(Config{StreamID: "s", TargetLatency: -1 * time.Second}); err == nil {
		t.Fatalf("New with negative TargetLatency = nil; want error")
	}
}

func TestController_NonRetriableErrorDoesNotMD(t *testing.T) {
	c := mustController(t, Config{
		StreamID:                "s",
		Floor:                   1,
		Ceiling:                 1000,
		InitialSize:             100,
		TargetLatency:           5 * time.Second,
		RetriableErrorThreshold: 3,
		RetriableErrorWindow:    time.Minute,
	})
	ctx := context.Background()
	plain := errors.New("not retriable")
	for i := 0; i < 10; i++ {
		c.ObserveBatch(ctx, time.Millisecond, 0, plain)
	}
	if got := c.NextBatchSize(); got != 100 {
		t.Fatalf("non-retriable errors NextBatchSize = %d; want 100 (no MD)", got)
	}
}

func TestController_ByteCapHintRateLimited(t *testing.T) {
	now := time.Now()
	clk := &fakeClock{t: now}
	c := mustController(t, Config{
		StreamID:       "s",
		Floor:          1,
		Ceiling:        1000,
		InitialSize:    100,
		TargetLatency:  5 * time.Second,
		CoolOffBatches: 20,
		Now:            clk.Now,
	})
	ctx := context.Background()
	// First call records the hint; second within the rate-limit
	// window is a no-op (we can't observe the slog output directly
	// without a captured handler, but a NoteByteCapDominant call
	// should still not panic and the controller should remain in a
	// valid state). The state-only invariant we check is that the
	// internal lastByteCapHint advances on first call.
	c.NoteByteCapDominant(ctx, 5, 80<<20, 64<<20)
	c.mu.Lock()
	first := c.lastByteCapHint
	c.mu.Unlock()
	if first.IsZero() {
		t.Fatalf("first NoteByteCapDominant did not record lastByteCapHint")
	}
	// Repeat under the rate-limit window — timestamp must NOT advance.
	clk.advance(time.Second)
	c.NoteByteCapDominant(ctx, 5, 80<<20, 64<<20)
	c.mu.Lock()
	second := c.lastByteCapHint
	c.mu.Unlock()
	if !second.Equal(first) {
		t.Fatalf("second NoteByteCapDominant advanced lastByteCapHint; want suppressed by rate-limit")
	}
	// Advance past the rate-limit window — next call SHOULD log.
	clk.advance(time.Hour)
	c.NoteByteCapDominant(ctx, 5, 80<<20, 64<<20)
	c.mu.Lock()
	third := c.lastByteCapHint
	c.mu.Unlock()
	if !third.After(first) {
		t.Fatalf("third NoteByteCapDominant did not advance lastByteCapHint after window expiry")
	}
}

// fakeClock is a tiny test-only clock implementing the controller's
// Now hook. Concurrency-safe only insofar as the unit tests don't run
// the controller from multiple goroutines.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
