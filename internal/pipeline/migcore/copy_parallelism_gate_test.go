// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestCopyParallelismGate_AcquireRelease pins the basic semaphore
// behaviour: a gate seeded with N tokens lets N acquires through and the
// (N+1)th blocks until a release.
func TestCopyParallelismGate_AcquireRelease(t *testing.T) {
	g := NewCopyParallelismGate(2, DefaultCopyBackoffPolicy)
	ctx := context.Background()

	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	// Third acquire must block until a release frees a token.
	blocked := make(chan error, 1)
	go func() { blocked <- g.Acquire(ctx) }()
	select {
	case err := <-blocked:
		t.Fatalf("third acquire should block on a 2-token gate, returned %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	g.Release()
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("third acquire after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("third acquire did not unblock after release")
	}
}

// TestCopyParallelismGate_AcquireHonoursCtx pins that a blocked acquire
// returns the ctx error on cancellation rather than hanging.
func TestCopyParallelismGate_AcquireHonoursCtx(t *testing.T) {
	g := NewCopyParallelismGate(1, DefaultCopyBackoffPolicy)
	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("acquire on cancelled ctx = %v, want context.Canceled", err)
	}
}

// TestCopyParallelismGate_ShrinkRetiresTokens pins the multiplicative
// shrink: a 53300 halves the effective cap, and finishing workers swallow
// the retired tokens so the live pool drains to the new cap.
func TestCopyParallelismGate_ShrinkRetiresTokens(t *testing.T) {
	// Zero-delay policy so the test never actually sleeps.
	p := CopyBackoffPolicy{MaxRetries: 10, BaseDelay: 0, MaxDelay: 0, MaxTotalWait: time.Hour}
	g := NewCopyParallelismGate(4, p)
	ctx := context.Background()

	// One worker holds a token and triggers a shrink (4 → 2).
	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := g.ShrinkAndBackoff(ctx, 1); err != nil {
		t.Fatalf("shrinkAndBackoff: %v", err)
	}

	g.mu.Lock()
	gotEff, gotRetire := g.effective, g.retire
	g.mu.Unlock()
	if gotEff != 2 {
		t.Errorf("effective after shrink = %d, want 2", gotEff)
	}
	// prev=4, next=2 → retire 2 tokens.
	if gotRetire != 2 {
		t.Errorf("retire after shrink = %d, want 2", gotRetire)
	}

	// The 3 not-yet-held tokens are still in the channel. As workers
	// finish, the first `retire` releases swallow tokens. Drain the
	// channel after the retirements settle and confirm the live capacity
	// is the new cap (2) minus the one this test still holds = 1 free.
	// Simulate: 3 peers each acquire then release.
	for i := 0; i < 3; i++ {
		if err := g.Acquire(ctx); err != nil {
			t.Fatalf("peer acquire %d: %v", i, err)
		}
	}
	// Now 0 tokens free (held: this test's 1 + 3 peers = 4 acquired, but
	// 0 retired yet because nobody released). Release the 3 peers: the
	// first 2 releases are swallowed (retire 2→0), the 3rd returns a real
	// token.
	for i := 0; i < 3; i++ {
		g.Release()
	}
	g.mu.Lock()
	if g.retire != 0 {
		t.Errorf("retire after 3 releases = %d, want 0 (2 swallowed)", g.retire)
	}
	g.mu.Unlock()

	// Exactly one token should now be free (the 3rd release). A second
	// acquire must block.
	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire the one surviving free token: %v", err)
	}
	blocked := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		blocked <- g.Acquire(c)
	}()
	if err := <-blocked; !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("after shrink to cap 2 with 2 held, extra acquire = %v, want DeadlineExceeded (no free tokens)", err)
	}
}

// TestCopyParallelismGate_GivesUpLoudly pins the bounded give-up: after
// the policy's retry bound is exhausted, shrinkAndBackoff returns a loud
// error wrapping ErrCopySlotsExhausted rather than looping forever.
func TestCopyParallelismGate_GivesUpLoudly(t *testing.T) {
	p := CopyBackoffPolicy{MaxRetries: 3, BaseDelay: 0, MaxDelay: 0, MaxTotalWait: time.Hour}
	g := NewCopyParallelismGate(8, p)
	ctx := context.Background()

	// Attempts 1..3 proceed.
	for i := 1; i <= 3; i++ {
		if _, err := g.ShrinkAndBackoff(ctx, 0); err != nil {
			t.Fatalf("attempt %d unexpectedly gave up: %v", i, err)
		}
	}
	// Attempt 4 gives up loudly.
	_, err := g.ShrinkAndBackoff(ctx, 0)
	if err == nil {
		t.Fatal("expected a give-up error after exhausting MaxRetries, got nil")
	}
	if !errors.Is(err, ErrCopySlotsExhausted) {
		t.Errorf("give-up error should wrap ErrCopySlotsExhausted; got %v", err)
	}
}

// TestCopyParallelismGate_ConcurrentFirstAttemptsAllProceed pins roadmap
// item 126 under the race detector.
//
// This test previously asserted the DEFECT as if it were the contract:
// "many chunks hammering concurrently produce exactly MaxRetries successes
// and the rest give up". That is the bug stated as an expectation. Sixteen
// DISTINCT chunks each meeting a slot shortage for the FIRST time have
// collectively retried nothing, and aborting eleven of them abandons the
// whole table on a transient the gate exists to ride out. The gate's own
// doc promised "a transient slot shortage degrades to slower-but-correct";
// at the shipped 4x4 it degraded to a failed run.
//
// The retry budget belongs to a CHUNK. The shrink stays run-wide, which the
// assertion below also checks: sixteen concurrent events must collapse the
// effective parallelism hard.
func TestCopyParallelismGate_ConcurrentFirstAttemptsAllProceed(t *testing.T) {
	p := CopyBackoffPolicy{MaxRetries: 5, BaseDelay: 0, MaxDelay: 0, MaxTotalWait: time.Hour}
	g := NewCopyParallelismGate(16, p)
	ctx := context.Background()

	const peers = 16
	var (
		mu       sync.Mutex
		proceeds int
		giveUps  int
		wg       sync.WaitGroup
	)
	for i := 0; i < peers; i++ {
		wg.Add(1)
		go func(chunk int) {
			defer wg.Done()
			_, err := g.ShrinkAndBackoff(ctx, chunk)
			mu.Lock()
			if err == nil {
				proceeds++
			} else {
				giveUps++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if proceeds != peers || giveUps != 0 {
		t.Errorf("first attempt on %d DISTINCT chunks: %d proceeded, %d gave up; want all %d to proceed.\n\n"+
			"Each of these chunks has retried ZERO times. Giving up here aborts the whole table on a "+
			"transient slot shortage the gate exists to ride out — and it is what happened at every "+
			"default multi-core parallelism (roadmap item 126).",
			peers, proceeds, giveUps, peers)
	}

	// The multiplicative decrease is still RUN-WIDE and must have bitten
	// hard: 16 concurrent events halve repeatedly down to the floor.
	if got := g.Effective(); got != 1 {
		t.Errorf("effective parallelism = %d after %d concurrent slot-exhaustion events; want the floor "+
			"of 1 — the shrink is deliberately run-wide and must still collapse fast", got, peers)
	}
}

// TestCopyParallelismGate_OneChunkStillGivesUp is the other half: making the
// budget per-chunk must NOT make it unbounded. A single chunk that keeps
// meeting a saturated target still fails loudly at MaxRetries, which is what
// stops an unavailable target from stalling the run forever.
func TestCopyParallelismGate_OneChunkStillGivesUp(t *testing.T) {
	p := CopyBackoffPolicy{MaxRetries: 5, BaseDelay: 0, MaxDelay: 0, MaxTotalWait: time.Hour}
	g := NewCopyParallelismGate(16, p)
	ctx := context.Background()

	for i := 1; i <= p.MaxRetries; i++ {
		if _, err := g.ShrinkAndBackoff(ctx, 7); err != nil {
			t.Fatalf("chunk 7 attempt %d gave up early: %v", i, err)
		}
	}
	_, err := g.ShrinkAndBackoff(ctx, 7)
	if err == nil {
		t.Fatal("chunk 7 never gave up past MaxRetries — a per-chunk budget must still be BOUNDED, or a " +
			"genuinely saturated target stalls the run indefinitely")
	}
	if !errors.Is(err, ErrCopySlotsExhausted) {
		t.Errorf("give-up error does not wrap ErrCopySlotsExhausted: %v", err)
	}

	// And a DIFFERENT chunk still has its own budget — that is the point.
	if _, err := g.ShrinkAndBackoff(ctx, 8); err != nil {
		t.Errorf("chunk 8 inherited chunk 7's exhausted budget: %v\n\n"+
			"The budget is per chunk; one chunk failing must not pre-abort its peers.", err)
	}
}
