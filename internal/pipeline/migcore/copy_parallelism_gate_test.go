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

// TestCopyParallelismGate_ShrinkTakesEffectImmediately pins the
// multiplicative shrink: a 53300 halves the cap, and the new cap binds the
// very next acquire.
//
// This replaces TestCopyParallelismGate_ShrinkRetiresTokens, which pinned
// the retire-counter mechanism the Bug 228 fix deleted. The OBSERVABLE
// contract is stronger now and the test says so: under the old lazy
// retirement a shrink to 2 still admitted workers up to the ORIGINAL
// capacity until enough releases had been swallowed, so the cap the log
// line announced was not the cap in force. It is now.
func TestCopyParallelismGate_ShrinkTakesEffectImmediately(t *testing.T) {
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
	if got := g.Effective(); got != 2 {
		t.Errorf("effective after shrink = %d, want 2", got)
	}

	// Cap 2 with 1 held ⇒ exactly one more admission, then the gate blocks.
	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("second acquire under the shrunk cap: %v", err)
	}
	blocked := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		blocked <- g.Acquire(c)
	}()
	if err := <-blocked; !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("after shrink to cap 2 with 2 held, a third acquire = %v, want DeadlineExceeded", err)
	}

	// Releasing frees the slots back up to the shrunk cap — and no further.
	g.Release()
	g.Release()
	for i := range 2 {
		if err := g.Acquire(ctx); err != nil {
			t.Fatalf("post-release acquire %d under cap 2: %v", i, err)
		}
	}
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		blocked <- g.Acquire(c)
	}()
	if err := <-blocked; !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the shrunk cap stopped being enforced after a release cycle: %v", err)
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
