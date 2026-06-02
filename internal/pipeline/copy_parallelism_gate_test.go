// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

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
	g := newCopyParallelismGate(2, defaultCopyBackoffPolicy)
	ctx := context.Background()

	if err := g.acquire(ctx); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if err := g.acquire(ctx); err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	// Third acquire must block until a release frees a token.
	blocked := make(chan error, 1)
	go func() { blocked <- g.acquire(ctx) }()
	select {
	case err := <-blocked:
		t.Fatalf("third acquire should block on a 2-token gate, returned %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	g.release()
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
	g := newCopyParallelismGate(1, defaultCopyBackoffPolicy)
	if err := g.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("acquire on cancelled ctx = %v, want context.Canceled", err)
	}
}

// TestCopyParallelismGate_ShrinkRetiresTokens pins the multiplicative
// shrink: a 53300 halves the effective cap, and finishing workers swallow
// the retired tokens so the live pool drains to the new cap.
func TestCopyParallelismGate_ShrinkRetiresTokens(t *testing.T) {
	// Zero-delay policy so the test never actually sleeps.
	p := copyBackoffPolicy{maxRetries: 10, baseDelay: 0, maxDelay: 0, maxTotalWait: time.Hour}
	g := newCopyParallelismGate(4, p)
	ctx := context.Background()

	// One worker holds a token and triggers a shrink (4 → 2).
	if err := g.acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := g.shrinkAndBackoff(ctx, 1); err != nil {
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
		if err := g.acquire(ctx); err != nil {
			t.Fatalf("peer acquire %d: %v", i, err)
		}
	}
	// Now 0 tokens free (held: this test's 1 + 3 peers = 4 acquired, but
	// 0 retired yet because nobody released). Release the 3 peers: the
	// first 2 releases are swallowed (retire 2→0), the 3rd returns a real
	// token.
	for i := 0; i < 3; i++ {
		g.release()
	}
	g.mu.Lock()
	if g.retire != 0 {
		t.Errorf("retire after 3 releases = %d, want 0 (2 swallowed)", g.retire)
	}
	g.mu.Unlock()

	// Exactly one token should now be free (the 3rd release). A second
	// acquire must block.
	if err := g.acquire(ctx); err != nil {
		t.Fatalf("acquire the one surviving free token: %v", err)
	}
	blocked := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		blocked <- g.acquire(c)
	}()
	if err := <-blocked; !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("after shrink to cap 2 with 2 held, extra acquire = %v, want DeadlineExceeded (no free tokens)", err)
	}
}

// TestCopyParallelismGate_GivesUpLoudly pins the bounded give-up: after
// the policy's retry bound is exhausted, shrinkAndBackoff returns a loud
// error wrapping errCopySlotsExhausted rather than looping forever.
func TestCopyParallelismGate_GivesUpLoudly(t *testing.T) {
	p := copyBackoffPolicy{maxRetries: 3, baseDelay: 0, maxDelay: 0, maxTotalWait: time.Hour}
	g := newCopyParallelismGate(8, p)
	ctx := context.Background()

	// Attempts 1..3 proceed.
	for i := 1; i <= 3; i++ {
		if _, err := g.shrinkAndBackoff(ctx, 0); err != nil {
			t.Fatalf("attempt %d unexpectedly gave up: %v", i, err)
		}
	}
	// Attempt 4 gives up loudly.
	_, err := g.shrinkAndBackoff(ctx, 0)
	if err == nil {
		t.Fatal("expected a give-up error after exhausting maxRetries, got nil")
	}
	if !errors.Is(err, errCopySlotsExhausted) {
		t.Errorf("give-up error should wrap errCopySlotsExhausted; got %v", err)
	}
}

// TestCopyParallelismGate_ConcurrentShrinkBackoffBound pins, under the
// race detector, that the shared backoff bound is enforced across peer
// goroutines: many chunks hammering shrinkAndBackoff concurrently produce
// exactly maxRetries successes and the rest give up, with no data race on
// the shared counters.
func TestCopyParallelismGate_ConcurrentShrinkBackoffBound(t *testing.T) {
	p := copyBackoffPolicy{maxRetries: 5, baseDelay: 0, maxDelay: 0, maxTotalWait: time.Hour}
	g := newCopyParallelismGate(16, p)
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
			_, err := g.shrinkAndBackoff(ctx, chunk)
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

	if proceeds != p.maxRetries {
		t.Errorf("concurrent proceeds = %d, want exactly maxRetries=%d", proceeds, p.maxRetries)
	}
	if giveUps != peers-p.maxRetries {
		t.Errorf("concurrent give-ups = %d, want %d", giveUps, peers-p.maxRetries)
	}
	// Effective parallelism must never drop below the floor of 1.
	g.mu.Lock()
	if g.effective < 1 {
		t.Errorf("effective parallelism dropped below floor: %d", g.effective)
	}
	g.mu.Unlock()
}
