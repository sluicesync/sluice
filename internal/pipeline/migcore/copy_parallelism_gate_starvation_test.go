// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Token-conservation and non-starvation for the shared copy gate
// (audit 2026-08-01 P1, which reported that the gate "can permanently deadlock
// migrate after two connection-slot shrinks, silently, with no error").
//
// These exist to ATTEMPT that deadlock rather than to argue about it. A shrink
// does not hand a token back — it retires one, meaning some future Release
// swallows its token instead of returning it — so the accounting is easy to
// get wrong in the direction that starves the pool, and a starved pool is
// exactly the reported failure: every worker blocked in Acquire, no error, no
// give-up, because the code that gives up only runs on a slot-exhaustion the
// blocked workers can never reach.
//
// The invariant the implementation actually maintains is:
//
//	physical tokens == effective + retire-pending,   effective >= 1
//
// A shrink moves d from effective into retire, and a swallowing Release moves
// one out of retire and destroys one physical token — so physical never falls
// below effective, and effective is floored at 1. That is the property under
// test. If it holds, the reported deadlock cannot occur; if any future change
// breaks it, these fail rather than a migration hanging in the field.

package migcore

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestCopyGate_SurvivesRepeatedShrinks drives the exact shape the finding
// names — repeated shrinks, more than two — and requires the pool to still
// hand out work afterwards.
func TestCopyGate_SurvivesRepeatedShrinks(t *testing.T) {
	for _, start := range []int{2, 3, 4, 8, 16} {
		t.Run("initial="+itoaGate(start), func(t *testing.T) {
			g := NewCopyParallelismGate(start, DefaultCopyBackoffPolicy)
			ctx := context.Background()

			// Every worker acquires, so the channel is empty and every token
			// is held — the worst case for retirement accounting.
			for range start {
				if err := g.Acquire(ctx); err != nil {
					t.Fatalf("initial acquire: %v", err)
				}
			}

			// Shrink repeatedly, as concurrent slot-exhaustion on several
			// chunks would. More than two, since two is what was reported.
			for range 5 {
				if _, err := g.ShrinkAndBackoff(ctx, 0); err != nil {
					break // give-up verdict is a legitimate, loud outcome
				}
			}

			if eff := g.Effective(); eff < 1 {
				t.Fatalf("effective parallelism fell to %d; the gate can no longer admit any worker", eff)
			}

			// Now every holder finishes. Some releases are swallowed by the
			// retirement; at least one token must survive, or the next chunk
			// blocks forever.
			for range start {
				g.Release()
			}

			if !acquireWithin(t, g, 2*time.Second) {
				t.Fatalf("after %d shrinks and %d releases the pool is EMPTY — a worker acquiring here "+
					"blocks forever, with no error and no give-up, because the give-up bound only runs on "+
					"a slot-exhaustion a blocked worker can never reach. This is audit P1.", 5, start)
			}
		})
	}
}

// TestCopyGate_ConcurrentShrinkAndReleaseNeverStarves is the racy version: many
// workers acquiring, releasing and shrinking at once. Run under -race in CI.
func TestCopyGate_ConcurrentShrinkAndReleaseNeverStarves(t *testing.T) {
	const start = 8
	g := NewCopyParallelismGate(start, DefaultCopyBackoffPolicy)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range start {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := g.Acquire(ctx); err != nil {
				return
			}
			// Half the workers report slot exhaustion before finishing.
			if i%2 == 0 {
				_, _ = g.ShrinkAndBackoff(ctx, i)
			}
			g.Release()
		}(i)
	}
	wg.Wait()

	if eff := g.Effective(); eff < 1 {
		t.Fatalf("effective parallelism is %d after concurrent shrinks", eff)
	}
	if !acquireWithin(t, g, 2*time.Second) {
		t.Fatal("pool starved after concurrent shrink/release; every subsequent chunk would block " +
			"forever with no error (audit P1)")
	}
}

// TestCopyGate_EffectiveNeverReachesZero pins the floor the whole argument
// rests on. If a future change let effective reach 0, the pool could legally
// drain to nothing and the deadlock P1 describes would become real.
func TestCopyGate_EffectiveNeverReachesZero(t *testing.T) {
	g := NewCopyParallelismGate(1, DefaultCopyBackoffPolicy)
	ctx := context.Background()
	if err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	for range 3 {
		if _, err := g.ShrinkAndBackoff(ctx, 0); err != nil {
			break
		}
		if eff := g.Effective(); eff < 1 {
			t.Fatalf("effective fell to %d — the gate would admit no worker", eff)
		}
	}
	g.Release()
	if !acquireWithin(t, g, 2*time.Second) {
		t.Fatal("a gate that started at parallelism 1 starved itself by shrinking")
	}
}

// acquireWithin reports whether a token can be taken within d.
func acquireWithin(t *testing.T, g *CopyParallelismGate, d time.Duration) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return g.Acquire(ctx) == nil
}

func itoaGate(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
