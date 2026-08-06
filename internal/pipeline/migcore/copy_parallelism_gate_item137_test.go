// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 137: the per-chunk retry budget is keyed by (table, chunk),
// not by chunk index alone.
//
// The defect it closes is not a coding slip — it is an INVARIANT that stopped
// holding. Item 126's key was justified in a comment reading "the gate is
// constructed per table, so the index is unique within a gate", which was
// true when written. ADR-0123 made the gate run-wide and nothing recomputed
// the argument, so table A's chunk 3 and table B's chunk 3 quietly shared one
// give-up bound and item 126's advertised bound could fire early.
//
// Per the working agreement, an invariant is a hypothesis until you can name
// the test that fails when it breaks. This is that test.

package migcore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestShrinkAndBackoff_BudgetIsPerTableChunkNotChunkIndex exhausts one
// table's chunk-3 budget and requires ANOTHER table's chunk 3 — same index,
// different table — to still have its own.
//
// The independent expected value is item 126's own contract: MaxRetries
// backoffs per chunk before a give-up. The second table is asked for exactly
// that many and must get them; sharing the budget gives it zero.
func TestShrinkAndBackoff_BudgetIsPerTableChunkNotChunkIndex(t *testing.T) {
	p := CopyBackoffPolicy{MaxRetries: 3, BaseDelay: 0, MaxDelay: 0, MaxTotalWait: time.Hour}
	g := NewCopyParallelismGate(16, p)
	ctx := context.Background()

	const chunk = 3
	// Burn "orders" chunk 3 all the way to its give-up.
	for i := 1; i <= p.MaxRetries; i++ {
		if _, err := g.ShrinkAndBackoff(ctx, "orders", chunk); err != nil {
			t.Fatalf(`"orders" chunk %d attempt %d gave up early: %v`, chunk, i, err)
		}
	}
	if _, err := g.ShrinkAndBackoff(ctx, "orders", chunk); !errors.Is(err, ErrCopySlotsExhausted) {
		t.Fatalf(`"orders" chunk %d did not give up at the bound; err = %v`, chunk, err)
	}

	// A DIFFERENT table's chunk 3 must still have the FULL budget. Pre-item-137
	// the key was the index alone, so this loop aborted on its first call.
	for i := 1; i <= p.MaxRetries; i++ {
		if _, err := g.ShrinkAndBackoff(ctx, "invoices", chunk); err != nil {
			t.Fatalf(`"invoices" chunk %d attempt %d gave up after "orders" chunk %d exhausted ITS budget: %v. `+
				"The two tables share one give-up bound, so item 126's per-chunk budget fires earlier than it "+
				"advertises (roadmap item 137)", chunk, i, chunk, err)
		}
	}
	if _, err := g.ShrinkAndBackoff(ctx, "invoices", chunk); !errors.Is(err, ErrCopySlotsExhausted) {
		t.Errorf(`"invoices" chunk %d did not give up at its own bound; err = %v`, chunk, err)
	}
}

// TestShrinkAndBackoff_BackoffExponentIsPerTableChunk pins the OTHER half of
// the key. `waitByChunk` feeds the backoff exponent and the accumulated-wait
// bound, so a shared key also makes one table's waiting inflate another's
// delay. Keyed correctly, two tables at the same index climb the same ladder
// independently.
func TestShrinkAndBackoff_BackoffExponentIsPerTableChunk(t *testing.T) {
	p := CopyBackoffPolicy{MaxRetries: 8, BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second, MaxTotalWait: time.Hour}
	g := NewCopyParallelismGate(16, p)
	ctx := context.Background()

	const chunk = 5
	ordersDelays := make([]time.Duration, 0, 4)
	for range 4 {
		d, err := g.ShrinkAndBackoff(ctx, "orders", chunk)
		if err != nil {
			t.Fatalf(`"orders" chunk %d gave up early: %v`, chunk, err)
		}
		ordersDelays = append(ordersDelays, d)
	}

	// Anti-vacuity: the ladder must actually climb, or "the same sequence"
	// would be a comparison of four identical zeros.
	if ordersDelays[len(ordersDelays)-1] <= ordersDelays[0] {
		t.Fatalf("backoff did not escalate across attempts (%v) — this test cannot distinguish a shared "+
			"exponent from a private one", ordersDelays)
	}

	for i := range ordersDelays {
		d, err := g.ShrinkAndBackoff(ctx, "invoices", chunk)
		if err != nil {
			t.Fatalf(`"invoices" chunk %d attempt %d gave up: %v`, chunk, i+1, err)
		}
		if d != ordersDelays[i] {
			t.Errorf(`"invoices" chunk %d attempt %d backed off %v; want %v — the same ladder "orders" chunk %d `+
				"climbed. A shared (index-only) key makes one table's accumulated wait set another's delay "+
				"(roadmap item 137)", chunk, i+1, d, ordersDelays[i], chunk)
		}
	}
}
