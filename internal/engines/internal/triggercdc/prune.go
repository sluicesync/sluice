// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"context"
	"time"
)

// Prune batching + cadence tuning shared by every trigger-CDC engine (ADR-0137,
// Bug 165, repo-audit P-1). A monolithic `DELETE ... WHERE id <= cut` over a
// large backlog is one long transaction — a WAL burst plus long-held locks on
// the source (and on the single-writer SQLite/D1 path it stalls the SOURCE
// APPLICATION's writes for its whole duration, or blows D1's per-query CPU
// ceiling). [InBatches] instead steps the id keyset in bounded batches (id is
// the change-log's PK, so each step is an index range scan), one short statement
// per batch, so the source's own writers interleave between batches and a
// failure or budget-stop loses at most one batch of progress. The per-transport
// batch SIZE stays engine-side (PG bigserial vs the local-file / D1 ceilings);
// only the loop + the auto-prune cadence live here.
const (
	// AutoPruneTickBudget bounds one AUTO-PRUNE tick (ADR-0137 Phase B). The
	// sidecar's cadence is 5 min; stopping well short of it means a huge backlog
	// is worked off across ticks instead of holding the change-log hostage for
	// minutes. The operator-run CLI prune passes no budget (0) — an explicit
	// `sluice trigger prune` runs to completion.
	AutoPruneTickBudget = 30 * time.Second

	// RecountEvery re-anchors the auto-prune sidecar's remaining-rows estimate
	// with a true COUNT(*) every Nth tick (the default 5-min cadence makes that
	// hourly). Between recounts the estimate is rows-affected arithmetic — the
	// P-1 fix for the per-tick COUNT(*) full scan.
	RecountEvery = 12
)

// BatchFunc deletes one keyset step — change-log rows with floor < id <= upper —
// and returns the rows deleted. upper never exceeds the caller's cut, which is
// how the batching preserves the ADR-0137 invariant: only rows at-or-below the
// durably-applied frontier are ever deleted, no matter how the batching steps.
type BatchFunc func(ctx context.Context, floor, upper int64) (int64, error)

// InBatches reaps change-log rows with id <= cut in bounded keyset steps of
// `step` ids, starting from minID (the change-log's current MIN(id) — indexed
// and cheap). done=false means the time budget ran out with rows still below
// cut; the caller resumes on its next tick (the floor re-derives from MIN(id),
// so resumption is free). budget <= 0 disables the budget (the operator-run CLI
// path). ctx is consulted between batches so a shutdown never waits behind a
// multi-batch backlog.
func InBatches(
	ctx context.Context, minID, cut, step int64, budget time.Duration, del BatchFunc,
) (deleted int64, done bool, err error) {
	if minID <= 0 || minID > cut {
		// Empty change-log, or nothing at-or-below the cut.
		return 0, true, nil
	}
	var deadline time.Time
	if budget > 0 {
		deadline = time.Now().Add(budget)
	}
	for floor := minID - 1; floor < cut; {
		if err := ctx.Err(); err != nil {
			return deleted, false, err
		}
		upper := min(cut, floor+step)
		n, err := del(ctx, floor, upper)
		if err != nil {
			return deleted, false, err
		}
		deleted += n
		floor = upper
		if !deadline.IsZero() && floor < cut && time.Now().After(deadline) {
			// Budget exhausted mid-backlog: stop here; the next tick resumes.
			return deleted, false, nil
		}
	}
	return deleted, true, nil
}

// Bookkeeper tracks the auto-prune sidecar's remaining-rows estimate across
// ticks so per-tick observability doesn't cost a COUNT(*) full scan of the
// change-log (P-1). The estimate is one-sided arithmetic — it subtracts each
// tick's deleted rows but cannot see the capture triggers' concurrent inserts —
// so every [RecountEvery]-th tick runs one true COUNT to re-anchor it. Not
// concurrency-safe; the single auto-prune sidecar goroutine owns it (the same
// ownership contract as the pipeline's autoPruneGate).
type Bookkeeper struct {
	ticks     int64 // prune ticks observed (drives the recount cadence)
	remaining int64 // estimated change-log rows left; meaningful only once anchored
	anchored  bool  // a true recount has run at least once
}

// Tick advances the tick counter and reports whether THIS tick should re-anchor
// the estimate with a true COUNT (the first tick, then every [RecountEvery]-th).
func (b *Bookkeeper) Tick() (recount bool) {
	b.ticks++
	return b.ticks == 1 || b.ticks%RecountEvery == 0
}

// NoteDeleted subtracts a tick's deletions from the estimate (floored at 0).
func (b *Bookkeeper) NoteDeleted(n int64) {
	if !b.anchored {
		return
	}
	b.remaining -= n
	if b.remaining < 0 {
		b.remaining = 0
	}
}

// Anchor resets the estimate from a true recount.
func (b *Bookkeeper) Anchor(count int64) {
	b.remaining = count
	b.anchored = true
}

// Remaining reports the current estimated change-log rows left (meaningful only
// once [Bookkeeper.Anchored] is true).
func (b *Bookkeeper) Remaining() int64 { return b.remaining }

// Anchored reports whether a true recount has run at least once.
func (b *Bookkeeper) Anchored() bool { return b.anchored }

// PrimeRecount positions the tick counter so the NEXT [Bookkeeper.Tick] lands on
// the recount cadence. It is the seam the engine auto-prune tests use to force a
// recount tick deterministically without reaching into the counter.
func (b *Bookkeeper) PrimeRecount() { b.ticks = RecountEvery - 1 }
