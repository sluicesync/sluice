// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Change-log retention for the trigger-CDC SOURCE (ADR-0137, Bug 165). The
// capture triggers never reap consumed rows, so `sluice_change_log` grows
// unbounded for the life of a continuous sync. [Prune] / [PruneD1] reap rows the
// TARGET has already durably applied — keyed off a `cut` id the CLI derives from
// the target's persisted CDC watermark, NEVER the source reader's read cursor
// (which runs ahead of the durable frontier; pruning there would delete
// not-yet-applied rows → silent loss on warm-resume). This package only executes
// the DELETE; computing a safe `cut` is the CLI's job (ADR-0137 §1).

// Batching + cadence tuning for the prune DELETE (repo-audit P-1). A monolithic
// `DELETE ... WHERE id <= cut` over a large backlog stalls the single-writer
// source (local SQLite: the SOURCE APPLICATION's writes block for its whole
// duration) or blows D1's per-query CPU ceiling (a failed tick retries an even
// bigger delete — prune falls behind permanently). The prune instead steps the
// id keyset in bounded batches (id is the change-log's AUTOINCREMENT PK, so
// each step is an index range scan), one short statement per batch, so the
// source's writer interleaves between batches and a failure or budget-stop
// loses at most one batch of progress. Per-transport batch sizes live next to
// their executors ([localPruneBatchSize] / [d1PruneBatchSize]).
const (
	// pruneTimeBudget bounds one AUTO-PRUNE tick (ADR-0137 Phase B). The
	// sidecar's cadence is 5 min; stopping well short of it means a huge
	// backlog is worked off across ticks instead of holding the change-log
	// hostage for minutes. The operator-run CLI [Prune] / [PruneD1] pass no
	// budget (0) — an explicit `sluice trigger prune` runs to completion.
	pruneTimeBudget = 30 * time.Second

	// pruneRecountEvery re-anchors the auto-prune sidecar's remaining-rows
	// estimate with a true COUNT(*) every Nth tick (the default 5-min cadence
	// makes that hourly). Between recounts the estimate is rows-affected
	// arithmetic — the P-1 fix for the per-tick COUNT(*) full scan.
	pruneRecountEvery = 12
)

// pruneBatchFunc deletes one keyset step — change-log rows with
// floor < id <= upper — and returns the rows deleted. upper never exceeds the
// caller's cut, which is how the batching preserves the ADR-0137 invariant:
// only rows at-or-below the durably-applied frontier are ever deleted, no
// matter how the batching steps. [executor.pruneChangeLogBatch] satisfies it.
type pruneBatchFunc func(ctx context.Context, floor, upper int64) (int64, error)

// pruneInBatches reaps change-log rows with id <= cut in bounded keyset steps
// of `step` ids, starting from minID (the change-log's current MIN(id) —
// indexed and cheap). done=false means the time budget ran out with rows still
// below cut; the caller resumes on its next tick (the floor re-derives from
// MIN(id), so resumption is free). budget <= 0 disables the budget (the
// operator-run CLI path). ctx is consulted between batches so a shutdown never
// waits behind a multi-batch backlog.
func pruneInBatches(
	ctx context.Context, minID, cut, step int64, budget time.Duration, del pruneBatchFunc,
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

// pruneBookkeeper tracks the auto-prune sidecar's remaining-rows estimate
// across ticks so per-tick observability doesn't cost a COUNT(*) full scan of
// the change-log (P-1). The estimate is one-sided arithmetic — it subtracts
// each tick's deleted rows but cannot see the capture triggers' concurrent
// inserts — so every [pruneRecountEvery]-th tick runs one true COUNT to
// re-anchor it. Not concurrency-safe; the single auto-prune sidecar goroutine
// owns it (the same ownership contract as the pipeline's autoPruneGate).
type pruneBookkeeper struct {
	ticks     int64 // prune ticks observed (drives the recount cadence)
	remaining int64 // estimated change-log rows left; meaningful only once anchored
	anchored  bool  // a true recount has run at least once
}

// tick advances the tick counter and reports whether THIS tick should
// re-anchor the estimate with a true COUNT (the first tick, then every
// pruneRecountEvery-th).
func (b *pruneBookkeeper) tick() (recount bool) {
	b.ticks++
	return b.ticks == 1 || b.ticks%pruneRecountEvery == 0
}

// noteDeleted subtracts a tick's deletions from the estimate (floored at 0).
func (b *pruneBookkeeper) noteDeleted(n int64) {
	if !b.anchored {
		return
	}
	b.remaining -= n
	if b.remaining < 0 {
		b.remaining = 0
	}
}

// anchor resets the estimate from a true recount.
func (b *pruneBookkeeper) anchor(count int64) {
	b.remaining = count
	b.anchored = true
}

// PruneOptions controls a change-log prune. Cut is the inclusive upper bound —
// rows with id <= Cut are deleted. The caller guarantees Cut is a safe bound (at
// or below the target's durably-applied frontier minus a margin); this package
// trusts it and does not re-derive it.
type PruneOptions struct {
	// Cut deletes change-log rows with id <= Cut. The CLI only calls Prune when
	// Cut > 0 (a non-positive cut is a no-op it handles before dispatching).
	Cut int64

	// Vacuum runs VACUUM after the DELETE to reclaim file space (off by default —
	// VACUUM rewrites the whole database). On D1 it executes over the /query API
	// (best-effort; surfaces loudly if the endpoint rejects it).
	Vacuum bool

	// DryRun reports the current change-log stats without deleting anything.
	DryRun bool
}

// PruneResult is the operator-facing outcome of a prune.
type PruneResult struct {
	Deleted      int64 // rows DELETEd (0 on a dry-run)
	Vacuumed     bool  // VACUUM was applied
	RemainingMin int64 // MIN(id) of the change-log after the prune (0 when empty)
	Remaining    int64 // COUNT(*) of the change-log after the prune
}

// Prune reaps durably-applied rows from a local SQLite file's change-log.
func Prune(ctx context.Context, dsn string, opts PruneOptions) (*PruneResult, error) {
	return prune(ctx, localBackend(dsn), opts)
}

// PruneD1 reaps durably-applied rows from a live Cloudflare D1 database's
// change-log over the /query HTTP API (ADR-0136 transport).
func PruneD1(ctx context.Context, dsn string, opts PruneOptions) (*PruneResult, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	return prune(ctx, b, opts)
}

// PruneConsumedChangeLog implements [ir.ChangeLogPruner] (ADR-0137 Phase B): the
// streamer's in-stream auto-prune sidecar calls it on a cadence with the TARGET's
// durably-persisted position token. The decode stays engine-owned — it reuses
// [AppliedLastID] (which refuses a FOREIGN token loudly) to extract the applied
// frontier, then reaps `id <= appliedLastID - keep` in bounded keyset batches
// under [pruneTimeBudget] (a partial tick resumes on the next cadence). A
// non-positive cut is a safe no-op. The DELETEs run on a FRESH writable executor
// opened from the reader's backend (not the read-only poll executor), so they
// never contend with — or race the Close of — the polling connection; unlike
// pgtrigger's pooled prune connection, the per-tick open is deliberate here: a
// held writable local-SQLite connection would pin the WAL read-mark (Bug 167),
// and the D1 executor is a stateless HTTP client whose open is free.
func (r *CDCReader) PruneConsumedChangeLog(ctx context.Context, durablePositionToken string, keep int64) (int64, error) {
	appliedLastID, err := AppliedLastID(durablePositionToken)
	if err != nil {
		return 0, err
	}
	cut := appliedLastID - keep
	if cut <= 0 {
		// Nothing safely below the durable frontier minus the margin yet.
		return 0, nil
	}
	exec, err := r.b.openExec(ctx, false)
	if err != nil {
		return 0, fmt.Errorf("%s: prune: open: %w", r.b.driver, err)
	}
	defer func() { _ = exec.close() }()
	minID, err := exec.minChangeLogID(ctx)
	if err != nil {
		return 0, fmt.Errorf("%s: prune: min id: %w", r.b.driver, err)
	}
	deleted, done, err := pruneInBatches(
		ctx, minID, cut, exec.pruneBatchSize(), pruneTimeBudget, exec.pruneChangeLogBatch,
	)
	if err != nil {
		return deleted, fmt.Errorf("%s: prune: delete: %w", r.b.driver, err)
	}
	r.notePruneTick(ctx, exec, deleted, done)
	return deleted, nil
}

// notePruneTick maintains the sidecar's remaining-rows estimate: rows-affected
// arithmetic per tick, re-anchored by a true COUNT every [pruneRecountEvery]-th
// tick (P-1 — never a per-tick COUNT(*) full scan). Purely observability; a
// failed recount keeps the stale estimate and the next recount tick retries.
func (r *CDCReader) notePruneTick(ctx context.Context, exec executor, deleted int64, done bool) {
	if r.pruneBook.tick() {
		if minID, count, err := exec.changeLogStats(ctx); err == nil {
			r.pruneBook.anchor(count)
			slog.DebugContext(ctx, r.b.driver+": auto-prune recount",
				slog.Int64("remaining", count), slog.Int64("min_id", minID))
		}
	} else {
		r.pruneBook.noteDeleted(deleted)
	}
	if !done {
		slog.DebugContext(ctx, r.b.driver+": auto-prune tick budget exhausted; resuming next tick",
			slog.Int64("deleted", deleted))
	}
}

// prune is the transport-neutral reaper used by both [Prune] (local file) and
// [PruneD1] (D1 over HTTP). It opens a writable executor, verifies the change-log
// exists (refusing loudly otherwise — a prune against a non-setup source is an
// operator error, not a silent no-op), DELETEs id <= Cut in bounded keyset
// batches, optionally VACUUMs, and reports the post-prune stats. Idempotent:
// re-running with the same Cut deletes nothing new.
func prune(ctx context.Context, b backend, opts PruneOptions) (*PruneResult, error) {
	exec, err := b.openExec(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("%s: prune: open: %w", b.driver, err)
	}
	defer func() { _ = exec.close() }()

	exists, err := exec.changeLogExists(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: prune: check change-log: %w", b.driver, err)
	}
	if !exists {
		return nil, fmt.Errorf(
			"%s: prune: change-log table %q not found on the source — run `sluice trigger setup` first",
			b.driver, ChangeLogTable,
		)
	}

	res := &PruneResult{}
	if opts.DryRun {
		if res.RemainingMin, res.Remaining, err = exec.changeLogStats(ctx); err != nil {
			return nil, fmt.Errorf("%s: prune: stats: %w", b.driver, err)
		}
		return res, nil
	}

	// Batched (P-1) with no time budget — the operator asked for this prune,
	// so it runs to completion.
	minID, err := exec.minChangeLogID(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: prune: min id: %w", b.driver, err)
	}
	if res.Deleted, _, err = pruneInBatches(
		ctx, minID, opts.Cut, exec.pruneBatchSize(), 0, exec.pruneChangeLogBatch,
	); err != nil {
		return nil, fmt.Errorf("%s: prune: delete: %w", b.driver, err)
	}
	if opts.Vacuum {
		if err := exec.vacuum(ctx); err != nil {
			return res, fmt.Errorf("%s: prune: vacuum: %w", b.driver, err)
		}
		res.Vacuumed = true
	}
	if res.RemainingMin, res.Remaining, err = exec.changeLogStats(ctx); err != nil {
		return res, fmt.Errorf("%s: prune: stats: %w", b.driver, err)
	}
	return res, nil
}
