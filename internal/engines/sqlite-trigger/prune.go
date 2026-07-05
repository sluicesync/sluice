// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
)

// Change-log retention for the trigger-CDC SOURCE (ADR-0137, Bug 165). The
// capture triggers never reap consumed rows, so `sluice_change_log` grows
// unbounded for the life of a continuous sync. [Prune] / [PruneD1] reap rows the
// TARGET has already durably applied — keyed off a `cut` id the CLI derives from
// the target's persisted CDC watermark, NEVER the source reader's read cursor
// (which runs ahead of the durable frontier; pruning there would delete
// not-yet-applied rows → silent loss on warm-resume). This package only executes
// the DELETE; computing a safe `cut` is the CLI's job (ADR-0137 §1).

// Batching note (repo-audit P-1). A monolithic `DELETE ... WHERE id <= cut` over
// a large backlog stalls the single-writer source (local SQLite: the SOURCE
// APPLICATION's writes block for its whole duration) or blows D1's per-query CPU
// ceiling (a failed tick retries an even bigger delete — prune falls behind
// permanently). [triggercdc.InBatches] steps the id keyset in bounded batches
// instead (id is the change-log's AUTOINCREMENT PK, so each step is an index
// range scan), one short statement per batch, so the source's writer interleaves
// between batches and a failure or budget-stop loses at most one batch of
// progress. Per-transport batch sizes live next to their executors
// ([localPruneBatchSize] / [d1PruneBatchSize]); the tick budget + recount cadence
// live in [triggercdc] (shared across trigger engines).

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
// under [triggercdc.AutoPruneTickBudget] (a partial tick resumes on the next
// cadence). A non-positive cut is a safe no-op. The DELETEs run on a FRESH writable executor
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
	deleted, done, err := triggercdc.InBatches(
		ctx, minID, cut, exec.pruneBatchSize(), triggercdc.AutoPruneTickBudget, exec.pruneChangeLogBatch,
	)
	if err != nil {
		return deleted, fmt.Errorf("%s: prune: delete: %w", r.b.driver, err)
	}
	r.notePruneTick(ctx, exec, deleted, done)
	return deleted, nil
}

// notePruneTick maintains the sidecar's remaining-rows estimate: rows-affected
// arithmetic per tick, re-anchored by a true COUNT every [triggercdc.RecountEvery]-th
// tick (P-1 — never a per-tick COUNT(*) full scan). Purely observability; a
// failed recount keeps the stale estimate and the next recount tick retries.
func (r *CDCReader) notePruneTick(ctx context.Context, exec executor, deleted int64, done bool) {
	if r.pruneBook.Tick() {
		if minID, count, err := exec.changeLogStats(ctx); err == nil {
			r.pruneBook.Anchor(count)
			slog.DebugContext(ctx, r.b.driver+": auto-prune recount",
				slog.Int64("remaining", count), slog.Int64("min_id", minID))
		}
	} else {
		r.pruneBook.NoteDeleted(deleted)
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
	if res.Deleted, _, err = triggercdc.InBatches(
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
