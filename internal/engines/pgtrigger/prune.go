// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/engines/postgres"
)

// Change-log retention for the pgtrigger CDC SOURCE (ADR-0137, Bug 165). The
// capture function never reaps consumed rows, so `sluice_change_log` grows
// unbounded for the life of a continuous sync. [Prune] reaps rows the TARGET has
// already durably applied — keyed off a `cut` id the CLI derives from the
// target's persisted CDC watermark, NEVER the source reader's read cursor (which
// runs ahead of the durable frontier; pruning there would delete not-yet-applied
// rows → silent loss on warm-resume). PG reclaims the freed space via autovacuum
// (no VACUUM option here; that is the SQLite/D1 path's concern).

// Batching + cadence tuning for the prune DELETE (repo-audit P-1). A monolithic
// `DELETE ... WHERE id <= cut` over a large backlog is one long transaction — a
// WAL burst plus long-held row locks on the source. The prune instead steps the
// id keyset in bounded batches (id is the change-log's BIGSERIAL PK, so each
// step is an index range scan), one short statement per batch, so the source's
// own writers interleave between batches and a failure or budget-stop loses at
// most one batch of progress.
const (
	// pgPruneBatchSize bounds one DELETE batch. ~20k rows keeps each statement
	// short (a bounded WAL burst, briefly-held locks) while still clearing a
	// multi-million-row backlog within a few auto-prune ticks.
	pgPruneBatchSize = 20_000

	// pruneTimeBudget bounds one AUTO-PRUNE tick (ADR-0137 Phase B). The
	// sidecar's cadence is 5 min; stopping well short of it means a huge
	// backlog is worked off across ticks instead of holding the change-log
	// hostage for minutes. The operator-run CLI [Prune] passes no budget (0) —
	// an explicit `sluice trigger prune` runs to completion.
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
// matter how the batching steps.
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

	// Schema is the source-side PG schema holding the change-log. Defaults to the
	// DSN's `schema` query parameter (typically "public").
	Schema string

	// DryRun reports the current change-log stats without deleting anything.
	DryRun bool
}

// PruneResult is the operator-facing outcome of a prune.
type PruneResult struct {
	Deleted      int64 // rows DELETEd (0 on a dry-run)
	RemainingMin int64 // MIN(id) of the change-log after the prune (0 when empty)
	Remaining    int64 // COUNT(*) of the change-log after the prune
}

// Prune reaps durably-applied rows from the source PG change-log. It connects to
// the source, verifies the change-log exists (refusing loudly otherwise — a
// prune against a non-setup source is an operator error, not a silent no-op),
// DELETEs id <= Cut in bounded keyset batches, and reports the post-prune stats.
// Idempotent: re-running with the same Cut deletes nothing new.
func Prune(ctx context.Context, dsn string, opts PruneOptions) (*PruneResult, error) {
	cfg, err := parseDSNCompat(dsn)
	if err != nil {
		return nil, err
	}
	schema := opts.Schema
	if schema == "" {
		schema = cfg.schema
	}

	// One-shot CLI operation with no stream-id in play; the empty label gets the
	// `sluice/control/-` fallback application_name.
	db, err := postgres.OpenPgxDB(cfg.dsn, "")
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: prune: open: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pgtrigger: prune: ping: %w", err)
	}

	exists, err := changeLogTableExists(ctx, db, schema)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: prune: check change-log: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf(
			"pgtrigger: prune: change-log table %q not found in schema %q — run `sluice trigger setup` first",
			ChangeLogTable, schema,
		)
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(ChangeLogTable)
	res := &PruneResult{}
	if opts.DryRun {
		if res.RemainingMin, res.Remaining, err = pgChangeLogStats(ctx, db, tableRef); err != nil {
			return nil, fmt.Errorf("pgtrigger: prune: stats: %w", err)
		}
		return res, nil
	}

	// id <= cut (not <): cut is the durably-applied frontier minus a margin, so
	// id == cut is itself durably applied and safe to remove. Batched (P-1) with
	// no time budget — the operator asked for this prune, so it runs to
	// completion.
	minID, err := pgChangeLogMinID(ctx, db, tableRef)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: prune: min id: %w", err)
	}
	if res.Deleted, _, err = pruneInBatches(
		ctx, minID, opts.Cut, pgPruneBatchSize, 0, pgPruneBatch(db, tableRef),
	); err != nil {
		return nil, fmt.Errorf("pgtrigger: prune: delete: %w", err)
	}
	if res.RemainingMin, res.Remaining, err = pgChangeLogStats(ctx, db, tableRef); err != nil {
		return res, fmt.Errorf("pgtrigger: prune: stats: %w", err)
	}
	return res, nil
}

// PruneConsumedChangeLog implements [ir.ChangeLogPruner] (ADR-0137 Phase B): the
// streamer's in-stream auto-prune sidecar calls it on a cadence with the TARGET's
// durably-persisted position token. The decode stays engine-owned — it reuses
// [AppliedLastID] (which refuses a FOREIGN token loudly) to extract the applied
// frontier, then reaps `id <= appliedLastID - keep` in bounded keyset batches
// under [pruneTimeBudget] (a partial tick resumes on the next cadence). A
// non-positive cut is a safe no-op. The DELETEs run on the reader's dedicated
// prune pool ([CDCReader.prunePool] — reused across ticks, separate from the
// poll pool), so the prune never contends with the polling connection.
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
	db, tableRef, err := r.prunePool(ctx)
	if err != nil {
		return 0, err
	}
	minID, err := pgChangeLogMinID(ctx, db, tableRef)
	if err != nil {
		return 0, fmt.Errorf("pgtrigger: prune: min id: %w", err)
	}
	deleted, done, err := pruneInBatches(
		ctx, minID, cut, pgPruneBatchSize, pruneTimeBudget, pgPruneBatch(db, tableRef),
	)
	if err != nil {
		return deleted, fmt.Errorf("pgtrigger: prune: delete: %w", err)
	}
	r.notePruneTick(ctx, db, tableRef, deleted, done)
	return deleted, nil
}

// prunePool returns the reader's dedicated auto-prune connection pool, opening
// it on first use and reusing it across prune ticks (the P-1 fix for the
// dial+ping-per-tick churn of delegating to the one-shot [Prune]). The pool is
// separate from the reader's poll pool (r.db) so a prune statement never
// contends with the polling loop's connection; [CDCReader.Close] releases it.
// The change-log-exists guard runs once at open — the same loud refusal [Prune]
// gives the CLI.
func (r *CDCReader) prunePool(ctx context.Context) (*sql.DB, string, error) {
	r.pruneMu.Lock()
	defer r.pruneMu.Unlock()
	tableRef := quoteIdent(r.schema) + "." + quoteIdent(ChangeLogTable)
	if r.pruneDB != nil {
		return r.pruneDB, tableRef, nil
	}
	// Same connection-label shape as the one-shot CLI prune: no stream id in
	// play at this layer, so the empty label gets `sluice/control/-`.
	db, err := postgres.OpenPgxDB(r.dsn, "")
	if err != nil {
		return nil, "", fmt.Errorf("pgtrigger: prune: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, "", fmt.Errorf("pgtrigger: prune: ping: %w", err)
	}
	exists, err := changeLogTableExists(ctx, db, r.schema)
	if err != nil {
		_ = db.Close()
		return nil, "", fmt.Errorf("pgtrigger: prune: check change-log: %w", err)
	}
	if !exists {
		_ = db.Close()
		return nil, "", fmt.Errorf(
			"pgtrigger: prune: change-log table %q not found in schema %q — run `sluice trigger setup` first",
			ChangeLogTable, r.schema,
		)
	}
	r.pruneDB = db
	return db, tableRef, nil
}

// notePruneTick maintains the sidecar's remaining-rows estimate: rows-affected
// arithmetic per tick, re-anchored by a true COUNT every [pruneRecountEvery]-th
// tick (P-1 — never a per-tick COUNT(*) full scan). Purely observability; a
// failed recount keeps the stale estimate and the next recount tick retries.
func (r *CDCReader) notePruneTick(ctx context.Context, db *sql.DB, tableRef string, deleted int64, done bool) {
	if r.pruneBook.tick() {
		if minID, count, err := pgChangeLogStats(ctx, db, tableRef); err == nil {
			r.pruneBook.anchor(count)
			slog.DebugContext(ctx, "pgtrigger: auto-prune recount",
				slog.Int64("remaining", count), slog.Int64("min_id", minID))
		}
	} else {
		r.pruneBook.noteDeleted(deleted)
	}
	if !done {
		slog.DebugContext(ctx, "pgtrigger: auto-prune tick budget exhausted; resuming next tick",
			slog.Int64("deleted", deleted))
	}
}

// pgPruneBatch binds one bounded keyset DELETE over db/tableRef. Split out so
// the CLI [Prune] and the auto-prune sidecar share the exact statement shape.
func pgPruneBatch(db *sql.DB, tableRef string) pruneBatchFunc {
	return func(ctx context.Context, floor, upper int64) (int64, error) {
		tag, err := db.ExecContext(ctx,
			"DELETE FROM "+tableRef+" WHERE id > $1 AND id <= $2", floor, upper)
		if err != nil {
			return 0, err
		}
		return tag.RowsAffected()
	}
}

// pgChangeLogMinID returns COALESCE(MIN(id), 0) of the change-log — the
// batching loop's keyset floor. Indexed (id is the PK) and cheap.
func pgChangeLogMinID(ctx context.Context, db *sql.DB, tableRef string) (int64, error) {
	var minID int64
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MIN(id), 0) FROM "+tableRef).Scan(&minID); err != nil {
		return 0, err
	}
	return minID, nil
}

// pgChangeLogStats returns COALESCE(MIN(id), 0) and COUNT(*) of the change-log.
// tableRef is the already-quoted schema.table reference.
func pgChangeLogStats(ctx context.Context, db *sql.DB, tableRef string) (minID, count int64, err error) {
	err = db.QueryRowContext(ctx, "SELECT COALESCE(MIN(id), 0), COUNT(*) FROM "+tableRef).Scan(&minID, &count)
	if err != nil {
		return 0, 0, err
	}
	return minID, count, nil
}
