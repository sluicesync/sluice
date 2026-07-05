// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
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

// pgPruneBatchSize bounds one DELETE batch (repo-audit P-1). A monolithic
// `DELETE ... WHERE id <= cut` over a large backlog is one long transaction — a
// WAL burst plus long-held row locks on the source. [triggercdc.InBatches] steps
// the id keyset in bounded batches instead (id is the change-log's BIGSERIAL PK,
// so each step is an index range scan), one short statement per batch. ~20k rows
// keeps each statement short (a bounded WAL burst, briefly-held locks) while
// still clearing a multi-million-row backlog within a few auto-prune ticks. The
// tick budget + recount cadence live in [triggercdc] (shared across trigger
// engines).
const pgPruneBatchSize = 20_000

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
	if res.Deleted, _, err = triggercdc.InBatches(
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
// under [triggercdc.AutoPruneTickBudget] (a partial tick resumes on the next
// cadence). A non-positive cut is a safe no-op. The DELETEs run on the reader's dedicated
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
	deleted, done, err := triggercdc.InBatches(
		ctx, minID, cut, pgPruneBatchSize, triggercdc.AutoPruneTickBudget, pgPruneBatch(db, tableRef),
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
// arithmetic per tick, re-anchored by a true COUNT every [triggercdc.RecountEvery]-th
// tick (P-1 — never a per-tick COUNT(*) full scan). Purely observability; a
// failed recount keeps the stale estimate and the next recount tick retries.
func (r *CDCReader) notePruneTick(ctx context.Context, db *sql.DB, tableRef string, deleted int64, done bool) {
	if r.pruneBook.Tick() {
		if minID, count, err := pgChangeLogStats(ctx, db, tableRef); err == nil {
			r.pruneBook.Anchor(count)
			slog.DebugContext(ctx, "pgtrigger: auto-prune recount",
				slog.Int64("remaining", count), slog.Int64("min_id", minID))
		}
	} else {
		r.pruneBook.NoteDeleted(deleted)
	}
	if !done {
		slog.DebugContext(ctx, "pgtrigger: auto-prune tick budget exhausted; resuming next tick",
			slog.Int64("deleted", deleted))
	}
}

// pgPruneBatch binds one bounded keyset DELETE over db/tableRef. Split out so
// the CLI [Prune] and the auto-prune sidecar share the exact statement shape.
func pgPruneBatch(db *sql.DB, tableRef string) triggercdc.BatchFunc {
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
