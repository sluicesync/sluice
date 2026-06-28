// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"

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
// DELETEs id <= Cut, and reports the post-prune stats. Idempotent: re-running
// with the same Cut deletes nothing new.
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
	// id == cut is itself durably applied and safe to remove.
	tag, err := db.ExecContext(ctx, "DELETE FROM "+tableRef+" WHERE id <= $1", opts.Cut)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: prune: delete: %w", err)
	}
	if res.Deleted, err = tag.RowsAffected(); err != nil {
		return nil, fmt.Errorf("pgtrigger: prune: rows affected: %w", err)
	}
	if res.RemainingMin, res.Remaining, err = pgChangeLogStats(ctx, db, tableRef); err != nil {
		return res, fmt.Errorf("pgtrigger: prune: stats: %w", err)
	}
	return res, nil
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
