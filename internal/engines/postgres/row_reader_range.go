// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Range-bounds and row-count queries for the parallel-bulk-copy
// orchestrator (v0.5.0).
//
// The orchestrator's chunk-boundary computation needs MIN(pk) /
// MAX(pk) on the source table to divide the PK range into N parallel
// chunks; the throughput-metric layer needs an approximate row count
// to feed ETA. Both sit on the [RowReader] (rather than as separate
// types) because they share the connection lifetime and identifier
// quoting with the existing row-stream code.
//
// ETA uses pg_class.reltuples for the row-count estimate when the
// table is in a queryable schema. reltuples is an autovacuum-
// maintained estimate; on a table that hasn't been vacuumed in a
// while it can be off by orders of magnitude. For a multi-TB load
// the v0.5.0 trade-off is "fast estimate beats accurate count" — a
// COUNT(*) on a 500 GB table can take minutes, which is exactly the
// kind of metadata cost the throughput-metric layer is supposed to
// hide. ADR-0019 documents the choice.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// RangeBounds implements [ir.RangeBoundsQuerier]. Queries the source
// for MIN(pk) and MAX(pk) on a single column. Empty tables surface as
// (nil, nil, nil) so the orchestrator routes to single-reader without
// a special error code.
//
// Safe on a snapshot-pinned reader (ADR-0079 v1.1): MIN/MAX is a single
// statement that fully closes its rows (deferred Close below) before the
// caller proceeds, and the chunk-boundary decision always runs BEFORE the
// per-chunk copy streams open (single-goroutine, [tryParallelCopyTable] →
// [resolveChunks] precedes [runChunks]) — so it never overlaps a stream on
// the shared pinned *sql.Conn. This is what lets the sync fast cold-start
// chunk a large table within-table; the pre-v1.1 closer==nil refusal here
// was the conservative guard that blocked it.
func (r *RowReader) RangeBounds(ctx context.Context, table *ir.Table, pkColumn string) (minVal, maxVal any, err error) {
	if table == nil {
		return nil, nil, errors.New("postgres: RangeBounds: table is nil")
	}
	if pkColumn == "" {
		return nil, nil, errors.New("postgres: RangeBounds: pkColumn is empty")
	}
	tableRef := quoteIdent(r.schema) + "." + quoteIdent(table.Name)
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		quoteIdent(pkColumn), quoteIdent(pkColumn), tableRef)

	rows, qerr := r.q.QueryContext(ctx, q) //nolint:rowserrcheck,sqlclosecheck // both handled below; linters don't follow this scope
	if qerr != nil {
		return nil, nil, fmt.Errorf("postgres: RangeBounds query: %w", qerr)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return nil, nil, fmt.Errorf("postgres: RangeBounds rows: %w", rerr)
		}
		return nil, nil, errors.New("postgres: RangeBounds: zero rows from MIN/MAX query")
	}
	var minHolder, maxHolder sql.NullInt64
	if scanErr := rows.Scan(&minHolder, &maxHolder); scanErr != nil {
		return nil, nil, fmt.Errorf("postgres: RangeBounds scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, nil, fmt.Errorf("postgres: RangeBounds rows: %w", rerr)
	}
	if !minHolder.Valid || !maxHolder.Valid {
		// Empty table.
		return nil, nil, nil
	}
	return minHolder.Int64, maxHolder.Int64, nil
}

// CountRows implements [ir.RowCounter]. Returns a row count via
// pg_class.reltuples — fast (one catalog lookup, no full scan).
//
// reltuples is autovacuum-/ANALYZE-maintained: on a never-analyzed
// table it is the sentinel -1 (PG14+) or 0 (older PG). That stale
// ~0 is not merely an ETA miss — [shouldParallelChunk] consumes
// CountRows for parallel-copy *eligibility*, so a never-analyzed
// large table (the normal migrate cold-start: load/restore a
// source, then migrate, before autovacuum runs) would silently
// fall to the single-reader path. That was ADR-0042 finding N1.
// So when reltuples is non-positive — never-analyzed, or genuinely
// empty — we fall back to an exact COUNT(*): one seq scan, one
// time at preflight, triggered only when stats are absent (good
// stats short-circuit before the scan), and correct whether the
// table turns out to be large or empty. The wart has a name (N1),
// a test, and this comment per the codebase's clean-code tenet.
//
// Returns (0, nil) when the table doesn't appear in pg_class (e.g. a
// non-default schema). The throughput-metric layer treats zero as "no
// estimate" and the orchestrator routes a zero count to the single-reader
// path.
//
// Pinned-reader discipline (the ADR-0079 v1.1 contract). A snapshot-pinned
// reader — the externally-owned stream reader (closer == nil) OR a
// self-closing SnapshotImporter reader (snapshotPinned) — runs all its
// queries on ONE pinned *sql.Conn, which database/sql serialises through
// closemu: two OVERLAPPING queries on it self-deadlock. CountRows never
// overlaps now (the reltuples probe fully closes its rows in
// [reltuplesEstimate] before any second query — the root cause of the
// a8d065d deadlock was the old code firing exactCount while the reltuples
// rows were still open). The remaining rule is purely cost: on a pinned
// reader we DECLINE the exact-COUNT(*) fallback, because that full seq scan
// would run on the live snapshot connection (potentially minutes on a huge
// table). So a pinned reader gets the reltuples estimate when stats exist
// (enabling within-table chunking on the sync fast path, ADR-0079 v1.1) and
// "no estimate" (0 → single-stream) when they don't — never-analyzed tables
// stay single-stream, exactly as before. The non-pinned migrate reader keeps
// the exact fallback (ADR-0042 N1). The decision-phase probe always precedes
// the copy stream (single-goroutine), so no probe ever races a stream.
func (r *RowReader) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("postgres: CountRows: table is nil")
	}
	count, err := r.reltuplesEstimate(ctx, table)
	if err != nil {
		return 0, err
	}
	if count > 0 {
		return count, nil
	}
	// reltuples non-positive => never-analyzed (sentinel -1 / 0) or
	// genuinely empty.
	if r.closer == nil || r.snapshotPinned {
		// Pinned reader: skip the exact COUNT(*) seq scan (cost, not
		// safety — the reltuples rows are already closed). 0 ⇒ single-stream.
		return 0, nil
	}
	// Non-pinned migrate reader: resolve exactly so parallel-copy eligibility
	// is correct on a freshly-loaded source (ADR-0042 N1). Safe here too — the
	// reltuples rows closed in reltuplesEstimate before this second query.
	return r.exactCount(ctx, table)
}

// reltuplesEstimate runs the pg_class.reltuples catalog lookup and returns
// the estimate (0 when the table isn't in pg_class). It fully consumes and
// CLOSES its result rows before returning, so a caller may safely issue a
// second query on the same pinned *sql.Conn afterward — this sequencing is
// what makes [CountRows]'s exactCount fallback deadlock-free on a single
// pinned connection (the a8d065d fix lives here).
func (r *RowReader) reltuplesEstimate(ctx context.Context, table *ir.Table) (int64, error) {
	q := `SELECT COALESCE((SELECT reltuples::bigint
	                       FROM pg_class c
	                       JOIN pg_namespace n ON n.oid = c.relnamespace
	                       WHERE n.nspname = $1 AND c.relname = $2), 0)`
	rows, err := r.q.QueryContext(ctx, q, r.schema, table.Name) //nolint:rowserrcheck,sqlclosecheck // handled below
	if err != nil {
		return 0, fmt.Errorf("postgres: CountRows query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return 0, fmt.Errorf("postgres: CountRows rows: %w", rerr)
		}
		return 0, nil
	}
	var count int64
	if scanErr := rows.Scan(&count); scanErr != nil {
		return 0, fmt.Errorf("postgres: CountRows scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return 0, fmt.Errorf("postgres: CountRows rows: %w", rerr)
	}
	return count, nil
}

// exactCount runs SELECT COUNT(*) for the table. Used only as the
// CountRows fallback when pg_class.reltuples is non-positive
// (stale/never-analyzed or empty). Identifiers are quoted, not
// parameterized — Postgres does not bind identifiers.
func (r *RowReader) exactCount(ctx context.Context, table *ir.Table) (int64, error) {
	q := `SELECT COUNT(*) FROM ` + quoteIdent(r.schema) + `.` + quoteIdent(table.Name)
	rows, err := r.q.QueryContext(ctx, q) //nolint:rowserrcheck,sqlclosecheck // handled below
	if err != nil {
		return 0, fmt.Errorf("postgres: CountRows exact COUNT(*): %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return 0, fmt.Errorf("postgres: CountRows exact rows: %w", rerr)
		}
		return 0, nil
	}
	var count int64
	if scanErr := rows.Scan(&count); scanErr != nil {
		return 0, fmt.Errorf("postgres: CountRows exact scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return 0, fmt.Errorf("postgres: CountRows exact rows: %w", rerr)
	}
	return count, nil
}
