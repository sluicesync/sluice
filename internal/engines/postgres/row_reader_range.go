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
// Safe on a snapshot-pinned reader (ADR-0079 v1.1). RangeBounds runs
// STRICTLY pre-stream: the chunk-boundary decision
// ([tryParallelCopyTable] → [resolveChunks] → [computeChunkBoundaries])
// always precedes [runChunks] opening any per-chunk copy stream, and the
// decision phase is single-goroutine — so this MIN/MAX never overlaps an
// in-flight row-stream on the shared pinned *sql.Conn. The query is a
// single fully-closed statement (the deferred Close below), so it also
// never self-overlaps. The pre-v1.1 `closer == nil` refusal here was the
// conservative guard that blocked within-table chunking on the sync fast
// cold-start; this is what re-enables it.
func (r *RowReader) RangeBounds(ctx context.Context, table *ir.Table, pkColumn string) (minVal, maxVal any, err error) {
	if table == nil {
		return nil, nil, errors.New("postgres: RangeBounds: table is nil")
	}
	if pkColumn == "" {
		return nil, nil, errors.New("postgres: RangeBounds: pkColumn is empty")
	}
	tableRef := quoteIdent(r.effectiveSchema(table)) + "." + quoteIdent(table.Name)
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
// non-default schema), or when the reader is snapshot-pinned
// (closer == nil OR snapshotPinned) — in the latter case a concurrent
// CountRows would deadlock against the in-flight row-stream on the same
// pinned connection. The throughput-metric layer ([kickOffRowCount])
// treats zero as "no estimate".
//
// The pinned-returns-0 short-circuit is LOAD-BEARING and stays: CountRows
// is wired into the ETA probe, which fires CONCURRENTLY with the in-flight
// copy stream on this same pinned reader — a count query racing the stream
// would conflict on the shared *sql.Conn. The within-table chunk DECISION
// does NOT use this method; it uses [RowReader.EstimateRowCount], which
// reads off a fresh conn precisely so the ETA path stays safe by
// construction (ADR-0079 v1.1).
func (r *RowReader) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("postgres: CountRows: table is nil")
	}
	if r.closer == nil || r.snapshotPinned {
		// Snapshot-pinned reader (externally-owned stream reader OR a
		// self-closing SnapshotImporter reader): all queries run on one
		// pinned *sql.Conn, so a count query fired while the in-flight
		// copy stream holds the conn would conflict on closemu. Return
		// "no estimate"; the ETA layer reports rate-only.
		return 0, nil
	}
	q := `SELECT COALESCE((SELECT reltuples::bigint
	                       FROM pg_class c
	                       JOIN pg_namespace n ON n.oid = c.relnamespace
	                       WHERE n.nspname = $1 AND c.relname = $2), 0)`
	rows, err := r.q.QueryContext(ctx, q, r.effectiveSchema(table), table.Name) //nolint:rowserrcheck,sqlclosecheck // handled below
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
	// reltuples non-positive => never-analyzed (sentinel -1 / 0) or
	// genuinely empty. Resolve it exactly so parallel-copy
	// eligibility is correct on a freshly-loaded source (ADR-0042
	// N1). Snapshot-pinned readers already returned above, so the
	// exact COUNT(*) cannot deadlock the in-flight stream.
	if count <= 0 {
		return r.exactCount(ctx, table)
	}
	return count, nil
}

// EstimateRowCount implements [ir.RowCountEstimator]: the pre-stream,
// chunk-DECISION-only row-count estimate (NEVER the ETA path — see
// [RowReader.CountRows]). It returns the table's pg_class.reltuples
// estimate (>0 → within-table chunking is eligible) or 0 (→ single-stream;
// a never-ANALYZEd table reports the sentinel and stays single-stream, the
// named ADR-0079 v1.1 limitation).
//
// The pinned/non-pinned split is the whole point of having a separate
// surface from CountRows:
//
//   - Pinned reader (closer == nil OR snapshotPinned — a PG snapshot
//     stream/import reader): the reltuples lookup runs on a FRESH
//     connection opened from r.estimatorDSN, NOT on the pinned *sql.Conn.
//     reltuples is snapshot-insensitive catalog metadata, so an
//     off-snapshot read is correct, and a fresh conn cannot race the
//     pinned reader's in-flight stream — that connection conflict is
//     exactly the regression a relaxed CountRows caused (the ETA probe
//     firing on the pinned conn mid-copy → driver: bad connection). The
//     exact-COUNT(*) fallback is DECLINED on a pinned reader: that full
//     seq scan would run on the live snapshot connection (minutes on a
//     huge table) — cost, not safety. So a pinned reader gets reltuples
//     when stats exist and 0 otherwise.
//   - Non-pinned migrate reader (an *sql.DB-backed reader): query on r.q
//     and KEEP the exact-COUNT(*) fallback, so migrate's chunk decision is
//     byte-identical to its prior CountRows behaviour (ADR-0042 N1 — a
//     freshly-loaded, never-ANALYZEd source still parallelizes).
func (r *RowReader) EstimateRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("postgres: EstimateRowCount: table is nil")
	}
	pinned := r.closer == nil || r.snapshotPinned
	if pinned {
		return r.reltuplesOffConn(ctx, table)
	}
	count, err := r.reltuplesEstimate(ctx, r.q, table)
	if err != nil {
		return 0, err
	}
	if count > 0 {
		return count, nil
	}
	// reltuples non-positive => never-analyzed (sentinel -1 / 0) or
	// genuinely empty; resolve exactly so the migrate chunk decision is
	// correct on a freshly-loaded source (ADR-0042 N1). The reltuples rows
	// are already closed (reltuplesEstimate fully consumes them), so this
	// second query is safe on the *sql.DB pool.
	return r.exactCount(ctx, table)
}

// reltuplesEstimate runs the pg_class.reltuples catalog lookup against q
// and returns the estimate (0 when the table isn't in pg_class). It fully
// consumes and CLOSES its result rows before returning. Shared by the
// pinned (fresh-conn) and non-pinned (pool) EstimateRowCount paths.
func (r *RowReader) reltuplesEstimate(ctx context.Context, q querier, table *ir.Table) (int64, error) {
	const stmt = `SELECT COALESCE((SELECT reltuples::bigint
	                       FROM pg_class c
	                       JOIN pg_namespace n ON n.oid = c.relnamespace
	                       WHERE n.nspname = $1 AND c.relname = $2), 0)`
	rows, err := q.QueryContext(ctx, stmt, r.effectiveSchema(table), table.Name) //nolint:rowserrcheck,sqlclosecheck // handled below
	if err != nil {
		return 0, fmt.Errorf("postgres: EstimateRowCount query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return 0, fmt.Errorf("postgres: EstimateRowCount rows: %w", rerr)
		}
		return 0, nil
	}
	var count int64
	if scanErr := rows.Scan(&count); scanErr != nil {
		return 0, fmt.Errorf("postgres: EstimateRowCount scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return 0, fmt.Errorf("postgres: EstimateRowCount rows: %w", rerr)
	}
	return count, nil
}

// reltuplesOffConn opens a FRESH short-lived connection from r.estimatorDSN
// and runs the reltuples lookup on it, leaving the reader's pinned *sql.Conn
// untouched. Used by [RowReader.EstimateRowCount] on a snapshot-pinned
// reader: pg_class.reltuples is snapshot-insensitive, so this off-snapshot
// read is correct, and it cannot race the pinned reader's in-flight stream.
// A negative reltuples (the never-ANALYZEd sentinel) or a missing
// estimatorDSN reports 0 (→ single-stream); the exact-COUNT(*) fallback is
// deliberately NOT taken on a pinned reader (a full seq scan on the live
// snapshot conn — cost, not safety).
func (r *RowReader) reltuplesOffConn(ctx context.Context, table *ir.Table) (int64, error) {
	if r.estimatorDSN == "" {
		// No DSN threaded in (e.g. a NewSnapshotRowReader built by a
		// delegating engine that didn't set it): no estimate, single-stream.
		return 0, nil
	}
	db, err := openDB(ctx, &pgConfig{dsn: r.estimatorDSN, schema: r.schema})
	if err != nil {
		return 0, fmt.Errorf("postgres: EstimateRowCount open: %w", err)
	}
	defer func() { _ = db.Close() }()
	count, err := r.reltuplesEstimate(ctx, db, table)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		// never-ANALYZEd sentinel / genuinely empty: single-stream (the
		// named ADR-0079 v1.1 limitation; the exact COUNT(*) is declined on
		// a pinned reader by design).
		return 0, nil
	}
	return count, nil
}

// exactCount runs SELECT COUNT(*) for the table. Used only as the
// CountRows fallback when pg_class.reltuples is non-positive
// (stale/never-analyzed or empty). Identifiers are quoted, not
// parameterized — Postgres does not bind identifiers.
func (r *RowReader) exactCount(ctx context.Context, table *ir.Table) (int64, error) {
	q := `SELECT COUNT(*) FROM ` + quoteIdent(r.effectiveSchema(table)) + `.` + quoteIdent(table.Name)
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
