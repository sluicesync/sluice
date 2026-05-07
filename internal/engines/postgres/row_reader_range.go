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

	"github.com/orware/sluice/internal/ir"
)

// RangeBounds implements [ir.RangeBoundsQuerier]. Queries the source
// for MIN(pk) and MAX(pk) on a single column. Empty tables surface as
// (nil, nil, nil) so the orchestrator routes to single-reader without
// a special error code.
//
// On a snapshot-pinned reader (the one returned by
// [Engine.OpenSnapshotStream]) this would conflict with a concurrent
// row-stream because both share the same pinned *sql.Conn — the
// query interface allows only one in-flight statement at a time. The
// snapshot path doesn't currently invoke parallel-copy, so this is
// not exercised in practice; the defensive check exists so a future
// caller doesn't deadlock silently.
func (r *RowReader) RangeBounds(ctx context.Context, table *ir.Table, pkColumn string) (minVal, maxVal any, err error) {
	if r.closer == nil {
		// Snapshot-pinned reader; concurrent queries would deadlock.
		return nil, nil, errors.New("postgres: RangeBounds: not supported on snapshot-pinned reader")
	}
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

// CountRows implements [ir.RowCounter]. Returns an approximate row
// count via pg_class.reltuples — fast (one catalog lookup, no full
// scan) but staleness-tolerant: on a table that hasn't been
// autovacuumed in a while the estimate can lag actual cardinality by
// orders of magnitude. The throughput-metric layer is the only
// consumer; it treats the result as an ETA hint, not a correctness
// invariant.
//
// Returns (0, nil) when the table doesn't appear in pg_class (e.g. a
// non-default schema), or when the reader is snapshot-pinned
// (closer == nil) — in the latter case a concurrent CountRows would
// deadlock against the in-flight row-stream on the same connection.
// The throughput-metric layer treats zero as "no estimate".
func (r *RowReader) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("postgres: CountRows: table is nil")
	}
	if r.closer == nil {
		// Snapshot-pinned reader; concurrent queries would deadlock.
		return 0, nil
	}
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
	// reltuples is -1 on tables that have never been analyzed; clamp
	// to 0 so the orchestrator's "no estimate" path triggers cleanly.
	if count < 0 {
		count = 0
	}
	return count, nil
}
