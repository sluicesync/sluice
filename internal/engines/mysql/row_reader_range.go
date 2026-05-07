// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Range-bounds and row-count queries for the parallel-bulk-copy
// orchestrator (v0.5.0).
//
// Mirrors the Postgres implementation: MIN/MAX for chunk-boundary
// computation and an information_schema row-count estimate for ETA.
// The MySQL row-count estimate uses information_schema.tables.TABLE_ROWS,
// which InnoDB maintains as a lazy approximation. Like pg_class.reltuples
// it can lag actual cardinality on a busy table; the throughput-metric
// layer treats it as a hint.
//
// MyISAM tables (rare in modern deployments) maintain TABLE_ROWS
// exactly, so the estimate is accurate there. PlanetScale's vtgate
// surfaces a similar approximation through information_schema; the
// query shape is portable across vanilla MySQL and PlanetScale.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// RangeBounds implements [ir.RangeBoundsQuerier]. Queries the source
// for MIN(pk) and MAX(pk) on a single column. Empty tables surface as
// (nil, nil, nil).
//
// On a snapshot-pinned reader (the one returned by
// [Engine.OpenSnapshotStream]) this would conflict with a concurrent
// row-stream because both share the same pinned *sql.Conn. Returns
// an error in that case so the orchestrator falls back cleanly.
func (r *RowReader) RangeBounds(ctx context.Context, table *ir.Table, pkColumn string) (minVal, maxVal any, err error) {
	if r.closer == nil {
		return nil, nil, errors.New("mysql: RangeBounds: not supported on snapshot-pinned reader")
	}
	if table == nil {
		return nil, nil, errors.New("mysql: RangeBounds: table is nil")
	}
	if pkColumn == "" {
		return nil, nil, errors.New("mysql: RangeBounds: pkColumn is empty")
	}
	tableRef := quoteIdent(table.Name)
	if r.schema != "" {
		tableRef = quoteIdent(r.schema) + "." + quoteIdent(table.Name)
	}
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		quoteIdent(pkColumn), quoteIdent(pkColumn), tableRef)

	rows, qerr := r.q.QueryContext(ctx, q) //nolint:rowserrcheck,sqlclosecheck // handled below
	if qerr != nil {
		return nil, nil, fmt.Errorf("mysql: RangeBounds query: %w", qerr)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return nil, nil, fmt.Errorf("mysql: RangeBounds rows: %w", rerr)
		}
		return nil, nil, errors.New("mysql: RangeBounds: zero rows from MIN/MAX query")
	}
	var minHolder, maxHolder sql.NullInt64
	if scanErr := rows.Scan(&minHolder, &maxHolder); scanErr != nil {
		return nil, nil, fmt.Errorf("mysql: RangeBounds scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, nil, fmt.Errorf("mysql: RangeBounds rows: %w", rerr)
	}
	if !minHolder.Valid || !maxHolder.Valid {
		return nil, nil, nil
	}
	return minHolder.Int64, maxHolder.Int64, nil
}

// CountRows implements [ir.RowCounter]. Returns an approximate row
// count via information_schema.tables.TABLE_ROWS. InnoDB maintains
// the value as a lazy estimate; MyISAM is exact. Either way the
// orchestrator treats the value as an ETA hint, not a correctness
// invariant.
//
// Returns (0, nil) when the table doesn't appear in
// information_schema (typically because the schema/database name
// differs from r.schema), or when the reader is snapshot-pinned
// (closer == nil) — concurrent queries on the snapshot connection
// would deadlock against the in-flight row-stream.
func (r *RowReader) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("mysql: CountRows: table is nil")
	}
	if r.closer == nil {
		// Snapshot-pinned reader; concurrent queries would deadlock.
		return 0, nil
	}
	schema := r.schema
	if schema == "" {
		// information_schema lookups need the schema (database) name.
		// Without it we can't disambiguate same-named tables across
		// databases — surface an honest "no estimate" rather than
		// risk reporting a wrong-table count.
		return 0, nil
	}
	q := `SELECT COALESCE(TABLE_ROWS, 0)
	      FROM information_schema.tables
	      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	rows, err := r.q.QueryContext(ctx, q, schema, table.Name) //nolint:rowserrcheck,sqlclosecheck // handled below
	if err != nil {
		return 0, fmt.Errorf("mysql: CountRows query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return 0, fmt.Errorf("mysql: CountRows rows: %w", rerr)
		}
		return 0, nil
	}
	var count int64
	if scanErr := rows.Scan(&count); scanErr != nil {
		return 0, fmt.Errorf("mysql: CountRows scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return 0, fmt.Errorf("mysql: CountRows rows: %w", rerr)
	}
	if count < 0 {
		count = 0
	}
	return count, nil
}
