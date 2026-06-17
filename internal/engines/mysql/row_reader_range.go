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
	"strings"

	"sluicesync.dev/sluice/internal/ir"
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

// SampleKeysetBoundaries implements [ir.KeysetSampler] (ADR-0096). It
// returns n-1 interior boundary tuples that split the table into n
// approximately equal ROW-COUNT slices ordered by the PK columns, for a
// non-integer / composite PK that the MIN/MAX/divide path can't handle.
//
// One windowed scan of the PK index does it: ROW_NUMBER() assigns each
// row its 1-based PK-ordered rank, COUNT(*) OVER () gives the total, and
// we keep the rows at rank ceil(total*k/n) for k in 1..n-1. The PK index
// is covering for this query (only PK columns are projected), so it never
// touches the clustered row. The split is by actual count, so it is
// skew-free regardless of how clustered the keyspace is.
//
// On a snapshot-pinned reader this would conflict with a concurrent
// row-stream on the shared pinned conn; returns an error so the
// orchestrator falls back to single-reader. It also runs strictly
// pre-stream on the migrate reader, so the *sql.DB-backed query is safe.
//
// Fewer than n-1 distinct boundaries (tiny or heavily-duplicate-keyed
// table) or an empty table returns fewer / zero tuples — not an error;
// the orchestrator collapses to fewer chunks or single-reader.
func (r *RowReader) SampleKeysetBoundaries(ctx context.Context, table *ir.Table, pkColumns []string, n int) ([][]any, error) {
	if r.closer == nil {
		return nil, errors.New("mysql: SampleKeysetBoundaries: not supported on snapshot-pinned reader")
	}
	if table == nil {
		return nil, errors.New("mysql: SampleKeysetBoundaries: table is nil")
	}
	if len(pkColumns) == 0 {
		return nil, errors.New("mysql: SampleKeysetBoundaries: no PK columns")
	}
	if n <= 1 {
		return nil, fmt.Errorf("mysql: SampleKeysetBoundaries: n must be > 1, got %d", n)
	}

	tableRef := quoteIdent(table.Name)
	if r.schema != "" {
		tableRef = quoteIdent(r.schema) + "." + quoteIdent(table.Name)
	}
	pkQuoted := make([]string, len(pkColumns))
	for i, c := range pkColumns {
		pkQuoted[i] = quoteIdent(c)
	}
	pkList := strings.Join(pkQuoted, ", ")

	// Target ranks ceil(total*k/n) for k in 1..n-1. The CASE arithmetic
	// is integer ceil: (total*k + n - 1) / n. Keeping every k in one IN
	// list means a single scan.
	rankExpr := make([]string, 0, n-1)
	for k := 1; k < n; k++ {
		rankExpr = append(rankExpr, fmt.Sprintf("(total*%d + %d) DIV %d", k, n-1, n))
	}
	q := fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s,
			       ROW_NUMBER() OVER (ORDER BY %s) AS rn,
			       COUNT(*)     OVER ()            AS total
			FROM %s
		) s
		WHERE rn IN (%s)
		ORDER BY rn`,
		pkList, pkList, pkList, tableRef, strings.Join(rankExpr, ", "))

	rows, qerr := r.q.QueryContext(ctx, q) //nolint:rowserrcheck,sqlclosecheck // handled below
	if qerr != nil {
		return nil, fmt.Errorf("mysql: SampleKeysetBoundaries query: %w", qerr)
	}
	defer func() { _ = rows.Close() }()

	out, err := scanKeysetBoundaryRows(rows, len(pkColumns))
	if err != nil {
		return nil, fmt.Errorf("mysql: SampleKeysetBoundaries: %w", err)
	}
	return out, nil
}

// scanKeysetBoundaryRows scans each result row into a width-pkWidth
// []any tuple. Driver-returned []byte (the common shape for MySQL
// string/binary/decimal columns) is copied so the scan buffer isn't
// reused under us; other scalar shapes pass through. The values feed the
// chunk-boundary comparator and the cursor predicate, which both handle
// []byte / string / int64 / time.Time uniformly.
func scanKeysetBoundaryRows(rows *sql.Rows, pkWidth int) ([][]any, error) {
	var out [][]any
	for rows.Next() {
		holders := make([]any, pkWidth)
		ptrs := make([]any, pkWidth)
		for i := range holders {
			ptrs[i] = &holders[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan boundary: %w", err)
		}
		tuple := make([]any, pkWidth)
		for i, v := range holders {
			if b, ok := v.([]byte); ok {
				cp := make([]byte, len(b))
				copy(cp, b)
				tuple[i] = cp
			} else {
				tuple[i] = v
			}
		}
		out = append(out, tuple)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("boundary rows: %w", err)
	}
	return out, nil
}
