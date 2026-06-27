// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Range-bounds, row-count, and keyset-boundary queries for the parallel-
// bulk-copy orchestrator (ADR-0128 within-table chunking; #3 of the SQLite
// queue). Mirrors the Postgres / MySQL implementations:
//
//   - RangeBounds (MIN/MAX) drives the single-integer-PK MIN/MAX/divide
//     chunk strategy;
//   - SampleKeysetBoundaries (ROW_NUMBER() window) drives the non-integer /
//     composite-PK sampled-keyset strategy;
//   - CountRows / EstimateRowCount feed the ETA probe and the chunk
//     DECISION respectively.
//
// These all run STRICTLY pre-stream and single-goroutine on the primary
// reader (the chunk-boundary decision precedes any per-chunk copy stream),
// so they share the reader's *sql.DB pool safely; SQLite permits concurrent
// readers, and the source is opened read-only with a busy-timeout.
//
// # The `.sql`-dump source stays single-reader (the load-bearing routing)
//
// A `.sql` TEXT dump source (ADR-0130) is materialized into a temp DB owned
// by THIS reader (r.tempPath != ""). The orchestrator mints each per-chunk
// reader via Engine.OpenRowReader, which for a dump would RE-materialize a
// fresh independent temp DB per chunk — wasteful (re-parsing the dump N
// times) for no benefit. So a dump source is routed to the single-reader
// path: EstimateRowCount (the chunk-DECISION surface, [shouldParallelChunk])
// returns (0, nil) — "no estimate → single-stream" — and RangeBounds /
// SampleKeysetBoundaries defensively report "no chunking" too, so even a
// future caller that bypassed the estimate cannot chunk a dump. The binary
// `.db` path (r.tempPath == "") is the one that gains chunking.

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Static assertions: the SQLite reader implements the optional chunk-
// orchestration surfaces.
var (
	_ ir.RangeBoundsQuerier = (*RowReader)(nil)
	_ ir.KeysetSampler      = (*RowReader)(nil)
	_ ir.RowCounter         = (*RowReader)(nil)
	_ ir.RowCountEstimator  = (*RowReader)(nil)
)

// RangeBounds implements [ir.RangeBoundsQuerier]: MIN/MAX of a single
// integer PK column for the MIN/MAX/divide chunk strategy. An empty table
// surfaces as (nil, nil, nil) so the orchestrator routes to single-reader.
//
// A `.sql`-dump source reports (nil, nil, nil) — "empty / no chunking" —
// so it never chunks (see the file-header routing note); the binary `.db`
// path runs the real MIN/MAX.
func (r *RowReader) RangeBounds(ctx context.Context, table *ir.Table, pkColumn string) (minVal, maxVal any, err error) {
	if table == nil {
		return nil, nil, errors.New("sqlite: RangeBounds: table is nil")
	}
	if pkColumn == "" {
		return nil, nil, errors.New("sqlite: RangeBounds: pkColumn is empty")
	}
	if r.tempPath != "" {
		// Materialized `.sql` dump: single-reader only (file-header note).
		return nil, nil, nil
	}
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		quoteIdent(pkColumn), quoteIdent(pkColumn), quoteIdent(table.Name))

	rows, qerr := r.db.QueryContext(ctx, q) //nolint:rowserrcheck,sqlclosecheck // both handled below; linters don't follow this scope
	if qerr != nil {
		return nil, nil, fmt.Errorf("sqlite: RangeBounds query: %w", qerr)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return nil, nil, fmt.Errorf("sqlite: RangeBounds rows: %w", rerr)
		}
		return nil, nil, errors.New("sqlite: RangeBounds: zero rows from MIN/MAX query")
	}
	var minHolder, maxHolder sql.NullInt64
	if scanErr := rows.Scan(&minHolder, &maxHolder); scanErr != nil {
		return nil, nil, fmt.Errorf("sqlite: RangeBounds scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, nil, fmt.Errorf("sqlite: RangeBounds rows: %w", rerr)
	}
	if !minHolder.Valid || !maxHolder.Valid {
		// Empty table.
		return nil, nil, nil
	}
	return minHolder.Int64, maxHolder.Int64, nil
}

// CountRows implements [ir.RowCounter]: an exact `SELECT COUNT(*)` for the
// ETA probe. A local SQLite file makes the count cheap (covering scan of a
// PK index in the common case), so an exact count beats an approximation,
// and SQLite's concurrent-reader support means it can run on the reader's
// pool alongside the in-flight copy stream without conflict. Returns
// (0, nil) — "no estimate" — for a `.sql`-dump source.
func (r *RowReader) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("sqlite: CountRows: table is nil")
	}
	if r.tempPath != "" {
		return 0, nil
	}
	return r.countRows(ctx, table)
}

// EstimateRowCount implements [ir.RowCountEstimator]: the pre-stream chunk-
// DECISION row count ([shouldParallelChunk]). It is the same exact
// `SELECT COUNT(*)` as [CountRows] (cheap on a local file) EXCEPT it is the
// designated routing point for the `.sql`-dump source: a dump returns
// (0, nil) so the orchestrator keeps it on the single-reader path (the
// per-chunk readers would each re-materialize the dump — see the file-
// header note). A real `.db` returns the exact count.
func (r *RowReader) EstimateRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("sqlite: EstimateRowCount: table is nil")
	}
	if r.tempPath != "" {
		// Materialized `.sql` dump: route to single-reader (file-header note).
		return 0, nil
	}
	return r.countRows(ctx, table)
}

// countRows runs the exact COUNT(*) shared by CountRows and
// EstimateRowCount. Identifiers are quoted, not parameterized (SQLite does
// not bind identifiers).
func (r *RowReader) countRows(ctx context.Context, table *ir.Table) (int64, error) {
	q := "SELECT COUNT(*) FROM " + quoteIdent(table.Name)
	rows, err := r.db.QueryContext(ctx, q) //nolint:rowserrcheck,sqlclosecheck // handled below
	if err != nil {
		return 0, fmt.Errorf("sqlite: CountRows query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return 0, fmt.Errorf("sqlite: CountRows rows: %w", rerr)
		}
		return 0, nil
	}
	var count int64
	if scanErr := rows.Scan(&count); scanErr != nil {
		return 0, fmt.Errorf("sqlite: CountRows scan: %w", scanErr)
	}
	if rerr := rows.Err(); rerr != nil {
		return 0, fmt.Errorf("sqlite: CountRows rows: %w", rerr)
	}
	if count < 0 {
		count = 0
	}
	return count, nil
}

// SampleKeysetBoundaries implements [ir.KeysetSampler] (ADR-0128 / the
// ADR-0096 strategy): n-1 interior boundary tuples that split the table
// into n approximately equal ROW-COUNT slices ordered by the PK columns —
// the path for a non-integer single PK (text / blob / decimal-as-text) or
// a composite PK that MIN/MAX/divide can't handle.
//
// One windowed scan (SQLite 3.25+ window functions; modernc supports
// them): ROW_NUMBER() ranks each row in PK order, COUNT(*) OVER () gives
// the total, and we keep the rows at rank ceil(total*k/n) for k in 1..n-1.
// The split is by actual row count, so it is skew-free on any keyspace
// clustering — the reason the keyset strategy exists for exactly the
// string / blob keys MIN/MAX/divide would skew on.
//
// CRITICAL exactly-once contract (the Bug-74 silent-row-loss class): the
// window ORDER BY references the TABLE-QUALIFIED real PK columns, so it
// uses each column's INTRINSIC collation — the SAME order the per-chunk
// [ReadRowsBatchBounded] lower/upper bounds and ORDER BY use (they are
// also column-collation by construction; see row_reader_batch.go). No
// explicit COLLATE is injected anywhere. That agreement is what makes the
// union of the half-open (boundary[k-1], boundary[k]] chunks reconstruct
// every row exactly once for a NOCASE / custom-collation TEXT PK.
//
// Fewer than n-1 distinct boundaries (a tiny / heavily-duplicate-keyed
// table) or an empty table returns fewer / zero tuples — NOT an error; the
// orchestrator collapses to fewer chunks or single-reader. A `.sql`-dump
// source returns zero tuples (single-reader; file-header note).
func (r *RowReader) SampleKeysetBoundaries(ctx context.Context, table *ir.Table, pkColumns []string, n int) ([][]any, error) {
	if table == nil {
		return nil, errors.New("sqlite: SampleKeysetBoundaries: table is nil")
	}
	if len(pkColumns) == 0 {
		return nil, errors.New("sqlite: SampleKeysetBoundaries: no PK columns")
	}
	if n <= 1 {
		return nil, fmt.Errorf("sqlite: SampleKeysetBoundaries: n must be > 1, got %d", n)
	}
	if r.tempPath != "" {
		// Materialized `.sql` dump: single-reader only (file-header note).
		return nil, nil
	}

	pkColsIR, err := pkColumnsOf(table, pkColumns)
	if err != nil {
		return nil, fmt.Errorf("sqlite: SampleKeysetBoundaries: %w", err)
	}

	tbl := quoteIdent(table.Name)
	// projection: raw-storage-class PK values (selectColumnExpr coalesce-
	// wraps a temporal PK so modernc returns its raw storage class, matching
	// what the cursor predicate binds — see selectColumnExpr).
	proj := make([]string, len(pkColsIR))
	outNames := make([]string, len(pkColsIR))
	qualified := make([]string, len(pkColsIR))
	for i, c := range pkColsIR {
		proj[i] = selectColumnExpr(c)
		outNames[i] = quoteIdent(c.Name)
		qualified[i] = tbl + "." + quoteIdent(c.Name)
	}
	projList := strings.Join(proj, ", ")
	outList := strings.Join(outNames, ", ")
	orderList := strings.Join(qualified, ", ")

	// Target ranks ceil(total*k/n) for k in 1..n-1, as integer arithmetic:
	// (total*k + n-1) / n. SQLite integer division truncates toward zero, so
	// the +n-1 lifts a positive quotient to its ceiling. n and k are
	// orchestrator ints (no user input → no bind needed).
	rankExpr := make([]string, 0, n-1)
	for k := 1; k < n; k++ {
		rankExpr = append(rankExpr, fmt.Sprintf("(total*%d + %d) / %d", k, n-1, n))
	}
	stmt := fmt.Sprintf(`SELECT %s FROM (
			SELECT %s,
			       ROW_NUMBER() OVER (ORDER BY %s) AS rn,
			       COUNT(*)     OVER ()            AS total
			FROM %s
		) s
		WHERE rn IN (%s)
		ORDER BY rn`,
		outList, projList, orderList, tbl, strings.Join(rankExpr, ", "))

	rows, qerr := r.db.QueryContext(ctx, stmt) //nolint:rowserrcheck,sqlclosecheck // handled below
	if qerr != nil {
		return nil, fmt.Errorf("sqlite: SampleKeysetBoundaries query: %w", qerr)
	}
	defer func() { _ = rows.Close() }()

	out, err := scanKeysetBoundaryRows(rows, len(pkColsIR))
	if err != nil {
		return nil, fmt.Errorf("sqlite: SampleKeysetBoundaries: %w", err)
	}
	return out, nil
}

// pkColumnsOf resolves the table's PK column names to their *ir.Column in
// PK order, erroring if any is missing from the column list.
func pkColumnsOf(table *ir.Table, pkColumns []string) ([]*ir.Column, error) {
	out := make([]*ir.Column, len(pkColumns))
	for i, name := range pkColumns {
		var found *ir.Column
		for _, c := range table.Columns {
			if c.Name == name {
				found = c
				break
			}
		}
		if found == nil {
			return nil, fmt.Errorf("primary key column %q not found in table %q", name, table.Name)
		}
		out[i] = found
	}
	return out, nil
}

// scanKeysetBoundaryRows scans each result row into a width-pkWidth []any
// tuple. modernc hands back each PK value's native storage-class Go type on
// the *any path (TEXT→string, INTEGER→int64, REAL→float64, BLOB→[]byte);
// BLOB []byte is copied so the scan buffer isn't reused under us. The
// values feed the chunk-boundary comparator and the cursor predicate, which
// round-trip them back as `?` params compared against the column in its
// native collation.
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
