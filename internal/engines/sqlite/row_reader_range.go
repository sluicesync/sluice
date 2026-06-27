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
	_ ir.RangeBoundsQuerier      = (*RowReader)(nil)
	_ ir.KeysetSampler           = (*RowReader)(nil)
	_ ir.RowCounter              = (*RowReader)(nil)
	_ ir.RowCountEstimator       = (*RowReader)(nil)
	_ ir.BatchedReadDisqualifier = (*RowReader)(nil)
)

// pkCursorDisqualified reports whether table's primary key contains a column
// whose resolved IR type cannot round-trip a DECODED cursor value back into
// the page's `>` / `<=` bound — SQLite's temporal and decimal families. It is
// the load-bearing silent-loss guard for the cursor path (the Bug-74 class).
//
// The cursor loops advance the `after` tuple from the DECODED streamed row
// value (the same value the writer applies), then re-bind it as `(pk) > (?)`
// for the next page. A temporal column decodes to a Go time.Time (Date /
// Timestamp) or a formatted time-of-day string (Time), and a NUMERIC column
// to a decimal string (value_decode.go) — but the column's raw STORAGE class
// is INTEGER / REAL / TEXT by app convention. Re-binding the decoded value
// then compares in the WRONG class against the column's ORDER BY:
//
//   - unixepoch / unixmillis / julian (numeric storage): a time.Time binds as
//     TEXT (`t.String()`), SQLite ranks numeric < text and NUMERIC affinity
//     can't coerce that string, so `pk > <text>` is FALSE for every row → the
//     2nd page of each chunk is EMPTY → silent truncation to one page.
//   - ISO text storage with a timezone/`T` separator: `t.String()` normalizes
//     to UTC and appends ` +0000 UTC`, reordering against the BINARY ORDER BY
//     → already-copied rows re-selected (dup) or skipped (loss).
//   - decimal-as-string: usually rescued by NUMERIC affinity but the
//     scientific-notation / precision edges are unpinned, so it is excluded
//     conservatively (a family on a family-dispatched path).
//
// The original stored form is unrecoverable from the decoded value, so such a
// table must NOT drive a cursor at all — it falls back to the whole-table
// single-reader copy. Integer / text (BINARY or NOCASE) / blob / composites
// of those round-trip exactly and stay cursor-eligible.
func pkCursorDisqualified(table *ir.Table) (disqualified bool, reason string) {
	if table == nil || table.PrimaryKey == nil {
		return false, ""
	}
	for _, pkc := range table.PrimaryKey.Columns {
		for _, c := range table.Columns {
			if c.Name != pkc.Column {
				continue
			}
			switch c.Type.(type) {
			case ir.Date, ir.Time, ir.Timestamp, ir.DateTime, ir.Decimal:
				return true, fmt.Sprintf("primary key column %q is %s, whose decoded cursor value cannot round-trip SQLite's raw storage class; single-reader path",
					c.Name, c.Type.String())
			}
			break
		}
	}
	return false, ""
}

// chunkDisqualified reports whether table must NOT take the within-table
// parallel-chunk path. Two causes: a materialized `.sql` dump (per-chunk
// readers would each re-materialize the temp DB — see the file header) or a
// PK whose decoded cursor can't round-trip ([pkCursorDisqualified]). All four
// chunk-DECISION surfaces consult it so a disqualified table routes to the
// single-reader copy uniformly.
func (r *RowReader) chunkDisqualified(table *ir.Table) bool {
	if r.tempPath != "" {
		return true
	}
	dq, _ := pkCursorDisqualified(table)
	return dq
}

// DisqualifiesBatchedRead implements [ir.BatchedReadDisqualifier]: it vetoes
// the cursor-paginated read (and thus the per-batch resume path) for a table
// whose PK can't round-trip a decoded cursor. Unlike [chunkDisqualified] it
// does NOT consider tempPath — a `.sql`-dump source reads fine through the
// SINGLE owning reader's own temp DB on the per-batch resume path; only the
// per-CHUNK re-materialization is the dump's problem.
func (r *RowReader) DisqualifiesBatchedRead(table *ir.Table) (disqualified bool, reason string) {
	return pkCursorDisqualified(table)
}

// RangeBounds implements [ir.RangeBoundsQuerier]: MIN/MAX of a single
// integer PK column for the MIN/MAX/divide chunk strategy. An empty table
// surfaces as (nil, nil, nil) so the orchestrator routes to single-reader.
//
// A chunk-disqualified source (a `.sql` dump, or a non-round-trippable PK —
// [chunkDisqualified]) reports (nil, nil, nil) — "no chunking" — so it never
// chunks; the binary `.db` integer-PK path runs the real MIN/MAX.
func (r *RowReader) RangeBounds(ctx context.Context, table *ir.Table, pkColumn string) (minVal, maxVal any, err error) {
	if table == nil {
		return nil, nil, errors.New("sqlite: RangeBounds: table is nil")
	}
	if pkColumn == "" {
		return nil, nil, errors.New("sqlite: RangeBounds: pkColumn is empty")
	}
	if r.chunkDisqualified(table) {
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
// PK index in the common case), so an exact count beats an approximation.
// This is the ONE chunk-decision surface that runs CONCURRENTLY with the
// in-flight copy and the table-pool's deferred Close (the ETA probe is
// fire-and-forget; see [kickOffRowCount]), so it must touch only
// concurrency-safe reader state: the *sql.DB handle is safe for concurrent
// use (including a concurrent Close), and tempPath is set once at
// construction and never mutated (Close removes the temp file via a sync.Once
// WITHOUT clearing the field), so [chunkDisqualified]'s read of tempPath here
// does not race Close. Returns (0, nil) — "no estimate" — for a chunk-
// disqualified source ([chunkDisqualified]: a `.sql` dump or a
// non-round-trippable PK), uniform with the other chunk-decision surfaces.
func (r *RowReader) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("sqlite: CountRows: table is nil")
	}
	if r.chunkDisqualified(table) {
		return 0, nil
	}
	return r.countRows(ctx, table)
}

// EstimateRowCount implements [ir.RowCountEstimator]: the pre-stream chunk-
// DECISION row count ([shouldParallelChunk]). It is the same exact
// `SELECT COUNT(*)` as [CountRows] (cheap on a local file) EXCEPT it is the
// designated routing point that returns (0, nil) — "route to single-reader" —
// for any chunk-disqualified source ([chunkDisqualified]): a `.sql` dump (the
// per-chunk readers would each re-materialize it) OR a PK whose decoded
// cursor can't round-trip SQLite's raw storage (temporal/decimal — the
// silent-loss guard). A real `.db` with a round-trippable PK returns the
// exact count.
func (r *RowReader) EstimateRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("sqlite: EstimateRowCount: table is nil")
	}
	if r.chunkDisqualified(table) {
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
// orchestrator collapses to fewer chunks or single-reader. A chunk-
// disqualified source (a `.sql` dump, or a non-round-trippable PK —
// [chunkDisqualified]) returns zero tuples (single-reader).
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
	if r.chunkDisqualified(table) {
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
