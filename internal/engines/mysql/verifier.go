// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// defaultCountChunkSize controls the chunked-count fallback for
// PlanetScale large tables. PS enforces a per-query row-read budget
// (~100K rows by default); 50K leaves a 2x safety margin. Configurable
// via SchemaReader.CountChunkSize when wired to the CLI / config; the
// constant is the conservative default.
const defaultCountChunkSize = 50000

// ExactRowCount implements [ir.Verifier]. Returns the exact row count
// for the given table.
//
// Two strategies:
//   - **Single-shot SELECT COUNT(*)** when the table has no usable
//     integer PK (composite PK, no PK, or non-integer PK). Simple,
//     works against vanilla MySQL. May hit PlanetScale's per-query
//     row-read budget on tables with > ~100K rows; the resulting
//     error surfaces clearly with PS's `row count exceeded` message
//     baked into the wrap.
//   - **Chunked SUM(COUNT(*) per PK range)** when the table has a
//     single integer PK. Splits the count across PK ranges of
//     [defaultCountChunkSize] rows each so each subquery stays under
//     PS's row-read budget. Scales to billions of rows on PS-MySQL.
//
// Authoritative count (vs. RowReader's CountRows which uses
// information_schema.tables.table_rows for ETA hints — that's
// approximate and lags actual cardinality on InnoDB tables that
// haven't been ANALYZE-d recently): `sluice verify` needs counts
// that won't silently disagree with what's stored, so we pay the
// full-scan cost.
//
// Returns (0, error) on any operational failure; (0, nil) is reserved
// for "table is empty."
func (r *SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("mysql: ExactRowCount: table is nil")
	}
	if r.db == nil {
		return 0, errors.New("mysql: ExactRowCount: reader not opened")
	}
	pkCol, ok := singleIntegerPKColumn(table)
	if !ok {
		// Fall back to single-shot count. May hit PS row-read limit
		// on large tables; the error surfaces as-is and the operator
		// can workaround by adding a single-int PK or restricting
		// verify scope to chunkable tables.
		return singleShotCount(ctx, r.db, table.Name)
	}
	return chunkedCount(ctx, r.db, table.Name, pkCol, defaultCountChunkSize)
}

// SampleRowHashes implements [ir.SampleVerifier]. Same shape as the
// Postgres sibling — see that doc for the strategy + cross-engine
// constraint discussion.
//
// MySQL-specific tweaks: identifier quoting via backticks; column
// rendering via CAST(col AS CHAR) which renders integers as decimal,
// floats as their default text repr, datetime/timestamp as canonical
// MySQL form; CONCAT_WS skips NULLs same as PG.
func (r *SchemaReader) SampleRowHashes(ctx context.Context, table *ir.Table, n int, seed int64) ([]ir.SampledRowHash, error) {
	if table == nil {
		return nil, errors.New("mysql: SampleRowHashes: table is nil")
	}
	if r.db == nil {
		return nil, errors.New("mysql: SampleRowHashes: reader not opened")
	}
	if n <= 0 {
		return nil, fmt.Errorf("mysql: SampleRowHashes: n must be > 0, got %d", n)
	}
	pkCols, err := singleOrCompositePKColumns(table)
	if err != nil {
		return nil, fmt.Errorf("mysql: SampleRowHashes %s: %w", table.Name, err)
	}
	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = fmt.Sprintf("IFNULL(CAST(%s AS CHAR), '')", quoteIdent(c.Name))
	}
	pkExpr := strings.Join(quotedPKList(pkCols), ` , '|' , `)
	pkSelect := fmt.Sprintf("CONCAT_WS('|', %s)", pkExpr)
	if len(pkCols) == 1 {
		pkSelect = fmt.Sprintf("CAST(%s AS CHAR)", quoteIdent(pkCols[0]))
	}
	hashExpr := "MD5(CONCAT_WS('|', " + strings.Join(cols, ", ") + "))"
	q := fmt.Sprintf(
		"SELECT %s AS pk, %s AS hash FROM %s ORDER BY MD5(CONCAT(%s, '%d')) LIMIT %d",
		pkSelect, hashExpr,
		quoteIdent(table.Name),
		pkSelect, seed, n,
	)
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("mysql: SampleRowHashes %s: %w", table.Name, err)
	}
	defer func() { _ = rows.Close() }()

	var samples []ir.SampledRowHash
	for rows.Next() {
		var pk, hash string
		if err := rows.Scan(&pk, &hash); err != nil {
			return nil, fmt.Errorf("mysql: SampleRowHashes scan %s: %w", table.Name, err)
		}
		samples = append(samples, ir.SampledRowHash{PrimaryKey: pk, Hash: hash})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: SampleRowHashes rows %s: %w", table.Name, err)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].PrimaryKey < samples[j].PrimaryKey })
	return samples, nil
}

// singleShotCount returns SELECT COUNT(*) FROM table. Fast on small
// tables; may hit PS row-read budget on tables > ~100K rows.
func singleShotCount(ctx context.Context, db *sql.DB, tableName string) (int64, error) {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(tableName))
	var count int64
	if err := db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("mysql: count %s: %w", tableName, err)
	}
	return count, nil
}

// chunkedCount sums COUNT(*) across PK ranges of chunkSize rows so
// each subquery stays under PlanetScale's per-query row-read budget.
// Cost: ⌈row_count / chunkSize⌉ + 1 queries (the +1 is for MIN/MAX).
// For a 1M-row table at chunkSize=50000, that's 21 queries — fast on
// any modern MySQL, well under PS's per-query budget.
func chunkedCount(ctx context.Context, db *sql.DB, tableName, pkCol string, chunkSize int64) (int64, error) {
	// Get PK bounds.
	boundsQ := fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM %s`,
		quoteIdent(pkCol), quoteIdent(pkCol), quoteIdent(tableName))
	var minV, maxV sql.NullInt64
	if err := db.QueryRowContext(ctx, boundsQ).Scan(&minV, &maxV); err != nil {
		return 0, fmt.Errorf("mysql: chunked-count bounds %s: %w", tableName, err)
	}
	if !minV.Valid || !maxV.Valid {
		return 0, nil // empty table
	}
	// Walk PK ranges. Half-open interval [start, end); the final
	// chunk uses <= maxV to include the maximum row.
	var total int64
	for start := minV.Int64; start <= maxV.Int64; start += chunkSize {
		end := start + chunkSize
		var partial int64
		var partQ string
		if end > maxV.Int64 {
			partQ = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s >= ? AND %s <= ?`,
				quoteIdent(tableName), quoteIdent(pkCol), quoteIdent(pkCol))
			if err := db.QueryRowContext(ctx, partQ, start, maxV.Int64).Scan(&partial); err != nil {
				return 0, fmt.Errorf("mysql: chunked-count partial [%d..%d] %s: %w", start, maxV.Int64, tableName, err)
			}
		} else {
			partQ = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s >= ? AND %s < ?`,
				quoteIdent(tableName), quoteIdent(pkCol), quoteIdent(pkCol))
			if err := db.QueryRowContext(ctx, partQ, start, end).Scan(&partial); err != nil {
				return 0, fmt.Errorf("mysql: chunked-count partial [%d..%d) %s: %w", start, end, tableName, err)
			}
		}
		total += partial
	}
	return total, nil
}

// singleIntegerPKColumn reports whether the table has a single
// integer-typed primary key. Tables with composite PKs, no PK, or
// non-integer PK return ok=false; the caller falls back to single-
// shot counting.
func singleIntegerPKColumn(t *ir.Table) (string, bool) {
	if t.PrimaryKey == nil || len(t.PrimaryKey.Columns) != 1 {
		return "", false
	}
	pkName := t.PrimaryKey.Columns[0].Column
	for _, c := range t.Columns {
		if c.Name != pkName {
			continue
		}
		if _, ok := c.Type.(ir.Integer); ok {
			return pkName, true
		}
		return "", false
	}
	return "", false
}

// singleOrCompositePKColumns is the sample-mode equivalent of
// singleIntegerPKColumn — returns the PK columns regardless of type
// (sample-mode only needs to construct a deterministic ordering, not
// arithmetic). Returns an error when there's no PK at all.
func singleOrCompositePKColumns(t *ir.Table) ([]string, error) {
	if t.PrimaryKey == nil || len(t.PrimaryKey.Columns) == 0 {
		return nil, errors.New("table has no primary key (sample-mode requires a PK for deterministic sampling)")
	}
	cols := make([]string, len(t.PrimaryKey.Columns))
	for i, c := range t.PrimaryKey.Columns {
		cols[i] = c.Column
	}
	return cols, nil
}

// quotedPKList wraps each name in MySQL backtick-identifier quotes.
func quotedPKList(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quoteIdent(n)
	}
	return out
}
