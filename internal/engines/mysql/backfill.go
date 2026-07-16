// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// In-place backfill executor for `sluice backfill` (ADR-0159).
//
// The SQL shapes mirror row_reader_batch.go's cursor-paginated reads —
// the same row-comparison predicates on the PK tuple, compared by the
// server in the column's native collation (the ADR-0096 exactly-once
// contract) — but drive UPDATEs instead of SELECTs:
//
//	-- chunk-boundary walk (index-only over the PK):
//	SELECT pk1, pk2 FROM `tbl`
//	WHERE (pk1, pk2) > (?, ?)
//	ORDER BY pk1, pk2 LIMIT 1 OFFSET N-1
//
//	-- one bounded chunk:
//	UPDATE `tbl` SET `new_col` = <expr verbatim>
//	WHERE (pk1, pk2) > (?, ?) AND (pk1, pk2) <= (?, ?) AND (<where verbatim>)
//
// MySQL natively supports row-constructor comparison in UPDATE WHERE
// clauses; per-column boolean logic is incorrect for composite-PK
// descent and must not be used (same rule as the batched reader).
//
// The MySQL path covers the PlanetScale and Vitess flavors: vtgate
// accepts plain bounded UPDATEs, and the per-chunk bound is exactly
// what keeps each statement under the errno-3024 synchronous-
// transaction-time wall (ADR-0148) a single whole-table UPDATE hits.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Static assertions: the executor satisfies the IR surface and the
// engine exposes the opener.
var (
	_ ir.BackfillExecutor       = (*BackfillExecutor)(nil)
	_ ir.BackfillExecutorOpener = Engine{}
)

// BackfillExecutor is the MySQL implementation of
// [ir.BackfillExecutor]. One value per backfill run; owns its
// connection pool.
type BackfillExecutor struct {
	db *sql.DB
}

// OpenBackfillExecutor returns a [BackfillExecutor] bound to the
// database identified by dsn. Implements [ir.BackfillExecutorOpener];
// the backfill orchestrator type-asserts on this method so engines
// without an in-place UPDATE surface can omit it.
func (e Engine) OpenBackfillExecutor(ctx context.Context, dsn string) (ir.BackfillExecutor, error) {
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	return &BackfillExecutor{db: db}, nil
}

// Close releases the underlying connection pool.
func (x *BackfillExecutor) Close() error {
	if x.db == nil {
		return nil
	}
	return x.db.Close()
}

// NextChunkUpperBound implements [ir.BackfillExecutor]: the PK tuple of
// the last row in the next batch of up to limit rows after `after`.
// Two index-only queries at most: the exact limit-th row first, and —
// only when fewer than limit rows remain — the last remaining row via
// a descending scan.
func (x *BackfillExecutor) NextChunkUpperBound(ctx context.Context, table *ir.Table, after []any, limit int) (upper []any, ok bool, err error) {
	if err := validateBackfillArgs(table, after, limit); err != nil {
		return nil, false, err
	}

	// The limit-th row past the cursor — the full batch's upper bound.
	upper, err = x.scanPKRow(ctx, buildBackfillBoundarySelect(table, len(after) > 0, false, limit), after)
	switch {
	case err == nil:
		return upper, true, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, false, fmt.Errorf("mysql: NextChunkUpperBound: %w", err)
	}

	// Fewer than limit rows remain: the LAST remaining row (descending
	// scan, LIMIT 1) is the final chunk's upper bound.
	upper, err = x.scanPKRow(ctx, buildBackfillBoundarySelect(table, len(after) > 0, true, limit), after)
	switch {
	case err == nil:
		return upper, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("mysql: NextChunkUpperBound: %w", err)
	}
}

// scanPKRow runs a single-row PK-tuple query and returns the
// normalized tuple, or sql.ErrNoRows.
func (x *BackfillExecutor) scanPKRow(ctx context.Context, query string, args []any) ([]any, error) {
	rows, err := x.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	for i, v := range vals {
		vals[i] = normalizeBackfillCursorValue(v)
	}
	return vals, rows.Err()
}

// normalizeBackfillCursorValue converts a raw driver-scanned PK value
// into a form that (a) re-binds into the row-comparison predicates and
// (b) survives the resume store's JSON round-trip. []byte passes
// through RAW: the store's cursor envelope (ir.TableProgress,
// {"_t":"bytes"}) round-trips it byte-exact, and the driver binds
// []byte and string identically in the predicates. (The pre-envelope
// string(t) conversion here fed invalid-UTF-8 PK bytes to
// encoding/json, which replaced them with U+FFFD — a silently
// misplaced cursor on BINARY/VARBINARY/BLOB keys; audit 2026-07-15
// CRITICAL-2.) time.Time would JSON-envelope fine, but MySQL does not
// reliably parse RFC 3339's `T`/`Z` in comparisons, so it becomes
// MySQL's native literal form up front.
func normalizeBackfillCursorValue(v any) any {
	if t, ok := v.(time.Time); ok {
		return t.Format("2006-01-02 15:04:05.999999")
	}
	return v
}

// ExecBackfillChunk implements [ir.BackfillExecutor]: one bounded
// UPDATE over (after, upper]. Returns MySQL's affected-row count,
// which counts CHANGED rows — a re-applied chunk whose rows already
// hold the target values reports 0, which is exactly the idempotent-
// re-run signal the orchestrator surfaces.
func (x *BackfillExecutor) ExecBackfillChunk(ctx context.Context, table *ir.Table, sets []ir.BackfillSet, where string, after, upper []any) (int64, error) {
	if err := validateBackfillArgs(table, after, 1); err != nil {
		return 0, err
	}
	if len(sets) == 0 {
		return 0, errors.New("mysql: ExecBackfillChunk: no SET clauses")
	}
	if len(upper) != len(table.PrimaryKey.Columns) {
		return 0, fmt.Errorf("mysql: ExecBackfillChunk: upper has %d values, table %q has %d PK columns",
			len(upper), table.Name, len(table.PrimaryKey.Columns))
	}
	query := buildBackfillUpdate(table, sets, where, len(after) > 0)
	args := make([]any, 0, len(after)+len(upper))
	args = append(args, after...)
	args = append(args, upper...)
	res, err := x.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("mysql: backfill chunk update failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("mysql: backfill chunk rows-affected: %w", err)
	}
	return n, nil
}

// BackfillStatement implements [ir.BackfillExecutor]: the mid-walk
// chunk UPDATE (both bounds present) for `--dry-run` preview.
func (x *BackfillExecutor) BackfillStatement(table *ir.Table, sets []ir.BackfillSet, where string) (string, error) {
	if err := validateBackfillArgs(table, nil, 1); err != nil {
		return "", err
	}
	if len(sets) == 0 {
		return "", errors.New("mysql: BackfillStatement: no SET clauses")
	}
	return buildBackfillUpdate(table, sets, where, true), nil
}

// CountRemaining implements [ir.BackfillExecutor].
func (x *BackfillExecutor) CountRemaining(ctx context.Context, table *ir.Table, where string) (int64, error) {
	if table == nil {
		return 0, errors.New("mysql: CountRemaining: table is nil")
	}
	query := "SELECT COUNT(*) FROM " + quoteIdent(table.Name)
	if where != "" {
		query += " WHERE (" + where + ")"
	}
	var n int64
	if err := x.db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysql: backfill count remaining: %w", err)
	}
	return n, nil
}

// validateBackfillArgs is the shared defensive gate: the orchestrator
// refuses no-PK tables upstream (coded), but a malformed call must not
// reach SQL generation.
func validateBackfillArgs(table *ir.Table, after []any, limit int) error {
	if table == nil {
		return errors.New("mysql: backfill: table is nil")
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return fmt.Errorf("mysql: backfill: table %q has no primary key; cannot use a keyset-chunked backfill", table.Name)
	}
	if limit <= 0 {
		return fmt.Errorf("mysql: backfill: limit must be > 0, got %d", limit)
	}
	if len(after) != 0 && len(after) != len(table.PrimaryKey.Columns) {
		return fmt.Errorf("mysql: backfill: after has %d values, table %q has %d PK columns",
			len(after), table.Name, len(table.PrimaryKey.Columns))
	}
	return nil
}

// backfillPKParts returns the quoted PK column list, the joined tuple,
// and one tuple's worth of `?` placeholders.
func backfillPKParts(table *ir.Table) (pkTuple, placeholders string) {
	pkCols := table.PrimaryKey.Columns
	pkList := make([]string, len(pkCols))
	for i, c := range pkCols {
		pkList[i] = quoteIdent(c.Column)
	}
	return strings.Join(pkList, ", "),
		strings.TrimSuffix(strings.Repeat("?, ", len(pkCols)), ", ")
}

// buildBackfillBoundarySelect returns the chunk-boundary query: the
// limit-th PK tuple past the cursor (desc=false, LIMIT 1 OFFSET
// limit-1), or the LAST remaining tuple (desc=true, descending LIMIT 1
// — the final-partial-chunk fallback). Index-only over the PK; LIMIT/
// OFFSET are embedded as literals for the same reason the batched
// reader embeds LIMIT (orchestrator-controlled ints, no user input).
func buildBackfillBoundarySelect(table *ir.Table, hasCursor, desc bool, limit int) string {
	pkTuple, placeholders := backfillPKParts(table)
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(pkTuple)
	sb.WriteString(" FROM ")
	sb.WriteString(quoteIdent(table.Name))
	if hasCursor {
		sb.WriteString(" WHERE (" + pkTuple + ") > (" + placeholders + ")")
	}
	sb.WriteString(" ORDER BY ")
	if desc {
		pkCols := table.PrimaryKey.Columns
		ordered := make([]string, len(pkCols))
		for i, c := range pkCols {
			ordered[i] = quoteIdent(c.Column) + " DESC"
		}
		sb.WriteString(strings.Join(ordered, ", "))
		sb.WriteString(" LIMIT 1")
	} else {
		sb.WriteString(pkTuple)
		fmt.Fprintf(&sb, " LIMIT 1 OFFSET %d", limit-1)
	}
	return sb.String()
}

// buildBackfillUpdate returns the bounded chunk UPDATE. hasLower=false
// is the first chunk (no lower-bound predicate). Placeholders bind in
// clause order: the lower tuple first, then the upper — the caller
// appends after... then upper... to match. The SET expressions and the
// where predicate are emitted VERBATIM (operator-supplied native SQL,
// the --expr-override posture); only column identifiers are quoted.
func buildBackfillUpdate(table *ir.Table, sets []ir.BackfillSet, where string, hasLower bool) string {
	pkTuple, placeholders := backfillPKParts(table)
	setList := make([]string, len(sets))
	for i, s := range sets {
		setList[i] = quoteIdent(s.Column) + " = " + s.Expr
	}
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(quoteIdent(table.Name))
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(setList, ", "))
	sb.WriteString(" WHERE ")
	if hasLower {
		sb.WriteString("(" + pkTuple + ") > (" + placeholders + ") AND ")
	}
	sb.WriteString("(" + pkTuple + ") <= (" + placeholders + ")")
	if where != "" {
		sb.WriteString(" AND (" + where + ")")
	}
	return sb.String()
}
