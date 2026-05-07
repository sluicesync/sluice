// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PK-cursor batched reads for the resume-mid-table path.
//
// The bulk-copy orchestrator drives ReadRowsBatch with the previous
// batch's last-applied PK (the "after" cursor) and a row-count limit.
// The query shape is a row-comparison predicate, which both PG and
// MySQL natively support and which is the correct form for descending
// into composite-PK ordering:
//
//	SELECT cols FROM "schema"."tbl"
//	WHERE (pk1, pk2) > ($1, $2)
//	ORDER BY pk1, pk2
//	LIMIT N
//
// Per-column boolean logic (pk1 > $1 OR (pk1 = $1 AND pk2 > $2)) is
// equivalent in result set but is more error-prone, harder to read,
// and forces a different optimizer path on some PG versions. The
// row-comparison form is canonical and matches PG's documented
// row-constructor comparison semantics.
//
// On the first batch, the cursor is nil; the predicate is omitted
// entirely and the SELECT is `ORDER BY pk1, pk2 LIMIT N`. See
// ADR-0018 for the broader context.

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// ReadRowsBatch implements [ir.BatchedRowReader]. See the file-header
// comment for the SQL shape and the design rationale.
//
// Returns a non-nil error for tables without a primary key — the
// orchestrator's classifier rejects no-PK tables before reaching this
// method, but a defensive check is cheaper than a malformed SQL
// statement at runtime.
func (r *RowReader) ReadRowsBatch(ctx context.Context, table *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("postgres: ReadRowsBatch: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("postgres: ReadRowsBatch: table %q has no columns", table.Name)
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return nil, fmt.Errorf("postgres: ReadRowsBatch: table %q has no primary key; cannot use cursor-paginated reads", table.Name)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("postgres: ReadRowsBatch: limit must be > 0, got %d", limit)
	}
	pkCols := table.PrimaryKey.Columns
	if len(after) != 0 && len(after) != len(pkCols) {
		return nil, fmt.Errorf("postgres: ReadRowsBatch: after has %d values, table %q has %d PK columns",
			len(after), table.Name, len(pkCols))
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	query := buildBatchedSelect(r.schema, table, limit, len(after) > 0)
	args := append([]any{}, after...)

	// rowserrcheck and sqlclosecheck can't follow rows into the
	// goroutine; both are handled inside stream() (Close via defer,
	// Err checked once iteration ends).
	rows, err := r.q.QueryContext(ctx, query, args...) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("postgres: ReadRowsBatch: query failed: %w", err)
	}

	out := make(chan ir.Row)
	go r.stream(ctx, rows, table, out)
	return out, nil
}

// buildBatchedSelect returns the cursor-paginated SELECT statement
// for table. When hasCursor is true the WHERE row-comparison predicate
// is included; when false the SELECT is the unbounded
// "first-batch" form (ORDER BY pk LIMIT N).
//
// Generated columns are excluded from the SELECT list — same
// invariant as [buildSelect].
//
// LIMIT is embedded as a literal rather than a parameter because PG
// doesn't accept a parameter in LIMIT for some plan-cache shapes,
// and the value is orchestrator-controlled (not user input) so the
// SQL-injection surface is non-existent.
func buildBatchedSelect(schema string, table *ir.Table, limit int, hasCursor bool) string {
	src := nonGeneratedColumns(table.Columns)
	colsList := make([]string, len(src))
	for i, c := range src {
		colsList[i] = quoteIdent(c.Name)
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)

	pkCols := table.PrimaryKey.Columns
	pkList := make([]string, len(pkCols))
	for i, c := range pkCols {
		pkList[i] = quoteIdent(c.Column)
	}
	pkTuple := strings.Join(pkList, ", ")

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(colsList, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(tableRef)
	if hasCursor {
		// Row-comparison predicate. Numbered placeholders match the
		// number of PK columns; the orchestrator passes them in PK
		// declaration order via the after slice.
		placeholders := make([]string, len(pkCols))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		sb.WriteString(" WHERE (")
		sb.WriteString(pkTuple)
		sb.WriteString(") > (")
		sb.WriteString(strings.Join(placeholders, ", "))
		sb.WriteString(")")
	}
	sb.WriteString(" ORDER BY ")
	sb.WriteString(pkTuple)
	fmt.Fprintf(&sb, " LIMIT %d", limit)
	return sb.String()
}
