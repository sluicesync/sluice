// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PK-cursor batched reads for the resume-mid-table path.
//
// See the postgres engine's row_reader_batch.go for the design
// rationale — same SQL shape, different identifier-quoting and
// placeholder syntax. MySQL backticks identifiers and uses `?`
// placeholders; the row-comparison predicate is identical:
//
//	SELECT cols FROM `tbl`
//	WHERE (pk1, pk2) > (?, ?)
//	ORDER BY pk1, pk2
//	LIMIT N
//
// MySQL natively supports row-constructor comparison (since 4.1).
// Per-column boolean logic is incorrect for composite-PK descent and
// must not be used.

package mysql

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
// method, but the defensive check is cheaper than a malformed SQL
// statement at runtime.
func (r *RowReader) ReadRowsBatch(ctx context.Context, table *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mysql: ReadRowsBatch: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("mysql: ReadRowsBatch: table %q has no columns", table.Name)
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return nil, fmt.Errorf("mysql: ReadRowsBatch: table %q has no primary key; cannot use cursor-paginated reads", table.Name)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("mysql: ReadRowsBatch: limit must be > 0, got %d", limit)
	}
	pkCols := table.PrimaryKey.Columns
	if len(after) != 0 && len(after) != len(pkCols) {
		return nil, fmt.Errorf("mysql: ReadRowsBatch: after has %d values, table %q has %d PK columns",
			len(after), table.Name, len(pkCols))
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	query := buildBatchedSelect(table, limit, len(after) > 0)
	args := append([]any{}, after...)

	// rowserrcheck and sqlclosecheck can't follow rows into the
	// goroutine; both are handled inside stream() (Close via defer,
	// Err checked once iteration ends).
	rows, err := r.q.QueryContext(ctx, query, args...) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("mysql: ReadRowsBatch: query failed: %w", err)
	}

	out := make(chan ir.Row)
	go r.stream(ctx, rows, table, out)
	return out, nil
}

// buildBatchedSelect returns the cursor-paginated SELECT for table.
// hasCursor=true emits the row-comparison WHERE predicate;
// hasCursor=false emits the first-batch form (no WHERE).
//
// Generated columns are excluded from the SELECT list — same
// invariant as [buildSelect].
//
// LIMIT is embedded as a literal because it's an orchestrator-
// controlled int (no user input) and parameterising LIMIT in MySQL
// has historical compatibility quirks across versions.
func buildBatchedSelect(table *ir.Table, limit int, hasCursor bool) string {
	src := sourceReadableColumns(table.Columns)
	colsList := make([]string, len(src))
	for i, c := range src {
		colsList[i] = quoteIdent(c.Name)
	}

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
	sb.WriteString(quoteIdent(table.Name))
	if hasCursor {
		placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(pkCols)), ", ")
		sb.WriteString(" WHERE (")
		sb.WriteString(pkTuple)
		sb.WriteString(") > (")
		sb.WriteString(placeholders)
		sb.WriteString(")")
	}
	sb.WriteString(" ORDER BY ")
	sb.WriteString(pkTuple)
	fmt.Fprintf(&sb, " LIMIT %d", limit)
	return sb.String()
}
