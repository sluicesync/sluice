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
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Static assertion: the MySQL reader satisfies the bounded batched
// surface (which embeds [ir.BatchedRowReader]).
var _ ir.BoundedBatchedRowReader = (*RowReader)(nil)

// ReadRowsBatch implements [ir.BatchedRowReader]. See the file-header
// comment for the SQL shape and the design rationale.
//
// Returns a non-nil error for tables without a primary key — the
// orchestrator's classifier rejects no-PK tables before reaching this
// method, but the defensive check is cheaper than a malformed SQL
// statement at runtime.
func (r *RowReader) ReadRowsBatch(ctx context.Context, table *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	return r.readRowsBatch(ctx, table, after, nil, limit, "ReadRowsBatch")
}

// ReadRowsBatchBounded implements [ir.BoundedBatchedRowReader]: the same
// PK-cursor page as [ReadRowsBatch] but additionally clipped to an
// INCLUSIVE upper PK (`(pk) <= upTo`). Pushing the chunk's upper bound
// into the SQL WHERE — in the SAME collation the ORDER BY uses — is what
// makes within-table chunk coverage exactly-once for string / varchar /
// decimal PKs under a case-/accent-insensitive collation like
// utf8mb4_0900_ai_ci (ADR-0096; see the interface doc).
func (r *RowReader) ReadRowsBatchBounded(ctx context.Context, table *ir.Table, after, upTo []any, limit int) (<-chan ir.Row, error) {
	return r.readRowsBatch(ctx, table, after, upTo, limit, "ReadRowsBatchBounded")
}

// readRowsBatch is the shared body for the lower-bound-only and
// lower+upper-bounded batched reads. upTo == nil reproduces the original
// ReadRowsBatch query exactly (no upper-bound predicate).
func (r *RowReader) readRowsBatch(ctx context.Context, table *ir.Table, after, upTo []any, limit int, op string) (<-chan ir.Row, error) {
	if table == nil {
		return nil, fmt.Errorf("mysql: %s: table is nil", op)
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("mysql: %s: table %q has no columns", op, table.Name)
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return nil, fmt.Errorf("mysql: %s: table %q has no primary key; cannot use cursor-paginated reads", op, table.Name)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("mysql: %s: limit must be > 0, got %d", op, limit)
	}
	pkCols := table.PrimaryKey.Columns
	if len(after) != 0 && len(after) != len(pkCols) {
		return nil, fmt.Errorf("mysql: %s: after has %d values, table %q has %d PK columns",
			op, len(after), table.Name, len(pkCols))
	}
	if len(upTo) != 0 && len(upTo) != len(pkCols) {
		return nil, fmt.Errorf("mysql: %s: upTo has %d values, table %q has %d PK columns",
			op, len(upTo), table.Name, len(pkCols))
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	query := buildBatchedSelect(table, limit, len(after) > 0, len(upTo) > 0, r.rowFilters[table.Name])
	// Bind in clause order: lower-bound placeholders first, then upper.
	args := make([]any, 0, len(after)+len(upTo))
	args = append(args, after...)
	args = append(args, upTo...)

	// rowserrcheck and sqlclosecheck can't follow rows into the
	// goroutine; both are handled inside stream() (Close via defer,
	// Err checked once iteration ends).
	rows, err := r.q.QueryContext(ctx, query, args...) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("mysql: %s: query failed: %w", op, err)
	}

	out := make(chan ir.Row, rowChanBuffer)
	go r.stream(ctx, rows, table, out, nil)
	return out, nil
}

// buildBatchedSelect returns the cursor-paginated SELECT for table.
// hasCursor=true emits the row-comparison LOWER bound (`(pk) > (?...)`);
// hasUpper=true emits the INCLUSIVE row-comparison UPPER bound
// (`(pk) <= (?...)`); neither emits the first-batch form (no WHERE).
//
// CRITICAL (ADR-0096): both bounds are row-comparison predicates on the
// PK tuple, so MySQL compares them in the column's collation — the SAME
// order the ORDER BY uses. This is what makes chunk coverage exactly-once
// for a case-/accent-insensitive collation like utf8mb4_0900_ai_ci;
// clipping the upper bound in Go with a byte comparator would diverge
// from the ORDER BY and silently drop boundary-straddling rows.
//
// Generated columns are excluded from the SELECT list — same
// invariant as [buildSelect].
//
// LIMIT is embedded as a literal because it's an orchestrator-
// controlled int (no user input) and parameterising LIMIT in MySQL
// has historical compatibility quirks across versions.
//
// predicate is the operator's `--where` row filter for this table
// (ADR-0173 Phase 1), or "" for none. When present it is added as one
// more parenthesized conjunct in the WHERE — ALWAYS parenthesized so a
// disjunctive predicate (`a OR b`) can't escape the keyset chunk bounds
// it is ANDed with — before the ORDER BY / LIMIT, so a filtered chunked
// read stays exactly-once.
func buildBatchedSelect(table *ir.Table, limit int, hasCursor, hasUpper bool, predicate string) string {
	src := sourceReadableColumns(table.Columns)
	colsList := make([]string, len(src))
	for i, c := range src {
		colsList[i] = selectColumnExpr(c)
	}

	// PK refs in WHERE/ORDER BY are TABLE-QUALIFIED on purpose. A
	// temporal PK column is projected as `CAST(`c` AS CHAR) AS `c`` by
	// selectColumnExpr (the Vector A zero-date fix), which introduces a
	// SELECT-list alias with the same bare name. An unqualified
	// `ORDER BY `c`` binds to that CHAR alias — a STRING sort — while
	// the cursor predicate `(`c`) > (?)` (WHERE can't see aliases)
	// compares the DATE-typed column against a time.Time cursor value.
	// Lexical ISO order happens to match date order, so pagination was
	// correct-but-fragile; the alias sort also defeated the PRIMARY
	// index (forced filesort). Qualifying to `tbl`.`c`` binds both
	// clauses to the real column: date-typed throughout, index-ordered.
	tbl := quoteIdent(table.Name)
	pkCols := table.PrimaryKey.Columns
	pkList := make([]string, len(pkCols))
	for i, c := range pkCols {
		pkList[i] = tbl + "." + quoteIdent(c.Column)
	}
	pkTuple := strings.Join(pkList, ", ")

	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(pkCols)), ", ")

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(colsList, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(quoteIdent(table.Name))

	// Placeholders bind in clause order: the lower-bound tuple first, then
	// the upper-bound tuple. readRowsBatch binds after... then upTo... to
	// match.
	var conds []string
	if hasCursor {
		conds = append(conds, "("+pkTuple+") > ("+placeholders+")")
	}
	if hasUpper {
		conds = append(conds, "("+pkTuple+") <= ("+placeholders+")")
	}
	if predicate != "" {
		conds = append(conds, "("+predicate+")")
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	sb.WriteString(" ORDER BY ")
	sb.WriteString(pkTuple)
	fmt.Fprintf(&sb, " LIMIT %d", limit)
	return sb.String()
}
