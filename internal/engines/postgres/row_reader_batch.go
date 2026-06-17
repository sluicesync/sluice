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
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Static assertion: the PG reader satisfies the bounded batched surface
// (which embeds [ir.BatchedRowReader]).
var _ ir.BoundedBatchedRowReader = (*RowReader)(nil)

// ReadRowsBatch implements [ir.BatchedRowReader]. See the file-header
// comment for the SQL shape and the design rationale.
//
// Returns a non-nil error for tables without a primary key — the
// orchestrator's classifier rejects no-PK tables before reaching this
// method, but a defensive check is cheaper than a malformed SQL
// statement at runtime.
func (r *RowReader) ReadRowsBatch(ctx context.Context, table *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	return r.readRowsBatch(ctx, table, after, nil, limit, "ReadRowsBatch")
}

// ReadRowsBatchBounded implements [ir.BoundedBatchedRowReader]: the same
// PK-cursor page as [ReadRowsBatch] but additionally clipped to an
// INCLUSIVE upper PK (`(pk) <= upTo`). Pushing the chunk's upper bound
// into the SQL WHERE — in the SAME collation the ORDER BY uses — is what
// makes within-table chunk coverage exactly-once for string / varchar /
// decimal PKs under a non-C collation (ADR-0096; see the interface doc).
func (r *RowReader) ReadRowsBatchBounded(ctx context.Context, table *ir.Table, after, upTo []any, limit int) (<-chan ir.Row, error) {
	return r.readRowsBatch(ctx, table, after, upTo, limit, "ReadRowsBatchBounded")
}

// readRowsBatch is the shared body for the lower-bound-only and
// lower+upper-bounded batched reads. upTo == nil means "no upper bound"
// (the last chunk / whole-table resume), reproducing the original
// ReadRowsBatch query exactly.
func (r *RowReader) readRowsBatch(ctx context.Context, table *ir.Table, after, upTo []any, limit int, op string) (<-chan ir.Row, error) {
	if table == nil {
		return nil, fmt.Errorf("postgres: %s: table is nil", op)
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("postgres: %s: table %q has no columns", op, table.Name)
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return nil, fmt.Errorf("postgres: %s: table %q has no primary key; cannot use cursor-paginated reads", op, table.Name)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("postgres: %s: limit must be > 0, got %d", op, limit)
	}
	pkCols := table.PrimaryKey.Columns
	if len(after) != 0 && len(after) != len(pkCols) {
		return nil, fmt.Errorf("postgres: %s: after has %d values, table %q has %d PK columns",
			op, len(after), table.Name, len(pkCols))
	}
	if len(upTo) != 0 && len(upTo) != len(pkCols) {
		return nil, fmt.Errorf("postgres: %s: upTo has %d values, table %q has %d PK columns",
			op, len(upTo), table.Name, len(pkCols))
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	query := buildBatchedSelect(r.schema, table, limit, len(after) > 0, len(upTo) > 0)
	// Args are bound in clause order: lower-bound placeholders ($1..) then
	// upper-bound placeholders, matching buildBatchedSelect's numbering.
	args := make([]any, 0, len(after)+len(upTo))
	args = append(args, after...)
	args = append(args, upTo...)

	// rowserrcheck and sqlclosecheck can't follow rows into the
	// goroutine; both are handled inside stream() (Close via defer,
	// Err checked once iteration ends).
	rows, err := r.q.QueryContext(ctx, query, args...) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: query failed: %w", op, err)
	}

	out := make(chan ir.Row, rowChanBuffer)
	go r.stream(ctx, rows, table, out)
	return out, nil
}

// buildBatchedSelect returns the cursor-paginated SELECT statement
// for table. When hasCursor is true a row-comparison LOWER bound
// (`(pk) > ($1...)`) is included; when hasUpper is true an INCLUSIVE
// row-comparison UPPER bound (`(pk) <= (...)`) is included. With neither
// the SELECT is the unbounded "first-batch" form (ORDER BY pk LIMIT N).
//
// CRITICAL (ADR-0096): both bounds are row-comparison predicates on the
// PK tuple, so PG compares them in the column's NATIVE collation — the
// SAME order the ORDER BY uses. This is what makes chunk coverage
// exactly-once for non-C-collation string/decimal PKs; clipping the
// upper bound in Go with a byte comparator instead would diverge from
// the ORDER BY and silently drop boundary-straddling rows.
//
// Generated columns are excluded from the SELECT list — same
// invariant as [buildSelect].
//
// LIMIT is embedded as a literal rather than a parameter because PG
// doesn't accept a parameter in LIMIT for some plan-cache shapes,
// and the value is orchestrator-controlled (not user input) so the
// SQL-injection surface is non-existent.
func buildBatchedSelect(schema string, table *ir.Table, limit int, hasCursor, hasUpper bool) string {
	src := sourceReadableColumns(table.Columns)
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

	// Placeholders are numbered across both bounds in clause order: the
	// lower-bound tuple gets $1..$len(pk), then the upper-bound tuple
	// continues the numbering. readRowsBatch binds after... then upTo...
	// in exactly that order.
	ph := 0
	nextPlaceholders := func() string {
		out := make([]string, len(pkCols))
		for i := range out {
			ph++
			out[i] = fmt.Sprintf("$%d", ph)
		}
		return strings.Join(out, ", ")
	}

	var conds []string
	if hasCursor {
		conds = append(conds, "("+pkTuple+") > ("+nextPlaceholders()+")")
	}
	if hasUpper {
		conds = append(conds, "("+pkTuple+") <= ("+nextPlaceholders()+")")
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
