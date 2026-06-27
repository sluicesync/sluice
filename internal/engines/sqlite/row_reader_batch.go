// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PK-cursor batched reads for the parallel within-table chunk-copy path
// (ADR-0128 within-table chunking; #3 of the SQLite queue).
//
// The bulk-copy orchestrator drives ReadRowsBatch with the previous
// batch's last-applied PK (the "after" cursor) and a row-count limit;
// for within-table chunks it drives ReadRowsBatchBounded, which clips the
// page to the chunk's INCLUSIVE upper PK. The query shape mirrors the
// Postgres / MySQL readers — a row-VALUE comparison predicate, which
// SQLite supports natively since 3.15 (2016) and modernc.org/sqlite
// (current) implements:
//
//	SELECT <cols> FROM "t"
//	WHERE ("t"."pk1", "t"."pk2") > (?, ?)        -- lower bound (cursor)
//	  AND ("t"."pk1", "t"."pk2") <= (?, ?)       -- upper bound (chunk clip)
//	ORDER BY "t"."pk1", "t"."pk2"
//	LIMIT N
//
// Per-column boolean logic (pk1 > ? OR (pk1 = ? AND pk2 > ?)) is
// result-equivalent but error-prone and is NOT used; the row-value form
// is canonical for composite-PK descent.
//
// CRITICAL exactly-once contract (the Bug-74 silent-row-loss class): both
// the lower/upper clip AND the ORDER BY must use the SAME total order, and
// that order is the PK column's INTRINSIC collation (BINARY by default,
// NOCASE / a custom collation when the column declares one). SQLite picks
// the collating sequence for a comparison `col <op> ?` from the LEFT
// operand's column, and ORDER BY on a column uses that same column
// collation — so as long as we (a) put the column on the left of every
// bound, (b) reference the REAL column (table-qualified, never a SELECT
// alias — see below), and (c) inject NO explicit COLLATE, all three —
// sampler boundary order, lower bound, upper bound, ORDER BY — agree by
// construction. A boundary-straddling row can then land in exactly one
// half-open (lower, upper] chunk, never zero and never two.

package sqlite

import (
	"context"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Static assertion: the SQLite reader satisfies the bounded batched
// surface (which embeds [ir.BatchedRowReader]).
var _ ir.BoundedBatchedRowReader = (*RowReader)(nil)

// ReadRowsBatch implements [ir.BatchedRowReader]. See the file-header
// comment for the SQL shape and the design rationale.
//
// Returns a non-nil error for tables without a primary key — the
// orchestrator's classifier rejects no-PK tables before reaching this
// method, but the defensive check is cheaper than a malformed SQL
// statement at runtime, and the orchestrator treats the error as a
// signal to fall back to the single-reader path.
func (r *RowReader) ReadRowsBatch(ctx context.Context, table *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	return r.readRowsBatch(ctx, table, after, nil, limit, "ReadRowsBatch")
}

// ReadRowsBatchBounded implements [ir.BoundedBatchedRowReader]: the same
// PK-cursor page as [ReadRowsBatch] but additionally clipped to an
// INCLUSIVE upper PK (`(pk) <= upTo`). Pushing the chunk's upper bound
// into the SQL WHERE — in the SAME collation the ORDER BY uses — is what
// makes within-table chunk coverage exactly-once for a NOCASE / custom-
// collation TEXT PK or a BLOB PK (ADR-0128; see the interface doc and the
// file-header CRITICAL note). nil upTo means "no upper bound" (the last
// chunk), reproducing [ReadRowsBatch] exactly.
func (r *RowReader) ReadRowsBatchBounded(ctx context.Context, table *ir.Table, after, upTo []any, limit int) (<-chan ir.Row, error) {
	return r.readRowsBatch(ctx, table, after, upTo, limit, "ReadRowsBatchBounded")
}

// readRowsBatch is the shared body for the lower-bound-only and
// lower+upper-bounded batched reads. upTo == nil reproduces the original
// ReadRowsBatch query exactly (no upper-bound predicate).
func (r *RowReader) readRowsBatch(ctx context.Context, table *ir.Table, after, upTo []any, limit int, op string) (<-chan ir.Row, error) {
	if table == nil {
		return nil, fmt.Errorf("sqlite: %s: table is nil", op)
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("sqlite: %s: table %q has no columns", op, table.Name)
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return nil, fmt.Errorf("sqlite: %s: table %q has no primary key; cannot use cursor-paginated reads", op, table.Name)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("sqlite: %s: limit must be > 0, got %d", op, limit)
	}
	pkCols := table.PrimaryKey.Columns
	if len(after) != 0 && len(after) != len(pkCols) {
		return nil, fmt.Errorf("sqlite: %s: after has %d values, table %q has %d PK columns",
			op, len(after), table.Name, len(pkCols))
	}
	if len(upTo) != 0 && len(upTo) != len(pkCols) {
		return nil, fmt.Errorf("sqlite: %s: upTo has %d values, table %q has %d PK columns",
			op, len(upTo), table.Name, len(pkCols))
	}
	// Loud refusal for a PK whose decoded cursor value can't round-trip the
	// raw storage class (temporal / decimal — the Bug-74 silent-loss class;
	// see pkCursorDisqualified). The orchestrator already routes such tables
	// to the single-reader path (the chunk surfaces return 0;
	// canResumePerBatch consults DisqualifiesBatchedRead), so this is the
	// defensive backstop: a direct caller fails LOUDLY here rather than
	// driving a cursor that silently truncates or dups a page.
	if disq, reason := pkCursorDisqualified(table); disq {
		return nil, fmt.Errorf("sqlite: %s: table %q: %s", op, table.Name, reason)
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	hasRowid := r.tableHasRowid(ctx, table.Name)
	// Generated columns are NOT selected — the target re-derives them from
	// their GENERATED clause (ADR-0133); kept in lockstep with [buildSelect].
	cols := nonGeneratedColumns(table.Columns)
	query := buildBatchedSelect(cols, table, hasRowid, limit, len(after) > 0, len(upTo) > 0)
	// Bind in clause order: lower-bound placeholders first, then upper.
	args := make([]any, 0, len(after)+len(upTo))
	args = append(args, after...)
	args = append(args, upTo...)

	// rowserrcheck/sqlclosecheck can't follow rows into the streaming
	// goroutine; both are handled inside stream() (Close via defer, Err
	// checked once iteration ends) — same contract as [ReadRows].
	rows, err := r.db.QueryContext(ctx, query, args...) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("sqlite: %s: query %q failed: %w", op, table.Name, err)
	}

	out := make(chan ir.Row, rowChanBuffer)
	go r.stream(ctx, rows, table.Name, cols, hasRowid, out)
	return out, nil
}

// buildBatchedSelect returns the cursor-paginated SELECT for table.
// hasCursor=true emits the row-value LOWER bound (`(pk) > (?...)`);
// hasUpper=true emits the INCLUSIVE row-value UPPER bound (`(pk) <=
// (?...)`); neither emits the unbounded first-batch form (no WHERE). cols
// is the projected (non-generated) column set; hasRowid prefixes `rowid`
// so the streamer can name the rowid in a fidelity-refusal message (it is
// not part of the cursor — same projection [buildSelect] uses).
//
// CRITICAL (ADR-0128 exactly-once, see the file-header note): the PK refs
// in WHERE and ORDER BY are TABLE-QUALIFIED (`"t"."pk"`) on purpose. A
// temporal PK column is projected by [selectColumnExpr] as
// `coalesce("c","c") AS "c"` (the modernc-temporal-parse wart), which adds
// a SELECT-list alias with the same bare name. An UNQUALIFIED `ORDER BY
// "c"` could bind to that coalesce-expression alias — whose collation is
// the expression default (BINARY), NOT the column's declared collation —
// while the cursor predicate `("c") > (?)` (WHERE never sees aliases) uses
// the column collation. For a NOCASE PK the two would DIVERGE and a
// boundary-straddling row could fall into no chunk (silent loss). Table-
// qualifying both clauses binds them to the REAL column so ordering and
// both bounds share the column's intrinsic collation. No explicit COLLATE
// is ever injected.
//
// LIMIT is embedded as a literal because it is an orchestrator-controlled
// int (no user input), matching the PG/MySQL readers.
func buildBatchedSelect(cols []*ir.Column, table *ir.Table, hasRowid bool, limit int, hasCursor, hasUpper bool) string {
	parts := make([]string, 0, len(cols)+1)
	if hasRowid {
		parts = append(parts, "rowid")
	}
	for _, c := range cols {
		parts = append(parts, selectColumnExpr(c))
	}

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
	sb.WriteString(strings.Join(parts, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(tbl)

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
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	sb.WriteString(" ORDER BY ")
	sb.WriteString(pkTuple)
	fmt.Fprintf(&sb, " LIMIT %d", limit)
	return sb.String()
}
