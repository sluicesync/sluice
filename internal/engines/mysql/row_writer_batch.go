// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Idempotent INSERT path for the resume-mid-table bulk-copy.
//
// See the postgres engine's row_writer_batch.go for the design
// rationale — same shape, different syntax. MySQL uses the row-alias
// UPSERT form (8.0.20+):
//
//	INSERT INTO `tbl` (`a`, `b`, `id`) VALUES (?, ?, ?), ... AS new
//	ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`
//
// The row alias (`AS new`) is the modern form that lets us reference
// the new row's values by column without VALUES() (which is
// deprecated on MySQL 8.0.20+). PlanetScale's Vitess proxy supports
// the alias form; pre-8.0.20 MySQL deployments are below the project's
// declared minimum so the alias form is safe.
//
// When every column is a PK column there's nothing meaningful to
// SET on conflict; the statement falls back to a no-op
// `id = new.id` reassignment so the conflict is absorbed silently.
// Same shape as the change_applier's buildInsertSQL fallback for
// no-non-PK tables.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// WriteRowsIdempotent implements [ir.IdempotentRowWriter]. The shape
// mirrors [writeBatched] but the SQL goes through
// [buildBatchUpsert] which appends the ON DUPLICATE KEY UPDATE clause.
func (w *RowWriter) WriteRowsIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("mysql: WriteRowsIdempotent: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("mysql: WriteRowsIdempotent: table %q has no columns", table.Name)
	}
	if rows == nil {
		return errors.New("mysql: WriteRowsIdempotent: rows channel is nil")
	}
	return w.writeBatchedIdempotent(ctx, table, rows)
}

// writeBatchedIdempotent is the upsert-form of [writeBatched]. The
// per-batch flush mechanics are identical (flush on whichever of
// row-count cap and byte-size cap fires first; ADR-0028); only the
// SQL changes.
func (w *RowWriter) writeBatchedIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}
	byteCap := w.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

	pkCols := primaryKeyColumns(table)

	batch := make([]ir.Row, 0, limit)
	var batchBytes int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := buildBatchUpsert(table, len(batch), pkCols)
		args := flattenArgs(batch, table)
		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("mysql: idempotent insert into %q (%d rows): %w", table.Name, len(batch), err)
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				return flush()
			}
			batch = append(batch, row)
			batchBytes += ir.ApproximateRowBytes(row)
			if len(batch) >= limit || batchBytes >= byteCap {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// buildBatchUpsert returns the upsert-form of the multi-row INSERT.
// pkCols are the primary-key column names in declaration order.
//
// When pkCols is empty (no-PK table), the function falls back to a
// plain INSERT — the orchestrator routes no-PK tables to truncate-
// and-redo so this fallback is defensive.
func buildBatchUpsert(table *ir.Table, rowCount int, pkCols []string) string {
	cols := nonGeneratedColumns(table.Columns)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = quoteIdent(c.Name)
	}

	rowPart := buildRowPlaceholder(len(cols))
	rowParts := make([]string, rowCount)
	for i := range rowParts {
		rowParts[i] = rowPart
	}

	var sb strings.Builder
	fmt.Fprintf(
		&sb, "INSERT INTO %s (%s) VALUES %s",
		quoteIdent(table.Name),
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)

	if len(pkCols) == 0 {
		return sb.String()
	}

	pkSet := make(map[string]struct{}, len(pkCols))
	for _, c := range pkCols {
		pkSet[c] = struct{}{}
	}
	nonPK := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, isPK := pkSet[c.Name]; isPK {
			continue
		}
		nonPK = append(nonPK, c.Name)
	}
	sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
	if len(nonPK) == 0 {
		// Every column is a PK. Re-assign the first PK to itself so
		// the statement parses; the conflict resolves to a no-op
		// UPDATE which is the right semantic.
		sb.WriteString(quoteIdent(pkCols[0]))
		sb.WriteString(" = new.")
		sb.WriteString(quoteIdent(pkCols[0]))
		return sb.String()
	}
	parts := make([]string, len(nonPK))
	for i, c := range nonPK {
		parts[i] = fmt.Sprintf("%s = new.%s", quoteIdent(c), quoteIdent(c))
	}
	sb.WriteString(strings.Join(parts, ", "))
	return sb.String()
}

// primaryKeyColumns returns the PK column names in declaration order,
// or an empty slice when the table has no PK. Centralised so the
// idempotent writer and any future callers share the same extraction
// shape.
func primaryKeyColumns(table *ir.Table) []string {
	if table == nil || table.PrimaryKey == nil {
		return nil
	}
	out := make([]string, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		out[i] = c.Column
	}
	return out
}
