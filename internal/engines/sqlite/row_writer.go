// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// defaultMaxRowsPerBatch caps how many rows fold into one multi-row INSERT
// (ADR-0134). The effective per-statement row count is further bounded by
// [maxBindParamsPerStmt] / column-count so a wide table never exceeds
// SQLite's bound-parameter limit.
const defaultMaxRowsPerBatch = 200

// defaultMaxBufferBytes is the soft per-batch byte cap when the caller
// doesn't set one (ir.MaxBufferBytesSetter). Bounds heap for wide rows.
const defaultMaxBufferBytes int64 = 64 << 20 // 64 MiB

// maxBindParamsPerStmt is a conservative cap on bound parameters in one
// INSERT, comfortably under SQLite's SQLITE_MAX_VARIABLE_NUMBER (999 on
// pre-3.32 builds, 32766 after). Staying under the smaller floor keeps the
// writer correct regardless of the driver's compiled limit.
const maxBindParamsPerStmt = 900

// RowWriter performs bulk inserts into a SQLite target via multi-row
// parameterised INSERTs inside a single per-table transaction (SQLite's
// fast-load path — there is no COPY/LOAD DATA). It implements
// [ir.RowWriter] and [ir.MaxBufferBytesSetter]. The writer holds an open
// writable *sql.DB (FK enforcement off — see connect.go); callers Close it
// to release the pool.
type RowWriter struct {
	db   *sql.DB
	path string

	// maxRowsPerBatch caps rows per INSERT; tests override it. Zero uses
	// defaultMaxRowsPerBatch (further bounded by the bind-param cap).
	maxRowsPerBatch int

	// maxBufferBytes is the soft per-batch byte cap (ir.MaxBufferBytesSetter).
	// Zero/negative means "no byte cap"; the row-count cap remains.
	maxBufferBytes int64
}

// SetMaxBufferBytes implements [ir.MaxBufferBytesSetter]. Called by the
// orchestrator after OpenRowWriter when --max-buffer-bytes is set. Zero or
// negative means "no byte cap".
func (w *RowWriter) SetMaxBufferBytes(bytes int64) {
	w.maxBufferBytes = bytes
}

// Close releases the underlying connection pool.
func (w *RowWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// TruncateTable empties the target table (resume truncate-and-redo path).
// SQLite has no TRUNCATE statement; DELETE FROM is the equivalent and is
// fast within the engine's optimised truncate path. Implements
// [ir.TableTruncator].
func (w *RowWriter) TruncateTable(ctx context.Context, table *ir.Table) error {
	if table == nil {
		return errors.New("sqlite: TruncateTable: table is nil")
	}
	if _, err := w.db.ExecContext(ctx, "DELETE FROM "+quoteIdent(table.Name)); err != nil {
		return fmt.Errorf("sqlite: truncate %q: %w", table.Name, err)
	}
	return nil
}

// DropTable drops the target table (the --reset-target-data path).
// Implements [ir.TableDropper]. IF EXISTS keeps it idempotent across
// partial-failure retries.
func (w *RowWriter) DropTable(ctx context.Context, table *ir.Table) error {
	if table == nil {
		return errors.New("sqlite: DropTable: table is nil")
	}
	if _, err := w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent(table.Name)); err != nil {
		return fmt.Errorf("sqlite: drop %q: %w", table.Name, err)
	}
	return nil
}

// WriteRows consumes rows from the channel and inserts them into table via
// multi-row INSERTs inside one transaction. Returns when the channel is
// closed (commit) or when ctx is cancelled / a DB or encode error occurs
// (rollback + error). A value SQLite cannot faithfully store is refused
// LOUDLY (naming the table.column) rather than silently coerced.
func (w *RowWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("sqlite: WriteRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("sqlite: WriteRows: table %q has no columns", table.Name)
	}
	if rows == nil {
		return errors.New("sqlite: WriteRows: rows channel is nil")
	}

	cols := nonGeneratedColumns(table.Columns)
	if len(cols) == 0 {
		return fmt.Errorf("sqlite: WriteRows: table %q has no insertable (non-generated) columns", table.Name)
	}

	rowsPerBatch := w.resolveRowsPerBatch(len(cols))
	byteCap := w.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin tx for %q: %w", table.Name, err)
	}
	// rollback on any non-commit exit; a no-op after a successful Commit.
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	batch := make([]ir.Row, 0, rowsPerBatch)
	var batchBytes int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := buildBatchInsert(table.Name, cols, len(batch))
		args, err := flattenArgs(table.Name, cols, batch)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("sqlite: insert into %q (%d rows): %w", table.Name, len(batch), err)
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("sqlite: commit %q: %w", table.Name, err)
				}
				committed = true
				return nil
			}
			batch = append(batch, row)
			batchBytes += ir.ApproximateRowBytes(row)
			if len(batch) >= rowsPerBatch || batchBytes >= byteCap {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// resolveRowsPerBatch bounds the per-INSERT row count by both the operator
// row cap and SQLite's bound-parameter limit (cols × rows ≤
// maxBindParamsPerStmt), never below 1.
func (w *RowWriter) resolveRowsPerBatch(numCols int) int {
	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}
	if numCols > 0 {
		if byParams := maxBindParamsPerStmt / numCols; byParams >= 1 && byParams < limit {
			limit = byParams
		}
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// buildBatchInsert renders the parameterised multi-row INSERT for cols and
// a given row count: INSERT INTO "t" ("c1","c2") VALUES (?,?),(?,?),...
func buildBatchInsert(tableName string, cols []*ir.Column, rowCount int) string {
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = quoteIdent(c.Name)
	}
	rowPart := rowPlaceholder(len(cols))
	rowParts := make([]string, rowCount)
	for i := range rowParts {
		rowParts[i] = rowPart
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		quoteIdent(tableName),
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)
}

// rowPlaceholder returns a single row's "(?, ?, ...)" placeholder fragment.
func rowPlaceholder(numCols int) string {
	if numCols == 1 {
		return "(?)"
	}
	return "(" + strings.Repeat("?, ", numCols-1) + "?)"
}

// flattenArgs walks the batch row-major and produces the flat []any the
// driver binds, encoding each value via [encodeValue] (refusing loudly any
// value SQLite cannot faithfully store, naming table.column).
func flattenArgs(tableName string, cols []*ir.Column, batch []ir.Row) ([]any, error) {
	args := make([]any, 0, len(batch)*len(cols))
	for _, row := range batch {
		for _, col := range cols {
			enc, err := encodeValue(col, row[col.Name])
			if err != nil {
				return nil, fmt.Errorf("sqlite: table %q: %w", tableName, err)
			}
			args = append(args, enc)
		}
	}
	return args, nil
}
