// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Idempotent INSERT path for the resume-mid-table bulk-copy.
//
// The cold-start bulk-copy uses COPY FROM STDIN (faster, atomic at
// the table level). Per-batch resume can't use COPY because pgx
// CopyFrom streams in a single logical operation that commits all-
// or-nothing — there's no checkpoint boundary to plant a cursor at.
// The resume path therefore drops to batched INSERT statements with
// ON CONFLICT (PK) DO UPDATE on the conflict target.
//
// The shape of the INSERT mirrors [buildBatchInsert] (the
// non-resume batched path) but adds the conflict clause:
//
//	INSERT INTO "schema"."tbl" ("a", "b", "id") VALUES ($1, $2, $3), ...
//	ON CONFLICT ("id") DO UPDATE SET "a" = EXCLUDED."a", "b" = EXCLUDED."b"
//
// Why DO UPDATE rather than DO NOTHING: the bulk-copy replay window
// is between batch commit and checkpoint write. In the rare case
// where a row in the to-be-replayed batch has a different value on
// the source vs. what landed pre-crash (e.g. someone updated it
// between commits), DO UPDATE re-applies the source row. DO NOTHING
// would silently keep the stale value. ADR-0018 covers this.
//
// PG-specific: when every column is a PK column there's nothing to
// SET on conflict, so the statement falls back to DO NOTHING — which
// is the right semantic ("the conflicting row already has the same
// PK and there are no non-PK columns to overwrite").

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// WriteRowsIdempotent implements [ir.IdempotentRowWriter]. The shape
// mirrors [writeViaBatch] but the SQL goes through
// [buildBatchUpsert] which appends the ON CONFLICT clause.
func (w *RowWriter) WriteRowsIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("postgres: WriteRowsIdempotent: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("postgres: WriteRowsIdempotent: table %q has no columns", table.Name)
	}
	if rows == nil {
		return errors.New("postgres: WriteRowsIdempotent: rows channel is nil")
	}
	return w.writeViaBatchIdempotent(ctx, table, rows)
}

// writeViaBatchIdempotent is the upsert-form of [writeViaBatch]. It
// uses the same per-batch flush mechanics — accumulate rows up to
// maxRowsPerBatch (or maxBufferBytes, ADR-0028, whichever fires
// first), then Exec a multi-row INSERT — but the SQL adds
// ON CONFLICT (pk) DO UPDATE.
//
// We deliberately do not pin a connection here: the orchestrator
// commits cursor state via [ir.MigrationStateStore.Write] *between*
// batch flushes (sequential commits, not co-tx — see ADR-0018), so
// each batch can run on whatever pool connection database/sql hands
// us.
func (w *RowWriter) writeViaBatchIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
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
		query := buildBatchUpsert(w.schema, table, len(batch), pkCols)
		args, err := flattenArgs(batch, table)
		if err != nil {
			return fmt.Errorf("postgres: prepare args for %q: %w", table.Name, err)
		}
		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("postgres: idempotent insert into %q (%d rows): %w", table.Name, len(batch), err)
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
// and-redo so the bulk-copy will only call this path with a PK
// present, but the fallback keeps the function safe for other
// callers (and any future direct invocation by an experimental shape).
func buildBatchUpsert(schema string, table *ir.Table, rowCount int, pkCols []string) string {
	cols := nonGeneratedColumns(table.Columns)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = quoteIdent(c.Name)
	}

	numCols := len(cols)
	rowParts := make([]string, rowCount)
	paramIdx := 0
	for i := range rowParts {
		params := make([]string, numCols)
		for j := range params {
			paramIdx++
			params[j] = fmt.Sprintf("$%d", paramIdx)
		}
		rowParts[i] = "(" + strings.Join(params, ", ") + ")"
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)
	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES %s",
		tableRef,
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)

	if len(pkCols) == 0 {
		return sb.String()
	}

	// ON CONFLICT (pk) DO UPDATE SET non-pk = EXCLUDED.non-pk
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, c := range pkCols {
		pkSet[c] = struct{}{}
	}
	conflictTarget := make([]string, len(pkCols))
	for i, c := range pkCols {
		conflictTarget[i] = quoteIdent(c)
	}

	nonPK := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, isPK := pkSet[c.Name]; isPK {
			continue
		}
		// Generated columns are already excluded by nonGeneratedColumns
		// above; no need to re-check.
		nonPK = append(nonPK, c.Name)
	}

	sb.WriteString(" ON CONFLICT (")
	sb.WriteString(strings.Join(conflictTarget, ", "))
	sb.WriteByte(')')
	if len(nonPK) == 0 {
		// Every column is a PK column — the conflicting row IS the
		// row we wanted to write. DO NOTHING absorbs it silently.
		sb.WriteString(" DO NOTHING")
		return sb.String()
	}
	sb.WriteString(" DO UPDATE SET ")
	parts := make([]string, len(nonPK))
	for i, c := range nonPK {
		parts[i] = fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(c), quoteIdent(c))
	}
	sb.WriteString(strings.Join(parts, ", "))
	return sb.String()
}

// primaryKeyColumns returns the PK column names in declaration order,
// or an empty slice when the table has no PK. Centralised so both the
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
