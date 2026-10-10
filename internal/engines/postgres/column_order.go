// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// The RENAME-SWAP-FORWARD column-order record — Postgres store
// ([ir.ColumnOrderStore]; internal/ir/column_order_record.go).
//
// One row per (stream, source namespace, table), keyed by the
// [ir.ColumnOrderKey] surrogate (engine-identical with MySQL, as the
// schema history's version_key is). Best-effort by the same operator
// decision as the apply marks: a role that cannot create the table runs
// without records, and a column-order change on such a stream refuses as
// unprovable instead of being accepted.

const columnOrderTableName = appliershared.ColumnOrderTableName

// columnOrderTableRef is the schema-qualified, quoted record-table reference.
func columnOrderTableRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(columnOrderTableName)
}

// columnOrderTable is the record table; the two column lists are the
// [ir.EncodeColumnNames] form. Every write names updated_at explicitly, so
// no DEFAULT is relied on.
func columnOrderTable(schema string) controlTable {
	return controlTable{
		name:   columnOrderTableName,
		schema: schema,
		create: `CREATE TABLE IF NOT EXISTS ` + columnOrderTableRef(schema) + ` (
		order_key      CHAR(64)     NOT NULL,
		stream_id      VARCHAR(255) NOT NULL,
		schema_name    TEXT         NOT NULL,
		table_name     TEXT         NOT NULL,
		source_columns TEXT         NOT NULL,
		target_columns TEXT         NOT NULL,
		updated_at     TIMESTAMP    NOT NULL DEFAULT (` + utcNowSQL + `),
		PRIMARY KEY (order_key)
	)`,
	}
}

// ensureColumnOrderTable creates the record table when it is absent
// (detect-first, through [controlTable.ensure]).
func ensureColumnOrderTable(ctx context.Context, db *sql.DB, schema string) error {
	return columnOrderTable(schema).ensure(ctx, db)
}

// buildWriteColumnOrderSQL returns the record upsert (sql, args), shared by
// the serial path and the pipelined queue the way the schema-history
// upsert is.
func buildWriteColumnOrderSQL(schema, streamID string, r ir.ColumnOrderRecord) (stmt string, args []any, err error) {
	src, err := ir.EncodeColumnNames(r.SourceColumns)
	if err != nil {
		return "", nil, fmt.Errorf("postgres: write column order for %s.%s: source: %w", r.Schema, r.Table, err)
	}
	tgt, err := ir.EncodeColumnNames(r.TargetColumns)
	if err != nil {
		return "", nil, fmt.Errorf("postgres: write column order for %s.%s: target: %w", r.Schema, r.Table, err)
	}
	q := "INSERT INTO " + columnOrderTableRef(schema) + " " +
		"(order_key, stream_id, schema_name, table_name, source_columns, target_columns, updated_at) " +
		"VALUES ($1, $2, $3, $4, $5, $6, " + utcNowSQL + ") " +
		"ON CONFLICT (order_key) DO UPDATE SET " +
		"source_columns = EXCLUDED.source_columns, target_columns = EXCLUDED.target_columns, updated_at = EXCLUDED.updated_at"
	return q, []any{ir.ColumnOrderKey(streamID, r.Schema, r.Table), streamID, r.Schema, r.Table, src, tgt}, nil
}

// columnOrderUnavailable returns the error a record operation reports when
// the table could not be created, or nil.
func (a *ChangeApplier) columnOrderUnavailable() error {
	if a.columnOrderEnsureErr == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ir.ErrColumnOrderStoreUnavailable, a.columnOrderEnsureErr)
}

// WriteColumnOrders implements [ir.ColumnOrderStore].
func (a *ChangeApplier) WriteColumnOrders(ctx context.Context, streamID string, records []ir.ColumnOrderRecord) error {
	if err := a.columnOrderUnavailable(); err != nil {
		return err
	}
	for _, r := range records {
		q, args, err := buildWriteColumnOrderSQL(a.controlSchema, streamID, r)
		if err != nil {
			return err
		}
		if _, err := a.db.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("postgres: write column order for %s.%s: %w", r.Schema, r.Table, err)
		}
	}
	return nil
}

// ReadColumnOrders implements [ir.ColumnOrderStore]. A stored list that
// does not decode refuses the read: a damaged record is never a baseline.
func (a *ChangeApplier) ReadColumnOrders(ctx context.Context, streamID string) ([]ir.ColumnOrderRecord, error) {
	if err := a.columnOrderUnavailable(); err != nil {
		return nil, err
	}
	q := "SELECT schema_name, table_name, source_columns, target_columns FROM " +
		columnOrderTableRef(a.controlSchema) + " WHERE stream_id = $1"
	rows, err := a.db.QueryContext(ctx, q, streamID)
	if err != nil {
		return nil, fmt.Errorf("postgres: read column orders: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []ir.ColumnOrderRecord{}
	for rows.Next() {
		var (
			r        ir.ColumnOrderRecord
			src, tgt string
		)
		if err := rows.Scan(&r.Schema, &r.Table, &src, &tgt); err != nil {
			return nil, fmt.Errorf("postgres: read column orders: %w", err)
		}
		if r.SourceColumns, err = ir.DecodeColumnNames(src); err != nil {
			return nil, fmt.Errorf("postgres: read column order for %s.%s: source: %w", r.Schema, r.Table, err)
		}
		if r.TargetColumns, err = ir.DecodeColumnNames(tgt); err != nil {
			return nil, fmt.Errorf("postgres: read column order for %s.%s: target: %w", r.Schema, r.Table, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read column orders: %w", err)
	}
	return out, nil
}

// boundaryColumnOrderSQL renders the record a boundary carries for its own
// transaction, or "" when it carries none. A record the target cannot keep
// is an error: the pipeline attaches one only after a successful read, so
// reaching this with the table missing is a broken invariant.
func (a *ChangeApplier) boundaryColumnOrderSQL(streamID string, v ir.SchemaSnapshot) (stmt string, args []any, err error) {
	if v.TargetColumns == nil {
		return "", nil, nil
	}
	if err := a.columnOrderUnavailable(); err != nil {
		return "", nil, errors.Join(errors.New("postgres: applier: a boundary carries a column-order record the target cannot keep"), err)
	}
	return buildWriteColumnOrderSQL(a.controlSchema, streamID, ir.ColumnOrderRecord{
		Schema: v.Schema, Table: v.Table, SourceColumns: ir.ColumnNames(v.IR), TargetColumns: v.TargetColumns,
	})
}

// The RENAME-SWAP-FORWARD column-order store.
var _ ir.ColumnOrderStore = (*ChangeApplier)(nil)
