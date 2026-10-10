// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// The RENAME-SWAP-FORWARD column-order record — MySQL store
// ([ir.ColumnOrderStore]; internal/ir/column_order_record.go).
//
// One row per (stream, source namespace, table), keyed by the fixed-width
// [ir.ColumnOrderKey] surrogate for the same InnoDB reason as the schema
// history's version_key. Best-effort by the same operator decision as the
// apply marks: a role or a safe-migrations branch that cannot create the
// table runs without records, and a column-order change on such a stream
// refuses as unprovable instead of being accepted.

const columnOrderTableName = appliershared.ColumnOrderTableName

// columnOrderTableDDL renders the record table. The identifier columns are
// binary-collated (the control-table rule); the two column lists are the
// [ir.EncodeColumnNames] form, LONGTEXT because a wide table's names run
// past TEXT's 64 KiB.
func columnOrderTableDDL(controlKeyspace string) string {
	return `CREATE TABLE IF NOT EXISTS ` + controlTableRef(controlKeyspace, columnOrderTableName) + ` (
	order_key      CHAR(64)     ` + controlIdentifierCollateClause + ` NOT NULL,
	stream_id      VARCHAR(255) ` + controlIdentifierCollateClause + ` NOT NULL,
	schema_name    VARCHAR(255) ` + controlIdentifierCollateClause + ` NOT NULL,
	table_name     VARCHAR(255) ` + controlIdentifierCollateClause + ` NOT NULL,
	source_columns LONGTEXT     NOT NULL,
	target_columns LONGTEXT     NOT NULL,
	updated_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (order_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

// ensureColumnOrderTable creates the record table when it is absent —
// detect-then-create, so a table that is already there costs no DDL.
func ensureColumnOrderTable(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	exists, err := controlTableExists(ctx, db, controlKeyspace, columnOrderTableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure %s: %w", columnOrderTableName, err)
	}
	if exists {
		return nil
	}
	ddl := columnOrderTableDDL(controlKeyspace)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure %s: %w", columnOrderTableName, wrapControlTableBootstrapError(wrapDDLError(err), ddl))
	}
	return nil
}

// columnOrderUpsertSQL is the record upsert in the caller's spelling.
func columnOrderUpsertSQL(controlKeyspace string, upsert upsertSpelling) string {
	return "INSERT INTO " + controlTableRef(controlKeyspace, columnOrderTableName) + " " +
		"(order_key, stream_id, schema_name, table_name, source_columns, target_columns, updated_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)" +
		upsert.clauseOpen() +
		"source_columns = " + upsert.newRowRef("source_columns") + ", " +
		"target_columns = " + upsert.newRowRef("target_columns") + ", " +
		"updated_at = " + upsert.newRowRef("updated_at")
}

// writeColumnOrder upserts one record through exec — the pool for the cold
// start's records, the apply transaction for a boundary's.
func writeColumnOrder(ctx context.Context, exec schemaHistoryExecer, controlKeyspace string, upsert upsertSpelling, streamID string, r ir.ColumnOrderRecord) error {
	src, err := ir.EncodeColumnNames(r.SourceColumns)
	if err != nil {
		return fmt.Errorf("mysql: write column order for %s.%s: source: %w", r.Schema, r.Table, err)
	}
	tgt, err := ir.EncodeColumnNames(r.TargetColumns)
	if err != nil {
		return fmt.Errorf("mysql: write column order for %s.%s: target: %w", r.Schema, r.Table, err)
	}
	key := ir.ColumnOrderKey(streamID, r.Schema, r.Table)
	if _, err := exec.ExecContext(ctx, columnOrderUpsertSQL(controlKeyspace, upsert), key, streamID, r.Schema, r.Table, src, tgt); err != nil {
		return fmt.Errorf("mysql: write column order for %s.%s: %w", r.Schema, r.Table, err)
	}
	return nil
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
		if err := writeColumnOrder(ctx, a.db, a.controlKeyspace, a.upsert, streamID, r); err != nil {
			return err
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
		controlTableRef(a.controlKeyspace, columnOrderTableName) + " WHERE stream_id = ?"
	rows, err := a.db.QueryContext(ctx, q, streamID)
	if err != nil {
		return nil, fmt.Errorf("mysql: read column orders: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []ir.ColumnOrderRecord{}
	for rows.Next() {
		var (
			r        ir.ColumnOrderRecord
			src, tgt string
		)
		if err := rows.Scan(&r.Schema, &r.Table, &src, &tgt); err != nil {
			return nil, fmt.Errorf("mysql: read column orders: %w", err)
		}
		if r.SourceColumns, err = ir.DecodeColumnNames(src); err != nil {
			return nil, fmt.Errorf("mysql: read column order for %s.%s: source: %w", r.Schema, r.Table, err)
		}
		if r.TargetColumns, err = ir.DecodeColumnNames(tgt); err != nil {
			return nil, fmt.Errorf("mysql: read column order for %s.%s: target: %w", r.Schema, r.Table, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: read column orders: %w", err)
	}
	return out, nil
}

// writeBoundaryColumnOrder persists the record a boundary carries, on the
// boundary's own transaction (nil TargetColumns: nothing to record). A
// record the target cannot keep is an error here: the pipeline attaches one
// only after a successful read, so reaching this with the table missing is
// a broken invariant, not a degraded target.
func (a *ChangeApplier) writeBoundaryColumnOrder(ctx context.Context, tx *sql.Tx, streamID string, v ir.SchemaSnapshot) error {
	if v.TargetColumns == nil {
		return nil
	}
	if err := a.columnOrderUnavailable(); err != nil {
		return errors.Join(errors.New("mysql: applier: a boundary carries a column-order record the target cannot keep"), err)
	}
	return writeColumnOrder(ctx, tx, a.controlKeyspace, a.upsert, streamID, ir.ColumnOrderRecord{
		Schema: v.Schema, Table: v.Table, SourceColumns: ir.ColumnNames(v.IR), TargetColumns: v.TargetColumns,
	})
}

// The RENAME-SWAP-FORWARD column-order store (column_order.go).
var _ ir.ColumnOrderStore = (*ChangeApplier)(nil)
