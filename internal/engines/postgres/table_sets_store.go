// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var (
	_ ir.PendingAddedTablesStore = (*ChangeApplier)(nil)
	_ ir.EstablishedTablesStore  = (*ChangeApplier)(nil)
)

// tableSetColumnNames are sluice_cdc_state's three table-set columns, ensured
// together (R6 S1(b)): a control table one DDL request brought up to date
// carries all three, so no record write can find one present and the next
// refused.
var tableSetColumnNames = []string{
	appliershared.PendingAddedTablesColumn, appliershared.EstablishedTablesColumn, appliershared.DroppedTablesColumn,
}

// tableSetColumnType is the type each table-set column is declared with.
const tableSetColumnType = "TEXT"

// tableSetColumn is one of sluice_cdc_state's table-set columns.
// Deliberately NOT in [cdcStateTable]'s column list: that list is ensured at
// every start, and an ALTER there fails `sync start` for a role that cannot
// alter a pre-created control table. They are ensured on the first write
// instead, detecting first ([controlTable.ensureColumns]) so a DML-only role
// on a table that already carries them passes.
func tableSetColumn(name string) controlColumn {
	return controlColumn{name: name, def: name + " " + tableSetColumnType + " NULL"}
}

// pgNowMillisExpr is the server's wall clock in Unix milliseconds:
// clock_timestamp(), not now(), which is frozen at the start of the
// transaction. It rides the SELECT that reads the control row (P1).
const pgNowMillisExpr = "(EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000)::bigint"

// tableSetQueries renders the statements the shared table-set helpers run
// against one column of this applier's control table.
func (a *ChangeApplier) tableSetQueries(col string) appliershared.TableSetQueries {
	ref := controlTableRef(a.controlSchema)
	read := "SELECT " + col + ", " + pgNowMillisExpr + " FROM " + ref + " WHERE stream_id = $1"
	return appliershared.TableSetQueries{
		Column:          col,
		Read:            read,
		ReadForUpdate:   read + " FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = $1 WHERE stream_id = $2",
		IsMissingColumn: isUndefinedColumnErr,
	}
}

// pendingAddQueries renders [appliershared.CompletePendingAdd]'s statements.
func (a *ChangeApplier) pendingAddQueries() appliershared.PendingAddQueries {
	ref := controlTableRef(a.controlSchema)
	cols := appliershared.PendingAddedTablesColumn + ", " + appliershared.EstablishedTablesColumn + ", " + appliershared.DroppedTablesColumn
	return appliershared.PendingAddQueries{
		ReadForUpdate: "SELECT " + cols + " FROM " + ref + " WHERE stream_id = $1 FOR UPDATE",
		Update: "UPDATE " + ref + " SET " + appliershared.PendingAddedTablesColumn + " = $1, " +
			appliershared.EstablishedTablesColumn + " = $2, " + appliershared.DroppedTablesColumn + " = $3 WHERE stream_id = $4",
	}
}

// tableSetColumnsDDL is the statement that adds all three columns, named in
// every refusal for a missing one.
func (a *ChangeApplier) tableSetColumnsDDL() string {
	t := cdcStateTable(a.controlSchema)
	stmts := make([]string, len(tableSetColumnNames))
	for i, c := range tableSetColumnNames {
		stmts[i] = t.addColumnDDL(tableSetColumn(c))
	}
	return strings.Join(stmts, "; ")
}

// ensureTableSetColumns ensures all three table-set columns exist (see
// [tableSetColumn]) and have the declared type.
func (a *ChangeApplier) ensureTableSetColumns(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("postgres: applier: table sets: streamID is empty")
	}
	defs := make([]controlColumn, len(tableSetColumnNames))
	for i, c := range tableSetColumnNames {
		defs[i] = tableSetColumn(c)
	}
	if err := cdcStateTable(a.controlSchema).ensureColumns(ctx, a.db, defs); err != nil {
		return fmt.Errorf("%w (the three table-set columns are added together; to add them by hand: %s)", err, a.tableSetColumnsDDL())
	}
	return a.verifyTableSetColumnTypes(ctx)
}

// verifyTableSetColumnTypes refuses a table-set column of any type but TEXT
// (L2): a DBA-created VARCHAR(n) would cut a set at a length the codec
// cannot see. A column that does not exist yet is not judged. Verified once
// per applier.
func (a *ChangeApplier) verifyTableSetColumnTypes(ctx context.Context) error {
	if a.tableSetColumnsVerified.Load() {
		return nil
	}
	rows, err := a.db.QueryContext(ctx,
		"SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a "+
			"WHERE a.attrelid = pg_catalog.to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped",
		controlTableRef(a.controlSchema))
	if err != nil {
		return fmt.Errorf("postgres: applier: read the table-set column types: %w", err)
	}
	defer func() { _ = rows.Close() }()
	want := map[string]bool{}
	for _, c := range tableSetColumnNames {
		want[c] = true
	}
	found := 0
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return fmt.Errorf("postgres: applier: read the table-set column types: %w", err)
		}
		if !want[name] {
			continue
		}
		found++
		if typ != "text" {
			return fmt.Errorf("postgres: applier: control-table column %s is %s, not text, and a value longer than it "+
				"holds would be cut where the record cannot tell; change it with: ALTER TABLE %s ALTER COLUMN %s TYPE TEXT",
				name, typ, controlTableRef(a.controlSchema), name)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: applier: read the table-set column types: %w", err)
	}
	if found == len(tableSetColumnNames) {
		a.tableSetColumnsVerified.Store(true)
	}
	return nil
}

// updateTableSet runs edit against col: ensuring all three columns first
// when ensure is set, and verifying the type of whichever exist otherwise.
func (a *ChangeApplier) updateTableSet(ctx context.Context, streamID, col string, ensure, requireRow bool, edit appliershared.TableSetEdit) error {
	if ensure {
		if err := a.ensureTableSetColumns(ctx, streamID); err != nil {
			return err
		}
	} else if err := a.verifyTableSetColumnTypes(ctx); err != nil {
		return err
	}
	return classifyApplierError(appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(col), streamID, requireRow, edit))
}

// RecordPendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) RecordPendingAddedTable(ctx context.Context, streamID, table, token string) error {
	return a.updateTableSet(ctx, streamID, appliershared.PendingAddedTablesColumn, true, true, appliershared.PendingAdd(table, token))
}

// TouchPendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) TouchPendingAddedTable(ctx context.Context, streamID, table, token string, alive bool) error {
	superseded := false
	if err := a.updateTableSet(ctx, streamID, appliershared.PendingAddedTablesColumn, false, false,
		appliershared.PendingTouch(table, token, alive, &superseded)); err != nil {
		return err
	}
	if superseded {
		return fmt.Errorf("postgres: pending add-table %q: %w", table, ir.ErrPendingAddSuperseded)
	}
	return nil
}

// CompletePendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) CompletePendingAddedTable(ctx context.Context, streamID, table, token string) error {
	if err := a.ensureTableSetColumns(ctx, streamID); err != nil {
		return err
	}
	return classifyApplierError(appliershared.CompletePendingAdd(ctx, a.db, controlCfg, a.pendingAddQueries(), streamID, table, token))
}

// ClearPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ClearPendingAddedTables(ctx context.Context, streamID string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	return a.updateTableSet(ctx, streamID, appliershared.PendingAddedTablesColumn, false, false, appliershared.PendingClear(tables))
}

// AbandonPendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) AbandonPendingAddedTable(ctx context.Context, streamID, table string) error {
	return a.updateTableSet(ctx, streamID, appliershared.PendingAddedTablesColumn, false, false, appliershared.PendingAbandon(table))
}

// NarrowEstablishedRecord implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) NarrowEstablishedRecord(ctx context.Context, streamID string, inScope, gone []string) ([]string, error) {
	if err := a.ensureTableSetColumns(ctx, streamID); err != nil {
		return nil, err
	}
	ref := controlTableRef(a.controlSchema)
	q := appliershared.RecordQueries{
		ReadForUpdate: "SELECT " + appliershared.EstablishedTablesColumn + ", " + appliershared.DroppedTablesColumn +
			" FROM " + ref + " WHERE stream_id = $1 FOR UPDATE",
		Update: "UPDATE " + ref + " SET " + appliershared.EstablishedTablesColumn + " = $1, " +
			appliershared.DroppedTablesColumn + " = $2 WHERE stream_id = $3",
	}
	leaving, err := appliershared.NarrowEstablished(ctx, a.db, controlCfg, q, streamID, inScope, gone)
	return leaving, classifyApplierError(err)
}

// ReadPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ReadPendingAddedTables(ctx context.Context, streamID string) ([]ir.PendingAddedTable, time.Time, error) {
	entries, now, err := appliershared.ReadPendingEntries(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID)
	return entries, now, classifyApplierError(err)
}

// ReadEstablishedRecord implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) ReadEstablishedRecord(ctx context.Context, streamID string) (ir.EstablishedRecord, error) {
	rec, err := appliershared.ReadEstablishedRecord(ctx, a.db, controlCfg,
		a.tableSetQueries(appliershared.EstablishedTablesColumn), a.tableSetQueries(appliershared.DroppedTablesColumn), streamID)
	return rec, classifyApplierError(err)
}

// BeginColdStartRecord implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) BeginColdStartRecord(ctx context.Context, streamID string) error {
	return a.updateTableSet(ctx, streamID, appliershared.EstablishedTablesColumn, true, false, appliershared.BeginColdStart())
}

// RecordEstablishedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) RecordEstablishedTables(ctx context.Context, streamID string, tables []string) error {
	return a.updateTableSet(ctx, streamID, appliershared.EstablishedTablesColumn, true, true, appliershared.EstablishedReplace(tables))
}

// RecordDroppedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) RecordDroppedTables(ctx context.Context, streamID string, tables []string) error {
	return a.updateTableSet(ctx, streamID, appliershared.DroppedTablesColumn, true, true, appliershared.EstablishedReplace(tables))
}
