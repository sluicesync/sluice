// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var (
	_ ir.PendingAddedTablesStore = (*ChangeApplier)(nil)
	_ ir.EstablishedTablesStore  = (*ChangeApplier)(nil)
)

// tableSetColumn is one of sluice_cdc_state's table-set columns.
// Deliberately NOT in [cdcStateTable]'s column list: that list is ensured at
// every start, and an ALTER there fails `sync start` for a role that cannot
// alter a pre-created control table. Each is ensured on its first write
// instead, detecting first ([controlTable.ensureColumns]) so a DML-only role
// on a table that already carries it passes.
func tableSetColumn(name string) controlColumn {
	return controlColumn{name: name, def: name + " TEXT NULL"}
}

// tableSetQueries renders the statements the shared table-set helpers run
// against one column of this applier's control table.
func (a *ChangeApplier) tableSetQueries(col string) appliershared.TableSetQueries {
	ref := controlTableRef(a.controlSchema)
	return appliershared.TableSetQueries{
		Column:          col,
		Read:            "SELECT " + col + " FROM " + ref + " WHERE stream_id = $1",
		ReadForUpdate:   "SELECT " + col + " FROM " + ref + " WHERE stream_id = $1 FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = $1 WHERE stream_id = $2",
		IsMissingColumn: isUndefinedColumnErr,
	}
}

func (a *ChangeApplier) ensureTableSetColumn(ctx context.Context, col string) error {
	return cdcStateTable(a.controlSchema).ensureColumns(ctx, a.db, []controlColumn{tableSetColumn(col)})
}

// RecordPendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) RecordPendingAddedTable(ctx context.Context, streamID, table string) error {
	if streamID == "" {
		return errors.New("postgres: applier: RecordPendingAddedTable: streamID is empty")
	}
	if err := a.ensureTableSetColumn(ctx, appliershared.PendingAddedTablesColumn); err != nil {
		return err
	}
	return appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID, true,
		appliershared.PendingAdd(table))
}

// ClearPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ClearPendingAddedTables(ctx context.Context, streamID string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	return classifyApplierError(appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID, false,
		appliershared.PendingClear(tables)))
}

// ReadPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ReadPendingAddedTables(ctx context.Context, streamID string) ([]string, error) {
	tables, _, err := appliershared.ReadTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID)
	return tables, classifyApplierError(err)
}

// ReadEstablishedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) ReadEstablishedTables(ctx context.Context, streamID string) (tables []string, recorded bool, err error) {
	tables, recorded, err = appliershared.ReadTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.EstablishedTablesColumn), streamID)
	return tables, recorded, classifyApplierError(err)
}

// RecordEstablishedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) RecordEstablishedTables(ctx context.Context, streamID string, tables []string) error {
	if streamID == "" {
		return errors.New("postgres: applier: RecordEstablishedTables: streamID is empty")
	}
	if err := a.ensureTableSetColumn(ctx, appliershared.EstablishedTablesColumn); err != nil {
		return err
	}
	return appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.EstablishedTablesColumn), streamID, true,
		appliershared.EstablishedReplace(tables))
}

// AddEstablishedTable implements [ir.EstablishedTablesStore]. No column, no
// row or no record is nothing to add to.
func (a *ChangeApplier) AddEstablishedTable(ctx context.Context, streamID, table string) error {
	return classifyApplierError(appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.EstablishedTablesColumn), streamID, false,
		appliershared.EstablishedAdd(table)))
}
