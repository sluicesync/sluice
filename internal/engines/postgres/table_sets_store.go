// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"time"

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

// updateTableSet runs edit against col, ensuring the column first when ensure
// is set.
func (a *ChangeApplier) updateTableSet(ctx context.Context, streamID, col string, ensure, requireRow bool, edit appliershared.TableSetEdit) error {
	if ensure {
		if streamID == "" {
			return errors.New("postgres: applier: " + col + ": streamID is empty")
		}
		if err := cdcStateTable(a.controlSchema).ensureColumns(ctx, a.db, []controlColumn{tableSetColumn(col)}); err != nil {
			return err
		}
	}
	return classifyApplierError(appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(col), streamID, requireRow, edit))
}

// RecordPendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) RecordPendingAddedTable(ctx context.Context, streamID, table string, now time.Time) error {
	return a.updateTableSet(ctx, streamID, appliershared.PendingAddedTablesColumn, true, true, appliershared.PendingAdd(table, now))
}

// HeartbeatPendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) HeartbeatPendingAddedTable(ctx context.Context, streamID, table string, now time.Time) error {
	return a.updateTableSet(ctx, streamID, appliershared.PendingAddedTablesColumn, false, false, appliershared.PendingHeartbeat(table, now))
}

// ClearPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ClearPendingAddedTables(ctx context.Context, streamID string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	return a.updateTableSet(ctx, streamID, appliershared.PendingAddedTablesColumn, false, false, appliershared.PendingClear(tables))
}

// ReadPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ReadPendingAddedTables(ctx context.Context, streamID string) ([]ir.PendingAddedTable, error) {
	entries, err := appliershared.ReadPendingEntries(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID)
	return entries, classifyApplierError(err)
}

// ReadEstablishedRecord implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) ReadEstablishedRecord(ctx context.Context, streamID string) (ir.EstablishedRecord, error) {
	rec, err := appliershared.ReadEstablishedRecord(ctx, a.db, controlCfg,
		a.tableSetQueries(appliershared.EstablishedTablesColumn), a.tableSetQueries(appliershared.DroppedTablesColumn), streamID)
	return rec, classifyApplierError(err)
}

// ResetEstablishedRecord implements [ir.EstablishedTablesStore]. No DDL: a
// column that does not exist holds nothing to reset.
func (a *ChangeApplier) ResetEstablishedRecord(ctx context.Context, streamID string) error {
	if err := a.updateTableSet(ctx, streamID, appliershared.EstablishedTablesColumn, false, false, appliershared.SetNull()); err != nil {
		return err
	}
	return a.updateTableSet(ctx, streamID, appliershared.DroppedTablesColumn, false, false, appliershared.SetNull())
}

// RecordEstablishedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) RecordEstablishedTables(ctx context.Context, streamID string, tables []string) error {
	return a.updateTableSet(ctx, streamID, appliershared.EstablishedTablesColumn, true, true, appliershared.EstablishedReplace(tables))
}

// RecordDroppedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) RecordDroppedTables(ctx context.Context, streamID string, tables []string) error {
	return a.updateTableSet(ctx, streamID, appliershared.DroppedTablesColumn, true, true, appliershared.EstablishedReplace(tables))
}

// AddEstablishedTable implements [ir.EstablishedTablesStore]. No column, no
// row or no record is nothing to add to.
func (a *ChangeApplier) AddEstablishedTable(ctx context.Context, streamID, table string) error {
	if err := a.updateTableSet(ctx, streamID, appliershared.EstablishedTablesColumn, false, false, appliershared.EstablishedAdd(table)); err != nil {
		return err
	}
	return a.updateTableSet(ctx, streamID, appliershared.DroppedTablesColumn, false, false, appliershared.SetRemove(table))
}
