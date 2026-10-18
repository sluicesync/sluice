// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

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

// tableSetQueries renders the statements the shared table-set helpers run
// against one column of this applier's control table.
func (a *ChangeApplier) tableSetQueries(col string) appliershared.TableSetQueries {
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	return appliershared.TableSetQueries{
		Column:          col,
		Read:            "SELECT " + col + " FROM " + ref + " WHERE stream_id = ?",
		ReadForUpdate:   "SELECT " + col + " FROM " + ref + " WHERE stream_id = ? FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = ? WHERE stream_id = ?",
		IsMissingColumn: isMySQLUnknownColumnErr,
	}
}

// ensureTableSetColumn adds a table-set column on first write rather than at
// every start (the detect-then-ALTER path issues no DDL when it is present):
// `sync start` on a control table this binary cannot ALTER — a PlanetScale
// safe-migrations branch, a DBA-pre-created table — keeps running as before,
// and only the write that needs the column fails, loudly, with the statement
// to ship. LONGTEXT: a set is unbounded in principle, and TEXT refuses a
// write past 64 KB.
func (a *ChangeApplier) ensureTableSetColumn(ctx context.Context, col string) error {
	return ensureCrossEngineParityColumn(ctx, a.db, a.controlKeyspace, col, "LONGTEXT NULL")
}

// updateTableSet runs edit against col, ensuring the column first when ensure
// is set.
func (a *ChangeApplier) updateTableSet(ctx context.Context, streamID, col string, ensure, requireRow bool, edit appliershared.TableSetEdit) error {
	if ensure {
		if streamID == "" {
			return errors.New("mysql: applier: " + col + ": streamID is empty")
		}
		if err := a.ensureTableSetColumn(ctx, col); err != nil {
			return err
		}
	}
	return appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(col), streamID, requireRow, edit)
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
	return appliershared.ReadPendingEntries(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID)
}

// ReadEstablishedRecord implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) ReadEstablishedRecord(ctx context.Context, streamID string) (ir.EstablishedRecord, error) {
	return appliershared.ReadEstablishedRecord(ctx, a.db, controlCfg,
		a.tableSetQueries(appliershared.EstablishedTablesColumn), a.tableSetQueries(appliershared.DroppedTablesColumn), streamID)
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
