// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

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

// RecordPendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) RecordPendingAddedTable(ctx context.Context, streamID, table string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RecordPendingAddedTable: streamID is empty")
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
	return appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID, false,
		appliershared.PendingClear(tables))
}

// ReadPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ReadPendingAddedTables(ctx context.Context, streamID string) ([]string, error) {
	tables, _, err := appliershared.ReadTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID)
	return tables, err
}

// ReadEstablishedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) ReadEstablishedTables(ctx context.Context, streamID string) (tables []string, recorded bool, err error) {
	return appliershared.ReadTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.EstablishedTablesColumn), streamID)
}

// RecordEstablishedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) RecordEstablishedTables(ctx context.Context, streamID string, tables []string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RecordEstablishedTables: streamID is empty")
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
	return appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.EstablishedTablesColumn), streamID, false,
		appliershared.EstablishedAdd(table))
}
