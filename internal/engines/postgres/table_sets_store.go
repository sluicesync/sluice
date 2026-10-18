// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
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

// pgServerNowMillis reads the server's wall clock: clock_timestamp(), not
// now(), which is frozen at the start of the transaction it runs in.
const pgServerNowMillis = "SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000)::bigint"

// tableSetQueries renders the statements the shared table-set helpers run
// against one column of this applier's control table.
func (a *ChangeApplier) tableSetQueries(col string) appliershared.TableSetQueries {
	ref := controlTableRef(a.controlSchema)
	return appliershared.TableSetQueries{
		Column:          col,
		Read:            "SELECT " + col + " FROM " + ref + " WHERE stream_id = $1",
		ReadForUpdate:   "SELECT " + col + " FROM " + ref + " WHERE stream_id = $1 FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = $1 WHERE stream_id = $2",
		ServerNowMillis: pgServerNowMillis,
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

// ensureTableSetColumns ensures cols exist (see [tableSetColumn]).
func (a *ChangeApplier) ensureTableSetColumns(ctx context.Context, streamID string, cols ...string) error {
	if streamID == "" {
		return errors.New("postgres: applier: table sets: streamID is empty")
	}
	defs := make([]controlColumn, len(cols))
	for i, c := range cols {
		defs[i] = tableSetColumn(c)
	}
	return cdcStateTable(a.controlSchema).ensureColumns(ctx, a.db, defs)
}

// updateTableSet runs edit against col, ensuring the column first when ensure
// is set.
func (a *ChangeApplier) updateTableSet(ctx context.Context, streamID, col string, ensure, requireRow bool, edit appliershared.TableSetEdit) error {
	if ensure {
		if err := a.ensureTableSetColumns(ctx, streamID, col); err != nil {
			return err
		}
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
	if err := a.ensureTableSetColumns(ctx, streamID, appliershared.PendingAddedTablesColumn,
		appliershared.EstablishedTablesColumn, appliershared.DroppedTablesColumn); err != nil {
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
	if err := a.ensureTableSetColumns(ctx, streamID, appliershared.EstablishedTablesColumn, appliershared.DroppedTablesColumn); err != nil {
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
