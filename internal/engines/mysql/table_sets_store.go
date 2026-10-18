// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

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

// mysqlServerNowMillis reads the server's clock in Unix milliseconds, with no
// time-zone conversion anywhere: UTC_TIMESTAMP(6) is UTC whatever the
// session's time_zone, and TIMESTAMPDIFF over two DATETIMEs is plain
// arithmetic. UNIX_TIMESTAMP(NOW(6)) was not DST-proof: NOW renders the
// session zone's local time, and in a fall-back hour that local time names
// two instants, of which UNIX_TIMESTAMP picks one — an hour off for the other
// (pinned by TestMySQLServerNowMillis_DSTFallBack).
const mysqlServerNowMillis = "SELECT TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6)) DIV 1000"

// tableSetQueries renders the statements the shared table-set helpers run
// against one column of this applier's control table.
func (a *ChangeApplier) tableSetQueries(col string) appliershared.TableSetQueries {
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	return appliershared.TableSetQueries{
		Column:          col,
		Read:            "SELECT " + col + " FROM " + ref + " WHERE stream_id = ?",
		ReadForUpdate:   "SELECT " + col + " FROM " + ref + " WHERE stream_id = ? FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = ? WHERE stream_id = ?",
		ServerNowMillis: mysqlServerNowMillis,
		IsMissingColumn: isMySQLUnknownColumnErr,
	}
}

// pendingAddQueries renders [appliershared.CompletePendingAdd]'s statements.
func (a *ChangeApplier) pendingAddQueries() appliershared.PendingAddQueries {
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	cols := appliershared.PendingAddedTablesColumn + ", " + appliershared.EstablishedTablesColumn + ", " + appliershared.DroppedTablesColumn
	return appliershared.PendingAddQueries{
		ReadForUpdate: "SELECT " + cols + " FROM " + ref + " WHERE stream_id = ? FOR UPDATE",
		Update: "UPDATE " + ref + " SET " + appliershared.PendingAddedTablesColumn + " = ?, " +
			appliershared.EstablishedTablesColumn + " = ?, " + appliershared.DroppedTablesColumn + " = ? WHERE stream_id = ?",
	}
}

// ensureTableSetColumns adds table-set columns on first write rather than at
// every start (the detect-then-ALTER path issues no DDL when they are
// present): `sync start` on a control table this binary cannot ALTER — a
// PlanetScale safe-migrations branch, a DBA-pre-created table — keeps running
// as before, and only the write that needs a column fails, loudly, with the
// statement to ship. LONGTEXT: a set is unbounded in principle, and TEXT
// refuses a write past 64 KB.
func (a *ChangeApplier) ensureTableSetColumns(ctx context.Context, streamID string, cols ...string) error {
	if streamID == "" {
		return errors.New("mysql: applier: table sets: streamID is empty")
	}
	for _, col := range cols {
		if err := ensureCrossEngineParityColumn(ctx, a.db, a.controlKeyspace, col, "LONGTEXT NULL"); err != nil {
			return err
		}
	}
	return nil
}

// updateTableSet runs edit against col, ensuring the column first when ensure
// is set.
func (a *ChangeApplier) updateTableSet(ctx context.Context, streamID, col string, ensure, requireRow bool, edit appliershared.TableSetEdit) error {
	if ensure {
		if err := a.ensureTableSetColumns(ctx, streamID, col); err != nil {
			return err
		}
	}
	return appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(col), streamID, requireRow, edit)
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
		return fmt.Errorf("mysql: pending add-table %q: %w", table, ir.ErrPendingAddSuperseded)
	}
	return nil
}

// CompletePendingAddedTable implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) CompletePendingAddedTable(ctx context.Context, streamID, table, token string) error {
	if err := a.ensureTableSetColumns(ctx, streamID, appliershared.PendingAddedTablesColumn,
		appliershared.EstablishedTablesColumn, appliershared.DroppedTablesColumn); err != nil {
		return err
	}
	return appliershared.CompletePendingAdd(ctx, a.db, controlCfg, a.pendingAddQueries(), streamID, table, token)
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
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	q := appliershared.RecordQueries{
		ReadForUpdate: "SELECT " + appliershared.EstablishedTablesColumn + ", " + appliershared.DroppedTablesColumn +
			" FROM " + ref + " WHERE stream_id = ? FOR UPDATE",
		Update: "UPDATE " + ref + " SET " + appliershared.EstablishedTablesColumn + " = ?, " +
			appliershared.DroppedTablesColumn + " = ? WHERE stream_id = ?",
	}
	return appliershared.NarrowEstablished(ctx, a.db, controlCfg, q, streamID, inScope, gone)
}

// ReadPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ReadPendingAddedTables(ctx context.Context, streamID string) ([]ir.PendingAddedTable, time.Time, error) {
	return appliershared.ReadPendingEntries(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.PendingAddedTablesColumn), streamID)
}

// ReadEstablishedRecord implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) ReadEstablishedRecord(ctx context.Context, streamID string) (ir.EstablishedRecord, error) {
	return appliershared.ReadEstablishedRecord(ctx, a.db, controlCfg,
		a.tableSetQueries(appliershared.EstablishedTablesColumn), a.tableSetQueries(appliershared.DroppedTablesColumn), streamID)
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
