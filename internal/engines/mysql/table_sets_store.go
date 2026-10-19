// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

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

// tableSetColumnDef is each table-set column's declaration. LONGTEXT: a set
// is unbounded in principle, and TEXT refuses (or, under a relaxed
// --mysql-sql-mode, truncates) a write past 64 KB.
const tableSetColumnDef = "LONGTEXT NULL"

// mysqlNowMillisExpr is the server's clock in Unix milliseconds, with no
// time-zone conversion anywhere: UTC_TIMESTAMP(6) is UTC whatever the
// session's time_zone, and TIMESTAMPDIFF over two DATETIMEs is plain
// arithmetic. UNIX_TIMESTAMP(NOW(6)) was not DST-proof: NOW renders the
// session zone's local time, and in a fall-back hour that local time names
// two instants, of which UNIX_TIMESTAMP picks one — an hour off for the other
// (pinned by TestMySQLServerNowMillis_DSTFallBack). It rides the SELECT that
// reads the control row (P1): vtgate sends a table-less SELECT to a tablet of
// its choosing, which on a sharded keyspace need not hold the row.
const mysqlNowMillisExpr = "TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6)) DIV 1000"

// tableSetQueries renders the statements the shared table-set helpers run
// against one column of this applier's control table.
func (a *ChangeApplier) tableSetQueries(col string) appliershared.TableSetQueries {
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	read := "SELECT " + col + ", " + mysqlNowMillisExpr + " FROM " + ref + " WHERE stream_id = ?"
	return appliershared.TableSetQueries{
		Column:          col,
		Read:            read,
		ReadForUpdate:   read + " FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = ? WHERE stream_id = ?",
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

// tableSetColumnsDDL is the statement that adds all three columns, named in
// every refusal for a missing one.
func (a *ChangeApplier) tableSetColumnsDDL() string {
	adds := make([]string, len(tableSetColumnNames))
	for i, c := range tableSetColumnNames {
		adds[i] = "ADD COLUMN `" + c + "` " + tableSetColumnDef
	}
	return "ALTER TABLE " + controlTableRef(a.controlKeyspace, controlTableName) + " " + strings.Join(adds, ", ")
}

// ensureTableSetColumns adds the three table-set columns on first write
// rather than at every start (the detect-then-ALTER path issues no DDL when
// they are present): `sync start` on a control table this binary cannot
// ALTER — a PlanetScale safe-migrations branch, a DBA-pre-created table —
// keeps running as before, and only the write that needs the columns fails,
// loudly, naming the one statement that adds all three. Then their type is
// verified.
func (a *ChangeApplier) ensureTableSetColumns(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: table sets: streamID is empty")
	}
	for _, col := range tableSetColumnNames {
		if err := ensureCrossEngineParityColumn(ctx, a.db, a.controlKeyspace, col, tableSetColumnDef); err != nil {
			return fmt.Errorf("%w (the three table-set columns are added together; to add them by hand: %s)", err, a.tableSetColumnsDDL())
		}
	}
	return a.verifyTableSetColumnTypes(ctx)
}

// verifyTableSetColumnTypes refuses a table-set column of any type but
// LONGTEXT (L2): a DBA-created TEXT or VARCHAR column refuses a long set
// under sluice's strict sql_mode and, under a relaxed --mysql-sql-mode,
// truncates it — at a line boundary, which the codec cannot see. A column
// that does not exist yet is not judged. Verified once per applier.
func (a *ChangeApplier) verifyTableSetColumnTypes(ctx context.Context) error {
	if a.tableSetColumnsVerified.Load() {
		return nil
	}
	schemaRHS, schemaArgs := controlSchemaPredicate(a.controlKeyspace)
	q := "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = " + schemaRHS +
		" AND TABLE_NAME = ? AND COLUMN_NAME IN (?, ?, ?)"
	args := append(append([]any{}, schemaArgs...), controlTableName,
		tableSetColumnNames[0], tableSetColumnNames[1], tableSetColumnNames[2])
	rows, err := a.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("mysql: applier: read the table-set column types: %w", err)
	}
	defer func() { _ = rows.Close() }()
	found := 0
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return fmt.Errorf("mysql: applier: read the table-set column types: %w", err)
		}
		found++
		if !strings.EqualFold(typ, "longtext") {
			return fmt.Errorf("mysql: applier: control-table column %s is %s, not LONGTEXT, so a long record is refused "+
				"or, under a relaxed sql_mode, cut short where the record cannot tell; change it with: ALTER TABLE %s "+
				"MODIFY COLUMN `%s` %s", name, typ, controlTableRef(a.controlKeyspace, controlTableName), name, tableSetColumnDef)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mysql: applier: read the table-set column types: %w", err)
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
func (a *ChangeApplier) CompletePendingAddedTable(ctx context.Context, streamID, table, token, id string) error {
	if err := a.ensureTableSetColumns(ctx, streamID); err != nil {
		return err
	}
	return appliershared.CompletePendingAdd(ctx, a.db, controlCfg, a.pendingAddQueries(), streamID, table, token, id)
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
func (a *ChangeApplier) NarrowEstablishedRecord(ctx context.Context, streamID string, n ir.RecordNarrowing) ([]string, error) {
	if err := a.ensureTableSetColumns(ctx, streamID); err != nil {
		return nil, err
	}
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	q := appliershared.RecordQueries{
		ReadForUpdate: "SELECT " + appliershared.EstablishedTablesColumn + ", " + appliershared.DroppedTablesColumn +
			" FROM " + ref + " WHERE stream_id = ? FOR UPDATE",
		Update: "UPDATE " + ref + " SET " + appliershared.EstablishedTablesColumn + " = ?, " +
			appliershared.DroppedTablesColumn + " = ? WHERE stream_id = ?",
	}
	return appliershared.NarrowEstablished(ctx, a.db, controlCfg, q, streamID, n)
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
func (a *ChangeApplier) RecordEstablishedTables(ctx context.Context, streamID string, tables []string, ids map[string]string) error {
	return a.updateTableSet(ctx, streamID, appliershared.EstablishedTablesColumn, true, true, appliershared.EstablishedReplaceIDs(tables, ids))
}

// RecordDroppedTables implements [ir.EstablishedTablesStore].
func (a *ChangeApplier) RecordDroppedTables(ctx context.Context, streamID string, tables []string) error {
	return a.updateTableSet(ctx, streamID, appliershared.DroppedTablesColumn, true, true, appliershared.EstablishedReplace(tables))
}

var _ ir.LiveAddedTableRetirer = (*ChangeApplier)(nil)

// RetireLiveAddedTables implements [ir.LiveAddedTableRetirer]: the running
// stream's LIVE-ADDED-TABLE-EXCLUDED write ([appliershared.RetireLiveAddedTables])
// over this engine's comma-list live_added_tables.
func (a *ChangeApplier) RetireLiveAddedTables(ctx context.Context, streamID string, tables []string) (string, error) {
	if err := a.ensureTableSetColumns(ctx, streamID); err != nil {
		return "", err
	}
	if err := ensureLiveAddedTablesColumn(ctx, a.db, a.controlKeyspace); err != nil {
		return "", err
	}
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	q := appliershared.RetireQueries{
		ReadForUpdate: "SELECT " + appliershared.PendingAddedTablesColumn + ", " + appliershared.EstablishedTablesColumn + ", " +
			appliershared.DroppedTablesColumn + ", live_added_tables FROM " + ref + " WHERE stream_id = ? FOR UPDATE",
		Update: "UPDATE " + ref + " SET " + appliershared.PendingAddedTablesColumn + " = ?, " + appliershared.EstablishedTablesColumn +
			" = ?, " + appliershared.DroppedTablesColumn + " = ?, live_added_tables = ? WHERE stream_id = ?",
	}
	codec := appliershared.LiveAddedCodec{
		Decode: func(stored string) ([]string, error) { return parseLiveAddedTables(stored), nil },
		Encode: func(tables []string) any {
			if len(tables) == 0 {
				return nil
			}
			return strings.Join(tables, ",")
		},
	}
	return appliershared.RetireLiveAddedTables(ctx, a.db, controlCfg, q, codec, streamID, tables)
}
