// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.PendingAddedTablesStore = (*ChangeApplier)(nil)

// pendingAddedTablesQueries renders the statements the shared helpers run
// against this applier's control table.
func (a *ChangeApplier) pendingAddedTablesQueries() appliershared.PendingAddedTablesQueries {
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	col := appliershared.PendingAddedTablesColumn
	return appliershared.PendingAddedTablesQueries{
		Read:            "SELECT " + col + " FROM " + ref + " WHERE stream_id = ?",
		ReadForUpdate:   "SELECT " + col + " FROM " + ref + " WHERE stream_id = ? FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = ? WHERE stream_id = ?",
		IsMissingColumn: isMySQLUnknownColumnErr,
	}
}

// RecordPendingAddedTable implements [ir.PendingAddedTablesStore]. The
// column is added here, on first use, rather than at every start (the
// detect-then-ALTER path issues no DDL when it is present): `sync start` on a
// control table this binary cannot ALTER — a PlanetScale safe-migrations
// branch, a pre-created table — keeps running exactly as before, and only an
// add-table there fails, loudly and before it changes anything, with the
// statement to ship.
func (a *ChangeApplier) RecordPendingAddedTable(ctx context.Context, streamID, table string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RecordPendingAddedTable: streamID is empty")
	}
	// LONGTEXT: the set is unbounded in principle, and a TEXT column refuses
	// a write past 64 KB.
	if err := ensureCrossEngineParityColumn(ctx, a.db, a.controlKeyspace, appliershared.PendingAddedTablesColumn, "LONGTEXT NULL"); err != nil {
		return err
	}
	return appliershared.UpdatePendingAddedTables(ctx, a.db, controlCfg, a.pendingAddedTablesQueries(), streamID, true,
		func(set map[string]struct{}) { set[table] = struct{}{} })
}

// ClearPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ClearPendingAddedTables(ctx context.Context, streamID string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	return appliershared.UpdatePendingAddedTables(ctx, a.db, controlCfg, a.pendingAddedTablesQueries(), streamID, false,
		func(set map[string]struct{}) {
			for _, t := range tables {
				delete(set, t)
			}
		})
}

// ReadPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ReadPendingAddedTables(ctx context.Context, streamID string) ([]string, error) {
	return appliershared.ReadPendingAddedTables(ctx, a.db, controlCfg, a.pendingAddedTablesQueries(), streamID)
}
