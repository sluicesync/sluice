// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.PendingAddedTablesStore = (*ChangeApplier)(nil)

// pendingAddedTablesColumn is sluice_cdc_state's add-table pending-set
// column. Deliberately NOT in [cdcStateTable]'s column list: that list is
// ensured at every start, and an ALTER there fails `sync start` for a role
// that cannot alter a pre-created control table. It is ensured on the first
// Record instead (see [ChangeApplier.RecordPendingAddedTable]).
var pendingAddedTablesColumn = controlColumn{
	name: appliershared.PendingAddedTablesColumn,
	def:  appliershared.PendingAddedTablesColumn + " TEXT NULL",
}

// pendingAddedTablesQueries renders the statements the shared helpers run
// against this applier's control table.
func (a *ChangeApplier) pendingAddedTablesQueries() appliershared.PendingAddedTablesQueries {
	ref := controlTableRef(a.controlSchema)
	col := appliershared.PendingAddedTablesColumn
	return appliershared.PendingAddedTablesQueries{
		Read:            "SELECT " + col + " FROM " + ref + " WHERE stream_id = $1",
		ReadForUpdate:   "SELECT " + col + " FROM " + ref + " WHERE stream_id = $1 FOR UPDATE",
		Update:          "UPDATE " + ref + " SET " + col + " = $1 WHERE stream_id = $2",
		IsMissingColumn: isUndefinedColumnErr,
	}
}

// RecordPendingAddedTable implements [ir.PendingAddedTablesStore]. Detects
// the column first and adds it only when missing ([controlTable.ensureColumns]),
// so a DML-only role on a control table that already carries it passes.
func (a *ChangeApplier) RecordPendingAddedTable(ctx context.Context, streamID, table string) error {
	if streamID == "" {
		return errors.New("postgres: applier: RecordPendingAddedTable: streamID is empty")
	}
	if err := cdcStateTable(a.controlSchema).ensureColumns(ctx, a.db, []controlColumn{pendingAddedTablesColumn}); err != nil {
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
	return classifyApplierError(appliershared.UpdatePendingAddedTables(ctx, a.db, controlCfg, a.pendingAddedTablesQueries(), streamID, false,
		func(set map[string]struct{}) {
			for _, t := range tables {
				delete(set, t)
			}
		}))
}

// ReadPendingAddedTables implements [ir.PendingAddedTablesStore].
func (a *ChangeApplier) ReadPendingAddedTables(ctx context.Context, streamID string) ([]string, error) {
	tables, err := appliershared.ReadPendingAddedTables(ctx, a.db, controlCfg, a.pendingAddedTablesQueries(), streamID)
	return tables, classifyApplierError(err)
}
