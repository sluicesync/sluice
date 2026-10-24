// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.AddedTableReplayWindowStore = (*ChangeApplier)(nil)

// ensureCopyPositionsColumn adds added_table_copy_positions on first use,
// detect-first (no DDL when it exists), and refuses a column of any type but
// LONGTEXT: a window holds a VGTID, which has no length bound sluice
// controls. It is deliberately NOT one of [tableSetColumnNames]: those are
// ensured by every table-set write, a warm resume's narrowing included, and
// adding it there would make every `sync start` on a control table this
// binary cannot ALTER refuse for a column only `schema add-table` writes.
func (a *ChangeApplier) ensureCopyPositionsColumn(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: added-table copy positions: streamID is empty")
	}
	col := appliershared.AddedTableCopyPositionsColumn
	if err := ensureCrossEngineParityColumn(ctx, a.db, a.controlKeyspace, appliershared.AddedTableCopyPositionsColumn, tableSetColumnDef); err != nil {
		return fmt.Errorf("%w (to add it by hand: ALTER TABLE %s ADD COLUMN `%s` %s)", err,
			controlTableRef(a.controlKeyspace, controlTableName), col, tableSetColumnDef)
	}
	schemaRHS, schemaArgs := controlSchemaPredicate(a.controlKeyspace)
	var typ string
	err := a.db.QueryRowContext(ctx, "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = "+schemaRHS+
		" AND TABLE_NAME = ? AND COLUMN_NAME = ?", append(append([]any{}, schemaArgs...), controlTableName, col)...).Scan(&typ)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("mysql: applier: control-table column %s is missing after it was ensured", col)
	case err != nil:
		return fmt.Errorf("mysql: applier: read the type of control-table column %s: %w", col, err)
	case !strings.EqualFold(typ, "longtext"):
		return fmt.Errorf("mysql: applier: control-table column %s is %s, not LONGTEXT, so a long position is refused or, "+
			"under a relaxed sql_mode, cut short; change it with: ALTER TABLE %s MODIFY COLUMN `%s` %s",
			col, typ, controlTableRef(a.controlKeyspace, controlTableName), col, tableSetColumnDef)
	}
	return nil
}

// PrepareAddedTableReplayWindows implements [ir.AddedTableReplayWindowStore].
func (a *ChangeApplier) PrepareAddedTableReplayWindows(ctx context.Context, streamID string) error {
	return a.ensureCopyPositionsColumn(ctx, streamID)
}

// RecordAddedTableReplayWindows implements [ir.AddedTableReplayWindowStore].
func (a *ChangeApplier) RecordAddedTableReplayWindows(ctx context.Context, streamID string, windows []ir.AddedTableReplayWindow) error {
	edit, err := appliershared.ReplayWindowsRecord(windows)
	if err != nil {
		return fmt.Errorf("mysql: applier: %w", err)
	}
	return a.writeCopyPositions(ctx, streamID, edit)
}

// ResetAddedTableReplayWindows implements [ir.AddedTableReplayWindowStore].
func (a *ChangeApplier) ResetAddedTableReplayWindows(ctx context.Context, streamID string, windows []ir.AddedTableReplayWindow) error {
	edit, err := appliershared.ReplayWindowsReset(windows)
	if err != nil {
		return fmt.Errorf("mysql: applier: %w", err)
	}
	return a.writeCopyPositions(ctx, streamID, edit)
}

// writeCopyPositions ensures the column, then applies edit under the row
// lock. The row must exist.
func (a *ChangeApplier) writeCopyPositions(ctx context.Context, streamID string, edit appliershared.TableSetEdit) error {
	if err := a.ensureCopyPositionsColumn(ctx, streamID); err != nil {
		return err
	}
	return appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.AddedTableCopyPositionsColumn), streamID, true, edit)
}

// ReadAddedTableReplayWindows implements [ir.AddedTableReplayWindowStore].
func (a *ChangeApplier) ReadAddedTableReplayWindows(ctx context.Context, streamID string) ([]ir.AddedTableReplayWindow, error) {
	return appliershared.ReadReplayWindows(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.AddedTableCopyPositionsColumn), streamID)
}
