// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.AddedTableReplayWindowStore = (*ChangeApplier)(nil)

// copyPositionsColumn is added_table_copy_positions' declaration, a
// table-set column like the others ([tableSetColumn]).
func copyPositionsColumn() controlColumn {
	return tableSetColumn(appliershared.AddedTableCopyPositionsColumn)
}

// ensureCopyPositionsColumn adds added_table_copy_positions on first use,
// detect-first, and refuses a column of any type but TEXT. It is
// deliberately NOT one of [tableSetColumnNames]: those are ensured by every
// table-set write, a warm resume's narrowing included, and adding it there
// would make every `sync start` on a control table this role cannot ALTER
// refuse for a column only `schema add-table` writes.
func (a *ChangeApplier) ensureCopyPositionsColumn(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("postgres: applier: added-table copy positions: streamID is empty")
	}
	t := cdcStateTable(a.controlSchema)
	col := copyPositionsColumn()
	if err := t.ensureColumns(ctx, a.db, []controlColumn{col}); err != nil {
		return fmt.Errorf("%w (to add it by hand: %s)", err, t.addColumnDDL(col))
	}
	var typ string
	err := a.db.QueryRowContext(ctx,
		"SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a "+
			"WHERE a.attrelid = pg_catalog.to_regclass($1) AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped",
		controlTableRef(a.controlSchema), col.name).Scan(&typ)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("postgres: applier: control-table column %s is missing after it was ensured", col.name)
	case err != nil:
		return fmt.Errorf("postgres: applier: read the type of control-table column %s: %w", col.name, err)
	case typ != "text":
		return fmt.Errorf("postgres: applier: control-table column %s is %s, not text, and a value longer than it holds "+
			"would be cut where the record cannot tell; change it with: ALTER TABLE %s ALTER COLUMN %s TYPE TEXT",
			col.name, typ, controlTableRef(a.controlSchema), col.name)
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
		return fmt.Errorf("postgres: applier: %w", err)
	}
	return a.writeCopyPositions(ctx, streamID, edit)
}

// ResetAddedTableReplayWindows implements [ir.AddedTableReplayWindowStore].
func (a *ChangeApplier) ResetAddedTableReplayWindows(ctx context.Context, streamID string, windows []ir.AddedTableReplayWindow) error {
	edit, err := appliershared.ReplayWindowsReset(windows)
	if err != nil {
		return fmt.Errorf("postgres: applier: %w", err)
	}
	return a.writeCopyPositions(ctx, streamID, edit)
}

// writeCopyPositions ensures the column, then applies edit under the row
// lock. The row must exist.
func (a *ChangeApplier) writeCopyPositions(ctx context.Context, streamID string, edit appliershared.TableSetEdit) error {
	if err := a.ensureCopyPositionsColumn(ctx, streamID); err != nil {
		return err
	}
	return classifyApplierError(appliershared.UpdateTableSet(ctx, a.db, controlCfg,
		a.tableSetQueries(appliershared.AddedTableCopyPositionsColumn), streamID, true, edit))
}

// ReadAddedTableReplayWindows implements [ir.AddedTableReplayWindowStore].
func (a *ChangeApplier) ReadAddedTableReplayWindows(ctx context.Context, streamID string) ([]ir.AddedTableReplayWindow, error) {
	windows, err := appliershared.ReadReplayWindows(ctx, a.db, controlCfg, a.tableSetQueries(appliershared.AddedTableCopyPositionsColumn), streamID)
	return windows, classifyApplierError(err)
}
