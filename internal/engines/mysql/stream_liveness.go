// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"log/slog"
	"strconv"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.StreamLivenessStore = (*ChangeApplier)(nil)

// streamLivenessColumnDef is sluice_cdc_state's liveness column
// ([appliershared.StreamLivenessColumn]). A fresh control table is created
// with it; an older one gets it from an attempt's start stamp, not from
// [ensureControlTable]: a stream whose role cannot ALTER the table (a
// PlanetScale branch with safe migrations, say) keeps starting, and the
// stamp's failure WARNs instead (a drained add-table then refuses, naming
// why).
const streamLivenessColumnDef = "BIGINT NULL"

// streamStoppedLiteral is [appliershared.StreamStoppedMillis] as SQL.
var streamStoppedLiteral = strconv.Itoa(appliershared.StreamStoppedMillis)

// streamLivenessQueries renders the liveness statements. The clock is
// [mysqlNowMillisExpr], evaluated in the statement that reads or writes the
// row, so on a sharded keyspace it is the clock of the tablet holding it.
//
// Every stamp names `updated_at = updated_at`: updated_at is ON UPDATE
// CURRENT_TIMESTAMP, and an UPDATE that assigns a column its own value
// suppresses the automatic bump, so a stamp does not make the row look
// freshly written to anything that reads updated_at.
func (a *ChangeApplier) streamLivenessQueries() appliershared.StreamLivenessQueries {
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	col := appliershared.StreamLivenessColumn
	set := "UPDATE " + ref + " SET " + col + " = "
	keep := ", updated_at = updated_at WHERE stream_id = ?"
	return appliershared.StreamLivenessQueries{
		Read:            "SELECT " + col + ", " + mysqlNowMillisExpr + " FROM " + ref + " WHERE stream_id = ?",
		Stamp:           set + mysqlNowMillisExpr + keep,
		Stop:            set + streamStoppedLiteral + keep,
		StampAt:         set + "?" + keep,
		RestoreStop:     set + streamStoppedLiteral + keep + " AND " + col + " = ?",
		IsMissingColumn: isMySQLUnknownColumnErr,
	}
}

// stampsLiveness reports whether this applier's position writes stamp the
// liveness column (see [writePositionUpsertSQL]).
func (a *ChangeApplier) stampsLiveness() bool { return a.livenessColumn.Load() }

// detectLivenessColumn records whether the control table has the liveness
// column. Detect only: the ALTER that adds it belongs to an attempt's start
// stamp, which a role that may not ALTER survives. A failed probe leaves
// position writes unstamped (they still work), which a drained add-table
// reads as no record and refuses on.
func (a *ChangeApplier) detectLivenessColumn(ctx context.Context) {
	schemaRHS, schemaArgs := controlSchemaPredicate(a.controlKeyspace)
	q := "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	args := append(append([]any{}, schemaArgs...), controlTableName, appliershared.StreamLivenessColumn)
	var n int
	if err := a.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		slog.WarnContext(ctx, "mysql: could not tell whether sluice_cdc_state has its liveness column; position writes "+
			"will not stamp it", slog.String("error", err.Error()))
		return
	}
	a.livenessColumn.Store(n > 0)
}

// StampStreamAlive implements [ir.StreamLivenessStore]. It adds the column to
// a control table an older release created; a missing control table is left
// to the start's own ensure, which creates it with the column.
//
// Position writes start stamping only once the stamp's own UPDATE has run
// against the column, which proves it exists — not when the ALTER returned:
// under a Vitess ddl_strategy that queues the ALTER as an online migration,
// the column does not exist yet when the ALTER returns, and a position write
// naming it would fail every apply until the migration landed.
//
// It first re-detects the column (detect only, from information_schema, so
// a queued ALTER is not mistaken for a column), so position writes stamp even
// in a run that skipped the ensure (--schema-already-applied) or whose stamp
// then fails; and it stamps under a row lock that reads the value it
// replaces ([appliershared.StampStreamAliveOwned]).
func (a *ChangeApplier) StampStreamAlive(ctx context.Context, streamID string) (ir.StreamStamp, error) {
	a.detectLivenessColumn(ctx)
	present, err := controlTableExists(ctx, a.db, a.controlKeyspace, controlTableName)
	if err != nil || !present {
		return ir.StreamStamp{}, err
	}
	if err := ensureCrossEngineParityColumn(ctx, a.db, a.controlKeyspace, appliershared.StreamLivenessColumn, streamLivenessColumnDef); err != nil {
		return ir.StreamStamp{}, err
	}
	stamp, err := appliershared.StampStreamAliveOwned(ctx, a.db, controlCfg, a.streamLivenessQueries(), streamID)
	if err == nil {
		a.livenessColumn.Store(true)
	}
	return stamp, err
}

// RestoreStreamStopped implements [ir.StreamLivenessStore].
func (a *ChangeApplier) RestoreStreamStopped(ctx context.Context, streamID string, stamp ir.StreamStamp) (bool, error) {
	return appliershared.RestoreStreamStopped(ctx, a.db, controlCfg, a.streamLivenessQueries(), streamID, stamp)
}

// RecordStreamStopped implements [ir.StreamLivenessStore].
func (a *ChangeApplier) RecordStreamStopped(ctx context.Context, streamID string) error {
	_, err := appliershared.StampStreamLiveness(ctx, a.db, controlCfg, a.streamLivenessQueries().Stop, streamID)
	return err
}

// ReadStreamLiveness implements [ir.StreamLivenessStore].
func (a *ChangeApplier) ReadStreamLiveness(ctx context.Context, streamID string) (ir.StreamLiveness, error) {
	l, err := appliershared.ReadStreamLiveness(ctx, a.db, controlCfg, a.streamLivenessQueries(), streamID)
	return l, classifyApplierError(err)
}
