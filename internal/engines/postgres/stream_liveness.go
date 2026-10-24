// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"log/slog"
	"strconv"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.StreamLivenessStore = (*ChangeApplier)(nil)

// streamLivenessColumn is sluice_cdc_state's liveness column
// ([appliershared.StreamLivenessColumn]). A fresh control table is created
// with it; an older one gets it from an attempt's start stamp, never from the
// ensure at start: a role that cannot ALTER a pre-created control table keeps
// starting, and the stamp's failure WARNs instead (a drained add-table then
// refuses, naming why).
var streamLivenessColumn = controlColumn{
	name: appliershared.StreamLivenessColumn,
	def:  appliershared.StreamLivenessColumn + " BIGINT NULL",
}

// streamStoppedLiteral is [appliershared.StreamStoppedMillis] as SQL.
var streamStoppedLiteral = strconv.Itoa(appliershared.StreamStoppedMillis)

// streamLivenessQueries renders the liveness statements. The clock is
// [pgNowMillisExpr] (clock_timestamp, not the transaction-frozen now()),
// read in the same statement as the column.
func (a *ChangeApplier) streamLivenessQueries() appliershared.StreamLivenessQueries {
	ref := controlTableRef(a.controlSchema)
	col := appliershared.StreamLivenessColumn
	return appliershared.StreamLivenessQueries{
		Read:            "SELECT " + col + ", " + pgNowMillisExpr + " FROM " + ref + " WHERE stream_id = $1",
		Stamp:           "UPDATE " + ref + " SET " + col + " = " + pgNowMillisExpr + " WHERE stream_id = $1",
		Stop:            "UPDATE " + ref + " SET " + col + " = " + streamStoppedLiteral + " WHERE stream_id = $1",
		IsMissingColumn: isUndefinedColumnErr,
	}
}

// stampsLiveness reports whether this applier's position writes stamp the
// liveness column (see [buildWritePositionSQL]).
func (a *ChangeApplier) stampsLiveness() bool { return a.livenessColumn.Load() }

// detectLivenessColumn records whether the control table has the liveness
// column. Detect only: the ALTER that adds it belongs to an attempt's start
// stamp, which a role that may not ALTER survives. A failed probe leaves
// position writes unstamped (they still work), which a drained add-table
// reads as no record and refuses on.
func (a *ChangeApplier) detectLivenessColumn(ctx context.Context) {
	missing, found, err := missingColumns(ctx, a.db, controlTableRef(a.controlSchema), []string{appliershared.StreamLivenessColumn})
	if err != nil {
		slog.WarnContext(ctx, "postgres: could not tell whether sluice_cdc_state has its liveness column; position writes "+
			"will not stamp it", slog.String("error", err.Error()))
		return
	}
	a.livenessColumn.Store(found && !missing[appliershared.StreamLivenessColumn])
}

// StampStreamAlive implements [ir.StreamLivenessStore]. It adds the column to
// a control table an older release created; a missing control table is left
// to the start's own ensure, which creates it with the column. Position writes
// start stamping only once the stamp's own UPDATE has run against the column,
// which proves it exists.
func (a *ChangeApplier) StampStreamAlive(ctx context.Context, streamID string) (bool, error) {
	present, err := relationPresent(ctx, a.db, controlTableRef(a.controlSchema))
	if err != nil || !present {
		return false, err
	}
	if err := cdcStateTable(a.controlSchema).ensureColumns(ctx, a.db, []controlColumn{streamLivenessColumn}); err != nil {
		return false, err
	}
	stamped, err := appliershared.StampStreamLiveness(ctx, a.db, controlCfg, a.streamLivenessQueries().Stamp, streamID)
	if err == nil {
		a.livenessColumn.Store(true)
	}
	return stamped, err
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
