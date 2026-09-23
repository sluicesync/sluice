// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// markUnforwardedRefusal classifies the UNFORWARDED-SCHEMA-CHANGE refusal
// ([unforwardedChangeError]) with [ir.ErrUnforwardedSchemaChange] without
// touching its text, so the pipeline can persist it and refuse again after a
// process restart (the reader's baseline would otherwise already contain the
// change and accept it silently).
func markUnforwardedRefusal(err error) error {
	return ir.WithMarker(err, ir.ErrUnforwardedSchemaChange)
}

// ensureUnforwardedRefusalColumn adds sluice_cdc_state's unforwarded_refusal
// column when missing, on the same detect-then-ALTER path as the other
// additive columns (no DDL at all when present — the PlanetScale
// safe-migrations constraint). Called from [ensureControlTable] and again
// from [ChangeApplier.RecordUnforwardedRefusal]: a refusal must land even on
// a control table this binary never ensured (a `--schema-already-applied`
// run skips EnsureControlTable), because a refusal that fails to persist is
// accepted by the next restart.
func ensureUnforwardedRefusalColumn(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	return ensureCrossEngineParityColumn(ctx, db, controlKeyspace, appliershared.UnforwardedRefusalColumn, "TEXT NULL")
}

// RecordUnforwardedRefusal implements [ir.UnforwardedRefusalStore].
func (a *ChangeApplier) RecordUnforwardedRefusal(ctx context.Context, streamID, msg string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RecordUnforwardedRefusal: streamID is empty")
	}
	if err := ensureUnforwardedRefusalColumn(ctx, a.db, a.controlKeyspace); err != nil {
		return err
	}
	ref := controlTableRef(a.controlKeyspace, controlTableName)
	existsQ := "SELECT 1 FROM " + ref + " WHERE stream_id = ?"
	updateQ := "UPDATE " + ref + " SET " + appliershared.UnforwardedRefusalColumn + " = ? WHERE stream_id = ?"
	return appliershared.RecordUnforwardedRefusal(ctx, a.db, controlCfg, existsQ, updateQ, streamID, msg)
}

// ReadUnforwardedRefusal implements [ir.UnforwardedRefusalStore]. A control
// table without the column holds no record (see
// [appliershared.ReadUnforwardedRefusal]).
func (a *ChangeApplier) ReadUnforwardedRefusal(ctx context.Context, streamID string) (msg string, ok bool, err error) {
	q := "SELECT " + appliershared.UnforwardedRefusalColumn + " FROM " + controlTableRef(a.controlKeyspace, controlTableName) + " WHERE stream_id = ?"
	return appliershared.ReadUnforwardedRefusal(ctx, a.db, controlCfg, isMySQLUnknownColumnErr, q, streamID)
}

// ClearUnforwardedRefusal implements [ir.UnforwardedRefusalStore]. Tolerant
// of a missing row or table; a missing column means there is nothing to
// clear.
func (a *ChangeApplier) ClearUnforwardedRefusal(ctx context.Context, streamID string) error {
	q := "UPDATE " + controlTableRef(a.controlKeyspace, controlTableName) + " SET " + appliershared.UnforwardedRefusalColumn + " = NULL WHERE stream_id = ?"
	err := appliershared.TolerantExec(ctx, a.db, controlCfg, "clear unforwarded-schema-change refusal", q, streamID)
	if isMySQLUnknownColumnErr(err) {
		return nil
	}
	return err
}
