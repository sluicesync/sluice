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

// markUnforwardedRefusal classifies the UNFORWARDED-SCHEMA-CHANGE refusal
// ([unforwardedChangeError]) with [ir.ErrUnforwardedSchemaChange] without
// touching its text, so the pipeline can persist it and refuse again after a
// process restart (the reader's baseline would otherwise already contain the
// change and accept it silently).
func markUnforwardedRefusal(err error) error {
	return ir.WithMarker(err, ir.ErrUnforwardedSchemaChange)
}

// ensureUnforwardedRefusalColumn adds sluice_cdc_state's unforwarded_refusal
// column when missing. Called from [ensureControlTable] and again from
// [ChangeApplier.RecordUnforwardedRefusal] — a refusal must land even on a
// control table this binary never ensured (a `--schema-already-applied` run
// skips EnsureControlTable), because a refusal that fails to persist is
// accepted by the next restart.
func ensureUnforwardedRefusalColumn(ctx context.Context, db *sql.DB, schema string) error {
	alter := "ALTER TABLE " + controlTableRef(schema) + " ADD COLUMN IF NOT EXISTS " + appliershared.UnforwardedRefusalColumn + " TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure control table: add %s: %w", appliershared.UnforwardedRefusalColumn, err)
	}
	return nil
}

// RecordUnforwardedRefusal implements [ir.UnforwardedRefusalStore].
func (a *ChangeApplier) RecordUnforwardedRefusal(ctx context.Context, streamID, msg string) error {
	if streamID == "" {
		return errors.New("postgres: applier: RecordUnforwardedRefusal: streamID is empty")
	}
	if err := ensureUnforwardedRefusalColumn(ctx, a.db, a.controlSchema); err != nil {
		return err
	}
	q := "UPDATE " + controlTableRef(a.controlSchema) + " SET " + appliershared.UnforwardedRefusalColumn + " = $1 WHERE stream_id = $2"
	return appliershared.RecordUnforwardedRefusal(ctx, a.db, controlCfg, "", q, streamID, msg)
}

// ReadUnforwardedRefusal implements [ir.UnforwardedRefusalStore]. A control
// table without the column holds no record (see
// [appliershared.ReadUnforwardedRefusal]).
func (a *ChangeApplier) ReadUnforwardedRefusal(ctx context.Context, streamID string) (msg string, ok bool, err error) {
	q := "SELECT " + appliershared.UnforwardedRefusalColumn + " FROM " + controlTableRef(a.controlSchema) + " WHERE stream_id = $1"
	msg, ok, err = appliershared.ReadUnforwardedRefusal(ctx, a.db, controlCfg, isUndefinedColumnErr, q, streamID)
	return msg, ok, classifyApplierError(err)
}

// ClearUnforwardedRefusal implements [ir.UnforwardedRefusalStore]. Tolerant
// of a missing row or table; a missing column means there is nothing to
// clear.
func (a *ChangeApplier) ClearUnforwardedRefusal(ctx context.Context, streamID string) error {
	q := "UPDATE " + controlTableRef(a.controlSchema) + " SET " + appliershared.UnforwardedRefusalColumn + " = NULL WHERE stream_id = $1"
	err := appliershared.TolerantExec(ctx, a.db, controlCfg, "clear unforwarded-schema-change refusal", q, streamID)
	if isUndefinedColumnErr(err) {
		return nil
	}
	return err
}
