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
// column when missing. [ensureControlTable] adds it with the rest of
// [cdcStateTable]'s columns; this entry point is for
// [ChangeApplier.RecordUnforwardedRefusal] and
// [ChangeApplier.EnsureUnforwardedRefusalStorage] — a refusal must land even on a
// control table this binary never ensured (a `--schema-already-applied` run
// skips EnsureControlTable), because a refusal that fails to persist is
// accepted by the next restart.
//
// Detect FIRST ([controlTable.ensureColumns]). PostgreSQL checks table
// ownership before it evaluates IF NOT EXISTS, so an unconditional ALTER
// fails with "must be owner of table" for a DML-only role even when the
// column is already there (measured on PG 16) — the
// `--schema-already-applied` setup, with the control table pre-created by
// another role, is exactly that role. An ALTER that failed here meant the
// refusal was never recorded and the next restart accepted the change
// (2026-09-23 pre-tag review, second pass, finding 1). A control table that
// does not exist at all is refused by name (GC-40 (a)/(b)), not with the
// ALTER's bare "relation … does not exist".
func ensureUnforwardedRefusalColumn(ctx context.Context, db *sql.DB, schema string) error {
	return cdcStateTable(schema).ensureColumns(ctx, db, []controlColumn{unforwardedRefusalColumn})
}

// unforwardedRefusalColumn is sluice_cdc_state's persisted-refusal column.
var unforwardedRefusalColumn = controlColumn{
	name: appliershared.UnforwardedRefusalColumn,
	def:  appliershared.UnforwardedRefusalColumn + " TEXT NULL",
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

// EnsureUnforwardedRefusalStorage implements [ir.UnforwardedRefusalStore]:
// detect-then-ALTER, so a DML-only role on a control table that already
// carries the column passes, and one that cannot add a missing column fails
// the start loudly instead of failing to record a refusal later.
func (a *ChangeApplier) EnsureUnforwardedRefusalStorage(ctx context.Context) error {
	if err := ensureUnforwardedRefusalColumn(ctx, a.db, a.controlSchema); err != nil {
		return err
	}
	// A column that exists but this role cannot UPDATE (a column-level grant
	// that predates it, say) fails the same way later, when a refusal needs
	// recording. PostgreSQL checks UPDATE privilege at plan time, so a no-row
	// UPDATE proves it and writes nothing (third-pass review, finding 5).
	probe := "UPDATE " + controlTableRef(a.controlSchema) + " SET " + appliershared.UnforwardedRefusalColumn + " = " + appliershared.UnforwardedRefusalColumn + " WHERE false"
	if _, err := a.db.ExecContext(ctx, probe); err != nil {
		return fmt.Errorf("postgres: the unforwarded_refusal column is not writable by this role: %w", err)
	}
	return nil
}
