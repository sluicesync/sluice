// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
)

// ErrUnforwardedSchemaChange classifies the refusal a CDC reader raises when
// the source changed a schema object its change stream does not carry — a
// constraint, a policy, row level security, a default — so the target (or a
// backup chain) would be left silently weaker than the source (GC-2). The
// Postgres pgoutput reader and the MySQL-family binlog reader both attach it
// to their UNFORWARDED-SCHEMA-CHANGE refusal via [WithMarker], leaving the
// operator-facing text exactly as the engine wrote it.
//
// Its text IS the grep-stable marker, so an error that wraps it with %w reads
// the same as one that names the marker in prose.
//
// The pipeline keys on it to make the refusal SURVIVE a process restart: the
// reader's door compares against a baseline it takes when it starts, so a
// fresh process would baseline the already-changed catalog and accept the
// change silently forever. A run that ends with this error records it
// ([UnforwardedRefusalStore] for `sync`, the stream-state file for `backup
// stream`), and the next start refuses again until the operator acknowledges
// it with `--accept-unforwarded-schema-change`.
var ErrUnforwardedSchemaChange = errors.New("UNFORWARDED-SCHEMA-CHANGE")

// UnforwardedRefusalStore is the optional [ChangeApplier] surface that
// persists an [ErrUnforwardedSchemaChange] refusal on the stream's row of the
// per-target `sluice_cdc_state` table, so a restarted `sync` refuses again
// instead of re-baselining past the change.
//
// Implemented by the Postgres applier (and the pgtrigger engine, which
// delegates its applier to it) and the MySQL-family applier — every
// [ChangeApplier] that keeps a control table. The engines without an applier
// (SQLite, D1, the trigger-CDC SQLite/D1 variants, flatfile, mydumper) refuse
// to be a sync target at OpenChangeApplier, so there is nothing to persist.
type UnforwardedRefusalStore interface {
	// RecordUnforwardedRefusal stores msg on streamID's row, replacing any
	// earlier record. The row must exist — the CDC anchor creates it before
	// any change stream opens, so a refusal always has one — and a missing
	// row is an error, never a silent no-op: the caller has to be able to
	// say the refusal was NOT persisted. Idempotent.
	RecordUnforwardedRefusal(ctx context.Context, streamID, msg string) error

	// ReadUnforwardedRefusal returns the recorded refusal, or ok=false when
	// there is none: no row, no control table, or a control table that
	// pre-dates the column (Record adds the column before it writes, so a
	// table without it cannot be holding a record).
	ReadUnforwardedRefusal(ctx context.Context, streamID string) (msg string, ok bool, err error)

	// ClearUnforwardedRefusal removes the record. Idempotent and tolerant of
	// a missing row or table.
	ClearUnforwardedRefusal(ctx context.Context, streamID string) error
}
