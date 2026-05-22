// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// ADR-0054 Shape A Phase 2 — live cross-shard DDL coordination.
//
// The pipeline's LeaseManager owns the state machine + heartbeat
// goroutine; engines own the per-engine SQL via the surface defined
// in this file. The engines plug in by implementing
// [ShardConsolidationLeaseStore] on their ChangeApplier (the pipeline
// probes via type-assertion, the same shape as
// [ShardColumnSetter] / [TableEmptyChecker]).
//
// Defined in the IR package so engines can implement it without
// importing pipeline (which would create a cycle). The pipeline-side
// state-machine + heartbeat live in
// `internal/pipeline/shard_consolidation_lease.go`.

// ShardConsolidationLeaseRow is the durable row shape engines
// exchange with the pipeline's LeaseManager. Engines own the SQL
// (one CREATE TABLE per engine; the columns are identical), the
// pipeline owns the semantics. See ADR-0054 §1.
//
// All time fields are wall-clock on the TARGET database (so the
// "expires_at > now()" check the engine performs is consistent with
// what other peer streams see). The HasX bool flags carry the NULL /
// NOT NULL distinction across the engine boundary without exposing
// engine-specific sql.NullTime.
type ShardConsolidationLeaseRow struct {
	// TargetTableFullName is the consolidated target table's full
	// name (schema-qualified for PG; bare table name for MySQL since
	// MySQL has a flat namespace). The row's PRIMARY KEY.
	TargetTableFullName string

	// LeaseHolderStreamID is the stream that currently holds (or
	// last held, on EXPIRED) the lease. Empty when the row was
	// just created and the holder field is still being written.
	LeaseHolderStreamID string

	// LeaseExpiresAt is the wall-clock at which the lease will be
	// considered EXPIRED if not extended. HasLeaseExpiresAt
	// distinguishes "not set yet" from "set to a past time".
	LeaseExpiresAt    time.Time
	HasLeaseExpiresAt bool

	// DDLText is the recorded DDL the lease-holder is applying.
	// Populated by [ShardConsolidationLeaseStore.RecordDDLText]
	// BEFORE the ALTER fires, so a takeover-stream's
	// probe-and-record can read what the prior holder intended.
	DDLText string

	// DDLChecksum is the SHA-256 hex of the normalized DDLText.
	// Populated only when the holder reaches the FinalizeLeaseApply
	// path (state APPLIED). Peer observers compare against their
	// own checksum and refuse loudly on mismatch.
	DDLChecksum string

	// AppliedSchemaVersion is the boundary version the holder
	// recorded. Zero before FinalizeLeaseApply.
	AppliedSchemaVersion int64

	// AppliedAt is the wall-clock at which the holder's apply
	// finalize-UPDATE committed. HasAppliedAt distinguishes the
	// NULL (not-yet-applied) case from a zero time-value.
	AppliedAt    time.Time
	HasAppliedAt bool
}

// ProbeOutcome classifies the takeover-stream's view of the target
// schema vs the prior lease-holder's recorded shape, per ADR-0054 §4.
// Used as the return type of the ADR-0054 Phase 2c per-shape probes.
// Lives in `ir` so engines can implement [ShardConsolidationProber]
// without importing pipeline (which would create a cycle with the
// integration-tagged tests in pipeline/).
type ProbeOutcome int

const (
	// ProbeOutcomeApplied — the target schema reflects the prior
	// holder's recorded change.
	ProbeOutcomeApplied ProbeOutcome = iota

	// ProbeOutcomeNotApplied — the target schema is unchanged; the
	// takeover-stream re-applies the DDL.
	ProbeOutcomeNotApplied

	// ProbeOutcomeInconsistent — the target schema is in a partial
	// state inconsistent with the recorded shape; refuse loudly.
	ProbeOutcomeInconsistent
)

// String renders a ProbeOutcome for logs and refusal messages.
func (o ProbeOutcome) String() string {
	switch o {
	case ProbeOutcomeApplied:
		return "applied"
	case ProbeOutcomeNotApplied:
		return "not-applied"
	case ProbeOutcomeInconsistent:
		return "inconsistent"
	}
	return "unknown"
}

// ShardConsolidationProber is the engine-side surface for the
// ADR-0054 Phase 2c takeover-stream's probe-and-record path. The
// pipeline calls one of these methods based on the classified shape;
// the engine queries its own information_schema / pg_catalog for the
// observable effect.
//
// Implemented on the ChangeApplier — the same type that holds
// [ShardConsolidationLeaseStore], so a single type-assertion at
// engagement time confirms both surfaces.
type ShardConsolidationProber interface {
	// ProbeAddColumn returns Applied when ALL named columns exist on
	// the target; NotApplied when NONE exist; Inconsistent on
	// partial state.
	ProbeAddColumn(ctx context.Context, table *Table, cols []*Column) (ProbeOutcome, error)

	// ProbeDropColumn inverts ProbeAddColumn (Applied when NONE
	// exist).
	ProbeDropColumn(ctx context.Context, table *Table, cols []*Column) (ProbeOutcome, error)

	// ProbeCreateIndex returns Applied when ALL named indexes exist;
	// NotApplied when NONE; Inconsistent on partial.
	ProbeCreateIndex(ctx context.Context, table *Table, indexes []*Index) (ProbeOutcome, error)

	// ProbeDropIndex inverts ProbeCreateIndex.
	ProbeDropIndex(ctx context.Context, table *Table, indexes []*Index) (ProbeOutcome, error)

	// ProbeAlterColumnType returns Applied when the column's IR type
	// on the target matches want.Type; NotApplied when it matches
	// the pre-DDL type; Inconsistent on absent column.
	ProbeAlterColumnType(ctx context.Context, table *Table, want *Column) (ProbeOutcome, error)

	// ProbeAlterColumnNullability returns Applied when the column's
	// Nullable on the target matches want.Nullable; NotApplied when
	// it matches the pre-state; Inconsistent on absent column.
	ProbeAlterColumnNullability(ctx context.Context, table *Table, want *Column) (ProbeOutcome, error)
}

// ShardConsolidationLeaseStore is the engine-private surface a
// [ChangeApplier] (or any handle to the target's control schema) can
// implement to drive the ADR-0054 lease primitive. Each method maps
// to one durable transition of the lease row.
//
// The pipeline package probes for this via type-assertion on the
// applier; engines that don't implement it inherit the no-live-
// coordination default (pre-ADR-0054 behaviour, which still works
// via the drained model — operators pass `--no-coordinate-live-ddl`
// to opt out of live coordination explicitly). The shipping MySQL
// and Postgres engines both implement it (ADR-0054 §1 / 3).
//
// Defined in the IR package rather than pipeline because engines
// must implement it without importing pipeline (which would create
// a cycle).
type ShardConsolidationLeaseStore interface {
	// TryAcquireLease conditionally INSERTs or UPDATEs the row keyed
	// by tableName so that lease_holder_stream_id = streamID and
	// lease_expires_at = expires. The conditional path fires only
	// when the row is absent OR its lease_expires_at <= now() AND
	// applied_at IS NULL. Returns acquired=true on success and
	// (acquired=false, current snapshot) on contention so the caller
	// can decide between wait and refuse-loudly.
	//
	// On takeover (acquired=true with a prior holder's ddl_text
	// still populated and applied_at NULL), the returned row's
	// DDLText carries the prior holder's recorded text so the
	// caller's probe-and-record path can read it.
	TryAcquireLease(
		ctx context.Context,
		tableName, streamID string,
		expires time.Time,
	) (acquired bool, current ShardConsolidationLeaseRow, err error)

	// HeartbeatLease extends lease_expires_at to expires iff the row
	// is still held by streamID and not yet finalized. Returns
	// extended=false when another stream has taken the lease over
	// (the holder must exit the apply path; the caller's ctx is
	// left untouched).
	HeartbeatLease(
		ctx context.Context,
		tableName, streamID string,
		expires time.Time,
	) (extended bool, err error)

	// RecordDDLText UPDATEs the row's ddl_text (and only ddl_text)
	// iff the row is held by streamID and not yet finalized. Called
	// by the holder BEFORE the ALTER fires so probe-and-record on
	// takeover has the recorded DDL to compare against. Returns
	// recorded=false when the lease has been taken over.
	RecordDDLText(
		ctx context.Context,
		tableName, streamID, ddlText string,
	) (recorded bool, err error)

	// FinalizeLeaseApply UPDATEs the row to applied_at = now,
	// ddl_text = ddlText, ddl_checksum = ddlChecksum,
	// applied_schema_version = appliedSchemaVersion, iff the row is
	// still held by streamID and not yet finalized. Returns
	// finalized=false when the lease has been taken over between
	// heartbeat and finalize.
	FinalizeLeaseApply(
		ctx context.Context,
		tableName, streamID, ddlText, ddlChecksum string,
		appliedSchemaVersion int64,
	) (finalized bool, err error)

	// ObserveLease reads the row for tableName, returning ok=false
	// when no row exists. The caller (peer stream / `sync status`)
	// classifies the row into the pipeline's LeaseState via the
	// pipeline's lease-row classifier. Tolerant of the table being
	// absent (returns ok=false, nil) so dry-run / pre-EnsureControl-
	// Table inspection paths don't error.
	ObserveLease(
		ctx context.Context,
		tableName string,
	) (row ShardConsolidationLeaseRow, ok bool, err error)
}
