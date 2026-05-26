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

	// AnchorPosition is the source-side CDC position at which this
	// boundary's DDL was observed (the SchemaSnapshot's Position).
	// Recorded by [ShardConsolidationLeaseStore.FinalizeLeaseApply] so
	// the v0.76.0 lease GC sweep (task #21) can compare it against
	// every stream's persisted position via the engine's
	// [PositionOrderer] and only delete rows every live stream has
	// already advanced past.
	//
	// Has{Anchor} distinguishes NULL (legacy v0.75.0 rows that pre-date
	// the additive `anchor_position` migration; GC defensively retains
	// them) from a freshly-written boundary.
	AnchorPosition Position
	HasAnchor      bool
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

	// ProbeRenameColumn returns Applied when newName is present on
	// the target AND oldName is absent AND (when want is non-nil) the
	// IR Type of newName matches want.Type — i.e. the column was
	// renamed as recorded, preserving its catalog type. NotApplied
	// when oldName is present AND newName is absent (the prior
	// holder crashed before issuing the RENAME). Inconsistent on any
	// other shape — both names absent, both names present, or
	// newName present with the wrong type. The type-match arm mirrors
	// the v0.76.0 ProbeAlterColumnType v2 silent-divergence catch:
	// without it, a drop+re-add of newName with a different type
	// could pass the existence-only check.
	ProbeRenameColumn(ctx context.Context, table *Table, oldName, newName string, want *Column) (ProbeOutcome, error)

	// ProbeAddCheck returns Applied when ALL named CHECK constraints
	// exist on the target; NotApplied when NONE exist; Inconsistent
	// on partial state. CHECK identity is by Name (the catalog
	// requires named CHECKs per ADR-0064 — unnamed CHECKs are
	// classifier-skipped upstream).
	ProbeAddCheck(ctx context.Context, table *Table, checks []*CheckConstraint) (ProbeOutcome, error)

	// ProbeDropCheck inverts ProbeAddCheck (Applied when NONE
	// exist).
	ProbeDropCheck(ctx context.Context, table *Table, checks []*CheckConstraint) (ProbeOutcome, error)

	// ProbeModifyCheck returns Applied when oldName is absent AND
	// newConstraint.Name is present on the target (and, when the
	// engine can read the catalog's constraint expression, it
	// matches newConstraint.Expr after the same dialect-normalization
	// the emit-path uses). NotApplied when oldName is present AND
	// newConstraint.Name is absent (the DROP half didn't fire).
	// Inconsistent on any other shape — both names absent, both
	// names present, newConstraint.Name present with the wrong
	// expression — mirroring the v0.76.0 ProbeAlterColumnType v2
	// silent-divergence catch (a DROP+ADD with a different expression
	// must not pass the existence check).
	ProbeModifyCheck(ctx context.Context, table *Table, oldName string, newConstraint *CheckConstraint) (ProbeOutcome, error)
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
	// applied_schema_version = appliedSchemaVersion, anchor_position
	// = anchor.Token + source_engine = anchor.Engine, iff the row is
	// still held by streamID and not yet finalized. Returns
	// finalized=false when the lease has been taken over between
	// heartbeat and finalize.
	//
	// anchor is the source-side CDC position at which the boundary's
	// DDL was observed. A zero-value anchor is permitted (e.g. unit
	// tests / engines without CDC) and stored as NULL — the v0.76.0
	// lease GC sweep (task #21) defensively retains NULL-anchor rows.
	FinalizeLeaseApply(
		ctx context.Context,
		tableName, streamID, ddlText, ddlChecksum string,
		appliedSchemaVersion int64,
		anchor Position,
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

// ShardConsolidationLeaseLister is the optional surface engines can
// implement to enumerate every row in the
// `sluice_shard_consolidation_lease` control table. Used by
// `sluice sync status` for the ADR-0054 §6 operator-visibility
// surface. The shipping PG and MySQL engines implement it.
//
// Sibling-tier to [ShardConsolidationLeaseStore] — distinct interface
// so engines that don't yet implement listing inherit the
// no-lease-listing default (status shows the existing per-stream rows
// but omits the consolidation_lease block). Tolerant of the table
// being absent (returns an empty slice, nil) so a status query
// against a fresh target doesn't error.
type ShardConsolidationLeaseLister interface {
	ListLeases(ctx context.Context) ([]ShardConsolidationLeaseRow, error)
}

// ShardConsolidationLeaseDeleter is the optional surface engines can
// implement to remove a single row from the
// `sluice_shard_consolidation_lease` control table by its primary key
// (TargetTableFullName). Used by the v0.76.0 lease GC sweep
// (`pipeline.sweepConsolidationLeases`, task #21) to garbage-collect
// APPLIED rows every live stream has already advanced past.
//
// Sibling-tier to [ShardConsolidationLeaseStore] / Lister — distinct
// interface so engines that don't yet implement deletion inherit the
// no-GC default (rows accumulate; operationally fine for v1, the v1
// follow-up this addresses). Tolerant of the table being absent and
// of the row being absent (both treated as "nothing to delete"; the
// sweeper's row-iteration is the authoritative existence check).
//
// The shipping PG and MySQL engines both implement it (ADR-0054
// v0.76.0 closure).
type ShardConsolidationLeaseDeleter interface {
	DeleteLease(ctx context.Context, tableName string) error
}
