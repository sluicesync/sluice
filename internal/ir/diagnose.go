// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// DiagnoseSnapshot is the engine-side payload the `sluice diagnose`
// operator-bundle assembler embeds in its ZIP. Engines populate the
// fields they can; missing data is signalled by zero values rather
// than errors so a partially-degraded source / target still produces
// a useful bundle (operators filing GH issues against a half-broken
// stream is exactly the case the feature exists to serve).
//
// **Engine-neutrality.** The bundle assembler can't import specific
// engine packages (the orchestrator's contract per CLAUDE.md's
// "Contain Postgres complexity" tenet); engine-specific signals — PG
// slot state, MySQL master-status — surface here as opaque JSON the
// assembler embeds verbatim. The structured fields below are the
// engine-neutral surface every engine SHOULD populate when it can.
//
// **Privacy contract.** This snapshot is unconditionally embedded at
// the bundle's `standard` privacy level. Engines MUST NOT include
// row-level data or credential material in any field; this is server-
// state diagnostics, not a data dump.
type DiagnoseSnapshot struct {
	// EngineName is the engine's registered name (per Engine.Name()).
	// Always populated.
	EngineName string

	// EngineVersion is the database server's reported version string
	// — `SELECT version()` on PG, `SELECT VERSION()` on MySQL. Empty
	// when the engine can't probe (operator passed a malformed DSN
	// and Open() failed; we still want a bundle with whatever else
	// we can capture).
	EngineVersion string

	// DSNFingerprint is the host:port + database-name locator from
	// the DSN, with userinfo and credentials stripped. Mirrors the
	// shape `internal/redact.redactDSNForAudit` produces for the
	// `--keyset-source db:DSN` audit-log line. Never carries password
	// material; safe to ship in an operator bundle at `standard`
	// privacy.
	DSNFingerprint string

	// EngineState is an engine-specific JSON blob the bundle
	// assembler embeds verbatim. PG populates with slot state
	// (pg_replication_slots row for the active slot, wal_status, the
	// slot's confirmed_flush_lsn relative to pg_current_wal_lsn).
	// MySQL populates with master-status (binlog file + position,
	// GTID executed set, gtid_mode). Empty when the engine has no
	// server-state diagnostics to surface (or can't reach the
	// server).
	EngineState []byte

	// Capabilities is the engine's declared capability set. Embedded
	// in the bundle so a future schema-translation regression filed
	// in an operator bundle is reproducible against the same
	// declared shape. Always populated.
	Capabilities Capabilities
}

// DiagnoseProber is the optional engine-side surface the `sluice
// diagnose` bundle assembler probes via type-assertion (the same
// shape as [HealthReporter]). Engines that don't implement it surface
// in the bundle as "diagnose-probe-not-supported" — the assembler
// continues with whatever it could collect rather than refusing
// (loud-failure does NOT apply to diagnose: best-effort collection is
// the whole point; the bundle is the diagnostic, refusing to write
// one because one probe failed would be self-defeating).
//
// Lives on SchemaReader (probed at OpenSchemaReader time) rather than
// ChangeApplier so the operator can request a bundle against EITHER
// side of a stream — sometimes the failure is source-side (slot
// missing, binlog rotation lag) and the target is unreachable; the
// CLI accepts --source-driver / --source independently from --target.
type DiagnoseProber interface {
	// DiagnoseBundle returns a structured snapshot of the engine's
	// current server-state diagnostics. The streamID argument scopes
	// the probe (PG looks up the slot for this stream; MySQL pulls
	// master-status that overlaps the binlog the stream is reading
	// from). Engines without a stream concept (the engine-neutral
	// baseline) MAY ignore the streamID.
	//
	// Errors here are surfaced in the bundle as a per-section reason
	// string rather than propagated — see the package comment on
	// best-effort collection.
	DiagnoseBundle(ctx context.Context, streamID string) (DiagnoseSnapshot, error)
}

// SchemaHistoryReader is the optional surface a [ChangeApplier] can
// implement to enumerate `sluice_cdc_schema_history` rows for the
// `sluice diagnose` bundle. Sibling-tier to [SchemaHistoryCompactor]:
// the compactor writes (deletes), the reader reads, both engine-
// private storage.
//
// **Why a separate interface.** The compactor takes a position floor
// and returns a delete-count; the reader takes a stream-id and
// returns row payloads. Splitting keeps each method's contract
// focused and lets engines implement either independently. The
// engine implementations land on the same per-engine storage layer
// (`internal/engines/postgres/schema_history.go` and the MySQL
// equivalent).
//
// **Bounded result set.** The limit argument is the maximum number of
// most-recent rows to return; engines MUST honour it to keep diagnose-
// bundle assembly bounded (a long-running stream can accumulate
// hundreds of boundaries even with ADR-0049 Chunk-B's true-delta
// gate). The standard `basic`-level limit is 100; the assembler passes
// the operator-facing constant.
//
// Tolerant of the schema-history table being absent (returns an empty
// slice and nil — operators filing diagnose bundles against pre-
// ADR-0049 streams or fresh targets should still get a bundle).
type SchemaHistoryReader interface {
	// ListSchemaHistory returns up to limit most-recent retained
	// schema-history rows for streamID. Order is "most recent first"
	// — engine implementations sort by inserted_at DESC (or
	// equivalent) and truncate to limit. limit <= 0 falls back to a
	// safe default (100) rather than returning unbounded rows.
	ListSchemaHistory(ctx context.Context, streamID string, limit int) ([]RetainedSchemaVersionRow, error)
}

// RetainedSchemaVersionRow is the rendered shape one
// `sluice_cdc_schema_history` row takes in the diagnose bundle. Wider
// than [RetainedSchemaVersion] (the resolve-time shape) because
// operators inspecting a bundle want the natural-tuple identity
// (schema, table) visible without re-deriving from VersionKey.
//
// **Privacy contract.** No row data; the snapshot serialised in
// TableJSON is the IR schema (column names, types, constraints) the
// engine produced — that's schema metadata, not data, and is safe at
// `basic` privacy.
type RetainedSchemaVersionRow struct {
	// VersionKey is the per-row surrogate primary key (see
	// [SchemaVersionKey]). Useful for cross-referencing with engine-
	// side logs.
	VersionKey string

	// StreamID is the row's stream-id (matches the request's
	// streamID, but echoed back for completeness).
	StreamID string

	// SchemaName + TableName are the natural-tuple identity for the
	// affected table.
	SchemaName string
	TableName  string

	// AnchorPosition is the source position the DDL boundary was
	// observed at, engine-opaque per ADR-0007.
	AnchorPosition string

	// TableJSON is the IR-schema snapshot the engine persisted. The
	// bundle assembler embeds it verbatim.
	TableJSON []byte
}
