// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// TargetMetricsHistoryStore is the OPTIONAL, engine-neutral surface a
// [ChangeApplier] can implement to persist a bounded ROLLING HISTORY of
// the polled target-health snapshots ([TargetHealthSnapshot]) in a
// sluice metadata table on the TARGET, so `sluice diagnose` (or a plain
// SELECT) can surface the recent TREND — is CPU climbing? did storage
// just step? — without the operator scripting the provider's metrics API
// themselves (ADR-0107 item 35).
//
// It is probed by type-assertion exactly like [SchemaHistoryReader]: the
// pipeline recorder sidecar asserts it on the opened ChangeApplier and
// no-op-degrades when the engine doesn't implement it. Engine-neutral —
// the PlanetScale telemetry provider FILLS the rows via the recorder, but
// the table contract here is generic, so a future provider for another
// platform could populate the same shape with no change to this seam (no
// engine imports leak into core ir).
//
// ADVISORY / OBSERVABILITY ONLY, exactly like the [TargetTelemetry] seam
// it records from. Nothing here participates in the value path or the
// exactly-once frontier: a recorded sample can never advance a position,
// drop a change, or stall the stream, and EVERY error from these methods
// is swallowed at WARN by the recorder (the history is a diagnostic, not
// a correctness surface). The store is BOUNDED — the recorder prunes on a
// retention window so the table never grows without limit.
//
// **Honesty contract (the *Known fields).** A [TargetHealthSnapshot]
// distinguishes "metric observed as 0" from "metric unobserved" via a
// companion *Known flag; this store MUST preserve that distinction. An
// unobserved value is persisted as SQL NULL (never 0), and on read the
// *Known flag is reconstructed as (value IS NOT NULL) — so a recipient
// inspecting the history never mistakes "unobserved" for "idle", mirroring
// the (snap, ok) honesty the rest of the telemetry path keeps.
type TargetMetricsHistoryStore interface {
	// EnsureTargetMetricsHistory creates the bounded metadata table if it
	// doesn't exist (CREATE TABLE IF NOT EXISTS, additive — it never
	// touches sluice_cdc_state or any user data). Idempotent. Called once
	// by the recorder at startup; an error here disables the recorder for
	// the run (logged at WARN, never fatal).
	EnsureTargetMetricsHistory(ctx context.Context) error

	// RecordTargetMetricsSample INSERTs one history row. Fields whose
	// companion *Known flag is false are written as SQL NULL (the
	// "unobserved" encoding), never 0.
	RecordTargetMetricsSample(ctx context.Context, s TargetMetricsSample) error

	// PruneTargetMetricsHistory DELETEs every row older than retain
	// (sampled_at < now-retain), keeping the table bounded. Called
	// periodically by the recorder.
	PruneTargetMetricsHistory(ctx context.Context, retain time.Duration) error

	// ListTargetMetricsHistory returns up to limit most-recent rows for
	// streamID, ordered by sampled_at DESC, reconstructing each row's
	// *Known flag from the NULLness of the stored value. Tolerant of the
	// table being absent (returns an empty slice and nil), so a diagnose
	// bundle against a target that never wired telemetry still assembles.
	// limit <= 0 falls back to a safe default rather than returning an
	// unbounded result.
	ListTargetMetricsHistory(ctx context.Context, streamID string, limit int) ([]TargetMetricsHistoryRow, error)
}

// TargetMetricsSample is one point-in-time telemetry sample destined for
// the rolling-history table. It carries the same distilled CPU / memory /
// storage / lag / connection fields as [TargetHealthSnapshot] (each gated
// by its *Known flag), plus the recording context (stream + the optional
// database/branch the provider was filtered to). Engine-neutral — no
// engine-specific row identity leaks in (the engine store owns the
// surrogate PK).
type TargetMetricsSample struct {
	// StreamID scopes the sample to the recording stream. Database and
	// Branch are the optional provider-filter labels; the recorder may
	// leave them empty (the [TargetTelemetry.Sample] surface does not
	// expose them), in which case they persist as empty strings.
	StreamID string
	Database string
	Branch   string

	// SampledAt is the wall-clock time of the underlying poll (copied
	// from the snapshot). The recorder dedupes on it: the source only
	// updates ~once a minute, so a tick that re-reads the same SampledAt
	// is NOT re-persisted.
	SampledAt time.Time

	// CPUUtil / MemUtil are utilisation fractions in [0, 1]; *Known false
	// ⇒ persisted as NULL.
	CPUUtil  float64
	CPUKnown bool
	MemUtil  float64
	MemKnown bool

	// StorageUtil + the raw volume figures; StorageKnown false ⇒ all
	// three persist as NULL.
	StorageUtil           float64
	StorageAvailableBytes int64
	StorageCapacityBytes  int64
	StorageKnown          bool

	// ReplicaLagSeconds + the connection counts are the secondary
	// signals; each *Known gates its column(s).
	ReplicaLagSeconds float64
	LagKnown          bool
	ActiveConnections int
	MaxConnections    int
	ConnKnown         bool
}

// TargetMetricsHistoryRow is the rendered shape one persisted history row
// takes on read. Same fields as [TargetMetricsSample] (the *Known flags
// reconstructed from column NULLness); nothing engine-specific (the
// surrogate PK stays inside the engine store).
type TargetMetricsHistoryRow struct {
	StreamID string
	Database string
	Branch   string

	SampledAt time.Time

	CPUUtil  float64
	CPUKnown bool
	MemUtil  float64
	MemKnown bool

	StorageUtil           float64
	StorageAvailableBytes int64
	StorageCapacityBytes  int64
	StorageKnown          bool

	ReplicaLagSeconds float64
	LagKnown          bool
	ActiveConnections int
	MaxConnections    int
	ConnKnown         bool
}
