// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// GrowGate coordinates a cold-copy quiesce during a target storage-grow /
// reparent window (ADR-0110). One gate is shared across every cold-copy
// write lane in a run (the W tables × D fan-out workers, plus the
// pipeline source-read retry and the storage-headroom telemetry sidecar),
// so a transient that one lane hits — or a proactive storage-headroom
// signal — can quiesce ALL lanes together for the window instead of each
// independently hammer-retrying the struggling target.
//
// It is the engine-neutral seam, defined here in internal/ir alongside
// [TargetTelemetry], so both the pipeline orchestrator and the engine
// packages can reach it WITHOUT the engine importing the pipeline: the
// concrete coordinator lives in internal/pipeline and is threaded to the
// MySQL RowWriter via its config (the same setter-interface pattern as
// [MaxBufferBytesSetter] / [CopyDurableProgressReporter]).
//
// A nil GrowGate ⇒ pre-ADR-0110 behaviour, byte-for-byte: Await returns
// immediately and Trip is a no-op. Construct/thread it via a typed-nil
// guard so a nil *concrete* value never becomes a non-nil interface (the
// telemetryHintOrNil pattern in internal/pipeline) — otherwise a caller's
// `gate != nil` check would wrongly fire and then nil-deref.
//
// ADVISORY about WHEN, never about WHAT. The gate only changes how a
// wait is spent (coordinated-and-calm vs independent-and-hammering); it
// NEVER swallows a terminal error, advances a position, or marks a table
// complete. The per-lane bounded retry budgets (the cold-copy
// reparent-retry and source-read-resume loops) remain the AUTHORITATIVE
// loud-on-exhaustion floor: a genuinely-dead target still surfaces.
type GrowGate interface {
	// Await blocks while the gate is CLOSED (a pause is in effect) and
	// returns nil the instant it reopens. It returns ctx.Err() promptly
	// on cancel — this is the load-bearing no-deadlock contract: when any
	// lane exhausts its retry budget and the errgroup cancels the run
	// ctx, every parked Await unwinds cleanly. When the gate is OPEN (the
	// common case) it is a cheap near-lock-free return that adds no
	// measurable cost to an untroubled copy.
	Await(ctx context.Context) error

	// Trip closes the gate (or extends an in-effect pause) and records
	// reason for the structured log. Idempotent and concurrency-safe:
	// concurrent trips from many lanes + the telemetry sidecar COALESCE
	// into ONE pause window rather than stacking.
	Trip(reason string)
}

// GrowGateSetter is the OPTIONAL surface a cold-copy [RowWriter] implements
// to receive the run's shared [GrowGate] (ADR-0110). The pipeline threads
// one gate per cold-copy run onto every writer it opens via this setter —
// the same construction-time wiring pattern as [MaxBufferBytesSetter] and
// [CopyDurableProgressReporter] — so the engine package never imports the
// pipeline. A nil gate (or a writer that doesn't implement the setter)
// degrades to pre-ADR-0110 behaviour, byte-for-byte.
type GrowGateSetter interface {
	SetGrowGate(gate GrowGate)
}

// TransientClassifier is the OPTIONAL surface an engine [SchemaWriter]
// implements so the orchestrator can ask whether an error from a
// NON-applier path is a transient storage-grow / reparent shape worth
// retrying (ADR-0114). The applier / cold-copy paths already route their
// driver returns through the engine's internal classifier, which wraps a
// transient as an [ir.RetriableError]; but the post-copy DDL phases
// (CreateIndexes / CreateConstraints / CreateViews / SyncIdentitySequences)
// exec their DDL directly and return the RAW driver error, so the
// pipeline can't see the same verdict via errors.As. This surface exposes
// that verdict: the engine delegates to the exact same classifier the
// apply path uses, so the DDL-phase retry recognises a PlanetScale
// reparent (PG 57P01 / MySQL "not serving") identically to the row path.
//
// A SchemaWriter that doesn't implement this ⇒ the DDL-phase retry is a
// no-op (one attempt, terminal on any error) — the pre-ADR-0114 behaviour,
// byte-for-byte. The classifier is a pure verdict over the error: it never
// mutates state, advances a position, or swallows a terminal error.
type TransientClassifier interface {
	// IsTransientError reports whether err (or anything it wraps) is a
	// classified transient (connection-drop / reparent / storage-grow)
	// the DDL-phase retry should ride out. A non-transient (a real DDL
	// fault, a type error, a constraint violation) returns false and the
	// phase fails loudly, exactly as before.
	IsTransientError(err error) bool
}

// ReparentObserverSetter is the OPTIONAL surface a cold-copy [RowWriter]
// implements to report, per table, that it observed a classified
// storage-grow / reparent transient while writing — the signal the restore
// reconciliation phase (ADR-0113) uses to know which tables a target's
// reparent may have silently under-copied (PlanetScale's grow-reparent can
// drop committed-but-unreplicated rows that the reactive grow-gate cannot
// recover, because they were lost before the first transient was seen).
//
// The writer calls the observer with the table name at the SAME point it
// trips the [GrowGate] (the first classified transient on a flush). The
// restore wires one observer per run onto every writer via this setter
// (the same construction-time pattern as [GrowGateSetter]); after the bulk
// copy it re-derives every marked table from its immutable chunks (TRUNCATE
// + serial redo, or idempotent re-apply for a chain segment) so the table
// exactly matches the manifest regardless of what the reparent dropped.
//
// A nil observer (or a writer that doesn't implement the setter) ⇒ no
// reconciliation tracking, byte-for-byte the pre-ADR-0113 behaviour.
type ReparentObserverSetter interface {
	SetReparentObserver(observe func(table string))
}
