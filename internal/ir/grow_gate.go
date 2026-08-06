// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

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

	// Trip closes the gate (or coalesces into an in-effect pause) and records
	// reason + evidence for the structured log. Idempotent and
	// concurrency-safe: concurrent trips from many lanes + the telemetry
	// sidecar COALESCE into ONE pause window rather than stacking.
	//
	// evidence is what the caller ACTUALLY OBSERVED, not what it suspects —
	// see [GrowEvidence]. It is descriptive only: the gate's behaviour is
	// identical for every value, so a caller can never make the copy quiesce
	// harder by claiming more.
	Trip(reason string, evidence GrowEvidence)
}

// GrowEvidence records what a [GrowGate.Trip] actually OBSERVED about the
// target, as opposed to what the trip's cause might have been.
//
// # Why this exists (roadmap item 143)
//
// The gate trips on the engine's whole retriable-transient class, and every
// operator-facing sentence attached to that trip used to assert a cause it had
// no evidence for — "likely a primary reparent / 'not serving'". Two
// independent datasets say that assertion is almost always wrong: in the
// 2026-08-05 field log 244 of 246 gate windows were opened by
// `vtgate connection error: no endpoints`, a routing/transport error carrying
// no reparent evidence at all, and zero by any reparent-evidenced face; and
// across ~870 absorbed drops over six A/B runs on real PlanetScale, zero
// carried reparent evidence. A word like "likely" in a log line is a
// hypothesis nobody can check, and it sent readers looking for a reparent
// that did not happen.
//
// So the claim is DERIVED rather than asserted. Each engine classifies the
// error it is tripping on with the SAME predicates its retriable classifier
// uses, and the log states the verdict instead of guessing at a cause.
//
// # What this does NOT do, deliberately
//
// It does not narrow WHEN the gate trips. Phase A of item 143 could not
// establish that a real storage-grow reparent is distinguishable at the trip
// point — a genuine grow's most common face on an in-flight write is the
// connection dying, which is byte-identical to a plain transport drop — so
// gating the quiesce on [GrowEvidenceTargetFace] would trade a cheap
// false-positive quiesce for the possibility of missing the very event the
// gate exists for. Absence of evidence is not evidence of absence, and this
// type is named for what it is: evidence, not cause.
//
// What it DOES buy is that the next field log can answer the question this
// item could not: how often the gate fires on a face that actually names a
// serving transition. Today every trip reason is raw error text and the
// answer has to be re-derived by hand from 246 log lines.
type GrowEvidence uint8

const (
	// GrowEvidenceNone — the trip carried NO storage-grow / serving-transition
	// evidence. The error is a transport/connection loss, a contention shape, a
	// timeout, or any other classified transient that says nothing about the
	// target's serving state. This is the ZERO VALUE on purpose (the v0.99.51
	// zero-value trap, applied to a CLAIM rather than to behaviour): a caller
	// that forgets to classify must under-claim, never over-claim, because
	// over-claiming a cause is the defect this type was added to remove.
	GrowEvidenceNone GrowEvidence = iota

	// GrowEvidenceTargetFace — the error IS one of the documented storage-grow
	// / serving-transition faces: the target said it was out of disk, that it
	// was read-only, or that no primary was serving / a reparent was in
	// progress. The engine derives this from the same predicates that made the
	// error retriable, so the two cannot drift.
	GrowEvidenceTargetFace

	// GrowEvidenceTelemetry — no error at all: an out-of-band storage-headroom
	// observation (the ADR-0110 telemetry sidecar) says the target is
	// approaching its auto-grow boundary. This is the only PROACTIVE trip
	// source and the only one whose evidence is about storage rather than
	// about a failed write.
	GrowEvidenceTelemetry
)

// String renders the verdict as the stable token that appears in the
// structured logs. These strings are operator-facing and greppable across a
// run's log, which is the whole point of the type — keep them stable.
func (e GrowEvidence) String() string {
	switch e {
	case GrowEvidenceTargetFace:
		return "target-grow-face"
	case GrowEvidenceTelemetry:
		return "telemetry-headroom"
	case GrowEvidenceNone:
		return "no-grow-evidence"
	default:
		return "no-grow-evidence"
	}
}

// GrowGateQuiesceObserver is the OPTIONAL surface a [GrowGate] implements so
// an observer can tell a DELIBERATE pause from a stall (roadmap item 146).
//
// The idle-progress copy watchdog reports a run that has made no forward
// progress for N minutes. An ADR-0110 pause window is exactly that — every
// lane parked in [GrowGate.Await], by design, for up to GrowGateMaxHold — so
// a watchdog that could not see it would cry wolf on every storage grow, get
// suppressed, and then catch nothing. Asking the gate is what keeps the
// warning meaningful.
//
// A gate that does not implement this reads as "never quiesced", which is the
// conservative direction for a WARN-only signal (it may over-report, never
// under-report). The concrete [migcore.GrowGate] implements it, asserted
// against the real type rather than a test stub.
type GrowGateQuiesceObserver interface {
	// QuiescedSince reports whether the gate was CLOSED at any point at or
	// after t — i.e. whether any part of the interval [t, now] was a
	// deliberate coordinated pause rather than lost time.
	QuiescedSince(t time.Time) bool
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
