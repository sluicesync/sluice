// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// TargetTelemetry is the engine-neutral, ADVISORY surface a control-plane
// telemetry provider implements so the apply path can see the target's
// resource state (CPU / memory / storage / lag / connections) directly and
// react PROACTIVELY — before the reactive AIMD controller, the tx-killer, or
// a storage-resize stall would otherwise push back.
//
// This is the seam that lets the PlanetScale Prometheus-metrics capability
// (ADR-0107) plug in WITHOUT any PlanetScale import in the engine-neutral
// core. The core defines only this interface and the plain
// [TargetHealthSnapshot] report; the optional PlanetScale provider lives in
// its own package (`internal/planetscale/telemetry`), implements this
// interface against the control-plane metrics endpoint, and is wired onto
// the streamer only when the operator opts in. No engine package, no
// pipeline orchestrator code, and nothing in `internal/ir` ever imports the
// provider — exactly the IR-first / contain-PS-complexity posture the
// connection-budget probe ([TargetConnectionBudgetProber]) and the PG
// slot-spill reporter already follow.
//
// ADVISORY ONLY. The frontier (exactly-once), the per-lane AIMD controller,
// and the in-lane tx-killer recovery stay AUTHORITATIVE. A telemetry value
// is a hint that may bias a controller's ceiling or trigger an operator-
// facing signal; it can never, by itself, advance a position, drop a change,
// or stall the stream. A provider outage / staleness MUST degrade to today's
// reactive behaviour (see [TargetHealthSnapshot.Fresh]) — never an error that
// kills the run.
//
// The provider is poll-driven OFF the apply hot path (~15-30s cadence, owned
// by the provider, not the applier). Sample is therefore cheap and lock-free
// from the caller's side: it returns the most recent poll's cached snapshot,
// never blocking on a live control-plane round-trip.
type TargetTelemetry interface {
	// Sample returns the most recently polled health snapshot for the
	// stream's target branch/keyspace. It MUST NOT block on a live network
	// round-trip — it returns the provider's cached value (the background
	// poll loop refreshes it). ok=false means "no usable signal right now"
	// (provider not yet warmed up, or the last poll failed and the cached
	// value has gone stale) and the caller degrades to its reactive
	// behaviour, exactly as if no provider were wired. ctx is honoured only
	// for cancellation of the (non-blocking) read.
	Sample(ctx context.Context) (snap TargetHealthSnapshot, ok bool)
}

// TargetHealthSnapshot is the engine-neutral, point-in-time view of a sync
// target's resource state, distilled from whatever the provider's control
// plane exposes (for PlanetScale: the Prometheus metrics endpoint). The
// fields are deliberately a small, provider-agnostic distillation — NOT a
// passthrough of every PlanetScale metric — so the core stays engine-neutral
// and a future provider for another platform can populate the same shape.
//
// Every utilisation field is a fraction in [0, 1] (1.0 == fully saturated)
// with a companion *Known flag: a provider that cannot observe a given metric
// leaves the value 0 and the flag false, so a consumer never mistakes
// "unobserved" for "idle". Mirrors the (snap, ok) honesty of the PG
// slot-spill reporter.
type TargetHealthSnapshot struct {
	// SampledAt is the wall-clock time of the underlying poll. Consumers
	// combine it with the provider's freshness window to decide whether the
	// snapshot is still actionable (see Fresh).
	SampledAt time.Time

	// CPUUtil / MemUtil are the target's CPU and memory utilisation as a
	// fraction in [0, 1] (operator's stated #1 priority). *Known is false
	// when the provider could not observe the metric.
	CPUUtil  float64
	CPUKnown bool
	MemUtil  float64
	MemKnown bool

	// StorageUtil is volume used / capacity in [0, 1]; StorageAvailableBytes
	// / StorageCapacityBytes carry the raw figures for the storage-resize
	// anticipation path (use (b)) and operator observability (use (c)).
	// StorageKnown is false when the provider could not observe the volume
	// metrics.
	StorageUtil           float64
	StorageAvailableBytes int64
	StorageCapacityBytes  int64
	StorageKnown          bool

	// ReplicaLagSeconds and ActiveConnections are the SECONDARY signals
	// (operator priority is CPU/mem/storage first). *Known guards each.
	ReplicaLagSeconds float64
	LagKnown          bool
	ActiveConnections int
	MaxConnections    int
	ConnKnown         bool
}

// Fresh reports whether the snapshot is recent enough to act on, given a
// provider-supplied freshness window (typically ~3x the poll cadence). A
// stale snapshot is treated as "no signal" by consumers — the proactive
// damping degrades to reactive AIMD rather than acting on a possibly-wrong
// old reading. now is injected so unit tests stay deterministic.
func (s TargetHealthSnapshot) Fresh(now time.Time, window time.Duration) bool {
	if s.SampledAt.IsZero() || window <= 0 {
		return false
	}
	return now.Sub(s.SampledAt) <= window
}
