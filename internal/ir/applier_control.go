// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// BatchSizeProvider is the optional surface a [ChangeApplier] can
// implement to consult an external controller for each batch's target
// size. When unset, the applier uses the static value passed to
// [BatchedChangeApplier.ApplyBatch].
//
// AIMD controller wiring (ADR-0052). The pipeline streamer constructs
// the [appliercontrol.Controller] when --apply-batch-size != static
// and threads it onto the applier via a setter (engine-side
// SetBatchSizeProvider method). The applier wraps each internal batch
// with `provider.NextBatchSize()` to discover the controller's
// current target. The static --apply-batch-size value becomes a CAP
// the controller never exceeds; the floor is ADR-0017's conservative
// default of 1.
//
// Implementations MUST be safe for concurrent reads — the applier's
// hot loop reads on every batch boundary while a separate metrics
// goroutine may snapshot controller state at scrape time. The
// [appliercontrol.Controller] implementation uses a mutex; cheaper
// shapes (atomic loads) are acceptable as long as the contract holds.
//
// Engines that don't implement the corresponding optional setter on
// their applier (or operators who pass --no-auto-tune) fall back to
// the static --apply-batch-size value and never construct a
// controller. Zero overhead on the default path.
type BatchSizeProvider interface {
	// NextBatchSize returns the controller's current target batch
	// size. Implementations MUST return a value in [1, ceiling]; the
	// floor of 1 is the ADR-0017 conservative-default invariant. The
	// applier treats the return value as an upper bound for the next
	// batch — early flushes (TxCommit, byte-cap, idle) may commit a
	// smaller batch.
	NextBatchSize() int
}

// BatchObserver is the optional surface a [ChangeApplier] can
// implement to report per-batch outcomes back to an external
// controller (latency, row count, retriable-error count). Mirrors the
// v0.45.0 `applier: batch latency` DEBUG telemetry, but as a
// programmatic signal a controller can consume.
//
// AIMD controller wiring (ADR-0052). Called once per batch — after
// tx.Commit on the success path, after rollback on the failure path.
// The latency is the wall-clock duration from "batch begin" to "tx
// commit returned" (success) or "rollback complete" (failure). The
// rows count is the number of changes the applier attempted to apply
// in the batch (zero on a pre-tx error before any dispatch). The err
// argument carries the failure cause; the controller's classifier
// walks the error chain via [errors.As] for [RetriableError].
//
// Implementations MUST tolerate ctx cancellation gracefully — the
// observer is called even on the failure path where ctx may already
// be cancelled. The controller's slog calls use ctx, but the actual
// state update is unconditional (a controller that "stops observing"
// on cancellation would silently lose the failure signal).
//
// Engines that don't implement the corresponding optional setter on
// their applier never construct a controller and never reach this
// path. Zero overhead on the default path.
type BatchObserver interface {
	// ObserveBatch reports one batch's outcome. The ctx flows through
	// to slog so log lines stay correlated with the applier's request
	// context (stream-id, deadline). Idempotent on zero-latency /
	// zero-rows / nil-error tuples (the idle-flush of an empty batch
	// — no signal to feed the controller).
	ObserveBatch(ctx context.Context, latency time.Duration, rows int, err error)
}

// BatchSizeProviderSetter is the optional surface a [ChangeApplier]
// can implement to receive an external [BatchSizeProvider]. Mirrors
// the [RedactorSetter] / [StreamIDSetter] / [ApplyExecTimeoutSetter]
// shape — the pipeline streamer type-asserts after applier
// construction and calls the setter when the AIMD controller is
// engaged.
//
// Engines that don't implement this setter inherit the pre-ADR-0052
// behaviour: --apply-batch-size remains a static row-count cap.
// Engines that DO implement it (both shipping engines after ADR-0052
// lands) consult the provider on every batch boundary.
//
// A nil provider is allowed and means "no controller — fall back to
// the static cap." The setter is idempotent.
type BatchSizeProviderSetter interface {
	SetBatchSizeProvider(p BatchSizeProvider)
}

// BatchObserverSetter is the optional surface a [ChangeApplier] can
// implement to receive an external [BatchObserver]. Companion to
// [BatchSizeProviderSetter] — the two are always wired together when
// the AIMD controller is engaged.
//
// A nil observer is allowed and means "no controller — don't bother
// computing per-batch latencies for observation." The setter is
// idempotent.
type BatchObserverSetter interface {
	SetBatchObserver(o BatchObserver)
}

// BatchSizeController is one AIMD controller's full surface — both the
// batch-size provider and the per-batch observer — bundled so a single
// value can drive ONE apply lane's adaptive sizing end to end. The
// [appliercontrol.Controller] satisfies it by construction (it
// implements both halves already).
//
// It exists for the ADR-0104 concurrent key-hash apply path
// ([LaneAIMDSetter]): each of the W in-order lanes owns its OWN
// controller, consulting NextBatchSize before reading a batch and
// feeding ObserveBatch its commit outcome — so a tx-killer on a slow
// lane shrinks only that lane, while the other lanes keep riding at
// their own sizes. The serial path keeps the separate
// [BatchSizeProviderSetter] / [BatchObserverSetter] wiring (one
// controller); this is the per-lane bundle.
type BatchSizeController interface {
	BatchSizeProvider
	BatchObserver
}

// LaneAIMDSetter is the optional surface a [ChangeApplier] implements to
// receive the per-lane AIMD controllers for the ADR-0104 concurrent
// key-hash apply path: W controllers, one per apply lane, in lane-index
// order (controllers[i] drives lane i). The streamer builds them only
// when --apply-concurrency W > 1 and the applier implements this surface;
// each controller carries the same Config as the serial single-controller
// path (Floor 1, Ceiling --apply-batch-size, the resolved target
// latency) but its OWN OnShrink, so per-lane shrink decisions stay
// independent.
//
// Each lane drives its controller from a SINGLE goroutine
// (NextBatchSize before a read, ObserveBatch after a commit), so no two
// goroutines touch the same controller — but the [appliercontrol.Controller]
// is concurrency-safe regardless (the metrics scraper reads Snapshot).
//
// Both shipping engines implement it: MySQL's key-hash lanes (ADR-0104)
// and Postgres's lane router (ADR-0105). A future engine that adopts
// --apply-concurrency without a per-lane surface simply doesn't implement
// it and inherits the static per-lane batch size.
type LaneAIMDSetter interface {
	SetLaneAIMDControllers(controllers []BatchSizeController)
}
