// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0107 Phase 1 advisory target-telemetry consumption. The
// engine-neutral seam ([ir.TargetTelemetry] / [ir.TargetHealthSnapshot])
// is defined in internal/ir; this file holds the pipeline-side
// consumers — the AIMD proactive back-off HINT adapter and the
// storage-resize anticipation WARN — plus the freshness/cadence
// constants. The real PlanetScale provider is Phase 2 (its own package,
// the only PS-importing code); Phase 1 is driven entirely by a fake in
// the tests. nil provider ⇒ none of this engages: pre-ADR-0107
// behaviour, byte-for-byte.

const (
	// telemetryPollInterval is the cadence the Phase-2 provider polls the
	// control plane at (defined here so Phase 1's freshness window and the
	// storage-WARN tick can reference one canonical value). Phase 1 does
	// NOT start a poll loop — the provider owns that — but the storage
	// sidecar ticks at this cadence to read the provider's cached sample.
	telemetryPollInterval = 20 * time.Second

	// telemetryFreshnessWindow is how old a snapshot may be and still be
	// acted on (see [ir.TargetHealthSnapshot.Fresh]). 3x the poll cadence
	// tolerates a single missed poll before a consumer degrades to its
	// reactive path. A snapshot older than this is treated as "no signal".
	telemetryFreshnessWindow = 3 * telemetryPollInterval
)

// telemetryHint adapts an [ir.TargetTelemetry] provider into the
// [appliercontrol.TelemetryHint] surface the AIMD controller consults.
// It owns the freshness check and the high-water comparison, so the
// controller (and the appliercontrol package) never see the raw snapshot
// or the ir import — exactly the contain-PS-complexity posture the seam
// exists for. One adapter is shared across all lanes' controllers: each
// lane's controller calls Saturated() under its OWN mutex, and the call
// is a non-blocking read of the provider's cached sample plus a couple of
// float comparisons, so the shared adapter introduces no cross-lane
// coupling beyond the provider's own (lock-free) Sample contract.
type telemetryHint struct {
	provider  ir.TargetTelemetry
	highWater float64
	window    time.Duration

	// ctx scopes the (non-blocking) Sample read. Captured at construction
	// from the apply context so a cancelled stream stops consulting.
	ctx context.Context

	// now is injected for deterministic freshness in tests; nil ⇒ time.Now.
	now func() time.Time
}

// newTelemetryHint builds the adapter, or returns nil when no provider is
// wired (so callers can pass the result straight into the controller
// Config and a nil hint degrades to the reactive path). highWater <= 0
// falls back to [appliercontrol.DefaultTelemetryHighWater].
func newTelemetryHint(ctx context.Context, provider ir.TargetTelemetry, highWater float64) *telemetryHint {
	if provider == nil {
		return nil
	}
	if highWater <= 0 {
		highWater = appliercontrol.DefaultTelemetryHighWater
	}
	return &telemetryHint{
		provider:  provider,
		highWater: highWater,
		window:    telemetryFreshnessWindow,
		ctx:       ctx,
		now:       time.Now,
	}
}

// Saturated reports whether the freshest snapshot shows CPU or memory at
// or above the high-water mark. ok=false when the provider has no usable
// signal (not warmed up, or the cached snapshot has gone stale, or the
// metric was unobserved) — the controller then degrades to its reactive
// AIMD path. A *known* metric below the mark returns (false, true): a
// genuine "target is healthy" signal that resets the controller's edge
// latch. Implements [appliercontrol.TelemetryHint].
func (h *telemetryHint) Saturated() (saturated, ok bool) {
	if h == nil || h.provider == nil {
		return false, false
	}
	snap, sampleOK := h.provider.Sample(h.ctx)
	if !sampleOK || !snap.Fresh(h.nowOrDefault(), h.window) {
		return false, false
	}
	// Need at least one of CPU / mem observed to give a verdict. If
	// neither is known the provider can't speak to saturation — degrade.
	if !snap.CPUKnown && !snap.MemKnown {
		return false, false
	}
	if snap.CPUKnown && snap.CPUUtil >= h.highWater {
		return true, true
	}
	if snap.MemKnown && snap.MemUtil >= h.highWater {
		return true, true
	}
	return false, true
}

// telemetryHintOrNil converts a possibly-nil *telemetryHint into the
// controller's [appliercontrol.TelemetryHint] interface field WITHOUT the
// typed-nil trap: assigning a nil *telemetryHint straight to an interface
// yields a NON-nil interface (concrete type, nil value), which would make
// the controller's `cfg.TelemetryHint != nil` guard wrongly fire and then
// nil-deref. Returning a true nil interface keeps the "no provider ⇒ no
// hint" degrade exact.
func telemetryHintOrNil(h *telemetryHint) appliercontrol.TelemetryHint {
	if h == nil {
		return nil
	}
	return h
}

func (h *telemetryHint) nowOrDefault() time.Time {
	if h.now != nil {
		return h.now()
	}
	return time.Now()
}

// startStorageHeadroomWatch spawns the ADR-0107 storage-resize
// anticipation sidecar: a slow-tick goroutine that reads the provider's
// cached snapshot and, on the rising EDGE of crossing the storage
// high-water mark, logs ONE structured WARN so an operator knows a
// resize/reparent may interrupt apply shortly. It is pure
// anticipation/explanation — it NEVER pauses, gates, or errors the
// stream; items 30/33's retriable classification remains the actual
// ride-through. Edge-triggered: it warns once per crossing, then stays
// quiet until storage drops back below the mark (so a sustained-low-
// headroom target doesn't flood the log every tick).
//
// nil provider ⇒ no goroutine (a total no-op, the default). The
// goroutine exits when ctx is cancelled; the caller does not track it.
func (s *Streamer) startStorageHeadroomWatch(ctx context.Context, streamID string) {
	if s.TargetTelemetry == nil {
		return
	}
	provider := s.TargetTelemetry
	logger := slog.Default()
	go func() {
		ticker := time.NewTicker(telemetryPollInterval)
		defer ticker.Stop()
		// warned latches the edge so we WARN once per crossing, not per tick.
		warned := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				warned = evalStorageHeadroomTick(ctx, logger, provider, streamID, warned)
			}
		}
	}()
}

// evalStorageHeadroomTick is one tick of the storage-headroom watch,
// pulled out so the edge-trigger semantics are unit-testable without a
// live 20s ticker. It reads the provider's cached snapshot and returns
// the NEXT value of the edge latch: it WARNs (and latches true) only on
// the rising edge of crossing the high-water mark, re-arms (latches
// false) once headroom recovers, and leaves the latch untouched when
// there is no usable signal — so a transient stale poll never re-arms a
// spurious WARN.
func evalStorageHeadroomTick(
	ctx context.Context,
	logger *slog.Logger,
	provider ir.TargetTelemetry,
	streamID string,
	warned bool,
) bool {
	snap, ok := provider.Sample(ctx)
	if !ok || !snap.StorageKnown || !snap.Fresh(time.Now(), telemetryFreshnessWindow) {
		return warned
	}
	low := snap.StorageUtil >= appliercontrol.DefaultTelemetryHighWater
	switch {
	case low && !warned:
		logStorageHeadroomWarn(ctx, logger, streamID, snap)
		return true
	case !low:
		// Headroom recovered (e.g. a resize completed) — re-arm so the
		// next crossing warns again.
		return false
	default:
		// low && warned: sustained low headroom, already warned — hold.
		return warned
	}
}

// logStorageHeadroomWarn emits the single structured operator WARN for a
// storage-headroom crossing. Pulled out so the unit test can assert the
// message + the edge-trigger semantics without a live ticker.
func logStorageHeadroomWarn(ctx context.Context, logger *slog.Logger, streamID string, snap ir.TargetHealthSnapshot) {
	logger.WarnContext(
		ctx, "target storage approaching capacity — a resize/reparent may briefly interrupt apply shortly (items 30/33 retry transparently)",
		slog.String("stream_id", streamID),
		slog.Float64("storage_util", snap.StorageUtil),
		slog.Int64("storage_available_bytes", snap.StorageAvailableBytes),
		slog.Int64("storage_capacity_bytes", snap.StorageCapacityBytes),
		slog.Float64("high_water", appliercontrol.DefaultTelemetryHighWater),
		slog.String("hint", "this is anticipation only — sluice does not pause; if a brief apply stall follows it is the resize, and the stream rides through it"),
	)
}
