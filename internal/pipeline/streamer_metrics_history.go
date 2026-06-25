// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0107 item 35 — target-metrics rolling-history recorder sidecar.
//
// A slow-tick goroutine that mirrors [Streamer.startStorageHeadroomWatch]:
// it reads the telemetry provider's CACHED snapshot on each tick (off the
// apply hot path) and persists it into the bounded
// sluice_target_metrics_history metadata table on the target, so
// `sluice diagnose` (or a plain SELECT) surfaces the recent CPU / mem /
// storage / lag / conn TREND without the operator scripting the metrics
// API. ADVISORY / OBSERVABILITY ONLY — every error is logged at WARN and
// SWALLOWED; the recorder can never stall or crash the sync.

const (
	// defaultMetricsHistoryRetention bounds the rolling-history table: rows
	// older than this are pruned. 7 days is enough to see a multi-day trend
	// (the operator-facing "is storage climbing over the week?" question)
	// while staying tiny — at the ~60s scrape cadence that is ~10k rows.
	defaultMetricsHistoryRetention = 7 * 24 * time.Hour

	// metricsHistoryPruneEveryTicks is how many record ticks pass between
	// prune passes. At telemetryPollInterval (60s) that is ~30min, frequent
	// enough to keep the table bounded without a DELETE on every tick.
	metricsHistoryPruneEveryTicks = 30
)

// startTargetMetricsHistoryRecorder spawns the rolling-history recorder for
// the stream. It mirrors [startStorageHeadroomWatch]'s shape: a no-op when
// there is nothing to record, a single goroutine otherwise, exiting on
// ctx.Done. The caller does not track the goroutine.
//
// No-op (returns without spawning) when:
//   - provider is nil (no telemetry wired — the default; pre-ADR-0107
//     byte-for-byte),
//   - the applier does not implement [ir.TargetMetricsHistoryStore] (an
//     engine without the surface),
//   - or s.SuppressTargetMetricsHistory is set (the operator opted out).
//
// On start it calls EnsureTargetMetricsHistory ONCE; an error there logs a
// WARN and returns (no goroutine) — the recorder is advisory, a missing
// table must never fail the run. The goroutine then ticks at
// telemetryPollInterval: each tick reads the provider's cached sample and,
// only when its SampledAt DIFFERS from the last persisted one (dedupe — the
// source updates ~once a minute, so re-reading the same sample is not
// re-recorded), records it; every metricsHistoryPruneEveryTicks ticks it
// also prunes to defaultMetricsHistoryRetention. All store errors are
// logged at WARN and swallowed.
func (s *Streamer) startTargetMetricsHistoryRecorder(ctx context.Context, streamID string, applier ir.ChangeApplier, provider ir.TargetTelemetry) {
	if provider == nil || s.SuppressTargetMetricsHistory {
		return
	}
	store, ok := applier.(ir.TargetMetricsHistoryStore)
	if !ok {
		return
	}
	logger := slog.Default()
	if err := store.EnsureTargetMetricsHistory(ctx); err != nil {
		logger.WarnContext(
			ctx, "target-metrics history: ensure table failed; recorder disabled for this run (advisory only, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
		return
	}
	go func() {
		ticker := time.NewTicker(telemetryPollInterval)
		defer ticker.Stop()
		// lastSampledAt dedupes: persist a sample only when its poll
		// timestamp advances past the last one we wrote. Zero until the
		// first record so the first fresh sample always persists.
		var lastSampledAt time.Time
		tick := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tick++
				recordTargetMetricsTick(ctx, logger, store, provider, streamID, &lastSampledAt)
				if tick%metricsHistoryPruneEveryTicks == 0 {
					if err := store.PruneTargetMetricsHistory(ctx, defaultMetricsHistoryRetention); err != nil {
						logger.WarnContext(
							ctx, "target-metrics history: prune failed (advisory only, sync unaffected)",
							slog.String("stream_id", streamID),
							slog.String("error", err.Error()),
						)
					}
				}
			}
		}
	}()
}

// recordTargetMetricsTick is one record tick, pulled out so the
// dedupe + failure-isolation semantics are unit-testable without a live
// 60s ticker. It reads the provider's cached sample and, when it is fresh
// AND its SampledAt advances past *lastSampledAt, records it and updates
// *lastSampledAt. ok=false (no usable signal) and a same-timestamp sample
// (the source hasn't updated) are both no-ops; a record error is logged at
// WARN and swallowed (the dedupe cursor is NOT advanced on a failed write,
// so the next tick retries the same sample).
func recordTargetMetricsTick(
	ctx context.Context,
	logger *slog.Logger,
	store ir.TargetMetricsHistoryStore,
	provider ir.TargetTelemetry,
	streamID string,
	lastSampledAt *time.Time,
) {
	snap, ok := provider.Sample(ctx)
	if !ok || snap.SampledAt.IsZero() {
		return
	}
	if !snap.SampledAt.After(*lastSampledAt) {
		// Same (or older) poll than the last persisted one — the source
		// only updates ~once a minute, so re-reading the cached sample is
		// not re-recorded (honest cadence, no fabricated resolution).
		return
	}
	sample := ir.TargetMetricsSample{
		StreamID:              streamID,
		SampledAt:             snap.SampledAt,
		CPUUtil:               snap.CPUUtil,
		CPUKnown:              snap.CPUKnown,
		MemUtil:               snap.MemUtil,
		MemKnown:              snap.MemKnown,
		StorageUtil:           snap.StorageUtil,
		StorageAvailableBytes: snap.StorageAvailableBytes,
		StorageCapacityBytes:  snap.StorageCapacityBytes,
		StorageKnown:          snap.StorageKnown,
		ReplicaLagSeconds:     snap.ReplicaLagSeconds,
		LagKnown:              snap.LagKnown,
		ActiveConnections:     snap.ActiveConnections,
		MaxConnections:        snap.MaxConnections,
		ConnKnown:             snap.ConnKnown,
	}
	if err := store.RecordTargetMetricsSample(ctx, sample); err != nil {
		logger.WarnContext(
			ctx, "target-metrics history: record failed (advisory only, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
		return
	}
	*lastSampledAt = snap.SampledAt
}
