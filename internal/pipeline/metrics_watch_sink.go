// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/telemetrysink"
)

// Persistent-sample sink wiring (roadmap item 75c) — the mapping from the
// engine-neutral telemetry snapshot to a durable record, plus the
// failure-isolated write.
//
// The `--notify-*` sinks deliver EDGE-TRIGGERED threshold alerts; this one
// records EVERY polled sample, which is what turns `metrics-watch` from an
// alerter into a collector an operator can point at their own portal. Both
// share the same advisory posture: a dead sink is logged and swallowed.

// sampleRecord maps one polled snapshot into a persistable record.
//
// The *Known honesty contract is carried STRUCTURALLY into the persisted
// row: an unobserved metric becomes a nil pointer (JSON `null`), never a 0
// that a portal would plot as "idle". A tick with no usable sample at all
// (ok=false) records fresh=false with every metric null — a visible gap
// rather than a missing row, so a consumer can tell "the watch was running
// and saw nothing" from "the watch was down".
func sampleRecord(now time.Time, label, database, branch string, snap ir.TargetHealthSnapshot, ok bool) telemetrysink.Record {
	rec := telemetrysink.Record{
		At:       now.UTC(),
		Watch:    label,
		Database: database,
		Branch:   branch,
		Fresh:    ok,
	}
	if !ok {
		return rec
	}
	if !snap.SampledAt.IsZero() {
		sampledAt := snap.SampledAt.UTC()
		rec.SampledAt = &sampledAt
	}
	if snap.CPUKnown {
		rec.CPUUtil = ptrOf(snap.CPUUtil)
	}
	if snap.MemKnown {
		rec.MemUtil = ptrOf(snap.MemUtil)
	}
	if snap.StorageKnown {
		rec.StorageUtil = ptrOf(snap.StorageUtil)
		rec.StorageAvailableBytes = ptrOf(snap.StorageAvailableBytes)
		rec.StorageCapacityBytes = ptrOf(snap.StorageCapacityBytes)
	}
	if snap.LagKnown {
		rec.ReplicaLagSeconds = ptrOf(snap.ReplicaLagSeconds)
	}
	if snap.ConnKnown {
		rec.ActiveConnections = ptrOf(int64(snap.ActiveConnections))
		rec.MaxConnections = ptrOf(int64(snap.MaxConnections))
	}
	return rec
}

// ptrOf returns a pointer to v — the "observed" half of the record's
// null-means-unobserved encoding.
func ptrOf[T any](v T) *T { return &v }

// writeSampleRecords delivers a tick's records to the sink with the
// load-bearing failure isolation: any error is logged ONCE at WARN and
// SWALLOWED. A full disk, a dead push endpoint, or a record the codec
// refused must never stall or fail a poll.
func writeSampleRecords(ctx context.Context, logger *slog.Logger, sink telemetrysink.Sink, recs []telemetrysink.Record) {
	if sink == nil || len(recs) == 0 {
		return
	}
	if err := sink.Write(ctx, recs); err != nil {
		if ctx.Err() != nil {
			return // shutdown, not a sink failure
		}
		logger.WarnContext(
			ctx, "metrics-watch sample sink write failed (advisory only — the watch is unaffected)",
			slog.String("sink", sink.Name()),
			slog.Int("records", len(recs)),
			slog.String("error", err.Error()),
		)
	}
}
