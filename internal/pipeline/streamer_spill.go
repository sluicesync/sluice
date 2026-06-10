// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// attachSpillReporter opens a source-side [ir.SchemaReader], type-asserts
// it to [ir.SlotSpillReporter] (today only PG implements), and plugs a
// per-scrape spill-stats snapshotter into the metrics server (severity-B
// finding F2 of the 2026-05-22 PG-internals research run). Returns a
// cleanup closure that closes the SchemaReader at metrics-server
// teardown; the closure is always non-nil so the caller can `defer` it
// unconditionally.
//
// Non-fatal on every branch: a failure to open the source, a missing
// SlotSpillReporter assertion, or an empty resolved slot name simply
// leaves the reporter unattached, and the spill-counter metric lines
// never appear in /metrics. Spill is secondary signal; the rest of the
// metric set should continue working.
//
// The opened SchemaReader is dedicated to spill polling — it doesn't
// share state with the CDC reader (which lives on a different connection
// in replication mode and isn't safe to share for ad-hoc reads). The
// per-scrape query is cheap (a single-row lookup) so the dedicated
// connection's cost is negligible vs. the visibility win.
func (s *Streamer) attachSpillReporter(ctx context.Context, srv *MetricsServer, streamID string) func() {
	noop := func() {}
	if s.Source == nil || s.SourceDSN == "" {
		return noop
	}
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		slog.DebugContext(
			ctx, "metrics: spill reporter skipped — open source schema reader failed",
			slog.String("stream_id", streamID),
			slog.String("err", err.Error()),
		)
		return noop
	}
	spiller, ok := sr.(ir.SlotSpillReporter)
	if !ok {
		// Engine doesn't expose spill stats (today: MySQL). Close the
		// reader we just opened so the connection doesn't sit idle for
		// the streamer's lifetime.
		closeIf(sr)
		return noop
	}
	slot := s.SlotName
	if slot == "" {
		slot = defaultPGSlotName
	}
	srv.AttachSpillReporter(func(qctx context.Context) (SpillSnapshot, bool, error) {
		stats, statsOK, sErr := spiller.SlotSpillStats(qctx, slot)
		if sErr != nil {
			return SpillSnapshot{}, false, sErr
		}
		if !statsOK {
			return SpillSnapshot{}, false, nil
		}
		return SpillSnapshot{
			StreamID:   streamID,
			SlotName:   slot,
			SpillTxns:  stats.SpillTxns,
			SpillBytes: stats.SpillBytes,
		}, true, nil
	})
	slog.InfoContext(
		ctx, "metrics: spill reporter attached",
		slog.String("stream_id", streamID),
		slog.String("slot", slot),
	)
	return func() {
		srv.AttachSpillReporter(nil)
		closeIf(sr)
	}
}
