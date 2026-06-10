// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// attachSlotHealthProbe opens a source-side [ir.SchemaReader], type-
// asserts it to [ir.SlotHealthReporter] (today only PG implements),
// and spawns a per-stream goroutine that periodically probes the slot's
// retention pressure and active-liveness — severity-A finding F13 of
// the 2026-05-22 Reddit-research run, see ADR-0059. Returns a cleanup
// closure that cancels the goroutine and closes the SchemaReader at
// streamer teardown; the closure is always non-nil so the caller can
// `defer` it unconditionally.
//
// Non-fatal on every branch: failure to open the source, missing
// SlotHealthReporter assertion (cross-engine pair where the source is
// MySQL), or empty resolved slot name leaves the probe unattached and
// the stream runs without F13 surfacing. Slot health is secondary
// signal; the rest of the pipeline must work without it.
//
// The opened SchemaReader is dedicated to slot polling — it doesn't
// share state with the CDC reader (which lives on a different
// connection in replication mode and isn't safe to share for ad-hoc
// reads). The per-probe query is cheap (one row + one GUC lookup) so
// the dedicated connection's cost is negligible vs. the visibility
// win on the F13 silent-loss class.
//
// Mirrors [Streamer.attachSpillReporter]'s wiring shape. The two
// reporters live on the same engine surface (PG SchemaReader) but
// independent interfaces — each can opt in without the other; both
// are skipped cleanly when the engine doesn't implement them.
func (s *Streamer) attachSlotHealthProbe(ctx context.Context, streamID string) *slotHealthProbeAttachment {
	noop := &slotHealthProbeAttachment{}
	if s.Source == nil || s.SourceDSN == "" {
		return noop
	}
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		slog.DebugContext(
			ctx, "slot-health probe skipped — open source schema reader failed",
			slog.String("stream_id", streamID),
			slog.String("err", err.Error()),
		)
		return noop
	}
	reporter, ok := sr.(ir.SlotHealthReporter)
	if !ok {
		// Engine doesn't expose slot health (today: MySQL). Close the
		// reader so the connection doesn't sit idle for the streamer's
		// lifetime.
		closeIf(sr)
		return noop
	}
	slot := s.SlotName
	if slot == "" {
		slot = defaultPGSlotName
	}

	probeCtx, cancel := context.WithCancel(ctx)
	att := &slotHealthProbeAttachment{
		cancel: cancel,
		close:  func() { closeIf(sr) },
	}

	go slotHealthProbeLoop(probeCtx, reporter, slot, streamID, DefaultSlotHealthThresholds(), slotHealthProbeTickInterval)

	slog.InfoContext(
		ctx, "slot-health probe attached",
		slog.String("stream_id", streamID),
		slog.String("slot", slot),
		slog.Duration("tick_interval", slotHealthProbeTickInterval),
		slog.String("see", "ADR-0059"),
	)
	return att
}

// defaultPGSlotName mirrors the default slot identifier hard-coded in
// `internal/engines/postgres/cdc_reader.go` (`defaultSlot = "sluice_slot"`).
// Duplicated here to avoid coupling `internal/pipeline` to the postgres
// engine package; the constant has been stable across releases and is
// surfaced as a CLI default in `--slot-name` help text.
const defaultPGSlotName = "sluice_slot"
