// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
)

// seedLiveAddedFilter performs a one-shot read of the per-target
// `sluice_cdc_state.live_added_tables` column at streamer startup so
// the dispatch filter sees previously-recorded live-adds before any
// events flow. Without this, a streamer restart after a partial
// live-add would have a window where events on the live-added table
// were dropped (poll is on a 5s tick) — the seed closes that window.
//
// Engines without [liveAddedTablesReader] (PG; pre-v0.27.0 control
// table) silently skip; the streamer behaves as if there are no
// live-adds, which is correct for those engines (PG uses
// publication-add instead of filter-flip — ADR-0030).
func (s *Streamer) seedLiveAddedFilter(ctx context.Context, applier ir.ChangeApplier, streamID string, target *liveAddedFilter) {
	reader, ok := applier.(liveAddedTablesReader)
	if !ok {
		return
	}
	tables, err := reader.ReadLiveAddedTables(ctx, streamID)
	if err != nil {
		slog.DebugContext(
			ctx, "live-added-tables seed read failed; poll will retry on first tick",
			slog.String("stream_id", streamID),
			slog.String("err", err.Error()),
		)
		return
	}
	if len(tables) == 0 {
		return
	}
	target.Set(tables)
	slog.InfoContext(
		ctx, "live-added tables observed at startup; merging into dispatch filter (ADR-0034)",
		slog.String("stream_id", streamID),
		slog.Any("tables", tables),
	)
}

// startLiveAddedTablesPoll wires the optional live-added-tables poll
// goroutine for ADR-0034. Mirrors [startStopSignalPoll]'s shape:
// engines that implement [liveAddedTablesReader] (MySQL) get a poll;
// engines that don't (PG, test stubs) skip cleanly.
//
// The poll runs for the duration of applyCtx — same lifetime as
// pollStopSignal — so it shuts down cleanly on graceful drain or
// Ctrl-C.
func (s *Streamer) startLiveAddedTablesPoll(applyCtx context.Context, applier ir.ChangeApplier, streamID string, target *liveAddedFilter) {
	reader, ok := applier.(liveAddedTablesReader)
	if !ok {
		slog.DebugContext(
			applyCtx, "live-added-tables poll skipped: applier does not implement ReadLiveAddedTables",
			slog.String("stream_id", streamID),
		)
		return
	}
	slog.DebugContext(
		applyCtx, "live-added-tables poll started",
		slog.String("stream_id", streamID),
	)
	go pollLiveAddedTables(applyCtx, reader, streamID, target)
}

// startStopSignalPoll wires the optional stop-signal poll goroutine
// when the applier supports it. The goroutine reads the control
// row's stop flag every few seconds; when set, it cancels streamCtx
// so the CDC reader's pump exits, the change channel closes, and
// the apply loop commits its in-flight partial batch via the
// channel-closed branch (Bug 15 CLI fix, ADR-0025). cancelApply is
// passed through to pollStopSignal as a hard-timeout fallback if
// the graceful drain doesn't complete in time.
//
// Test stubs that don't implement stopFlagReader skip the poll
// entirely — the existing Ctrl-C / ctx-cancel path remains the only
// way to stop those streams, which matches their pre-stop-signal
// behavior.
func (s *Streamer) startStopSignalPoll(applyCtx context.Context, applier ir.ChangeApplier, streamID string, cancelStream, cancelApply context.CancelFunc, observed *atomic.Bool) {
	reader, ok := applier.(stopFlagReader)
	if !ok {
		slog.DebugContext(
			applyCtx, "stop-signal poll skipped: applier does not implement ReadStopRequested",
			slog.String("stream_id", streamID),
		)
		return
	}
	slog.DebugContext(
		applyCtx, "stop-signal poll started",
		slog.String("stream_id", streamID),
	)
	go pollStopSignal(applyCtx, reader, streamID, cancelStream, cancelApply, observed)
}

// openApplier returns the applier to use plus a flag indicating
// whether the Streamer owns its lifecycle. Owns => Streamer must
// Close it. Borrowed => caller is responsible.
//
// The applier receives the operator-supplied `--target-schema`
// override (ADR-0031): user-data INSERT/UPDATE/DELETE land in the
// per-source schema, while `sluice_cdc_state` stays in the DSN's
// default schema. The engine-side [SchemaSetter] is the contract
// that splits the two — PG's applier preserves the original
// (control-table) schema before applying the override to the
// user-data schema.
func (s *Streamer) openApplier(ctx context.Context) (ir.ChangeApplier, bool, error) {
	if s.Applier != nil {
		// Pre-supplied appliers are typically test stubs whose
		// lifecycle the caller owns; we still hand them the byte cap
		// + target-schema override so a stub that wants to honour
		// them can. Real production callers leave Applier nil and
		// hit the OpenChangeApplier branch below.
		applyMaxBufferBytes(s.Applier, s.MaxBufferBytes)
		applyTargetSchema(s.Applier, s.TargetSchema)
		applyExecTimeout(s.Applier, s.ApplyExecTimeout)
		applyRedactor(s.Applier, s.Redactor)
		if err := checkShardColumnSupport(s.Applier, s.InjectShardColumn, "sync"); err != nil {
			return nil, false, wrapWithHint(PhaseConnect, err)
		}
		applyShardColumn(s.Applier, s.InjectShardColumn)
		// ADR-0054 Shape A Phase 2: engage live-coordination lease
		// manager when the operator's flags + target engine allow.
		if err := s.engageShardCoordination(ctx, s.Applier); err != nil {
			return nil, false, wrapWithHint(PhaseConnect, err)
		}
		// ADR-0058: engage single-stream ADD COLUMN forwarding when
		// the operator opts in and Shape A is NOT engaged. No-op
		// otherwise.
		if err := s.engageAddColumnForward(ctx); err != nil {
			return nil, false, wrapWithHint(PhaseConnect, err)
		}
		return s.Applier, false, nil
	}
	a, err := s.Target.OpenChangeApplier(ctx, s.TargetDSN)
	if err != nil {
		return nil, false, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target change applier: %w", err))
	}
	applyMaxBufferBytes(a, s.MaxBufferBytes)
	applyTargetSchema(a, s.TargetSchema)
	applyExecTimeout(a, s.ApplyExecTimeout)
	applyRedactor(a, s.Redactor)
	if err := checkShardColumnSupport(a, s.InjectShardColumn, "sync"); err != nil {
		closeIf(a)
		return nil, false, wrapWithHint(PhaseConnect, err)
	}
	applyShardColumn(a, s.InjectShardColumn)
	// ADR-0054 Shape A Phase 2: engage live-coordination lease
	// manager when the operator's flags + target engine allow.
	if err := s.engageShardCoordination(ctx, a); err != nil {
		closeIf(a)
		return nil, false, wrapWithHint(PhaseConnect, err)
	}
	// ADR-0058: engage single-stream ADD COLUMN forwarding when
	// the operator opts in and Shape A is NOT engaged. No-op
	// otherwise.
	if err := s.engageAddColumnForward(ctx); err != nil {
		closeIf(a)
		return nil, false, wrapWithHint(PhaseConnect, err)
	}
	return a, true, nil
}

// retagPositionForSource normalises a persisted ir.Position so its
// Engine field matches the source engine's name. The applier always
// stamps recovered positions with the applier's own (target's) engine
// name on the way out of the control table; on cross-engine resume
// the source CDC reader's decoder would reject the position with
// "wrong engine" because the tag refers to the target. Re-stamping
// here, before the position reaches the source CDC reader, makes
// every (source, target) pair round-trip cleanly through whichever
// decoder the source engine uses.
//
// The from-now sentinel (empty Engine and Token) is returned
// untouched — every CDC reader's decoder treats that pair as
// "start at the source's current position" and must not see a
// non-empty Engine tag. An otherwise-empty token paired with a
// non-empty engine is also passed through unchanged so the source
// decoder can surface the malformed-token error itself.
//
// See [Streamer.Run] for the call site and Bug 20 in CHANGELOG for
// the cross-engine pair this generalises (PlanetScale source →
// Postgres target).
func retagPositionForSource(persisted ir.Position, sourceEngine string) ir.Position {
	if persisted.Engine == "" && persisted.Token == "" {
		return persisted
	}
	if persisted.Token == "" {
		return persisted
	}
	persisted.Engine = sourceEngine
	return persisted
}
