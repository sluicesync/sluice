// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
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
		applyApplyConcurrency(s.Applier, s.resolvedApplyConcurrency)
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
	applyApplyConcurrency(a, s.resolvedApplyConcurrency)
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

// phaseResolveStreamIdentity is runOnce's opening phase: validate the
// streamer's field surface, reset per-attempt state, apply the
// slot-name prefix + engine-default table-exclusion conventions, and
// resolve the stream id. No source/target I/O happens here — only
// log lines.
func (s *Streamer) phaseResolveStreamIdentity(ctx context.Context) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	// ADR-0074 Phase 1b.2: surface multi-database flag-combo errors
	// before any I/O (mutually-exclusive scope flags, unsupported
	// combinations). The per-database snapshot + routing wiring lives in
	// coldStartMultiDatabase, reached via the dispatch switch below.
	if s.multiDatabaseMode() {
		if err := s.validateMultiDatabaseStream(); err != nil {
			return "", err
		}
	}

	// Reset the per-attempt source-error handle (GitHub #19). Each
	// iteration opens a fresh CDC reader; carrying a stale handle
	// from a previous attempt would surface an already-handled error.
	s.sourceErrFn = nil
	// ADR-0094: same per-attempt reset for the reshard-reopen handle.
	s.sourceReshard = nil

	// Apply the sluice-prefix convention to the operator-supplied
	// slot name (v0.10.2). Empty stays empty (engine default);
	// `shard_a` becomes `sluice_shard_a`; already-prefixed names
	// pass through. Mutated in place because Streamer is single-
	// shot per Run; the resolved name flows through to both the
	// CDC-reader and snapshot-stream open paths and surfaces in
	// log lines so operators can correlate against
	// pg_replication_slots.
	if resolved := resolveSlotName(s.SlotName); resolved != s.SlotName {
		slog.InfoContext(
			ctx, "applying sluice slot-name prefix convention",
			slog.String("operator_supplied", s.SlotName),
			slog.String("resolved", resolved),
		)
		s.SlotName = resolved
	}

	// Engine-default exclusions (Bug 22 / v0.8.1): merge in any
	// patterns the source engine surfaces via [ir.DefaultTableExcluder]
	// — today PlanetScale's `_vt_*` Vitess shadow tables, triggered
	// either by the planetscale flavor flag or by a vanilla-mysql DSN
	// pointing at a PlanetScale endpoint. Replaced in-place;
	// Streamer is single-shot per Run.
	if eff, added := effectiveTableFilter(s.Filter, s.Source, s.SourceDSN); len(added) > 0 {
		slog.InfoContext(
			ctx, "applying engine-default table exclusions",
			slog.String("engine", s.Source.Name()),
			slog.Any("patterns", added),
		)
		s.Filter = eff
	}

	streamID := s.resolveStreamID()
	slog.InfoContext(ctx, "stream starting", slog.String("stream_id", streamID))
	return streamID, nil
}

// phaseStartMetricsServer starts the optional Prometheus /metrics
// endpoint (---- 1a ----). Returns the running server (nil when
// --metrics-listen is unset or on DryRun) plus the spill-reporter
// cleanup closure the caller must defer — always non-nil so the defer
// is unconditional (a no-op when nothing was attached). On a Start
// failure the spill reporter is released HERE, because the caller's
// defer is never registered on the error path; that mirrors the
// pre-split inline defer ordering exactly.
func (s *Streamer) phaseStartMetricsServer(ctx context.Context, applier ir.ChangeApplier, aimdController *appliercontrol.Controller, streamID string) (*MetricsServer, func(), error) {
	if s.MetricsListen == "" || s.DryRun {
		return nil, func() {}, nil
	}
	metricsSrv, mErr := NewMetricsServer(s.MetricsListen, applier)
	if mErr != nil {
		return nil, func() {}, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: prepare metrics server: %w", mErr))
	}
	metricsSrv.SetBuildInfo(s.BuildVersion, s.BuildCommit)
	if aimdController != nil {
		metricsSrv.AttachAIMDController(aimdController)
	}
	// ADR-0104/0105 concurrent key-hash apply (item 31's default path): the
	// per-lane controllers have no single handle to thread through
	// aimdController, so maybeAttachAIMDController parks them on the streamer.
	// Attach them here so each lane surfaces as its own `lane="N"`-labeled
	// AIMD series. Serial path leaves this nil — byte-identical to before.
	if len(s.laneAIMDControllers) > 0 {
		metricsSrv.AttachLaneAIMDControllers(s.laneAIMDControllers)
	}
	// Severity-B finding F2 (2026-05-22 PG-internals research): when
	// the source supports it (PG 14+), attach a spill-stats reporter
	// so per-scrape `pg_stat_replication_slots.spill_*` counters are
	// surfaced as Prometheus metrics. A bind-time failure or an
	// unsupported source engine is non-fatal — the streamer keeps
	// running with the rest of the metric set; the spill lines just
	// don't appear in /metrics. See [attachSpillReporter].
	spillCleanup := s.attachSpillReporter(ctx, metricsSrv, streamID)
	// ADR-0107 use (c): re-export the optional target-health snapshot as
	// the sluice_target_* gauge family. nil provider ⇒ no attach ⇒ no
	// extra lines, byte-identical to before.
	if s.TargetTelemetry != nil {
		metricsSrv.AttachTargetTelemetry(s.TargetTelemetry)
	}
	// Roadmap item 45: expose the engine-neutral sync-lag gauge. The tracker
	// was created in runOnce before this phase (nil unless observation was
	// opted into); the apply-phase interceptor feeds it.
	if s.syncLag != nil {
		metricsSrv.AttachSyncLagSource(s.syncLag)
	}
	if mErr := metricsSrv.Start(); mErr != nil {
		spillCleanup()
		return nil, func() {}, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: start metrics server: %w", mErr))
	}
	slog.InfoContext(ctx, "metrics server listening", slog.String("addr", s.MetricsListen))
	return metricsSrv, spillCleanup, nil
}

// phasePrepareControlTable runs the control-table preamble (---- 2 →
// 2.7 ----): ensure the per-target control table exists, clear a
// stale stop signal, record the resolved slot name + target schema on
// the applier, and run the stream-id collision / source-fingerprint
// check — in that order. Each sub-step documents its own dry-run /
// optional-interface gating.
func (s *Streamer) phasePrepareControlTable(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	// ---- 2. Ensure the control table exists ----
	// Skip on dry-run — that's a write, and dry-run is read-only.
	// ReadPosition below tolerates a missing control table by
	// returning ok=false (same as "no row").
	if !s.DryRun && !s.SchemaAlreadyApplied {
		if err := applier.EnsureControlTable(ctx); err != nil {
			if s.multiDatabaseMode() {
				// ADR-0074 Phase 1b.2: in multi-database mode the target
				// DSN must name a "home" database for the per-target
				// sluice_cdc_state control table (user data routes to
				// per-source-database namespaces under it). A server-only
				// target DSN with no database has nowhere to put the
				// control table — name one and the per-source databases
				// still route correctly.
				return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
					"pipeline: ensure control table (multi-database mode): the target DSN must name a database "+
						"to host sluice_cdc_state (user data still routes to per-source-database namespaces): %w", err,
				))
			}
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: ensure control table: %w", err))
		}
	}

	// ---- 2.5. Clear any leftover stop signal from a previous run ----
	// Without this, `sluice sync stop` leaves stop_requested_at set
	// after the streamer drains and exits; the next `sync start`
	// would then see the stale flag and exit within the first poll
	// interval (Bug 11 in v0.3.2 testing). Skip on dry-run for the
	// same read-only reason as EnsureControlTable above.
	if !s.DryRun {
		if err := applier.ClearStopRequested(ctx, streamID); err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: clear stop signal: %w", err))
		}
	}

	// ---- 2.6. Record the active stream's resolved slot name on the
	// applier (Phase 2 mid-stream live add-table, ADR-0030).
	// SetSlotName is structural / optional — engines without slots
	// (MySQL: binlog stream is the slot) don't implement it, and the
	// nil check skips the call cleanly. The applier threads the slot
	// name into every subsequent writePositionTx call so the per-
	// target sluice_cdc_state row's slot_name column reflects what
	// the streamer is actually consuming. `sluice schema add-table
	// --no-drain` reads this back via ListStreams to look up the
	// right slot's confirmed_flush_lsn for its LSN-floor check.
	//
	// The slot name passed here is post-resolveSlotName: a custom
	// `--slot-name=shard_a` has already become `sluice_shard_a`.
	// Empty input means the engine default (`sluice_slot`); the
	// fallback lives in the add-table orchestrator's lookup, so we
	// pass the empty string through verbatim.
	if !s.DryRun {
		if setter, ok := applier.(slotNameSetter); ok {
			setter.SetSlotName(s.SlotName)
		}
		// Record the operator-supplied `--target-schema NAME` (Bug 46,
		// ADR-0031) on the applier so subsequent position-writes
		// populate the sluice_cdc_state row's target_schema column.
		// `sluice schema add-table` reads the column back to detect a
		// mismatch between operator-supplied flag and active stream's
		// recorded namespace. Engines without schema-vs-database
		// distinction (MySQL) don't implement; the validate gate
		// already refused --target-schema upstream for those engines.
		if setter, ok := applier.(targetSchemaSetter); ok {
			setter.SetTargetSchema(s.TargetSchema)
		}
	}

	// ---- 2.7. Stream-id collision detection + source-DSN fingerprint
	// recording (ADR-0031, Phase 2 of multi-source).
	// Computes the truncated SHA-256 of the source DSN's host+port+
	// database tuple, then:
	//   1. Lists existing streams; refuses if the stream-id row's
	//      recorded fingerprint differs from the new one (the typo /
	//      wrong-source case).
	//   2. Records the fingerprint on the applier so subsequent
	//      writePositionTx calls populate the sluice_cdc_state row's
	//      source_dsn_fingerprint column.
	// Skipped on DryRun (read-only; no fingerprint write expected).
	// Engines without fingerprint support no-op cleanly: an empty
	// fingerprint passes the collision check and the recorder type-
	// assertion fails closed.
	if !s.DryRun {
		fingerprint := fingerprintSourceDSN(s.SourceDSN)
		if fingerprint != "" {
			existing, err := applier.ListStreams(ctx)
			if err != nil {
				return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: list streams for fingerprint check: %w", err))
			}
			if err := checkStreamIDCollision(streamID, fingerprint, existing); err != nil {
				return wrapWithHint(PhaseSchemaApply, err)
			}
			applySourceFingerprint(applier, fingerprint)
		}
	}
	return nil
}

// phaseLookupPosition resolves the stream's starting position
// (---- 3 ----) and reports whether one was found (warm resume) or
// not (cold start). The position-source priority and the cross-engine
// re-stamp rationale (Bug 20) are documented inline.
func (s *Streamer) phaseLookupPosition(ctx context.Context, applier ir.ChangeApplier, streamID string) (ir.Position, bool, error) {
	// Position source priority (highest to lowest):
	//   1. PositionFromManifestStore (Phase 3.3.B). When non-nil, the
	//      chain's terminal manifest's EndPosition replaces both the
	//      ReadPosition lookup and the cold-start fall-through. The
	//      operator passing `--position-from-manifest` has explicitly
	//      asked for chain handoff; a slot-missing fall-through to
	//      cold-start would silently re-bulk and defeat the point.
	//   2. applier.ReadPosition (warm resume). Existing v0.3.x flow.
	//   3. Cold start. The default when neither of the above is set.
	var (
		persisted ir.Position
		found     bool
	)
	if s.PositionFromManifestStore != nil {
		chainPos, err := LoadChainTerminalPosition(ctx, s.PositionFromManifestStore)
		if err != nil {
			return ir.Position{}, false, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: %w", err))
		}
		// Run Phase 3.3.C pre-flight checks before opening CDC. PG-only
		// today; MySQL has no operator-attention surface here. Refuses
		// when a check is fatal (slot lost / missing); warns otherwise
		// (or refuses on warning when StrictPreflight is set).
		if err := s.runPositionFromManifestPreflight(ctx, chainPos); err != nil {
			return ir.Position{}, false, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: position-from-manifest preflight: %w", err))
		}
		persisted = retagPositionForSource(chainPos, s.Source.Name())
		found = true
		slog.InfoContext(
			ctx, "position-from-manifest: using chain terminal position",
			slog.String("stream_id", streamID),
			slog.String("position_engine", chainPos.Engine),
			slog.String("position_token", truncateDryRunToken(chainPos.Token, 60)),
		)
	} else {
		var err error
		persisted, found, err = applier.ReadPosition(ctx, streamID)
		if err != nil {
			return ir.Position{}, false, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: read position: %w", err))
		}
		// The applier stamps every row it reads back with the applier's
		// own engine name (target's engine), but the position itself is a
		// source-side artifact (a MySQL GTID set, a Postgres LSN). On
		// cross-engine resume — e.g. source=planetscale, target=postgres —
		// the source CDC reader's decoder rejects the position because its
		// Engine tag matches the target instead of the source. v0.1.0's
		// Bug 2 fix patched the same-family case (PS↔MySQL) by widening
		// the MySQL decoder's engine acceptance, but didn't generalise to
		// truly cross-engine pairs (Bug 20). Re-stamping with the source
		// engine's own name here makes every (source, target) pair
		// round-trip cleanly through its source decoder, including
		// PS-source → PG-target.
		if found {
			persisted = retagPositionForSource(persisted, s.Source.Name())
		}
	}
	return persisted, found, nil
}

// sourceAutoResnapshotOnInvalidPosition reports whether a purged/invalid resume
// position is a ROUTINE, auto-recoverable event for this source — so the
// proactive warm-resume → ir.ErrPositionInvalid fall-through should
// auto-resnapshot (forceFresh) rather than refuse. True for GTID/binlog sources
// (vanilla MySQL CDCBinlog, Vitess/PlanetScale CDCVStream), where the
// binlog/GTID retention window legitimately advances past an old snapshot
// position (ADR-0093; the live Track-B/D finding). FALSE for logical-
// replication-slot sources (PG CDCLogicalReplication) and trigger CDC
// (CDCTriggers): a lost PG slot is an abnormal failover/config event, not a
// routine purge, so the fall-through refuses LOUDLY and leaves the
// data-preserving recovery (--reset-target-data / --force-cold-start) to a
// deliberate operator choice — the ADR-0075 Phase 2b rig-confirmed contract,
// pinned by TestStreamer_MultiSchema_SlotLossRefusesLoudly. The operator's
// explicit --restart-from-scratch is unaffected (it forces fresh for any
// engine); this only governs the AUTOMATIC fall-through.
func sourceAutoResnapshotOnInvalidPosition(source ir.Engine) bool {
	switch source.Capabilities().CDC {
	case ir.CDCBinlog, ir.CDCVStream:
		return true
	default:
		return false
	}
}

// phaseOpenChangeStream is the ---- 4 ---- dispatch: cold start vs
// warm resume vs multi-database fan-out, including the ADR-0022
// slot-missing fall-through and the v0.99.8 interrupted-COPY resume
// routing. streamCtx scopes the CDC reader's pump; ctx (the parent)
// is used for the operator-facing fall-through log lines, matching
// the pre-split inline behaviour.
//
// stop is always non-nil — every dispatch branch assigns it (the
// branches' own error paths clean up inline and hand back a no-op) —
// so the caller's unconditional deferred `stop()` closes whichever
// reader/stream the winning branch opened. warmResumed reports
// whether the apply loop is about to consume from a CDC reader opened
// at the persisted position — the ADR-0049 Chunk C cache-prime
// discriminator (see the comment kept on the declaration this named
// return replaced, now at the call site).
func (s *Streamer) phaseOpenChangeStream(ctx, streamCtx context.Context, lsnTracker any, applier ir.ChangeApplier, streamID string, persisted ir.Position, found bool) (changes <-chan ir.Change, stop func(), warmResumed bool, err error) {
	// warmResumed tracks whether the apply loop is about to consume
	// from a CDC reader opened at the persisted position (vs. a fresh
	// post-snapshot reader). The ADR-0049 Chunk C cache prime keys on
	// this discriminator: only a true warm-resume primes from
	// storage; every cold-start path (initial, --reset-target-data
	// recovery, or warm-resume → ErrPositionInvalid fall-through) is
	// brand-new-stream-equivalent and skips the prime.
	// resumeCopyFrom is the interrupted-cold-start resume cursor (v0.99.8).
	// It is non-zero only when ALL of: a position is persisted (found), the
	// source engine implements the resumer surface, and the persisted
	// position carries a mid-COPY TablePKs cursor. In that case the resume
	// must route through coldStart's bulk-COPY path seeded from the cursor
	// — NOT through warmResume's plain CDC reader, which would apply the
	// un-copied COPY tail one row at a time (~10 rows/sec, the silent
	// degrade this fixes). A cursor-less persisted position (completed
	// cold-start) leaves this zero and stays on the fast warmResume path.
	var resumeCopyFrom ir.Position
	if found && !s.ResetTargetData && !s.RestartFromScratch {
		if resumer, ok := s.Source.(ir.SnapshotStreamResumer); ok && resumer.PositionCarriesCopyCursor(persisted) {
			resumeCopyFrom = persisted
		}
	}
	switch {
	case s.multiDatabaseMode():
		// ADR-0074 Phase 1b: multi-database fan-out. The cold-start path
		// (1b.2) captures ONE spanning consistent snapshot across the
		// selected databases → per-namespace bulk-copy → single server-wide
		// binlog CDC routed per-change. The warm-resume path (1b.3) skips
		// the snapshot+copy entirely: it re-resolves the selected database
		// set, opens a bare server-wide CDC reader, re-scopes it to the set,
		// enables routing, and resumes the single server-wide binlog from
		// the one persisted position.
		//
		// --reset-target-data / --restart-from-scratch are the explicit
		// re-cold-start overrides; they bypass warm-resume (handled inside
		// coldStartMultiDatabase, which ignores the persisted position) so a
		// fresh multi-database cold-start runs even when a position exists.
		// Ordering mirrors the single-database dispatch above: the
		// destructive/force-fresh flags win over a persisted position.
		switch {
		case s.ResetTargetData, s.RestartFromScratch:
			// forceFresh = RestartFromScratch (ResetTargetData has its own
			// destructive drop+clear branch inside; restart re-copies onto the
			// populated target).
			changes, stop, err = s.coldStartMultiDatabase(streamCtx, lsnTracker, applier, streamID, s.RestartFromScratch)
		case found:
			changes, stop, err = s.warmResumeMultiDatabase(streamCtx, persisted, lsnTracker, applier, streamID)
			warmResumed = err == nil
			// Slot-missing fall-through (ADR-0022), multi-database analogue:
			// if the persisted server-wide position references binlog the
			// source has since purged, the reader returns an error wrapping
			// [ir.ErrPositionInvalid]. The only path forward is a fresh
			// multi-database cold-start (re-snapshot across the selected set).
			// Mirrors the single-database branch above; Bug 9's preflight
			// still gates destructive dest-table operations.
			if err != nil && errors.Is(err, ir.ErrPositionInvalid) {
				// ADR-0093: --no-auto-resnapshot suppresses the ADR-0022
				// pre-flight fall-through too (kept consistent with the
				// reactive path), surfacing a loud actionable terminal error
				// instead of an automatic re-snapshot. Close the warmResume
				// reader here and return a NO-OP stop (NOT nil) — runOnce's
				// `defer func(){ stop() }()` calls the returned stop, so nil
				// would panic.
				if s.SuppressAutoResnapshotOnInvalidPosition {
					stop()
					return nil, func() {}, false, invalidPositionOptOutError(err)
				}
				slog.WarnContext(
					ctx, "multi-database warm resume: persisted position is no longer valid; falling through to cold start",
					slog.String("stream_id", streamID),
					slog.String("position_token", persisted.Token),
					slog.String("source_engine", persisted.Engine),
					slog.String("err", err.Error()),
				)
				stop()
				// Auto-resnapshot re-copies onto the populated target (its
				// persisted server-wide position was purged) — but ONLY for
				// GTID/binlog sources where a purge is routine. A PG
				// logical-slot source (CDCLogicalReplication) gets forceFresh
				// =false here so the gate refuses LOUDLY (the ADR-0075 Phase 2b
				// deliberate-recovery contract; TestStreamer_MultiSchema_
				// SlotLossRefusesLoudly). Same engine-aware gate as single-DB.
				changes, stop, err = s.coldStartMultiDatabase(streamCtx, lsnTracker, applier, streamID, sourceAutoResnapshotOnInvalidPosition(s.Source))
				warmResumed = false
			}
		default:
			changes, stop, err = s.coldStartMultiDatabase(streamCtx, lsnTracker, applier, streamID, false)
		}
	case s.ResetTargetData:
		changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID, ir.Position{}, false)
	case s.RestartFromScratch:
		// Force a fresh cold-start from row 0, ignoring any persisted
		// position (incl. a mid-COPY cursor). The cold-start gate
		// (coldStartGatePreflight) makes the re-copy land cleanly per source:
		// an idempotent reader (VStream/PlanetScale, PG) keeps its tables and
		// absorbs the re-copied overlap via UPSERT; a non-idempotent reader
		// (native MySQL binlog, plain INSERT) has its in-scope target tables
		// dropped + recreated first so the copy doesn't dup-key (Error 1062)
		// on the prior copy's rows. Either way the cdc-state row is preserved
		// (only --reset-target-data clears it). warmResumed stays false (a
		// fresh cold-start resets effective schema-history state), so the
		// cache prime gets the brand-new-stream sentinel below.
		slog.InfoContext(
			ctx, "restart-from-scratch: forcing a fresh cold-start, ignoring the persisted position",
			slog.String("stream_id", streamID),
		)
		changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID, ir.Position{}, true)
	case resumeCopyFrom.Token != "" || resumeCopyFrom.Engine != "":
		// Interrupted cold-start: resume the bulk COPY from the persisted
		// cursor (seeded snapshot stream → batched bulk-COPY writer), then
		// transition to CDC exactly as a fresh cold-start does. The target
		// keeps its partial copy; the idempotent COPY writer absorbs the
		// overlap. warmResumed stays false: coldStart's bulk-copy resets
		// the applier's effective schema-history state, same as a fresh
		// cold-start, so the schema-history cache prime gets the
		// brand-new-stream sentinel below.
		slog.InfoContext(
			ctx, "persisted position carries a mid-COPY cursor; resuming interrupted cold-start via the bulk path",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 60)),
		)
		changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID, resumeCopyFrom, false)
	case found:
		changes, stop, err = s.warmResume(streamCtx, persisted, lsnTracker)
		warmResumed = err == nil
		// Slot-missing fall-through (ADR-0022) is suppressed when the
		// position came from a manifest chain: the operator explicitly
		// asked for "resume from this chain"; silently re-bulking would
		// defeat the chain handoff. Surface the error verbatim so the
		// operator gets the slot-recovery flow's message; cases 7/8
		// from the design doc cover the recovery options.
		if err != nil && errors.Is(err, ir.ErrPositionInvalid) && s.PositionFromManifestStore == nil {
			// ADR-0093: --no-auto-resnapshot suppresses the ADR-0022
			// pre-flight fall-through too (kept consistent with the reactive
			// path), surfacing a loud actionable terminal error instead of an
			// automatic re-snapshot. The PositionFromManifestStore == nil
			// guard above keeps the backup-chain case on its own
			// surface-verbatim path regardless of this flag. Close the
			// warmResume reader here and return a NO-OP stop (NOT nil) —
			// runOnce's `defer func(){ stop() }()` calls the returned stop,
			// so nil would panic.
			if s.SuppressAutoResnapshotOnInvalidPosition {
				stop()
				return nil, func() {}, false, invalidPositionOptOutError(err)
			}
			slog.WarnContext(
				ctx, "warm resume: persisted position is no longer valid; falling through to cold start",
				slog.String("stream_id", streamID),
				slog.String("position_token", persisted.Token),
				slog.String("source_engine", persisted.Engine),
				slog.String("err", err.Error()),
			)
			// warmResume failed; its stop is the no-op (it cleaned up
			// its reader inline). coldStart's stop supersedes it.
			stop()
			// Auto-resnapshot re-copies onto the EXISTING (populated) target
			// whose persisted position was purged — but ONLY for GTID/binlog
			// sources (CDCBinlog/CDCVStream) where a purge is routine: there
			// forceFresh suppresses the populated-target refusal that dead-ended
			// the live Track-B/D run (idempotent readers absorb via UPSERT;
			// native MySQL drops + recreates first). A PG logical-slot source
			// (CDCLogicalReplication) gets forceFresh=false so the gate refuses
			// LOUDLY — the ADR-0075 Phase 2b deliberate-recovery contract
			// (TestStreamer_MultiSchema_SlotLossRefusesLoudly). The operator's
			// explicit --restart-from-scratch is unaffected (forces fresh for
			// any engine); this governs only the AUTOMATIC fall-through.
			changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID, ir.Position{}, sourceAutoResnapshotOnInvalidPosition(s.Source))
			// coldStart supersedes the warm resume — schema-history
			// stays brand-new from the applier's perspective (the
			// snapshot bulk-copy reset effective state).
			warmResumed = false
		}
	default:
		changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID, ir.Position{}, false)
	}
	return changes, stop, warmResumed, err
}

// phasePrimeSchemaHistoryCache primes the applier's ADR-0049 Chunk C
// active-version cache (---- 4b ----). warmResumed selects the
// persisted position; every cold-start path passes the
// brand-new-stream sentinel (empty Position), which the engine
// short-circuits to a no-op. See the call site in [Streamer.runOnce]
// for the loud-floor / optional-interface rationale.
func (s *Streamer) phasePrimeSchemaHistoryCache(ctx context.Context, applier ir.ChangeApplier, streamID string, warmResumed bool, persisted ir.Position) error {
	if primer, ok := applier.(schemaHistoryCachePrimer); ok {
		var primePos ir.Position
		if warmResumed {
			primePos = persisted
		}
		if err := primer.PrimeSchemaHistoryCache(ctx, streamID, primePos); err != nil {
			return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: prime schema-history cache: %w", err))
		}
	}
	return nil
}

// phaseStartApplySidecars wires the apply loop's sidecar goroutines:
// the stop-signal poll (Bug 15 CLI fix, ADR-0025), the ADR-0034
// live-added-tables seed + poll, and the GitHub #23 Phase A
// heartbeat. Returns the live-added filter the dispatch-side change
// filter merges additively. All three run for the duration of
// applyCtx and shut down with it.
func (s *Streamer) phaseStartApplySidecars(applyCtx context.Context, applier ir.ChangeApplier, streamID string, cancelStream, cancelApply context.CancelFunc, stopObserved *atomic.Bool) *liveAddedFilter {
	s.startStopSignalPoll(applyCtx, applier, streamID, cancelStream, cancelApply, stopObserved)

	// ---- 4a. Live-added-tables poll for ADR-0034 (MySQL Phase 2
	// mid-stream live add-table). The orchestrator's
	// `sluice schema add-table --no-drain TABLE` writes the new table
	// into the per-target sluice_cdc_state row's live_added_tables
	// column; the poll goroutine here picks that up on its 5s tick
	// cadence and merges into the dispatch filter additively. The
	// liveAddedFilter is also seeded once at startup so a streamer
	// restart after a partial live-add picks up the previously-recorded
	// additions before any events flow.
	//
	// Engines without the surface (PG, in-memory test stubs) skip both
	// the seed and the poll cleanly — type assertion fails and the
	// dispatch filter sees an empty live-added set forever, preserving
	// pre-v0.27.0 behaviour.
	liveFilter := &liveAddedFilter{}
	s.seedLiveAddedFilter(applyCtx, applier, streamID, liveFilter)
	s.startLiveAddedTablesPoll(applyCtx, applier, streamID, liveFilter)

	// GitHub #23 Phase A heartbeat: a periodic INFO log line so a
	// silent stall (process alive, no apply, no log) is
	// distinguishable from a wedge (process alive, no apply, no
	// heartbeat either). Operators on default log level see the
	// stream is alive; the absence of these lines combined with no
	// `applier: batch` lines is the silent-stall signature.
	startHeartbeat(applyCtx, streamID, s.HeartbeatInterval)

	// ADR-0107 Phase 1 (b): storage-resize anticipation. When a telemetry
	// provider is wired, a slow-tick sidecar warns ONCE per crossing when
	// the target's storage volume approaches capacity, so an operator can
	// anticipate the resize/reparent that items 30/33 ride through. No
	// provider ⇒ no goroutine; it never pauses or gates the stream. The
	// apply phase has no cold-copy lanes, so it passes a nil gate (ADR-0110):
	// WARN-only, exactly as before; the coordinated pause is a cold-copy-
	// phase concern (runColdStartParallel wires its own gated watch).
	s.startStorageHeadroomWatch(applyCtx, streamID, nil)

	// Roadmap item 45: the engine-neutral sync-lag threshold alerter. No
	// tracker / no threshold / no sink ⇒ no goroutine. UNGATED from
	// PlanetScale telemetry — fires on sluice's own apply lag on any engine.
	// Observability only — a dead sink is logged-and-swallowed, never able to
	// stall or crash the stream.
	s.startSyncLagNotifier(applyCtx, streamID)

	// ADR-0107 items 35 (rolling-history recorder) + 36 (threshold alerter)
	// are NO LONGER started here — they are started earlier, in runOnce (see
	// startTelemetrySidecars), so they cover the COLD-COPY phase too (the
	// loaded, storage-grow-prone window where they matter most), not just CDC
	// apply (roadmap item 39). The applier opened in runOnce step 1 lives for
	// the whole attempt, so a single start spans both phases.
	return liveFilter
}

// startTelemetrySidecars starts the ADR-0107 target-telemetry CONSUMERS that
// should run for the WHOLE stream attempt — the item-35 rolling-history
// recorder and the item-36 threshold alerter — so they cover the cold-copy
// phase as well as CDC apply (roadmap item 39; before this they started only
// at phaseStartApplySidecars, leaving cold-copy — where the target is under
// the heaviest write load and storage grows happen — uncovered).
//
// It is called ONCE per attempt from runOnce, right after the applier is open
// (step 1) and the stream-id is resolved. The applier lives for the whole
// attempt (runOnce's defer), so reusing it here is safe; during cold-copy the
// applier connection is idle (no apply loop yet), so the recorder's metadata
// writes don't contend. ctx is a run-scoped context cancelled when the attempt
// returns, so both goroutines exit cleanly (no cross-attempt leak on a
// warm-resume loop).
//
// Both consumers are individually no-ops when their preconditions aren't met
// (nil provider / no store impl / opted-out / no sink+rule), so this is a
// total no-op for a sync that hasn't configured PlanetScale telemetry.
func (s *Streamer) startTelemetrySidecars(ctx context.Context, applier ir.ChangeApplier, streamID string) {
	// Item 35: rolling-history recorder. No provider / no store impl /
	// --suppress-target-metrics-history ⇒ no goroutine. Advisory; every error
	// logged at WARN and swallowed.
	s.startTargetMetricsHistoryRecorder(ctx, streamID, applier, s.TargetTelemetry)

	// Item 36: sync-scoped threshold alerter. No provider / no sink / no rule
	// ⇒ no goroutine. Observability only — a dead sink is logged-and-swallowed,
	// never able to stall or crash the stream.
	s.startTargetMetricsNotifier(ctx, streamID, applier, s.TargetTelemetry, s.buildMetricsNotifier())
}

// phaseWireInterceptChain wraps the raw change channel with the
// dispatch-side filter + intercept stack: the live-add-aware table
// filter, the ADR-0054 Shape-A SchemaSnapshot coordination intercept,
// and the ADR-0058 single-stream ADD COLUMN forwarding intercept
// (mutually exclusive with Shape A). The cold-start seed snapshots
// are handed to both intercepts and cleared here so a streamer
// restart picks up a fresh seed in its next coldStart run.
func (s *Streamer) phaseWireInterceptChain(applyCtx context.Context, changes <-chan ir.Change, liveFilter *liveAddedFilter, streamID string) <-chan ir.Change {
	filtered := filterChangesWithLiveAdd(applyCtx, changes, s.Filter, liveFilter)
	// ADR-0054 Phase 2d: when live coordination is engaged, intercept
	// SchemaSnapshot events to route through the lease + per-shape
	// applier + probe before forwarding to the downstream applier.
	// Nil router (drained model / engine doesn't support coordination)
	// makes the intercept a verbatim pass-through.
	filtered = interceptSchemaSnapshotsForCoordination(applyCtx, filtered, s.coldStartSeedSnapshots, s.boundaryRouter, &s.schemaSnapshotErr)
	// ADR-0058: when --forward-schema-add-column is set AND Shape A is
	// NOT engaged, wrap the changes channel with the
	// [interceptAddColumnForward] intercept. The intercept observes
	// SchemaSnapshot boundaries, applies the target ALTER for ADD
	// COLUMN, optionally backfills, and refuses loudly on every other
	// recognized shape. When Shape A IS engaged, the boundary router
	// above already handles every shape — this branch is skipped.
	//
	// The cold-start seed (s.coldStartSeedSnapshots) is consumed here
	// when Shape A is NOT engaged — the Shape-A intercept above ignores
	// the seed when router==nil. Bug 89 fix: without this hand-off, the
	// intercept's per-table cache stays empty until the first DDL
	// boundary, and MySQL's CDC reader (unlike PG's pgoutput) emits
	// SchemaSnapshot only AFTER DDL — so the first ALTER silently
	// passes through as the anchor rather than being classified and
	// forwarded.
	if s.forwardSchemaEnabled() && s.boundaryRouter == nil && s.addColumnForwardWriter != nil {
		if deltaApplier, ok := s.addColumnForwardWriter.(ir.ShapeDeltaApplier); ok {
			deps := schemaForwardDeps{
				applier:          deltaApplier,
				sourceEngineName: s.Source.Name(),
				targetEngineName: s.Target.Name(),
			}
			if s.addColumnForwardSchemaReader != nil {
				deps.defaultProber = newSourceDefaultProber(s.addColumnForwardSchemaReader)
			}
			if s.BackfillAddedColumn {
				if br, ok := s.addColumnForwardReader.(ir.BatchedRowReader); ok {
					deps.backfill = &schemaForwardBackfill{
						reader:    br,
						streamID:  streamID,
						batchSize: defaultBulkBatchSize,
					}
				}
			}
			filtered = interceptAddColumnForward(applyCtx, filtered, s.coldStartSeedSnapshots, deps, &s.schemaSnapshotErr)
		}
	}
	// Clear the cold-start seed after handing it to BOTH intercepts so
	// a streamer restart picks up a fresh seed in its next coldStart
	// run.
	s.coldStartSeedSnapshots = nil
	// Roadmap item 46 (ADR-0121): the delayed-replica gate. When
	// --apply-delay is set, hold each change until its source commit
	// timestamp + delay has elapsed before forwarding it to the applier (the
	// MASTER_DELAY "oops window" DR pattern). It sits UPSTREAM of the applier,
	// so a held-but-unapplied change never advances the durable resume
	// position (ADR-0007) — a crash mid-delay-window re-reads the held tail on
	// resume (exactly-once via ADR-0010). Held changes backpressure to the
	// source read rather than accumulating in heap. Off by default (0 ⇒ no
	// gate, no extra goroutine, default apply path byte-identical). It is
	// wired BEFORE the sync-lag observer below so the observer sees the
	// post-delay timestamp and the tracker subtracts ApplyDelay back out (the
	// intentional delay is not "falling behind"; ADR-0121 §5).
	if s.ApplyDelay > 0 {
		filtered = delayChanges(applyCtx, filtered, s.ApplyDelay, time.Now, realSleep)
	}
	// Roadmap item 45: final pass-through that feeds the sync-lag tracker
	// from each change's source commit timestamp. Wired only when the
	// operator opted into the metrics endpoint or a sync-lag alert (nil
	// tracker ⇒ no extra goroutine, default path byte-identical). It sits
	// LAST so it observes exactly what reaches the applier across the
	// batched, per-change, and concurrent-lane paths.
	if s.syncLag != nil {
		filtered = observeSyncLagChanges(applyCtx, filtered, s.syncLag)
	}
	return filtered
}

// phaseSettleDispatch classifies the apply loop's outcome after
// [Streamer.dispatchApply] returns: surface a SchemaSnapshot
// intercept error stored by the ADR-0054/ADR-0058 intercepts,
// classify the dispatch error (Bug 57: retriable-before-ctx-
// termination ordering), surface a source CDC reader pump error
// (GitHub issue #19), and clear the stop flag after a stop-signal-
// driven graceful drain. ctx is the OUTER context — applyCtx may
// already be cancelled by the time the drain completes.
func (s *Streamer) phaseSettleDispatch(ctx context.Context, applier ir.ChangeApplier, streamID string, dispatchErr error, stopObserved *atomic.Bool) error {
	// ADR-0054 Phase 2d: a SchemaSnapshot intercept error short-
	// circuits the changes channel; the dispatchErr path sees a clean
	// close. Surface the intercept's stored error here so the
	// streamer's standard error-classification path picks it up.
	if dispatchErr == nil {
		if snapErrPtr := s.schemaSnapshotErr.Load(); snapErrPtr != nil && *snapErrPtr != nil {
			dispatchErr = *snapErrPtr
		}
	}
	if dispatchErr != nil {
		// Bug 57 fix (v0.52.2): a wrapped [ir.RetriableError] containing
		// context.DeadlineExceeded (from --apply-exec-timeout) MUST
		// surface to runWithRetry so the existing retry loop activates.
		// Pre-v0.52.2 the bare errors.Is checks on Canceled/Deadline
		// matched via Unwrap and swallowed the timeout as clean
		// shutdown, defeating the v0.52.0/v0.52.1 silent-stall fix.
		// Order matters: check retriable first, fall through only when
		// genuine ctx termination AND not retriable.
		_, isRetriable := classifyRetriable(dispatchErr)
		isCtxTermination := errors.Is(dispatchErr, context.Canceled) || errors.Is(dispatchErr, context.DeadlineExceeded)
		if isRetriable || !isCtxTermination {
			return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: apply changes: %w", dispatchErr))
		}
		// Bare ctx termination from outer shutdown — fall through to the
		// stop-cleanup path below; runOnce returns nil.
	}
	// GitHub issue #19: if the changes channel closed because the
	// source CDC reader's pump hit a transient error (the channel-
	// close path also fires on clean ctx-cancel and on the operator's
	// graceful-stop signal, both of which are nil-Err cases), surface
	// the wrapped error so [runWithRetry] classifies it as
	// [ir.RetriableError] and retries. Pre-v0.46.0 this exited 0
	// silently — a `read: connection reset` from the source mid-
	// stream looked indistinguishable from a normal EOF to the
	// applier.
	if dispatchErr == nil {
		if srcErr := surfaceSourceError(s.sourceErrFn); srcErr != nil {
			return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: source cdc reader: %w", srcErr))
		}
	}
	// On a stop-signal-driven graceful drain, clear stop_requested_at
	// so a CLI `sync stop --wait` polling for completion sees the
	// cleared flag and returns success. Use the outer ctx because
	// applyCtx may already be cancelled here.
	if stopObserved.Load() {
		if err := applier.ClearStopRequested(ctx, streamID); err != nil {
			slog.WarnContext(
				ctx, "failed to clear stop_requested_at after graceful drain; sync stop --wait may time out",
				slog.String("stream_id", streamID),
				slog.String("error", err.Error()),
			)
		}
	}
	return nil
}
