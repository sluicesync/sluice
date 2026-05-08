// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
)

// publicationEnsurer is the optional engine-side surface for engines
// that need a publication (or analogous CDC-source-side scope object)
// established before snapshot capture / CDC start. Postgres
// implements it (Bug 13, ADR-0021); MySQL does not.
//
// Tables is the post-filter source-table list — schema-qualifying is
// the engine's job. Empty tables means "fall back to the engine's
// default scope" (FOR ALL TABLES on PG); the streamer never passes
// nil — when the schema is empty, [coldStart] returns before this
// is called.
type publicationEnsurer interface {
	EnsurePublication(ctx context.Context, dsn string, tables []string) error
}

// lsnTrackerProvider is the optional applier-side surface for
// engines that produce applied-LSN feedback (Bug 15, ADR-0020). The
// applier owns the tracker; the streamer fetches it via this
// interface and hands it to the matching CDC reader via
// [lsnTrackerAttacher].
//
// Returns an opaque value (typed `any`) so the pipeline package
// stays free of engine-specific types. The matching CDC reader
// type-asserts internally — only same-engine pairs (PG applier ↔
// PG reader) actually wire anything; cross-engine pairs harmlessly
// hand an unrelated value to the attacher and the attacher's type-
// assertion fails closed.
type lsnTrackerProvider interface {
	LSNTracker() any
}

// lsnTrackerAttacher is the optional CDC-reader-side surface for
// engines that consume applied-LSN feedback (Bug 15, ADR-0020). On
// a successful type-assertion of the opaque tracker to its native
// shape, the reader keeps a pointer and uses it on its keepalive
// path; on failure it ignores the value and falls back to streamed-
// LSN keepalives.
type lsnTrackerAttacher interface {
	AttachLSNTracker(t any)
}

// Streamer is the long-running orchestrator: it captures a consistent
// source snapshot (cold start) or resumes from a previously-persisted
// position (warm resume), runs the bulk-copy phase if needed, then
// streams ongoing changes to a [ir.ChangeApplier] until ctx is
// cancelled.
//
// Each applied change writes its source position into the target's
// sluice_cdc_state table inside the same transaction as the data
// write — progress and data move together, per ADR-0007. A restart
// looks up the persisted position and skips the snapshot+bulk-copy
// phase entirely; combined with the applier's idempotency on retry,
// every event lands on the target exactly once.
//
// The simple-mode counterpart is [Migrator]; the two share the
// schema-apply + bulk-copy phases via [runBulkCopy] and diverge
// after that step (Migrator returns; Streamer keeps streaming).
type Streamer struct {
	// Source is the engine the source DSN belongs to. Must declare
	// CDC support (Capabilities().CDC != ir.CDCNone).
	Source ir.Engine

	// Target is the engine the target DSN belongs to. May be the
	// same as Source for same-engine streams.
	Target ir.Engine

	// SourceDSN, TargetDSN are the engine-native connection strings.
	SourceDSN string
	TargetDSN string

	// StreamID is the position-table key for this stream. When
	// empty, the Streamer auto-generates one from source+target
	// engine names and DSN host info. Operator-supplied IDs let
	// multiple concurrent streams share a target without clobbering
	// each other's position.
	StreamID string

	// Applier is optional. When nil, the Streamer auto-opens one
	// via Target.OpenChangeApplier(ctx, TargetDSN). Tests inject a
	// stub; production callers leave it nil. When non-nil, the
	// Streamer assumes the caller owns the applier's lifecycle and
	// does NOT call Close on it.
	Applier ir.ChangeApplier

	// Mappings is the per-column type-override list from sluice.yaml.
	// Consumed only on the cold-start path, where the schema-apply
	// phase needs the rewritten types. Warm resume reuses the target
	// schema as-is, so the field is ignored on that branch.
	Mappings []config.Mapping

	// ExpressionMappings is the per-column generated-expression
	// override list. Same cold-start-only consumption as Mappings.
	// See [Migrator.ExpressionMappings] for the rationale and
	// ADR-0016 §"Added in v0.10.0".
	ExpressionMappings []config.ExpressionMapping

	// SlotName, when non-empty, overrides the engine's default
	// replication-slot name on engines that have a slot concept
	// (Postgres). Engines without slots (MySQL: binlog stream is
	// the slot) silently ignore this field. Used to run multiple
	// concurrent sluice instances against the same source —
	// without a per-instance slot name they'd collide on the
	// hard-coded `sluice_slot` default. v0.10.2.
	SlotName string

	// DryRun, when true, prints what Run would do (cold-start vs
	// warm-resume, source schema summary or persisted-position
	// token) and returns without opening the snapshot stream,
	// applying any data, or modifying the target's control table.
	// Symmetric with the Migrator's existing DryRun flag.
	//
	// The position lookup against the target's control table still
	// happens — that's a read, not a write, and it's the only way
	// to tell the operator "this is a cold start" vs "this would
	// resume from <position>". The control table itself is NOT
	// created on dry-run; the lookup uses the tolerant readPosition
	// path that returns "no row" when the table doesn't exist yet.
	DryRun bool

	// Filter selects which source tables participate in the
	// stream. Applied to the cold-start schema (so bulk-copy and
	// schema-apply only see allowed tables) and to the dispatch
	// loop (so CDC events for excluded tables are dropped before
	// the applier sees them). The empty filter keeps every table.
	//
	// Caveat: position only advances when an event is applied. A
	// stream that consists entirely of dropped events for a long
	// time accumulates position lag bounded by the source-side
	// WAL/binlog retention. In practice every workload mixes
	// allowed and dropped events and the next applied event
	// advances the position past the dropped ones.
	Filter TableFilter

	// ViewFilter selects which source views are created on the
	// target during the cold-start phase. CDC events for views
	// don't exist (views aren't replicated by either engine's CDC
	// surface), so this filter only affects the schema-apply step.
	// Same shape as [Filter] for views.
	ViewFilter ViewFilter

	// SkipViews, when true, drops every view from the schema before
	// any phase runs. Useful when the operator manages views
	// out-of-band (Atlas/sqitch/liquibase) and doesn't want sluice
	// to round-trip the definitions on cold-start.
	SkipViews bool

	// ForceColdStart, when true, skips the cold-start pre-flight
	// check that refuses a fresh stream into a target with
	// pre-existing rows. The check protects against Bug 9 (cold-
	// start hangs after a killed-mid-copy run leaves partial dest
	// data behind); this flag is the explicit override for the
	// rare case of bulk-copying into a populated table. Ignored on
	// the warm-resume path — that branch doesn't bulk-copy.
	ForceColdStart bool

	// ResetTargetData, when true, clears the cdc-state row and drops
	// every source-schema table on the target before starting a fresh
	// cold-start stream. The destructive recovery path for the
	// v0.5.2 slot-missing fall-through and similar wedged-state
	// scenarios. See ADR-0023.
	//
	// Forces the cold-start branch: warm-resume is bypassed in favour
	// of cold-start after the reset wipes both the state row and the
	// dest tables. The pre-flight refusal is skipped on the same run
	// — the drop loop runs to completion first. Engines that don't
	// expose the optional [ir.TableDropper] / [ir.StreamCleaner]
	// surfaces cause the flag to error clearly before any work runs.
	ResetTargetData bool

	// ApplyBatchSize is the upper bound on changes per target
	// transaction. 0 or 1 means one-change-per-tx (the conservative
	// v0.3.x default). Larger values amortise per-tx commit
	// overhead at the cost of a larger replay-on-crash window. The
	// applier's idempotent semantics (ADR-0010) make the replay
	// safe; the position-and-data atomicity (ADR-0007) is preserved
	// per batch — the position of the last applied change in a
	// batch is written in the same tx as the batch's data writes.
	//
	// Schema-change events (Truncate today; AddColumn / DropColumn
	// when the IR grows them) flush the in-progress batch and
	// apply alone so the applier's column-type cache is scoped per
	// schema epoch. The cap is an upper bound, not a target —
	// small streams don't accumulate.
	//
	// Engines that don't implement [ir.BatchedChangeApplier] fall
	// back to per-change Apply regardless of this field; in
	// practice every shipping engine implements it (see ADR-0017).
	ApplyBatchSize int

	// MetricsListen, when non-empty, starts a Prometheus-format
	// `/metrics` HTTP endpoint at the given address (e.g. `:9090`)
	// for the duration of the stream. Off by default — the metrics
	// surface is opt-in so operators can keep the network footprint
	// minimal when they don't need scrape-based monitoring. Phase 2
	// of the sync-health monitoring proto-ADR; the existing
	// `sluice sync health` probe is the cron-friendly equivalent.
	//
	// Metric set: see [emitMetrics] for the full list. Briefly:
	// `sluice_seconds_since_last_apply`, `sluice_stream_known`,
	// `sluice_metrics_scrape_unix_seconds` — all gauges, labelled
	// by `stream_id`. Read at scrape time from the target's
	// `sluice_cdc_state` via the existing `ListStreams` surface; no
	// instrumentation of the apply hot path.
	MetricsListen string

	// MaxBufferBytes is the soft upper bound on per-batch buffered
	// memory in the CDC applier (and, on the cold-start branch, the
	// bulk-copy writer). Each in-flight target transaction tracks
	// the accumulated row-value bytes of its buffered changes and
	// commits early when the cap is reached, even if the row-count
	// cap (--apply-batch-size) hasn't fired. This bounds memory on
	// streams whose source transactions contain a few wide rows
	// (TEXT / BYTEA / JSON at MB scale).
	//
	// Zero means use the default (64 MiB). The cap is a soft target:
	// a single change larger than the cap still applies — better to
	// land it than to wedge the stream. See ADR-0028.
	MaxBufferBytes int64
}

// Run executes a snapshot+CDC stream. See [Streamer] for the full
// flow.
//
// Returns nil on clean ctx cancellation; non-nil on any phase
// failure. Resources (snapshot stream, target writers, applier)
// are released before return regardless of outcome.
func (s *Streamer) Run(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}

	// Apply the sluice-prefix convention to the operator-supplied
	// slot name (v0.10.2). Empty stays empty (engine default);
	// `shard_a` becomes `sluice_shard_a`; already-prefixed names
	// pass through. Mutated in place because Streamer is single-
	// shot per Run; the resolved name flows through to both the
	// CDC-reader and snapshot-stream open paths and surfaces in
	// log lines so operators can correlate against
	// pg_replication_slots.
	if resolved := resolveSlotName(s.SlotName); resolved != s.SlotName {
		slog.InfoContext(ctx, "applying sluice slot-name prefix convention",
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
		slog.InfoContext(ctx, "applying engine-default table exclusions",
			slog.String("engine", s.Source.Name()),
			slog.Any("patterns", added),
		)
		s.Filter = eff
	}

	streamID := s.resolveStreamID()
	slog.InfoContext(ctx, "stream starting", slog.String("stream_id", streamID))

	// ---- 1. Open / wire the applier first ----
	applier, ownsApplier, err := s.openApplier(ctx)
	if err != nil {
		return err
	}
	if ownsApplier {
		defer closeIf(applier)
	}

	// ---- 1a. Optional Prometheus metrics endpoint ----
	// When --metrics-listen is set, a small HTTP server runs alongside
	// the stream exposing a Prometheus-format /metrics surface.
	// Off by default; opt-in. Lifecycle is scoped to the streamer's
	// Run — Started before the stream begins, Closed in the deferred
	// teardown. A bind failure at startup is fatal (operator asked
	// for the listener; misconfigured port shouldn't be silent).
	// Skipped on DryRun: dry-run doesn't run a real stream, so
	// metrics for it aren't useful.
	if s.MetricsListen != "" && !s.DryRun {
		metricsSrv, mErr := NewMetricsServer(s.MetricsListen, applier)
		if mErr != nil {
			return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: prepare metrics server: %w", mErr))
		}
		if mErr := metricsSrv.Start(); mErr != nil {
			return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: start metrics server: %w", mErr))
		}
		slog.InfoContext(ctx, "metrics server listening", slog.String("addr", s.MetricsListen))
		defer func() { _ = metricsSrv.Close() }()
	}

	// ---- 2. Ensure the control table exists ----
	// Skip on dry-run — that's a write, and dry-run is read-only.
	// ReadPosition below tolerates a missing control table by
	// returning ok=false (same as "no row").
	if !s.DryRun {
		if err := applier.EnsureControlTable(ctx); err != nil {
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

	// ---- 3. Look up the persisted position ----
	persisted, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: read position: %w", err))
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

	// ---- 3.5. Dry-run: print plan and exit before any state mutation. ----
	if s.DryRun {
		return s.logDryRunPlan(ctx, streamID, persisted, found)
	}

	// ---- 3.6. Fetch the applier's LSN-feedback tracker (if any) ----
	// Slot-ack-after-apply (Bug 15, ADR-0020): the postgres applier
	// exposes a tracker the matching CDC reader reads from on its
	// keepalive path. The tracker is opaque (typed `any`) so the
	// pipeline package stays engine-neutral; the matching reader's
	// AttachLSNTracker type-asserts internally. Cross-engine pairs
	// (PG applier → MySQL reader, etc.) harmlessly hand a value the
	// reader doesn't recognise; nothing breaks because the reader's
	// fallback path (streamed-LSN keepalive) is correct for engines
	// without an async-batched apply layer.
	var lsnTracker any
	if provider, ok := applier.(lsnTrackerProvider); ok {
		lsnTracker = provider.LSNTracker()
	}

	// ---- 4. Branch: cold start vs warm resume ----
	//
	// Two cancellable contexts share the parent ctx:
	//
	//   - streamCtx scopes the CDC reader's pump (and, transitively,
	//     the snapshot + bulk-copy phases of cold-start). When this
	//     cancels, the reader's pump exits and `defer close(out)`
	//     closes the change channel — the applier sees the close
	//     via its existing channel-closed branch and commits its
	//     in-flight partial batch CLEANLY before returning. This is
	//     the graceful-drain shape the CLI `sync stop` path needs
	//     (Bug 15 CLI fix, ADR-0025).
	//
	//   - applyCtx scopes the apply loop. Cancelling it tells the
	//     applier to roll back any open transaction immediately —
	//     the abort shape used for parent ctx cancellation (Ctrl-C)
	//     and as the hard-timeout fallback when graceful drain
	//     doesn't complete in stopDrainTimeout.
	//
	// Slot-missing fall-through (ADR-0022): if warm resume fails
	// because the persisted position references state that no longer
	// exists on the source (PG slot dropped, MySQL binlog purged),
	// the CDC reader returns an error wrapping [ir.ErrPositionInvalid].
	// The persisted position is by definition unrecoverable; the
	// only path forward is cold-start (re-snapshot + fresh slot).
	// We log a loud WARN naming the slot/position so monitoring
	// catches the recovery event, then re-enter coldStart with the
	// same lsnTracker. Bug 9's pre-flight refusal still gates
	// destructive dest-table operations — auto-fall-through does
	// not silently destroy data.
	//
	// --reset-target-data (ADR-0023): destructive recovery — clear
	// the cdc-state row, drop dest tables, then run cold-start. The
	// reset itself happens inside coldStart so it shares the schema
	// read + row writer that already need to open. We force the
	// cold-start branch here rather than risk warmResume seeing the
	// stale row before reset clears it.
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	applyCtx, cancelApply := context.WithCancel(ctx)
	defer cancelApply()

	var changes <-chan ir.Change
	switch {
	case s.ResetTargetData:
		changes, err = s.coldStart(streamCtx, lsnTracker, applier, streamID)
	case found:
		changes, err = s.warmResume(streamCtx, persisted, lsnTracker)
		if err != nil && errors.Is(err, ir.ErrPositionInvalid) {
			slog.WarnContext(ctx, "warm resume: persisted position is no longer valid; falling through to cold start",
				slog.String("stream_id", streamID),
				slog.String("position_token", persisted.Token),
				slog.String("source_engine", persisted.Engine),
				slog.String("err", err.Error()),
			)
			changes, err = s.coldStart(streamCtx, lsnTracker, applier, streamID)
		}
	default:
		changes, err = s.coldStart(streamCtx, lsnTracker, applier, streamID)
	}
	if err != nil {
		return err
	}
	if changes == nil {
		// coldStart returns (nil, nil) when the source schema is
		// empty — nothing to do.
		return nil
	}

	// ---- 5. Apply ----
	// The dispatch-side filter wraps `changes` with a goroutine
	// that drops events whose qualified name doesn't pass the
	// filter. No-op pass-through when the filter is empty.
	//
	// stopObserved is set by pollStopSignal the moment it first sees
	// the control-table stop flag. After dispatchApply returns we
	// inspect it to decide whether to clear the flag (graceful drain
	// initiated by `sync stop`) or leave it (Ctrl-C / outer ctx cancel
	// — the operator's stop request, if any, didn't drive this exit).
	// The cleared flag is the signal `sync stop --wait` polls for.
	var stopObserved atomic.Bool
	s.startStopSignalPoll(applyCtx, applier, streamID, cancelStream, cancelApply, &stopObserved)

	filtered := filterChanges(applyCtx, changes, s.Filter)
	dispatchErr := s.dispatchApply(applyCtx, applier, streamID, filtered)
	if dispatchErr != nil && !errors.Is(dispatchErr, context.Canceled) && !errors.Is(dispatchErr, context.DeadlineExceeded) {
		return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: apply changes: %w", dispatchErr))
	}
	// On a stop-signal-driven graceful drain, clear stop_requested_at
	// so a CLI `sync stop --wait` polling for completion sees the
	// cleared flag and returns success. Use the outer ctx because
	// applyCtx may already be cancelled here.
	if stopObserved.Load() {
		if err := applier.ClearStopRequested(ctx, streamID); err != nil {
			slog.WarnContext(ctx, "failed to clear stop_requested_at after graceful drain; sync stop --wait may time out",
				slog.String("stream_id", streamID),
				slog.String("error", err.Error()),
			)
		}
	}
	return nil
}

// dispatchApply routes the change channel to the applier's batched
// or per-change Apply path. When ApplyBatchSize > 1 and the applier
// implements [ir.BatchedChangeApplier], the batched path runs;
// otherwise the per-change path runs (preserving v0.3.x semantics
// bit-for-bit).
//
// The optional-interface probe means engines that don't yet
// implement the batched form keep working — type assertion fails
// silently and we fall through to Apply. ADR-0017 covers the
// design choice.
func (s *Streamer) dispatchApply(ctx context.Context, applier ir.ChangeApplier, streamID string, changes <-chan ir.Change) error {
	if s.ApplyBatchSize > 1 {
		if batched, ok := applier.(ir.BatchedChangeApplier); ok {
			slog.DebugContext(ctx, "applier: batched apply enabled",
				slog.String("stream_id", streamID),
				slog.Int("apply_batch_size", s.ApplyBatchSize),
			)
			return batched.ApplyBatch(ctx, streamID, changes, s.ApplyBatchSize)
		}
		slog.WarnContext(ctx, "applier: --apply-batch-size requested but applier does not implement BatchedChangeApplier; falling back to per-change apply",
			slog.String("stream_id", streamID),
			slog.Int("apply_batch_size", s.ApplyBatchSize),
		)
	}
	return applier.Apply(ctx, streamID, changes)
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
		slog.DebugContext(applyCtx, "stop-signal poll skipped: applier does not implement ReadStopRequested",
			slog.String("stream_id", streamID),
		)
		return
	}
	slog.DebugContext(applyCtx, "stop-signal poll started",
		slog.String("stream_id", streamID),
	)
	go pollStopSignal(applyCtx, reader, streamID, cancelStream, cancelApply, observed)
}

// logDryRunPlan describes what Run would do without doing it via
// structured slog records. Cold-start logs the source schema summary
// so operators can catch missing-tables / unexpected-column-counts
// before the migration starts; warm-resume logs the persisted
// position token (truncated for readability) so operators can see
// whether the stream is positioned where they expect.
//
// The source schema read for cold-start is the only source-side
// touch the dry-run does — same level of access the regular
// cold-start would do, just without then opening the snapshot
// stream or starting CDC.
func (s *Streamer) logDryRunPlan(ctx context.Context, streamID string, persisted ir.Position, found bool) error {
	slog.InfoContext(ctx, "dry run: stream plan",
		slog.String("source", s.Source.Name()),
		slog.String("source_host", redactedHost(s.SourceDSN)),
		slog.String("target", s.Target.Name()),
		slog.String("target_host", redactedHost(s.TargetDSN)),
		slog.String("stream_id", streamID),
	)
	if found {
		slog.InfoContext(ctx, "dry run: warm resume from persisted position",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 80)),
		)
		return nil
	}
	slog.InfoContext(ctx, "dry run: cold start — would capture snapshot, bulk-copy, then start CDC",
		slog.String("stream_id", streamID),
	)

	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "dry run: source schema has no tables — nothing to stream")
		return nil
	}
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return err
	}
	applyViewFilter(ctx, schema, s.ViewFilter, s.SkipViews)
	mapped, err := translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: dry-run: apply mappings: %w", err)
	}
	if _, err := translate.ApplyExpressionOverrides(mapped, s.ExpressionMappings); err != nil {
		return fmt.Errorf("pipeline: dry-run: apply expression overrides: %w", err)
	}
	slog.InfoContext(ctx, "dry run: tables to bulk-copy and tail via CDC",
		slog.Int("tables", len(schema.Tables)),
	)
	for _, t := range schema.Tables {
		// secondary_indexes excludes the primary key (reported via
		// primary_key) — see migrate.go logPlan for the rationale.
		slog.InfoContext(ctx, "dry run: table",
			slog.String("name", t.Name),
			slog.Int("columns", len(t.Columns)),
			slog.Bool("primary_key", t.PrimaryKey != nil),
			slog.Int("secondary_indexes", len(t.Indexes)),
			slog.Int("foreign_keys", len(t.ForeignKeys)),
		)
	}
	return nil
}

// truncateDryRunToken trims a position token to maxLen characters
// with an ellipsis when longer. Position tokens are JSON blobs that
// can run hundreds of bytes; the dry-run output stays scannable.
func truncateDryRunToken(token string, maxLen int) string {
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen-1] + "…"
}

// openApplier returns the applier to use plus a flag indicating
// whether the Streamer owns its lifecycle. Owns => Streamer must
// Close it. Borrowed => caller is responsible.
func (s *Streamer) openApplier(ctx context.Context) (ir.ChangeApplier, bool, error) {
	if s.Applier != nil {
		// Pre-supplied appliers are typically test stubs whose
		// lifecycle the caller owns; we still hand them the byte cap
		// so a stub that wants to honour it can. Real production
		// callers leave Applier nil and hit the OpenChangeApplier
		// branch below.
		applyMaxBufferBytes(s.Applier, s.MaxBufferBytes)
		return s.Applier, false, nil
	}
	a, err := s.Target.OpenChangeApplier(ctx, s.TargetDSN)
	if err != nil {
		return nil, false, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target change applier: %w", err))
	}
	applyMaxBufferBytes(a, s.MaxBufferBytes)
	return a, true, nil
}

// warmResume opens a CDC reader on the source and starts streaming
// from the persisted position. No snapshot, no bulk-copy.
//
// lsnTracker is the opaque applied-LSN feedback channel (Bug 15,
// ADR-0020). Attached to the reader before StreamChanges so the
// keepalive path uses applied-LSN from the very first ack — no
// window where the slot could advance past un-applied work just
// because the reader was constructed before the tracker was
// passed through. nil tracker means the engine doesn't support
// LSN feedback (the pre-v0.5.0 shape) or the applier isn't a
// matching engine; the reader falls back to streamed-LSN.
//
// Warm resume reuses the publication scope established at cold
// start; we don't re-read the schema or re-call EnsurePublication
// here. Defence-in-depth lives in the applier's dispatch path
// (skip-with-warning on unknown tables).
func (s *Streamer) warmResume(ctx context.Context, persisted ir.Position, lsnTracker any) (<-chan ir.Change, error) {
	slog.InfoContext(ctx, "warm resume from persisted position",
		slog.String("position_token", persisted.Token),
	)
	cdc, err := openCDCReaderWithOptionalSlot(ctx, s.Source, s.SourceDSN, s.SlotName)
	if err != nil {
		return nil, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: open cdc reader: %w", err))
	}
	if lsnTracker != nil {
		if attacher, ok := cdc.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}
	// CDC reader's Close is async (cancellation-driven), but we
	// don't have a clean handle to call it from here. Streamer.Run's
	// returning will cancel ctx; the pump exits and closes the
	// channel.
	changes, err := cdc.StreamChanges(ctx, persisted)
	if err != nil {
		closeIf(cdc)
		return nil, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	return changes, nil
}

// coldStart performs the original §4 flow: read schema → ensure
// publication scope → snapshot → bulk-copy → start CDC from
// snapshot's position.
//
// lsnTracker is the opaque applied-LSN feedback channel (Bug 15,
// ADR-0020) — attached to the snapshot stream's CDC reader before
// StreamChanges so the keepalive path uses applied-LSN from the
// first ack onwards.
//
// applier and streamID are the engine-side handles for the optional
// `--reset-target-data` recovery path (ADR-0023): when [s.ResetTargetData]
// is set, the cdc-state row is cleared via [ir.StreamCleaner] and dest
// tables are dropped via [ir.TableDropper] before the bulk-copy phase
// begins. Both surfaces are optional; an engine that doesn't expose
// them surfaces a clear refusal rather than running a partial reset.
func (s *Streamer) coldStart(ctx context.Context, lsnTracker any, applier ir.ChangeApplier, streamID string) (<-chan ir.Change, error) {
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "source schema has no tables; nothing to stream")
		return nil, nil
	}

	// Prune by table filter before mappings + bulk-copy so the
	// excluded tables never reach the target schema-apply phase.
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return nil, err
	}
	applyViewFilter(ctx, schema, s.ViewFilter, s.SkipViews)

	// ---- Scope the source-side publication to the filtered table
	// list (Bug 13, ADR-0021). On engines that don't have
	// publications (MySQL), this is a no-op; on Postgres, this is
	// what stops a CREATE TABLE on the source mid-sync from
	// crashing the applier with "table public.X has no columns".
	// Run BEFORE OpenSnapshotStream so the snapshot's slot pins a
	// catalog snapshot that already has the scoped publication.
	if pe, ok := s.Source.(publicationEnsurer); ok {
		tables := tableNamesForPublication(schema)
		if err := pe.EnsurePublication(ctx, s.SourceDSN, tables); err != nil {
			return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: ensure publication scope: %w", err))
		}
	}

	// Apply per-column type overrides before the schema-write phase
	// sees the schema. Warm resume skips this step — by then the
	// target schema is already shaped from the cold-start run.
	schema, err = translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: apply mappings: %w", err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, s.ExpressionMappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: apply expression overrides: %w", err)
	}

	stream, err := openSnapshotStreamWithOptionalSlot(ctx, s.Source, s.SourceDSN, s.SlotName)
	if err != nil {
		return nil, wrapWithHint(PhaseSnapshot, fmt.Errorf("pipeline: open snapshot stream: %w", err))
	}
	// stream.Close is deferred by the caller indirectly via
	// Streamer.Run's defer chain — we keep the handle alive past
	// this function so the snapshot+CDC pair stays valid.
	slog.InfoContext(ctx, "cold start; snapshot captured",
		slog.String("position_token", stream.Position.Token),
	)

	sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
	if err != nil {
		_ = stream.Close()
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target schema writer: %w", err))
	}
	rw, err := s.Target.OpenRowWriter(ctx, s.TargetDSN)
	if err != nil {
		closeIf(sw)
		_ = stream.Close()
		return nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target row writer: %w", err))
	}
	applyMaxBufferBytes(rw, s.MaxBufferBytes)

	if s.ResetTargetData {
		if err := resetTargetDataForStream(ctx, schema, rw, applier, streamID); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return nil, err
		}
	} else {
		// Cold-start pre-flight: refuse if any target table already
		// contains data. See preflight.go for the rationale (Bug 9).
		// Streamer's cold-start branch is the analogue of Migrator's
		// non-resume cold-start path; warm-resume doesn't run bulk-copy
		// and is therefore not gated by this check.
		if err := preflightColdStart(ctx, schema, rw, s.ForceColdStart); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return nil, err
		}
	}

	if err := runBulkCopy(ctx, schema, stream.Rows, sw, rw); err != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return nil, err
	}
	closeIf(rw)
	closeIf(sw)
	// Release the snapshot transaction and import-side connections
	// now that bulk-copy is done — without this, Postgres holds the
	// snapshot tx as `idle in transaction` for the entire CDC
	// lifetime (Bug 21), keeping AccessShareLock on every snapshotted
	// table and blocking ALTER on the source. The slot's logical
	// position is independent of the exporting tx; CDC continues on
	// its own connection.
	if err := stream.ReleaseRows(); err != nil {
		slog.WarnContext(ctx, "release snapshot rows failed; CDC will continue but the snapshot tx may stay open",
			slog.String("error", err.Error()),
		)
	}
	slog.InfoContext(ctx, "bulk-copy complete; entering CDC mode")

	if lsnTracker != nil {
		if attacher, ok := stream.Changes.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}

	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		_ = stream.Close()
		return nil, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	// stream stays alive for the rest of Run; cleanup happens via
	// the function's defer chain when ctx cancels and pump exits.
	// We don't have a clean way to defer here while returning the
	// channel; the OS reclaims connections at process exit, and ctx
	// cancellation tears down the goroutines that hold them.
	return changes, nil
}

// tableNamesForPublication returns the bare table names from a
// post-filter schema, in declaration order. Used by the publication-
// scope step (Bug 13, ADR-0021) — schema-qualifying happens in the
// engine because schema is an engine-side concept (PG namespaces vs.
// MySQL databases vs. future engines).
func tableNamesForPublication(schema *ir.Schema) []string {
	if schema == nil {
		return nil
	}
	out := make([]string, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		out = append(out, t.Name)
	}
	return out
}

// validate enforces the required-fields contract.
func (s *Streamer) validate() error {
	switch {
	case s.Source == nil:
		return errors.New("pipeline: Streamer.Source engine is nil")
	case s.Target == nil:
		return errors.New("pipeline: Streamer.Target engine is nil")
	case s.SourceDSN == "":
		return errors.New("pipeline: Streamer.SourceDSN is empty")
	case s.TargetDSN == "":
		return errors.New("pipeline: Streamer.TargetDSN is empty")
	case s.Source.Capabilities().CDC == ir.CDCNone:
		return fmt.Errorf("pipeline: Streamer.Source engine %q declares CDC=None", s.Source.Name())
	}
	return nil
}

// resolveStreamID returns the operator-supplied StreamID if non-
// empty; otherwise generates a deterministic ID from source+target
// engine names and DSN host info (passwords stripped). The result
// is length-bounded to fit VARCHAR(255) on the MySQL control table.
func (s *Streamer) resolveStreamID() string {
	if s.StreamID != "" {
		return s.StreamID
	}
	id := fmt.Sprintf("%s://%s -> %s://%s",
		s.Source.Name(), redactedHost(s.SourceDSN),
		s.Target.Name(), redactedHost(s.TargetDSN))
	if len(id) > 255 {
		id = id[:255]
	}
	return id
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

// sluiceSlotPrefix is prepended to operator-supplied slot names
// that don't already start with it. Convention: every sluice-created
// replication slot starts with `sluice_` so operators can find them
// all with `pg_replication_slots WHERE slot_name LIKE 'sluice\_%'`
// for cleanup, audits, and disambiguation from other tools' slots
// (Debezium, native logical replication subscribers, etc.). The
// default slot name is `sluice_slot` (already prefixed); custom
// names like `--slot-name shard_a` become `sluice_shard_a`.
const sluiceSlotPrefix = "sluice_"

// ResolveSlotName is the exported counterpart of [resolveSlotName].
// CLI commands outside the pipeline package (today: `sluice backup
// full --slot-name`) call through to apply the sluice-prefix
// convention without re-implementing it.
func ResolveSlotName(operatorSupplied string) string {
	return resolveSlotName(operatorSupplied)
}

// resolveSlotName applies the sluice-prefix convention to an
// operator-supplied slot name. Empty input passes through unchanged
// — the empty signal means "use the engine's default" (which is
// already `sluice_slot`). Names already starting with `sluice_`
// pass through verbatim. Anything else gets the prefix prepended.
//
// Examples:
//
//	""                → ""              (engine default)
//	"shard_a"         → "sluice_shard_a"
//	"sluice_shard_a"  → "sluice_shard_a" (idempotent)
//	"sluice_slot"     → "sluice_slot"
//
// Centralised here so the prefix policy applies uniformly to both
// the CDC-reader and snapshot-stream open paths, and any future
// CLI / YAML / env entry points.
func resolveSlotName(operatorSupplied string) string {
	if operatorSupplied == "" {
		return ""
	}
	if strings.HasPrefix(operatorSupplied, sluiceSlotPrefix) {
		return operatorSupplied
	}
	return sluiceSlotPrefix + operatorSupplied
}

// openCDCReaderWithOptionalSlot calls the engine's slot-aware
// OpenCDCReaderWithSlot when slotName is non-empty AND the engine
// implements [ir.CDCReaderWithSlotOpener]. Otherwise falls back to
// the default OpenCDCReader. Engines without slot concepts (MySQL)
// silently ignore an operator-supplied slot name.
//
// The split keeps the streamer's main paths readable — the
// type-assertion dance lives in one place rather than at every
// open-CDC call site.
func openCDCReaderWithOptionalSlot(ctx context.Context, source ir.Engine, dsn, slotName string) (ir.CDCReader, error) {
	if slotName == "" {
		return source.OpenCDCReader(ctx, dsn)
	}
	if opener, ok := source.(ir.CDCReaderWithSlotOpener); ok {
		return opener.OpenCDCReaderWithSlot(ctx, dsn, slotName)
	}
	// Engine doesn't implement the slot-aware surface. Use the
	// default and emit a debug-level note so the operator can spot
	// the silent ignore via --log-level=debug if curious.
	slog.DebugContext(ctx, "engine does not implement CDCReaderWithSlotOpener; --slot-name silently ignored",
		slog.String("engine", source.Name()),
	)
	return source.OpenCDCReader(ctx, dsn)
}

// openSnapshotStreamWithOptionalSlot is the snapshot-stream sibling
// of openCDCReaderWithOptionalSlot. Same dispatch shape.
func openSnapshotStreamWithOptionalSlot(ctx context.Context, source ir.Engine, dsn, slotName string) (*ir.SnapshotStream, error) {
	if slotName == "" {
		return source.OpenSnapshotStream(ctx, dsn)
	}
	if opener, ok := source.(ir.SnapshotStreamWithSlotOpener); ok {
		return opener.OpenSnapshotStreamWithSlot(ctx, dsn, slotName)
	}
	slog.DebugContext(ctx, "engine does not implement SnapshotStreamWithSlotOpener; --slot-name silently ignored",
		slog.String("engine", source.Name()),
	)
	return source.OpenSnapshotStream(ctx, dsn)
}

// redactedHost extracts a "host:port" (or "host") fragment from the
// DSN, dropping passwords and other connection params. Both URI
// (postgres://, mysql://) and KV-pair (libpq, MySQL DSN) forms are
// accepted; falls back to "" on parse failure rather than leaking
// sensitive material.
func redactedHost(dsn string) string {
	// URI form, e.g. "postgres://u:p@host:5432/db?...".
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if u, err := url.Parse(dsn); err == nil {
			return u.Host
		}
		return ""
	}
	// MySQL DSN form, e.g. "user:pass@tcp(host:port)/dbname?params".
	// Pull out the part inside tcp(...) if present.
	if at := strings.Index(dsn, "@tcp("); at >= 0 {
		body := dsn[at+5:]
		if end := strings.Index(body, ")"); end >= 0 {
			return body[:end]
		}
	}
	// libpq KV form, e.g. "host=localhost port=5432 user=...".
	host, port := "", ""
	for _, tok := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "host":
			host = v
		case "port":
			port = v
		}
	}
	if host == "" {
		return ""
	}
	if port != "" {
		return host + ":" + port
	}
	return host
}
