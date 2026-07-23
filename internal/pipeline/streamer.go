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
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
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

// slotNameSetter is the optional applier-side surface for engines
// that record the active stream's replication-slot name on the
// per-target control table (Phase 2 mid-stream live add-table,
// ADR-0030). PG implements; MySQL does not (no slot concept). The
// streamer calls SetSlotName once at startup; the applier threads
// the value into every subsequent position-write so the
// sluice_cdc_state row's slot_name column reflects what the streamer
// is actually consuming. `sluice schema add-table --no-drain`
// recovers the slot name via ListStreams to look up the right slot's
// confirmed_flush_lsn.
type slotNameSetter interface {
	SetSlotName(slotName string)
}

// pollIntervalSetter is the optional CDC-reader-side surface for
// engines that poll the source (today: postgres-trigger; cadence
// default 1 s). Push-based engines (pgoutput, binlog, VStream) do not
// implement this — they have no poll loop to tune. The streamer
// calls SetPollInterval at most once per stream, between
// [ir.CDCReader] open and [ir.CDCReader.StreamChanges]; the engine
// reader captures the new value before the first poll fires.
// Roadmap item 18(c) / ADR-0066 §6.
type pollIntervalSetter interface {
	SetPollInterval(d time.Duration)
}

// schemaForwardModeSetter is the optional CDC-reader-side surface for
// engines whose reader enforces a mid-stream schema-change gate that must
// be relaxed when ADR-0091 forwarding is enabled. Today only the Postgres
// pgoutput reader implements it: its [postgres.CDCReader] hard-refuses
// DROP COLUMN / ALTER COLUMN TYPE mid-stream (Bug 112/119/120 closure) at
// the source-read level, BEFORE the boundary reaches the ADR-0091 forward
// intercept (F7a GAP #1). With forwarding on, those unambiguous shapes
// must instead be emitted as SchemaSnapshots so the intercept can forward
// them (the GAP #3 applier-cache invalidation keeps decode correct);
// RENAME TABLE / DROP+CREATE / RENAME COLUMN stay refused at the reader.
//
// MySQL's binlog reader re-reads information_schema on a DDL boundary and
// never gated destructive column shapes, so it does not implement this —
// the type-assertion silently no-ops. The streamer calls SetSchemaForward
// once per stream, between [ir.CDCReader] open and
// [ir.CDCReader.StreamChanges], so the reader captures the mode before the
// first RelationMessage is parsed.
type schemaForwardModeSetter interface {
	SetSchemaForward(enabled bool)
}

// targetSchemaSetter is the optional applier-side surface for
// engines that record the operator-supplied `--target-schema NAME`
// on the per-target control table (Bug 46, ADR-0031). PG implements;
// MySQL does not (no schema-vs-database distinction; --target-schema
// is refused upstream for MySQL targets). The streamer calls
// SetTargetSchema once at startup; the applier threads the value
// into every subsequent position-write so the sluice_cdc_state row's
// target_schema column reflects what the streamer is routing CDC
// events to. `sluice schema add-table` reads the recorded value back
// to refuse loudly when the operator supplies a mismatched flag, or
// to inherit the stream's namespace when the flag is omitted.
//
// Distinct from [ir.SchemaSetter]: SetSchema mutates the user-data
// namespace the writer / applier currently writes into;
// SetTargetSchema records the operator's stated intent so a future
// command can detect a mismatch.
type targetSchemaSetter interface {
	SetTargetSchema(name string)
}

// schemaHistoryCachePrimer is the optional applier-side surface for
// engines that maintain an ADR-0049 active-version cache (Chunk C).
// The streamer calls PrimeSchemaHistoryCache once at apply-loop entry
// so a warm-resumed stream pre-seeds the cache with the schema
// version in effect at the persisted position — eliminating a
// per-row resolve, the "O(1) amortised" Consequences mandate.
//
// Brand-new streams (cold-start) pass the snapshot-anchor position,
// which the engine treats as the brand-new-stream sentinel (empty
// Token) and skips the prime entirely (there is no history yet; the
// reader's first SchemaSnapshot populates the cache via the engine's
// post-commit hook). The engine-side contract is documented on each
// engine's [PrimeSchemaHistoryCache] doc.
//
// Engines that don't implement (cross-engine pairs where the applier
// is in-memory test stub, or a future engine that hasn't shipped
// Chunk C yet) silently skip — the cache stays empty and the
// loud-floor resolve fires on the first event needing schema history,
// which is the pre-Chunk-C behaviour and an acceptable degradation
// for engines without active-version support.
type schemaHistoryCachePrimer interface {
	PrimeSchemaHistoryCache(ctx context.Context, streamID string, currentPos ir.Position) error
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

	// PublicationName, when non-empty, overrides the engine's default
	// publication name on engines with a shared source-side filter
	// object (Postgres: `sluice_pub`). Engines without one (the MySQL
	// family — each stream opens its own binlog/VStream reader)
	// silently ignore it.
	//
	// The sibling of SlotName, and needed for the same reason: the
	// slot is per-consumer but the publication is NOT, so two
	// concurrent streams over one PG source that share a publication
	// will silently de-scope each other when the second cold-starts
	// (its `ALTER PUBLICATION ... SET TABLE` replaces the member set
	// atomically). Set this per stream — alongside SlotName — whenever
	// several streams run against one Postgres source. ADR-0175.
	//
	// Empty does NOT always mean the shared engine default any more
	// (ADR-0176 prerequisite chunk): [phaseResolvePublicationScope]
	// ratchets an empty value onto the publication recorded in the
	// stream's sluice_cdc_state row (warm-resume continuity), and a
	// genuinely NEW stream with a `--where` row filter derives a
	// per-stream default (`sluice_<stream-id>`). A stream with no
	// record and no filter keeps the shared `sluice_pub` default —
	// byte-identical to before. An explicit value here always wins
	// (and updates the record, WARNing when they differ).
	PublicationName string

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

	// PlanSink, when non-nil AND DryRun is set, receives the built
	// [StreamPlan] INSTEAD of the human slog rendering — the CLI's
	// `--dry-run --format json` hookup, mirroring
	// [Migrator.PlanSink]. Ignored when DryRun is false.
	PlanSink func(*StreamPlan)

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
	Filter migcore.TableFilter

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

	// SkipForeignKeys, when true, creates NO foreign-key constraints on the
	// target at cold-start (`--skip-foreign-keys`), while keeping each
	// skipped FK's referencing column tuple indexed — a deterministic backing
	// index is synthesized for any FK whose columns no existing target index
	// already covers as a left-prefix (see applySkipForeignKeys). Only the
	// cold-start DDL path creates FKs (runBulkCopyWithOpts → CreateConstraints);
	// steady-state CDC apply never does, so this is a cold-start-only shaping
	// step. The primary use case is a continuous-sync transition onto a target
	// with limited FK support (Vitess/PlanetScale sharded keyspaces) or when
	// FKs are managed out-of-band. Default off — byte-identical to before.
	SkipForeignKeys bool

	// SkipORMTables, when true, drops recognized ORM/framework
	// migration-bookkeeping tables (flyway_schema_history,
	// _prisma_migrations, schema_migrations, …) from the cold-start
	// source schema, announcing each skip loudly (ADR-0143). The prune
	// runs before the snapshot/publication scope is computed, so a
	// continuous sync neither cold-copies nor (on publication-scoped
	// sources) streams them.
	//
	// ★ Zero-value-safe (the v0.99.51 trap): the zero value (false) is
	// DO-NOT-skip — the default every programmatic / broker / fleet
	// construction gets. ONLY the `sync` CLI defaults this on (flipped
	// off by --include-orm-tables). See [Migrator.SkipORMTables].
	SkipORMTables bool

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

	// RestartFromScratch, when true, forces a fresh cold-start that
	// re-copies from row 0 — IGNORING any persisted resume position
	// (incl. a mid-COPY TablePKs cursor) — WITHOUT dropping the target
	// (the idempotent COPY writer absorbs the re-copied overlap). It is
	// the explicit "force-fresh-COPY" knob: --force-cold-start only
	// skips the pre-flight (it still warm-resumes when a position
	// exists), and --reset-target-data is the heavier destructive path
	// (it DROPs the dest tables). This sits between them. Like
	// ResetTargetData it forces the cold-start branch and skips the
	// populated-target pre-flight (you are deliberately re-copying onto
	// existing rows). Cleared after the first iteration so a retry does
	// not perpetually re-cold-start.
	RestartFromScratch bool

	// SuppressAutoResnapshotOnInvalidPosition is the OPT-OUT for the
	// ADR-0093 auto-recovery from a resume against an invalid/purged
	// source position. Deliberately an opt-out so the ZERO VALUE (false)
	// is the safe, binlog-parity default — auto-recover — for every
	// Streamer construction (CLI, tests, future callers) without each
	// having to set a field. Set true only by `--no-auto-resnapshot`.
	//
	// Default (false) = auto-recover: BOTH the pre-flight fall-through
	// (the ADR-0022 sites in [phaseOpenChangeStream]) AND the reactive
	// recovery (a [ir.ErrPositionInvalid] surfaced from the VStream
	// pump's Recv — see ADR-0093 — routed by [Run] / [runWithRetry])
	// re-enter cold-start in the same Run, non-destructively (the
	// idempotent COPY writer absorbs the overlap; no target drop). The
	// reactive recovery is bounded to ONE re-snapshot per Run: a second
	// consecutive [ir.ErrPositionInvalid] after a fresh cold-start is
	// terminal — the source is purging faster than a snapshot completes,
	// which auto-retry cannot fix and must surface loudly.
	//
	// True (operator set --no-auto-resnapshot) = both paths suppressed:
	// [ir.ErrPositionInvalid] surfaces as a loud, actionable terminal
	// error naming the recovery commands. For operators who would rather
	// decide a (potentially expensive) full re-snapshot deliberately than
	// have it happen automatically.
	SuppressAutoResnapshotOnInvalidPosition bool

	// SchemaAlreadyApplied, when true, declares that the target's
	// schema (and the `sluice_cdc_state` control table) have been
	// pre-created out-of-band. Sluice skips every DDL phase during
	// cold-start: no CREATE TABLE / CREATE INDEX / ADD FOREIGN KEY /
	// CREATE VIEW / SyncIdentitySequences / EnsureControlTable.
	// Operators on environments that block direct DDL — PlanetScale
	// branches with Safe Migrations enabled (GitHub issue #17),
	// schema-managed-by-Atlas/Liquibase shops — use this flag after
	// pushing schema changes via their managed pipeline.
	//
	// The pre-flight refusal that checks for non-empty target tables
	// is also skipped (the operator's promise is "everything I
	// need is already there"); bulk-copy runs into the operator-
	// prepared empty tables.
	//
	// Operator responsibilities when this flag is set:
	//   - Every source table must exist on the target with a
	//     compatible schema. Sluice does NOT validate the schemas
	//     match — translation policies still apply at the IR layer
	//     for cross-engine pairs, but the target's catalog state is
	//     trusted as-is.
	//   - The `sluice_cdc_state` control table must exist on the
	//     target before the run starts (the DDL is in
	//     internal/engines/{mysql,postgres}/control_table.go).
	//
	// Skipping schema-apply does NOT skip the source-side snapshot
	// or the CDC pump — only the target-side DDL phases.
	SchemaAlreadyApplied bool

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

	// BuildVersion / BuildCommit populate the /metrics sluice_build_info
	// gauge. Set from the cmd layer's -ldflags build vars; empty ⇒ the
	// emitter's "dev"/"unknown" placeholders. Observability-only.
	BuildVersion string
	BuildCommit  string

	// TargetTelemetry, when non-nil, is an advisory control-plane health
	// provider (ADR-0107). Consulted OFF the hot path for proactive AIMD
	// back-off, storage-resize anticipation, and operator observability.
	// nil (the default) ⇒ every consumer takes its reactive path, so the
	// zero value is the safe/common default (no zero-value trap — see
	// CLAUDE.md's v0.99.51 note): a Streamer constructed anywhere without
	// this field behaves exactly as it did pre-ADR-0107. Wired only by
	// cmd/sluice when the operator opts into PlanetScale metrics (Phase 2
	// supplies the real provider; Phase 1 exercises it with a fake).
	TargetTelemetry ir.TargetTelemetry

	// SuppressTargetMetricsHistory is the OPT-OUT for the ADR-0107 item 35
	// rolling-history recorder: when a telemetry provider is wired, sluice
	// also persists each poll into the bounded sluice_target_metrics_history
	// table on the target so `sluice diagnose` surfaces the recent trend.
	// Deliberately an opt-out so the ZERO VALUE (false) is the safe default —
	// record-when-telemetry-wired — for every construction (CLI, tests,
	// future callers) without each having to set a field. Set true only by
	// `--suppress-target-metrics-history`.
	//
	// Zero-value safety (the v0.99.51 trap): the recorder only STARTS when a
	// telemetry provider is non-nil, and TargetTelemetry is nil for every
	// non-CLI construction (tests, broker/chain paths), so a zero-value
	// Streamer never records regardless of this flag — the default is safe by
	// construction. The opt-out naming is belt-and-suspenders so the field's
	// zero value is also the common-case "record" intent for the one
	// construction (the CLI) that does wire telemetry.
	SuppressTargetMetricsHistory bool

	// ADR-0107 item 36 — the sync-scoped target-metrics threshold ALERTER.
	// When a telemetry provider is wired AND at least one sink URL is set,
	// a sidecar evaluates these thresholds against the polled snapshot and
	// fires edge-triggered, cooldown'd notifications to the configured sinks.
	// All zero ⇒ INERT (opt-in): the alerter only runs when an operator
	// supplies a sink URL AND at least one threshold, so the zero value is
	// the safe default for every construction (no zero-value trap — the
	// feature is gated on a non-empty URL, not on a bool defaulting true).
	// Observability only — failure-isolated, never on the value path.
	//
	// The URLs are credentials (set via env at the CLI). A threshold of 0
	// leaves its rule inert. NotifyStorageGrowthPerMin is the storage
	// rate-of-change rule, expressed as a FRACTION-of-capacity per minute
	// (e.g. 0.02 = storage util climbing 2%/min) so the threshold is
	// capacity-relative; inert at 0.
	NotifyWebhookURL          string
	NotifySlackWebhookURL     string
	NotifyStorageUtil         float64
	NotifyCPUUtil             float64
	NotifyMemUtil             float64
	NotifyLagSeconds          float64
	NotifyStorageGrowthPerMin float64
	NotifyCooldown            time.Duration

	// NotifySyncLagSeconds is the threshold (in seconds) for the
	// engine-neutral SYNC-LAG alert (roadmap item 45): fire when sluice's
	// OWN "seconds behind source" apply lag is at or above this value. It
	// is UNGATED from PlanetScale telemetry — works on MySQL and Postgres
	// alike, needing only a --notify-webhook/--notify-slack sink. Distinct
	// from NotifyLagSeconds, which is the PS control-plane target-internal
	// replica lag (sluice_target_replica_lag_seconds). 0 (the default) ⇒
	// INERT, so the zero value is the safe off default for every
	// construction (no zero-value trap — gated on a non-zero threshold + a
	// sink, never on a bool defaulting true). Observability only —
	// failure-isolated, never on the value path.
	NotifySyncLagSeconds float64

	// NotifyDeadTupleRatio / NotifyXIDAge are the thresholds for the
	// TARGET-side autovacuum advisory rules (the item-36 vacuum rule
	// family, roadmap 2026-07-22): fire when the worst user table's
	// dead-tuple ratio (dead/(dead+live), 0-1) or the database's
	// age(datfrozenxid) is at or above the value. Like NotifySyncLagSeconds
	// they are UNGATED from PlanetScale telemetry — the signal is probed
	// from the target's own catalog via [ir.TargetVacuumHealthReporter]
	// (Postgres targets only; a non-implementing target WARNs once and
	// leaves the rules inert). 0 (the default) ⇒ INERT — the zero value is
	// the safe off default for every construction. Observability only —
	// failure-isolated, never on the value path.
	NotifyDeadTupleRatio float64
	NotifyXIDAge         float64

	// NotifySMTP is the optional email/SMTP sink (roadmap item 48), wired
	// into the SAME alerter path as the webhook/Slack sinks so every
	// threshold alert — the ADR-0107 rules AND the item-45 sync-lag rule —
	// can be delivered by email. INERT unless [notify.SMTPConfig.Configured]
	// (a non-empty Host), so the zero value is the safe off default for every
	// construction. The password is supplied via env only (never the command
	// line). Advisory + failure-isolated, never on the value path.
	NotifySMTP notify.SMTPConfig

	// SuppressSchemaDriftNotify opts OUT of the ADR-0157 schema-drift alert:
	// a critical notification fired to the SAME sinks as the metrics alerter
	// (webhook/Slack/SMTP) the moment a source DDL stalls the sync, carrying
	// the drift detail + the recovery steps so an unattended operator is
	// paged instead of discovering the stall in the logs. The desired
	// default is ON, so the field is named for the OPT-OUT it isn't: the
	// zero value (false) leaves the alert ENABLED for every construction
	// (the v0.99.51 zero-value-safe posture). The CLI sets it from
	// `!--notify-schema-drift`. Inert unless a sink is configured; advisory +
	// failure-isolated + telemetry-independent — it never affects the
	// (already stalled) sync, and it needs no telemetry provider.
	SuppressSchemaDriftNotify bool

	// lastSchemaDriftNotified is the ADR-0157 edge-once latch: the message
	// identity of the last schema-drift refusal we fired an alert on. The
	// streamer surfaces the SAME pending intercept error while stalled and a
	// retry loop re-observes it, so this holds the notification to ONCE per
	// distinct refusal; it re-arms (clears) when no refusal is pending (the
	// stall cleared). Owned by the settle path
	// ([Streamer.observeSchemaDriftForNotify], called from
	// [phaseSettleDispatch]), which runs single-goroutine per attempt with
	// sequential retries — so a plain field, not an atomic, is sufficient.
	lastSchemaDriftNotified string

	// schemaDriftNotifierForTest is a TEST-ONLY seam: when non-nil,
	// [Streamer.observeSchemaDriftForNotify] delivers to it instead of the
	// sink set assembled from the notify URLs, so a unit/integration test can
	// capture the fired schema-drift notification without standing up a real
	// webhook/Slack/SMTP endpoint. nil in production (the cost is one nil
	// check on the stall path).
	schemaDriftNotifierForTest notify.Notifier

	// SuppressSlotHealthNotify opts OUT of the roadmap-64a slot-health alert
	// (ADR-0059 implementation note): the ADR-0059 threshold crossings —
	// WAL retention pressure at 70% (warning) / 85% (critical) of
	// max_slot_wal_keep_size, 30m slot inactivity (warning) — fired to the
	// SAME sinks as the metrics alerter and the schema-drift alert
	// (webhook/Slack/SMTP), so an unattended operator is paged before the
	// slot invalidates instead of discovering wal_status='lost' in the
	// logs. The desired default is ON, so the field is named for the
	// OPT-OUT it isn't: the zero value (false) leaves the alert ENABLED
	// for every construction (the v0.99.51 zero-value-safe posture). The
	// CLI sets it from `!--notify-slot-health`. Inert unless a sink is
	// configured AND the source implements [ir.SlotHealthReporter] (today:
	// Postgres logical replication); the structured slog WARNs fire
	// regardless. Advisory + failure-isolated — never on the value path.
	SuppressSlotHealthNotify bool

	// slotHealthNotifierForTest is a TEST-ONLY seam mirroring
	// schemaDriftNotifierForTest: when non-nil, [Streamer.slotHealthNotifier]
	// resolves to it instead of the sink set assembled from the notify URLs.
	// nil in production.
	slotHealthNotifierForTest notify.Notifier

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

	// IndexBuildMem is the operator's `--index-build-mem` value (a
	// per-build maintenance_work_mem in bytes; 0 = auto), threaded to
	// the PG target SchemaWriter on the cold-start branch via
	// [ir.IndexBuildTuner] before the deferred CreateIndexes phase. 0
	// leaves the writer to autotune from a pg_settings probe (the
	// dominant index-build lever, on by default). Only the cold-start
	// path builds indexes; warm-resume never opens a SchemaWriter.
	// Inert on engines without the tuner (MySQL target). See
	// docs/dev/notes/index-build-phase-tuning.md.
	IndexBuildMem int64

	// IndexBuildParallelism is the operator's `--index-build-parallelism`
	// value (the number of concurrent index builds; 0 = auto), threaded
	// to the PG target SchemaWriter on the cold-start branch via
	// [ir.IndexBuildTuner] before the deferred CreateIndexes phase
	// (Phase B). 0 lets the writer derive a conservative worker count
	// bounded by the target's spare connection budget AND a memory budget.
	// Only the cold-start path builds indexes; warm-resume never opens a
	// SchemaWriter. Inert on engines without the tuner (MySQL target).
	// See docs/dev/notes/index-build-phase-tuning.md.
	IndexBuildParallelism int

	// IndexBuildFallback is the optional out-of-band index-build channel
	// (ADR-0148: the PlanetScale deploy-request fallback for the errno-3024
	// statement-time wall / errno-1105 safe-migrations direct-DDL block),
	// threaded onto the cold-start target SchemaWriter — single- and
	// multi-database branches — via the optional
	// [ir.IndexBuildFallbackSetter] surface right after it opens. Only the
	// cold-start path builds indexes; warm-resume never opens a
	// SchemaWriter. The orchestrator stays engine-neutral: the value is
	// composed by the CLI (which knows the target is PlanetScale and holds
	// the control-plane credentials) and passed through opaquely; engines
	// without the setter skip cleanly. Mirrors [Migrator.IndexBuildFallback]
	// (audit 2026-07-15 MED-A1: the fallback originally reached only the
	// migrate path).
	//
	// Zero-value-safe: nil (every programmatic / fleet / broker / test
	// caller) leaves the direct index build byte-identical to before the
	// fallback existed.
	IndexBuildFallback ir.IndexBuildFallback

	// MaxTargetConnections is the operator's --max-target-connections
	// explicit ceiling on the target connection budget (connection-
	// resilience item 4). On the cold-start branch the streamer runs a
	// connection-budget preflight that refuses loudly when the target
	// has no free slot for the copy + CDC connections (the opaque
	// slot-exhaustion FATAL the feature targets). Unlike the Migrator's
	// parallel-copy path there's no parallelism to cap here — the
	// streamer's cold-start is single-reader — so this value's role is
	// the loud-refusal floor plus an explicit ceiling.
	//
	// Zero (the default) means "auto" — probe-and-refuse-on-exhaustion
	// with no operator-imposed ceiling. Target-engine-specific: a no-op
	// on engines without a connection-slot model (today: MySQL).
	//
	// On the ADR-0079 FAST cold-start (PG source with a shareable exported
	// snapshot) it ALSO bounds the cross-table × within-table copy +
	// index-build connection product, exactly as it does for migrate — the
	// serial fallback keeps the single-reader loud-refusal role above.
	MaxTargetConnections int

	// BulkParallelism, TableParallelism, BulkParallelMinRows, BulkBatchSize,
	// and RawCopyFormat configure the ADR-0079 FAST cold-start copy — the
	// migrate-speed cross-table pool (ADR-0076) + index-build overlap
	// (ADR-0077) + same-engine raw passthrough (ADR-0078) the sync
	// cold-start engages when the source surfaces a shareable exported
	// snapshot ([ir.SnapshotStream.SnapshotName]) and implements
	// [ir.SnapshotImporterOpener] (Postgres). On every other source
	// (MySQL, VStream) the cold-start stays serial and these are inert.
	// Semantics mirror the identically-named [Migrator] fields verbatim;
	// see [migcore.ResolveBulkParallelism] / [migcore.ResolveTableParallelism] /
	// [migcore.ResolveBulkParallelMinRows] for the 0=auto rules.
	BulkParallelism     int
	TableParallelism    int
	BulkParallelMinRows int64
	BulkBatchSize       int
	RawCopyFormat       ir.RawCopyFormat

	// CopyFanoutDegree is the WRITE-side parallel fan-out degree for the
	// idempotent VStream/CDC snapshot cold-start copy (ADR-0097) — the
	// PS-MySQL gap the FAST cold-start (above) can't reach because the
	// VStream READ side is a single un-chunkable stream. On that serial
	// idempotent path the writer falls back to one cross-region-RTT-bound
	// batched-INSERT connection; this fans the single incoming row stream
	// out to N PK-hash-partitioned writer workers. ZERO-VALUE-SAFE (the
	// v0.99.51 trap): the Go zero value resolves to the conservative
	// default degree via [resolveCopyFanoutDegree] — never "zero workers".
	// 1 forces serial. Inert on every path that isn't the idempotent
	// cold-start with a parallel-capable writer + a per-table PK.
	CopyFanoutDegree int

	// NoIntraTableStealing opts OUT of intra-table PK-range work-stealing on the
	// native-MySQL concurrent cold-copy (ADR-0119, roadmap 21b): with it set,
	// every table is copied as a single whole-table work item (the tier-(a)
	// whole-table-stealing behaviour). OPT-OUT-named so the Go zero value
	// (false) keeps intra-table stealing ON — the common default — for every
	// non-CLI construction (the v0.99.51 zero-value trap). Inert on every source
	// that isn't the native-MySQL multi-snapshot work-stealing reader.
	NoIntraTableStealing bool

	// NoFloatExactReread opts OUT of the post-COPY single-precision FLOAT
	// exact re-read repair on a VStream (PlanetScale/Vitess) cold-start
	// (roadmap open-bug 2026-07-09). The VStream COPY lands FLOAT columns
	// display-rounded to 6 significant digits; by default the cold-start
	// re-reads them EXACTLY from the source over SQL and UPDATEs the target
	// by PK before CDC begins. Set this to skip the repair (the values
	// retain the rounding). OPT-OUT-named so the Go zero value (false) keeps
	// the repair ON — the correct default — for every non-CLI construction
	// (the v0.99.51 zero-value trap). Inert on every source whose snapshot
	// reader does not display-round FLOAT (vanilla MySQL, Postgres).
	NoFloatExactReread bool

	// ReapStaleBackends opts the operator into terminating sluice's own
	// orphaned backends on the target during the cold-start preflight
	// (connection-resilience Phase 2, item 2). Detection runs always and
	// reports loudly; this flag authorises the destructive
	// pg_terminate_backend. Default off — detect-and-report is the safe
	// baseline (a legitimately-running concurrent sluice process on the
	// same target is a real possibility). No-op on engines without a
	// backend model (today: MySQL).
	ReapStaleBackends bool

	// ApplyExecTimeout is the per-statement deadline plumbed to the
	// target [ir.ChangeApplier] via the optional
	// [ir.ApplyExecTimeoutSetter] interface. Each tx.ExecContext call
	// in the apply path is wrapped in a context.WithTimeout of this
	// duration; on expiry the driver returns
	// [context.DeadlineExceeded], which the engine-side classifier
	// recognises as retriable so the runWithRetry loop activates.
	//
	// Zero or negative disables the per-exec timeout — the
	// pre-v0.52.0 behaviour where a hung destination connection
	// could block the apply goroutine indefinitely. The CLI's
	// `sync start --apply-exec-timeout=DUR` flag is the canonical
	// operator-facing knob; the default (60s) is intentionally long
	// enough for a legitimately slow batch upsert but short enough
	// to bound the silent-stall window (GitHub issue #23).
	ApplyExecTimeout time.Duration

	// ApplyDelay is the roadmap-item-46 / ADR-0121 delayed-replica knob: in
	// steady-state CDC apply, hold each change until its source commit
	// timestamp + ApplyDelay has elapsed on the local wall clock before
	// applying it (the MySQL MASTER_DELAY "oops window" DR pattern — a target
	// deliberately held behind so an operator can stop sluice before an
	// accidental DROP / bad migration replicates, then recover from the
	// still-intact target). Engine-neutral; the cold-start / bulk-copy phase
	// is unaffected (only steady-state CDC delays).
	//
	// Zero (the default) means no delay — byte-identical to today's apply
	// path (no interceptor, no extra goroutine), so the zero value is the safe
	// off default for every construction (no zero-value trap). The CLI's
	// `sync start --apply-delay=DUR` flag is the operator knob.
	//
	// Resume stays exactly-once across a crash mid-delay-window: the delay
	// gate sits UPSTREAM of the applier, which is the only thing that advances
	// the durable resume position (ADR-0007), so a held-but-unapplied change
	// never advances the position and is re-read on resume. See
	// [delayChanges] and ADR-0121. Held changes backpressure to the source
	// read rather than accumulating in heap; for delays approaching the
	// source's replication idle timeout (PG wal_sender_timeout, MySQL
	// net_write_timeout) raise that server-side timeout (ADR-0121 §3).
	ApplyDelay time.Duration

	// ApplyConcurrency is the ADR-0104 (item 23(c)) / ADR-0105 (item 26)
	// key-hash apply LANE count W plumbed to the target [ir.ChangeApplier]
	// via the optional [ir.ApplyConcurrencySetter] interface. The merged CDC
	// stream is fanned across W in-order lanes by primary-key hash, each
	// committing concurrently on a dedicated backend, lifting aggregate apply
	// throughput toward W× — the lever closing the item-23 cross-region wedge.
	//
	// ADR-0106 (item 31): the field follows the established `--table-parallelism`
	// contract — `0 = auto:N` (adaptive, connection-budget-bounded), `1 =
	// explicit serial opt-out`, `N > 1 = honored`. The raw int alone
	// distinguishes unset/0 (→ auto:N) from an explicit 1 (→ serial), so no
	// sentinel is needed. The `0 → auto:N` resolution happens HERE at the
	// Streamer level (in [resolveApplyConcurrency], called per attempt by
	// [runOnce] into [resolvedApplyConcurrency]), NOT only in the CLI — exactly
	// as the sibling [AutoTune] field defaults on at the streamer level — so any
	// programmatic / test / broker caller that leaves the field zero gets the
	// fast default too, rather than re-triggering the v0.99.51
	// zero-value-safe-default trap.
	//
	// Engagement is still gated downstream: concurrency runs ONLY when the
	// resolved W > 1 AND the engine applier implements [ir.ApplyConcurrencySetter]
	// AND it can open a dedicated lane pool (its pipelineCfg is set — true for
	// every applier opened via OpenChangeApplier, false for direct-API / test
	// stubs). A caller that does NOT wire a real applier therefore stays serial
	// regardless of the resolved value (bounds the blast radius). The CLI's
	// `sync start --apply-concurrency=W` flag is the operator knob.
	ApplyConcurrency int

	// AutoTune controls whether the AIMD apply-batch-size controller
	// (ADR-0052) is engaged for this stream. Per ADR-0052 DP-1 the
	// default is "on" — operators pass `--no-auto-tune` to opt out.
	// The CLI's [sluice sync start] command resolves the flag to this
	// field; the default at the streamer level is also true so any
	// programmatic caller that doesn't set it gets the opted-in shape.
	//
	// When true and [ApplyBatchSize] > 1 and the target applier
	// implements both [ir.BatchSizeProviderSetter] and
	// [ir.BatchObserverSetter], the streamer constructs an
	// [appliercontrol.Controller] with engine-default ceiling /
	// target-latency defaults and threads it onto the applier. When
	// false, the static [ApplyBatchSize] cap is used (the pre-v0.72.0
	// behaviour).
	AutoTune bool

	// ApplyTuneTargetLatency is the operator-supplied p95 target the
	// AIMD controller drives AI/MD around (ADR-0052 DP-2). Zero falls
	// back to the engine-default — planetscale=5s, mysql=10s,
	// postgres=10s — per the resolveAIMDTargetLatency helper. Only
	// consulted when AutoTune is true.
	ApplyTuneTargetLatency time.Duration

	// Redactor is the operator-configured PII redaction policy.
	// PII Phase 1 (roadmap item 15a; GitHub issue #24). Same shape
	// as [Migrator.Redactor] — see that field's doc for the design.
	// CDC apply paths route every change row through
	// [pipeline.migcore.RedactRow] before dispatch when this field is
	// non-nil and non-empty.
	Redactor *redact.Registry

	// RowFilters is the operator's per-table `--where TABLE=<predicate>`
	// row filter (ADR-0173 Phase 2, continuous *filtered* sync), keyed by
	// SOURCE table name. The SAME map drives BOTH legs: the cold-start
	// snapshot copy pushes the native-SQL predicate down into the source
	// read (Phase 1 reuse, [migcore.ApplyRowFilters]), and the CDC leg
	// evaluates it CLIENT-SIDE per change with the ADR-0173 row-move
	// dispatch ([interceptWhereFilter]) — so the two legs cannot diverge on
	// scope. [preflightRowFilters] compiles each predicate + verifies the
	// source delivers full before-images at sync-start; nil/empty is the
	// byte-identical unfiltered default.
	RowFilters map[string]string

	// WhereStrictCollation opts OUT of ADR-0174 Piece 1's faithful
	// case/accent-insensitive comparison (the --where-strict-collation flag):
	// when true, a string `--where` on a non-byte-exact collation is refused
	// at sync-start, the pre-0174 strict behavior. Zero value (false) is the
	// common, faithful default — every construction path that never sets it
	// gets the useful behavior, not the strict one (the v0.99.51 trap).
	WhereStrictCollation bool

	// PositionFromManifestStore is the [irbackup.Store] the chain
	// terminal position is read from when the operator passes
	// `--position-from-manifest=<chain-url>`. The Streamer uses the
	// store's terminal manifest's [irbackup.Manifest.EndPosition] as the
	// resume position, bypassing the per-target [sluice_cdc_state]
	// row read (which a fresh-restored target wouldn't have). Phase
	// 3.3.B; mutually exclusive with the resume-from-control-table
	// path because they describe different position sources.
	//
	// nil means the field is not in use (the legacy resume path runs
	// unchanged).
	PositionFromManifestStore irbackup.Store

	// StrictPreflight, when true, promotes the soft warnings emitted
	// by the Phase 3.3.C pre-flight checks (PG `wal_keep_size`
	// sufficiency, Patroni-managed source detection) to hard refusals
	// before CDC starts. Default false: warnings log but the run
	// proceeds. Operators flip this on when they want a strict
	// "fail loudly on any preflight signal" posture (CI gate, scripted
	// runbook, post-incident audit). The slot-existence check is
	// always a refusal; this flag only affects the warning-grade
	// checks.
	StrictPreflight bool

	// PatroniMode controls how the Phase 3.3.C Patroni / HA-managed
	// source detection runs. Valid values: "auto" (default — run the
	// engine heuristics, warn if detected), "on" (skip heuristics,
	// always warn — operator forcing the warning), "off" (skip
	// heuristics, never warn — operator overriding the warning, e.g.
	// confirmed self-hosted single-node PG without HA). Empty string
	// is treated as "auto".
	//
	// Added in v0.17.3 (Bug 36): the v0.17.2 heuristics systematically
	// missed managed-PG services with tenant-isolated permissions
	// (PlanetScale Postgres, Aurora when superuser-restricted, etc.),
	// so operators of those services need an explicit override.
	PatroniMode string

	// TargetSchema is the per-source target-schema namespace
	// (`--target-schema NAME`, ADR-0031). When set, every emitted
	// CREATE TABLE / ALTER TABLE / CREATE INDEX / CREATE TYPE
	// prefixes its identifier with the schema name. Used to land
	// multiple sluice streams on the same target without table-name
	// collisions (Shape B microservices → analytics warehouse).
	//
	// PG-only: engines whose [ir.Capabilities.SchemaScope] is not
	// [ir.SchemaScopeNamespaced] (today: MySQL) refuse the field at
	// validate time with a clear "use a different --target DSN
	// database to namespace per-source streams" message. Empty
	// preserves today's behaviour (use the target DSN's default
	// schema, typically `public`).
	//
	// The control tables (`sluice_cdc_state`) stay in the DSN's
	// default schema regardless — they're per-target metadata, not
	// per-stream user data, and multiple target-schemas on one DSN
	// share a single state table. Stream-id keys disambiguate.
	TargetSchema string

	// EnabledPGExtensions is the operator's `--enable-pg-extension`
	// allowlist (ADR-0032). PG → PG only — the validate gate refuses
	// the field when either side isn't PG. Threaded through the
	// freshly-opened source SchemaReader / RowReader / SnapshotStream
	// and target SchemaWriter / RowWriter / ChangeApplier via
	// [ir.ExtensionAware]. Empty preserves the pre-v0.26.0
	// loud-failure behaviour for extension-owned types.
	EnabledPGExtensions []string

	// DatabaseFilter selects which source databases participate in a
	// multi-database fan-out `sync start` (ADR-0074 Phase 1b.2). A
	// non-empty Include/Exclude — or AllDatabases — switches the Streamer
	// into multi-database mode: it cold-starts the selected databases
	// under ONE spanning consistent snapshot, copies each to a same-named
	// target namespace, then routes the single server-wide binlog CDC
	// stream per-change to the right namespace. Empty (the zero value)
	// with AllDatabases=false is the default — byte-identical
	// single-database behaviour. Mirrors [Migrator.DatabaseFilter].
	DatabaseFilter DatabaseFilter

	// AllDatabases is the `--all-databases` convenience: stream every
	// non-system database on the source server. Mutually exclusive with a
	// non-empty DatabaseFilter; switches the Streamer into multi-database
	// mode (see DatabaseFilter). Mirrors [Migrator.AllDatabases].
	AllDatabases bool

	// NamespaceMap is the optional per-namespace source → target rename for
	// a multi-namespace fan-out `sync start` (ADR-0142,
	// --map-database/--map-schema). Identity by default (byte-identical
	// pre-ADR-0142 fan-out). A non-empty map ALSO engages multi-database
	// mode and is applied at the cold-start target-namespace derivation AND
	// at the steady-state CDC change-router (a captured change for source
	// namespace OLD is applied into target namespace NEW). Mirrors
	// [Migrator.NamespaceMap].
	NamespaceMap NamespaceRenameMap

	// InjectShardColumn is the ADR-0048 Shape A discriminator-column
	// spec the per-shard streamer opts into via
	// `--inject-shard-column NAME=VALUE`. When [ShardColumnSpec.Engaged]
	// is true, the streamer runs the IR-pass + bulk-copy stamp + CDC
	// applier wiring on every cold-start, and applies the
	// populated-target preflight (the loud replacement for
	// `--force-cold-start`). Zero-value (empty Name) is the no-op
	// default — single-source streams pay nothing.
	//
	// Mirror of [Migrator.InjectShardColumn]. See ADR-0048's resolved
	// DP-1 (option (a) — two-surface split) for the design rationale.
	InjectShardColumn ShardColumnSpec

	// AllowCrossShardMerge opts out of the Bug 152 cross-shard-collision
	// preflight (CLI: `--allow-cross-shard-merge`). Mirror of
	// [Migrator.AllowCrossShardMerge]: set only when the key is globally
	// unique across shards; the safe default is false (guard active).
	AllowCrossShardMerge bool

	// CoordinateLiveDDL controls ADR-0054 Shape A Phase 2 live
	// cross-shard DDL coordination. Default true (engaged when
	// [InjectShardColumn] is engaged; no-op otherwise). Operators on
	// the v0.72.x drained model — `sync stop --wait` → migrate →
	// `sync start --resume` — set this to false via the CLI's
	// `--no-coordinate-live-ddl` flag to preserve pre-ADR-0054
	// semantics.
	//
	// When engaged, observed DDL boundaries on each per-shard stream
	// route through a [LeaseManager] keyed off the consolidated
	// target table; the first stream to notice acquires the lease,
	// applies the DDL exactly once, and records the applied schema
	// version + DDL checksum. Peer streams observe the recorded
	// state and skip the apply, continuing CDC against the migrated
	// target without a drain.
	CoordinateLiveDDL bool

	// ShardCoordinationLease holds the lease-timing knobs operator-
	// tunable via the `--shard-coordination-{lease-duration,
	// renew-deadline,retry-period}` CLI flags. The zero value uses
	// the ADR-0054 §2 defaults (30s / 20s / 10s). Only consulted
	// when [CoordinateLiveDDL] is true and [InjectShardColumn] is
	// engaged.
	ShardCoordinationLease LeaseConfig

	// SchemaChanges is the ADR-0091 single-stream schema-change-
	// forwarding mode: "forward" (the default) or "refuse". When
	// "forward" AND [InjectShardColumn] is NOT engaged, the streamer
	// wraps the CDC change channel with the [interceptAddColumnForward]
	// intercept: every unambiguous source DDL shape (ADD / DROP COLUMN,
	// ALTER COLUMN TYPE / NULLABILITY, CREATE / DROP INDEX, ADD / DROP /
	// MODIFY CHECK) forwards to the target via [ir.ShapeDeltaApplier];
	// RENAME COLUMN, multi-shape combos, and computed-DEFAULT ADD
	// COLUMN refuse loudly with the drained-model recovery hint. When
	// "refuse", any source DDL refuses loudly (the conservative
	// pre-ADR-0091 behavior).
	//
	// Empty string is treated as "forward" (the default) so
	// zero-valued Streamer configs in tests and older callers get the
	// shipping default. Wired via the `--schema-changes` CLI flag.
	//
	// No-op when [InjectShardColumn] is engaged: Shape A's
	// [BoundaryRouter] already handles every recognized shape via
	// the lease (ADR-0054 DP-E).
	SchemaChanges string

	// ForwardSchemaAddColumn is the DEPRECATED ADR-0058 ADD-COLUMN-only
	// opt-in, subsumed by [SchemaChanges] (ADR-0091). When set it
	// triggers a one-time deprecation WARN; it does NOT change behavior
	// (forwarding is on by default via SchemaChanges). Kept for one
	// deprecation cycle so existing configs/flags keep working.
	ForwardSchemaAddColumn bool

	// BackfillAddedColumn enables source-side bounded backfill of
	// already-shipped target rows after a forwarded ADD COLUMN
	// lands. Only consulted when [ForwardSchemaAddColumn] is true.
	// ADR-0058 §1c.
	//
	// When set, the intercept opens a [ir.BatchedRowReader] against
	// the source and emits synthetic [ir.Update] events for every
	// row already on the target, populating the new column with the
	// source's per-row value rather than the column's DEFAULT.
	BackfillAddedColumn bool

	// ApplyRetryAttempts caps the number of consecutive retriable
	// apply failures the streamer will absorb before giving up and
	// returning the underlying error. ADR-0038. Zero or one means
	// "no retry" (preserve pre-v0.42.0 fail-on-first behaviour);
	// higher values enable bounded retry. Default when nil/zero on
	// the [Streamer] receiver is supplied by the CLI's flag default
	// (`--apply-retry-attempts`, default 8).
	//
	// Consecutive-failure counter resets when the persisted CDC
	// position advances between attempts (a successful batch landed
	// since the last failure) — so a streamer surviving for hours
	// doesn't carry retry debt forward.
	ApplyRetryAttempts int

	// ApplyRetryBackoffBase is the base interval for the exponential
	// backoff between retriable apply failures. ADR-0038. Doubles
	// on each consecutive failure, capped at [ApplyRetryBackoffCap].
	// Zero means use the default (100ms). Only consulted when
	// [ApplyRetryAttempts] > 1.
	ApplyRetryBackoffBase time.Duration

	// ApplyRetryBackoffCap is the upper bound on each per-attempt
	// backoff interval. ADR-0038. Zero means use the default (30s).
	// Only consulted when [ApplyRetryAttempts] > 1.
	ApplyRetryBackoffCap time.Duration

	// HeartbeatInterval, when > 0, enables a per-stream goroutine that
	// logs an INFO line every interval reporting the stream is alive.
	// GitHub #23 Phase A: distinguishes silent-stall (process alive
	// but no apply, no log) from wedge (process alive, no heartbeat
	// either). Zero disables; the CLI's default is 60s. Operators
	// chasing a stall set --heartbeat-interval=10s for faster signal.
	HeartbeatInterval time.Duration

	// PollInterval overrides the engine's default CDC-reader poll
	// cadence. Roadmap item 18(c) / ADR-0066 §6. Consulted only by
	// CDC readers that implement the [pollIntervalSetter] optional
	// surface — today: postgres-trigger (default 1 s). Engines whose
	// CDC stream is push-based (pgoutput, binlog, VStream) silently
	// ignore this. Zero leaves the engine's default in place.
	PollInterval time.Duration

	// AutoPruneChangeLog opts IN to the ADR-0137 Phase-B in-stream auto-prune
	// of a trigger-CDC source's `sluice_change_log` (Bug 165). When set AND the
	// source implements [ir.ChangeLogPruner] (sqlite-trigger / d1-trigger /
	// pgtrigger), a failure-isolated sidecar reaps durably-applied change-log
	// rows on a cadence so the source doesn't grow unbounded, without the
	// operator scheduling `sluice trigger prune` via cron.
	//
	// Zero-value safety (the v0.99.51 trap): default false = OFF = no
	// auto-prune, the pre-Phase-B behaviour for EVERY construction (CLI, tests,
	// broker/chain paths, future callers). Auto-deleting source rows is made an
	// explicit operator opt-in for the first cut. The sidecar is ALSO a no-op
	// for any non-trigger source (typed-nil pruner), so a set flag on a vanilla
	// PG/MySQL sync does nothing. Set true only by `--auto-prune-change-log`.
	AutoPruneChangeLog bool

	// AutoPruneInterval is the wall-clock cadence the auto-prune sidecar reaps
	// at. Zero ⇒ [defaultAutoPruneInterval] (5 min). Only consulted when
	// AutoPruneChangeLog is set.
	AutoPruneInterval time.Duration

	// AutoPruneKeep is the belt-and-suspenders safety margin: keep the most
	// recent N change-log ids below the durable frontier unpruned (the same
	// meaning as `sluice trigger prune --keep`). The frontier itself is already
	// durably applied so even 0 is safe. Only consulted when AutoPruneChangeLog
	// is set; a negative value is clamped to 0.
	AutoPruneKeep int64

	// SourceHeartbeatInterval, when > 0, enables the F17 source-side
	// heartbeat writer (ADR-0061). The streamer attaches a per-stream
	// goroutine that periodically INSERTs a row into the sluice-owned
	// heartbeat table on the source DB; the INSERT generates WAL /
	// binlog traffic so the consumer's position advances even against
	// an otherwise-idle source. Zero (the default) leaves the source
	// untouched — F17 is opt-in because the INSERT is a behaviour
	// change on the source DB that operators on regulated systems must
	// explicitly enable. Operators on low-traffic / idle-prone sources
	// set --source-heartbeat-interval=30s (typical) to prevent slot
	// eviction / binlog rotation past the consumer's position.
	SourceHeartbeatInterval time.Duration

	// SourceHeartbeatPruneWindow is the age threshold for the periodic
	// DELETE that bounds heartbeat-table growth. Rows whose ts column
	// is older than this duration are dropped. Zero disables prune (the
	// table grows unbounded — useful for forensic inspection on short
	// runs); the production default is 1h (see
	// [DefaultSourceHeartbeatPruneWindow]). Only consulted when
	// [SourceHeartbeatInterval] > 0.
	SourceHeartbeatPruneWindow time.Duration

	// SourceHeartbeatTableName overrides the per-source table name the
	// F17 writer uses (default `sluice_heartbeat`). Operators with a
	// hostile DBA-managed namespace can pre-create a differently-named
	// table and point the writer at it. Empty falls back to the
	// default. Only consulted when [SourceHeartbeatInterval] > 0.
	SourceHeartbeatTableName string

	// NoSourceHeartbeat is the opt-OUT escape hatch: when true, the
	// F17 writer is skipped even if [SourceHeartbeatInterval] > 0
	// (e.g. an operator overrode the YAML config's interval via the
	// CLI flag without wanting to edit YAML). The streamer's
	// attachSourceHeartbeat returns the noop attachment immediately
	// when this is set.
	NoSourceHeartbeat bool

	// sourceErrFn is the per-attempt closure that returns the source
	// CDC reader's stored Err() — see GitHub issue #19. The pump's
	// channel close is the normal EOF path; without surfacing the
	// reader's error, a transient `read: connection reset` from the
	// source mid-stream produced a clean nil exit instead of a
	// retriable shape. Each [runOnce] iteration resets to nil before
	// opening a fresh reader; coldStart / warmResume populate the
	// field with the reader's Err method when the type exposes one
	// (every shipping CDC reader does). runOnce reads after
	// dispatchApply returns; the wrapped error is surfaced to
	// runWithRetry which classifies it against [ir.RetriableError]
	// in the standard way.
	sourceErrFn func() error

	// sourceReshard is the per-attempt CDC reader cast to
	// [ir.ReshardReopener] when the reader implements it (the VStream
	// flavors; nil for binlog MySQL / Postgres). ADR-0094: after the
	// change channel closes cleanly, runOnce calls ReopenAfterReshard to
	// follow a source reshard (split/merge/MoveTables) onto the new shard
	// layout instead of exiting. Reset to nil per attempt alongside
	// sourceErrFn; populated where sourceErrFn is.
	sourceReshard ir.ReshardReopener

	// changeLogPruner is the per-attempt CDC reader cast to
	// [ir.ChangeLogPruner] when the reader implements it (the trigger-CDC
	// engines: sqlite-trigger / d1-trigger / pgtrigger; nil for every other
	// source, which has no change-log). ADR-0137 Phase B: the auto-prune
	// sidecar ([startAutoPruneChangeLog]) uses it to reap the source
	// change-log on a cadence. Reset to nil per attempt alongside sourceErrFn;
	// populated where sourceErrFn is (coldStart / warmResume).
	changeLogPruner ir.ChangeLogPruner

	// runOnceFn is a test seam: when non-nil, [Run] / [runWithRetry]
	// invoke it in place of [runOnce]. Production always leaves it nil
	// (runOnceCall defaults to s.runOnce), so behaviour is identical;
	// the ADR-0093 reactive-cold-start tests inject a stub here to drive
	// the retry/recovery loop without booting a full pipeline.
	runOnceFn func(context.Context) error

	// aimdResumeSize carries the AIMD controller's shrunk batch size
	// ACROSS runOnce restarts within one Run (the v0.99.69
	// sustained-tx-killer fix). The controller is constructed per
	// runOnce (maybeAttachAIMDController), so a tx-killer abort that
	// propagates out of runOnce to the ADR-0038 streamer-level retry
	// loop would otherwise discard the shrink and re-attach a fresh
	// controller at the ceiling — re-submitting the same too-large
	// batch that was just killed, exhausting the retry budget. The
	// controller's OnShrink hook stores the post-MD size here; the
	// NEXT maybeAttachAIMDController reads it as the new InitialSize so
	// the re-applied batch starts small and converges. atomic because
	// OnShrink fires from the apply goroutine while the next runOnce
	// reads it from the retry-loop goroutine. Zero = "no prior shrink;
	// start at the ceiling" — the natural cold-start default. Never
	// grows back to the ceiling on its own: a healthy stream's AI
	// re-climbs within the live controller, and the next-run InitialSize
	// is clamped to [1, ceiling] so this can never exceed the operator's
	// --apply-batch-size cap.
	aimdResumeSize atomic.Int64

	// leaseMgr is the ADR-0054 Shape A Phase 2 live-coordination
	// lease manager. Constructed by [engageShardCoordination] when
	// [CoordinateLiveDDL] is true, [InjectShardColumn] is engaged,
	// and the target applier implements
	// [ir.ShardConsolidationLeaseStore]. Nil otherwise (drained
	// model or non-Shape-A stream).
	leaseMgr *LeaseManager

	// boundaryRouter ties leaseMgr to the per-shape applier + prober
	// for the SchemaSnapshot intercept path (ADR-0054 Phase 2d).
	// Constructed alongside leaseMgr when ALL of
	// [ir.ShardConsolidationLeaseStore], [ir.ShapeDeltaApplier], and
	// [ir.ShardConsolidationProber] are implementable on the target.
	// Nil when live-coordination is engaged but the engine doesn't
	// expose the apply/probe surfaces — in that case the engagement
	// itself refused loudly upstream, so a nil router here means the
	// stream is the no-coordinate path.
	boundaryRouter *BoundaryRouter

	// shapeWriter is the SchemaWriter the BoundaryRouter uses for
	// the per-shape DDL apply path. Owned by the Streamer's Run
	// lifetime; closed alongside other streamer resources.
	shapeWriter ir.SchemaWriter

	// addColumnForwardWriter is the SchemaWriter the ADR-0058
	// single-stream ADD COLUMN forwarding intercept uses to issue
	// [ir.SchemaDeltaApplier.AlterAddColumn] against the target.
	// Constructed by [engageAddColumnForward] when
	// [ForwardSchemaAddColumn] is true and Shape A is NOT engaged
	// (Shape A has its own writer via [shapeWriter]). Closed via
	// [closeAddColumnForward] alongside other streamer resources.
	addColumnForwardWriter ir.SchemaWriter

	// addColumnForwardReader is the source-side row reader used by
	// the ADR-0058 backfill loop (only opened when
	// [BackfillAddedColumn] is true). Owned by the Streamer's Run
	// lifetime.
	addColumnForwardReader ir.RowReader

	// addColumnForwardSchemaReader is the source-side schema reader
	// used by the ADR-0058 §2a volatility probe (Bug 90 closure,
	// v0.79.1). Always opened alongside [addColumnForwardWriter] when
	// [ForwardSchemaAddColumn] is true and Shape A is NOT engaged.
	// The intercept calls ReadSchema() at most once per ADD COLUMN
	// forward to surface the source's canonical DEFAULT expression
	// text — pgoutput's RelationMessage and MySQL's TableMapEvent
	// both drop the DEFAULT, so the in-band CDC IR can't be the
	// source of truth for the volatility classification.
	addColumnForwardSchemaReader ir.SchemaReader

	// schemaSnapshotErr is the error sink the SchemaSnapshot
	// intercept writes to when the BoundaryRouter refuses (probe
	// inconsistent, checksum mismatch, unrecognized shape). The
	// streamer's runOnce surfaces the error via the standard
	// dispatchErr classification path.
	schemaSnapshotErr atomic.Pointer[error]

	// whereFilter is the compiled ADR-0173 Phase 2 client-side row filter,
	// built by [preflightRowFilters] from [RowFilters] at sync-start and
	// consumed by the CDC-leg [interceptWhereFilter]. Nil when no --where is
	// configured. whereFilterErr is the error sink that intercept writes to
	// when a filtered UPDATE/DELETE arrives without a before-image (the
	// mid-stream partial-image belt); surfaced via the standard dispatchErr
	// classification path, mirroring schemaSnapshotErr.
	whereFilter    *whereCDCFilter
	whereFilterErr atomic.Pointer[error]

	// serverSideRowFilters is the subset of [RowFilters] pushed into a VStream
	// source's SERVER-side stream filter (cold-start COPY + warm-resume). It
	// equals RowFilters on every path EXCEPT a VStream source with a
	// PAD-SPACE-collation --where column: those tables are OMITTED here (streamed
	// unfiltered server-side) and filtered CLIENT-side instead, because the
	// VStream server filter is NO-PAD and can't reproduce their `=` (audit
	// 2026-07-19 A0). Set by [preflightRowFilters]; read by the cold-start open
	// and the warm-resume SetServerSideRowFilters.
	serverSideRowFilters map[string]string
	// clientCopyFilter is the PAD-faithful cold-start COPY keep-predicate for the
	// A0 fallback — non-nil ONLY on a VStream source with a PAD-SPACE-collation
	// --where column, installed on the snapshot reader via
	// [ir.ClientCopyFilterSetter]. nil on every other path.
	clientCopyFilter func(table string, row ir.Row) bool

	// coldStartSeedSnapshots is the ADR-0054 Bug 83 fix: synthetic
	// SchemaSnapshots reflecting the pre-Shape-A-rewrite source IR
	// per filtered table. Set by [coldStart] before
	// [translate.InjectShardColumn] runs; consumed by
	// [interceptSchemaSnapshotsForCoordination] to pre-populate its
	// boundary cache so the first CDC SchemaSnapshot is correctly
	// classified as a real boundary (not as the cold-start anchor).
	// Nil when --inject-shard-column is unset, --no-coordinate-live-ddl
	// is set, or the stream is warm-resuming (warm resume doesn't run
	// cold-start; the intercept's cache is seeded by the resumed
	// position's first observed SchemaSnapshot, which is fine because
	// the applier's target schema is the same as when cold-start
	// completed).
	coldStartSeedSnapshots []ir.SchemaSnapshot

	// resolvedApplyConcurrency is the per-attempt resolution of the operator's
	// [ApplyConcurrency] field (ADR-0106, item 31): `0 → auto:N`, `1 → 1`
	// (explicit serial), `N > 1 → N`. Computed once per [runOnce] attempt by
	// [resolveApplyConcurrency] (before the applier opens), then read by
	// [openApplier] (the [ir.ApplyConcurrencySetter] plumb) and
	// [maybeAttachAIMDController] (the per-lane AIMD wiring) so the auto:N
	// default actually engages concurrency everywhere — not just on the one
	// CLI path. Re-derived each attempt because the PG connection-budget probe
	// that bounds auto:N may see a different live slot count on a retry. The
	// operator's raw [ApplyConcurrency] is never mutated (a retry must
	// re-resolve from the same input).
	resolvedApplyConcurrency int

	// laneAIMDControllers holds the W per-lane AIMD controllers built by
	// [attachLaneAIMDControllers] on the ADR-0104/0105 concurrent key-hash
	// apply path. The serial single-controller path returns its controller
	// directly to the metrics phase; the per-lane path has no single
	// controller to return, so it parks the slice here for
	// [phaseStartMetricsServer] to attach to the metrics server (each lane
	// emitted as its own `lane="N"`-labeled series). Re-set per runOnce
	// attempt alongside the controllers it describes; nil on the serial path
	// and when --apply-concurrency resolves to 1.
	laneAIMDControllers []*appliercontrol.Controller

	// syncLag is the per-attempt engine-neutral "seconds behind source"
	// tracker (roadmap item 45). Created fresh at the top of each [runOnce]
	// attempt ONLY when an operator opted into the metrics endpoint or a
	// sync-lag alert (otherwise nil, so the default apply path adds no
	// interceptor and no goroutine). The change-stream interceptor writes it
	// (lock-free), the /metrics scrape and the alerter tick read it.
	syncLag *syncLagTracker
}

// Run executes a snapshot+CDC stream with optional retry on
// transient applier errors. See [Streamer] for the field surface
// and ADR-0038 for the retry policy.
//
// When [Streamer.ApplyRetryAttempts] <= 1 (the v0.41.x default
// when the field is zero on the receiver), Run is a thin wrapper
// over [runOnce] preserving pre-v0.42.0 behaviour. When > 1, Run
// catches errors that satisfy [ir.RetriableError] and retries the
// inner pipeline with exponential backoff, returning a terminal
// error after a budget of consecutive same-position failures.
//
// Returns nil on clean ctx cancellation or successful stream
// completion; non-nil on terminal error or retry-budget exhaustion.
// Resources (snapshot stream, target writers, applier) are
// released by each [runOnce] iteration regardless of outcome.
func (s *Streamer) Run(ctx context.Context) error {
	// Driver/host mismatch pre-flight — runs once here (the DSNs can't
	// change between retry attempts) and before any reader/writer is
	// opened. Refuses e.g. the vanilla mysql driver pointed at a
	// PlanetScale host, naming the --source-driver / --target-driver flag
	// to fix. No-op for engines without ir.DSNValidator.
	if err := preflightDSNValidation(s.Source, s.SourceDSN, s.Target, s.TargetDSN); err != nil {
		return err
	}

	// Managed-host advisories (items 69a/70a): WARN-level sibling of the
	// refusal above. cdc=true — a sync anchors and consumes a CDC
	// position, so both the pooler-endpoint WARN (most poolers strip
	// the replication parameter, failing slot creation) and the managed-
	// MySQL binlog-retention WARNs (DigitalOcean, Vultr) apply here.
	migcore.WarnSourceHostAdvisories(ctx, s.Source, s.SourceDSN, true)

	// GitHub #18 Phase 2: static safety-rail. Warn (don't refuse)
	// when an operator combination is known to hit Vitess's 20s
	// tx-killer under sustained load. The threshold matches the
	// validation-rig observations (PS-MySQL cross-region failed at
	// batch=100, worked at 25-50).
	warnIfApplyBatchSizeRisky(ctx, s)

	// ADR-0173 Phase 2: continuous filtered sync. Compile each `--where`
	// predicate against the source schema + verify the source delivers full
	// before-images, ONCE here (before any attempt, cold-start or warm
	// resume) — an unsupported predicate or a mis-configured source refuses
	// up front, never after data moves. No-op when RowFilters is empty.
	if err := s.preflightRowFilters(ctx); err != nil {
		return err
	}

	attempts := s.ApplyRetryAttempts
	if attempts < 1 {
		attempts = 1
	}
	if attempts == 1 {
		// Retry disabled: single-attempt semantics identical to v0.41.x,
		// except a reactive [ir.ErrPositionInvalid] is routed to the
		// one-shot cold-start re-snapshot (ADR-0093) the same as the
		// retry path — so the VStream purged-position recovery does not
		// depend on --apply-retry-attempts being set.
		return s.runOnceWithReactiveResnapshot(ctx)
	}
	return s.runWithRetry(ctx, attempts)
}

// runOnceCall invokes the per-attempt pipeline body. Production leaves
// [runOnceFn] nil and this calls [runOnce]; the ADR-0093 tests inject a
// stub so they can drive the reactive-cold-start loop without a full
// pipeline boot.
func (s *Streamer) runOnceCall(ctx context.Context) error {
	if s.runOnceFn != nil {
		return s.runOnceFn(ctx)
	}
	return s.runOnce(ctx)
}

// runOnceWithReactiveResnapshot runs the pipeline once and, on a reactive
// [ir.ErrPositionInvalid] (ADR-0093 — e.g. a VStream resume from a purged
// GTID position surfaced via the pump's Recv), applies the one-shot
// cold-start re-snapshot recovery. Used by the single-attempt [Run] path;
// [runWithRetry] inlines the equivalent recovery in its loop so it
// composes with the ADR-0038 retry budget.
//
// Bounded to ONE re-snapshot: a second consecutive ErrPositionInvalid
// after the forced cold-start is terminal (loud), since it means the
// source is purging faster than a snapshot completes.
func (s *Streamer) runOnceWithReactiveResnapshot(ctx context.Context) error {
	resnapshotted := false
	for {
		err := s.runOnceCall(ctx)
		if !s.isReactiveInvalidPosition(err) {
			return err
		}
		retry, rerr := s.reactiveResnapshotDecision(ctx, err, resnapshotted)
		if !retry {
			return rerr
		}
		resnapshotted = true
	}
}

// isReactiveInvalidPosition reports whether err is an
// [ir.ErrPositionInvalid] surfaced reactively from a run attempt (ADR-0093)
// — i.e. invalid-position, and NOT a bare ctx cancellation/deadline. A
// wrapped retriable error carrying DeadlineExceeded is excluded because it
// is handled by the retry path, not the cold-start path.
func (s *Streamer) isReactiveInvalidPosition(err error) bool {
	if err == nil {
		return false
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		return false
	}
	// A genuine ctx termination (operator Ctrl-C / sync stop) must not be
	// mistaken for an invalid-position recovery trigger. ErrPositionInvalid
	// never wraps ctx errors in practice, but guard explicitly.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

// reactiveResnapshotDecision implements the ADR-0093 reactive recovery
// policy for a confirmed [ir.ErrPositionInvalid] (see
// [isReactiveInvalidPosition]). It returns (retry=true, nil) when the
// caller should re-run the pipeline once in forced cold-start, or
// (retry=false, terminalErr) when the position must surface as a loud
// terminal error.
//
//   - auto (default, !Suppress) && !alreadyResnapshotted: log a loud
//     WARN, set RestartFromScratch so the next attempt re-snapshots
//     non-destructively, and signal retry.
//   - auto (default, !Suppress) && alreadyResnapshotted: bounded —
//     a second consecutive invalid position after a fresh cold-start is
//     terminal (the source is purging faster than a snapshot completes).
//   - SuppressAutoResnapshotOnInvalidPosition: opt-out — surface the loud,
//     actionable terminal error naming the recovery commands.
func (s *Streamer) reactiveResnapshotDecision(ctx context.Context, err error, alreadyResnapshotted bool) (bool, error) {
	if s.SuppressAutoResnapshotOnInvalidPosition {
		return false, invalidPositionOptOutError(err)
	}
	if alreadyResnapshotted {
		return false, fmt.Errorf(
			"pipeline: source position is still invalid immediately after an automatic cold-start re-snapshot — the source is purging its binlogs/retention faster than a fresh snapshot can complete; this cannot be auto-recovered. Reduce snapshot duration (more --table-parallelism / --bulk-parallelism), widen the source's binlog retention, or run with --no-auto-resnapshot and recover deliberately: %w", err,
		)
	}
	slog.WarnContext(
		ctx, "source resume position is no longer valid; auto re-snapshotting (cold-start) to recover (ADR-0093). Suppress with --no-auto-resnapshot.",
		slog.String("stream_id", s.resolveStreamID()),
		slog.String("err", err.Error()),
	)
	// Force a fresh cold-start on the next attempt. RestartFromScratch
	// discards the persisted (now-invalid) position; the cold-start gate
	// then makes the re-copy land cleanly per source: an idempotent reader
	// (VStream/PlanetScale) absorbs the re-copied overlap via UPSERT with no
	// target drop, while a non-idempotent reader (native MySQL binlog, plain
	// INSERT) drops + recreates the in-scope target tables first so the copy
	// doesn't dup-key (Error 1062) on the prior copy's leftover rows. The
	// cdc-state row is preserved either way.
	s.RestartFromScratch = true
	return true, nil
}

// invalidPositionOptOutError formats the loud, actionable terminal error
// returned when --no-auto-resnapshot suppresses the automatic re-snapshot
// (ADR-0093). It names the explicit recovery commands so the operator can
// decide. Shared by the reactive path and the pre-flight fall-through
// sites so the opt-out message is identical everywhere.
func invalidPositionOptOutError(err error) error {
	return fmt.Errorf(
		"pipeline: the persisted source position is no longer valid (older than the source's retained binlogs / purged) and --no-auto-resnapshot is set, so sluice will not auto re-snapshot. Re-run with --restart-from-scratch for a fresh cold-start (idempotent sources absorb the overlap with no target drop; non-idempotent sources such as native MySQL binlog drop + recreate the in-scope target tables first so the plain-INSERT copy starts clean, preserving the cdc-state row), or --reset-target-data to also clear the cdc-state row and drop the tables: %w", err,
	)
}

// runOnce executes a single snapshot+CDC pipeline attempt. The
// public [Run] method wraps this with the ADR-0038 retry policy
// when [Streamer.ApplyRetryAttempts] > 1; otherwise Run delegates
// here directly. Single-attempt semantics match v0.41.x.
//
// Returns nil on clean ctx cancellation; non-nil on any phase
// failure. Resources (snapshot stream, target writers, applier)
// are released before return regardless of outcome.
func (s *Streamer) runOnce(ctx context.Context) error {
	// ---- 0. Validate + resolve identity ----
	// Field-surface validation, per-attempt state reset, slot-name +
	// engine-default-exclusion conventions, stream-id resolution.
	streamID, err := s.phaseResolveStreamIdentity(ctx)
	if err != nil {
		return err
	}

	// ---- 0.5. Resolve the adaptive apply-concurrency default (ADR-0106) ----
	// `--apply-concurrency 0` (unset) → auto:N (connection-budget-bounded);
	// `1` stays explicit serial; `N > 1` honored. Resolved HERE (per attempt,
	// before the applier opens) so both the applier plumb (step 1) and the
	// per-lane AIMD wiring (step 1.5) read the same resolved value — the
	// streamer-level default that makes auto:N actually engage everywhere,
	// not just on the one CLI path.
	s.resolvedApplyConcurrency = s.resolveApplyConcurrency(ctx)

	// ---- 1. Open / wire the applier first ----
	applier, ownsApplier, err := s.openApplier(ctx)
	if err != nil {
		return err
	}
	if ownsApplier {
		defer migcore.CloseIf(applier)
	}
	// ADR-0054 Phase 2d: release shape-coordination resources when
	// engaged (SchemaWriter for per-shape DDL; the lease store /
	// prober live on the applier and are released by migcore.CloseIf above).
	defer s.closeShardCoordination()
	// ADR-0058: release the single-stream ADD COLUMN forwarding
	// resources (SchemaWriter + optional source RowReader). No-op
	// when the feature isn't engaged.
	defer s.closeAddColumnForward()
	// PII Phase 2.c (v0.59.0): plumb the resolved stream-id into the
	// applier so randomize:* CDC redactions derive replay-stable seeds.
	// Empty streamID is a no-op via applyStreamID's guard; in
	// practice resolveStreamID always returns a non-empty value.
	applyStreamID(applier, streamID)

	// ---- 1.5. Optional AIMD apply-batch-size controller (ADR-0052) ----
	// When --auto-tune is on (the default) AND --apply-batch-size > 1
	// AND the applier exposes both BatchSizeProviderSetter +
	// BatchObserverSetter, construct a controller and wire it onto
	// the applier. The static ApplyBatchSize becomes a CAP the
	// controller can never exceed; the floor is 1 (ADR-0017
	// conservative-default). Off paths (--no-auto-tune, ApplyBatchSize
	// <= 1, engine without setters) preserve the pre-ADR-0052
	// static-cap behaviour bit-for-bit — zero overhead on the
	// opt-out path.
	aimdController := s.maybeAttachAIMDController(ctx, applier, streamID)
	// TEST-ONLY: hand the applier's per-flush observer seat to the seam
	// when the AIMD controller didn't take it. See the var doc on
	// [batchApplyObserverForTest]; production runs never set it.
	if batchApplyObserverForTest != nil && aimdController == nil {
		if setter, ok := applier.(ir.BatchObserverSetter); ok {
			setter.SetBatchObserver(batchApplyObserverForTest)
		}
	}

	// ---- 1.6. Target-telemetry sidecars for the WHOLE attempt (ADR-0107
	// items 35 + 36, roadmap item 39) ----
	// The rolling-history recorder + threshold alerter are started HERE (not
	// at the apply phase) so they cover the COLD-COPY phase too — the loaded,
	// storage-grow-prone window where they matter most. The applier opened in
	// step 1 lives for the whole attempt and is idle during cold-copy, so
	// reusing it is safe. telemetryCtx is cancelled when this attempt returns,
	// so the goroutines exit cleanly (no cross-attempt leak on warm-resume).
	// Total no-op when PlanetScale telemetry isn't configured.
	telemetryCtx, cancelTelemetry := context.WithCancel(ctx)
	defer cancelTelemetry()
	s.startTelemetrySidecars(telemetryCtx, applier, streamID)

	// ---- 1a. Optional Prometheus metrics endpoint ----
	// When --metrics-listen is set, a small HTTP server runs alongside
	// the stream exposing /metrics, /healthz, and /readyz. Off by
	// default; opt-in. Lifecycle is scoped to the streamer's Run —
	// started by the phase below, closed by the defers here. A bind
	// failure at startup is fatal (operator asked for the listener;
	// misconfigured port shouldn't be silent). Skipped on DryRun:
	// dry-run doesn't run a real stream, so metrics for it aren't
	// useful.
	//
	// metricsSrv is hoisted out of the phase so the apply-phase
	// preamble below can flip its /readyz signal after cold-start /
	// warm-resume completes. The defers stay HERE — not in the phase —
	// so teardown order against the applier / shard-coordination
	// defers above is exactly the pre-split order.
	// Roadmap item 45: create the per-attempt sync-lag tracker BEFORE the
	// metrics server (which attaches it) and the apply-phase interceptor
	// (which feeds it). nil unless the operator opted into /metrics or a
	// sync-lag alert, so the default apply path stays byte-identical.
	if !s.DryRun && s.syncLagObservationWanted() {
		// Roadmap item 46 (ADR-0121 §5): seed the tracker with ApplyDelay so a
		// delayed replica's intentional hold is subtracted out of the reported
		// sync lag (0 on a non-delayed stream — unchanged behaviour).
		s.syncLag = newSyncLagTracker(s.ApplyDelay)
	} else {
		s.syncLag = nil
	}

	metricsSrv, spillCleanup, err := s.phaseStartMetricsServer(ctx, applier, aimdController, streamID)
	if err != nil {
		return err
	}
	defer spillCleanup()
	if metricsSrv != nil {
		defer func() { _ = metricsSrv.Close() }()
	}

	// ---- 1b. Pre-emptive slot-health probe (ADR-0059, F13) ----
	// Always-on (not gated on --metrics-listen): the operator-visible
	// surface is structured slog WARNs, not a scrape endpoint. Skipped
	// on DryRun for the same reason as the metrics server. Non-fatal:
	// a missing reporter (cross-engine pair with a MySQL source) or a
	// failed source-DB open leaves the probe unattached and the stream
	// runs without F13 surfacing — same shape as attachSpillReporter.
	if !s.DryRun {
		slotProbe := s.attachSlotHealthProbe(ctx, streamID)
		defer slotProbe.Close()
	}

	// ---- 1c. Source-side heartbeat writer (ADR-0061, F17) ----
	// Opt-in (gated on --source-heartbeat-interval > 0). The writer
	// periodically INSERTs a row into a sluice-owned table on the
	// source so the CDC consumer's position advances even against an
	// otherwise-idle source — preventing PG slot eviction / MySQL
	// binlog rotation past the consumer position. Skipped on DryRun
	// (writes are not dry-run-safe). Non-fatal on every branch: a
	// missing engine surface, failed source open, or insufficient
	// privilege all WARN once and leave the writer unattached. See
	// attachSourceHeartbeat for the gating logic.
	if !s.DryRun {
		heartbeat := s.attachSourceHeartbeat(ctx, streamID)
		defer heartbeat.Close()
	}

	// ---- 2 → 2.7. Prepare the per-target control table ----
	// Existence (2), stale stop-signal clear (2.5), slot-name +
	// target-schema recording (2.6), stream-id collision /
	// source-fingerprint check (2.7) — in that order; all skipped on
	// dry-run.
	if err := s.phasePrepareControlTable(ctx, applier, streamID); err != nil {
		return err
	}

	// ---- 2.8. Resolve the stream's EFFECTIVE publication (ADR-0176
	// prerequisite chunk) ----
	// The recorded-name ratchet + the per-stream default for NEW
	// filtered PG-source streams. Runs after the control table is
	// prepared (the record lives there) and before any source
	// connection opens (the publication name rides EnsurePublication
	// and every START_REPLICATION). No-op for sources without a
	// publication concept.
	if err := s.phaseResolvePublicationScope(ctx, applier, streamID); err != nil {
		return err
	}

	// ---- 3. Look up the persisted position ----
	// Source priority: --position-from-manifest chain terminal >
	// applier.ReadPosition (warm resume) > cold start. The phase doc
	// carries the full rationale, including the cross-engine position
	// re-stamp (Bug 20).
	persisted, found, err := s.phaseLookupPosition(ctx, applier, streamID)
	if err != nil {
		return err
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

	var (
		changes <-chan ir.Change
		// stop closes the CDC reader / snapshot stream so the engine-
		// side streaming goroutine (go-mysql BinlogSyncer, PG slot
		// reader) is joined deterministically when runOnce returns.
		// Cancelling streamCtx alone only unwinds the pump; the syncer
		// goroutine would otherwise run to its reconnect budget after
		// Run returned and leak into the next test, racing the global
		// slog.Default() that captureSlog swaps (the -race FAIL this
		// fixes). Always non-nil; defer is unconditional.
		stop = func() {}
	)
	// The lambda is required, not gocritic's suggested `defer stop()`:
	// stop is reassigned by coldStart/warmResume below, and a bare
	// `defer stop()` would bind the no-op value captured here instead
	// of the real teardown closure.
	defer func() { stop() }() //nolint:gocritic // deferUnlambda: stop is reassigned after this defer; the closure is load-bearing
	// Which branch runs — cold start, warm resume, multi-database
	// fan-out, the ADR-0022 slot-missing fall-through, the v0.99.8
	// interrupted-COPY resume routing — is the phase's job; the
	// contexts and the stop/cancel defers stay HERE so teardown order
	// is unchanged. warmResumed is the ADR-0049 Chunk C cache-prime
	// discriminator: only a true warm-resume primes from storage.
	var warmResumed bool
	changes, stop, warmResumed, err = s.phaseOpenChangeStream(ctx, streamCtx, lsnTracker, applier, streamID, persisted, found)
	if err != nil {
		return err
	}
	if changes == nil {
		// coldStart returns (nil, nil) when the source schema is
		// empty — nothing to do.
		return nil
	}

	// ---- 4b. ADR-0049 Chunk C: prime the applier's active-version
	// cache. On warm resume, the persisted position is non-empty and
	// the prime resolves the schema in effect there for every table
	// with retained history (one storage hit per primed table; the
	// hot path stays cache-only thereafter). On cold start, we pass
	// the brand-new-stream sentinel (empty Position) — the engine
	// short-circuits to a no-op (there is no schema-history yet; the
	// reader's first SchemaSnapshot populates the cache via the
	// engine's post-commit hook).
	//
	// A per-table loud floor (errors.Is ir.ErrPositionInvalid) is
	// propagated verbatim: the persisted position is older than the
	// oldest retained schema version on some table → ADR-0022
	// cold-start re-snapshot is the only safe recovery. Surfacing
	// the error lets the existing runOnce slot-missing fall-through
	// branch above handle it (loud → cold-start), preserving the
	// ADR-0049 DP-2 "loud, never silent" floor.
	//
	// Optional-interface probe: engines that don't implement the
	// primer surface (cross-engine pairs where the applier is an
	// in-memory test stub, or a future engine pre-Chunk-C) silently
	// skip — pre-Chunk-C behaviour with the loud-floor still intact.
	if err := s.phasePrimeSchemaHistoryCache(applyCtx, applier, streamID, warmResumed, persisted); err != nil {
		return err
	}

	// Streaming phase entered — flip /readyz to 200. Orchestrators
	// (k8s, Heroku, systemd) gating traffic on readiness now see the
	// stream as in-service. Bound on the apply-loop entry, not the
	// channel hand-off below, so a failing prime-cache above keeps
	// /readyz at 503 (the right signal — the streamer is about to
	// return an error and exit).
	if metricsSrv != nil {
		metricsSrv.MarkReady()
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
	liveFilter := s.phaseStartApplySidecars(applyCtx, applier, streamID, cancelStream, cancelApply, &stopObserved)

	// Apply, following any source reshard onto the new shard layout
	// (ADR-0094). The OUTER ctx is used for settle (applyCtx may already
	// be cancelled by the time a graceful drain completes).
	return s.applyWithReshardFollow(ctx, applyCtx, applier, streamID, changes, liveFilter, &stopObserved)
}

// applyWithReshardFollow drains the change channel through the applier and,
// on a CLEAN channel close that the source reader reports as a reshard
// (ADR-0094), reopens the stream against the new shard layout (journal-
// stamped GTIDs — no gap/overlap) and re-applies; otherwise it settles
// normally. The loop is bounded by [maxReshardReopensPerRun] so a reshard
// storm or a buggy reader fails loud rather than spinning silently.
//
// Reshard auto-follow is attempted only when: the close was clean
// (dispatchErr==nil — a non-nil error is a real apply failure to
// settle/retry), the reader exposes [ir.ReshardReopener], the sync is
// single-stream (Shape-A reshard is deferred per ADR-0094), and no
// intercept error is pending (a reopen must never mask a schema-forward /
// coordination failure — that must reach [phaseSettleDispatch]).
func (s *Streamer) applyWithReshardFollow(
	ctx, applyCtx context.Context,
	applier ir.ChangeApplier,
	streamID string,
	changes <-chan ir.Change,
	liveFilter *liveAddedFilter,
	stopObserved *atomic.Bool,
) error {
	reshardReopens := 0
	for {
		// Wrap the change channel: live-add-aware table filter → ADR-0054
		// Shape-A SchemaSnapshot intercept → ADR-0058 ADD COLUMN forward
		// intercept. The cold-start seed is consumed and cleared inside
		// the phase on the FIRST call; a reopened iteration re-wires the
		// chain with a nil seed (a reshard is a continuation, not a fresh
		// cold start), which is exactly right.
		filtered := s.phaseWireInterceptChain(applyCtx, changes, liveFilter, streamID)
		dispatchErr := s.dispatchApply(applyCtx, applier, streamID, filtered)

		if dispatchErr == nil && s.sourceReshard != nil && !s.InjectShardColumn.Engaged() && !s.interceptErrorPending() {
			newChanges, wasReshard, rerr := s.sourceReshard.ReopenAfterReshard(applyCtx)
			if wasReshard {
				if rerr != nil {
					return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: reshard reopen: %w", rerr))
				}
				reshardReopens++
				if reshardReopens > maxReshardReopensPerRun {
					return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf(
						"pipeline: reshard reopen budget exhausted after %d reopens in one run "+
							"(possible reshard storm or a reader re-signalling without progress); "+
							"restart the sync to resume", reshardReopens,
					))
				}
				slog.InfoContext(
					applyCtx, "cdc: source reshard — following the new shard layout",
					slog.String("stream_id", streamID),
					slog.Int("reopen", reshardReopens),
				)
				changes = newChanges
				continue
			}
		}

		// Settle the outcome: surface intercept-stored errors, classify
		// the dispatch error (Bug 57: retriable before ctx-termination),
		// surface a source CDC pump error (GitHub #19), and clear the stop
		// flag after a graceful drain.
		return s.phaseSettleDispatch(ctx, applier, streamID, dispatchErr, stopObserved)
	}
}

// maxReshardReopensPerRun bounds ADR-0094 reshard auto-follow within a
// single Run. Each reopen requires a real vtgate JOURNAL (a completed
// reshard), so this ceiling is far above any plausible real-world count —
// it exists only so a buggy reader that re-signals a reshard without
// making progress fails LOUD rather than spinning. A genuine long-lived
// stream that somehow crosses this many reshards just restarts and
// resumes.
const maxReshardReopensPerRun = 256

// interceptErrorPending reports whether an ADR-0054/ADR-0058 SchemaSnapshot
// intercept has stored a (non-nil) error this attempt. Used by the
// reshard auto-follow guard so a reopen never masks a schema-forwarding /
// coordination failure — that must reach [phaseSettleDispatch] and be
// classified/surfaced.
func (s *Streamer) interceptErrorPending() bool {
	p := s.schemaSnapshotErr.Load()
	return p != nil && *p != nil
}

// surfaceSourceError returns the source CDC reader's stored Err()
// when the pump terminated due to a non-cancellation failure
// (GitHub issue #19). Filters two no-op cases:
//
//   - sourceErrFn nil — the engine's reader doesn't expose Err().
//     Pre-v0.46 readers and same-shape future readers stay silent.
//   - srcErr is context.Canceled / context.DeadlineExceeded — the
//     pump's check is best-effort, and a ctx-driven shutdown must
//     not surface as a retriable error that the retry loop would
//     loop on after the parent cancellation.
//
// Returns nil for both no-op cases; the underlying error otherwise.
// The caller wraps with phase + retry-loop context.
func surfaceSourceError(sourceErrFn func() error) error {
	if sourceErrFn == nil {
		return nil
	}
	srcErr := sourceErrFn()
	if srcErr == nil ||
		errors.Is(srcErr, context.Canceled) ||
		errors.Is(srcErr, context.DeadlineExceeded) {
		return nil
	}
	return srcErr
}

// batchApplyObserverForTest is a TEST-ONLY observability seam (ADR-0077's
// onTableCopiedObserver disposition): when non-nil, runOnce installs it as
// the applier's [ir.BatchObserver] so an integration test can count
// coalesced flushes and per-flush row counts — asserting the batching
// MECHANISM directly instead of inferring it from timing-sensitive
// dest-side commit-count deltas (the
// TestStreamer_PostgresToPostgres_BatchedApply threshold-flake class).
// Installed only when the AIMD controller didn't take the observer seat,
// so tests that use it run with AutoTune off; it observes the SERIAL
// apply path only (the ADR-0104 per-lane path reports to its lane
// controllers instead), so those tests also pin ApplyConcurrency to 1.
// Production code never sets it; the cost is a nil check per attempt.
var batchApplyObserverForTest ir.BatchObserver

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
			slog.DebugContext(
				ctx, "applier: batched apply enabled",
				slog.String("stream_id", streamID),
				slog.Int("apply_batch_size", s.ApplyBatchSize),
			)
			return batched.ApplyBatch(ctx, streamID, changes, s.ApplyBatchSize)
		}
		slog.WarnContext(
			ctx, "applier: --apply-batch-size requested but applier does not implement BatchedChangeApplier; falling back to per-change apply",
			slog.String("stream_id", streamID),
			slog.Int("apply_batch_size", s.ApplyBatchSize),
		)
	}
	return applier.Apply(ctx, streamID, changes)
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
		return cdcUnsupportedError(s.Source,
			fmt.Errorf("pipeline: Streamer.Source engine %q declares CDC=None", s.Source.Name()))
	}
	if err := migcore.ValidateTargetSchema(s.Target, s.TargetSchema); err != nil {
		return err
	}
	return validateEnabledPGExtensions(s.Source, s.Target, s.EnabledPGExtensions)
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
