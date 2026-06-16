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

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
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
	// see [resolveBulkParallelism] / [resolveTableParallelism] /
	// [resolveBulkParallelMinRows] for the 0=auto rules.
	BulkParallelism     int
	TableParallelism    int
	BulkParallelMinRows int64
	BulkBatchSize       int
	RawCopyFormat       ir.RawCopyFormat

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
	// [pipeline.redactRow] before dispatch when this field is
	// non-nil and non-empty.
	Redactor *redact.Registry

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

	// runOnceFn is a test seam: when non-nil, [Run] / [runWithRetry]
	// invoke it in place of [runOnce]. Production always leaves it nil
	// (runOnceCall defaults to s.runOnce), so behaviour is identical;
	// the ADR-0093 reactive-cold-start tests inject a stub here to drive
	// the retry/recovery loop without booting a full pipeline.
	runOnceFn func(context.Context) error

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
	// GitHub #18 Phase 2: static safety-rail. Warn (don't refuse)
	// when an operator combination is known to hit Vitess's 20s
	// tx-killer under sustained load. The threshold matches the
	// validation-rig observations (PS-MySQL cross-region failed at
	// batch=100, worked at 25-50).
	warnIfApplyBatchSizeRisky(ctx, s)

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
	// Force a fresh, non-destructive cold-start on the next attempt: the
	// idempotent COPY writer absorbs the re-copied overlap (no target drop).
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
		"pipeline: the persisted source position is no longer valid (older than the source's retained binlogs / purged) and --no-auto-resnapshot is set, so sluice will not auto re-snapshot. Re-run with --restart-from-scratch for a non-destructive fresh cold-start (the idempotent copy absorbs the overlap), or --reset-target-data to drop and re-copy: %w", err,
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

	// ---- 1. Open / wire the applier first ----
	applier, ownsApplier, err := s.openApplier(ctx)
	if err != nil {
		return err
	}
	if ownsApplier {
		defer closeIf(applier)
	}
	// ADR-0054 Phase 2d: release shape-coordination resources when
	// engaged (SchemaWriter for per-shape DDL; the lease store /
	// prober live on the applier and are released by closeIf above).
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

	// Wrap the change channel: live-add-aware table filter → ADR-0054
	// Shape-A SchemaSnapshot intercept → ADR-0058 ADD COLUMN forward
	// intercept. The cold-start seed is consumed and cleared inside
	// the phase.
	filtered := s.phaseWireInterceptChain(applyCtx, changes, liveFilter, streamID)
	dispatchErr := s.dispatchApply(applyCtx, applier, streamID, filtered)
	// Settle the outcome: surface intercept-stored errors, classify
	// the dispatch error (Bug 57: retriable before ctx-termination),
	// surface a source CDC pump error (GitHub #19), and clear the stop
	// flag after a graceful drain. Uses the OUTER ctx — applyCtx may
	// already be cancelled here.
	return s.phaseSettleDispatch(ctx, applier, streamID, dispatchErr, &stopObserved)
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
		return fmt.Errorf("pipeline: Streamer.Source engine %q declares CDC=None", s.Source.Name())
	}
	if err := validateTargetSchema(s.Target, s.TargetSchema); err != nil {
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
