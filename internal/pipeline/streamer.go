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

	"github.com/orware/sluice/internal/appliercontrol"
	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
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

	// PositionFromManifestStore is the [ir.BackupStore] the chain
	// terminal position is read from when the operator passes
	// `--position-from-manifest=<chain-url>`. The Streamer uses the
	// store's terminal manifest's [ir.Manifest.EndPosition] as the
	// resume position, bypassing the per-target [sluice_cdc_state]
	// row read (which a fresh-restored target wouldn't have). Phase
	// 3.3.B; mutually exclusive with the resume-from-control-table
	// path because they describe different position sources.
	//
	// nil means the field is not in use (the legacy resume path runs
	// unchanged).
	PositionFromManifestStore ir.BackupStore

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

	// leaseMgr is the ADR-0054 Shape A Phase 2 live-coordination
	// lease manager. Constructed by [engageShardCoordination] when
	// [CoordinateLiveDDL] is true, [InjectShardColumn] is engaged,
	// and the target applier implements
	// [ir.ShardConsolidationLeaseStore]. Nil otherwise (drained
	// model or non-Shape-A stream).
	//
	// Currently set by [engageShardCoordination] at applier-open
	// time; the SchemaSnapshot routing through this manager lands
	// in Phase 2c (probe-and-record + apply gate). The field is
	// exposed via [ShardConsolidationLeaseManager] for tests.
	leaseMgr *LeaseManager
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
		// Retry disabled: behaviour identical to v0.41.x.
		return s.runOnce(ctx)
	}
	return s.runWithRetry(ctx, attempts)
}

// classifyRetriable inspects err and returns (matchedWrapper, true)
// when err carries an [ir.RetriableError] with `Retriable() == true`.
// Returns (nil, false) otherwise. The matched wrapper is exposed so
// callers can read `RetryHint()` without redoing `errors.As`.
//
// Bug 57 fix (v0.52.2) load-bearing helper: this MUST be checked
// BEFORE any `errors.Is(err, context.DeadlineExceeded)` /
// `context.Canceled` short-circuit. The applier classifier wraps
// `context.DeadlineExceeded` (from `--apply-exec-timeout`) as a
// retriable error; that wrapping preserves the inner error via
// `Unwrap`, so `errors.Is(wrappedErr, context.DeadlineExceeded)`
// returns true and pre-v0.52.2 streamer logic mistook the wrapped
// timeout for a clean shutdown signal — exiting the retry loop with
// zero retry attempts. The fix is to test the wrapper class first
// and only treat unwrapped ctx-termination as clean shutdown.
func classifyRetriable(err error) (ir.RetriableError, bool) {
	var re ir.RetriableError
	if errors.As(err, &re) && re.Retriable() {
		return re, true
	}
	return nil, false
}

// runWithRetry wraps [runOnce] with the ADR-0038 retry loop. Opens
// a side-channel applier to read the persisted CDC position between
// attempts so the consecutive-failure counter can reset whenever an
// attempt made forward progress (a successful batch committed before
// the failure that triggered the retry).
//
// First iteration: respects [Streamer.ResetTargetData] as the caller
// supplied it. Subsequent iterations always warm-resume — the v0.41.0
// pre-CDC anchor write guarantees a persisted position exists by the
// time any retriable apply error fires, so warm-resume is always
// possible. ResetTargetData is cleared after the first iteration so
// a transient applier failure during the retry path does not
// re-trigger the destructive reset.
//
// On clean shutdown, terminal error, ctx cancellation, or budget
// exhaustion, returns the appropriate error (or nil on clean
// shutdown). Budget exhaustion wraps the final transient with a
// "retry budget exhausted" prefix so the operator sees both the
// counter outcome and the underlying cause.
func (s *Streamer) runWithRetry(ctx context.Context, attempts int) error {
	base := s.ApplyRetryBackoffBase
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	maxBackoff := s.ApplyRetryBackoffCap
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	// Side-channel applier for between-attempt position reads. The
	// inner runOnce owns its own applier with a fresh open/close
	// per iteration; this one stays alive across the whole retry
	// loop so progress is observable.
	posReader, err := s.Target.OpenChangeApplier(ctx, s.TargetDSN)
	if err != nil {
		// GitHub #17 papercut: when the open failure is a non-
		// retriable startup error (parse error, bad DSN, unreachable
		// target), the retry-policy WARN is noise — the inner
		// runOnce is about to fail with the same error and exit.
		// We skip the WARN for those shapes; for genuinely-
		// transient open failures (network blip mid-startup), the
		// WARN still fires and the single-attempt fall-through is
		// the right behaviour.
		if !isTransientOpenError(err) {
			slog.DebugContext(
				ctx, "applier: retry policy disabled (cannot open position reader); falling through to single-attempt run",
				slog.String("err", err.Error()),
			)
		} else {
			slog.WarnContext(
				ctx, "applier: retry policy disabled (cannot open position reader); falling through to single-attempt run",
				slog.String("err", err.Error()),
			)
		}
		return s.runOnce(ctx)
	}
	defer closeIf(posReader)

	streamID := s.resolveStreamID()
	var consecutive int

	for {
		beforePos, beforeFound, _ := posReader.ReadPosition(ctx, streamID)

		err := s.runOnce(ctx)
		if err == nil {
			return nil
		}
		// Bug 57 fix (v0.52.2): check the retriable wrapper BEFORE the
		// ctx-Cancel/DeadlineExceeded short-circuit. A wrapped
		// [ir.RetriableError] containing context.DeadlineExceeded (from
		// the applier's `--apply-exec-timeout` watchdog) traverses to
		// DeadlineExceeded via errors.Is's Unwrap walk. Pre-v0.52.2 the
		// check fired on that match and exited the streamer with zero
		// retry — the v0.52.0/v0.52.1 silent-stall fix was inert
		// because the timeout-driven retry never reached the retry loop.
		// The bare-ctx-termination case (operator Ctrl-C, sync stop
		// applyCtx cancel) still needs the early return below; it just
		// has to come AFTER the retriable check now.
		re, retriable := classifyRetriable(err)
		if !retriable {
			// Includes bare context.Canceled / context.DeadlineExceeded
			// (genuine ctx termination) and any non-retriable failure.
			// Returning err preserves the pre-v0.52.2 behaviour for
			// these shapes (callers branch on errors.Is themselves).
			return err
		}

		// Clear ResetTargetData after the first iteration so a
		// transient applier failure during retry does not trigger
		// another destructive reset of dest tables. The reset
		// happens at most once per Run.
		s.ResetTargetData = false

		afterPos, afterFound, _ := posReader.ReadPosition(ctx, streamID)
		progressed := beforeFound && afterFound && afterPos.Token != beforePos.Token
		if progressed {
			consecutive = 1
		} else {
			consecutive++
		}

		if consecutive >= attempts {
			return fmt.Errorf("pipeline: apply retry budget exhausted after %d consecutive failures at position %q: %w",
				consecutive, afterPos.Token, err)
		}

		backoff := computeRetryBackoff(consecutive, base, maxBackoff, re.RetryHint())
		slog.InfoContext(
			ctx, "applier: transient error; retrying",
			slog.String("stream_id", streamID),
			slog.Int("attempt", consecutive),
			slog.Int("max_attempts", attempts),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// warnIfApplyBatchSizeRisky emits a single WARN at startup when the
// operator's apply-batch-size + target combination is known to hit
// Vitess's 20s tx-killer under sustained load. GitHub #18 Phase 2:
// the validation-rig observations showed PS-MySQL cross-region
// failed at batch=100 (every batch hit tx-timeout, retry loop fired
// exhaustively), worked at 25-50.
//
// Triggers when target engine name is "planetscale" AND
// ApplyBatchSize > 50. The check is conservative — we don't try to
// detect cross-region from DSN host inspection (PS hostname formats
// vary; false negatives are better than the maintenance burden of a
// host-pattern table that grows stale). Operators on same-region
// PS-MySQL hit a benign WARN — better than missing the cross-region
// foot-gun entirely.
//
// Phase 3 (v0.46.0+) will replace this static rail with an AIMD
// controller that auto-discovers the right size per (source,
// target) pair from observed per-batch latency.
func warnIfApplyBatchSizeRisky(ctx context.Context, s *Streamer) {
	if s.Target == nil {
		return
	}
	maybeWarnApplyBatchSizeRisky(ctx, s.Target.Name(), s.ApplyBatchSize)
}

// maybeWarnApplyBatchSizeRisky is the testable core of
// [warnIfApplyBatchSizeRisky] — takes the target engine name and
// batch size directly so unit tests can exercise the policy without
// constructing a full Engine stub.
func maybeWarnApplyBatchSizeRisky(ctx context.Context, targetName string, batchSize int) {
	if targetName != "planetscale" {
		return
	}
	const riskyThreshold = 50
	if batchSize <= riskyThreshold {
		return
	}
	slog.WarnContext(
		ctx, "apply-batch-size > 50 against a planetscale target may exceed Vitess's 20s transaction-killer timeout under sustained CDC load",
		slog.Int("apply_batch_size", batchSize),
		slog.Int("safe_threshold", riskyThreshold),
		slog.String("hint", "if you see frequent 'mysql: applier: batch rollback on error' with 'code = Aborted ... for tx killer rollback', reduce --apply-batch-size to 25-50. See GitHub #18 for the auto-tuning controller planned for a future release."),
	)
}

// isTransientOpenError reports whether an applier-open error looks
// like a transient (network blip, brief DNS failure) vs a permanent
// startup failure (DSN parse error, bad credentials, unreachable
// hostname). Permanent failures don't benefit from the retry-policy
// WARN — the inner runOnce will surface the same error and exit;
// the WARN just makes the operator's first stderr line confusing
// (GitHub #17 papercut).
//
// Conservative classification: anything that looks like a parse or
// configuration error is permanent. Network-shape strings are
// transient. Unknown shapes default to transient so the existing
// behaviour (WARN + fall-through) is preserved.
func isTransientOpenError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "invalid DSN"),
		strings.Contains(msg, "DSN must include"),
		strings.Contains(msg, "parseDSN"),
		strings.Contains(msg, "Access denied"),
		strings.Contains(msg, "Unknown database"),
		strings.Contains(msg, "Authentication failed"):
		return false
	}
	return true
}

// computeRetryBackoff returns the per-attempt backoff per ADR-0038:
// exponential doubling from base, capped at max. When the engine's
// classifier provides a non-zero RetryHint, the hint overrides only
// when larger than the computed value (so engines cannot make retries
// fire sooner than the policy's exponential schedule). The hint
// itself is still capped at max so a buggy engine returning an
// unreasonable hint can't unbound the wait.
func computeRetryBackoff(attempt int, base, maxBackoff, hint time.Duration) time.Duration {
	b := base
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > maxBackoff {
			b = maxBackoff
			break
		}
	}
	if hint > b {
		b = hint
	}
	if b > maxBackoff {
		b = maxBackoff
	}
	return b
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
	if err := s.validate(); err != nil {
		return err
	}

	// Reset the per-attempt source-error handle (GitHub #19). Each
	// iteration opens a fresh CDC reader; carrying a stale handle
	// from a previous attempt would surface an already-handled error.
	s.sourceErrFn = nil

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

	// ---- 1. Open / wire the applier first ----
	applier, ownsApplier, err := s.openApplier(ctx)
	if err != nil {
		return err
	}
	if ownsApplier {
		defer closeIf(applier)
	}
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
		if aimdController != nil {
			metricsSrv.AttachAIMDController(aimdController)
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
	if !s.DryRun && !s.SchemaAlreadyApplied {
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

	// ---- 3. Look up the persisted position ----
	//
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
			return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: %w", err))
		}
		// Run Phase 3.3.C pre-flight checks before opening CDC. PG-only
		// today; MySQL has no operator-attention surface here. Refuses
		// when a check is fatal (slot lost / missing); warns otherwise
		// (or refuses on warning when StrictPreflight is set).
		if err := s.runPositionFromManifestPreflight(ctx, chainPos); err != nil {
			return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: position-from-manifest preflight: %w", err))
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
	// warmResumed tracks whether the apply loop is about to consume
	// from a CDC reader opened at the persisted position (vs. a fresh
	// post-snapshot reader). The ADR-0049 Chunk C cache prime keys on
	// this discriminator: only a true warm-resume primes from
	// storage; every cold-start path (initial, --reset-target-data
	// recovery, or warm-resume → ErrPositionInvalid fall-through) is
	// brand-new-stream-equivalent and skips the prime.
	var warmResumed bool
	switch {
	case s.ResetTargetData:
		changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID)
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
			changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID)
			// coldStart supersedes the warm resume — schema-history
			// stays brand-new from the applier's perspective (the
			// snapshot bulk-copy reset effective state).
			warmResumed = false
		}
	default:
		changes, stop, err = s.coldStart(streamCtx, lsnTracker, applier, streamID)
	}
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
	if primer, ok := applier.(schemaHistoryCachePrimer); ok {
		var primePos ir.Position
		if warmResumed {
			primePos = persisted
		}
		if err := primer.PrimeSchemaHistoryCache(applyCtx, streamID, primePos); err != nil {
			return wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: prime schema-history cache: %w", err))
		}
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

	filtered := filterChangesWithLiveAdd(applyCtx, changes, s.Filter, liveFilter)
	dispatchErr := s.dispatchApply(applyCtx, applier, streamID, filtered)
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

// maybeAttachAIMDController constructs an AIMD apply-batch-size
// controller (ADR-0052) and threads it onto the applier when:
//
//   - AutoTune is true (the v0.72.0 default; --no-auto-tune sets it
//     to false).
//   - ApplyBatchSize > 1 (the static-cap value; the controller
//     never exceeds this cap).
//   - The applier exposes both [ir.BatchSizeProviderSetter] and
//     [ir.BatchObserverSetter] (both shipping engines do after
//     ADR-0052).
//
// Returns the constructed controller for the metrics server to
// snapshot via AttachAIMDController, or nil when any of the above
// preconditions fails (the static --apply-batch-size cap remains
// the only flush trigger).
//
// Engines without the setters silently skip — the AIMD WARN is
// logged at DEBUG (not INFO) so a custom test stub doesn't drown
// out the operator-facing log surface; production engines all
// implement the setters by construction.
func (s *Streamer) maybeAttachAIMDController(ctx context.Context, applier ir.ChangeApplier, streamID string) *appliercontrol.Controller {
	if !s.AutoTune || s.ApplyBatchSize <= 1 {
		return nil
	}
	provSetter, hasProv := applier.(ir.BatchSizeProviderSetter)
	obsSetter, hasObs := applier.(ir.BatchObserverSetter)
	if !hasProv || !hasObs {
		slog.DebugContext(
			ctx, "applier: AIMD controller skipped — engine lacks BatchSizeProviderSetter or BatchObserverSetter",
			slog.String("stream_id", streamID),
		)
		return nil
	}

	target := s.ApplyTuneTargetLatency
	if target <= 0 {
		target = resolveAIMDTargetLatency(s.engineNameForAIMD())
	}

	cfg := appliercontrol.Config{
		StreamID:      streamID,
		EngineName:    s.engineNameForAIMD(),
		Floor:         1,
		Ceiling:       s.ApplyBatchSize,
		InitialSize:   s.ApplyBatchSize,
		TargetLatency: target,
	}
	ctrl, err := appliercontrol.New(cfg)
	if err != nil {
		slog.WarnContext(
			ctx, "applier: failed to construct AIMD controller; falling back to static apply-batch-size cap",
			slog.String("stream_id", streamID),
			slog.String("err", err.Error()),
		)
		return nil
	}

	provSetter.SetBatchSizeProvider(ctrl)
	obsSetter.SetBatchObserver(ctrl)
	slog.InfoContext(
		ctx, "applier: AIMD apply-batch-size controller engaged",
		slog.String("stream_id", streamID),
		slog.String("engine", cfg.EngineName),
		slog.Int("ceiling", cfg.Ceiling),
		slog.Duration("target_latency", cfg.TargetLatency),
	)
	return ctrl
}

// engineNameForAIMD returns the canonical engine name used for the
// AIMD controller's defaults lookup. Falls back to an empty string
// when the target engine is unset (test fixtures); resolveAIMDTargetLatency
// treats empty as "use the cross-engine default."
func (s *Streamer) engineNameForAIMD() string {
	if s.Target == nil {
		return ""
	}
	return s.Target.Name()
}

// resolveAIMDTargetLatency returns the engine-default p95 target
// latency per ADR-0052 DP-2:
//
//   - planetscale: 5s (Vitess 20s tx-killer + 4x headroom)
//   - mysql / postgres / any other named engine: 10s
//   - empty (unknown target — typically a test stub): 10s
func resolveAIMDTargetLatency(engineName string) time.Duration {
	if engineName == "planetscale" {
		return 5 * time.Second
	}
	return 10 * time.Second
}

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
	slog.InfoContext(
		ctx, "dry run: stream plan",
		slog.String("source", s.Source.Name()),
		slog.String("source_host", redactedHost(s.SourceDSN)),
		slog.String("target", s.Target.Name()),
		slog.String("target_host", redactedHost(s.TargetDSN)),
		slog.String("stream_id", streamID),
	)
	if found {
		slog.InfoContext(
			ctx, "dry run: warm resume from persisted position",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 80)),
		)
		return nil
	}
	slog.InfoContext(
		ctx, "dry run: cold start — would capture snapshot, bulk-copy, then start CDC",
		slog.String("stream_id", streamID),
	)

	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	if err := applyEnabledPGExtensions(ctx, sr, s.EnabledPGExtensions); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: dry-run: enable PG extensions on source: %w", err))
	}
	// ADR-0047 tier (b): live PG → PG sync may carry uncatalogued
	// extension types verbatim. Engine-name-only determination.
	applyVerbatimExtensionPassthrough(sr, verbatimLiveSameEnginePG(s.Source, s.Target))
	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (s.Filter already has engine defaults merged in Run).
	applyTableScope(sr, s.Filter)
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
	slog.InfoContext(
		ctx, "dry run: tables to bulk-copy and tail via CDC",
		slog.Int("tables", len(schema.Tables)),
	)
	for _, t := range schema.Tables {
		// secondary_indexes excludes the primary key (reported via
		// primary_key) — see migrate.go logPlan for the rationale.
		slog.InfoContext(
			ctx, "dry run: table",
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
		if err := s.engageShardCoordination(s.Applier); err != nil {
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
	if err := s.engageShardCoordination(a); err != nil {
		closeIf(a)
		return nil, false, wrapWithHint(PhaseConnect, err)
	}
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
//
// stop is a non-nil teardown closure the caller MUST defer. It closes
// the CDC reader, which terminates the engine's binlog/replication
// goroutine deterministically. Cancelling ctx alone only unwinds the
// pump (the channel closes when the pump exits); it does NOT stop the
// go-mysql BinlogSyncer goroutine spawned by StreamChanges. Without an
// explicit Close that goroutine runs to its reconnect-retry budget
// (~30s under a torn-down source) and keeps logging via slog.Default()
// — which, when a later test in the same binary swaps slog.Default()
// via captureSlog, surfaces a cross-test DATA RACE under `-race`. The
// closure is always non-nil (no-op on error paths, which clean up
// inline) so the caller can defer it unconditionally.
func (s *Streamer) warmResume(ctx context.Context, persisted ir.Position, lsnTracker any) (changes <-chan ir.Change, stop func(), err error) {
	stop = func() {}
	slog.InfoContext(
		ctx, "warm resume from persisted position",
		slog.String("position_token", persisted.Token),
	)
	cdc, err := openCDCReaderWithOptionalSlot(ctx, s.Source, s.SourceDSN, s.SlotName)
	if err != nil {
		return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: open cdc reader: %w", err))
	}
	if lsnTracker != nil {
		if attacher, ok := cdc.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}
	changes, err = cdc.StreamChanges(ctx, persisted)
	if err != nil {
		closeIf(cdc)
		return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	// Hand the caller a closure that closes the CDC reader. The reader's
	// Close cancels its pump AND closes the underlying syncer/slot, so
	// the engine-side streaming goroutine is joined deterministically
	// rather than left to run out its reconnect budget after ctx cancel.
	stop = func() { closeIf(cdc) }
	// GitHub issue #19: capture the reader's Err method so runOnce
	// can surface a pump error (transient `read: connection reset`
	// etc.) into the ADR-0038 retry loop after the changes channel
	// closes. Optional-interface probe; pre-v0.46 readers without
	// Err() pass through as nil and runOnce's check no-ops.
	if errer, ok := cdc.(interface{ Err() error }); ok {
		s.sourceErrFn = errer.Err
	}
	return changes, stop, nil
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
//
// stop mirrors [warmResume]'s teardown contract: a non-nil closure the
// caller MUST defer. It closes the snapshot stream, whose CloseFn
// closes the CDC reader and thus terminates the engine's binlog/
// replication goroutine deterministically. See warmResume's doc for
// why ctx cancellation alone leaks that goroutine into the next test
// (cross-test slog.Default() DATA RACE under `-race`).
func (s *Streamer) coldStart(ctx context.Context, lsnTracker any, applier ir.ChangeApplier, streamID string) (changes <-chan ir.Change, stop func(), err error) {
	stop = func() {}
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	if err := applyEnabledPGExtensions(ctx, sr, s.EnabledPGExtensions); err != nil {
		closeIf(sr)
		return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: enable PG extensions on source: %w", err))
	}
	// ADR-0047 tier (b): live PG → PG sync may carry uncatalogued
	// extension types verbatim. Engine-name-only determination.
	applyVerbatimExtensionPassthrough(sr, verbatimLiveSameEnginePG(s.Source, s.Target))
	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (s.Filter already has engine defaults merged in Run).
	applyTableScope(sr, s.Filter)
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "source schema has no tables; nothing to stream")
		return nil, stop, nil
	}

	// Prune by table filter before mappings + bulk-copy so the
	// excluded tables never reach the target schema-apply phase.
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return nil, stop, err
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
			return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: ensure publication scope: %w", err))
		}
	}

	// Apply per-column type overrides before the schema-write phase
	// sees the schema. Warm resume skips this step — by then the
	// target schema is already shaped from the cold-start run.
	schema, err = translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return nil, stop, fmt.Errorf("pipeline: apply mappings: %w", err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, s.ExpressionMappings)
	if err != nil {
		return nil, stop, fmt.Errorf("pipeline: apply expression overrides: %w", err)
	}
	// ADR-0048 Shape A discriminator-column injection. Runs after
	// ApplyMappings / ApplyExpressionOverrides and BEFORE the
	// target-side schema writer opens, so CREATE TABLE on the cold-
	// start branch sees the rewritten composite PK + the
	// SluiceInjected column. No-op when --inject-shard-column is
	// unset.
	if s.InjectShardColumn.Engaged() {
		schema, err = translate.InjectShardColumn(schema, s.InjectShardColumn.Name, ir.Varchar{Length: 64})
		if err != nil {
			return nil, stop, wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: inject shard column: %w", err))
		}
	}

	// Redaction-type pre-flight (Bug 60, v0.58.1): catch
	// mask:uuid on UUID-typed columns before the target schema
	// gets created. Runs after ApplyMappings so the operator's
	// `--type-override=col=text` workaround short-circuits the
	// refusal.
	if err := preflightRedactTypes(s.Redactor, schema); err != nil {
		return nil, stop, wrapWithHint(PhaseConnect, err)
	}

	stream, err := openSnapshotStreamWithOptionalSlot(ctx, s.Source, s.SourceDSN, s.SlotName)
	if err != nil {
		return nil, stop, wrapWithHint(PhaseSnapshot, fmt.Errorf("pipeline: open snapshot stream: %w", err))
	}
	// The snapshot+CDC handle stays alive past this function; the
	// returned stop closure (set on the success path below) closes it
	// so the engine-side streaming goroutine is joined deterministically
	// when Streamer.Run unwinds.
	slog.InfoContext(
		ctx, "cold start; snapshot captured",
		slog.String("position_token", stream.Position.Token),
	)

	sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
	if err != nil {
		_ = stream.Close()
		return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target schema writer: %w", err))
	}
	applyTargetSchema(sw, s.TargetSchema)
	if err := applyEnabledPGExtensions(ctx, sw, s.EnabledPGExtensions); err != nil {
		closeIf(sw)
		_ = stream.Close()
		return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: enable PG extensions on target: %w", err))
	}
	rw, err := s.Target.OpenRowWriter(ctx, s.TargetDSN)
	if err != nil {
		closeIf(sw)
		_ = stream.Close()
		return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target row writer: %w", err))
	}
	applyTargetSchema(rw, s.TargetSchema)
	applyMaxBufferBytes(rw, s.MaxBufferBytes)

	switch {
	case s.ResetTargetData:
		if err := resetTargetDataForStream(ctx, schema, rw, applier, streamID); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return nil, stop, err
		}
	case s.SchemaAlreadyApplied:
		// GitHub issue #17: operator promises every source table
		// exists on the target with a compatible schema, and that
		// the sluice_cdc_state control table has been pre-created.
		// Skip the preflight refusal — the operator's promise is
		// "everything I need is already there with no data"; we
		// can't validate that without round-trips that the operator
		// has explicitly opted out of. Bulk-copy runs into the
		// operator-prepared empty tables.
		slog.InfoContext(
			ctx, "schema-already-applied: skipping cold-start preflight + DDL phases (GitHub #17)",
			slog.String("stream_id", streamID),
		)
	default:
		// ADR-0048 Shape A populated-target preflight (DP-2). When
		// --inject-shard-column is set, this is the LOUD replacement
		// for `--force-cold-start`'s silent skip. No-op when the
		// flag is unset.
		if err := preflightShardConsolidation(ctx, schema, rw, s.InjectShardColumn.Name, s.InjectShardColumn.Value); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return nil, stop, err
		}
		// Cold-start pre-flight: refuse if any target table already
		// contains data. See preflight.go for the rationale (Bug 9).
		// Streamer's cold-start branch is the analogue of Migrator's
		// non-resume cold-start path; warm-resume doesn't run bulk-copy
		// and is therefore not gated by this check.
		// When --inject-shard-column is engaged, Shape-A's three-point
		// check above is the operator-opted-in replacement; the
		// classic cold-start preflight is suppressed in that case.
		if !s.InjectShardColumn.Engaged() {
			if err := preflightColdStart(ctx, schema, rw, s.ForceColdStart, preflightModeSync); err != nil {
				closeIf(rw)
				closeIf(sw)
				_ = stream.Close()
				return nil, stop, err
			}
		}
	}

	bulkOpts := bulkCopyOpts{
		SkipSchemaApply: s.SchemaAlreadyApplied,
		Redactor:        s.Redactor,
		Shard:           s.InjectShardColumn,
	}
	if err := runBulkCopyWithOpts(ctx, schema, stream.Rows, sw, rw, bulkOpts); err != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return nil, stop, err
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
		slog.WarnContext(
			ctx, "release snapshot rows failed; CDC will continue but the snapshot tx may stay open",
			slog.String("error", err.Error()),
		)
	}
	slog.InfoContext(ctx, "bulk-copy complete; entering CDC mode")

	// GitHub issue #15: persist the snapshot's anchor position on the
	// target BEFORE the first CDC batch lands. Without this write, the
	// cdc-state row stays absent through the entire window between
	// "entering CDC mode" and the first successful batch commit. A
	// crash, transient applier failure, or operator interrupt in that
	// window wedges the operator: warm-resume can't recover (no row),
	// and cold-start refuses (target tables already populated). The
	// only escape is `--reset-target-data` which re-runs the whole
	// bulk-copy.
	//
	// The position written here is the snapshot's anchor — the same
	// position StreamChanges resumes from on the next call. CDC from
	// this position is gapless and idempotent (ADR-0007, ADR-0010), so
	// a restart that reads this row and warm-resumes is correct: it
	// re-opens the slot at the same anchor and replays the same change
	// stream the failed run would have processed.
	//
	// Idempotent: this row is later overwritten by the first
	// applier.commitBatch — same row shape, monotonic position, same
	// (streamID, source_fingerprint, target_schema) tuple, so the
	// applier's writePositionTx absorbs the duplicate without conflict.
	if pw, ok := applier.(ir.PositionWriter); ok {
		if err := pw.WritePosition(ctx, streamID, stream.Position); err != nil {
			_ = stream.Close()
			return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: persist cold-start CDC anchor position: %w", err))
		}
		slog.DebugContext(
			ctx, "cold-start CDC anchor persisted",
			slog.String("stream_id", streamID),
			slog.String("position_token", stream.Position.Token),
		)
	} else {
		// Shipping engines all implement PositionWriter; an engine
		// that doesn't would have shipped with the issue #15 wedge,
		// but the fall-through preserves pre-fix behaviour rather than
		// hard-erroring.
		slog.WarnContext(
			ctx, "applier does not implement ir.PositionWriter; cold-start CDC anchor cannot be persisted — GitHub issue #15 wedge risk",
			slog.String("stream_id", streamID),
		)
	}

	if lsnTracker != nil {
		if attacher, ok := stream.Changes.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}

	changes, err = stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		_ = stream.Close()
		return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	// Close the snapshot stream when Streamer.Run unwinds. stream.Close
	// runs the engine CloseFn, which closes the CDC reader and joins the
	// engine-side streaming goroutine (go-mysql BinlogSyncer / PG slot
	// reader). Relying on ctx cancel alone left that goroutine running
	// to its reconnect budget after Run returned — a cross-test leak
	// that raced slog.Default() under `-race`.
	stop = func() { _ = stream.Close() }
	// GitHub issue #19: capture the reader's Err method so runOnce
	// can surface a pump error into the ADR-0038 retry loop after the
	// changes channel closes. See [warmResume] for the rationale —
	// same optional-interface probe pattern.
	if errer, ok := stream.Changes.(interface{ Err() error }); ok {
		s.sourceErrFn = errer.Err
	}
	// stream stays alive for the rest of Run; the returned stop closure
	// closes it when Run unwinds, joining the engine-side streaming
	// goroutine deterministically (no longer left to process-exit
	// reclaim — see the stop assignment above).
	return changes, stop, nil
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
	slog.DebugContext(
		ctx, "engine does not implement CDCReaderWithSlotOpener; --slot-name silently ignored",
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
	slog.DebugContext(
		ctx, "engine does not implement SnapshotStreamWithSlotOpener; --slot-name silently ignored",
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
