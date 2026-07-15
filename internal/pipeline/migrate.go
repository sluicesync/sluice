// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package pipeline implements the simple-mode orchestrator: a one-shot
// schema-and-data migration from a source database to a target. It is
// the layer that wires the IR's reader and writer interfaces into an
// end-to-end migration, given two engines.
//
// The simple-mode flow:
//
//  1. Read the source schema.
//  2. Translate (currently identity; the dedicated translator layer
//     lands in a future commit when cross-engine type rewriting needs
//     to be policy-driven rather than rejected with a clear error).
//  3. Apply schema phase 1: tables without indexes or constraints.
//  4. Bulk-copy data, table by table.
//  5. Apply schema phase 2: indexes.
//  6. Apply schema phase 3: foreign keys.
//
// The package does not depend on any specific engine package; engines
// are passed in as [ir.Engine] values, typically resolved by the CLI
// from the engines registry.
//
// Output goes through [log/slog]. The CLI configures the default
// handler (level, destination) in cmd/sluice/main.go; this package
// emits structured key/value lines via slog.Default(). Tests that
// want to assert on log output swap the default handler with a
// buffer-backed one for the duration of the test.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/redact"
)

// progressInterval is how often the bulk-copy progress ticker emits a
// line for a table that is actively receiving rows. Two seconds is the
// shortest cadence that still feels alive to a human watching tail -f
// without spamming aggregators on a many-table migration.
const progressInterval = 2 * time.Second

// Migrator runs a single simple-mode migration from Source/SourceDSN to
// Target/TargetDSN. Construct the value, then call Run with a context.
//
// Migrator does not retain state between Run calls — call it once per
// migration. Concurrent calls on the same value are not supported; if
// you want to run two migrations in parallel, instantiate two values.
type Migrator struct {
	// Source is the engine the source DSN belongs to (e.g. mysql,
	// postgres). Required.
	Source ir.Engine

	// Target is the engine the target DSN belongs to. May be the
	// same as Source for same-engine migrations. Required.
	Target ir.Engine

	// SourceDSN is the source-engine-native connection string.
	// Required.
	SourceDSN string

	// TargetDSN is the target-engine-native connection string.
	// Required.
	TargetDSN string

	// DryRun, when true, reads the source schema and prints what
	// would be applied without actually writing anything to the
	// target. Useful for verifying connectivity and previewing the
	// migration plan.
	DryRun bool

	// PlanSink, when non-nil AND DryRun is set, receives the built
	// [MigrationPlan] INSTEAD of the human slog rendering — the CLI's
	// `--dry-run --format json` hookup. On a multi-database fan-out
	// run the sink fires once per database; the caller merges. Ignored
	// when DryRun is false. nil (the zero value) keeps the slog plan.
	PlanSink func(*MigrationPlan)

	// Summary, when non-nil, collects the end-of-run per-table facts
	// the CLI's `--format json` result envelope renders. nil (the zero
	// value) disables the bookkeeping — every recording call is a
	// nil-safe no-op. See [migcore.RunSummary].
	Summary *migcore.RunSummary

	// Mappings is the per-column type-override list from sluice.yaml.
	// Applied after ReadSchema and before the schema-write phase, so
	// the named columns reach the target with the requested IR type.
	// nil/empty disables the override step entirely.
	Mappings []config.Mapping

	// ExpressionMappings is the per-column generated-expression
	// override list from sluice.yaml. Applied alongside Mappings:
	// the schema reader's emitted GeneratedExpr is replaced with the
	// operator's target-dialect text, and the dialect tag is cleared
	// so the writer-side translator skips the column entirely. The
	// escape hatch for cases the cross-dialect translator's hand-
	// coded rewrite table doesn't recognise (ADR-0016).
	ExpressionMappings []config.ExpressionMapping

	// InferTypes opts into the validated rich-type inference for
	// SQLite/D1 sources (`--infer-types`, ADR-0144): name-hinted
	// columns are promoted to richer Postgres types (boolean,
	// timestamptz/timestamp, jsonb, uuid) ONLY after the source engine
	// exhaustively validates that every non-NULL value conforms; the
	// promotion is injected as a validated override that rides the
	// existing override decode (no new value-conversion code). An
	// explicit Mappings entry always wins. The zero value (false) is
	// the safe default — conservative-and-lossless mapping, byte-
	// identical to before — so every programmatic caller is correct
	// without setting it. A non-SQLite/D1 source with InferTypes set is
	// refused loudly (inference is SQLite/D1-only).
	InferTypes bool

	// Filter selects which source tables participate in the
	// migration. Empty filter (zero value) keeps the previous
	// behaviour of migrating every table the source schema reader
	// returns. The filter is applied immediately after ReadSchema
	// and before any subsequent phase (schema apply, bulk copy,
	// indexes, constraints) so each phase consumes the pruned
	// schema implicitly.
	Filter migcore.TableFilter

	// ViewFilter selects which source views participate in the
	// migration's view-creation phase. Independent of [Filter] so
	// an operator can keep all tables but skip a subset of views.
	// Zero value keeps every view the schema reader returns.
	ViewFilter ViewFilter

	// SkipViews, when true, drops every view from the schema before
	// any phase runs — equivalent to setting ViewFilter to exclude
	// `*`, but with a clearer log line. Useful for cold-start
	// migrations that don't want to round-trip view definitions.
	SkipViews bool

	// Resume, when true, picks up where a previously-failed
	// migration with the same MigrationID left off. Reads the
	// per-target sluice_migrate_state row, branches on phase, and
	// skips work already recorded as complete. See
	// internal/pipeline/resume.go for the full design rationale.
	//
	// Default false preserves the v0.2.x semantics: a fresh run
	// against a target with no state row writes a new row and
	// runs every phase. A run against a target with an existing
	// non-complete state row errors out — operators must
	// explicitly opt into resume rather than silently overwriting.
	Resume bool

	// MigrationID is the stable identifier under which state is
	// persisted on the target. When empty, an ID is auto-derived
	// from source/target engine names plus DSN host info — same
	// shape Streamer.resolveStreamID uses for stream IDs. Operators
	// who need stable identity across DSN changes (DNS shifts, host
	// renames) should pass --migration-id explicitly.
	MigrationID string

	// ForceColdStart, when true, skips the cold-start pre-flight
	// check that refuses a fresh migration into a target with
	// pre-existing rows. The check protects against Bug 9 (cold-
	// start hangs after a killed-mid-copy run leaves partial dest
	// data behind); this flag is the explicit override for the
	// rare case of bulk-copying into a populated table.
	//
	// Ignored when Resume is true — resume *expects* dest tables
	// to have data, so the pre-flight doesn't run on that path
	// regardless of this flag.
	ForceColdStart bool

	// RawCopyFormat is the operator's --raw-copy-format intent for the
	// ADR-0078 same-engine raw-copy passthrough lane (item 3b(b)).
	// [ir.RawCopyText] (the default) is cross-major safe; [ir.RawCopyBinary]
	// is requested only on a matched-major same-engine pair (the
	// orchestrator probes both endpoints and downgrades to text loudly on
	// a mismatch). The "auto" CLI token maps to RawCopyBinary as the
	// *request* — negotiation, not the flag, decides the actual format —
	// while "text"/"binary" map to the respective constants. The lane
	// itself only engages when [rawCopyGate] proves there is no value
	// transform to skip; this field never affects the IR copy path.
	RawCopyFormat ir.RawCopyFormat

	// ResetTargetData, when true, clears the migrate-state row and
	// drops every source-schema table on the target before starting
	// a fresh cold-start migration. The destructive recovery path for
	// the v0.5.2 slot-missing fall-through and similar wedged-state
	// recovery scenarios. See ADR-0023.
	//
	// Mutually exclusive with Resume; the CLI rejects the combination
	// at parse time. The drop loop uses the optional [ir.TableDropper]
	// surface; engines that don't expose it surface a clear refusal.
	// The pre-flight refusal is skipped when this flag is set — the
	// drop loop runs to completion before any pre-flight probe could
	// fire.
	ResetTargetData bool

	// BulkBatchSize controls the per-batch row count for the
	// resume-mid-table checkpointing path (see ADR-0018). Each batch
	// commits with an updated cursor in
	// sluice_migrate_state.table_progress, so a crash mid-table
	// resumes without re-copying the prefix. Tables without a PK
	// fall back to truncate-and-redo regardless.
	//
	// Zero means use the default (5000). The value is only consulted
	// on the resume path — non-resume cold-start migrations use the
	// faster plain-INSERT / COPY-protocol path with no per-batch
	// checkpointing overhead.
	BulkBatchSize int

	// BulkParallelism is the number of parallel reader/writer pairs
	// per table during bulk copy (v0.5.0). Tables above
	// BulkParallelMinRows are split into BulkParallelism PK ranges
	// and copied concurrently. Tables below the threshold, tables
	// without an integer single-column PK, and the resume-from-v0.4.0
	// path all fall through to the v0.4.x single-reader behaviour.
	//
	// Zero means use the default — min(8, NumCPU). 1 disables
	// parallelism entirely (every table on the single-reader path).
	// See ADR-0019.
	BulkParallelism int

	// TableParallelism is the number of tables copied CONCURRENTLY in
	// the bulk-copy phase — the cross-table axis (roadmap item 3(a),
	// ADR-0076), composed with the within-table BulkParallelism axis.
	// The two multiply: at most TableParallelism × (effective
	// within-table parallelism) target connections are open at once, and
	// that product is bounded by the target's connection budget at the
	// single budget chokepoint (see [migcore.ResolveCopyParallelismBudget]).
	//
	// Zero means use the default — a small constant (4, matching
	// pgcopydb's --table-jobs default) bounded by the connection budget.
	// 1 disables cross-table concurrency (the pre-ADR-0076 serial-table
	// behaviour). Only the `migrate` path drives this; the sync
	// cold-start path stays serial by design (ADR-0076).
	TableParallelism int

	// BulkParallelMinRows is the row-count threshold below which a
	// table is copied with a single reader/writer pair regardless of
	// BulkParallelism. The per-chunk overhead (extra connections,
	// MIN/MAX query, per-chunk state writes) dominates on small
	// tables; the threshold avoids the overhead for them.
	//
	// Zero means use the default (80,000 as of v0.62.0; previously
	// 100,000). The new default absorbs the typical
	// information_schema row-count estimate undershoot on InnoDB —
	// 100k-actual tables register as ~95-99k via the catalog and
	// would have missed the prior 100k threshold by ~1%. Operators
	// wanting the pre-v0.62.0 behaviour pass
	// --bulk-parallel-min-rows=100000.
	BulkParallelMinRows int64

	// MaxBufferBytes is the soft upper bound on per-batch buffered
	// memory in the bulk-copy writer (and, when this Migrator is
	// later used as the cold-start half of a Streamer, the CDC
	// applier). The writer flushes when accumulated row-value bytes
	// reach the cap regardless of row count, so wide-row workloads
	// (TEXT / BYTEA / JSON columns at MB scale) don't blow out heap
	// when --bulk-batch-size's default of 5000 multiplies an unknown
	// row size into hundreds of MB.
	//
	// Zero means use the default (64 MiB). The cap is a soft target:
	// a single row larger than the cap still applies (the alternative
	// is to refuse it, which would silently break otherwise-valid
	// migrations). See ADR-0028.
	MaxBufferBytes int64

	// IndexBuildMem is the operator's `--index-build-mem` value (a
	// per-build maintenance_work_mem in bytes; 0 = auto), threaded to
	// the PG target SchemaWriter via [ir.IndexBuildTuner] before the
	// deferred CreateIndexes phase. 0 leaves the writer to autotune
	// maintenance_work_mem from a pg_settings probe — the dominant
	// index-build lever, on by default. Inert on engines without the
	// tuner (MySQL target). See
	// docs/dev/notes/index-build-phase-tuning.md.
	IndexBuildMem int64

	// IndexBuildParallelism is the operator's `--index-build-parallelism`
	// value (the number of concurrent index builds; 0 = auto), threaded
	// to the PG target SchemaWriter via [ir.IndexBuildTuner] before the
	// deferred CreateIndexes phase (Phase B). 0 lets the writer derive a
	// conservative worker count bounded by the target's spare connection
	// budget AND a memory budget (total ≈ N × per-build mem — the memory
	// × concurrency trap), so it can't OOM a small node. Inert on engines
	// without the tuner (MySQL target). See
	// docs/dev/notes/index-build-phase-tuning.md.
	IndexBuildParallelism int

	// Redactor is the operator-configured PII redaction policy.
	// PII Phase 1 (roadmap item 15a; GitHub issue #24). When non-nil
	// and non-empty, every row's per-column values are passed through
	// the registry's lookup → Strategy.Redact step before reaching
	// the per-engine prepareValue. nil or empty Registry is the
	// no-redactions hot path (zero-cost passthrough; see
	// [pipeline.migcore.RedactRow]).
	//
	// Phase 1's surface is internal-only: the CLI flag wiring is
	// deferred to a follow-up commit so the prep-doc's open design
	// questions (flag syntax, --require-redactions, key-source
	// shape) can be aligned before they're baked in. Test code and
	// direct Go API callers can populate this field today; CLI
	// operators get the feature once the flag lands.
	Redactor *redact.Registry

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
	TargetSchema string

	// EnabledPGExtensions is the operator's `--enable-pg-extension`
	// allowlist (ADR-0032). PG → PG only — the validate gate refuses
	// the field when either side isn't PG. Threaded through every
	// freshly-opened source SchemaReader / RowReader and target
	// SchemaWriter / RowWriter via [ir.ExtensionAware]; engines that
	// don't expose the surface skip cleanly. Empty preserves the
	// pre-v0.26.0 behaviour where extension-owned types surface as
	// loud refusals.
	EnabledPGExtensions []string

	// InjectShardColumn is the ADR-0048 Shape A discriminator-column
	// name + value (CLI: `--inject-shard-column NAME=VALUE`). When
	// non-empty, the orchestrator runs three additional steps:
	//
	//   1. [translate.InjectShardColumn] rewrites every PK-bearing
	//      table's IR — appending the column and making the PK
	//      composite (discriminator, …source PK). Tables without a
	//      base PK refuse loudly.
	//   2. The bulk-copy row stream goes through
	//      [shardStampRows], which stamps row[name]=value onto
	//      every row before it reaches the writer.
	//   3. [preflightShardConsolidation] runs against the target
	//      and refuses on any of: NULL discriminator on existing
	//      rows / VALUE already present / non-leading composite PK
	//      (DP-2 / DP-3 owner-confirmed shapes).
	//
	// Empty Name is the no-op default — every existing single-source
	// migrate path pays zero cost when Shape A isn't engaged.
	InjectShardColumn ShardColumnSpec

	// AllowCrossShardMerge opts out of the Bug 152 cross-shard-collision
	// preflight (CLI: `--allow-cross-shard-merge`). That preflight refuses
	// a multi-shard Vitess/PlanetScale source merging (via vtgate) into a
	// single non-discriminated target table whose PK/UNIQUE could collide
	// across shards — a silent-overwrite hazard. Set this only when the
	// key is globally unique across shards (Vitess sequences / UUID keys)
	// so no overwrite can occur; the safe default is false (guard active).
	// Mutually exclusive in effect with InjectShardColumn, which solves
	// the same hazard structurally.
	AllowCrossShardMerge bool

	// MaxTargetConnections is the operator's --max-target-connections
	// explicit ceiling on the bulk-copy connection pool (connection-
	// resilience item 4). Zero (the default) means "auto": the
	// connection-budget preflight probes the target's slot budget and
	// caps parallelism to fit. When set, it's an explicit upper bound the
	// auto-cap further bounds — it never raises the resolved
	// --bulk-parallelism.
	//
	// Target-engine-specific: engines without a connection-slot model
	// (today: MySQL) don't implement [ir.TargetConnectionBudgetProber],
	// so the budget preflight is a no-op and this ceiling is inert.
	MaxTargetConnections int

	// ReapStaleBackends opts the operator into terminating sluice's own
	// orphaned backends on the target during the cold-start preflight
	// (connection-resilience Phase 2, item 2). Detection runs always and
	// reports loudly; this flag is what authorises the destructive
	// pg_terminate_backend on each detected orphan. Default off —
	// detect-and-report is the safe baseline because a legitimately-
	// running concurrent sluice process on the same target is a real
	// possibility (the loud-failure / contain-Postgres-complexity tenets).
	//
	// Target-engine-specific: engines without a backend model (today:
	// MySQL) don't implement [ir.TargetStaleBackendReaper], so the step
	// is a no-op and this flag is inert.
	ReapStaleBackends bool

	// DatabaseFilter selects which source databases participate in a
	// multi-database fan-out migration (ADR-0074). Non-empty
	// Include/Exclude — or AllDatabases — switches the Migrator into
	// multi-database mode: the source DSN becomes a *server* connection
	// (its database component is optional), the orchestrator enumerates
	// databases via the source engine's [ir.DatabaseLister], and runs a
	// per-database snapshot loop that routes each source database to a
	// same-named target namespace (PG schema / MySQL database).
	//
	// Empty (the zero value) with AllDatabases=false is the default,
	// single-database mode — behaviour is byte-identical to a Migrator
	// that never had this field, and the source DSN must name a database
	// exactly as before.
	//
	// Mutually exclusive with TargetSchema: in multi-database mode the
	// per-database target namespace is the source database name, so an
	// operator-supplied single --target-schema is contradictory.
	DatabaseFilter DatabaseFilter

	// AllDatabases is the `--all-databases` convenience: migrate every
	// non-system database the source server exposes. Switches the
	// Migrator into multi-database mode (see DatabaseFilter) with an
	// empty include/exclude (every enumerated database passes). Mutually
	// exclusive with a non-empty DatabaseFilter.
	AllDatabases bool

	// NamespaceMap is the optional per-namespace source → target rename
	// for a multi-namespace fan-out (ADR-0142, --map-database/--map-schema).
	// The zero value is the identity map — every source namespace routes to
	// a same-named target namespace, byte-identical to pre-ADR-0142 fan-out.
	// A non-empty map ALSO engages multi-database mode (the map keys are the
	// selection when no --all-*/--include-*/--exclude-* flag is given), and
	// only changes the TARGET namespace identifier; reads, Table.Schema
	// stamping, the FK carve-out, and the per-source MigrationID stay on the
	// SOURCE name.
	NamespaceMap NamespaceRenameMap

	// multiDBDeferFKs is set by the multi-database orchestrator on each
	// per-database clone so the inner single-database run SKIPS the
	// foreign-key constraint phase. Cross-database FKs reference tables
	// in OTHER selected databases that may not exist on the target yet
	// (databases are migrated one at a time); deferring every FK to a
	// final pass after all databases' tables exist is the only correct
	// ordering. Same-database FKs are deferred too — harmless, and it
	// keeps the two-pass model uniform. Never set in single-database
	// mode (the FK phase runs inline as always). Unexported: an
	// orchestrator-internal mechanism, not an operator knob.
	multiDBDeferFKs bool

	// AllowDegradedFKs opts the operator into the pgcopydb-PR-#27-style
	// "tolerate dirty FK source" behaviour: when [SchemaWriter.CreateConstraints]
	// hits SQLSTATE 23503 on a validating `ADD CONSTRAINT`, retry as
	// `NOT VALID` and surface the degraded constraint in the pipeline's
	// operator-facing report. Default off — loud-failure-on-dirty-source
	// stays baseline; the operator opts in explicitly when migrating
	// from a known-dirty source.
	//
	// PG-target only by design. Other engines (today: MySQL) do not
	// implement the [ir.DegradedFKAllower] optional interface — the
	// orchestrator type-asserts at writer-open time and refuses loudly
	// if the flag is set against an unsupported target. See
	// `docs/dev/notes/pgcopydb-planetscale-fork-review.md`.
	AllowDegradedFKs bool

	// SkipForeignKeys opts into creating NO foreign-key constraints on the
	// target (`--skip-foreign-keys`) while keeping each skipped FK's
	// referencing column tuple indexed: for every FK, if no existing target
	// index already covers its referencing columns as a left-prefix, a
	// deterministic non-unique backing index is synthesized on that tuple.
	// The transform runs once on the finalized schema, before any DDL phase
	// (see applySkipForeignKeys), so the FK phase (CreateConstraints) creates
	// nothing and the synthesized indexes ride the ordinary idempotent
	// CreateIndexes phase. Engine-agnostic; the primary use case is targets
	// with limited FK support (Vitess/PlanetScale sharded keyspaces) or when
	// FKs are managed out-of-band. On a MySQL target it also preserves the
	// backing index MySQL would otherwise create only alongside the FK.
	//
	// Mutually exclusive with AllowDegradedFKs (opposite intents: one skips
	// FK creation entirely, the other creates FKs and tolerates dirty rows) —
	// refused loudly in validate. Default off — byte-identical to before.
	SkipForeignKeys bool

	// SkipORMTables, when true, drops recognized ORM/framework
	// migration-bookkeeping tables (flyway_schema_history,
	// _prisma_migrations, schema_migrations, …) from the source schema
	// after the table filter, announcing each skip loudly (ADR-0143).
	// Carrying a source's migration-history table to the target is
	// almost always wrong — it records migrations that ran against the
	// SOURCE, so the ORM on the target concludes they already ran.
	//
	// ★ Zero-value-safe (the v0.99.51 trap): the zero value (false) is
	// DO-NOT-skip, the conservative default every programmatic / broker /
	// test caller gets — they must never suddenly start dropping tables.
	// ONLY the CLI defaults this on (flipped off by --include-orm-tables),
	// so loud-skip-by-default is a CLI policy, not a library behaviour
	// change. A table named explicitly via --include-table is never
	// skipped; a generic-name collision (name matches but column shape
	// doesn't) is kept with a loud warning rather than silently dropped.
	SkipORMTables bool

	// UpfrontIndexes, when true, creates every secondary index BEFORE the
	// bulk copy — right after CreateTablesWithoutConstraints — instead of the
	// default deferred post-copy index phase, so the bulk INSERTs maintain the
	// indexes as they load (CLI: --upfront-indexes). Indexes-only: identity-
	// sequence sync and foreign-key constraints keep their positions (FKs are
	// still created last), so FK ordering is preserved — bare tables → indexes
	// → copy → FKs.
	//
	// The motivating case is a large PlanetScale-MySQL target, where a deferred
	// `ALTER … ADD INDEX` on a multi-GB table can exceed PlanetScale's max-
	// statement-execution-time limit (errno 3024) and die AFTER an otherwise-
	// correct copy, leaving the indexes uncreated. Building them upfront avoids
	// the post-hoc ALTER entirely, at the cost of a slower load (the INSERTs
	// maintain the index). Engine-neutral: it reuses the same
	// SchemaWriter.CreateIndexes the deferred phase calls, so it works for both
	// MySQL and Postgres targets.
	//
	// Zero-value-safe: the zero value (false) is the deferred post-copy build —
	// today's behaviour, byte-identical. Unlike the v0.99.51 trap, deferred is
	// GENUINELY the default here (the common case), so the on-behaviour
	// (upfront) is correctly the opt-in; no inverted NoUpfront/Suppress name is
	// needed.
	UpfrontIndexes bool

	// IndexBuildFallback is the optional out-of-band index-build channel
	// (ADR-0148: the PlanetScale deploy-request fallback for the errno-3024
	// statement-time wall / errno-1105 safe-migrations direct-DDL block),
	// threaded onto the target SchemaWriter via the optional
	// [ir.IndexBuildFallbackSetter] surface right after it opens. The
	// orchestrator stays engine-neutral: the value is composed by the CLI
	// (which knows the target is PlanetScale and holds the control-plane
	// credentials) and passed through opaquely; engines without the setter
	// skip cleanly.
	//
	// Zero-value-safe: nil (every programmatic / broker / test caller)
	// leaves the direct index build byte-identical to before the fallback
	// existed.
	IndexBuildFallback ir.IndexBuildFallback

	// Progress is the optional TTY-aware presentation sink (ADR-0155).
	// When nil (the zero value — every test, library embedder, broker /
	// fleet path, and the sync cold-start), the orchestrator falls back
	// to the structured-log sink via [progress.FromContext], so the
	// emitted slog records are byte-for-byte identical to before. The CLI
	// sets it to a [progress.TTYSink] only for an interactive terminal
	// run (stdout is a TTY, --log-format=text, no --no-progress); every
	// other invocation keeps the [progress.LogSink] behaviour. See
	// [Migrator.Run], which injects the sink into the run context so the
	// deep bulk-copy ticker can reach it without a threaded parameter.
	Progress progress.Sink

	// AnalyzeAfter, when true, runs a target-side per-table statistics
	// refresh (PG ANALYZE / MySQL ANALYZE TABLE / SQLite ANALYZE, via the
	// optional [ir.TableAnalyzer] surface) AFTER constraints and views
	// complete (CLI: --analyze-after). A freshly bulk-loaded table has
	// stale/empty planner statistics, so the first post-cutover queries
	// plan badly until a background ANALYZE catches up; pgcopydb runs a
	// per-table VACUUM ANALYZE by default for the same reason.
	//
	// The phase is ADVISORY and post-success: the migration's data and
	// DDL are already durably complete when it runs, so a per-table
	// analyze failure WARNs loudly (naming the table) and never fails the
	// run, and no resume state is recorded for it. Default off — the
	// pre-existing behaviour (leave statistics to the target's
	// autovacuum/background machinery) is unchanged unless opted in.
	AnalyzeAfter bool
}

// ShardColumnSpec carries the ADR-0048 Shape A discriminator
// column the operator opts into via
// `--inject-shard-column NAME=VALUE`. Both fields must be
// non-empty for the orchestrator to engage Shape A; an empty
// Name is the off-default. The pair lives on
// [Migrator]/[Streamer]/[Previewer]/[Differ] so every entry
// point can route it through the IR + value paths consistently.
type ShardColumnSpec struct {
	// Name is the discriminator column's identifier (e.g.
	// `source_shard_id`). Required when Shape A is engaged.
	Name string

	// Value is the per-shard literal the orchestrator stamps onto
	// every row (e.g. `us-east-1` / `1`). Today the CLI parser
	// hands a string; the field is `any` so future expansion can
	// thread an integer or UUID without changing this surface.
	Value any
}

// Engaged reports whether the operator has opted into Shape A by
// supplying both Name and Value. Used as the single
// branch-condition by every entry point so the off-default path
// is identical across migrate / sync / preview / diff.
func (s ShardColumnSpec) Engaged() bool {
	return s.Name != "" && s.Value != nil
}

// Run executes the migration. Returns nil on success or a wrapped
// error pointing at the phase that failed.
//
// Run honours ctx cancellation: if ctx is cancelled mid-migration,
// the underlying database operations return ctx.Err() and Run
// surfaces it. Partially-applied state on the target is the user's
// responsibility — for v1 there is no automatic rollback, since DDL
// in MySQL is implicit-commit and rollback after partial application
// is engine-dependent.
func (m *Migrator) Run(ctx context.Context) error {
	if err := m.validate(); err != nil {
		return err
	}

	// ADR-0155: attach the presentation sink to the run context once, so
	// every phase call-site (and the deep bulk-copy ticker) reaches it via
	// [progress.FromContext] without a threaded parameter. nil Progress is
	// ignored by NewContext, so FromContext falls back to the byte-identical
	// structured-log sink — the pre-ADR-0155 behaviour every un-wired caller
	// keeps.
	ctx = progress.NewContext(ctx, m.Progress)

	// Driver/host mismatch pre-flight — runs before any reader/writer is
	// opened (it only needs the engines + DSNs). Refuses e.g. the vanilla
	// mysql driver pointed at a PlanetScale host, naming the
	// --source-driver / --target-driver flag to fix. No-op for engines
	// without ir.DSNValidator.
	if err := preflightDSNValidation(m.Source, m.SourceDSN, m.Target, m.TargetDSN); err != nil {
		return err
	}

	// Managed-host advisories (items 69a/70a): WARN-level sibling of the
	// refusal above — e.g. a PG source host matching a known connection-
	// pooler pattern. cdc=false: migrate never returns to the source's
	// change stream, so CDC-retention advisories stay quiet.
	migcore.WarnSourceHostAdvisories(ctx, m.Source, m.SourceDSN, false)

	// Multi-database fan-out (ADR-0074). When any database-scope flag is
	// set, resolve the database set and run a per-database snapshot loop;
	// each iteration re-opens a single-database reader/writer (a DSN
	// clone with DBName set) and reuses 100% of the single-database path
	// below. Single-database mode falls straight through, byte-identical.
	if m.multiDatabaseMode() {
		return m.runMultiDatabase(ctx)
	}

	return m.runSingleDatabase(ctx, nil)
}

// runSingleDatabase is the original single-database orchestrator body.
// scope is nil for a genuine single-database run; in the multi-database
// fan-out it carries the per-database namespace + in-scope predicate the
// source reader needs (Table.Schema stamping + the FK carve-out).
func (m *Migrator) runSingleDatabase(ctx context.Context, scope *multiDBScope) error {
	// ---- 1 → 1.45. Read + gate the source schema ----
	// Engine-default exclusions, schema read, table/view filters, and
	// the source-side preflights. sr stays open for the preflights and
	// is closed when this function returns — the defer lives HERE so
	// the reader's lifetime spans the whole run, exactly as before the
	// phase split. A nil schema with a nil error is the empty-source
	// case (already logged inside the phase).
	sr, schema, err := m.phaseReadSourceSchema(ctx, scope)
	if sr != nil {
		defer migcore.CloseIf(sr)
	}
	if err != nil {
		return err
	}
	if schema == nil {
		return nil
	}

	// ---- 1.5 → 1.69. Translate + cross-engine gate the schema ----
	// Type/expression overrides, Shape-A injection, the ADR-0078
	// raw-copy lane gate (rawCopyOK threads into the copy deps below),
	// the redaction-type preflight, and the cross-engine refusals +
	// loud-notice scans — all before DryRun and any schema apply.
	schema, rawCopyOK, err := m.phaseTranslateAndGateSchema(ctx, sr, schema)
	if err != nil {
		return err
	}

	if m.DryRun {
		plan := m.buildDryRunPlan(ctx, schema)
		if m.PlanSink != nil {
			m.PlanSink(plan)
			return nil
		}
		m.logPlan(ctx, plan)
		return nil
	}

	// ---- 1.75. Open the migration-state store (if the target engine
	// supports it) and resolve the migration_id. ----
	rc, state, exitClean, err := m.openResumeContext(ctx, m.ResetTargetData)
	if err != nil {
		return err
	}
	if exitClean {
		// already-complete with --resume → log handled in
		// loadOrInitState; nothing else to do.
		return nil
	}
	if rc.enabled {
		defer migcore.CloseIf(rc.store)
	}
	resuming := m.Resume && rc.enabled

	// ---- 2. Open target writers ----
	sw, err := m.Target.OpenSchemaWriter(ctx, m.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open target schema writer: %w", err)))
	}
	migcore.ApplyTargetSchema(sw, m.TargetSchema)
	applyIndexBuildMem(sw, m.IndexBuildMem)
	applyIndexBuildParallelism(sw, m.IndexBuildParallelism)
	applyIndexBuildFallback(sw, m.IndexBuildFallback)
	if err := applyEnabledPGExtensions(ctx, sw, m.EnabledPGExtensions); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: enable PG extensions on target: %w", err)))
	}
	if m.AllowDegradedFKs {
		a, ok := sw.(ir.DegradedFKAllower)
		if !ok {
			return migcore.WrapWithHint(migcore.PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
				errors.New("pipeline: --allow-degraded-fks is set but the target engine doesn't support degraded FKs "+
					"(PG-target only by design; MySQL's nearest analogue, FOREIGN_KEY_CHECKS=0, is a different contract — "+
					"clean the source FK violations before migrating to a MySQL target)")))
		}
		a.EnableDegradedFKs()
	}
	defer migcore.CloseIf(sw)

	rw, err := m.Target.OpenRowWriter(ctx, m.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open target row writer: %w", err)))
	}
	migcore.ApplyTargetSchema(rw, m.TargetSchema)
	migcore.ApplyMaxBufferBytes(rw, m.MaxBufferBytes)
	defer migcore.CloseIf(rw)

	// ---- 2.5. Target-side preflights (RLS, stale backends) ----
	// Stale-backend detection MUST precede both the cold-start
	// preflight and the connection-budget probe (Bug 123) — the phase
	// carries the full ordering rationale.
	if err := m.phasePreflightTarget(ctx, rc, state, schema, rw); err != nil {
		return err
	}

	// ---- 3-6. Schema apply (phase 1) → bulk copy → indexes → constraints.
	// Shared exported snapshot (perf research delta 1): when the SOURCE
	// engine can export a plain-SQL shareable snapshot AND mint importer
	// readers pinned to it (PG), the primary reader IS the exporting
	// transaction's pinned reader and every additional table/chunk reader
	// imports the same snapshot — ONE consistent MVCC view across both
	// copy axes, the same ADR-0079 machinery the sync cold-start pins
	// with. The snapshot is released at copy-phase end (see
	// runBulkCopyTablePool), never held through the index/constraint
	// phases. Engines without the surfaces (MySQL, SQLite) and export
	// failures (hot-standby source) keep the independent per-connection
	// readers — the documented ADR-0019 v1 window — unchanged.
	var rr ir.RowReader
	sharedSnap := m.openSharedSourceSnapshot(ctx)
	if sharedSnap != nil {
		defer sharedSnap.close()
		rr = sharedSnap.snap.Rows
	} else {
		openedRR, err := m.Source.OpenRowReader(ctx, m.SourceDSN)
		if err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
				fmt.Errorf("pipeline: open source row reader: %w", err)))
		}
		defer migcore.CloseIf(openedRR)
		rr = openedRR
	}

	// Resume / reset / populated-target gate: --resume rides
	// TableProgress; --reset-target-data clears the target first;
	// otherwise the Shape-A / Bug 9 preflights refuse a populated
	// target.
	if err := m.phaseGateColdStart(ctx, rc, state, schema, rw, resuming); err != nil {
		return err
	}

	// Resolve the copy parallelism from the target's measured
	// connection budget at the single chokepoint: budget preflight
	// (item 4) → ADR-0077 index-build reservation → ADR-0076 table ×
	// within-table split.
	tableParallelism, withinParallelism, err := m.phaseResolveCopyParallelism(ctx, rc, state, sw)
	if err != nil {
		return err
	}

	// Negotiate the raw-copy wire format (ADR-0078) and assemble the
	// parallel bulk-copy dependency set (including the shared-snapshot
	// reader factory + copy-end release hook when engaged).
	parallelDeps := m.phaseBuildCopyDeps(ctx, schema, rr, rw, rawCopyOK, withinParallelism, sharedSnap)

	// Attach the run-scoped rows-copied accumulator (ADR-0155): each
	// bulk-copy ticker folds its final count in, and the summary below
	// reports the sum. Best-effort total (rows handed to the writer), for
	// the presentation layer only — no bearing on correctness or the
	// LogSink line.
	ctx, rowTotal := withRunRowTotal(ctx)

	if err := runBulkCopyPhases(ctx, rc, &state, schema, rr, sw, rw, resuming, m.BulkBatchSize, parallelDeps, tableParallelism, m.Redactor, m.InjectShardColumn, m.UpfrontIndexes, m.AnalyzeAfter); err != nil {
		return err
	}

	// Envelope bookkeeping: mirror the "migration complete" table set
	// into the optional Summary (nil-safe no-op otherwise). The per-table
	// row count stays out of the JSON envelope by design; the aggregate
	// migration-wide total is reported to the presentation sink below.
	for _, t := range schema.Tables {
		m.Summary.RecordTable(t.Schema, t.Name)
	}
	markComplete(ctx, rc, state)
	// Tables carries the LogSink's byte-identical "migration complete
	// tables=N" line; Fields drive the TTY summary panel (ADR-0155 phase
	// 2 — the panel-only Rows row appears only when a total is known).
	migFields := []progress.Field{{Label: "Tables", Value: progress.HumanCount(int64(len(schema.Tables)))}}
	if r := rowTotal.Load(); r > 0 {
		migFields = append(migFields, progress.Field{Label: "Rows", Value: progress.HumanCount(r)})
	}
	progress.FromContext(ctx).Summary(progress.Result{
		Tables: len(schema.Tables),
		Fields: migFields,
	})
	return nil
}

// openResumeContext resolves the migration_id, opens the per-target
// state store (when the engine supports it), and decides whether to
// short-circuit (already-complete + --resume) or proceed. The
// returned (resumeContext, state, exitClean) tuple carries every
// piece of state the rest of Run threads through phase boundaries.
//
// The only error path here is the "row found, refuse to overwrite"
// branch and the "store open / table ensure" failures; both surface
// before any target writers open so the operator gets a clean
// refusal rather than a half-open connection set.
func (m *Migrator) openResumeContext(ctx context.Context, resetting bool) (resumeContext, ir.MigrationState, bool, error) {
	store, err := openMigrationStateStore(ctx, m.Target, m.TargetDSN, m.TargetSchema)
	if err != nil {
		return resumeContext{}, ir.MigrationState{}, false, migcore.WrapWithHint(migcore.PhaseConnect, err)
	}
	rc := resumeContext{
		store:       store,
		migrationID: m.resolveMigrationID(),
		enabled:     store != nil,
	}
	state, exitClean, err := loadOrInitState(ctx, rc, m.Resume, resetting)
	if err != nil {
		if rc.enabled {
			migcore.CloseIf(rc.store)
		}
		return resumeContext{}, ir.MigrationState{}, false, err
	}
	return rc, state, exitClean, nil
}

// resolveMigrationID returns the operator-supplied MigrationID when
// non-empty, else an auto-derived value. Mirrors
// Streamer.resolveStreamID's contract; see [deriveMigrationID] for
// the hashing rationale.
func (m *Migrator) resolveMigrationID() string {
	if m.MigrationID != "" {
		return m.MigrationID
	}
	return deriveMigrationID(m.Source.Name(), m.SourceDSN, m.Target.Name(), m.TargetDSN, m.TargetSchema)
}

// runBulkCopy applies the shared phases that follow target-writer
// open with no resume awareness: schema phase 1 (tables without
// constraints) → bulk-copy of every table → identity-sequence sync →
// schema phase 2 (indexes) → schema phase 3 (foreign keys) → schema
// phase 4 (views). Used by the Streamer's cold-start path (which
// pre-dates the resume feature). [Migrator] uses [runBulkCopyPhases]
// instead so the per-phase boundaries can persist resume state.
//
// Phase 3.5 (identity-sequence sync) runs between bulk-copy and
// indexes so the next user-initiated INSERT against an identity
// column doesn't collide with bulk-copied IDs. Engines whose
// identity mechanism auto-bumps on direct INSERT (MySQL InnoDB)
// implement this as a no-op; the call costs nothing on those
// engines.
//
// Errors from any phase are wrapped with the phase name so the
// caller can pinpoint which step failed without parsing strings.
// bulkCopyOpts groups the optional behaviours that vary across
// runBulkCopy call sites without forcing a parameter explosion on
// the common path. Zero value is the historical behaviour.
// reportDegradedFKs surfaces any FK constraints the most-recent
// [ir.SchemaWriter.CreateConstraints] attached as NOT VALID (the
// operator opted into `--allow-degraded-fks` and the source had
// orphan rows). Each gets a WARN log line with the actionable Hint;
// a single summary line follows so the count is hard to miss in CLI
// output. No-op when the writer doesn't implement
// [ir.DegradedFKReporter] or when nothing was degraded.
func reportDegradedFKs(ctx context.Context, sw ir.SchemaWriter) {
	r, ok := sw.(ir.DegradedFKReporter)
	if !ok {
		return
	}
	fks := r.DegradedFKs()
	if len(fks) == 0 {
		return
	}
	sink := progress.FromContext(ctx)
	for _, fk := range fks {
		sink.Warn(
			"constraint attached degraded (NOT VALID)",
			slog.String("schema", fk.Schema),
			slog.String("table", fk.Table),
			slog.String("constraint", fk.ConstraintName),
			slog.String("referenced_table", fk.ReferencedTable),
			slog.String("reason", fk.Reason),
			slog.String("hint", fk.Hint),
		)
	}
	sink.Warn("constraints phase: degraded FKs",
		slog.Int("count", len(fks)),
		slog.String("action_required",
			"run `ALTER TABLE ... VALIDATE CONSTRAINT <name>` for each after fixing the orphan rows on the child tables"))
}

type bulkCopyOpts struct {
	// SkipSchemaApply, when true, suppresses every DDL phase
	// (CreateTablesWithoutConstraints, SyncIdentitySequences,
	// CreateIndexes, CreateConstraints, CreateViews). Only the
	// per-table data sweep (copyTable) runs. Used by the
	// `--schema-already-applied` operator flag (GitHub #17) on
	// targets that block direct DDL (PlanetScale Safe Migrations,
	// Atlas/Liquibase-managed schemas). Operators promise the
	// target catalog already matches the source's; sluice does not
	// validate.
	SkipSchemaApply bool

	// Redactor is the operator-configured PII redaction policy
	// (Phase 1, roadmap item 15a). nil/empty Registry is the
	// no-redactions hot path; see [migcore.RedactRow] and [redactRows] for
	// the wrap semantics.
	Redactor *redact.Registry

	// Shard is the ADR-0048 Shape A discriminator spec. Threaded
	// into copyTable so the per-row stamp lands on the bulk-copy
	// channel before the writer sees it. Zero-value (empty Name)
	// is the no-op default — single-source streams pay nothing.
	Shard ShardColumnSpec

	// CopyFanoutDegree is the WRITE-side parallel fan-out degree for
	// the idempotent VStream/CDC snapshot cold-start copy (ADR-0097).
	// Resolved through [resolveCopyFanoutDegree]: the Go zero value (0)
	// maps to the safe default degree, 1 is serial, >1 fans the single
	// incoming row stream out to N PK-hash-partitioned writer workers.
	// Only consulted on the idempotent cold-start path (the PS-MySQL
	// gap) and only when the writer implements
	// [ir.ParallelIdempotentCopyWriter] and the table has a usable PK;
	// otherwise the serial path runs. Zero-value-safe by construction
	// (the v0.99.51 trap): no value produces "zero workers".
	CopyFanoutDegree int

	// NoIntraTableStealing opts OUT of intra-table PK-range work-stealing on
	// the native-MySQL concurrent cold-copy (ADR-0119, roadmap 21b): when true,
	// every table is copied as ONE whole-table work item (the tier-(a)
	// whole-table-stealing behaviour). The Go zero value (false) keeps
	// intra-table stealing ON — the common default — so the field is
	// OPT-OUT-named (the v0.99.51 zero-value trap): a field named for the
	// on-behavior would silently invert to off for every non-CLI construction.
	// Inert on every path that isn't the native-MySQL work-stealing reader.
	NoIntraTableStealing bool
}

// runBulkCopyWithOpts is the configurable variant of [runBulkCopy].
// Existing callers stay on the zero-options shortcut; new callers
// use this to opt into [bulkCopyOpts.SkipSchemaApply] etc.
func runBulkCopyWithOpts(
	ctx context.Context,
	schema *ir.Schema,
	rows ir.RowReader,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
	opts bulkCopyOpts,
) error {
	if !opts.SkipSchemaApply {
		if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
			return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: create tables: %w", err))
		}
	}
	// Bug 125: the MySQL VStream snapshot reader re-emits COPY-phase
	// rows (binlog catchup) and can deliver legitimate rows out of PK
	// order (Vitess orders the scan by a cheaper unique key than the
	// flagged PK). When the reader declares this via
	// [ir.IdempotentCopyReader], route the bulk copy through the
	// engine's upsert path so the re-emissions absorb instead of
	// colliding on a unique key. Readers that don't declare it (PG
	// snapshot, MySQL binlog snapshot) keep the faster plain path.
	needsIdempotent := false
	if icr, ok := rows.(ir.IdempotentCopyReader); ok {
		needsIdempotent = icr.CopyNeedsIdempotentWriter()
	}
	// Durable-write watermark (v0.99.9): when the snapshot reader carries
	// a resumable mid-COPY cursor (CopyDurableProgressSink) AND the writer
	// can report durable flushes (CopyDurableProgressReporter), connect
	// them so the reader's COPY checkpoint never advances past the rows
	// the writer has durably committed. Without this, the VStream pump's
	// TablePKs cursor — which advances as rows are RECEIVED into the
	// bounded in-flight buffer, ahead of the consumer — could persist a
	// checkpoint ahead of the durable frontier; a hard crash would then
	// resume past un-written rows (silent loss). The writer reports
	// per-flush deltas; the sink sums them. Wired only on the idempotent
	// cold-start path (the only one with a resumable VStream source).
	//
	// The reporter is wired ONCE here for the whole run, but it stays
	// inert for any table copied via the ADR-0097 WRITE-side fan-out:
	// WriteRowsIdempotentParallel runs its workers with durable-progress
	// reporting suppressed (the per-worker flush order is not the reader's
	// enqueue order, so a mid-COPY breadcrumb could checkpoint past an
	// un-flushed early row — silent loss). Serial (non-fan-out) tables in
	// the same run keep the ADR-0072 mid-COPY checkpoint. See
	// copyTableColdStartIdempotentParallel + the MySQL writer's
	// WriteRowsIdempotentParallel for the full argument.
	//
	// ADR-0100: the durable watermark is wired ONLY when the serial table
	// loop runs. When the engine surfaces a concurrent-copy partition (≥2
	// disjoint groups → the W = K read→write pipelines below), the mid-COPY
	// checkpoint MUST stay disabled: under W concurrent consumers the
	// durable flushed-row frontier is not order-equivalent to any single
	// stream's enqueue order, so a mid-COPY breadcrumb could checkpoint past
	// an un-written early row → silent-loss-on-resume (the same ADR-0097 §3
	// argument the D-way fan-out already obeys). So we resolve the concurrent
	// partition FIRST and skip the watermark wiring entirely on that path.
	// (The engine pump also records no mid-COPY breadcrumb on the concurrent
	// path, so this is the second of two independent guards — ADR-0100 §6.)
	concGroups := concurrentCopyGroups(rows)
	if needsIdempotent && concGroups == nil {
		if sink, ok := rows.(ir.CopyDurableProgressSink); ok {
			if reporter, ok := rw.(ir.CopyDurableProgressReporter); ok {
				reporter.SetCopyDurableProgress(sink.AdvanceDurableRows)
			}
		}
	}
	// This (the sync cold-start path) stays SERIAL by design — it is NOT
	// wired into the cross-table copy pool (ADR-0076). It has no
	// parallelBulkCopyDeps, no connection budget, and no resume-state
	// mutex, and the snapshot-pinning + idempotent-COPY interplay
	// (CopyDurableProgressSink watermark, in-flight ordering) is delicate
	// enough that parallelising it is a separate, deliberately deferred
	// chunk. Only `sluice migrate` (runBulkCopyPhases) drives cross-table
	// concurrency.
	// ADR-0097: WRITE-side fan-out for the idempotent VStream/CDC
	// snapshot cold-start copy. The READ side is a single un-chunkable
	// vtgate stream, so the only lever is fanning the one row stream out
	// to N PK-hash-partitioned writer workers. Gated on: idempotent path
	// + a writer that implements the parallel capability + a resolved
	// degree > 1 + a per-table usable PK (the partition key). Falls
	// through to the serial idempotent copy otherwise — never silently
	// no-ops.
	fanoutDegree := resolveCopyFanoutDegree(opts.CopyFanoutDegree)
	// ADR-0100 / ADR-0101: WRITE-side cross-table concurrency. When the
	// snapshot reader surfaces a disjoint concurrent-copy partition (≥2
	// groups), drive W consumer pipelines, one per group, each writing its
	// group's tables (the ADR-0097 D-way fan-out composes per table → W × D).
	// This removes the serial one-table-at-a-time consumer.
	//
	// Two readers surface a partition, on opposite sides of the idempotency
	// axis:
	//   - VStream (ADR-0099/0100): re-emits COPY rows (Bug 125) → idempotent
	//     UPSERT path. needsIdempotent is true (it declares
	//     CopyNeedsIdempotentWriter).
	//   - Native MySQL binlog (ADR-0101): N FTWRL-coordinated consistent
	//     snapshots, each table read EXACTLY ONCE, gap-free + overlap-free →
	//     plain INSERT path. needsIdempotent is false.
	// The disjoint partition guarantees each table is written by exactly one
	// pipeline either way, so concurrently plain-INSERTing a gap-free
	// non-idempotent reader is safe (no re-emission to collide on). The old
	// "non-idempotent partition ⇒ refuse" guard was written before the native
	// concurrent path existed and would wrongly reject it; the safety it
	// protected (never concurrently plain-INSERT a RE-EMITTING stream) is
	// preserved because a re-emitting reader is, by definition, idempotent and
	// takes the UPSERT branch. needsIdempotent threads the branch through.
	//
	// When no partition is surfaced (PG / single-stream VStream / serial
	// native MySQL / K = 1), concGroups is nil and the serial loop below runs
	// BYTE-IDENTICALLY.
	if concGroups != nil {
		if err := runConcurrentTableCopy(ctx, concGroups, schema, rows, rw, opts.Redactor, opts.Shard, fanoutDegree, needsIdempotent, opts.NoIntraTableStealing); err != nil {
			return err
		}
	} else {
		if concurrentCopyDispatchObserver != nil {
			concurrentCopyDispatchObserver(0) // serial path taken
		}
		for _, table := range schema.Tables {
			if needsIdempotent {
				if err := copyTableColdStartIdempotentMaybeParallel(ctx, rows, rw, table, opts.Redactor, opts.Shard, fanoutDegree); err != nil {
					return migcore.WrapWithHint(migcore.PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", table.Name, err))
				}
				continue
			}
			// Plain (gap-free) path: compose the ADR-0097 D-way write fan-out
			// (ADR-0102) when the target writer is parallel-capable + the table
			// has a PK + degree > 1, else the single-writer copyTable. This
			// gives a SINGLE-stream native-MySQL cold-copy (W = 1) 1 × D
			// per-table fan-out; PG / VStream-single-stream targets that don't
			// implement ir.ParallelCopyWriter fall through to copyTable
			// byte-identically.
			if err := copyTablePlainMaybeParallel(ctx, rows, rw, table, opts.Redactor, opts.Shard, fanoutDegree); err != nil {
				return migcore.WrapWithHint(migcore.PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", table.Name, err))
			}
		}
	}
	if !opts.SkipSchemaApply {
		// ADR-0114: each post-copy DDL phase rides a storage-grow/reparent
		// instead of aborting the whole migration after a correct data copy.
		if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "identity-sequences", sw, func(ctx context.Context) error {
			return sw.SyncIdentitySequences(ctx, schema)
		}); err != nil {
			return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: sync identity sequences: %w", err))
		}
		if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "indexes", sw, func(ctx context.Context) error {
			return sw.CreateIndexes(ctx, schema)
		}); err != nil {
			return migcore.WrapWithHint(migcore.PhaseIndexes, fmt.Errorf("pipeline: create indexes: %w", err))
		}
		// Loud-failure safety net (SLUICE-E-INDEX-MISSING): the serial cold-start
		// path builds indexes via CreateIndexes directly (not the bug's overlap
		// path), but the net runs here too so the guarantee "no successful run
		// ships a schema missing an index it should have built" holds on EVERY
		// path. Engines without ir.IndexVerifier are a no-op.
		if err := verifyBuiltIndexes(ctx, sw, schema); err != nil {
			return migcore.WrapWithHint(migcore.PhaseIndexes, fmt.Errorf("pipeline: verify indexes: %w", err))
		}
	}
	if opts.SkipSchemaApply {
		// Skip the trailing constraints/views phases too — handled
		// by the loop below short-circuiting them in the same block.
		return nil
	}
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "constraints", sw, func(ctx context.Context) error {
		return sw.CreateConstraints(ctx, schema)
	}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConstraints, fmt.Errorf("pipeline: create constraints: %w", err))
	}
	reportDegradedFKs(ctx, sw)
	if err := migcore.RunViewsPhase(ctx, schema, sw); err != nil {
		return migcore.WrapWithHint(migcore.PhaseViews, err)
	}
	return nil
}

// runBulkCopyForAddTable is the mid-stream live add-table variant of
// [runBulkCopy]. It exists to close the v0.24.0 residual loss surface
// characterized in ADR-0036 (Phase A → Phase B):
//
//   - The orchestrator now CREATEs the target table BEFORE
//     publication-add (see [AddTable.Run] step 3a) so events on the
//     new table delivered to the active stream's applier between
//     publication-add and bulk-copy don't hit the applier's
//     errUnknownTable silent-drop branch. The CREATE in this helper
//     is therefore idempotent (CREATE TABLE IF NOT EXISTS on both
//     engines); the call is kept for symmetry + defense-in-depth on
//     the rare resume path where the orchestrator's early-create
//     didn't run.
//
//   - Bulk-copy uses [ir.IdempotentRowWriter] (INSERT ... ON CONFLICT
//     (pk) DO UPDATE) when the writer exposes it. Under load, a
//     small number of CDC events on the new table that committed in
//     the [publication-add, snapshot-open] window may already have
//     been applied by the active stream's applier by the time
//     bulk-copy reaches those rows; the idempotent path absorbs the
//     overlap. Engines without the surface fall back to plain
//     [ir.RowWriter.WriteRows] with a debug log noting the fallback
//     (no PG/MySQL engine currently lacks it).
//
// Other phases (identity sync, indexes, constraints, views) are
// identical to [runBulkCopy] — only the table-create + per-table
// row copy differ.
func runBulkCopyForAddTable(
	ctx context.Context,
	schema *ir.Schema,
	rows ir.RowReader,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
	redactor *redact.Registry,
	streamID string,
) error {
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: create tables: %w", err))
	}
	for _, table := range schema.Tables {
		if err := copyTableIdempotent(ctx, rows, rw, table, redactor, streamID); err != nil {
			return migcore.WrapWithHint(migcore.PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", table.Name, err))
		}
	}
	// ADR-0114: post-copy DDL phases ride a storage-grow/reparent.
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "identity-sequences", sw, func(ctx context.Context) error {
		return sw.SyncIdentitySequences(ctx, schema)
	}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: sync identity sequences: %w", err))
	}
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "indexes", sw, func(ctx context.Context) error {
		return sw.CreateIndexes(ctx, schema)
	}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseIndexes, fmt.Errorf("pipeline: create indexes: %w", err))
	}
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "constraints", sw, func(ctx context.Context) error {
		return sw.CreateConstraints(ctx, schema)
	}); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConstraints, fmt.Errorf("pipeline: create constraints: %w", err))
	}
	reportDegradedFKs(ctx, sw)
	if err := migcore.RunViewsPhase(ctx, schema, sw); err != nil {
		return migcore.WrapWithHint(migcore.PhaseViews, err)
	}
	return nil
}

// runBulkCopyPhases is the resume-aware variant of [runBulkCopy].
// Each of the five phases is a state-update boundary: state.Phase
// flips before the work runs, and on success the next iteration
// inherits the updated phase. On failure, [markFailed] persists the
// in-flight phase plus a truncated error message; the caller surfaces
// the original error.
//
// Resume semantics per phase:
//
//   - tables:       re-run unconditionally (idempotent CREATE TABLE).
//   - bulk_copy:    per-table classification (skip / truncate-redo /
//     resume-from-cursor / fresh) keyed off state.TableProgress.
//     Per-batch checkpointing on the cursor path; see ADR-0018.
//   - identity_sync, indexes, constraints: re-run unconditionally.
//     Idempotency is best-effort here; a CREATE INDEX with a clashing
//     name will fail. Future iterations can pre-query catalog tables
//     and skip pre-existing entries.
//
// bulkBatchSize is the per-batch row count for the cursor-bearing
// resume path. Zero falls back to migcore.DefaultBulkBatchSize. Ignored on
// the cold-start (non-resume) path which uses the faster plain-INSERT
// or COPY-protocol shape.
func runBulkCopyPhases(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	schema *ir.Schema,
	rows ir.RowReader,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
	resuming bool,
	bulkBatchSize int,
	parallel *parallelBulkCopyDeps,
	tableParallelism int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	upfrontIndexes bool,
	analyzeAfter bool,
) error {
	// ADR-0110 (v0.99.102): wire the run's coordinated grow-pause gate onto
	// the TOP-LEVEL cold-copy writer here, centrally. The native-concurrent /
	// fan-out path (copyTablePlainParallel / copyTableColdStartIdempotentParallel)
	// reuses THIS rw across all D workers — they all call w.flushWithReparentRetry
	// on the same *RowWriter — so setting the gate on rw engages the coordination
	// for the whole fan-out. (The migrate keyset-chunked path ADDITIONALLY wires
	// each fresh per-chunk writer in openOneChunkConn.) v0.99.100 wired ONLY the
	// chunked openOneChunkConn path, so the gate was inert in the sync cold-start /
	// native concurrent cold-copy path — the Track-D / PlanetScale CDC path — and
	// the v0.99.101 PS-320-v11 live run proved it: the gate tripped ZERO times
	// while the writers rode 74 real grow-window retries independently. nil gate
	// or a non-setter writer ⇒ no-op (pre-ADR-0110 behaviour, byte-for-byte).
	if parallel != nil {
		migcore.ApplyGrowGate(rw, parallel.growGate)
		// ADR-0141: wire the run's reparent observer onto the TOP-LEVEL writer
		// here too, centrally, alongside the grow-gate — the single-reader /
		// chunk-0 / fan-out lanes all flush through THIS rw, so a grow/reparent
		// transient on any of them reports through the shared tracker. The
		// reconciliation phase (after the copy+index block below, BOTH branches)
		// re-derives the touched tables, reusing this same rw so a redo that
		// itself reparents re-marks for another round. nil-safe (no tracker /
		// non-MySQL writer).
		migcore.ApplyReparentObserver(rw, parallel.reparentMark())
	}

	// ADR-0123: build the run's SINGLE connection-budget gate — one shared
	// counting semaphore sized to the resolved budget (tableParallelism ×
	// within-table parallelism), the same product ceiling ADR-0076 enforced as
	// two independent static caps. Every base table connection AND every
	// within-table PK-range chunk draws one token from it, so the budget is
	// REDISTRIBUTED at runtime instead of statically partitioned: when peer
	// tables finish and release their base tokens, an in-progress large table's
	// surplus chunks steal them, keeping the copy budget-wide down to the tail
	// (the measured tail-taper fix). Supersedes the per-table fixed-width gate.
	// Constructed once here so BOTH the index-overlap pool and the fallback
	// pool share it. nil parallel (no within-table axis) leaves copyGate nil,
	// and every consumer falls back to its pre-ADR-0123 behaviour.
	if parallel != nil && parallel.copyGate == nil {
		budget := tableParallelism * parallel.parallelism
		if budget < 1 {
			budget = 1
		}
		parallel.copyGate = migcore.NewCopyParallelismGate(budget, migcore.DefaultCopyBackoffPolicy)
		parallel.copyBudget = budget
	}

	// Phase 1: tables.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseTables); err != nil {
		// Phase mark is non-fatal; continue with the data work.
		_ = err
	}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: create tables: %w", err)
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseTables, err))
	}
	progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseTables))

	// --upfront-indexes (Migrator.UpfrontIndexes): build the secondary indexes
	// NOW — before the bulk copy — instead of the default deferred post-copy
	// phase, so the bulk INSERTs maintain them as they load. On a large
	// PlanetScale-MySQL target a deferred `ALTER … ADD INDEX` can run past the
	// max-statement-execution-time limit (errno 3024) and die after a correct
	// copy; building indexes upfront avoids the post-hoc ALTER entirely. This
	// relocates the SAME migcore.RunDDLPhaseWithReparentRetry("indexes", … CreateIndexes)
	// block the deferred path uses (engine-neutral, same reparent-retry + hint)
	// — it is not rewritten. Indexes-only: SyncIdentitySequences and the FK
	// CreateConstraints keep their positions below, so FK ordering is preserved
	// (bare tables → indexes → copy → FKs last). CreateIndexes is idempotent on
	// resume (Bug 131), so a --resume after a mid-copy crash re-runs this
	// without clashing on the already-built indexes. When false this block is
	// skipped and the phase order is byte-identical to before.
	if upfrontIndexes {
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIndexes); err != nil {
			_ = err
		}
		if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "indexes", sw, func(ctx context.Context) error {
			return sw.CreateIndexes(ctx, schema)
		}); err != nil {
			err = fmt.Errorf("pipeline: create indexes (upfront): %w", err)
			return migcore.WrapWithHint(migcore.PhaseIndexes, markFailed(ctx, rc, *state, ir.MigrationPhaseIndexes, err))
		}
		progress.FromContext(ctx).PhaseCompletedEarly(migPhase(ir.MigrationPhaseIndexes))
	}

	// Phase 2: bulk-copy. Per-table state-row updates here so a mid-
	// phase failure preserves the partial progress map.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseBulkCopy); err != nil {
		_ = err
	}
	if state.TableProgress == nil {
		state.TableProgress = map[string]ir.TableProgress{}
	}
	// stateMu serialises access to state.TableProgress across BOTH
	// concurrency axes: the per-chunk goroutines spawned by the
	// within-table parallel-copy path AND the cross-table copy pool
	// below (ADR-0076). The map itself is not safe for concurrent
	// writes; every writer takes the mutex to mutate its table's
	// entry and clones that entry under the lock before the
	// JSON-encoding writeTableProgress call outside it — one
	// progress-row upsert per checkpoint (ADR-0082), so peer tables
	// contend only on this mutex, never on a shared hot state row.
	var stateMu sync.Mutex

	// Phases 2 + 4: bulk-copy and secondary-index builds. ADR-0077: when
	// the target engine implements [ir.IncrementalIndexBuilder] (PG), the
	// two run OVERLAPPED — each table's indexes build as soon as its copy
	// lands, concurrently with the still-copying tables, closing the
	// sequential post-copy index tail. Engines without the surface (MySQL)
	// run the copy pool, then identity-sync, then the whole-schema
	// CreateIndexes — exactly the pre-ADR-0077 sequential order.
	if upfrontIndexes {
		// Indexes were already built above (before the copy), so run the plain
		// cross-table copy pool with NO overlapped index building and NO
		// post-copy index phase. The copy itself is byte-identical to the
		// non-IIB fallback branch's copy (runBulkCopyTablePool with a nil
		// onTableCopied) — only the index phase moved earlier. BOTH PG
		// (IncrementalIndexBuilder) and MySQL take THIS branch when upfront is
		// set, so upfront is not scoped to one engine family (pin the class,
		// not the representative). Identity-sync then runs in its usual spot.
		if err := runBulkCopyTablePool(
			ctx, rc, state, &stateMu, schema, rows, rw,
			resuming, bulkBatchSize, parallel, tableParallelism, redactor, shard, nil,
		); err != nil {
			return err
		}
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseBulkCopy))

		// Phase 3.5: identity sync.
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIdentitySync); err != nil {
			_ = err
		}
		if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "identity-sequences", sw, func(ctx context.Context) error {
			return sw.SyncIdentitySequences(ctx, schema)
		}); err != nil {
			err = fmt.Errorf("pipeline: sync identity sequences: %w", err)
			return migcore.WrapWithHint(migcore.PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseIdentitySync, err))
		}
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseIdentitySync))
	} else if ib, ok := sw.(ir.IncrementalIndexBuilder); ok {
		if err := runOverlappedCopyAndIndexPhase(
			ctx, rc, state, &stateMu, schema, rows, sw, rw, ib,
			resuming, bulkBatchSize, parallel, tableParallelism, redactor, shard,
		); err != nil {
			return err
		}
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseBulkCopy))
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseIndexes))

		// Phase 3.5: identity sync. Runs after the combined copy+index
		// phase; it depends on the copied rows (sequence high-water mark),
		// not on the indexes, so its position relative to index builds is
		// immaterial.
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIdentitySync); err != nil {
			_ = err
		}
		if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "identity-sequences", sw, func(ctx context.Context) error {
			return sw.SyncIdentitySequences(ctx, schema)
		}); err != nil {
			err = fmt.Errorf("pipeline: sync identity sequences: %w", err)
			return migcore.WrapWithHint(migcore.PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseIdentitySync, err))
		}
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseIdentitySync))
	} else {
		// Fallback (MySQL): serial copy → identity-sync → whole-schema
		// indexes, the pre-ADR-0077 ordering.
		if err := runBulkCopyTablePool(
			ctx, rc, state, &stateMu, schema, rows, rw,
			resuming, bulkBatchSize, parallel, tableParallelism, redactor, shard, nil,
		); err != nil {
			return err
		}
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseBulkCopy))

		// Phase 3.5: identity sync.
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIdentitySync); err != nil {
			_ = err
		}
		if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "identity-sequences", sw, func(ctx context.Context) error {
			return sw.SyncIdentitySequences(ctx, schema)
		}); err != nil {
			err = fmt.Errorf("pipeline: sync identity sequences: %w", err)
			return migcore.WrapWithHint(migcore.PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseIdentitySync, err))
		}
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseIdentitySync))

		// Phase 4: indexes.
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIndexes); err != nil {
			_ = err
		}
		if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "indexes", sw, func(ctx context.Context) error {
			return sw.CreateIndexes(ctx, schema)
		}); err != nil {
			err = fmt.Errorf("pipeline: create indexes: %w", err)
			return migcore.WrapWithHint(migcore.PhaseIndexes, markFailed(ctx, rc, *state, ir.MigrationPhaseIndexes, err))
		}
		progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseIndexes))
	}

	// Loud-failure safety net (SLUICE-E-INDEX-MISSING): every branch above —
	// upfront, overlapped, and the non-IIB fallback — is meant to have built
	// every secondary index by now. Verify it, so a silent index-build no-op
	// (the v0.99.x VStream miss, where a MySQL/PlanetScale target created NO
	// secondary indexes yet reported success) can never recur unnoticed. This
	// covers BOTH migrate and the fast-parallel sync cold-start, since both run
	// runBulkCopyPhases. Runs before the reparent reconcile (index EXISTENCE is
	// unaffected by the reconcile's TRUNCATE + re-INSERT). Engines without the
	// ir.IndexVerifier surface are a no-op.
	if err := verifyBuiltIndexes(ctx, sw, schema); err != nil {
		err = fmt.Errorf("pipeline: verify indexes: %w", err)
		return migcore.WrapWithHint(migcore.PhaseIndexes, markFailed(ctx, rc, *state, ir.MigrationPhaseIndexes, err))
	}

	// ADR-0141: reconcile any reparent-touched table AFTER the bulk-copy (and its
	// overlapped/secondary indexes) but BEFORE constraints. A PlanetScale
	// storage-grow reparent silently under-copies — it drops committed-but-
	// unreplicated rows the reactive grow-gate cannot recover — so re-derive each
	// touched table from the SOURCE (TRUNCATE + serial re-copy) until it matches
	// the source exactly. Placed here, after BOTH the IncrementalIndexBuilder
	// (PG, MySQL) overlapped branch AND the non-IIB fallback branch: every
	// production target implements IncrementalIndexBuilder and therefore takes the
	// overlapped branch, so scoping this to the fallback else made it dead code —
	// the reconcile never ran for a real MySQL target (the v0.99.160 miss, caught
	// by live PlanetScale re-validation). Running before the constraints phase
	// keeps the TRUNCATE free of FK dependencies; a touched table's already-built
	// secondary indexes are maintained by the re-INSERT. No-op at zero cost when
	// no reparent occurred (the common case); only the MySQL writer marks today,
	// so only a MySQL target ever reconciles.
	if err := reconcileMigrateReparentTouched(ctx, schema, rows, rw, parallel, redactor, shard); err != nil {
		return migcore.WrapWithHint(migcore.PhaseBulkCopy, markFailed(ctx, rc, *state, ir.MigrationPhaseBulkCopy, err))
	}

	// Phase 5: constraints.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseConstraints); err != nil {
		_ = err
	}
	if err := migcore.RunDDLPhaseWithReparentRetry(ctx, "constraints", sw, func(ctx context.Context) error {
		return sw.CreateConstraints(ctx, schema)
	}); err != nil {
		err = fmt.Errorf("pipeline: create constraints: %w", err)
		return migcore.WrapWithHint(migcore.PhaseConstraints, markFailed(ctx, rc, *state, ir.MigrationPhaseConstraints, err))
	}
	reportDegradedFKs(ctx, sw)
	progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseConstraints))

	// Phase 6: views. Final phase so all referenced base tables
	// exist by the time the view is created. View-to-view dependency
	// ordering uses a single-pass-with-retries policy (see
	// [migcore.RunViewsPhase]) — no SQL parser, no topological sort.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseViews); err != nil {
		_ = err
	}
	if err := migcore.RunViewsPhase(ctx, schema, sw); err != nil {
		return migcore.WrapWithHint(migcore.PhaseViews, markFailed(ctx, rc, *state, ir.MigrationPhaseViews, err))
	}
	progress.FromContext(ctx).PhaseCompleted(migPhase(ir.MigrationPhaseViews))

	// Advisory post-success phase: `--analyze-after` (perf research delta
	// 4). Runs LAST — after constraints and views — so the refreshed
	// statistics reflect the final table shape, and deliberately outside
	// the resume state machine: it can never fail the run (per-table WARN
	// only) and re-running it on a resume is a harmless re-ANALYZE.
	if analyzeAfter {
		runAnalyzeAfterPhase(ctx, schema, sw)
	}

	return nil
}

// reconcileMigrateReparentTouched re-derives every reparent-touched table from
// the SOURCE (ADR-0141) so it exactly matches the source, recovering rows a
// PlanetScale storage-grow reparent silently dropped that the grow-gate could
// not. The migrate analog of [Restore.reconcileReparentTouched]: where the
// restore replays from its immutable chunks, migrate replays from the live
// source — sound under migrate's existing static-source precondition (a plain
// migrate already captures no cross-table snapshot consistency; ADR-0019), and
// it makes the target match the CURRENT source for the touched table.
//
// Each redo runs through the SAME primary writer (which carries the reparent
// observer), so a redo that itself hits a reparent re-marks its table for the
// next round; the loop ends when a full pass drains empty — the sound proxy
// for "no reparent ⇒ no loss", since a reparent is the only loss vector.
// Bounded by [migcore.ReconcileMaxRounds] so a target that reparents on every serial
// redo surfaces loudly rather than looping forever. No-op when no reparent
// occurred (the common case — the tracker drains empty at zero cost).
func reconcileMigrateReparentTouched(
	ctx context.Context,
	schema *ir.Schema,
	rows ir.RowReader,
	rw ir.RowWriter,
	parallel *parallelBulkCopyDeps,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) error {
	if parallel == nil || parallel.reparentTracker == nil {
		return nil
	}
	byName := make(map[string]*ir.Table, len(schema.Tables))
	for _, t := range schema.Tables {
		byName[t.Name] = t
	}
	for round := 1; ; round++ {
		touched := parallel.reparentTracker.Drain()
		if len(touched) == 0 {
			return nil
		}
		if round > migcore.ReconcileMaxRounds {
			return fmt.Errorf(
				"migrate: reparent reconciliation did not converge after %d rounds — the target keeps reparenting during the serial redo (still-touched: %v); re-run with --bulk-parallelism 1 or migrate into a pre-sized / Metal target",
				migcore.ReconcileMaxRounds, touched,
			)
		}
		slog.WarnContext(
			ctx, "migrate: reconciling reparent-touched tables (ADR-0141) — re-deriving each from the source to recover any rows the target's storage-grow reparent dropped",
			slog.Int("round", round),
			slog.Int("tables", len(touched)),
			slog.Any("table_names", touched),
		)
		for _, name := range touched {
			table, ok := byName[name]
			if !ok {
				// Touched a table outside this run's schema (e.g. filtered out
				// after the mark) — nothing to re-derive.
				continue
			}
			if err := reapplyMigrateTableForReconcile(ctx, rows, rw, table, redactor, shard); err != nil {
				return fmt.Errorf("reconcile table %q: %w", name, err)
			}
		}
	}
}

// reapplyMigrateTableForReconcile re-derives one table from the SOURCE
// (ADR-0141): TRUNCATE the target table (via [ir.TableTruncator] — the MySQL
// RowWriter implements it) then re-copy it SERIALLY (within-table parallelism
// = 1, the single-stream [copyTable] pace that never outruns replication) into
// the now-empty table. No primary-key / UPSERT needed — the truncate leaves a
// clean target and indexes/constraints are later phases — exactly the cold
// restore's TRUNCATE+redo shape ([Restore.reapplyTableForReconcile]). The
// serial redo reuses the supplied primary writer (which carries the reparent
// observer), so a reparent during the redo re-marks the table for another
// round.
func reapplyMigrateTableForReconcile(
	ctx context.Context,
	rows ir.RowReader,
	rw ir.RowWriter,
	table *ir.Table,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) error {
	truncator, ok := rw.(ir.TableTruncator)
	if !ok {
		return fmt.Errorf(
			"migrate: target engine cannot TRUNCATE for reconciliation of %q; re-run with --bulk-parallelism 1",
			table.Name,
		)
	}
	if err := truncator.TruncateTable(ctx, table); err != nil {
		return fmt.Errorf("truncate before redo: %w", err)
	}
	return copyTable(ctx, rows, rw, table, redactor, shard)
}

// validate checks that all required fields are populated. Errors here
// indicate caller bugs; surface them clearly before any I/O happens.
func (m *Migrator) validate() error {
	switch {
	case m.Source == nil:
		return errors.New("pipeline: Source engine is nil")
	case m.Target == nil:
		return errors.New("pipeline: Target engine is nil")
	case m.SourceDSN == "":
		return errors.New("pipeline: SourceDSN is empty")
	case m.TargetDSN == "":
		return errors.New("pipeline: TargetDSN is empty")
	case m.Resume && m.ResetTargetData:
		return errors.New("pipeline: --resume and --reset-target-data are mutually exclusive")
	case m.SkipForeignKeys && m.AllowDegradedFKs:
		return errors.New("pipeline: --skip-foreign-keys and --allow-degraded-fks are mutually exclusive " +
			"(one skips FK creation entirely; the other creates FKs and tolerates dirty source rows)")
	}
	if err := migcore.ValidateTargetSchema(m.Target, m.TargetSchema); err != nil {
		return err
	}
	return validateEnabledPGExtensions(m.Source, m.Target, m.EnabledPGExtensions)
}

// dryRunRowCounts returns a best-effort map of table-name → row count
// for the supplied schema. Engines that implement [ir.RowCounter]
// (today: MySQL and Postgres) populate the map; failures (engine
// doesn't implement counting, RowReader open failure, per-table
// CountRows error) leave the entry as -1 so the caller can render
// "count unavailable" rather than "empty table". v0.10.2 / Item H.
//
// The RowReader is opened and closed inside this function. Errors
// are logged at Warn level (not returned) — dry-run output should
// degrade gracefully rather than refuse to print.
func dryRunRowCounts(ctx context.Context, source ir.Engine, dsn string, schema *ir.Schema) map[string]int64 {
	counts := make(map[string]int64, len(schema.Tables))
	for _, t := range schema.Tables {
		counts[t.Name] = -1 // default: count unavailable
	}

	rr, err := source.OpenRowReader(ctx, dsn)
	if err != nil {
		slog.WarnContext(
			ctx, "dry run: row counts unavailable (failed to open source row reader)",
			slog.String("error", err.Error()),
		)
		return counts
	}
	defer migcore.CloseIf(rr)

	counter, ok := rr.(ir.RowCounter)
	if !ok {
		slog.DebugContext(
			ctx, "dry run: source engine doesn't implement RowCounter; row counts omitted",
			slog.String("engine", source.Name()),
		)
		return counts
	}

	for _, t := range schema.Tables {
		n, err := counter.CountRows(ctx, t)
		if err != nil {
			slog.WarnContext(
				ctx, "dry run: row count failed for table",
				slog.String("table", t.Name),
				slog.String("error", err.Error()),
			)
			continue
		}
		counts[t.Name] = n
	}
	return counts
}

// applyCopyCheckpoint wires the resumable COPY-cursor checkpoint sink
// (ADR-0072 Phase B) onto a snapshot row reader that opts into it via
// [ir.CopyCheckpointer]. The sink upserts the in-progress snapshot
// position to the control table — the same row the cold-start CDC
// anchor and the apply path write, with the same idempotency contract —
// so a fault mid-COPY resumes from the checkpoint rather than re-copying
// the table from row 0.
//
// No-op unless BOTH sides are present: the reader implements
// CopyCheckpointer (only the VStream cold-start reader does today) and
// the applier implements [ir.PositionWriter] (every shipping engine
// does). When either is absent the snapshot path keeps its pre-ADR-0072
// behaviour (position persisted only at COPY_COMPLETED). The ctx passed
// to the sink at call time is the COPY pump's own context, supplied per
// checkpoint by the engine — there's no pipeline-side ctx to capture.
func applyCopyCheckpoint(rows ir.RowReader, applier ir.ChangeApplier, streamID string) {
	cp, ok := rows.(ir.CopyCheckpointer)
	if !ok {
		return
	}
	pw, ok := applier.(ir.PositionWriter)
	if !ok {
		return
	}
	cp.SetCopyCheckpoint(func(checkpointCtx context.Context, pos ir.Position) error {
		return pw.WritePosition(checkpointCtx, streamID, pos)
	})
}

// applyExecTimeout plumbs the streamer-side --apply-exec-timeout
// value to an engine-side [ir.ChangeApplier] that opts into the
// per-exec deadline via [ir.ApplyExecTimeoutSetter]. Engines that
// don't implement the setter inherit the pre-v0.52.0 behaviour
// (no per-statement deadline; the apply call uses only the
// streamer's parent context).
//
// Zero or negative duration is a no-op — the setter is not called,
// so the applier's existing default applies (typically "no timeout").
//
// Called immediately after each engine applier opens, before any
// ApplyBatch dispatch. Closes the GitHub #23 silent-stall failure
// mode by guaranteeing every Exec returns within a bounded window.
func applyExecTimeout(target any, d time.Duration) {
	if d <= 0 {
		return
	}
	if setter, ok := target.(ir.ApplyExecTimeoutSetter); ok {
		setter.SetExecTimeout(d)
	}
}

// applyRedactor plumbs the streamer-side --redact registry to a
// target [ir.ChangeApplier] that opts into PII redaction via
// [ir.RedactorSetter]. PII Phase 1.5: completes the operator
// contract that Phase 1's CHANGELOG documented as "bulk-copy
// only" — CDC apply paths now redact too. Engines that don't
// implement the setter inherit the pre-Phase-1.5 behaviour (CDC
// events flow through unredacted).
//
// nil registry skips the call (no setter invocation, no setter
// stored on the applier). Empty registry is also a no-op via the
// applier's own ApplyRow short-circuit.
func applyRedactor(target any, registry *redact.Registry) {
	if registry.Empty() {
		return
	}
	if setter, ok := target.(ir.RedactorSetter); ok {
		setter.SetRedactor(registry)
	}
}

// applyStreamID plumbs the streamer-side stream identifier to a
// target [ir.ChangeApplier] that opts into [ir.StreamIDSetter]
// (PII Phase 2.c, v0.59.0). The applier needs the stream-id to
// derive replay-stable seeds for randomize:* strategies on CDC
// events. Engines that don't implement the setter inherit the
// empty-streamID behaviour (randomize:* still replay-stable per
// (table, column, PK) tuple within the empty-streamID space).
//
// Empty streamID is a no-op; the setter is only invoked when the
// streamer has a non-empty identifier to pass through.
func applyStreamID(target any, streamID string) {
	if streamID == "" {
		return
	}
	if setter, ok := target.(ir.StreamIDSetter); ok {
		setter.SetStreamID(streamID)
	}
}

// applyShardColumn plumbs the streamer-side
// `--inject-shard-column NAME=VALUE` (ADR-0048 Shape A) to an
// engine-side [ir.ChangeApplier] that opts into
// [ir.ShardColumnSetter]. CDC apply paths stamp the
// discriminator onto every row-bearing change the same way the
// bulk-copy path stamps via [shardStampRows] — the two halves
// of DP-1's resolved two-surface split. Engines that don't
// implement the setter inherit the no-stamp default.
//
// Empty shard.Name is a no-op; the setter is only invoked when
// Shape A is engaged. Cross-engine refusal lives separately in
// [migcore.CheckCrossEngineSupportable] (a target engine that doesn't
// implement the setter is refused there before any data moves).
func applyShardColumn(target any, shard ShardColumnSpec) {
	if !shard.Engaged() {
		return
	}
	if setter, ok := target.(ir.ShardColumnSetter); ok {
		setter.SetShardColumn(shard.Name, shard.Value)
	}
}
