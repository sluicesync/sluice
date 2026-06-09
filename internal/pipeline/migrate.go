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
	"io"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
	"sluicesync.dev/sluice/internal/translate"
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

	// Filter selects which source tables participate in the
	// migration. Empty filter (zero value) keeps the previous
	// behaviour of migrating every table the source schema reader
	// returns. The filter is applied immediately after ReadSchema
	// and before any subsequent phase (schema apply, bulk copy,
	// indexes, constraints) so each phase consumes the pruned
	// schema implicitly.
	Filter TableFilter

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
	// single budget chokepoint (see [resolveCopyParallelismBudget]).
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
	// [pipeline.redactRow]).
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
	// Engine-default exclusions (Bug 22 / v0.8.1): merge in any
	// patterns the source engine surfaces via [ir.DefaultTableExcluder]
	// — today PlanetScale's `_vt_*` Vitess shadow tables, triggered
	// either by the planetscale flavor flag or by a vanilla-mysql DSN
	// pointing at a PlanetScale endpoint. Operator-supplied
	// --include-table short-circuits the merge. Replace the field
	// in-place because the orchestrator is single-shot per Run.
	if eff, added := effectiveTableFilter(m.Filter, m.Source, m.SourceDSN); len(added) > 0 {
		slog.InfoContext(
			ctx, "applying engine-default table exclusions",
			slog.String("engine", m.Source.Name()),
			slog.Any("patterns", added),
		)
		m.Filter = eff
	}

	// ---- 1. Open and read source schema ----
	// Source readers stay on the source DSN's schema — only the target
	// side is namespaced under --target-schema (ADR-0031).
	sr, err := m.Source.OpenSchemaReader(ctx, m.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	defer closeIf(sr)

	// ADR-0032: thread the operator's --enable-pg-extension allowlist
	// through the source-side reader before the schema scan. Engines
	// without ExtensionAware skip cleanly. Refusals (unknown name,
	// missing on source) bubble up as a clean error before any data
	// moves.
	if err := applyEnabledPGExtensions(ctx, sr, m.EnabledPGExtensions); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: enable PG extensions on source: %w", err))
	}

	// ADR-0047 tier (b): enable verbatim passthrough for uncatalogued
	// PG extension types ONLY when the run is provably same-engine
	// PG → PG (engine-name-only determination; the orchestrator stays
	// engine-neutral). Cross-engine / non-PG runs never enable it, so
	// the existing loud refusal (tier (c)) is preserved unchanged.
	applyVerbatimExtensionPassthrough(sr, verbatimLiveSameEnginePG(m.Source, m.Target))

	// catalog Bug 76: scope per-column type validation to the
	// to-be-migrated tables. m.Filter already has engine-default
	// exclusions merged just above, so this push-down matches the
	// authoritative post-read applyTableFilter prune below.
	applyTableScope(sr, m.Filter)

	// Multi-database fan-out (ADR-0074): tell the source reader it is
	// reading one database of the selected set so it stamps
	// Table.Schema / View.Schema with the database name and lifts the
	// flat-scope FK carve-out (populating ReferencedSchema + refusing an
	// out-of-scope cross-database FK loudly). No-op in single-database
	// mode (scope is nil) and on engines without [ir.MultiDatabaseScoper].
	applyMultiDatabaseScope(sr, scope)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "source schema has no tables; nothing to migrate")
		return nil
	}

	// ---- 1.25. Prune schema by table filter ----
	// Pruning here means every downstream phase (schema apply, bulk
	// copy, indexes, constraints) operates on the filtered set
	// implicitly — engines stay agnostic to the filter spec.
	if err := applyTableFilter(ctx, schema, m.Filter); err != nil {
		return err
	}
	applyViewFilter(ctx, schema, m.ViewFilter, m.SkipViews)

	// Multi-database fan-out (ADR-0074): defer EVERY foreign key to a
	// final cross-database pass. A cross-database FK references a table
	// in another selected database which may not exist on the target yet
	// (databases migrate one at a time, alphabetically), so creating it
	// inline races the referent's creation (MySQL Error 1824). Stripping
	// the FKs here makes Phase 5 a no-op for this database; the
	// orchestrator re-applies them all once every database's tables
	// exist (see applyDeferredMultiDBConstraints). Same-database FKs ride
	// the same deferral — uniform and harmless. The carve-out's
	// out-of-scope refusal already fired at ReadSchema, so the FKs
	// reaching here are all in-scope and safe to re-apply later.
	if m.multiDBDeferFKs {
		stripForeignKeys(schema)
	}

	// ---- 1.45. Source-side RLS preflight (task #52 sub-deliverable 1) ----
	// Refuses when any in-scope source table has RLS enabled AND the
	// connecting role lacks BYPASSRLS — the silent-snapshot-filter
	// class. Runs against the source SchemaReader (sr) AFTER the table
	// filter so an operator's `--exclude-table` of an RLS-enabled
	// table short-circuits the refusal (one of the documented
	// recovery hints). No-op on non-PG sources (the interface
	// type-assertion falls through silently).
	if err := preflightRLS(ctx, schema, sr, rlsSideSource); err != nil {
		return err
	}

	// XID-wraparound preflight (pgcopydb PR #17 adoption). Refuses
	// upfront when the source PG database is near the 32-bit wraparound
	// horizon (age(datfrozenxid) ≥ ~1.5B) — migrating from such a source
	// either races PG's global write-block or makes it worse. Gated on
	// PG-flavoured sources only; non-PG paths short-circuit.
	if err := preflightSourceXIDWraparound(ctx, sr, m.Source.Name()); err != nil {
		return err
	}

	// Partition preflight (Bug 100 / v0.92.0). Refuses upfront when
	// the source schema contains declaratively-partitioned tables,
	// since sluice would otherwise silently flatten the parent to a
	// plain heap (dropping the partition key + composite PK) AND
	// re-copy the children as separate heaps. PG-only; the source-
	// engine name gate inside the preflight excludes non-PG paths.
	if err := preflightPartitionedTables(ctx, sr, m.Source.Name(), schema); err != nil {
		return err
	}

	// ---- 1.5. Apply per-column type-mapping overrides ----
	schema, err = translate.ApplyMappings(schema, m.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply mappings: %w", err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, m.ExpressionMappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply expression overrides: %w", err)
	}
	// ---- 1.51. Shape A discriminator-column injection (ADR-0048) ----
	// Runs after ApplyMappings / ApplyExpressionOverrides and BEFORE
	// the cross-engine supportable pre-flight + every downstream
	// schema-apply step, so the rewritten composite PK and the
	// SluiceInjected column reach the target writer's CREATE TABLE
	// in their final shape. No-op when --inject-shard-column is
	// unset.
	if m.InjectShardColumn.Engaged() {
		schema, err = translate.InjectShardColumn(schema, m.InjectShardColumn.Name, ir.Varchar{Length: 64})
		if err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: inject shard column: %w", err))
		}
	}

	// ---- 1.52. Raw-copy passthrough gate (ADR-0078, item 3b(b)) ----
	// The SINGLE auditable value-fidelity predicate, evaluated ONCE here —
	// after every IR-mutation step (ApplyMappings / ApplyExpressionOverrides
	// / InjectShardColumn) so the gate sees the final transform set, and
	// before any data moves. Result is threaded as a bool into
	// parallelBulkCopyDeps; a per-table identity re-check (identityProjection)
	// + a reader/writer raw-surface assertion happen at per-table dispatch so
	// one odd table falls back without disabling the lane. The byte-pipe
	// bypasses the typed IR (= every value transform), so this gate is the
	// silent-loss backstop: any transform present ⇒ ok=false ⇒ IR copy path.
	rawCopyOK, rawCopyReason := rawCopyGate(rawCopyConfigForMigrator(m))
	if rawCopyOK {
		slog.InfoContext(ctx, "raw-copy passthrough lane eligible (ADR-0078)")
	} else {
		slog.DebugContext(ctx, "raw-copy passthrough lane not eligible; using IR copy path",
			slog.String("reason", rawCopyReason))
	}

	// ---- 1.55. Redaction-type pre-flight refusal (Bug 60, v0.58.1) ----
	// Catches mask:uuid on UUID-typed columns BEFORE schema apply so
	// the operator sees an actionable error at run-start instead of
	// a mid-bulk-copy pgx encode failure. Runs after ApplyMappings so
	// `--type-override=col=text` short-circuits the refusal.
	if err := preflightRedactTypes(m.Redactor, schema); err != nil {
		return wrapWithHint(PhaseConnect, err)
	}

	// ---- 1.6. Cross-engine pre-flight refusal ----
	// chain_restore has called this since v0.20.x; the simple-mode
	// migrate path missed the wire-up. Without this, cross-engine
	// PG → MySQL with an extension-owned index opclass (pg_trgm's
	// gin_trgm_ops, pgvector's vector_l2_ops, etc.) gets through
	// schema-translation and bulk-copy fine and then fails at the
	// CREATE INDEX step on MySQL with Error 1170 — far past the
	// point where the operator can cleanly recover. Surface the
	// refusal here so the recovery hint names the unsupportable
	// shape before any data moves.
	if m.Source.Name() != m.Target.Name() {
		if err := checkCrossEngineSupportable(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); err != nil {
			return err
		}
		// ---- 1.65. Untranslatable-expression pre-flight refusal ----
		// (Bug 8 structural backstop, v0.68.1.) MySQL-only constructs
		// that fall through the translator verbatim and are invalid
		// PostgreSQL (JSON_VALID was translated in v0.68.1; the
		// remaining loud tail — FIND_IN_SET, CONVERT_TZ, INET_ATON,
		// … — has no portable PG form). Previously these emitted
		// wrong DDL and aborted `migrate` at the CREATE TABLE phase
		// AFTER some tables were already created (partial-migration
		// state) with no preview warning. Refuse here — the same
		// pre-DDL point as the cross-engine-supportable check, before
		// DryRun and before any schema apply — so there is never a
		// partially-migrated target and the diagnostic is consistent
		// with `schema preview`.
		if err := translate.RefuseOnLoudGaps(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
			enabledExtensionSet(m.EnabledPGExtensions),
		); err != nil {
			return err
		}
		// Bug 14 GENERAL backstop (allowlist). RefuseOnLoudGaps above
		// is the curated-denylist layer (KNOWN MySQL-only constructs,
		// better construct-specific messages). This catches the
		// general tail: any function-call identifier with no provable
		// PG-valid form (SOUNDEX/ELT/CAST AS UNSIGNED/POINT/…) that the
		// translator would emit verbatim and PG would reject mid-
		// pipeline. Fires at the same pre-DDL point — before DryRun and
		// any schema apply — so there is never a partially-migrated
		// target and the diagnostic matches `schema preview`.
		if err := translate.RefuseOnUntranslatableExprs(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
			enabledExtensionSet(m.EnabledPGExtensions),
		); err != nil {
			return err
		}
		// Bug 9: a generated column referencing another generated
		// column in the same table — MySQL permits it, PG rejects with
		// 42P17 mid-create-tables (after partial migration). Refuse
		// cleanly up front, at the same pre-DDL point as the gates
		// above so the diagnostic matches `schema preview`.
		if err := translate.RefuseOnGeneratedColRefGeneratedCol(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); err != nil {
			return err
		}
		// Bug 20 residual: LOWER()/UPPER() over a bare string literal
		// in a GENERATED column — PG STORED generated columns need a
		// determinable collation a literal lacks (42P22). The ::text
		// rewrite rescues CHECK/DEFAULT but not a STORED gen col;
		// refuse cleanly up front rather than abort mid-create-tables.
		if err := translate.RefuseOnLowerUpperLiteralInGenerated(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); err != nil {
			return err
		}

		// ---- 1.67. Unsigned-bigint range-narrowing notice (Bug 11) ----
		// MySQL `bigint unsigned` maps uniformly to PG `bigint` so PK
		// and FK types match by construction (the FK-to-IDENTITY-PK
		// type mismatch that aborted every default ORM schema is gone).
		// The (2^63, 2^64) range loss is a deliberate, documented
		// policy — surfaced LOUDLY here (and at `schema preview`) so it
		// is never silent. This is a NOTICE, not a refusal: the
		// universal Rails/Laravel/Django schema must still migrate.
		// Emitted at WARN so it stands out in default-level logs.
		if noticeErr := translate.UnsignedBigintNoticeError(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); noticeErr != nil {
			slog.WarnContext(ctx, noticeErr.Error())
		}

		// ---- 1.68. Unconstrained-numeric widening notice (Bug 69) ----
		// An unconstrained PG `numeric` (no declared precision/scale)
		// maps to MySQL `DECIMAL(65,30)` (MySQL has no unbounded
		// DECIMAL). The pre-fix behaviour silently truncated to
		// DECIMAL(0,0); this preserves far more and surfaces the
		// deliberate widening LOUDLY here (and at `schema preview`) so
		// it is never silent. NOTICE, not a refusal — unconstrained
		// numeric is ubiquitous in PG schemas and must still migrate.
		// PG → PG is unaffected (round-trips as bare NUMERIC); the scan
		// short-circuits non-MySQL targets. Emitted at WARN so it
		// stands out in default-level logs.
		if noticeErr := translate.UnconstrainedNumericNoticeError(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); noticeErr != nil {
			slog.WarnContext(ctx, noticeErr.Error())
		}

		// ---- 1.69. Wide-varchar down-map notice (Bug 72) ----
		// A wide bounded PG `varchar(N)` (over MySQL's ~16383-char
		// utf8mb4 VARCHAR cap / 65535-byte row budget) is down-mapped
		// to a MySQL TEXT tier sized to hold N chars — mirroring the
		// unbounded `text` → LONGTEXT policy. The pre-fix behaviour
		// emitted VARCHAR(N) literally and died with a raw MySQL
		// Error 1074 / 1118 at create-tables. NOTICE, not a refusal —
		// wide free-text columns are common and must still migrate.
		// PG → PG is unaffected (varchar round-trips unchanged); the
		// scan short-circuits non-MySQL targets.
		if noticeErr := translate.WideVarcharNoticeError(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); noticeErr != nil {
			slog.WarnContext(ctx, noticeErr.Error())
		}
	}

	if m.DryRun {
		return m.logPlan(ctx, schema)
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
		defer closeIf(rc.store)
	}
	resuming := m.Resume && rc.enabled

	// ---- 2. Open target writers ----
	sw, err := m.Target.OpenSchemaWriter(ctx, m.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open target schema writer: %w", err)))
	}
	applyTargetSchema(sw, m.TargetSchema)
	applyIndexBuildMem(sw, m.IndexBuildMem)
	applyIndexBuildParallelism(sw, m.IndexBuildParallelism)
	if err := applyEnabledPGExtensions(ctx, sw, m.EnabledPGExtensions); err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: enable PG extensions on target: %w", err)))
	}
	if m.AllowDegradedFKs {
		a, ok := sw.(ir.DegradedFKAllower)
		if !ok {
			return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
				errors.New("pipeline: --allow-degraded-fks is set but the target engine doesn't support degraded FKs "+
					"(PG-target only by design; MySQL's nearest analogue, FOREIGN_KEY_CHECKS=0, is a different contract — "+
					"clean the source FK violations before migrating to a MySQL target)")))
		}
		a.EnableDegradedFKs()
	}
	defer closeIf(sw)

	rw, err := m.Target.OpenRowWriter(ctx, m.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open target row writer: %w", err)))
	}
	applyTargetSchema(rw, m.TargetSchema)
	applyMaxBufferBytes(rw, m.MaxBufferBytes)
	defer closeIf(rw)

	// ---- 2.5. Target-side RLS preflight (task #52 sub-deliverable 1) ----
	// Refuses when any in-scope target table has RLS enabled AND the
	// connecting role lacks BYPASSRLS — the INSERT-blocked-by-WITH-
	// CHECK class. Runs against the target RowWriter (rw) regardless
	// of resume / reset-target-data state: RLS is a role-permission
	// gate, not a state gate. No-op on non-PG targets.
	if err := preflightRLS(ctx, schema, rw, rlsSideTarget); err != nil {
		return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
	}

	// Stale-backend preflight (connection-resilience Phase 2, item 2).
	// Detect sluice's OWN orphaned backends on the target — a hard-killed
	// prior run whose server-side COPY backend still holds a target-table
	// lock and a connection slot. Detection runs always and reports
	// loudly; --reap-stale-backends authorises terminating them. No-op on
	// engines without a backend model (MySQL).
	//
	// This MUST run before BOTH the cold-start preflight and the
	// connection-budget probe below: the cold-start preflight reads each
	// target table (an AccessShare lock) to enforce the empty-target
	// contract, which an orphan's AccessExclusive lock blocks — so a reap
	// that ran *after* the preflight could never clear the very lockout it
	// exists to clear (Bug 123). Reaping here frees both the table lock the
	// cold-start preflight then needs and the slots the budget math sees.
	if err := preflightStaleBackends(
		ctx, m.Target, m.TargetDSN,
		targetWriteSchemas(schema, m.TargetSchema),
		m.ReapStaleBackends,
	); err != nil {
		return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
	}

	// ---- 3-6. Schema apply (phase 1) → bulk copy → indexes → constraints.
	rr, err := m.Source.OpenRowReader(ctx, m.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: open source row reader: %w", err)))
	}
	defer closeIf(rr)

	if resuming {
		logResumeStart(ctx, state, schema)
	} else {
		if m.ResetTargetData {
			if err := resetTargetData(ctx, schema, rw, rc.store, rc.migrationID); err != nil {
				return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
			}
		} else {
			// ADR-0048 Shape A populated-target preflight. When
			// --inject-shard-column is set, this is the LOUD
			// replacement for `--force-cold-start`'s silent skip
			// (DP-2): empty-target ⇒ pass through to cold-start
			// preflight; non-empty ⇒ run the three-point check
			// (NULL / value-present / composite-PK-lead). When
			// the flag is unset, this is a no-op and the
			// existing cold-start preflight is the only gate.
			if err := preflightShardConsolidation(ctx, schema, rw, m.InjectShardColumn.Name, m.InjectShardColumn.Value); err != nil {
				return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
			}
			// Cold-start pre-flight: refuse if any target table already
			// contains data. See preflight.go for the rationale (Bug 9).
			// Skipped on --resume (TableProgress drives that path) and
			// on --force-cold-start (explicit operator override).
			// When --inject-shard-column is engaged, the operator has
			// opted into the populated-target loud-refusal contract
			// above; the cold-start preflight is suppressed here
			// because Shape-A's three-point check is its replacement.
			if !m.InjectShardColumn.Engaged() {
				if err := preflightColdStart(ctx, schema, rw, m.ForceColdStart, preflightModeMigrate); err != nil {
					return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
				}
			}
		}
	}

	// Connection-budget preflight (connection-resilience item 4). Probe
	// the target's connection-slot budget BEFORE the per-table parallel-
	// copy pool opens, and cap the resolved parallelism so a wide
	// --bulk-parallelism can't exhaust a small target's slots mid-COPY.
	// No-op on engines without a connection-slot model (MySQL); refuses
	// loudly if the target has no free budget at all.
	copyParallelism, budgetReport, err := resolveTargetCopyParallelism(
		ctx, m.Target, m.TargetDSN,
		resolveBulkParallelism(m.BulkParallelism, runtime.NumCPU()),
		m.MaxTargetConnections,
	)
	if err != nil {
		return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
	}

	// Index-build overlap budget split (ADR-0077, roadmap item 3b(a)).
	// When index builds OVERLAP the copy (the target engine implements
	// [ir.IncrementalIndexBuilder] — PG), copy and index connections are
	// open SIMULTANEOUSLY, so the single measured budget has to cover BOTH.
	// splitCopyAndIndexBudget reserves a small slice for the index pool and
	// hands the copy axes only the remainder, with the invariant
	// indexBudget + copyBudget' <= CopyBudget enforced here at the single
	// chokepoint (no runtime semaphore). The index slice is threaded to the
	// SchemaWriter so its build pool sizes from it instead of self-probing
	// (which would double-count the copy pool's open connections).
	//
	// indexBudget == 0 means "don't overlap-split" — no measured ceiling
	// (MySQL / degraded probe), or the engine has no IncrementalIndexBuilder
	// (MySQL again, which overlaps via the post-copy whole-schema fallback).
	// In that case the copy axes see the full CopyBudget unchanged.
	copyBudgetForAxes := budgetReport.CopyBudget
	indexBudget := 0
	if _, overlaps := sw.(ir.IncrementalIndexBuilder); overlaps {
		ib, copyRemaining := splitCopyAndIndexBudget(budgetReport.CopyBudget, copyParallelism)
		if ib > 0 {
			indexBudget = ib
			copyBudgetForAxes = copyRemaining
		}
	}
	applyIndexBuildBudget(sw, indexBudget)

	// Cross-table copy pool (ADR-0076, roadmap item 3(a)). Split the
	// single connection budget across the table × within-table axes at
	// the SINGLE chokepoint so their PRODUCT can't exhaust the target's
	// slots (gotcha 2). The within factor (copyParallelism) is satisfied
	// first; the table factor gets whatever whole multiples remain, also
	// bounded by --max-target-connections. The within factor is then
	// pinned to its split value so each table's gate opens exactly
	// withinP connections — the product bound holds by construction.
	//
	// copyBudgetForAxes is the copy axes' slice after the ADR-0077 index
	// reservation (== CopyBudget when overlap isn't engaged).
	tableParallelism, withinParallelism := resolveCopyParallelismBudget(
		copyParallelism,
		resolveTableParallelism(m.TableParallelism),
		copyBudgetForAxes,
		m.MaxTargetConnections,
	)
	slog.InfoContext(
		ctx, "bulk-copy parallelism resolved",
		slog.Int("table_parallelism", tableParallelism),
		slog.Int("within_table_parallelism", withinParallelism),
		slog.Int("max_concurrent_connections", tableParallelism*withinParallelism),
		slog.Int("copy_budget", budgetReport.CopyBudget),
		slog.Int("copy_budget_after_index_reserve", copyBudgetForAxes),
		slog.Int("index_build_budget", indexBudget),
	)

	// Raw-copy format negotiation (ADR-0078). Only meaningful when the
	// gate held AND both the primary reader/writer implement the raw
	// surfaces; negotiation probes both endpoints' server majors and
	// downgrades a binary request to text loudly on a mismatch. When the
	// gate didn't hold the format is irrelevant (the lane never engages).
	rawCopyFormat := ir.RawCopyText
	if rawCopyOK {
		if exp, imp, ok := asRawCopyEndpoints(rr, rw); ok {
			rawCopyFormat = negotiateRawCopyFormat(ctx, m.RawCopyFormat, exp, imp)
		}
	}

	parallelDeps := &parallelBulkCopyDeps{
		source:         m.Source,
		target:         m.Target,
		sourceDSN:      m.SourceDSN,
		targetDSN:      m.TargetDSN,
		parallelism:    withinParallelism,
		minRows:        resolveBulkParallelMinRows(m.BulkParallelMinRows, len(schema.Tables)),
		maxBufferBytes: m.MaxBufferBytes,
		// ADR-0043 gate (3): --force-cold-start skipped the Bug 9
		// preflight, so the target may hold rows; the fast non-upsert
		// loader must not run on a chunk in that case.
		forceColdStart: m.ForceColdStart,
		// ADR-0078 raw-copy passthrough — run-level gate result + the
		// negotiated wire format. Per-table identity + raw-surface checks
		// happen at dispatch.
		rawCopyOK:     rawCopyOK,
		rawCopyFormat: rawCopyFormat,
	}

	if err := runBulkCopyPhases(ctx, rc, &state, schema, rr, sw, rw, resuming, m.BulkBatchSize, parallelDeps, tableParallelism, m.Redactor, m.InjectShardColumn); err != nil {
		return err
	}

	markComplete(ctx, rc, state)
	slog.InfoContext(ctx, "migration complete", slog.Int("tables", len(schema.Tables)))
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
		return resumeContext{}, ir.MigrationState{}, false, wrapWithHint(PhaseConnect, err)
	}
	rc := resumeContext{
		store:       store,
		migrationID: m.resolveMigrationID(),
		enabled:     store != nil,
	}
	state, exitClean, err := loadOrInitState(ctx, rc, m.Resume, resetting)
	if err != nil {
		if rc.enabled {
			closeIf(rc.store)
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
	for _, fk := range fks {
		slog.WarnContext(
			ctx, "constraint attached degraded (NOT VALID)",
			slog.String("schema", fk.Schema),
			slog.String("table", fk.Table),
			slog.String("constraint", fk.ConstraintName),
			slog.String("referenced_table", fk.ReferencedTable),
			slog.String("reason", fk.Reason),
			slog.String("hint", fk.Hint),
		)
	}
	slog.WarnContext(ctx, "constraints phase: degraded FKs",
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
	// no-redactions hot path; see [redactRow] and [redactRows] for
	// the wrap semantics.
	Redactor *redact.Registry

	// Shard is the ADR-0048 Shape A discriminator spec. Threaded
	// into copyTable so the per-row stamp lands on the bulk-copy
	// channel before the writer sees it. Zero-value (empty Name)
	// is the no-op default — single-source streams pay nothing.
	Shard ShardColumnSpec
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
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: create tables: %w", err))
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
	if needsIdempotent {
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
	for _, table := range schema.Tables {
		copyFn := copyTable
		if needsIdempotent {
			copyFn = copyTableColdStartIdempotent
		}
		if err := copyFn(ctx, rows, rw, table, opts.Redactor, opts.Shard); err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", table.Name, err))
		}
	}
	if !opts.SkipSchemaApply {
		if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: sync identity sequences: %w", err))
		}
		if err := sw.CreateIndexes(ctx, schema); err != nil {
			return wrapWithHint(PhaseIndexes, fmt.Errorf("pipeline: create indexes: %w", err))
		}
	}
	if opts.SkipSchemaApply {
		// Skip the trailing constraints/views phases too — handled
		// by the loop below short-circuiting them in the same block.
		return nil
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseConstraints, fmt.Errorf("pipeline: create constraints: %w", err))
	}
	reportDegradedFKs(ctx, sw)
	if err := runViewsPhase(ctx, schema, sw); err != nil {
		return wrapWithHint(PhaseViews, err)
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
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: create tables: %w", err))
	}
	for _, table := range schema.Tables {
		if err := copyTableIdempotent(ctx, rows, rw, table, redactor, streamID); err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", table.Name, err))
		}
	}
	if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: sync identity sequences: %w", err))
	}
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		return wrapWithHint(PhaseIndexes, fmt.Errorf("pipeline: create indexes: %w", err))
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseConstraints, fmt.Errorf("pipeline: create constraints: %w", err))
	}
	reportDegradedFKs(ctx, sw)
	if err := runViewsPhase(ctx, schema, sw); err != nil {
		return wrapWithHint(PhaseViews, err)
	}
	return nil
}

// copyTableIdempotent is the add-table variant of [copyTable]: it
// routes the row stream through [ir.IdempotentRowWriter.WriteRowsIdempotent]
// when the writer exposes it (INSERT ... ON CONFLICT (pk) DO UPDATE),
// falling back to plain [ir.RowWriter.WriteRows] otherwise. See
// [runBulkCopyForAddTable] for the v0.24.0 → Phase B fix rationale.
//
// Goroutine-lifecycle handling mirrors [copyTable] exactly — same
// child-ctx + defer-cancel shape so the row reader unwinds cleanly
// on error.
func copyTableIdempotent(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table, redactor *redact.Registry, streamID string) (retErr error) {
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	// PII Phase 1: same wrap as [copyTable] — nil/empty Registry
	// short-circuits to pass-through.
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, tablePKColumns(table), streamID)
	idem, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		slog.DebugContext(
			ctx, "add-table: row writer does not implement IdempotentRowWriter; falling back to plain WriteRows (the [publication-add, snapshot-open] overlap window may surface as a duplicate-key error under sustained load)",
			slog.String("table", table.Name),
		)
		if err := rw.WriteRows(copyCtx, table, redacted); err != nil {
			return fmt.Errorf("write rows: %w", err)
		}
		if err := redactErrFn(); err != nil {
			return fmt.Errorf("redact rows: %w", err)
		}
		// The writer drained the stream; surface any sticky reader
		// error so a mid-stream decode abort fails loudly (Bug 68).
		return readerStreamErr(rr, table)
	}
	if err := idem.WriteRowsIdempotent(copyCtx, table, redacted); err != nil {
		return fmt.Errorf("write rows (idempotent): %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	return readerStreamErr(rr, table)
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
// resume path. Zero falls back to defaultBulkBatchSize. Ignored on
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
) error {
	// Phase 1: tables.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseTables); err != nil {
		// Phase mark is non-fatal; continue with the data work.
		_ = err
	}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: create tables: %w", err)
		return wrapWithHint(PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseTables, err))
	}
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseTables)))

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
	// writes; every writer takes the mutex and clones the state under
	// the lock before the JSON-encoding writeState call (peer tables
	// write distinct keys of the same map — distinct keys under one
	// mutex is exactly cloneStateForWrite's design).
	var stateMu sync.Mutex

	// Phases 2 + 4: bulk-copy and secondary-index builds. ADR-0077: when
	// the target engine implements [ir.IncrementalIndexBuilder] (PG), the
	// two run OVERLAPPED — each table's indexes build as soon as its copy
	// lands, concurrently with the still-copying tables, closing the
	// sequential post-copy index tail. Engines without the surface (MySQL)
	// run the copy pool, then identity-sync, then the whole-schema
	// CreateIndexes — exactly the pre-ADR-0077 sequential order.
	if ib, ok := sw.(ir.IncrementalIndexBuilder); ok {
		if err := runOverlappedCopyAndIndexPhase(
			ctx, rc, state, &stateMu, schema, rows, sw, rw, ib,
			resuming, bulkBatchSize, parallel, tableParallelism, redactor, shard,
		); err != nil {
			return err
		}
		slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseBulkCopy)))
		slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseIndexes)))

		// Phase 3.5: identity sync. Runs after the combined copy+index
		// phase; it depends on the copied rows (sequence high-water mark),
		// not on the indexes, so its position relative to index builds is
		// immaterial.
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIdentitySync); err != nil {
			_ = err
		}
		if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
			err = fmt.Errorf("pipeline: sync identity sequences: %w", err)
			return wrapWithHint(PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseIdentitySync, err))
		}
		slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseIdentitySync)))
	} else {
		// Fallback (MySQL): serial copy → identity-sync → whole-schema
		// indexes, the pre-ADR-0077 ordering.
		if err := runBulkCopyTablePool(
			ctx, rc, state, &stateMu, schema, rows, rw,
			resuming, bulkBatchSize, parallel, tableParallelism, redactor, shard, nil,
		); err != nil {
			return err
		}
		slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseBulkCopy)))

		// Phase 3.5: identity sync.
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIdentitySync); err != nil {
			_ = err
		}
		if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
			err = fmt.Errorf("pipeline: sync identity sequences: %w", err)
			return wrapWithHint(PhaseSchemaApply, markFailed(ctx, rc, *state, ir.MigrationPhaseIdentitySync, err))
		}
		slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseIdentitySync)))

		// Phase 4: indexes.
		if err := markPhase(ctx, rc, state, ir.MigrationPhaseIndexes); err != nil {
			_ = err
		}
		if err := sw.CreateIndexes(ctx, schema); err != nil {
			err = fmt.Errorf("pipeline: create indexes: %w", err)
			return wrapWithHint(PhaseIndexes, markFailed(ctx, rc, *state, ir.MigrationPhaseIndexes, err))
		}
		slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseIndexes)))
	}

	// Phase 5: constraints.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseConstraints); err != nil {
		_ = err
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: create constraints: %w", err)
		return wrapWithHint(PhaseConstraints, markFailed(ctx, rc, *state, ir.MigrationPhaseConstraints, err))
	}
	reportDegradedFKs(ctx, sw)
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseConstraints)))

	// Phase 6: views. Final phase so all referenced base tables
	// exist by the time the view is created. View-to-view dependency
	// ordering uses a single-pass-with-retries policy (see
	// [runViewsPhase]) — no SQL parser, no topological sort.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseViews); err != nil {
		_ = err
	}
	if err := runViewsPhase(ctx, schema, sw); err != nil {
		return wrapWithHint(PhaseViews, markFailed(ctx, rc, *state, ir.MigrationPhaseViews, err))
	}
	slog.InfoContext(ctx, "migration: phase complete", slog.String("phase", string(ir.MigrationPhaseViews)))

	return nil
}

// runViewsPhase emits CREATE VIEW for every view in schema.Views with
// a retry policy that handles view-to-view dependency ordering without
// implementing a full SQL parser. The policy: emit views in declared
// order; on failure, accumulate the failed view in a retry list; after
// the first pass, retry the failed views up to 2 more times. If the
// retry list is non-empty after the third pass, surface the
// accumulated errors.
//
// Why retry rather than topological sort: parsing the view's SELECT
// body to extract referenced views requires a real SQL parser, which
// is out of scope for Phase 1 (and arguably ever — different engines
// have different SELECT grammars). Real-world view dependency depths
// are shallow (typically 1-2 levels of view-on-view); two retry
// passes covers the common cases. Operators with deeper dependency
// graphs (>2 levels of view-on-view chains) get a clear error
// pointing at the still-failing views and can manually reorder
// `--include-view` invocations to bootstrap the dependency chain.
//
// No-op on schemas without views; cheap when none fail.
func runViewsPhase(ctx context.Context, schema *ir.Schema, sw ir.SchemaWriter) error {
	if schema == nil || len(schema.Views) == 0 {
		return nil
	}

	// First pass: try every view. retry collects views that failed.
	pending := append([]*ir.View(nil), schema.Views...)
	var lastErrs []error

	const maxPasses = 3 // 1 initial + 2 retries
	for pass := 0; pass < maxPasses && len(pending) > 0; pass++ {
		var nextPending []*ir.View
		lastErrs = nil
		for _, v := range pending {
			single := &ir.Schema{Views: []*ir.View{v}}
			if err := sw.CreateViews(ctx, single); err != nil {
				if pass == maxPasses-1 {
					// Last pass — accumulate the error for the caller.
					lastErrs = append(lastErrs, fmt.Errorf("view %q: %w", v.Name, err))
				} else {
					slog.DebugContext(
						ctx, "view create failed, will retry",
						slog.String("view", v.Name),
						slog.Int("pass", pass+1),
						slog.String("error", err.Error()),
					)
				}
				nextPending = append(nextPending, v)
			}
		}
		if len(nextPending) == len(pending) && pass < maxPasses-1 {
			// No progress this pass — abort early. Trying again wouldn't
			// help (no view-create succeeded to unblock the rest). Force
			// the next iteration to be the last so the caller gets the
			// accumulated errors.
			slog.DebugContext(
				ctx, "no progress in views phase; bailing to error report",
				slog.Int("pending", len(nextPending)),
				slog.Int("pass", pass+1),
			)
			pass = maxPasses - 2 // next iteration is the last (records errors)
		}
		pending = nextPending
	}

	if len(pending) > 0 {
		// Build a single combined error so the operator sees every
		// still-failing view at once rather than just the first.
		names := make([]string, 0, len(pending))
		for _, v := range pending {
			names = append(names, v.Name)
		}
		base := fmt.Errorf("pipeline: create views failed after %d retries (%d still failing: %v); "+
			"view-to-view dependency depth may exceed retry budget — review and reorder declared view list",
			maxPasses-1, len(pending), names)
		if len(lastErrs) > 0 {
			return errors.Join(append([]error{base}, lastErrs...)...)
		}
		return base
	}

	slog.InfoContext(ctx, "views created", slog.Int("count", len(schema.Views)))
	return nil
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
	}
	if err := validateTargetSchema(m.Target, m.TargetSchema); err != nil {
		return err
	}
	return validateEnabledPGExtensions(m.Source, m.Target, m.EnabledPGExtensions)
}

// copyTable opens the source-side row stream, hands it off to the
// target writer, and waits for completion. The reader's lifetime
// covers exactly one table; the writer is reused across tables.
//
// A [progressTicker] sits in the pipe between reader and writer: it
// counts every row the orchestrator hands to the writer and emits a
// slog line every [progressInterval]. Stop is called via defer so
// progress reporting terminates even on writer error.
//
// Goroutine lifecycle on the error path (Bug 9): the row reader
// (e.g. postgres/row_reader.go::stream) and the tee both block on
// "out <- row" with a select on ctx.Done(). When WriteRows returns
// an error, neither goroutine has any reason to unwind on its own —
// the writer abandoned its consumer end of the channel, but the
// parent ctx is still alive (the caller may want to continue with
// other phases). Without an explicit cancel, both goroutines wedge
// forever; on a Postgres source that means the snapshot transaction
// never commits and PG shows "idle in transaction" sessions.
//
// The fix: derive a child context that's cancelled regardless of
// outcome (defer cancel). The reader and tee see ctx.Done() fire,
// drop their pending sends, and exit cleanly. The parent ctx is
// untouched, so the orchestrator can decide what to do next.
//
// readerStreamErr is the loud-failure gate for the bulk-copy paths
// (Bug 68). A streaming [ir.RowReader] scans and decodes rows on a
// background goroutine; a per-row scan/decode failure there closes
// the row channel exactly like a clean end-of-table would. Without
// observing [ir.RowReader.Err] after the channel drains, the
// orchestrator cannot tell "table fully read" from "a row failed and
// the stream aborted" — the writer simply sees fewer rows and the
// migrate exits 0 with the table silently truncated (the worst
// failure class under the project's loud-failure tenet). Every copy
// path MUST call this after the writer returns success and propagate
// a non-nil result as a hard failure. The error is wrapped so the
// table name and the originating reader error are both visible in
// the operator-facing message.
//
// context.Canceled / context.DeadlineExceeded are deliberately NOT
// treated as a stream failure. The batched + parallel copy paths
// cancel each batch's child context on purpose once the writer has
// drained it (the Bug-9 clean-unwind shape); the reader goroutine
// observes that cancel and stores it on its sticky error. That is a
// benign orchestrator-driven teardown, not a data-integrity failure.
// A genuine parent-context abort (operator Ctrl-C, deadline) is still
// surfaced — the writer returns the same ctx error and the
// orchestrator's own ctx checks fire — so suppressing it here cannot
// hide a real cancellation, only the self-inflicted per-batch one.
// The Bug-68 failure class (a scan/decode error) is never a context
// error; it is a `postgres: column …` / `mysql: scan: …` value, so
// this filter is precise.
func readerStreamErr(rr ir.RowReader, table *ir.Table) error {
	err := rr.Err()
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return fmt.Errorf("source row stream for table %q failed: %w", table.Name, err)
}

func copyTable(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table, redactor *redact.Registry, shard ShardColumnSpec) (retErr error) {
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	// Async row-count for ETA reporting. Best-effort: failures are
	// logged at warn level and the ETA stays unknown for the table's
	// duration. The engine row readers' [ir.RowCounter] implementations
	// short-circuit to (0, nil) on snapshot-pinned readers (single
	// *sql.Conn) so the streamer's snapshot path doesn't deadlock
	// against the in-flight row stream. See progress.go for the full
	// semantics.
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	// PII Phase 1: wrap the row stream with redaction if the operator
	// has configured rules. nil/empty Registry is a zero-cost
	// passthrough — redactRows returns the teed channel verbatim.
	//
	// streamID is empty for migrate runs (Migrator has no stream
	// identity); randomize:* strategies produce stable-per-row outputs
	// within a single migrate run because the seed is fully determined
	// by table + column + PK values. PK-less tables refuse on a
	// randomize:* rule via the strategy's own seed-required check;
	// preflight catches the same case earlier with a richer message.
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, tablePKColumns(table), "")
	// ADR-0048 Shape A: stamp the operator-supplied discriminator
	// onto every row before the writer sees it (the value half
	// of DP-1's two-surface split; sibling to the optional
	// ShardColumnSetter on the CDC path). shardStampRows is a
	// zero-cost passthrough when shard.Name is empty.
	stamped, _ := shardStampRows(copyCtx, redacted, shard.Name, shard.Value)
	if err := rw.WriteRows(copyCtx, table, stamped); err != nil {
		return fmt.Errorf("write rows: %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	// The writer returned without error, but it may have observed a
	// truncated stream because the reader aborted mid-table on a
	// scan/decode failure. Surface that loudly (Bug 68).
	return readerStreamErr(rr, table)
}

// copyTableColdStartIdempotent is the upsert-form of [copyTable] used
// when the snapshot reader declares [ir.IdempotentCopyReader] (Bug 125
// — the MySQL VStream COPY phase re-emits rows and can deliver them out
// of PK order). Routes the row stream through
// [ir.IdempotentRowWriter.WriteRowsIdempotent] so the re-emissions
// absorb via ON DUPLICATE KEY UPDATE / ON CONFLICT instead of colliding
// on a unique key.
//
// Goroutine-lifecycle, redaction, shard-stamping, and the Bug-68
// loud-failure gate are identical to [copyTable] — only the write call
// differs. A target writer that doesn't implement [ir.IdempotentRowWriter]
// is a loud refusal rather than a silent fallback: the reader has
// declared its rows NEED idempotent writes, so falling back to plain
// INSERT would re-introduce the duplicate-key collision the dedup
// removal was meant to fix. (Both shipping target engines implement
// the surface; this guards a future engine that forgets it.)
func copyTableColdStartIdempotent(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table, redactor *redact.Registry, shard ShardColumnSpec) (retErr error) {
	idem, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		return fmt.Errorf(
			"pipeline: table %q: snapshot reader requires an idempotent bulk-copy writer "+
				"(VStream COPY re-emits rows, Bug 125) but the target row writer does not "+
				"implement IdempotentRowWriter",
			table.Name,
		)
	}

	// Bug 125 cross-engine guard. A NO-PRIMARY-KEY table relies on the
	// writer upserting on a unique key to absorb VStream's COPY catchup
	// re-emissions. A writer whose idempotent path plain-INSERTs no-PK
	// tables (the Postgres target today) would DUPLICATE those rows now
	// that the source-side dedup is gone — refuse loudly rather than
	// silently corrupt. PK tables are safe on any idempotent writer
	// (ON CONFLICT / ON DUPLICATE KEY on the PK absorbs re-emissions),
	// so the guard only fires for PK-less tables.
	if len(tablePKColumns(table)) == 0 {
		icw, capable := idem.(ir.IdempotentCopyWriter)
		if !capable || !icw.HandlesNoPKIdempotentCopy() {
			return fmt.Errorf(
				"pipeline: table %q has no PRIMARY KEY and the target's idempotent bulk-copy "+
					"writer does not support no-PK upsert (VStream COPY re-emits rows out of order, "+
					"Bug 125; a plain INSERT would duplicate them). Add a PRIMARY KEY to the source "+
					"table, or migrate it with `migrate` instead of `sync start`",
				table.Name,
			)
		}
	}

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, tablePKColumns(table), "")
	stamped, _ := shardStampRows(copyCtx, redacted, shard.Name, shard.Value)
	if err := idem.WriteRowsIdempotent(copyCtx, table, stamped); err != nil {
		return fmt.Errorf("write rows (idempotent): %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	return readerStreamErr(rr, table)
}

// logPlan writes a human-readable summary of what Run would do via
// slog, without performing any writes. Used when DryRun is true.
//
// The plan is logged at Info level so it surfaces under the default
// handler. The header line is a single message; the per-table lines
// follow with structured attributes so an aggregator can pick out
// individual table summaries without parsing prose.
//
// Per-table row counts (v0.10.2) are best-effort. Sluice opens a
// throwaway [ir.RowReader] on the source and type-asserts for
// [ir.RowCounter]; engines that don't implement counting (or where
// the count fails — permissions, locked table, etc.) get a `-1`
// in the log so the operator sees "count unavailable" rather than
// thinking the table is empty. Counts on huge tables can be slow
// or approximate depending on the engine — see [ir.RowCounter]'s
// doc-comment.
func (m *Migrator) logPlan(ctx context.Context, schema *ir.Schema) error {
	slog.InfoContext(
		ctx, "dry run: migration plan",
		slog.String("source", m.Source.Name()),
		slog.String("target", m.Target.Name()),
		slog.Int("tables", len(schema.Tables)),
		slog.Int("views", len(schema.Views)),
	)

	// Open a throwaway RowReader for row counts. Best-effort: if it
	// fails to open, we still emit the per-table summary lines —
	// just with row_count=-1.
	counts := dryRunRowCounts(ctx, m.Source, m.SourceDSN, schema)

	for _, t := range schema.Tables {
		// Field naming note: secondary_indexes excludes the primary
		// key (which is reported separately via primary_key) — the IR
		// stores PK on its own field, and operators comparing against
		// psql / SHOW INDEX output have been confused by a bare
		// "indexes" count that didn't include PK.
		slog.InfoContext(
			ctx, "dry run: table",
			slog.String("name", t.Name),
			slog.Int("columns", len(t.Columns)),
			slog.Bool("primary_key", t.PrimaryKey != nil),
			slog.Int("secondary_indexes", len(t.Indexes)),
			slog.Int("foreign_keys", len(t.ForeignKeys)),
			slog.Int64("row_count", counts[t.Name]),
		)
	}
	slog.InfoContext(ctx, "dry run: for full target DDL with translation notes and advisory hints, run `sluice schema preview` (ADR-0024)")
	return nil
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
	defer closeIf(rr)

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

// closeIf calls Close on v if it implements io.Closer. Used to clean
// up the *sql.DB handles the engine readers/writers wrap.
func closeIf(v any) {
	if c, ok := v.(io.Closer); ok {
		_ = c.Close()
	}
}

// applyMaxBufferBytes plumbs the orchestrator-side --max-buffer-bytes
// value to an engine-side surface that opts into byte-bounded
// batching via [ir.MaxBufferBytesSetter]. Engines that don't
// implement the setter retain their pre-v0.7.0 row-count-only
// behaviour. Zero or negative bytes is the no-cap value (engines
// fall back to their built-in default if they have one).
//
// Called immediately after each engine writer/applier opens, before
// any WriteRows / ApplyBatch dispatch. See ADR-0028.
func applyMaxBufferBytes(target any, bytes int64) {
	if bytes <= 0 {
		return
	}
	if setter, ok := target.(ir.MaxBufferBytesSetter); ok {
		setter.SetMaxBufferBytes(bytes)
	}
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
// [checkCrossEngineSupportable] (a target engine that doesn't
// implement the setter is refused there before any data moves).
func applyShardColumn(target any, shard ShardColumnSpec) {
	if !shard.Engaged() {
		return
	}
	if setter, ok := target.(ir.ShardColumnSetter); ok {
		setter.SetShardColumn(shard.Name, shard.Value)
	}
}
