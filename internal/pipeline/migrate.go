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

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
	"github.com/orware/sluice/internal/translate"
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

	// Engine-default exclusions (Bug 22 / v0.8.1): merge in any
	// patterns the source engine surfaces via [ir.DefaultTableExcluder]
	// — today PlanetScale's `_vt_*` Vitess shadow tables, triggered
	// either by the planetscale flavor flag or by a vanilla-mysql DSN
	// pointing at a PlanetScale endpoint. Operator-supplied
	// --include-table short-circuits the merge. Replace the field
	// in-place because the orchestrator is single-shot per Run.
	if eff, added := effectiveTableFilter(m.Filter, m.Source, m.SourceDSN); len(added) > 0 {
		slog.InfoContext(ctx, "applying engine-default table exclusions",
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

	// ---- 1.5. Apply per-column type-mapping overrides ----
	schema, err = translate.ApplyMappings(schema, m.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply mappings: %w", err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, m.ExpressionMappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply expression overrides: %w", err)
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
	if err := applyEnabledPGExtensions(ctx, sw, m.EnabledPGExtensions); err != nil {
		return wrapWithHint(PhaseConnect, markFailed(ctx, rc, state, ir.MigrationPhasePending,
			fmt.Errorf("pipeline: enable PG extensions on target: %w", err)))
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
			// Cold-start pre-flight: refuse if any target table already
			// contains data. See preflight.go for the rationale (Bug 9).
			// Skipped on --resume (TableProgress drives that path) and
			// on --force-cold-start (explicit operator override).
			if err := preflightColdStart(ctx, schema, rw, m.ForceColdStart, preflightModeMigrate); err != nil {
				return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
			}
		}
	}

	parallelDeps := &parallelBulkCopyDeps{
		source:         m.Source,
		target:         m.Target,
		sourceDSN:      m.SourceDSN,
		targetDSN:      m.TargetDSN,
		parallelism:    resolveBulkParallelism(m.BulkParallelism, runtime.NumCPU()),
		minRows:        resolveBulkParallelMinRows(m.BulkParallelMinRows),
		maxBufferBytes: m.MaxBufferBytes,
		// ADR-0043 gate (3): --force-cold-start skipped the Bug 9
		// preflight, so the target may hold rows; the fast non-upsert
		// loader must not run on a chunk in that case.
		forceColdStart: m.ForceColdStart,
	}

	if err := runBulkCopyPhases(ctx, rc, &state, schema, rr, sw, rw, resuming, m.BulkBatchSize, parallelDeps, m.Redactor); err != nil {
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
	for _, table := range schema.Tables {
		if err := copyTable(ctx, rows, rw, table, opts.Redactor); err != nil {
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
		slog.DebugContext(ctx, "add-table: row writer does not implement IdempotentRowWriter; falling back to plain WriteRows (the [publication-add, snapshot-open] overlap window may surface as a duplicate-key error under sustained load)",
			slog.String("table", table.Name),
		)
		if err := rw.WriteRows(copyCtx, table, redacted); err != nil {
			return fmt.Errorf("write rows: %w", err)
		}
		if err := redactErrFn(); err != nil {
			return fmt.Errorf("redact rows: %w", err)
		}
		return nil
	}
	if err := idem.WriteRowsIdempotent(copyCtx, table, redacted); err != nil {
		return fmt.Errorf("write rows (idempotent): %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
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
	redactor *redact.Registry,
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
	// stateMu serialises access to state.TableProgress across the
	// per-chunk goroutines spawned by the parallel-copy path. The
	// map itself is not safe for concurrent writes; each chunk takes
	// the mutex when checkpointing its cursor.
	var stateMu sync.Mutex
	for _, table := range schema.Tables {
		if err := bulkCopyOneTable(ctx, rc, state, &stateMu, rows, rw, table, resuming, bulkBatchSize, parallel, redactor); err != nil {
			return err
		}
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

	// Phase 5: constraints.
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseConstraints); err != nil {
		_ = err
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		err = fmt.Errorf("pipeline: create constraints: %w", err)
		return wrapWithHint(PhaseConstraints, markFailed(ctx, rc, *state, ir.MigrationPhaseConstraints, err))
	}
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
					slog.DebugContext(ctx, "view create failed, will retry",
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
			slog.DebugContext(ctx, "no progress in views phase; bailing to error report",
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
func copyTable(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table, redactor *redact.Registry) (retErr error) {
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
	if err := rw.WriteRows(copyCtx, table, redacted); err != nil {
		return fmt.Errorf("write rows: %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	return nil
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
	slog.InfoContext(ctx, "dry run: migration plan",
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
		slog.InfoContext(ctx, "dry run: table",
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
		slog.WarnContext(ctx, "dry run: row counts unavailable (failed to open source row reader)",
			slog.String("error", err.Error()),
		)
		return counts
	}
	defer closeIf(rr)

	counter, ok := rr.(ir.RowCounter)
	if !ok {
		slog.DebugContext(ctx, "dry run: source engine doesn't implement RowCounter; row counts omitted",
			slog.String("engine", source.Name()),
		)
		return counts
	}

	for _, t := range schema.Tables {
		n, err := counter.CountRows(ctx, t)
		if err != nil {
			slog.WarnContext(ctx, "dry run: row count failed for table",
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
