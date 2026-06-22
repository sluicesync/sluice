// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// phaseReadSourceSchema is runSingleDatabase's opening phase: merge
// the engine-default table exclusions, open the source SchemaReader,
// read the schema, apply the table/view filters (+ the multi-database
// FK deferral), and run the source-side preflights (RLS, XID
// wraparound, partitioned tables) against the still-open reader.
//
// The returned SchemaReader is handed back to the caller, which owns
// its lifetime (a single deferred closeIf spanning the whole run) —
// it is returned non-nil even alongside an error so the caller's
// defer releases it on every post-open failure path, exactly as the
// pre-split inline defer did. A nil schema with a nil error is the
// empty-source case: nothing to migrate (already logged here).
func (m *Migrator) phaseReadSourceSchema(ctx context.Context, scope *multiDBScope) (ir.SchemaReader, *ir.Schema, error) {
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
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}

	// ADR-0032: thread the operator's --enable-pg-extension allowlist
	// through the source-side reader before the schema scan. Engines
	// without ExtensionAware skip cleanly. Refusals (unknown name,
	// missing on source) bubble up as a clean error before any data
	// moves.
	if err := applyEnabledPGExtensions(ctx, sr, m.EnabledPGExtensions); err != nil {
		return sr, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: enable PG extensions on source: %w", err))
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
		return sr, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "source schema has no tables; nothing to migrate")
		return sr, nil, nil
	}

	// ---- 1.25. Prune schema by table filter ----
	// Pruning here means every downstream phase (schema apply, bulk
	// copy, indexes, constraints) operates on the filtered set
	// implicitly — engines stay agnostic to the filter spec.
	if err := applyTableFilter(ctx, schema, m.Filter); err != nil {
		return sr, nil, err
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
		return sr, nil, err
	}

	// XID-wraparound preflight (pgcopydb PR #17 adoption). Refuses
	// upfront when the source PG database is near the 32-bit wraparound
	// horizon (age(datfrozenxid) ≥ ~1.5B) — migrating from such a source
	// either races PG's global write-block or makes it worse. Gated on
	// the PostgresBackend capability; non-PG paths short-circuit.
	if err := preflightSourceXIDWraparound(ctx, sr, m.Source.Capabilities()); err != nil {
		return sr, nil, err
	}

	// Partition preflight (Bug 100 / v0.92.0). Refuses upfront when
	// the source schema contains declaratively-partitioned tables,
	// since sluice would otherwise silently flatten the parent to a
	// plain heap (dropping the partition key + composite PK) AND
	// re-copy the children as separate heaps. PG-only; the
	// PostgresBackend capability gate inside the preflight excludes
	// non-PG paths.
	if err := preflightPartitionedTables(ctx, sr, m.Source.Capabilities(), schema); err != nil {
		return sr, nil, err
	}

	return sr, schema, nil
}

// phaseTranslateAndGateSchema shapes the post-filter schema for the
// target and runs every pre-DDL gate: per-column type overrides,
// generated-expression overrides, the ADR-0048 Shape-A discriminator
// injection, the ADR-0078 raw-copy lane gate (whose result is the
// returned bool), the Bug 60 redaction-type preflight, and — on
// cross-engine runs — the supportability refusals, loud-gap /
// untranslatable-expression backstops, and the loud-notice scans.
// All of it fires before DryRun and before any schema apply, so there
// is never a partially-migrated target and the diagnostics match
// `schema preview`.
func (m *Migrator) phaseTranslateAndGateSchema(ctx context.Context, schema *ir.Schema) (*ir.Schema, bool, error) {
	// ---- 1.5. Apply per-column type-mapping overrides ----
	schema, err := translate.ApplyMappings(schema, m.Mappings)
	if err != nil {
		return nil, false, fmt.Errorf("pipeline: apply mappings: %w", err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, m.ExpressionMappings)
	if err != nil {
		return nil, false, fmt.Errorf("pipeline: apply expression overrides: %w", err)
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
			return nil, false, wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: inject shard column: %w", err))
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
		return nil, false, wrapWithHint(PhaseConnect, err)
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
			return nil, false, err
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
			return nil, false, err
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
			return nil, false, err
		}
		// Bug 9: a generated column referencing another generated
		// column in the same table — MySQL permits it, PG rejects with
		// 42P17 mid-create-tables (after partial migration). Refuse
		// cleanly up front, at the same pre-DDL point as the gates
		// above so the diagnostic matches `schema preview`.
		if err := translate.RefuseOnGeneratedColRefGeneratedCol(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); err != nil {
			return nil, false, err
		}
		// Bug 20 residual: LOWER()/UPPER() over a bare string literal
		// in a GENERATED column — PG STORED generated columns need a
		// determinable collation a literal lacks (42P22). The ::text
		// rewrite rescues CHECK/DEFAULT but not a STORED gen col;
		// refuse cleanly up front rather than abort mid-create-tables.
		if err := translate.RefuseOnLowerUpperLiteralInGenerated(
			schema, m.Source.Name(), m.Target.Name(), "migrate",
		); err != nil {
			return nil, false, err
		}

		// ---- 1.67. Cross-engine schema-narrowing advisory notices ----
		// The unsigned-bigint range narrowing (Bug 11), the
		// unconstrained-numeric widening (Bug 69), and the wide-varchar
		// down-map (Bug 72) — each a deliberate, documented cross-engine
		// policy surfaced LOUDLY here (and at `schema preview`) so it is
		// never silent. These are NOTICES, not refusals: the universal
		// Rails/Laravel/Django (and ubiquitous unconstrained-numeric /
		// wide free-text) schemas must still migrate. Each scanner
		// self-short-circuits its non-applicable engine pair, so the
		// helper is safe to call unconditionally here inside the
		// cross-engine block. Shared verbatim with the `sync` cold-start
		// path (Bug 157 Q2) via emitCrossEngineTranslationNotices.
		emitCrossEngineTranslationNotices(ctx, schema, m.Source.Name(), m.Target.Name(), "migrate")
	}

	return schema, rawCopyOK, nil
}

// phasePreflightTarget runs the target-side preflights against the
// freshly-opened writers: the RLS gate (role-permission, not state —
// it fires regardless of resume / reset-target-data), then the
// stale-backend detect/reap. The stale-backend check MUST precede
// both the cold-start preflight and the connection-budget probe
// (Bug 123) — the inline comments carry the lock-ordering rationale.
func (m *Migrator) phasePreflightTarget(ctx context.Context, rc resumeContext, state ir.MigrationState, schema *ir.Schema, rw ir.RowWriter) error {
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
	return nil
}

// phaseGateColdStart is the resume / reset / populated-target gate:
// --resume logs the resume plan and rides TableProgress;
// --reset-target-data clears the target first; otherwise the
// ADR-0048 Shape-A three-point check and the Bug 9 cold-start
// preflight refuse a populated target (with --force-cold-start as
// the explicit override).
func (m *Migrator) phaseGateColdStart(ctx context.Context, rc resumeContext, state ir.MigrationState, schema *ir.Schema, rw ir.RowWriter, resuming bool) error {
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
			// Bug 152: refuse a multi-shard source merging into a single
			// non-discriminated, collision-capable target (silent
			// cross-shard overwrite). No-op when --inject-shard-column is
			// set (it solves the hazard) or for a single-shard/non-sharded
			// source. Runs first so the clearest diagnostic fires.
			if err := preflightCrossShardCollision(ctx, m.Source, m.SourceDSN, schema, m.InjectShardColumn.Engaged(), m.AllowCrossShardMerge); err != nil {
				return markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
			}
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
	return nil
}

// phaseResolveCopyParallelism resolves the bulk-copy parallelism from
// the target's measured connection budget at the single chokepoint:
// the connection-budget preflight (item 4), the ADR-0077 index-build
// overlap reservation (threaded onto the SchemaWriter), and the
// ADR-0076 table × within-table split whose product can never exceed
// the budget. Returns (tableParallelism, withinParallelism).
func (m *Migrator) phaseResolveCopyParallelism(ctx context.Context, rc resumeContext, state ir.MigrationState, sw ir.SchemaWriter) (tableParallelism, withinParallelism int, err error) {
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
		return 0, 0, markFailed(ctx, rc, state, ir.MigrationPhasePending, err)
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
	tableParallelism, withinParallelism = resolveCopyParallelismBudget(
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

	return tableParallelism, withinParallelism, nil
}

// phaseBuildCopyDeps negotiates the raw-copy wire format (ADR-0078 —
// only meaningful when the run-level gate held; per-table identity +
// raw-surface checks still happen at dispatch) and assembles the
// parallel bulk-copy dependency set handed to runBulkCopyPhases.
func (m *Migrator) phaseBuildCopyDeps(ctx context.Context, schema *ir.Schema, rr ir.RowReader, rw ir.RowWriter, rawCopyOK bool, withinParallelism int) *parallelBulkCopyDeps {
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
		// ADR-0110: one coordinated grow-pause gate for the whole cold-copy
		// run, shared across all lanes. Constructed UNCONDITIONALLY (no
		// EnableX config bool — the v0.99.51 zero-value trap); with no trip
		// source firing it is inert (Await fast-paths, no owner goroutine
		// spawns). The migrate path has no TargetTelemetry wired, so this is
		// the SIGNAL-driven gate (recovered=nil): the first classified
		// grow-transient on any lane quiesces the rest. ctx is the run ctx,
		// so the gate's owner goroutine exits on run unwind.
		growGate: growGateOrNil(newGrowGate(ctx, nil)),
	}

	return parallelDeps
}
