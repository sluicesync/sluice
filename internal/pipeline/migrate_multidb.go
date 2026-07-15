// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// Multi-database fan-out (ADR-0074). This file holds the orchestrator
// surface that lets one `sluice migrate` run copy N source databases to
// N same-named target namespaces in a single invocation. The design is
// deliberately additive: the heavy lifting stays in the existing
// single-database [Migrator.runSingleDatabase] body; this layer
// enumerates the database set, then runs that body once per database
// with a per-database scope (Table.Schema stamping + the FK carve-out)
// and per-database source/target DSNs.
//
// The MySQL-database ↔ namespace mapping (ADR-0031, reaffirmed by
// ADR-0074): each source database routes to a PG *schema* of the same
// name (MySQL → PG; the PG writer already emits Schema-qualified DDL via
// --target-schema) or a target *database* of the same name (MySQL →
// MySQL; auto-created via CREATE DATABASE IF NOT EXISTS).

// multiDBScope carries the per-database context the source schema reader
// needs in a fan-out run: the database name (stamped onto Table.Schema /
// View.Schema) and the in-scope predicate (the flat-scope FK carve-out's
// out-of-scope refusal). Nil in a genuine single-database run.
type multiDBScope struct {
	// database is the source database being read this iteration.
	database string

	// inScope reports whether a referenced database name is part of the
	// migrated set. Used by the MySQL reader's FK carve-out to refuse a
	// cross-database FK that points outside the selected databases.
	inScope func(database string) bool
}

// multiDatabaseMode reports whether any database-scope flag engages the
// fan-out path. Single-database mode (the default) returns false and the
// orchestrator runs byte-identically to its pre-ADR-0074 shape. A non-empty
// NamespaceMap (ADR-0142 --map-database/--map-schema) also engages the path:
// a map-only invocation migrates exactly the mapped namespaces.
func (m *Migrator) multiDatabaseMode() bool {
	return m.AllDatabases || !m.DatabaseFilter.IsEmpty() || !m.NamespaceMap.IsEmpty()
}

// runMultiDatabase resolves the selected database set and runs the
// single-database migrate body once per database. Each iteration gets a
// per-database source DSN (DBName set), a per-database target namespace
// (PG schema = database name, or an auto-created same-named MySQL
// database with its own target DSN), and a [multiDBScope] so the source
// reader stamps Table.Schema and lifts the FK carve-out.
//
// Databases are processed in lexicographic order for deterministic logs
// and reproducible failure points; a failure on one database aborts the
// run (no partial-success swallowing — the loud-failure tenet).
func (m *Migrator) runMultiDatabase(ctx context.Context) error {
	if err := m.validateMultiDatabase(); err != nil {
		return err
	}

	lister, ok := m.Source.(ir.DatabaseLister)
	if !ok {
		return fmt.Errorf(
			"pipeline: the multi-namespace fan-out flags (--all-databases / --include-database / "+
				"--exclude-database, or their --*-schema synonyms) require a source engine that can "+
				"enumerate namespaces, but %q does not (MySQL source = ADR-0074 databases; "+
				"Postgres source = ADR-0075 schemas)",
			m.Source.Name(),
		)
	}
	deriver, ok := m.Source.(ir.DatabaseDSNDeriver)
	if !ok {
		return fmt.Errorf(
			"pipeline: source engine %q cannot derive a per-namespace DSN for multi-namespace migrate (ADR-0074 / ADR-0075)",
			m.Source.Name(),
		)
	}

	all, err := lister.ListDatabases(ctx, m.SourceDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: list source namespaces: %w", err))
	}

	// Resolve the selected SOURCE set (ADR-0142: map-only ⇒ the map keys
	// ARE the selection; otherwise the filter decides and the map renames
	// within it), then refuse loudly if a rename-map key is not in that
	// selection (typo guard, before any data moves).
	selected := selectNamespaces(all, m.DatabaseFilter, m.AllDatabases, m.NamespaceMap)
	if err := crossCheckMapSelection(selected, m.NamespaceMap); err != nil {
		return err
	}
	if len(selected) == 0 {
		return errors.New(
			"pipeline: no source namespaces matched the scope; nothing to migrate " +
				"(check --include-database / --exclude-database / --include-schema / --exclude-schema / " +
				"--map-database / --map-schema, or that the source has non-system databases / schemas)",
		)
	}

	// Resolve each source namespace's TARGET name through the rename map and
	// refuse loudly on a many-to-one collision (two sources → one target),
	// even on a case-sensitive target where the fold preflight is a no-op.
	targets, err := resolveTargetNamespaces(selected, m.NamespaceMap)
	if err != nil {
		return err
	}

	selectedSet := make(map[string]struct{}, len(selected))
	for _, db := range selected {
		selectedSet[db] = struct{}{}
	}
	inScope := func(database string) bool {
		_, ok := selectedSet[database]
		return ok
	}

	slog.InfoContext(
		ctx, "multi-database migrate: resolved database set",
		slog.Int("count", len(selected)),
		slog.Any("databases", selected),
	)

	// ---- Pre-flight: unsafe target-namespace fold (ADR-0075 resolved
	// decision #1). When the target engine folds namespace names (MySQL
	// under lower_case_table_names != 0) two distinct source namespaces
	// can collapse to the same target database — a silent merge of two
	// namespaces' data. Refuse LOUDLY, naming both, before any data
	// moves. No-op on a target engine that doesn't fold (Postgres:
	// case-sensitive schemas) — the type-assertion falls through. ----
	// Run on the MAPPED target names (ADR-0142): a case-fold collision
	// between a mapped and an unmapped target is the same silent-merge
	// hazard. Identity map ⇒ targets == selected ⇒ byte-identical to before.
	if err := preflightNamespaceFoldCollisions(ctx, m.Target, m.TargetDSN, targets); err != nil {
		return err
	}

	// Per-namespace target routing is keyed on the TARGET engine exposing
	// [ir.DatabaseDSNDeriver]: auto-create each same-named target namespace
	// and re-point the target DSN at it. Both shipped targets implement it —
	// MySQL's EnsureDatabase is `CREATE DATABASE IF NOT EXISTS` (ADR-0074),
	// PG's is `CREATE SCHEMA IF NOT EXISTS` (ADR-0075) — so MySQL→MySQL and
	// {MySQL,PG}→PG all take the deriver branch below. The non-deriver `else`
	// is the fallback for a hypothetical namespaced target without the
	// surface (it routes via --target-schema and leans on the writer's own
	// auto-create); it is unreachable with today's engines but kept so the
	// orchestrator stays engine-neutral.
	targetDeriver, targetCanDeriveDB := m.Target.(ir.DatabaseDSNDeriver)

	// ---- Pre-flight: read every selected database's schema up front so
	// the flat-scope FK carve-out's out-of-scope refusal (raised inside
	// the MySQL reader's ReadSchema) fires BEFORE any data moves — the
	// loud-failure-first contract. Without this, an out-of-scope FK in a
	// late-alphabet database would only surface after earlier databases
	// had already migrated (a partial-migration state). The reads are
	// cheap (information_schema metadata) relative to the bulk copy. ----
	for _, database := range selected {
		if err := m.preflightMultiDBSchema(ctx, deriver, database, inScope); err != nil {
			return err
		}
	}

	// perRuns records each per-database clone (its resolved source/target
	// DSNs + target-schema) so the final deferred-FK pass can re-open the
	// same routing.
	perRuns := make([]Migrator, 0, len(selected))

	for i, database := range selected {
		// target is the source namespace's renamed TARGET (ADR-0142;
		// identity when unmapped). Only the target-namespace identifier
		// uses it — the source reads below stay keyed on `database`.
		target := targets[i]

		sourceDSN, err := deriver.WithDatabase(m.SourceDSN, database)
		if err != nil {
			return fmt.Errorf("pipeline: derive source DSN for database %q: %w", database, err)
		}

		// Build a per-database Migrator clone. The clone reuses every
		// operator-supplied option (filters, parallelism, redaction, …)
		// and only overrides the per-database routing fields.
		perDB := *m
		perDB.SourceDSN = sourceDSN
		// Clear the multi-database fields on the clone so its Run takes
		// the single-database path.
		perDB.DatabaseFilter = DatabaseFilter{}
		perDB.AllDatabases = false
		perDB.NamespaceMap = NamespaceRenameMap{}

		if targetCanDeriveDB {
			// Each source namespace → its (possibly renamed) target namespace,
			// auto-created (CREATE DATABASE for MySQL / CREATE SCHEMA for PG),
			// with the target DSN re-pointed at it via WithDatabase.
			// Skip the auto-create under --dry-run: a dry run must not
			// mutate the target (the per-database runSingleDatabase below
			// prints the plan and writes nothing). WithDatabase is pure, so
			// the routing is still derived for an accurate plan.
			if !m.DryRun {
				if err := targetDeriver.EnsureDatabase(ctx, m.TargetDSN, target); err != nil {
					return migcore.WrapWithHint(migcore.PhaseSchemaApply,
						fmt.Errorf("pipeline: ensure target database %q: %w", target, err))
				}
			}
			targetDSN, err := targetDeriver.WithDatabase(m.TargetDSN, target)
			if err != nil {
				return fmt.Errorf("pipeline: derive target DSN for database %q: %w", target, err)
			}
			perDB.TargetDSN = targetDSN
			perDB.TargetSchema = ""
		} else {
			// Fallback for a namespaced target without [ir.DatabaseDSNDeriver]
			// (none today): route to the (possibly renamed) target namespace
			// via --target-schema and lean on the writer's own auto-create.
			perDB.TargetSchema = target
		}

		// MigrationID must be unique per database so each database's
		// resumable migrate-state row is independent. Derive a
		// per-database suffix off whatever the operator supplied (or the
		// auto-derived base).
		perDB.MigrationID = multiDBMigrationID(m.MigrationID, database)

		// Defer all foreign keys to the final cross-database pass: a
		// cross-database FK references a table in another selected
		// database that may not exist on the target yet.
		perDB.multiDBDeferFKs = true

		slog.InfoContext(
			ctx, "multi-database migrate: starting database",
			slog.String("database", database),
		)
		scope := &multiDBScope{database: database, inScope: inScope}
		if err := perDB.runSingleDatabase(ctx, scope); err != nil {
			return fmt.Errorf("pipeline: migrate database %q: %w", database, err)
		}
		// Remember the routing so the deferred-FK pass can re-open the
		// same per-database source reader + target writer.
		perRuns = append(perRuns, perDB)
	}

	// ---- Final cross-database pass: apply the deferred foreign keys now
	// that every selected database's tables exist on the target. Skipped
	// under --dry-run (CreateConstraints writes to the target; the dry run
	// must not mutate it — the per-database plans above already printed
	// what would be created). ----
	if m.DryRun {
		slog.InfoContext(ctx, "multi-database migrate: dry-run, skipping target database creation and the deferred foreign-key pass",
			slog.Int("databases", len(selected)))
		return nil
	}
	// --skip-foreign-keys: each per-database run already stripped its FKs and
	// synthesized the backing indexes (see phaseReadSourceSchema), so the
	// cross-database FK pass must NOT run — there are no FKs to create.
	if m.SkipForeignKeys {
		slog.InfoContext(ctx, "multi-database migrate: --skip-foreign-keys set; foreign keys not created "+
			"(each database kept its referencing columns indexed)", slog.Int("databases", len(selected)))
	} else {
		for i, perDB := range perRuns {
			database := selected[i]
			scope := &multiDBScope{database: database, inScope: inScope}
			if err := perDB.applyDeferredConstraints(ctx, scope); err != nil {
				return fmt.Errorf("pipeline: apply foreign keys for database %q: %w", database, err)
			}
		}
	}

	slog.InfoContext(
		ctx, "multi-database migrate complete",
		slog.Int("databases", len(selected)),
	)
	return nil
}

// preflightNamespaceFoldCollisions refuses LOUDLY when two distinct
// selected source namespaces would FOLD to the same target-namespace
// identifier (ADR-0075 resolved decision #1) — the silent-merge hazard a
// PG → MySQL multi-schema fan-out carries because MySQL database names
// fold per `lower_case_table_names` while PG schema names are
// case-sensitive.
//
// No-op when the target engine doesn't implement [ir.NamespaceFolder]
// (Postgres target: case-sensitive, never folds), or when the fold is
// identity (MySQL with lct=0). Selected is the already-resolved,
// already-sorted source namespace set. The check runs before any data
// moves; a collision names BOTH source namespaces and the folded target
// identifier so the operator can rename or re-scope.
func preflightNamespaceFoldCollisions(ctx context.Context, target ir.Engine, targetDSN string, selected []string) error {
	folder, ok := target.(ir.NamespaceFolder)
	if !ok {
		return nil
	}
	// folded -> first source namespace that mapped to it.
	seen := make(map[string]string, len(selected))
	for _, src := range selected {
		folded, err := folder.FoldNamespace(ctx, targetDSN, src)
		if err != nil {
			return migcore.WrapWithHint(migcore.PhaseConnect,
				fmt.Errorf("pipeline: probe target namespace fold for %q: %w", src, err))
		}
		if prev, dup := seen[folded]; dup {
			return fmt.Errorf(
				"pipeline: source namespaces %q and %q both fold to the target namespace %q "+
					"(the target engine %q folds names case-insensitively, e.g. MySQL under "+
					"lower_case_table_names != 0) — sluice refuses to silently merge two source "+
					"namespaces into one target database; rename one source schema or drop one "+
					"from scope via --exclude-schema / --include-schema",
				prev, src, folded, target.Name(),
			)
		}
		seen[folded] = src
	}
	return nil
}

// preflightMultiDBSchema reads one selected database's schema purely to
// trigger the source reader's flat-scope FK carve-out — so an
// out-of-scope cross-database FK is refused LOUDLY before any database
// migrates (the loud-failure-first contract). The read result is
// discarded; only its error matters here.
func (m *Migrator) preflightMultiDBSchema(
	ctx context.Context,
	deriver ir.DatabaseDSNDeriver,
	database string,
	inScope func(string) bool,
) error {
	dsn, err := deriver.WithDatabase(m.SourceDSN, database)
	if err != nil {
		return fmt.Errorf("pipeline: derive source DSN for database %q: %w", database, err)
	}
	sr, err := m.Source.OpenSchemaReader(ctx, dsn)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: open source schema reader for %q: %w", database, err))
	}
	defer migcore.CloseIf(sr)
	migcore.ApplyTableScope(sr, m.Filter)
	applyMultiDatabaseScope(sr, &multiDBScope{database: database, inScope: inScope})
	if _, err := sr.ReadSchema(ctx); err != nil {
		// The out-of-scope FK refusal surfaces here; name the database
		// for the operator.
		return fmt.Errorf("pipeline: preflight database %q: %w", database, err)
	}
	return nil
}

// applyDeferredConstraints re-reads the (now FK-bearing) source schema
// for one database and applies ONLY its foreign-key constraints to the
// target — the final pass of the multi-database fan-out, run after every
// selected database's tables exist so cross-database references resolve.
//
// It re-opens a scoped source reader (to repopulate ReferencedSchema +
// re-validate the out-of-scope carve-out) and a target schema writer
// routed to this database's namespace (same DSN/target-schema the
// per-database run used), then calls CreateConstraints. CreateConstraints
// is idempotent on both engines (detect-then-skip), so a partial-failure
// retry is safe.
func (m *Migrator) applyDeferredConstraints(ctx context.Context, scope *multiDBScope) error {
	sr, err := m.Source.OpenSchemaReader(ctx, m.SourceDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	defer migcore.CloseIf(sr)
	migcore.ApplyTableScope(sr, m.Filter)
	applyMultiDatabaseScope(sr, scope)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if err := migcore.ApplyTableFilter(ctx, schema, m.Filter); err != nil {
		return err
	}
	if err := migcore.PreflightTableReads(sr, schema); err != nil {
		return err
	}
	// ADR-0143: prune the ORM tables here too so this deferred cross-database
	// constraints pass matches the per-database run that already skipped them
	// — without it CreateConstraints would target tables that were never
	// created on the target.
	applyORMTableSkip(ctx, schema, m.SkipORMTables, m.Filter)

	sw, err := m.Target.OpenSchemaWriter(ctx, m.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: open target schema writer: %w", err))
	}
	defer migcore.CloseIf(sw)
	migcore.ApplyTargetSchema(sw, m.TargetSchema)

	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return migcore.WrapWithHint(migcore.PhaseConstraints, fmt.Errorf("pipeline: create constraints: %w", err))
	}
	reportDegradedFKs(ctx, sw)
	return nil
}

// stripForeignKeys clears every table's ForeignKeys so the per-database
// constraint phase emits nothing; the multi-database orchestrator
// re-applies them in a final cross-database pass.
func stripForeignKeys(schema *ir.Schema) {
	for _, t := range schema.Tables {
		if t != nil {
			t.ForeignKeys = nil
		}
	}
}

// validateMultiDatabase enforces the multi-database-mode preconditions
// that don't fit the single-database [Migrator.validate]. Surfaced as
// caller-facing errors before any I/O.
func (m *Migrator) validateMultiDatabase() error {
	if m.AllDatabases && !m.DatabaseFilter.IsEmpty() {
		return errors.New(
			"pipeline: --all-databases is mutually exclusive with --include-database / --exclude-database",
		)
	}
	if m.TargetSchema != "" {
		return errors.New(
			"pipeline: --target-schema is incompatible with multi-database mode; " +
				"each source database routes to a same-named target namespace automatically (ADR-0074)",
		)
	}
	if m.InjectShardColumn.Engaged() {
		return errors.New(
			"pipeline: --inject-shard-column is not supported in multi-database mode (ADR-0074)",
		)
	}
	return nil
}

// multiDBMigrationID derives a per-database migration_id from the
// operator-supplied base (or "" for the auto-derived default) and the
// database name, so each database's resumable migrate-state row is
// distinct. With an empty base the single-database path auto-derives its
// own id from the (now per-database) DSNs, so we leave it empty and let
// that machinery run; with an explicit base we suffix the database name.
func multiDBMigrationID(base, database string) string {
	if base == "" {
		return ""
	}
	return base + "/" + database
}

// applyMultiDatabaseScope threads a [multiDBScope] into a freshly-opened
// source [ir.SchemaReader] when the reader implements
// [ir.MultiDatabaseScoper]. No-op when scope is nil (single-database
// mode) or the engine doesn't expose the surface (the post-read path
// still works; only the Table.Schema stamping + FK carve-out are
// engine-reader features).
func applyMultiDatabaseScope(sr ir.SchemaReader, scope *multiDBScope) {
	if scope == nil {
		return
	}
	if s, ok := sr.(ir.MultiDatabaseScoper); ok {
		s.SetMultiDatabaseScope(scope.database, scope.inScope)
	}
}
