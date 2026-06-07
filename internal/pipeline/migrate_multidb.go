// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"sluicesync.dev/sluice/internal/ir"
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
// orchestrator runs byte-identically to its pre-ADR-0074 shape.
func (m *Migrator) multiDatabaseMode() bool {
	return m.AllDatabases || !m.DatabaseFilter.IsEmpty()
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
			"pipeline: --all-databases / --include-database / --exclude-database require a source engine "+
				"that can enumerate databases, but %q does not (this is a MySQL-source feature; ADR-0074)",
			m.Source.Name(),
		)
	}
	deriver, ok := m.Source.(ir.DatabaseDSNDeriver)
	if !ok {
		return fmt.Errorf(
			"pipeline: source engine %q cannot derive a per-database DSN for multi-database migrate (ADR-0074)",
			m.Source.Name(),
		)
	}

	all, err := lister.ListDatabases(ctx, m.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: list source databases: %w", err))
	}

	selected := make([]string, 0, len(all))
	for _, db := range all {
		if m.DatabaseFilter.Allows(db) {
			selected = append(selected, db)
		}
	}
	sort.Strings(selected)
	if len(selected) == 0 {
		return errors.New(
			"pipeline: no source databases matched the database scope; nothing to migrate " +
				"(check --include-database / --exclude-database, or that the source server has non-system databases)",
		)
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

	// MySQL → MySQL target: auto-create each same-named database before
	// its writer opens. PG targets route via --target-schema (the PG
	// writer auto-creates the schema), so no target-database creation is
	// needed there. The ensure step is keyed on the TARGET engine
	// exposing [ir.DatabaseDSNDeriver]; PG does not, so PG targets skip it.
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

	for _, database := range selected {
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

		if targetCanDeriveDB {
			// MySQL → MySQL: each source database → a same-named target
			// database, auto-created, with the target DSN re-pointed.
			// Skip the CREATE DATABASE under --dry-run: a dry run must not
			// mutate the target (the per-database runSingleDatabase below
			// prints the plan and writes nothing). WithDatabase is pure, so
			// the routing is still derived for an accurate plan.
			if !m.DryRun {
				if err := targetDeriver.EnsureDatabase(ctx, m.TargetDSN, database); err != nil {
					return wrapWithHint(PhaseSchemaApply,
						fmt.Errorf("pipeline: ensure target database %q: %w", database, err))
				}
			}
			targetDSN, err := targetDeriver.WithDatabase(m.TargetDSN, database)
			if err != nil {
				return fmt.Errorf("pipeline: derive target DSN for database %q: %w", database, err)
			}
			perDB.TargetDSN = targetDSN
			perDB.TargetSchema = ""
		} else {
			// MySQL → PG (or any namespaced target): route to a target
			// schema of the same name. The PG writer auto-creates the
			// schema and emits Schema-qualified DDL.
			perDB.TargetSchema = database
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
	for i, perDB := range perRuns {
		database := selected[i]
		scope := &multiDBScope{database: database, inScope: inScope}
		if err := perDB.applyDeferredConstraints(ctx, scope); err != nil {
			return fmt.Errorf("pipeline: apply foreign keys for database %q: %w", database, err)
		}
	}

	slog.InfoContext(
		ctx, "multi-database migrate complete",
		slog.Int("databases", len(selected)),
	)
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
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader for %q: %w", database, err))
	}
	defer closeIf(sr)
	applyTableScope(sr, m.Filter)
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
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	defer closeIf(sr)
	applyTableScope(sr, m.Filter)
	applyMultiDatabaseScope(sr, scope)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if err := applyTableFilter(ctx, schema, m.Filter); err != nil {
		return err
	}

	sw, err := m.Target.OpenSchemaWriter(ctx, m.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target schema writer: %w", err))
	}
	defer closeIf(sw)
	applyTargetSchema(sw, m.TargetSchema)

	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseConstraints, fmt.Errorf("pipeline: create constraints: %w", err))
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
