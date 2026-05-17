// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// SchemaWriter applies an IR Schema to a PostgreSQL database in three
// phases (per the [ir.SchemaWriter] contract). Phase 1 is broken into
// two sub-steps because Postgres requires custom enum types to exist
// before tables that reference them:
//
//	phase 1a: CREATE TYPE ... AS ENUM for every enum column
//	phase 1b: CREATE TABLE for every table with columns + PK only
//
//	(bulk-load step happens here, outside the SchemaWriter)
//
//	phase 2:  CREATE INDEX for every non-PK index
//	phase 3:  ALTER TABLE ADD CONSTRAINT for every foreign key
//
// SchemaWriter holds an open *sql.DB; callers should call Close when
// finished to release the connection pool.
type SchemaWriter struct {
	db     *sql.DB
	schema string
	// hasPostGIS is set at engine open time via detectPostGIS. When
	// true, ir.Geometry columns emit as `geometry(<subtype>, <srid>)`;
	// when false, they're rejected with a clear "install postgis"
	// error rather than silently coerced.
	hasPostGIS bool
	// schemaEnsured guards against repeat CREATE SCHEMA IF NOT EXISTS
	// calls when the writer is reused across phases. Set on the first
	// CreateTablesWithoutConstraints (or PreviewDDL — preview is
	// idempotent and skips the ensure step).
	schemaEnsured bool
	// schemaExplicit records whether [SetSchema] was called with a
	// non-empty operator-supplied override (`--target-schema NAME`,
	// ADR-0031). When true, user-defined type idents (enums) emit
	// fully-qualified `"<schema>"."<typname>"` — required when the
	// per-source namespace isn't in PG's session `search_path`
	// (Bug 45). When false, the bare ident emits — preserves the
	// pre-ADR-0031 shape for default-`public` operators (the type
	// lands in `public`, which IS in `search_path`).
	schemaExplicit bool

	// enabledExtensions is the set of extension names the operator
	// opted into via `--enable-pg-extension` (ADR-0032). Populated by
	// [EnableExtensions], which preflights presence on the target.
	// Threaded into emitOpts so emitColumnType can dispatch
	// [ir.ExtensionType] columns through pgExtensionCatalog.
	enabledExtensions map[string]bool
}

// SetSchema implements [ir.SchemaSetter]. Called by the pipeline
// orchestrator when `--target-schema NAME` is set (ADR-0031). Empty
// input is treated as a no-op (preserves the writer's DSN-derived
// default). Subsequent CreateTablesWithoutConstraints / CreateIndexes
// / CreateConstraints emit DDL prefixed with the named schema.
//
// CreateTablesWithoutConstraints ensures the named schema exists via
// `CREATE SCHEMA IF NOT EXISTS` before any table emit; calling
// SetSchema after the schema has been ensured (re-set during a
// resume, etc.) clears the ensured flag so the new schema is
// validated on the next emit.
func (w *SchemaWriter) SetSchema(name string) {
	if name == "" {
		return
	}
	if name != w.schema {
		w.schemaEnsured = false
	}
	w.schema = name
	w.schemaExplicit = true
}

// EnableExtensions implements [ir.ExtensionAware] for PG (ADR-0032).
// Validates each requested extension name against pgExtensionCatalog
// and preflights presence on the target database. Refusals here fire
// before any schema-write phase runs — operator gets a clean error
// before sluice creates / alters anything on the target.
//
// The target-side preflight uses the writer's own *sql.DB pool, so a
// firewall or auth issue surfaces here rather than mid-emit.
//
// Empty / nil extensions is a no-op (today's default).
func (w *SchemaWriter) EnableExtensions(ctx context.Context, extensions []string) error {
	enabled, err := validateAndPreflightExtensionsAt(ctx, w.db, extensions, "target")
	if err != nil {
		return err
	}
	w.enabledExtensions = enabled
	return nil
}

// Close releases the underlying connection pool.
func (w *SchemaWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// CreateTablesWithoutConstraints emits CREATE TYPE statements for any
// enum columns, then CREATE TABLE for every table in s, in
// deterministic (alphabetical) order.
//
// Ensures the writer's target schema exists before any DDL runs
// (CREATE SCHEMA IF NOT EXISTS) — required when `--target-schema`
// names a per-source namespace that may not exist on a fresh target.
// Idempotent on schemas already in place.
func (w *SchemaWriter) CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateTablesWithoutConstraints: schema is nil")
	}
	if err := w.ensureSchema(ctx); err != nil {
		return err
	}

	// Phase 1a: enum types. We walk all columns and emit one
	// CREATE TYPE per enum *type*. A MySQL source has no enum type
	// identity, so each column synthesizes its own table+column-scoped
	// name (no sharing). A same-engine PG source carries the original
	// type name on ir.Enum.TypeName (catalog Bug 19c): two columns
	// referencing the same source type now resolve to the same name,
	// so we dedupe to emit `CREATE TYPE` exactly once — emitting it
	// twice would fail with "type already exists". Generated enum
	// columns are skipped: they emit as TEXT + table-level CHECK
	// (Bug 25 fallback) so no enum type is needed.
	createdEnumTypes := map[string]struct{}{}
	for _, table := range orderedTables(s) {
		for _, col := range table.Columns {
			enum, ok := col.Type.(ir.Enum)
			if !ok || col.IsGenerated() {
				continue
			}
			typeName := resolveEnumTypeName(enum, table.Name, col.Name)
			if _, done := createdEnumTypes[typeName]; done {
				continue
			}
			createdEnumTypes[typeName] = struct{}{}
			stmt := emitCreateEnumType(enum, w.schema, table.Name, col.Name)
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: create enum type for %s.%s: %w", table.Name, col.Name, err)
			}
		}
	}

	// Phase 1b: tables.
	opts := w.emitOpts()
	for _, table := range orderedTables(s) {
		stmt, err := emitTableDef(w.schema, table, opts)
		if err != nil {
			return err
		}
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: create table %q: %w", table.Name, err)
		}
	}

	// Phase 1c: table / column comments (catalog Bug 19d). PG models
	// comments as standalone COMMENT ON statements (MySQL carries them
	// inline, so its writer needs no equivalent phase). Idempotent —
	// COMMENT ON overwrites — so re-running the schema-write phase is
	// safe.
	for _, table := range orderedTables(s) {
		for _, stmt := range emitCommentStatements(w.schema, table) {
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: set comment on %q: %w", table.Name, err)
			}
		}
	}
	return nil
}

// CreateIndexes adds every non-PK index across the schema.
func (w *SchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateIndexes: schema is nil")
	}
	for _, table := range orderedTables(s) {
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			stmt, err := emitCreateIndex(w.schema, table.Name, idx, w.emitOpts())
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: create index %q on %q: %w", idx.Name, table.Name, err)
			}
		}
	}
	return nil
}

// CreateConstraints adds every foreign-key constraint across the
// schema. All referenced tables must already exist.
func (w *SchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateConstraints: schema is nil")
	}
	for _, table := range orderedTables(s) {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			stmt, err := emitAddForeignKey(w.schema, table.Name, fk)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: add foreign key %q on %q: %w", fk.Name, table.Name, err)
			}
		}
	}
	return nil
}

// SyncIdentitySequences advances every identity column's sequence
// past the maximum value present in the target table. Runs after
// bulk-copy completes so user-initiated INSERTs against IDs that
// would have been auto-generated don't collide with bulk-copied IDs.
//
// Implementation is two queries per identity column: a MAX read,
// followed by a conditional setval. The split (vs. a single SQL
// statement) avoids the Postgres edge case where setval(seq, 0)
// errors on a sequence with the default minvalue=1; an empty
// target table simply skips the setval and leaves the sequence at
// its default.
func (w *SchemaWriter) SyncIdentitySequences(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: SyncIdentitySequences: schema is nil")
	}
	for _, table := range orderedTables(s) {
		for _, col := range table.Columns {
			intT, isInt := col.Type.(ir.Integer)
			if !isInt || !intT.AutoIncrement {
				continue
			}
			if err := w.syncOneIdentity(ctx, table, col.Name); err != nil {
				return fmt.Errorf("postgres: sync identity %s.%s.%s: %w",
					w.schema, table.Name, col.Name, err)
			}
		}
	}
	return nil
}

// CreateViews emits `CREATE OR REPLACE VIEW` for regular views and
// `CREATE MATERIALIZED VIEW ... WITH DATA` for materialized views,
// in s.Views declaration order. View definitions are emitted verbatim;
// cross-engine view-body translation is a Phase 3 effort (see
// [ir.View]).
//
// The `WITH DATA` clause on materialized views populates the matview
// from the just-loaded target tables on creation, so the cold-start
// migration ends with a query-ready matview. Phase 2 will extend this
// with CDC-driven `REFRESH MATERIALIZED VIEW` on a configured cadence.
//
// `CREATE OR REPLACE` covers regular-view re-runs idempotently; PG
// rejects `CREATE OR REPLACE MATERIALIZED VIEW`, so a re-run of
// CreateViews against a target whose matview already exists raises
// "relation X already exists". The orchestrator's retry policy treats
// this as success (the view is in place; the second pass would have
// produced the same body anyway).
//
// View-to-view dependency ordering is the orchestrator's responsibility:
// CreateViews emits in declared order, so a view that references
// another view declared later in s.Views fails on the first pass and
// retries on a later pass. See [pipeline.runViewsPhase].
func (w *SchemaWriter) CreateViews(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateViews: schema is nil")
	}
	for _, view := range s.Views {
		if view == nil || view.Name == "" {
			continue
		}
		stmt := emitCreateView(w.schema, view)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: create view %q: %w", view.Name, err)
		}
	}
	return nil
}

// emitCreateView returns the appropriate `CREATE [OR REPLACE | MATERIALIZED]
// VIEW <schema>.<name> AS <definition>` statement for v. The definition
// body is emitted verbatim per Phase 1's no-translation policy. Schema
// qualification matches the writer's behaviour for tables — every
// identifier is namespace-qualified so the writer can round-trip into
// schemas other than the default `public`.
//
// **Bug 31 fix:** PG's `pg_views.definition` and `pg_matviews.definition`
// columns return the SELECT body with a trailing semicolon. The
// previous emit appended " WITH DATA;" or ";" directly, producing
// "... ; WITH DATA;" (which PG rejects as SQLSTATE 42601) or
// "... ;;" (which PG silently parses but is still ugly DDL). Trim
// trailing whitespace + semicolon before appending the trailer.
func emitCreateView(schema string, v *ir.View) string {
	qualified := quoteIdent(schema) + "." + quoteIdent(v.Name)
	body := trimTrailingSemicolon(v.Definition)
	if v.Materialized {
		return "CREATE MATERIALIZED VIEW " + qualified + " AS " + body + " WITH DATA;"
	}
	return "CREATE OR REPLACE VIEW " + qualified + " AS " + body + ";"
}

// syncOneIdentity reads MAX(<col>) on the target table; if non-NULL,
// runs setval on the column's underlying sequence so that next
// nextval returns MAX+1.
func (w *SchemaWriter) syncOneIdentity(ctx context.Context, table *ir.Table, column string) error {
	qualified := quoteIdent(w.schema) + "." + quoteIdent(table.Name)

	// Step 1: read MAX(<col>). NULL on empty table.
	maxQuery := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(column), qualified)
	var maxVal sql.NullInt64
	if err := w.db.QueryRowContext(ctx, maxQuery).Scan(&maxVal); err != nil {
		return fmt.Errorf("read max: %w", err)
	}
	if !maxVal.Valid {
		// Empty table — sequence's default is already correct.
		return nil
	}

	// Step 2: setval(seq, max). The third arg defaults to true →
	// next nextval returns max+1. The WHERE clause guards against
	// pg_get_serial_sequence returning NULL (defensive; should not
	// fire for any standard IDENTITY column).
	const setvalQuery = `
		SELECT setval(pg_get_serial_sequence($1, $2), $3, true)
		WHERE pg_get_serial_sequence($1, $2) IS NOT NULL`
	tableArg := w.schema + "." + table.Name
	if _, err := w.db.ExecContext(ctx, setvalQuery, tableArg, column, maxVal.Int64); err != nil {
		return fmt.Errorf("setval: %w", err)
	}
	return nil
}

// emitOpts builds the [emitOpts] value to thread into every
// emitter helper for this writer's lifetime. Centralised so
// adding a new field (HasPostGIS → +TargetSchema → +EnabledExtensions)
// doesn't fan out across half a dozen call-sites.
func (w *SchemaWriter) emitOpts() emitOpts {
	return emitOpts{
		HasPostGIS:        w.hasPostGIS,
		TargetSchema:      w.qualifyingSchema(),
		EnabledExtensions: w.enabledExtensions,
	}
}

// qualifyingSchema returns the schema name to thread into emitOpts
// for user-defined-type qualification (enums). Returns w.schema only
// when [SetSchema] was called with an operator override
// (`--target-schema NAME`, ADR-0031); otherwise returns "" so the
// emitter falls back to the bare-ident shape that pre-dates Bug 45.
//
// The split protects default-public operators from a behaviour
// change (their CREATE TABLE / column-type idents would suddenly
// emit `"public"."t_c_enum"` everywhere) while still fixing the
// load-bearing case: per-source namespaces that aren't in PG's
// session `search_path` need explicit qualification or CREATE TABLE
// fails with SQLSTATE 42704.
func (w *SchemaWriter) qualifyingSchema() string {
	if !w.schemaExplicit {
		return ""
	}
	return w.schema
}

// ensureSchema emits `CREATE SCHEMA IF NOT EXISTS` for the writer's
// configured schema. No-op for the default `public` schema (always
// exists) and on subsequent calls within the same writer's lifetime
// (schemaEnsured caches the success). Used by
// CreateTablesWithoutConstraints to bootstrap a per-source namespace
// when `--target-schema` (ADR-0031) names a schema that doesn't
// exist yet on a fresh target.
//
// PG identifier-quoting via [quoteIdent] keeps schema names with
// dashes / mixed case round-tripping cleanly through the catalog.
func (w *SchemaWriter) ensureSchema(ctx context.Context) error {
	if w.schemaEnsured || w.schema == "" || w.schema == "public" {
		w.schemaEnsured = true
		return nil
	}
	stmt := "CREATE SCHEMA IF NOT EXISTS " + quoteIdent(w.schema)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("postgres: ensure schema %q: %w", w.schema, err)
	}
	w.schemaEnsured = true
	return nil
}

// orderedTables returns s.Tables sorted alphabetically by name. The
// returned slice is independent of s.Tables.
func orderedTables(s *ir.Schema) []*ir.Table {
	out := append([]*ir.Table(nil), s.Tables...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// PreviewDDL returns every statement [SchemaWriter] would execute on
// s, in execution order, without touching the target database. Used by
// `sluice schema preview` (ADR-0024) to surface the target schema for
// operator inspection before any migration runs. The CREATE TABLE
// statements have their trailing semicolons stripped — the preview
// formatter re-adds them for human readability.
//
// Implementing this on the same struct as [SchemaWriter] keeps the
// schema-emit logic in one place: PreviewDDL routes through the same
// emitTableDef / emitCreateIndex / emitAddForeignKey helpers the
// execute path uses. The trade-off is that PreviewDDL needs the
// hasPostGIS flag — set by the engine's OpenSchemaWriter at construct
// time — so geometry columns render with the right
// `geometry(<subtype>, <srid>)` form. Operators previewing a target
// without PostGIS still see the same loud rejection the actual
// schema-write phase would raise.
func (w *SchemaWriter) PreviewDDL(_ context.Context, s *ir.Schema) ([]ir.DDLStatement, error) {
	if s == nil {
		return nil, errors.New("postgres: PreviewDDL: schema is nil")
	}

	out := make([]ir.DDLStatement, 0, len(s.Tables)*2)
	opts := w.emitOpts()

	// Phase 1a: enum types, in deterministic table+column order.
	// Deduped by resolved type name so a same-engine PG source's
	// shared enum type (catalog Bug 19c) previews one CREATE TYPE,
	// matching the apply path. Generated enum columns are skipped
	// (Bug 25): they emit as TEXT + table-level CHECK in the CREATE
	// TABLE body, so no enum type is needed.
	previewedEnumTypes := map[string]struct{}{}
	for _, table := range orderedTables(s) {
		for _, col := range table.Columns {
			enum, ok := col.Type.(ir.Enum)
			if !ok || col.IsGenerated() {
				continue
			}
			typeName := resolveEnumTypeName(enum, table.Name, col.Name)
			if _, done := previewedEnumTypes[typeName]; done {
				continue
			}
			previewedEnumTypes[typeName] = struct{}{}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "CREATE TYPE",
				SQL:   trimTrailingSemicolon(emitCreateEnumType(enum, w.schema, table.Name, col.Name)),
			})
		}
	}

	// Phase 1b: tables.
	for _, table := range orderedTables(s) {
		stmt, err := emitTableDef(w.schema, table, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, ir.DDLStatement{
			Table: table.Name,
			Kind:  "CREATE TABLE",
			SQL:   trimTrailingSemicolon(stmt),
		})
	}

	// Phase 1c: table / column comments (catalog Bug 19d).
	for _, table := range orderedTables(s) {
		for _, stmt := range emitCommentStatements(w.schema, table) {
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "COMMENT ON",
				SQL:   trimTrailingSemicolon(stmt),
			})
		}
	}

	// Phase 2: indexes.
	for _, table := range orderedTables(s) {
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			stmt, err := emitCreateIndex(w.schema, table.Name, idx, w.emitOpts())
			if err != nil {
				return nil, err
			}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "CREATE INDEX",
				SQL:   trimTrailingSemicolon(stmt),
			})
		}
	}

	// Phase 3: foreign-key constraints.
	for _, table := range orderedTables(s) {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			stmt, err := emitAddForeignKey(w.schema, table.Name, fk)
			if err != nil {
				return nil, err
			}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "ALTER TABLE",
				SQL:   trimTrailingSemicolon(stmt),
			})
		}
	}

	// Phase 4: views. Emitted last so all referenced base tables
	// exist by the time the view is created. Materialized views use
	// the `CREATE MATERIALIZED VIEW ... WITH DATA` shape.
	for _, view := range s.Views {
		if view == nil || view.Name == "" {
			continue
		}
		kind := "CREATE VIEW"
		if view.Materialized {
			kind = "CREATE MATERIALIZED VIEW"
		}
		out = append(out, ir.DDLStatement{
			Table: view.Name,
			Kind:  kind,
			SQL:   trimTrailingSemicolon(emitCreateView(w.schema, view)),
		})
	}

	return out, nil
}

// trimTrailingSemicolon strips trailing whitespace + a single trailing
// semicolon (if present) from s.
//
// Two consumers:
//   - DDL preview formatting: removes the executability-suffix ';' so
//     preview output can re-add it at render time.
//   - emitCreateView (Bug 31): normalizes PG's pg_views.definition /
//     pg_matviews.definition outputs which carry a trailing ';' that
//     breaks the matview `WITH DATA` trailer.
//
// Trims whitespace BOTH before and after the semicolon so trailing
// blank-newlines from catalog-column reads don't leave the result
// looking ragged.
func trimTrailingSemicolon(s string) string {
	s = strings.TrimRight(s, " \t\n\r")
	s = strings.TrimSuffix(s, ";")
	return strings.TrimRight(s, " \t\n\r")
}

// EmitColumnDef satisfies [ir.ColumnDDLPreviewer]. Returns the
// Postgres column-def fragment (`"name" TYPE [NOT NULL] [DEFAULT ...]
// [GENERATED ...]`) suitable for inlining into an `ALTER TABLE ...
// ADD COLUMN` suggestion in the schema-diff renderer (ADR-0029).
//
// The table argument is required for [ir.Enum] columns (PG creates a
// per-column enum type whose name is derived from the table+column
// pair); other IR types accept a nil table. Errors from the
// underlying emitter — e.g. GEOMETRY without PostGIS — surface
// verbatim so the operator sees the same diagnostic the actual write
// path would raise.
func (w *SchemaWriter) EmitColumnDef(_ context.Context, table *ir.Table, col *ir.Column) (string, error) {
	return emitColumnDef(table, col, w.emitOpts())
}

// AlterAddColumn implements [ir.SchemaDeltaApplier] for Postgres. Used
// by Phase 3 chain restore to apply column-add deltas captured on
// incremental manifests against the target. PG supports
// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` since version 9.6, so
// the call is idempotent across re-runs.
//
// The column-def fragment is rendered via the existing emitColumnDef
// (the same helper [EmitColumnDef] uses), so generated columns,
// defaults, NOT NULL, and PostGIS-aware GEOMETRY all flow through
// the established path.
func (w *SchemaWriter) AlterAddColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) error {
	if len(cols) == 0 {
		return nil
	}
	for _, col := range cols {
		def, err := emitColumnDef(table, col, w.emitOpts())
		if err != nil {
			return fmt.Errorf("alter add column: emit %q: %w", col.Name, err)
		}
		schemaName := w.schema
		if table.Schema != "" {
			schemaName = table.Schema
		}
		qualified := quoteIdent(schemaName) + "." + quoteIdent(table.Name)
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", qualified, def)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter add column %q on %s.%s: %w",
				col.Name, table.Schema, table.Name, err)
		}
	}
	return nil
}
