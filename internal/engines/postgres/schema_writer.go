// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

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

	// allowDegradedFKs is the operator's `--allow-degraded-fks` opt-in
	// (ir.DegradedFKAllower / pgcopydb PR #27 / pgcopydb fork review
	// notes). When true, [CreateConstraints] tolerates SQLSTATE 23503
	// from a validating ADD CONSTRAINT by retrying as `NOT VALID` and
	// appending the constraint to degradedFKs. Default off — the
	// loud-failure tenet stays the baseline; operators opt in
	// explicitly when migrating from a known-dirty source.
	allowDegradedFKs bool

	// degradedFKs accumulates the FKs that CreateConstraints attached
	// degraded on the most-recent run. Returned via DegradedFKs() so
	// the pipeline can surface them in the operator-facing report.
	// Reset at the start of each CreateConstraints call.
	degradedFKs []ir.DegradedFK

	// indexBuildMemOverride is the operator's `--index-build-mem` value
	// in bytes (0 = auto), threaded via [SetIndexBuildMem] before
	// CreateIndexes. When >0 it overrides the autotuned
	// maintenance_work_mem for the dedicated index-build session; 0
	// leaves CreateIndexes to derive the value from a pg_settings probe
	// (the dominant Phase-A lever — see index_build_tuning.go). The
	// auto-tuning runs regardless; this field only feeds the override
	// into the pure [computeIndexBuildTuning] computation.
	indexBuildMemOverride int64
}

// SetIndexBuildMem implements [ir.IndexBuildTuner]. Called by the
// pipeline orchestrator when the operator passes `--index-build-mem`
// (a byte size; 0 = auto). Must be called BEFORE [CreateIndexes];
// calling it after a run has no effect on that run. Negative or zero is
// the auto sentinel — CreateIndexes derives maintenance_work_mem from a
// pg_settings probe instead.
func (w *SchemaWriter) SetIndexBuildMem(bytes int64) {
	if bytes < 0 {
		bytes = 0
	}
	w.indexBuildMemOverride = bytes
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

	// Phase 1a': DOMAIN types (Bug 113 round-trip carry, v0.95.2).
	// Walk every column for an ir.Domain and emit CREATE DOMAIN
	// before any table that references it. Dedupe by domain Name —
	// two columns referencing the same source DOMAIN resolve to the
	// same name, so we emit `CREATE DOMAIN` exactly once. Phase 1a
	// (enum types) runs first because enums are independent and
	// DOMAINs depend on their base type but never on an enum. Phase
	// 1b (tables) runs after this so column references to the
	// DOMAIN name resolve to the just-created type.
	opts := w.emitOpts()
	createdDomainTypes := map[string]struct{}{}
	for _, table := range orderedTables(s) {
		for _, col := range table.Columns {
			dom, ok := col.Type.(ir.Domain)
			if !ok {
				continue
			}
			if _, done := createdDomainTypes[dom.Name]; done {
				continue
			}
			createdDomainTypes[dom.Name] = struct{}{}
			stmt, err := emitCreateDomainType(dom, w.schema, opts)
			if err != nil {
				return fmt.Errorf("postgres: emit domain type for %s.%s: %w", table.Name, col.Name, err)
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: create domain type for %s.%s: %w", table.Name, col.Name, err)
			}
		}
	}

	// Phase 1b: tables. opts already constructed for Phase 1a' above.
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

	// Phase 1d: row-level security (ADR-0063 — task #52 sub-deliverables
	// 2 + 3). For each table the source had RLS on, emit
	// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` (and `FORCE` when
	// the source had it), then `CREATE POLICY` for each captured
	// policy. ENABLE MUST precede CREATE POLICY: without ENABLE the
	// policies are defined but inert, which is the subtle-silent-
	// security-regression class the ADR exists to close. Empty
	// Policies + RLSEnabled=false (the common case on most schemas)
	// is a no-op.
	for _, table := range orderedTables(s) {
		for _, stmt := range emitRLSStatements(w.schema, table) {
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: apply RLS on %q: %w", table.Name, err)
			}
		}
	}
	return nil
}

// CreateIndexes adds every non-PK index across the schema.
//
// The secondary indexes are deferred to this phase (after the bulk
// COPY) so they build against an idle target. CreateIndexes grabs one
// dedicated connection from the pool, probes pg_settings, and raises
// maintenance_work_mem + max_parallel_maintenance_workers on that
// session before running the serial CREATE INDEX loop *on the same
// connection* — pooled w.db.ExecContext would land each statement on an
// arbitrary connection that doesn't carry the SET. maintenance_work_mem
// is the dominant index-build lever (in-memory sort vs external-merge);
// see index_build_tuning.go and docs/dev/notes/index-build-phase-tuning.md.
//
// The tuning is best-effort, mirroring the synchronous_commit precedent
// (change_applier.go): if the dedicated-conn grab, the probe, or a SET
// fails (permissions / managed-PG quirk), CreateIndexes logs a WARN and
// proceeds with the build untuned — the speedup must never be the thing
// that breaks a working index phase. A pooled-conn open failure or a
// CREATE INDEX failure is still a hard error.
func (w *SchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateIndexes: schema is nil")
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		// Couldn't even grab a dedicated connection from the pool — that
		// is the operator's connectivity problem, not a tuning concern;
		// fail loudly the same as every other connection open.
		return fmt.Errorf("postgres: CreateIndexes: acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	w.tuneIndexBuildSession(ctx, conn)

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
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: create index %q on %q: %w", idx.Name, table.Name, err)
			}
		}
	}
	return nil
}

// tuneIndexBuildSession probes pg_settings on conn and raises
// maintenance_work_mem + max_parallel_maintenance_workers for the index
// build. Best-effort: every failure path WARNs and returns, leaving the
// session at the provider defaults so the build still runs. Logs the
// values actually applied (INFO) on success so an operator can confirm
// the tuning took.
func (w *SchemaWriter) tuneIndexBuildSession(ctx context.Context, conn *sql.Conn) {
	probe, err := probeIndexBuildTuning(ctx, conn)
	if err != nil {
		slog.WarnContext(ctx,
			"postgres: index-build tuning probe failed; building indexes with provider-default maintenance_work_mem",
			slog.String("error", err.Error()))
		return
	}

	memBytes, workers := computeIndexBuildTuning(probe, w.indexBuildMemOverride)

	// maintenance_work_mem accepts a unit-suffixed string; PG's smallest
	// unit here is kB, so emit kB to avoid sub-kB truncation surprises.
	memKB := memBytes / 1024
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET maintenance_work_mem = '%dkB'", memKB)); err != nil {
		slog.WarnContext(ctx,
			"postgres: SET maintenance_work_mem denied; building indexes with provider-default value",
			slog.Int64("requested_kb", memKB),
			slog.String("error", err.Error()))
		return
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET max_parallel_maintenance_workers = %d", workers)); err != nil {
		slog.WarnContext(ctx,
			"postgres: SET max_parallel_maintenance_workers denied; maintenance_work_mem applied, workers left at default",
			slog.Int("requested_workers", workers),
			slog.String("error", err.Error()))
		return
	}

	slog.InfoContext(ctx,
		"postgres: index-build session tuned",
		slog.Int64("maintenance_work_mem_kb", memKB),
		slog.Int("max_parallel_maintenance_workers", workers),
		slog.Bool("operator_override", w.indexBuildMemOverride > 0),
		slog.Int64("probe_shared_buffers_bytes", probe.sharedBuffersBytes),
		slog.Int64("probe_maintenance_work_mem_bytes", probe.maintenanceWorkMemBytes),
		slog.Int("probe_max_worker_processes", probe.maxWorkerProcesses))
}

// CreateConstraints adds every foreign-key constraint across the
// schema. All referenced tables must already exist.
//
// When [EnableDegradedFKs] has been called (the operator opted into
// `--allow-degraded-fks`), SQLSTATE 23503 from the validating
// `ADD CONSTRAINT` triggers a one-shot retry of the same DDL with a
// trailing `NOT VALID` clause. PG attaches the constraint on the
// catalog without scanning existing rows; new rows are still rejected
// by the FK on write. The operator runs `ALTER TABLE ... VALIDATE
// CONSTRAINT <name>` after cleaning the orphan rows. The degraded
// constraint is appended to the writer's degradedFKs slice with an
// actionable Hint string; the pipeline surfaces the list to the
// operator after the constraints phase completes.
//
// Resets degradedFKs on entry so repeat invocations of the same
// writer don't carry residual state.
func (w *SchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateConstraints: schema is nil")
	}
	w.degradedFKs = nil
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
			_, execErr := w.db.ExecContext(ctx, stmt)
			if execErr == nil {
				continue
			}
			if !w.allowDegradedFKs || !isFKViolation(execErr) {
				return fmt.Errorf("postgres: add foreign key %q on %q: %w", fk.Name, table.Name, execErr)
			}
			// Operator opted into degraded FKs and PG reported 23503
			// (orphan rows on the child). Retry as NOT VALID — PG
			// records the constraint on the catalog without rescanning
			// existing rows; the operator validates later after fixing
			// the orphans (see Hint below).
			notValidStmt := appendNotValid(stmt)
			if _, retryErr := w.db.ExecContext(ctx, notValidStmt); retryErr != nil {
				return fmt.Errorf("postgres: add foreign key %q on %q (NOT VALID retry after %w): %w",
					fk.Name, table.Name, execErr, retryErr)
			}
			refTable := fk.ReferencedTable
			w.degradedFKs = append(w.degradedFKs, ir.DegradedFK{
				Schema:            w.schema,
				Table:             table.Name,
				ConstraintName:    fk.Name,
				LocalColumns:      append([]string(nil), fk.Columns...),
				ReferencedTable:   refTable,
				ReferencedColumns: append([]string(nil), fk.ReferencedColumns...),
				Reason:            execErr.Error(),
				Hint: fmt.Sprintf(
					"FK attached as NOT VALID due to orphan rows on the child table. "+
						"After fixing the orphan rows, run: "+
						"ALTER TABLE %s.%s VALIDATE CONSTRAINT %s;",
					quoteIdent(w.schema), quoteIdent(table.Name), quoteIdent(fk.Name),
				),
			})
		}
	}
	return nil
}

// EnableDegradedFKs implements [ir.DegradedFKAllower]. Called by the
// pipeline orchestrator when the operator passes `--allow-degraded-fks`.
// Must be called BEFORE [CreateConstraints]; calling it after a run
// has no effect on that run's behaviour.
func (w *SchemaWriter) EnableDegradedFKs() { w.allowDegradedFKs = true }

// DegradedFKs implements [ir.DegradedFKReporter]. Returns the list of
// FKs that the most-recent [CreateConstraints] attached degraded —
// nil/empty when either the feature wasn't enabled or every FK
// validated cleanly. The slice is a shallow copy; mutating the
// returned entries does not affect the writer's internal state, but
// don't rely on the slice header being independent (it is).
func (w *SchemaWriter) DegradedFKs() []ir.DegradedFK {
	if len(w.degradedFKs) == 0 {
		return nil
	}
	out := make([]ir.DegradedFK, len(w.degradedFKs))
	copy(out, w.degradedFKs)
	return out
}

// isFKViolation reports whether err is the SQLSTATE 23503 (foreign
// key violation) that PG returns when a validating `ADD CONSTRAINT
// ... FOREIGN KEY` finds at least one orphan row on the child table.
// The class also covers some other 23503 cases (inserts that would
// orphan a row); within the constraints phase, 23503 is unambiguously
// the validation-on-add case.
func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

// appendNotValid takes the SQL produced by [emitAddForeignKey] (which
// ends in a trailing `;`) and rewrites it to `... NOT VALID;`. Kept as
// a tiny helper rather than threading a bool through emitAddForeignKey
// to keep the existing emitter's call shape stable across the test
// surface and the rest of the writer.
func appendNotValid(stmt string) string {
	return strings.TrimSuffix(stmt, ";") + " NOT VALID;"
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
	// pg_get_serial_sequence parses arg 1 as an identifier text → regclass;
	// per SQL rules unquoted identifiers fold to lowercase. We must
	// quote both schema and table so case-preserved names ("Widgets")
	// resolve to the actual relation (Bug 87 / task #42 regression pin).
	tableArg := quoteIdent(w.schema) + "." + quoteIdent(table.Name)
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

	// Phase 1d: row-level security (ADR-0063). Same order as the
	// apply path: ALTER ... ENABLE / FORCE before any CREATE POLICY.
	for _, table := range orderedTables(s) {
		for _, stmt := range emitRLSStatements(w.schema, table) {
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  rlsStatementKind(stmt),
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
		// Bug 83 v0.73.1 — force Nullable=true on the emitted ADD COLUMN
		// regardless of the IR's Nullable flag. Two reasons:
		//
		//   1. Source-of-IR fidelity: pgoutput's RelationMessage (the
		//      wire shape the CDC reader projects into the post-DDL IR)
		//      does NOT carry pg_attribute.attnotnull, so every column
		//      in the CDC-projected IR has Nullable=false by zero-value
		//      default. Emitting that as `ADD COLUMN ... NOT NULL` on a
		//      non-empty target raises SQLSTATE 23502 — the exact
		//      failure shape of Bug 83 v0.73.1's PG integration pin.
		//
		//   2. v1 trade-off: target columns added via Phase 2 live
		//      cross-shard coordination land nullable. Operators who
		//      need NOT NULL on the target can `ALTER COLUMN SET NOT
		//      NULL` post-apply (Phase 2 of the consolidation flow is
		//      idempotent — re-running with a tightened nullability
		//      will land the constraint once the existing nulls are
		//      backfilled).
		//
		// Documented in CHANGELOG v0.73.1.
		emitCol := *col
		emitCol.Nullable = true
		def, err := emitColumnDef(table, &emitCol, w.emitOpts())
		if err != nil {
			return fmt.Errorf("alter add column: emit %q: %w", col.Name, err)
		}
		qualified := w.qualifyTable(table)
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", qualified, def)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter add column %q on %s.%s: %w",
				col.Name, table.Schema, table.Name, err)
		}
	}
	return nil
}

// qualifyTable returns the schema-qualified, quoted table reference
// for table. Shared across the ADR-0054 Phase 2c ShapeDeltaApplier
// methods so quoting + schema-override stay consistent.
func (w *SchemaWriter) qualifyTable(table *ir.Table) string {
	schemaName := w.schema
	if table.Schema != "" {
		schemaName = table.Schema
	}
	return quoteIdent(schemaName) + "." + quoteIdent(table.Name)
}

// AlterDropColumn implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0054 Phase 2c). PG supports `DROP COLUMN IF EXISTS` since
// v9.0; idempotent across re-runs.
func (w *SchemaWriter) AlterDropColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) error {
	if len(cols) == 0 {
		return nil
	}
	qualified := w.qualifyTable(table)
	for _, col := range cols {
		stmt := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", qualified, quoteIdent(col.Name))
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter drop column %q on %s.%s: %w",
				col.Name, table.Schema, table.Name, err)
		}
	}
	return nil
}

// CreateShapeIndex implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0054 Phase 2c). Reuses the existing emitCreateIndex emitter
// (the same helper CREATE TABLE / CreateIndexes uses) so every
// supported index variant (UNIQUE, USING <method>, INCLUDE,
// functional indexes) flows through one path. The emitter doesn't
// emit IF NOT EXISTS; this method wraps with the idempotent form
// (PG 9.5+).
func (w *SchemaWriter) CreateShapeIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) error {
	if len(indexes) == 0 {
		return nil
	}
	for _, idx := range indexes {
		if idx == nil || strings.EqualFold(idx.Name, "PRIMARY") {
			continue
		}
		stmt, err := emitCreateIndex(w.schema, table.Name, idx, w.emitOpts())
		if err != nil {
			return fmt.Errorf("create shape index: emit %q: %w", idx.Name, err)
		}
		// Promote bare `CREATE [UNIQUE] INDEX <name>` to idempotent
		// form. The first " INDEX " token is the keyword we want to
		// follow with IF NOT EXISTS.
		idempotent := strings.Replace(stmt, "INDEX ", "INDEX IF NOT EXISTS ", 1)
		if _, err := w.db.ExecContext(ctx, idempotent); err != nil {
			return fmt.Errorf("create shape index %q on %s.%s: %w",
				idx.Name, table.Schema, table.Name, err)
		}
	}
	return nil
}

// DropShapeIndex implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0054 Phase 2c). Uses `DROP INDEX IF EXISTS` (PG 8.2+). Index
// names are schema-scoped on PG; the pgIndexName prefix convention
// applies (matches emitCreateIndex).
func (w *SchemaWriter) DropShapeIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) error {
	if len(indexes) == 0 {
		return nil
	}
	schemaName := w.schema
	if table.Schema != "" {
		schemaName = table.Schema
	}
	for _, idx := range indexes {
		if idx == nil || strings.EqualFold(idx.Name, "PRIMARY") {
			continue
		}
		indexRef := quoteIdent(schemaName) + "." + quoteIdent(pgIndexName(table.Name, idx.Name))
		stmt := "DROP INDEX IF EXISTS " + indexRef
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("drop shape index %q on %s.%s: %w",
				idx.Name, table.Schema, table.Name, err)
		}
	}
	return nil
}

// AlterColumnType implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0054 Phase 2c). Idempotency on the post-state is by inherent
// PG behaviour: a same-type ALTER is a fast no-op (PG short-circuits
// when the catalog already matches).
//
// USING clause is intentionally NOT emitted — same-engine widening
// (INT → BIGINT, VARCHAR(10) → VARCHAR(20)) doesn't need it. Lossy
// or cross-format conversions surface as PG errors → loud-failure
// recovery (drained model).
func (w *SchemaWriter) AlterColumnType(ctx context.Context, table *ir.Table, want *ir.Column) error {
	if want == nil {
		return errors.New("postgres: alter column type: want column is nil")
	}
	typeStr, err := emitColumnType(want.Type, w.emitOpts())
	if err != nil {
		return fmt.Errorf("alter column type: emit %q: %w", want.Name, err)
	}
	qualified := w.qualifyTable(table)
	stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
		qualified, quoteIdent(want.Name), typeStr)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter column type %q on %s.%s: %w",
			want.Name, table.Schema, table.Name, err)
	}
	return nil
}

// AlterColumnNullability implements [ir.ShapeDeltaApplier] for
// Postgres (ADR-0054 Phase 2c). Emits SET / DROP NOT NULL based on
// want.Nullable. Idempotent on the post-state.
func (w *SchemaWriter) AlterColumnNullability(ctx context.Context, table *ir.Table, want *ir.Column) error {
	if want == nil {
		return errors.New("postgres: alter column nullability: want column is nil")
	}
	qualified := w.qualifyTable(table)
	action := "SET NOT NULL"
	if want.Nullable {
		action = "DROP NOT NULL"
	}
	stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s",
		qualified, quoteIdent(want.Name), action)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter column nullability %q on %s.%s: %w",
			want.Name, table.Schema, table.Name, err)
	}
	return nil
}

// AlterRenameColumn implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0054 v0.78.0 — task #22 RENAME COLUMN sub-task). PG
// `ALTER TABLE ... RENAME COLUMN <old> TO <new>` preserves type,
// nullability, default, comment, identity, and collation; only the
// name changes.
//
// Idempotency on the post-state: detect-then-RENAME via
// information_schema.columns. PG does not support
// `RENAME COLUMN IF EXISTS`, so we probe both names first:
//
//   - newName already present + oldName absent → no-op (post-state).
//   - oldName present + newName absent → emit RENAME.
//   - Both absent OR both present → return error (the takeover
//     stream's probe path catches this branch as Inconsistent;
//     direct apply path surfaces an operator-actionable refusal).
func (w *SchemaWriter) AlterRenameColumn(ctx context.Context, table *ir.Table, oldName, newName string) error {
	if oldName == "" || newName == "" {
		return errors.New("postgres: alter rename column: oldName and newName must be non-empty")
	}
	if oldName == newName {
		return nil
	}
	schemaName := w.schema
	if table.Schema != "" {
		schemaName = table.Schema
	}
	oldPresent, newPresent, err := pgColumnPairPresence(ctx, w.db, schemaName, table.Name, oldName, newName)
	if err != nil {
		return fmt.Errorf("alter rename column: probe %s.%s.(%s,%s): %w",
			schemaName, table.Name, oldName, newName, err)
	}
	switch {
	case !oldPresent && newPresent:
		// Idempotent post-state.
		return nil
	case oldPresent && !newPresent:
		// Apply.
	case oldPresent && newPresent:
		return fmt.Errorf(
			"postgres: alter rename column %s.%s: both %q and %q exist — "+
				"target schema cannot be reconciled without operator intervention",
			schemaName, table.Name, oldName, newName,
		)
	default: // !oldPresent && !newPresent
		return fmt.Errorf(
			"postgres: alter rename column %s.%s: neither %q nor %q exists",
			schemaName, table.Name, oldName, newName,
		)
	}
	qualified := w.qualifyTable(table)
	stmt := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		qualified, quoteIdent(oldName), quoteIdent(newName))
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter rename column %s.%s.%s -> %s: %w",
			schemaName, table.Name, oldName, newName, err)
	}
	return nil
}

// pgColumnPairPresence returns whether oldName and newName each
// exist in information_schema.columns for the named table. One
// query for both names so the probe is a single round-trip.
func pgColumnPairPresence(ctx context.Context, db sqlExecQueryer, schemaName, tableName, oldName, newName string) (oldPresent, newPresent bool, err error) {
	const q = `SELECT column_name FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = ANY($3)`
	rows, err := db.QueryContext(ctx, q, schemaName, tableName, []string{oldName, newName})
	if err != nil {
		return false, false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			return false, false, scanErr
		}
		switch name {
		case oldName:
			oldPresent = true
		case newName:
			newPresent = true
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return false, false, rowsErr
	}
	return oldPresent, newPresent, nil
}

// sqlExecQueryer is the narrow surface pgColumnPairPresence needs
// from *sql.DB / *sql.Tx — kept private to this file so the helper
// stays focused.
type sqlExecQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
