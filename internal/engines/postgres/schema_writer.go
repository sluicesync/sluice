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
	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
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

	// indexBuildParallelism is the operator's `--index-build-parallelism`
	// value (0 = auto), threaded via [SetIndexBuildParallelism] before
	// CreateIndexes (Phase B). When >0 it caps the concurrent index-build
	// worker count verbatim; 0 leaves CreateIndexes to derive a
	// conservative N from the memory + connection budgets (see
	// index_build_tuning.go). N=1 degenerates to exactly the Phase A
	// serial build. The auto path runs regardless; this field only feeds
	// the operator cap into the pure [computeIndexBuildConcurrency]
	// computation.
	indexBuildParallelism int

	// indexBuildBudget is the connection slice the combined copy+index
	// split reserved for the overlapped index-build pool (ADR-0077),
	// threaded via [SetIndexBuildBudget] before
	// [BuildTableIndexesFromChannel] runs. When >0 it REPLACES the index
	// pool's connection self-probe: copy connections are open
	// simultaneously now, so a fresh self-probe would double-count the
	// budget the copy pool already holds. 0 is the sentinel for "not
	// overlapping" — the whole-schema [CreateIndexes] path (no concurrent
	// copy) keeps its self-probe. Only consulted by
	// BuildTableIndexesFromChannel.
	indexBuildBudget int

	// tableIndexedCallback fires once per table after its last secondary
	// index finishes building on the overlap path (ADR-0077), set via
	// [SetTableIndexedCallback]. The pipeline uses it to flip IndexesBuilt.
	// nil (the default, and on the whole-schema CreateIndexes path) is a
	// no-op. May be invoked from any build worker goroutine.
	tableIndexedCallback func(table *ir.Table)
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

// SetIndexBuildParallelism implements [ir.IndexBuildTuner] (Phase B).
// Called by the pipeline orchestrator when the operator passes
// `--index-build-parallelism` (0 = auto). Must be called BEFORE
// [CreateIndexes]; calling it after a run has no effect on that run.
// Negative or zero is the auto sentinel — CreateIndexes derives a
// conservative concurrency from the memory + connection budgets instead.
func (w *SchemaWriter) SetIndexBuildParallelism(n int) {
	if n < 0 {
		n = 0
	}
	w.indexBuildParallelism = n
}

// SetIndexBuildBudget implements [ir.IndexBuildBudgetSetter] (ADR-0077).
// Called by the pipeline orchestrator with the connection slice the
// combined copy+index split reserved for the overlapped index-build pool.
// Must be called BEFORE [BuildTableIndexesFromChannel]. Negative or zero
// is the "not overlapping" sentinel — BuildTableIndexesFromChannel then
// keeps the self-probe (and the whole-schema [CreateIndexes] path is
// unaffected either way; it always self-probes).
func (w *SchemaWriter) SetIndexBuildBudget(connBudget int) {
	if connBudget < 0 {
		connBudget = 0
	}
	w.indexBuildBudget = connBudget
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
			// Bug 154: guard the CREATE TYPE so a resumed/restarted
			// cold-start (interrupted after this CREATE but before the
			// migration committed) re-runs idempotently instead of
			// failing with SQLSTATE 42710 "type already exists" — which
			// otherwise turns every restart into a crash-loop with zero
			// progress. PG has no `CREATE TYPE IF NOT EXISTS`; the
			// DO-block guard (shared with the CDC forward path via
			// guardedCreateEnumType) swallows duplicate_object.
			stmt := guardedCreateEnumType(emitCreateEnumType(enum, w.schema, table.Name, col.Name))
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

// indexBuildJob is one (table, index) unit of work the concurrent
// CreateIndexes worker pool consumes. The table name is carried
// alongside the index because emitCreateIndex needs it and the error
// message names it.
type indexBuildJob struct {
	tableName string
	idx       *ir.Index
}

// CreateIndexes adds every non-PK index across the schema, building them
// with a bounded concurrent worker pool (Phase B).
//
// The secondary indexes are deferred to this phase (after the bulk COPY)
// so they build against an idle target. CreateIndexes computes a worker
// count N bounded by BOTH the target's spare connection budget AND a
// memory budget (each concurrent build consumes its own
// maintenance_work_mem, so total ≈ N × per-build mem — the note's
// "memory × concurrency trap"), plus the index count and an operator
// `--index-build-parallelism` cap. Each of the N workers grabs its OWN
// dedicated connection, raises maintenance_work_mem (the aggregate
// budget DIVIDED across the workers) + max_parallel_maintenance_workers
// on that session, and builds its assigned indexes with plain
// `CREATE INDEX` (not CONCURRENTLY — the target is idle, so the faster
// locking build is correct). N=1 degenerates to exactly the prior serial
// behaviour. See index_build_tuning.go and
// docs/dev/notes/index-build-phase-tuning.md.
//
// Tuning is best-effort, mirroring the synchronous_commit precedent: a
// failed probe or denied SET logs a WARN and the affected worker builds
// untuned — the speedup must never break a working index phase. A
// dedicated-conn open failure or a CREATE INDEX failure IS a hard error:
// the first such error cancels the group so peers unwind, and surfaces.
// IsTransientError reports whether err is a classified storage-grow /
// reparent transient, satisfying [ir.TransientClassifier] (ADR-0114). It
// delegates to the SAME [classifyApplierError] the apply / cold-copy paths
// use, so the post-copy DDL-phase retry (CreateIndexes / CreateConstraints
// / CreateViews / SyncIdentitySequences) recognises a PlanetScale PG
// reparent (57P01/57P03, the read-only serving-transition window, the
// disk-full grow class) identically to the row-write path — no second
// classifier to drift. A non-transient (a real DDL fault) returns false
// and the phase fails loudly, exactly as before.
func (w *SchemaWriter) IsTransientError(err error) bool {
	var re ir.RetriableError
	return errors.As(classifyApplierError(err), &re) && re.Retriable()
}

// Compile-time proof the SchemaWriter exposes the DDL-phase retry verdict
// (ADR-0114) so the orchestrator's post-copy DDL retry engages.
var _ ir.TransientClassifier = (*SchemaWriter)(nil)

func (w *SchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateIndexes: schema is nil")
	}

	jobs := w.indexBuildJobs(s)
	if len(jobs) == 0 {
		return nil
	}

	conc := w.resolveIndexBuildConcurrency(ctx, len(jobs))

	slog.InfoContext(ctx,
		"postgres: building indexes",
		slog.Int("indexes", len(jobs)),
		slog.Int("workers", conc.workers),
		slog.Int64("per_build_maintenance_work_mem_kb", conc.perBuildMemBytes/1024),
		slog.Int("max_parallel_maintenance_workers", conc.parallelMaintenanceWorkers),
		slog.Bool("mem_operator_override", w.indexBuildMemOverride > 0),
		slog.Bool("parallelism_operator_override", w.indexBuildParallelism > 0))

	// N=1 keeps the exact prior serial shape: one dedicated connection,
	// no errgroup, no channel. This is the common case on every plan
	// below PS-640 (the note: parallelism barely helps there) and the
	// degenerate base case the worker pool must reduce to.
	if conc.workers <= 1 {
		return w.buildIndexesOnDedicatedConn(ctx, jobs, conc)
	}

	// Concurrent path: a shared, index-ordered job queue and N workers,
	// each on its own dedicated connection. errgroup's derived ctx
	// cancels every peer on the first hard error so no worker keeps
	// building after a sibling failed; each worker's deferred conn.Close
	// guarantees no connection leaks on the error/panic path.
	jobCh := make(chan indexBuildJob)
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < conc.workers; i++ {
		g.Go(func() error {
			return w.indexBuildWorker(gctx, jobCh, conc)
		})
	}
	// Feed the queue from the parent goroutine. Stop feeding (and surface
	// no feed error) if the group ctx is cancelled by a failing worker.
	g.Go(func() error {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case jobCh <- job:
			case <-gctx.Done():
				return nil
			}
		}
		return nil
	})
	return g.Wait()
}

// indexBuildJobs flattens the whole schema into a deterministically-
// ordered (table, index) work-list: tables alphabetical, indexes
// alphabetical within each table. The ordering is the same one the prior
// serial loop used, so a single-worker run reproduces the prior CREATE
// INDEX sequence exactly. Used by the whole-schema [CreateIndexes].
func (w *SchemaWriter) indexBuildJobs(s *ir.Schema) []indexBuildJob {
	return w.indexBuildJobsForTables(orderedTables(s))
}

// indexBuildJobsForTables flattens a SUBSET of tables into the same
// (table, index) work-list shape indexBuildJobs produces for the whole
// schema. Factored out (ADR-0077) so [BuildTableIndexesFromChannel] can
// queue exactly one completed table's secondary indexes as its copy lands,
// reusing the identical inline-skip + alphabetical-index ordering the
// whole-schema phase uses. Indexes are sorted within each table; the
// caller controls the table order (the channel-driven overlap path queues
// each table as it completes — order is data-arrival, not alphabetical).
func (w *SchemaWriter) indexBuildJobsForTables(tables []*ir.Table) []indexBuildJob {
	var jobs []indexBuildJob
	for _, table := range tables {
		// Bug 125: skip the unique index emitTableDef already promoted
		// inline as a CONSTRAINT for a PK-less table — re-creating it here
		// would fail with "relation already exists". Empty for the common
		// table (PK present or no qualifying unique key).
		skip := inlineSkipIndexNames(table)
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			if _, skipped := skip[idx.Name]; skipped {
				continue
			}
			jobs = append(jobs, indexBuildJob{tableName: table.Name, idx: idx})
		}
	}
	return jobs
}

// inlineSkipIndexNames returns the set of index names emitTableDef
// emits inline at CREATE TABLE time — today, the PK-less COPY unique
// key promoted as a CONSTRAINT (Bug 125 cross-engine symmetry). The
// index-build phases (live [CreateIndexes] and dry-run [PreviewDDL])
// skip these so they aren't re-created (which would raise a
// "relation already exists" error). Empty for the common table.
//
// Mirrors the MySQL engine's helper of the same name. PG has no
// AUTO_INCREMENT-supporting-index case (that entry is MySQL-only), so
// today only the COPY unique key can populate this set.
func inlineSkipIndexNames(table *ir.Table) map[string]struct{} {
	skip := make(map[string]struct{}, 1)
	if inline := inlineUniqueKeyForCopy(table); inline != nil {
		skip[inline.Name] = struct{}{}
	}
	return skip
}

// indexBuildPlan carries the resolved Phase B concurrency decision: the
// worker count, the per-build maintenance_work_mem each worker SETs, and
// the max_parallel_maintenance_workers value (the intra-build parallel
// worker count, shared by every worker). Bundled so the worker functions
// take one value rather than three loose ints.
type indexBuildPlan struct {
	workers                    int
	perBuildMemBytes           int64
	parallelMaintenanceWorkers int
}

// resolveIndexBuildConcurrency probes the target for the memory + worker
// tuning and the spare connection budget, then runs the pure
// [computeIndexBuildConcurrency] to decide the worker count and per-build
// memory. Best-effort throughout: a failed tuning probe degrades to a
// serial, untuned build (N=1, provider-default mem); a failed connection
// probe degrades to serial (N=1) but keeps whatever mem tuning succeeded.
// numJobs is the index count (an upper bound on useful workers).
func (w *SchemaWriter) resolveIndexBuildConcurrency(ctx context.Context, numJobs int) indexBuildPlan {
	// Probe the tuning GUCs on a throwaway pooled query. The per-worker
	// sessions re-derive their own SET values from this same plan; the
	// probe here only needs the numbers, not a dedicated session.
	probe, err := probeIndexBuildTuning(ctx, w.db)
	if err != nil {
		slog.WarnContext(ctx,
			"postgres: index-build tuning probe failed; building indexes serially with provider-default maintenance_work_mem",
			slog.String("error", err.Error()))
		// Serial, untuned: the worker path will see workers=1 and a zero
		// per-build mem (sentinel → leave the session at provider default).
		return indexBuildPlan{workers: 1, perBuildMemBytes: 0, parallelMaintenanceWorkers: 0}
	}

	workers := computeParallelMaintenanceWorkers(probe)
	memBudget := indexBuildMemBudget(probe)
	floor := indexBuildMemFloorFor(probe)

	// Connection budget bounds N alongside memory.
	//
	// ADR-0077: when SetIndexBuildBudget pre-reserved a slice (the overlap
	// path — copy connections are open simultaneously), use that RESERVED
	// value verbatim instead of self-probing. A fresh self-probe here would
	// count the slots the still-running copy pool already holds open as
	// "spare" and double-allocate the budget, blowing past the target's
	// ceiling. Zero means "not overlapping" — the whole-schema CreateIndexes
	// path self-probes as before. A probe failure degrades to serial
	// (connBudget 0 → workers floored at 1) but keeps the mem tuning.
	connBudget := w.indexBuildBudget
	if connBudget < 1 {
		var cbErr error
		connBudget, cbErr = probeIndexBuildConnBudget(ctx, w.db)
		if cbErr != nil {
			slog.WarnContext(ctx,
				"postgres: index-build connection-budget probe failed; building indexes serially",
				slog.String("error", cbErr.Error()))
			connBudget = 0
		}
	}

	conc := computeIndexBuildConcurrency(
		memBudget, floor, w.indexBuildMemOverride,
		connBudget, numJobs, w.indexBuildParallelism,
	)
	return indexBuildPlan{
		workers:                    conc.workers,
		perBuildMemBytes:           conc.perBuildMemBytes,
		parallelMaintenanceWorkers: workers,
	}
}

// buildIndexesOnDedicatedConn is the serial (N=1) path: one dedicated
// connection, tuned once, then every job built in order on it. This is
// the exact prior behaviour and the degenerate base case of the
// concurrent path.
func (w *SchemaWriter) buildIndexesOnDedicatedConn(ctx context.Context, jobs []indexBuildJob, plan indexBuildPlan) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		// Couldn't even grab a dedicated connection from the pool — that
		// is the operator's connectivity problem, not a tuning concern;
		// fail loudly the same as every other connection open.
		return fmt.Errorf("postgres: CreateIndexes: acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	w.tuneIndexBuildConn(ctx, conn, plan)
	return w.buildJobsOn(ctx, conn, jobs)
}

// indexBuildWorker is the concurrent-path worker body: grab a dedicated
// connection, tune it (best-effort), and drain the shared job channel.
// Each worker owns its connection for its whole lifetime so the SET
// session GUCs apply to every CREATE INDEX it runs; the deferred Close
// guarantees the connection is released even if a build fails or the
// group is cancelled.
func (w *SchemaWriter) indexBuildWorker(ctx context.Context, jobCh <-chan indexBuildJob, plan indexBuildPlan) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("postgres: CreateIndexes: acquire worker connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	w.tuneIndexBuildConn(ctx, conn, plan)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-jobCh:
			if !ok {
				return nil
			}
			if err := w.buildOneIndex(ctx, conn, job); err != nil {
				return err
			}
		}
	}
}

// buildJobsOn runs every job in order on conn. Shared by the serial path
// (the concurrent path drains a channel instead so workers interleave).
func (w *SchemaWriter) buildJobsOn(ctx context.Context, conn *sql.Conn, jobs []indexBuildJob) error {
	for _, job := range jobs {
		if err := w.buildOneIndex(ctx, conn, job); err != nil {
			return err
		}
	}
	return nil
}

// buildOneIndex emits and executes the CREATE INDEX for one job on conn.
func (w *SchemaWriter) buildOneIndex(ctx context.Context, conn *sql.Conn, job indexBuildJob) error {
	stmt, err := emitCreateIndex(w.schema, job.tableName, job.idx, w.emitOpts())
	if err != nil {
		return err
	}
	// Empty stmt = emitCreateIndex WARN-skipped a non-portable SQLite index
	// (ADR-0133 follow-up); nothing to execute.
	if stmt == "" {
		return nil
	}
	// Idempotent resume (Bug 131): a resume re-entering phase=indexes over a
	// table indexed in a prior run — or a partially-completed index phase —
	// would otherwise fail with "relation already exists". Promote to the
	// IF NOT EXISTS form (PG 9.5+), the same wrap CreateShapeIndex uses; the
	// first " INDEX " token is the keyword to follow. sluice owns these
	// tables, so a same-named index is the one it built.
	stmt = strings.Replace(stmt, "INDEX ", "INDEX IF NOT EXISTS ", 1)
	// Bug #114 (found live by the v0.99.118 fresh-DB re-validation):
	// `CREATE INDEX IF NOT EXISTS` is NOT atomic against an overlapping
	// same-name creation — its pg_class existence pre-check and the catalog
	// insert race. Under the ADR-0114 whole-phase reparent-retry over the
	// CONCURRENT index pool, a PlanetScale reparent makes a retry's CREATE
	// overlap the prior attempt's just-committed (replicated-to-the-new-
	// primary) build → unique_violation on `pg_class_relname_nsp_index`
	// (23505). That 23505 is correctly classified NON-transient (a user-table
	// dup-key must stay loud), so the reparent-retry layers don't catch it and
	// it aborted the restore with all data already correctly copied. Wrap the
	// exec in the SAME narrow catalog-race retry the CDC apply path uses
	// (retryOnCatalogRace → isCatalogRaceError): it retries ONLY the
	// pg_class/pg_type constraint-name 23505 shape (3× 50/100/200ms), keeping
	// every user-table 23505 loud per ADR-0038. This is the INNER layer (the
	// race resolves in ms); a transient 57P0x still propagates to the outer
	// reparent-retry. buildOneIndex is the single chokepoint for all three
	// index-build entry points (serial dedicated-conn / concurrent CreateIndexes
	// pool / overlap BuildTableIndexesFromChannel), so one wrap covers them all.
	return retryOnCatalogRace(ctx, func() error {
		if err := indexStmtExec(ctx, conn, stmt); err != nil {
			return fmt.Errorf("postgres: create index %q on %q: %w", job.idx.Name, job.tableName, err)
		}
		return nil
	})
}

// indexStmtExec executes a built CREATE INDEX statement on conn. It is a
// package var ONLY so the Bug #114 catalog-race-retry pin can inject a
// failing-then-succeeding exec without a live connection (a regression that
// unwraps buildOneIndex's retryOnCatalogRace must fail a test). Production
// always uses the real ExecContext.
var indexStmtExec = func(ctx context.Context, conn *sql.Conn, stmt string) error {
	_, err := conn.ExecContext(ctx, stmt)
	return err
}

// tuneIndexBuildConn raises maintenance_work_mem + max_parallel_maintenance_workers
// on conn from the resolved plan. Best-effort: every failure path WARNs
// and returns, leaving the session at the provider defaults so the build
// still runs.
//
// A zero plan.perBuildMemBytes is the "probe failed → untuned" sentinel:
// skip the SETs entirely and leave the session at provider defaults.
func (w *SchemaWriter) tuneIndexBuildConn(ctx context.Context, conn *sql.Conn, plan indexBuildPlan) {
	if plan.perBuildMemBytes <= 0 {
		// Probe failed upstream; nothing to apply, build untuned.
		return
	}

	// maintenance_work_mem accepts a unit-suffixed string; PG's smallest
	// unit here is kB, so emit kB to avoid sub-kB truncation surprises.
	memKB := plan.perBuildMemBytes / 1024
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET maintenance_work_mem = '%dkB'", memKB)); err != nil {
		slog.WarnContext(ctx,
			"postgres: SET maintenance_work_mem denied; building indexes with provider-default value",
			slog.Int64("requested_kb", memKB),
			slog.String("error", err.Error()))
		return
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET max_parallel_maintenance_workers = %d", plan.parallelMaintenanceWorkers)); err != nil {
		slog.WarnContext(ctx,
			"postgres: SET max_parallel_maintenance_workers denied; maintenance_work_mem applied, workers left at default",
			slog.Int("requested_workers", plan.parallelMaintenanceWorkers),
			slog.String("error", err.Error()))
		return
	}
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
			// Idempotent resume (Bug 131 same-class): a resume re-entering
			// phase=constraints over an already-added FK would otherwise fail
			// ("constraint ... already exists"). PG has no ADD CONSTRAINT IF
			// NOT EXISTS for FKs, so detect-then-skip via the catalog (mirrors
			// the CREATE INDEX IF NOT EXISTS idempotency). sluice owns these
			// tables, so a same-named FK is the one it built — including one a
			// prior degraded run attached NOT VALID.
			present, err := pgForeignKeyExists(ctx, w.db, w.schema, table.Name, fk.Name)
			if err != nil {
				return fmt.Errorf("postgres: probe foreign key %q on %q: %w", fk.Name, table.Name, err)
			}
			if present {
				continue
			}
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

// pgForeignKeyExists reports whether a FOREIGN KEY constraint named
// constraintName is present on schema.table. Used by CreateConstraints
// for idempotent resume (Bug 131 same-class) — PG has no ADD CONSTRAINT
// IF NOT EXISTS for FKs, so detect-then-skip is the portable pattern.
// Catalog-scoped to (constraint name, table, schema); contype 'f' is the
// FK type, so a same-named CHECK/UNIQUE on the table won't false-match.
func pgForeignKeyExists(ctx context.Context, db *sql.DB, schema, table, constraintName string) (bool, error) {
	const q = `SELECT EXISTS(
		SELECT 1 FROM pg_constraint c
		JOIN pg_class t      ON t.oid = c.conrelid
		JOIN pg_namespace n  ON n.oid = t.relnamespace
		WHERE c.contype = 'f' AND c.conname = $1 AND t.relname = $2 AND n.nspname = $3)`
	var exists bool
	if err := db.QueryRowContext(ctx, q, constraintName, table, schema).Scan(&exists); err != nil {
		return false, fmt.Errorf("pg_constraint probe: %w", err)
	}
	return exists, nil
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
		// Bug 125: skip the unique key emitTableDef promoted inline as a
		// CONSTRAINT for a PK-less table (mirrors the live CreateIndexes
		// skip) — listing it here would show a duplicate CREATE the apply
		// path never runs.
		skip := inlineSkipIndexNames(table)
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			if _, skipped := skip[idx.Name]; skipped {
				continue
			}
			stmt, err := emitCreateIndex(w.schema, table.Name, idx, w.emitOpts())
			if err != nil {
				return nil, err
			}
			// Empty stmt = a non-portable SQLite index was WARN-skipped.
			if stmt == "" {
				continue
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
		//
		// Bug 145: an enum column's def renders the named PG enum type
		// *ident*, not its definition, so the type must exist before the
		// ADD COLUMN or PG raises 42704 "type does not exist". Cold-start
		// creates enum types in a dedicated phase
		// (CreateTablesWithoutConstraints); the forward schema-change path
		// reaches a live target and must create it here (idempotently).
		if err := w.ensureEnumType(ctx, table, col); err != nil {
			return err
		}
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
		// Bug 150: a forwarded DROP COLUMN of an ENUM column leaves the
		// per-column enum type sluice synthesized for it orphaned (PG does NOT
		// cascade DROP COLUMN to a named type), and a later drop-then-readd of
		// a same-named column would silently reuse that stale type via the
		// idempotent ensureEnumType. Drop the type too — AFTER the column drop
		// (so its dependency is gone and a plain RESTRICT DROP TYPE succeeds);
		// IF EXISTS keeps it idempotent. The guard is load-bearing: see
		// [orphanedEnumTypeDrop].
		if dropStmt, ok := w.orphanedEnumTypeDrop(table, col); ok {
			if _, err := w.db.ExecContext(ctx, dropStmt); err != nil {
				return fmt.Errorf("drop orphaned enum type for %s.%s.%s: %w",
					w.schema, table.Name, col.Name, err)
			}
		}
	}
	return nil
}

// orphanedEnumTypeDrop returns the `DROP TYPE IF EXISTS` statement for the
// per-column enum type left behind by a forwarded DROP COLUMN, and whether
// one should be emitted (Bug 150).
//
// It fires ONLY for a sluice-SYNTHESIZED, per-column-dedicated enum type: a
// MySQL (or any non-PG) source has no enum type identity, so the IR carries
// ir.Enum.TypeName == "" and sluice named the target type
// "<table>_<col>_enum" — dedicated to exactly this column, hence safe to drop
// once the column is gone. A same-engine PG source PRESERVES the original type
// name (TypeName != ""), which PG allows to be SHARED across columns/tables
// (catalog Bug 19c) — dropping it on one column's drop could break other
// users, so those are NEVER auto-dropped here (the source's own DROP TYPE, if
// any, is the authority). Schema-qualified with w.schema to match the CREATE
// side (ensureEnumType / emitCreateEnumType); the forward path scrubs
// table.Schema so w.schema is the namespace both the column and the type live
// in.
func (w *SchemaWriter) orphanedEnumTypeDrop(table *ir.Table, col *ir.Column) (string, bool) {
	enum, ok := col.Type.(ir.Enum)
	if !ok || enum.TypeName != "" {
		return "", false
	}
	typeRef := quoteIdent(w.schema) + "." + quoteIdent(resolveEnumTypeName(enum, table.Name, col.Name))
	return "DROP TYPE IF EXISTS " + typeRef, true
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
		// Empty stmt = a non-portable SQLite index was WARN-skipped.
		if stmt == "" {
			continue
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
	// Bug 145: a MySQL `MODIFY col ENUM(...)` that adds a value arrives as
	// an alter-column-type shape, but on PG an enum-definition change is NOT
	// `ALTER COLUMN ... TYPE` (emitColumnType can't render an enum without
	// table+column context — it returns the "requires column context"
	// error) — it's `ALTER TYPE <type> ADD VALUE`. Route enum wants there.
	// Generated enum columns emit as TEXT + CHECK (Bug 25), no named type.
	if enum, isEnum := want.Type.(ir.Enum); isEnum && !want.IsGenerated() {
		return w.alterEnumAddValues(ctx, table, want, enum)
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

// ensureEnumType creates the named PG enum type for an enum column if it
// is not already present (Bug 145). Cold-start creates enum types in a
// dedicated phase (CreateTablesWithoutConstraints, which emits one
// CREATE TYPE per enum in w.schema); the forward schema-change path
// (AlterAddColumn / AlterColumnType) reaches a LIVE target where the type
// may be absent (first ADD COLUMN of an enum) or already present (a
// re-applied delta). PG has no `CREATE TYPE IF NOT EXISTS`, so the CREATE
// is wrapped in a DO block that swallows duplicate_object — idempotent.
//
// Uses w.schema (the same schema cold-start's emitCreateEnumType uses), so
// the created type matches the ident emitColumnDef renders via
// qualifiedEnumTypeRef. Generated enum columns emit as TEXT + a CHECK
// constraint (Bug 25) and need no named type, so they're skipped.
func (w *SchemaWriter) ensureEnumType(ctx context.Context, table *ir.Table, col *ir.Column) error {
	enum, ok := col.Type.(ir.Enum)
	if !ok || col.IsGenerated() {
		return nil
	}
	stmt := guardedCreateEnumType(emitCreateEnumType(enum, w.schema, table.Name, col.Name))
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("ensure enum type for %s.%s.%s: %w", w.schema, table.Name, col.Name, err)
	}
	return nil
}

// guardedCreateEnumType wraps a bare `CREATE TYPE ... AS ENUM` in a DO
// block that swallows SQLSTATE 42710 (duplicate_object), making the
// create idempotent — PG has no `CREATE TYPE IF NOT EXISTS`. Shared by
// cold-start (CreateTablesWithoutConstraints) and the CDC forward path
// (ensureEnumType) so a re-run of either against a target where the
// type already exists is a no-op instead of a hard failure (Bug 154,
// Bug 145). The argument is the statement emitCreateEnumType produces.
func guardedCreateEnumType(create string) string {
	return fmt.Sprintf("DO $$ BEGIN %s EXCEPTION WHEN duplicate_object THEN NULL; END $$;", create)
}

// alterEnumAddValues forwards an enum value-add (Bug 145): ensure the enum
// type exists, then append each value via `ALTER TYPE ... ADD VALUE IF NOT
// EXISTS` (idempotent — values already present are skipped, so a re-applied
// delta is a no-op). This faithfully forwards an APPEND — the common
// MySQL `MODIFY col ENUM(...)` that adds a value at the end.
//
// Documented edge: a value RENAME or REMOVAL on the source leaves the
// target enum a SUPERSET (ALTER TYPE cannot drop or rename a label, and
// this path has only the post-state values, not the diff). No data loss —
// every value the source can still produce remains valid on the target —
// but the target retains the old label. Reordering is likewise not
// reflected (ADD VALUE appends). These are rare for a forwarded change and
// stay loud-safe; the append case (the tested one) is exact.
func (w *SchemaWriter) alterEnumAddValues(ctx context.Context, table *ir.Table, want *ir.Column, enum ir.Enum) error {
	if err := w.ensureEnumType(ctx, table, want); err != nil {
		return err
	}
	typeRef := quoteIdent(w.schema) + "." + quoteIdent(resolveEnumTypeName(enum, table.Name, want.Name))
	for _, v := range enum.Values {
		stmt := fmt.Sprintf("ALTER TYPE %s ADD VALUE IF NOT EXISTS %s", typeRef, quoteSQLString(v))
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter enum type %s add value %q on %s.%s: %w",
				typeRef, v, table.Schema, table.Name, err)
		}
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
