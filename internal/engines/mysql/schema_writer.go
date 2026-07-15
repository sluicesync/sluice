// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// SchemaWriter applies an IR Schema to a MySQL database in three
// phases. The phases are deliberately separate so that the bulk-load
// step (between phases 1 and 2) can run against tables without indexes
// or constraints — typically several times faster than loading into a
// fully-constrained schema.
//
//	phase 1: CreateTablesWithoutConstraints
//	         CREATE TABLE for every table, columns + PRIMARY KEY only.
//
//	(bulk-load step happens here, outside the SchemaWriter)
//
//	phase 2: CreateIndexes
//	         ALTER TABLE ADD INDEX for every non-PRIMARY index.
//
//	phase 3: CreateConstraints
//	         ALTER TABLE ADD CONSTRAINT for every foreign key.
//
// SchemaWriter holds an open *sql.DB; callers should call Close when
// finished to release the connection pool.
type SchemaWriter struct {
	db     *sql.DB
	schema string

	// emitter carries this writer's resolved DDL-emit policy — the sql_mode
	// backslash-escaping rule for SQL string literals (task 2.5). Built once at
	// [Engine.OpenSchemaWriter] from the engine's --mysql-sql-mode override, so
	// every string literal this writer emits is escaped for the SAME sql_mode its
	// connections run under. The zero value (backslashEscapes=false) is NOT used:
	// OpenSchemaWriter always sets it via [newMySQLEmitter]; a bare-struct test
	// writer that needs the strict default sets emitter: stdEmitter explicitly.
	emitter mysqlEmitter

	// flavor records which MySQL-compatible service this writer targets,
	// threaded from the engine at OpenSchemaWriter. The overlapped
	// index-build path (ADR-0080) gates on it: PlanetScale/Vitess targets
	// (flavor.usesVStream()) DECLINE the overlap and fall back to the
	// post-copy whole-schema CreateIndexes, because concurrent
	// `ALTER … ADD INDEX` fights their online-DDL / Safe-Migrations queue.
	// The zero value (FlavorVanilla) runs the overlap, matching the
	// engine's "Engine{} behaves as vanilla MySQL" convention.
	flavor Flavor

	// indexBuildBudget is the connection slice the combined copy+index
	// split reserved for the overlapped index-build pool (ADR-0077/0080),
	// threaded via [SetIndexBuildBudget] before
	// [BuildTableIndexesFromChannel] runs. Stored for surface symmetry
	// with the PG writer's [ir.IndexBuildBudgetSetter], but NOT used to
	// size the MySQL index pool: MySQL implements no connection-slot
	// prober, so the orchestrator always hands it 0 (ADR-0080). The pool
	// sizes itself from the fixed-N policy in [resolveIndexBuildWorkers].
	indexBuildBudget int

	// tableIndexedCallback fires once per table after its last secondary
	// index finishes building on the overlap path (ADR-0080), set via
	// [SetTableIndexedCallback]. The pipeline uses it to flip IndexesBuilt
	// so a resume skips an already-indexed table. nil (the default, and on
	// the whole-schema CreateIndexes path) is a no-op. May be invoked from
	// any build worker goroutine.
	tableIndexedCallback func(table *ir.Table)

	// indexBuildFallback is the optional out-of-band index-build channel
	// (ADR-0148: the PlanetScale deploy-request fallback for the errno-3024
	// statement-time wall / errno-1105 safe-migrations direct-DDL block),
	// threaded via [SetIndexBuildFallback] before any index phase runs. nil
	// (the zero value every non-CLI construction gets) keeps the direct
	// ALTER path byte-identical to before — the fallback only ever ADDS a
	// recovery for a build that would otherwise fail. Consulted by the two
	// deferred-build paths a PlanetScale target takes: the whole-schema
	// [CreateIndexes] and the VStream serial overlap
	// ([buildEachAsCopiedSerial]); see schema_writer_index_fallback.go.
	indexBuildFallback ir.IndexBuildFallback

	// rlsWarnOnce gates the cross-engine PG → MySQL RLS-drop WARN
	// (ADR-0063 — task #52 sub-deliverable 3). A PG source carrying
	// any RLS-enabled table or attached policy logs a single WARN
	// line per writer lifetime (per stream) — fires once and never
	// again, regardless of how many tables carry RLS state. MySQL
	// has no RLS equivalent, so this is a documented "policy layer
	// silently dropped" carve-out the operator accepts by choosing
	// the PG → MySQL direction. Same-engine MySQL → PG passes
	// through cleanly (MySQL sources never populate the RLS fields).
	rlsWarnOnce sync.Once

	// inlineCheckSupported records whether the open MySQL server is
	// at least 8.0.16 — the version that added enforced CHECK
	// constraints. v0.97.0's PG → MySQL DOMAIN-CHECK inline translator
	// gates its emission on this flag: on supported servers a
	// translatable DOMAIN CHECK becomes a MySQL table-level CHECK on
	// CREATE TABLE; on older servers it falls through to the v0.96.2
	// WARN-only path. Probed once at writer open; zero value (false)
	// is the safe default — no inline CHECK is emitted, no regression
	// from pre-v0.97.0 behavior.
	inlineCheckSupported bool

	// domainWarnOnce gates the cross-engine PG → MySQL DOMAIN-CHECK
	// silent-downgrade WARN (residual carry-over from v0.95.x Bug 113
	// round-trip closure). When a PG source has a column typed as a
	// DOMAIN with attached CHECK constraints, the MySQL writer
	// downgrades the column to the DOMAIN's base type (MySQL has no
	// DOMAIN counterpart) — the column shape is preserved but the
	// CHECK constraint is dropped. Without a WARN this is a silent
	// CHECK-loss class on the cross-engine path (same family as the
	// original Bug 113). One WARN per writer lifetime carries the
	// list of affected (column, source_domain, target_base_type)
	// tuples plus the count of dropped CHECK constraints. Same-engine
	// PG → PG is unaffected (Bug 113 round-trip carry handles it);
	// MySQL-source schemas never populate ir.Domain so MySQL → PG /
	// MySQL → MySQL pass through cleanly.
	domainWarnOnce sync.Once
}

// Close releases the underlying connection pool.
func (w *SchemaWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// CreateTablesWithoutConstraints emits CREATE TABLE for every table
// in s in deterministic (alphabetical) order. The PRIMARY KEY is
// included inline; secondary indexes and foreign keys are deferred to
// later phases.
func (w *SchemaWriter) CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: CreateTablesWithoutConstraints: schema is nil")
	}
	w.maybeWarnRLSDrop(ctx, s)
	w.maybeWarnDomainCheckDrop(ctx, s)
	for _, table := range orderedTables(s) {
		stmt, err := w.emitter.emitTableDefWithDomainChecks(table, w.inlineCheckSupported)
		if err != nil {
			return err
		}
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: create table %q: %w", table.Name, wrapDDLError(err))
		}
	}
	return nil
}

// maybeWarnRLSDrop logs a single WARN per writer lifetime when the
// incoming schema carries any RLS-enabled table or attached policy
// (ADR-0063 — task #52 sub-deliverable 3). MySQL has no RLS surface,
// so policies and per-table ENABLE/FORCE flags drop silently
// otherwise — the WARN makes the cross-engine carve-out visible so
// operators routing PG → MySQL aren't surprised that their tenant
// filters didn't carry to the target.
//
// One WARN per stream regardless of table count: the sync.Once on
// the writer is the right scope. Per-table WARNs would flood logs
// for any non-trivial multi-tenant schema; the operator-visible fact
// is "policies are dropped on this run," not "policies are dropped
// on table X" (the latter would imply per-table opt-in which the
// engine doesn't provide).
//
// MySQL → PG never hits this path: the MySQL SchemaReader leaves
// RLSEnabled / RLSForced false and Policies empty by construction.
// The (unit-tested) MySQL-source-no-op shape is the symmetric green
// path for the cross-engine carve-out.
func (w *SchemaWriter) maybeWarnRLSDrop(ctx context.Context, s *ir.Schema) {
	if s == nil {
		return
	}
	var affected []string
	for _, t := range s.Tables {
		if t == nil {
			continue
		}
		if t.RLSEnabled || t.RLSForced || len(t.Policies) > 0 {
			affected = append(affected, t.Name)
		}
	}
	if len(affected) == 0 {
		return
	}
	w.rlsWarnOnce.Do(func() {
		slog.WarnContext(
			ctx,
			"mysql: PG → MySQL drops row-level security; "+
				"tables with policies will land on MySQL without per-tenant filters",
			slog.Int("affected_table_count", len(affected)),
			slog.String("affected_tables", strings.Join(affected, ",")),
			slog.String("hint", "MySQL has no RLS equivalent — operators routing PG → MySQL "+
				"accept the policy-layer drop, or re-target to PG (ADR-0063)"),
		)
	})
}

// maybeWarnDomainCheckDrop logs a single WARN per writer lifetime
// when the incoming schema carries any column typed as ir.Domain
// (residual carry-over from v0.95.x Bug 113 round-trip closure).
// MySQL has no DOMAIN counterpart; the writer's emitColumnType
// downgrades the column to the DOMAIN's base type (preserves the
// column shape) but the DOMAIN's CHECK constraints attached to the
// type are NOT re-emitted as MySQL table-level CHECKs. Without this
// WARN that's a silent CHECK-loss class on the cross-engine path —
// same family as the original Bug 113 silent-constraint-loss class.
//
// One WARN per writer lifetime carries the comma-joined list of
// affected columns (in "table.column" form), the parallel list of
// source DOMAIN names, the parallel list of target MySQL base types,
// and the count of dropped CHECK constraints. The structured fields
// are machine-parseable for operators piping slog JSON.
//
// MySQL → PG never hits this path: MySQL has no DOMAIN, so the MySQL
// SchemaReader never populates ir.Domain. Same-engine PG → PG is
// handled by the Bug 113 round-trip carry (Phase 1a' CREATE DOMAIN
// emission); only the PG → MySQL cross-engine path needs the WARN.
func (w *SchemaWriter) maybeWarnDomainCheckDrop(ctx context.Context, s *ir.Schema) {
	if s == nil {
		return
	}
	type affectedCol struct {
		table, column, domain, baseType string
		droppedChecks                   int
	}
	var affected []affectedCol
	totalDropped := 0
	for _, t := range s.Tables {
		if t == nil {
			continue
		}
		for _, c := range t.Columns {
			if c == nil {
				continue
			}
			dom, ok := c.Type.(ir.Domain)
			if !ok {
				continue
			}
			// v0.97.0: when the target MySQL supports inline CHECK
			// (8.0.16+) AND every attached DomainCheck translates, the
			// column's CHECKs are preserved verbatim on the dst — no
			// silent-CHECK-drop class to warn about. A column reaches
			// this branch only when at least one CHECK didn't translate,
			// when the version doesn't support CHECK at all, OR when
			// the DOMAIN has zero CHECKs (the column type was just a
			// type alias whose name doesn't carry to MySQL — still a
			// shape difference worth flagging).
			dropped := 0
			for _, chk := range dom.Checks {
				if !w.inlineCheckSupported {
					dropped++
					continue
				}
				if _, ok := translateDomainCheckToMySQL(c.Name, chk); !ok {
					dropped++
				}
			}
			// Column carries no silent-loss risk when it has at least
			// one CHECK AND all CHECKs translated AND emit-time inlined
			// them. Skip the WARN for that column.
			if len(dom.Checks) > 0 && dropped == 0 {
				continue
			}
			baseSpelling := dom.Name
			if dom.BaseType != nil {
				if spelled, err := w.emitter.emitColumnType(dom.BaseType); err == nil {
					baseSpelling = spelled
				}
			}
			affected = append(affected, affectedCol{
				table:         t.Name,
				column:        c.Name,
				domain:        dom.Name,
				baseType:      baseSpelling,
				droppedChecks: dropped,
			})
			totalDropped += dropped
		}
	}
	if len(affected) == 0 {
		return
	}
	cols := make([]string, len(affected))
	domains := make([]string, len(affected))
	baseTypes := make([]string, len(affected))
	for i, a := range affected {
		cols[i] = a.table + "." + a.column
		domains[i] = a.domain
		baseTypes[i] = a.baseType
	}
	w.domainWarnOnce.Do(func() {
		slog.WarnContext(
			ctx,
			"mysql: PG → MySQL downgrades DOMAIN-typed columns to their base type; "+
				"DOMAIN CHECK constraints are NOT re-emitted as MySQL table-level CHECKs",
			slog.Int("affected_column_count", len(affected)),
			slog.String("affected_columns", strings.Join(cols, ",")),
			slog.String("source_domains", strings.Join(domains, ",")),
			slog.String("target_base_types", strings.Join(baseTypes, ",")),
			slog.Int("check_constraint_dropped", totalDropped),
			slog.String("hint", "MySQL has no DOMAIN equivalent; "+
				"add a MySQL table-level CHECK (MySQL 8.0.16+) manually if input validation matters, "+
				"or re-target to PG to preserve the DOMAIN"),
		)
	})
}

// IsTransientError reports whether err is a classified storage-grow /
// reparent transient, satisfying [ir.TransientClassifier] (ADR-0114). It
// delegates to the SAME [classifyApplierError] the apply / cold-copy paths
// use, so the post-copy DDL-phase retry (CreateIndexes / CreateConstraints
// / CreateViews / SyncIdentitySequences) recognises a PlanetScale reparent
// identically to the row-write path — no second classifier to drift. A
// non-transient (a real DDL fault) returns false and the phase fails
// loudly, exactly as before.
func (w *SchemaWriter) IsTransientError(err error) bool {
	var re ir.RetriableError
	return errors.As(classifyApplierError(err), &re) && re.Retriable()
}

// Compile-time proof the SchemaWriter exposes the DDL-phase retry verdict
// (ADR-0114) so the orchestrator's post-copy DDL retry engages.
var _ ir.TransientClassifier = (*SchemaWriter)(nil)

// CreateIndexes adds every non-PRIMARY index across the schema. Each
// index is added with its own ALTER TABLE statement so a failure on
// one doesn't leave others in an indeterminate state.
//
// The order is (table name, index name) lexicographic — chosen for
// deterministic output more than any operational reason; index
// creation order doesn't affect correctness.
func (w *SchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: CreateIndexes: schema is nil")
	}
	// Flatten the whole schema into the same per-table work-list the
	// overlapped builder (ADR-0080) queues, then build each table's indexes
	// serially on the shared pool. Factoring the per-table loop body into
	// indexBuildJobsForTables keeps the emitted SQL — inline-skip set,
	// alphabetical index order, combined-ALTER grouping — byte-identical on
	// both paths.
	for _, job := range w.indexBuildJobsForTables(orderedTables(s)) {
		// The deploy-request fallback wrapper (ADR-0148) is a pass-through
		// when no fallback is configured — the direct build below is then
		// byte-identical to before.
		direct := func(ctx context.Context, job indexBuildJob) error {
			return w.buildTableIndexes(ctx, w.db, job)
		}
		if err := w.buildTableIndexesWithDeployFallback(ctx, job, direct); err != nil {
			return err
		}
	}
	return nil
}

// VerifyIndexes implements the [ir.IndexVerifier] loud-failure safety net:
// after the index phase, every secondary index the build TARGETED must exist
// on the target, or it returns a SLUICE-E-INDEX-MISSING refusal naming the
// missing `table.index` list.
//
// It reuses [indexBuildJobsForTables] so the verified set is EXACTLY the set
// the build path built — same inline-skip carve-out (the GitHub #25
// AUTO_INCREMENT supporting key + the Bug 125 PK-less COPY unique key are
// emitted at CREATE TABLE, never by the index phase, so probing for them here
// would false-flag). The expected set is checked against ONE batched
// information_schema.statistics read per [catalogProbeChunk] expected indexes
// (audit V-1) — the same catalog the idempotent build's detect-then-skip
// probes — rather than one probe per index: on a vtgate/PlanetScale target
// each probe is a serial cluster round trip, so a hundreds-of-index schema
// paid hundreds of RTTs at verify time on exactly the target the project is
// sold for.
//
// This net exists because the VStream/PlanetScale index-build path silently
// no-op'd (built NO secondary indexes) for releases — the project's #1
// silent-loss class. It runs for EVERY MySQL flavor (a general net, not scoped
// to the vstream path) so no future refactor can re-break the build path
// unnoticed.
func (w *SchemaWriter) VerifyIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: VerifyIndexes: schema is nil")
	}
	jobs := w.indexBuildJobsForTables(orderedTables(s))
	var wanted []catalogPair
	for _, job := range jobs {
		for _, idx := range job.idxs {
			wanted = append(wanted, catalogPair{table: job.tableName, name: idx.Name})
		}
	}
	existing, err := probeCatalogPairs(ctx, w.db, w.schema, wanted, statisticsPairsQuery)
	if err != nil {
		return fmt.Errorf("mysql: VerifyIndexes: probe expected indexes: %w", err)
	}
	var missing []string
	for _, job := range jobs {
		for _, idx := range job.idxs {
			if _, ok := existing[foldCatalogPair(job.tableName, idx.Name)]; !ok {
				missing = append(missing, job.tableName+"."+idx.Name)
			}
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeIndexMissing,
		"re-run with --resume to rebuild the missing indexes; if it recurs the target rejected the index DDL — check its DDL/online-migration policy",
		fmt.Errorf("mysql: VerifyIndexes: %d expected secondary index(es) absent on the target after the index phase: %s",
			len(missing), strings.Join(missing, ", ")),
	)
}

// Compile-time proof the SchemaWriter exposes the post-index-phase
// loud-failure verification net (SLUICE-E-INDEX-MISSING) so the orchestrator's
// [ir.IndexVerifier] check engages on both the migrate and sync cold-start
// paths.
var _ ir.IndexVerifier = (*SchemaWriter)(nil)

// indexBuildJob is one TABLE's secondary-index work the index-build paths
// consume (ADR-0080 follow-up). All of the table's eligible indexes travel
// together so [buildTableIndexes] can collapse the combinable ones into a
// single ALTER (one InnoDB scan); idxs is already inline-skip-filtered,
// PRIMARY-free, and alphabetically sorted by indexBuildJobsForTables.
type indexBuildJob struct {
	tableName string
	idxs      []*ir.Index
}

// indexBuildJobsForTables flattens a subset of tables into the per-table
// work-list both [CreateIndexes] (whole-schema) and the overlapped
// [BuildTableIndexesFromChannel] (per-table, as copies land) consume — ONE
// job per table that has at least one buildable secondary index. Factored out
// (ADR-0080) so the two paths emit identical SQL: the same inlineSkipIndexNames
// carve-out (GitHub #25 AUTO_INCREMENT supporting key + Bug 125 PK-less COPY
// unique key — re-creating either would raise a duplicate-index error) and the
// same alphabetical index ordering within each table. The caller controls
// table order: CreateIndexes passes orderedTables(s) (alphabetical, the prior
// serial order); the overlap path passes one just-copied table at a time
// (data-arrival order).
func (w *SchemaWriter) indexBuildJobsForTables(tables []*ir.Table) []indexBuildJob {
	var jobs []indexBuildJob
	for _, table := range tables {
		skip := inlineSkipIndexNames(table)
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		eligible := indexes[:0]
		for _, idx := range indexes {
			if _, skipped := skip[idx.Name]; skipped {
				continue
			}
			eligible = append(eligible, idx)
		}
		if len(eligible) == 0 {
			continue
		}
		jobs = append(jobs, indexBuildJob{
			tableName: table.Name,
			idxs:      append([]*ir.Index(nil), eligible...),
		})
	}
	return jobs
}

// dbExecer is the subset of *sql.DB / *sql.Conn buildTableIndexes needs. The
// whole-schema [CreateIndexes] builds on the pooled *sql.DB; the overlapped
// builder's workers each build on their OWN dedicated *sql.Conn so the
// concurrent ALTERs don't share one pooled connection.
type dbExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// buildTableIndexes builds all of one table's eligible secondary indexes
// (ADR-0080 follow-up). It probes the table's indexes, drops the ones that
// already exist, then emits the minimum set of ALTER statements via
// [emitCreateIndexesCombined] — combinable BTREE/UNIQUE indexes share one
// ALTER (a single InnoDB scan), FULLTEXT and SPATIAL each get their own — and
// executes them on execer. Shared verbatim by the serial whole-schema path and
// the overlapped per-table workers.
//
// Idempotent resume (Bug 131): a prior run that already created some of these
// indexes — a resume re-entering phase=indexes after a table-scope change, a
// partially-completed index phase, or (on the overlap path) a re-fed
// copied-not-fully-indexed table — would otherwise fail with MySQL 1061
// "Duplicate key name". MySQL has no `CREATE INDEX IF NOT EXISTS`, so
// detect-then-skip per index (the pattern the ADR-0054 shape applier uses),
// batched into ONE catalog read for the table's whole index set (audit V-1 —
// was one probe per index, a serial vtgate round trip apiece). The probe runs
// at ATTEMPT time, so the reparent-retry's replay re-probes and skips any
// index whose ALTER committed server-side but died unacknowledged — never a
// double-create. sluice owns these tables, so a same-named index is the one
// it built. When every index already exists the table is skipped entirely
// (no ALTER emitted).
func (w *SchemaWriter) buildTableIndexes(ctx context.Context, execer dbExecer, job indexBuildJob) error {
	wanted := make([]catalogPair, 0, len(job.idxs))
	for _, idx := range job.idxs {
		wanted = append(wanted, catalogPair{table: job.tableName, name: idx.Name})
	}
	existing, err := probeCatalogPairs(ctx, w.db, w.schema, wanted, statisticsPairsQuery)
	if err != nil {
		return fmt.Errorf("mysql: probe indexes on %q: %w", job.tableName, err)
	}
	pending := make([]*ir.Index, 0, len(job.idxs))
	for _, idx := range job.idxs {
		if _, ok := existing[foldCatalogPair(job.tableName, idx.Name)]; !ok {
			pending = append(pending, idx)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	stmts, err := emitCreateIndexesCombined(job.tableName, pending)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if _, err := execer.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: create indexes on %q: %w", job.tableName, wrapDDLError(err))
		}
	}
	return nil
}

// inlineSkipIndexNames returns the set of index names emitTableDef
// emits inline at CREATE TABLE time — the AUTO_INCREMENT supporting
// key (GitHub #25) and the PK-less COPY unique key (Bug 125). The
// index-emit phases skip these so they aren't re-created (which would
// raise a duplicate-index error). Empty for the common table.
func inlineSkipIndexNames(table *ir.Table) map[string]struct{} {
	skip := make(map[string]struct{}, 2)
	if inline := inlineAutoIncrementIndex(table); inline != nil {
		skip[inline.Name] = struct{}{}
	}
	if inline := inlineUniqueKeyForCopy(table); inline != nil {
		skip[inline.Name] = struct{}{}
	}
	return skip
}

// CreateConstraints adds every foreign-key constraint across the
// schema. All referenced tables must already exist (which they do
// after CreateTablesWithoutConstraints).
//
// The order is (child table name, constraint name) lexicographic.
func (w *SchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: CreateConstraints: schema is nil")
	}
	tables := orderedTables(s)
	// Idempotent resume (Bug 131 same-class): a prior run that already added
	// some of these FKs — a resume re-entering phase=constraints — would
	// otherwise fail with MySQL 1826 "Duplicate foreign key constraint name".
	// MySQL has no ADD CONSTRAINT IF NOT EXISTS, so detect-then-skip (mirrors
	// the index/CHECK idempotency), batched into ONE catalog read per
	// [catalogProbeChunk] FKs up front (audit V-1 — was one probe per FK, a
	// serial vtgate round trip apiece). Prefetching the whole set before any
	// ALTER is equivalent to the old lazy per-FK probe: sluice owns these
	// tables and this loop is the only FK adder, so nothing lands mid-phase
	// that the prefetch could miss — and the pipeline's DDL-phase retry
	// re-enters through this prefetch, re-seeing anything a killed attempt
	// committed. A same-named FK is the one sluice built.
	var wanted []catalogPair
	for _, table := range tables {
		for _, fk := range table.ForeignKeys {
			wanted = append(wanted, catalogPair{table: table.Name, name: fk.Name})
		}
	}
	existing, err := probeCatalogPairs(ctx, w.db, w.schema, wanted, foreignKeyPairsQuery)
	if err != nil {
		return fmt.Errorf("mysql: CreateConstraints: probe existing foreign keys: %w", err)
	}
	for _, table := range tables {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			if _, ok := existing[foldCatalogPair(table.Name, fk.Name)]; ok {
				continue
			}
			stmt, err := emitAddForeignKey(table.Schema, table.Name, fk)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("mysql: add foreign key %q on %q: %w", fk.Name, table.Name, wrapDDLError(err))
			}
		}
	}
	return nil
}

// AnalyzeTable implements [ir.TableAnalyzer] — the migrate
// orchestrator's opt-in `--analyze-after` post-migration statistics
// refresh. MySQL's `ANALYZE TABLE` returns a RESULT SET and reports
// per-table failures as rows (Msg_type='Error') rather than as a
// statement error — running it through Exec would swallow those, a
// silent-failure trap. So the statement is Queried and the result
// scanned; any Error row surfaces loudly with the server's message.
func (w *SchemaWriter) AnalyzeTable(ctx context.Context, table *ir.Table) error {
	if table == nil {
		return errors.New("mysql: AnalyzeTable: table is nil")
	}
	rows, err := w.db.QueryContext(ctx, "ANALYZE TABLE "+quoteIdent(table.Name))
	if err != nil {
		return fmt.Errorf("mysql: analyze table %q: %w", table.Name, err)
	}
	defer func() { _ = rows.Close() }()
	// Result shape: Table | Op | Msg_type | Msg_text.
	for rows.Next() {
		var tbl, op, msgType, msgText string
		if err := rows.Scan(&tbl, &op, &msgType, &msgText); err != nil {
			return fmt.Errorf("mysql: analyze table %q: scan result: %w", table.Name, err)
		}
		if strings.EqualFold(msgType, "error") {
			return fmt.Errorf("mysql: analyze table %q: %s", table.Name, msgText)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mysql: analyze table %q: %w", table.Name, err)
	}
	return nil
}

// SyncIdentitySequences is a no-op on MySQL. InnoDB's AUTO_INCREMENT
// counter is automatically advanced past explicit-value INSERTs at
// transaction commit time, so a post-bulk-copy sync isn't needed —
// the next user-initiated INSERT picks up where the bulk-copied IDs
// left off without any extra work.
func (w *SchemaWriter) SyncIdentitySequences(_ context.Context, _ *ir.Schema) error {
	return nil
}

// CreateViews emits `CREATE OR REPLACE VIEW` for every view in s in
// declaration order. View definitions are emitted verbatim — Phase 1
// punts on cross-engine view-body translation (see [ir.View]), so a
// PG-source-dialect definition applied to a MySQL target will fail
// loudly at apply time rather than silently corrupt the view body.
//
// The orchestrator is responsible for retrying on view-to-view
// dependency failures (one view referencing another that hasn't been
// created yet); CreateViews itself emits in declared order with no
// dependency analysis. See [pipeline.runViewsPhase].
//
// MySQL's view-body parser stores the SELECT in a re-canonicalised
// form (backtick-quoted identifiers, charset introducers, etc.) — when
// a sluice-managed view is round-tripped, the text the source reader
// returns differs slightly from the operator's original DDL but
// parses to the same logical view. `schema diff` accepts the round-
// tripped form as equal.
func (w *SchemaWriter) CreateViews(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("mysql: CreateViews: schema is nil")
	}
	for _, view := range s.Views {
		if view == nil || view.Name == "" {
			continue
		}
		if view.Materialized {
			// MySQL has no materialized-view concept. The schema
			// reader on PG sources tags matviews; the writer surface
			// here surfaces a clear error rather than silently
			// emitting a regular view with the matview's SELECT (the
			// loud-failure tenet).
			return fmt.Errorf("mysql: CreateViews: view %q is materialized; MySQL has no materialized view support", view.Name)
		}
		stmt := emitCreateView(view)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: create view %q: %w", view.Name, wrapDDLError(err))
		}
	}
	return nil
}

// emitCreateView returns the `CREATE OR REPLACE VIEW <name> AS
// <definition>` statement for a regular view. Identifier quoting
// follows the engine's existing conventions; the definition body is
// emitted verbatim per Phase 1's no-translation policy.
func emitCreateView(v *ir.View) string {
	return "CREATE OR REPLACE VIEW " + quoteIdent(v.Name) + " AS " + v.Definition + ";"
}

// orderedTables returns s.Tables sorted alphabetically by name. The
// returned slice is independent of s.Tables; callers may sort or
// modify it without affecting the schema.
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
func (w *SchemaWriter) PreviewDDL(_ context.Context, s *ir.Schema) ([]ir.DDLStatement, error) {
	if s == nil {
		return nil, errors.New("mysql: PreviewDDL: schema is nil")
	}

	out := make([]ir.DDLStatement, 0, len(s.Tables)*2)

	// Phase 1: tables.
	for _, table := range orderedTables(s) {
		stmt, err := w.emitter.emitTableDefWithDomainChecks(table, w.inlineCheckSupported)
		if err != nil {
			return nil, err
		}
		out = append(out, ir.DDLStatement{
			Table: table.Name,
			Kind:  "CREATE TABLE",
			SQL:   trimTrailingSemicolon(stmt),
		})
	}

	// Phase 2: secondary indexes. Skip the inline-emitted indexes
	// (GitHub #25 AUTO_INCREMENT key + Bug 125 PK-less COPY unique key,
	// same logic as CreateIndexes above).
	for _, table := range orderedTables(s) {
		skip := inlineSkipIndexNames(table)
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			if _, skipped := skip[idx.Name]; skipped {
				continue
			}
			stmt, err := emitCreateIndex(table.Name, idx)
			if err != nil {
				return nil, err
			}
			// Empty stmt = a non-portable SQLite index was WARN-skipped.
			if stmt == "" {
				continue
			}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "ALTER TABLE",
				SQL:   trimTrailingSemicolon(stmt),
			})
		}
	}

	// Phase 3: foreign keys.
	for _, table := range orderedTables(s) {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			stmt, err := emitAddForeignKey(table.Schema, table.Name, fk)
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
	// exist by the time the view is created.
	for _, view := range s.Views {
		if view == nil || view.Name == "" || view.Materialized {
			continue
		}
		out = append(out, ir.DDLStatement{
			Table: view.Name,
			Kind:  "CREATE VIEW",
			SQL:   trimTrailingSemicolon(emitCreateView(view)),
		})
	}

	return out, nil
}

// trimTrailingSemicolon removes a single trailing ';' from s, if
// present. MySQL DDL emitters terminate every statement with a
// semicolon for executability; preview output adds them back at format
// time so the wire shape is decoupled from the rendering shape.
func trimTrailingSemicolon(s string) string {
	return strings.TrimRight(s, ";")
}

// EmitColumnDef satisfies [ir.ColumnDDLPreviewer]. Returns the MySQL
// column-def fragment (“ `name` TYPE [GENERATED ...] [NOT NULL]
// [DEFAULT ...] [COMMENT '...']“) suitable for inlining into an
// `ALTER TABLE ... ADD COLUMN` suggestion in the schema-diff
// renderer (ADR-0029). The table is used only to NAME table.column
// in the DEFAULT-position loud-drop warn (nil-tolerant); MySQL's
// emitter needs no other table context for any IR type.
func (w *SchemaWriter) EmitColumnDef(_ context.Context, table *ir.Table, col *ir.Column) (string, error) {
	return w.emitter.emitColumnDef(tableNameOrEmpty(table), col)
}

// AlterAddColumn implements [ir.SchemaDeltaApplier] for MySQL. Used
// by Phase 3 chain restore to apply column-add deltas captured on
// incremental manifests against the target. MySQL gained
// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in 8.0.29; we probe
// information_schema.columns first instead, so the call is
// idempotent across re-runs and works on older 8.0.x and 5.7
// servers too.
func (w *SchemaWriter) AlterAddColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) error {
	if len(cols) == 0 {
		return nil
	}
	for _, col := range cols {
		exists, err := columnExists(ctx, w.db, w.schema, table.Name, col.Name)
		if err != nil {
			return fmt.Errorf("alter add column: probe %q: %w", col.Name, err)
		}
		if exists {
			continue
		}
		// Bug 83 v0.73.1 — force Nullable=true on the emitted ADD
		// COLUMN regardless of the IR's Nullable flag. MySQL's binlog-
		// derived CDC IR DOES carry nullability faithfully (loaded from
		// information_schema), but the PG sibling does not (pgoutput's
		// RelationMessage omits attnotnull); to keep the two engines'
		// behaviour symmetric — and to give operators a single
		// predictable rule for the live cross-shard coordination flow
		// — both engines emit ADD COLUMN nullable. Operators who need
		// NOT NULL on the target can apply `ALTER COLUMN SET NOT NULL`
		// post-apply once the existing rows have a backfilled value.
		// See CHANGELOG v0.73.1.
		emitCol := *col
		emitCol.Nullable = true
		def, err := w.emitter.emitColumnDef(table.Name, &emitCol)
		if err != nil {
			return fmt.Errorf("alter add column: emit %q: %w", col.Name, err)
		}
		// Cross-engine collation-drop WARN, per column: this forward
		// path bypasses emitTableDef's per-table aggregation, and a
		// forwarded ADD COLUMN carrying a PG-dialect collation would
		// otherwise drop it silently (emitColumnType omits foreign
		// collations).
		warnDroppedForeignCollation(table, col.Name, col.Type)
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
			quoteIdent(table.Name), def)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter add column %q on %s: %w",
				col.Name, table.Name, err)
		}
	}
	return nil
}

// columnExists reports whether table.column is already present in
// the target's information_schema.columns. Used by [AlterAddColumn]
// for idempotency on servers without `ADD COLUMN IF NOT EXISTS`.
func columnExists(ctx context.Context, db *sql.DB, schema, table, col string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_name = ?)`
	var exists bool
	if err := db.QueryRowContext(ctx, q, schema, table, col).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// indexExists reports whether table.indexName is present in the
// target's information_schema.statistics. Used by the ADR-0054 Phase
// 2c shape-applier methods (CreateShapeIndex / DropShapeIndex) for
// idempotency — MySQL lacks `CREATE INDEX IF NOT EXISTS` and
// `DROP INDEX IF EXISTS` in the versions sluice supports (8.0.x), so
// detect-then-DDL is the portable pattern. The bulk index/constraint
// phases use the batched [probeCatalogPairs] instead (audit V-1);
// the shape appliers stay on the single probe because CDC schema
// deltas arrive one object at a time.
func indexExists(ctx context.Context, db *sql.DB, schema, table, indexName string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name = ?)`
	var exists bool
	if err := db.QueryRowContext(ctx, q, schema, table, indexName).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// catalogProbeChunk caps how many wanted (table, name) pairs ride one batched
// catalog probe; larger sets are split into ceil(n/chunk) queries. Each chunk
// carries at most 1 + tables + names placeholders (≤ 1 + 2×chunk), far under
// MySQL's 65,535-placeholder limit and small enough to interpolate cleanly on
// the PS/Vitess text-protocol default (ADR-0153). At 400, the audit's
// 200-index PlanetScale schema verifies in ONE metadata query instead of 200
// serial vtgate round trips.
const catalogProbeChunk = 400

// catalogPair identifies one named per-table catalog object — a secondary
// index or a FOREIGN KEY constraint. A struct key, not a "table.name" string
// concat, so a dotted table name can never alias a sibling's object.
type catalogPair struct{ table, name string }

// foldCatalogPair normalizes a pair for existence-set membership. MySQL
// compares these names case-insensitively on the probing plane — index and
// constraint names always, table names per lower_case_table_names — and the
// old per-object probes inherited that from information_schema's ci column
// collation (`WHERE table_name = ?`). The Go-side set compare must keep that
// semantic: an lctn=1 target stores (and reports) a mixed-case CREATE TABLE
// name lowercased, and an exact-case compare would false-flag every one of
// its indexes as missing. ToLower is the simple-case-fold approximation of
// utf8_general_ci — faithful for identifier-realistic names.
func foldCatalogPair(table, name string) catalogPair {
	return catalogPair{table: strings.ToLower(table), name: strings.ToLower(name)}
}

// probeCatalogPairs reports which of the wanted (table, name) pairs exist on
// the target, reading the catalog in batched chunks instead of one probe per
// object (audit V-1: on a vtgate/PlanetScale target every probe is a serial
// cluster round trip, so the old N+1 shape cost hundreds of RTTs per phase at
// verify/build time on the exact target the project is sold for). queryFor
// renders one chunk's SQL for the given table/name placeholder counts —
// [statisticsPairsQuery] or [foreignKeyPairsQuery]; every value rides a
// parameter (or the flavor's interpolated equivalent — the same protocol the
// per-object probes used), never string-built into the query. Returned keys
// are [foldCatalogPair]-normalized; look them up the same way. The result may
// carry existing pairs beyond the wanted set (the query is the tables×names
// cross product); callers only test wanted-pair membership, so the overshoot
// is never load-bearing.
func probeCatalogPairs(
	ctx context.Context,
	db *sql.DB,
	schema string,
	wanted []catalogPair,
	queryFor func(nTables, nNames int) string,
) (map[catalogPair]struct{}, error) {
	out := make(map[catalogPair]struct{}, len(wanted))
	for start := 0; start < len(wanted); start += catalogProbeChunk {
		end := start + catalogProbeChunk
		if end > len(wanted) {
			end = len(wanted)
		}
		if err := probeCatalogPairsChunk(ctx, db, schema, wanted[start:end], queryFor, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// probeCatalogPairsChunk runs ONE batched catalog query for a chunk of wanted
// pairs and folds the returned (table, name) rows into out.
func probeCatalogPairsChunk(
	ctx context.Context,
	db *sql.DB,
	schema string,
	chunk []catalogPair,
	queryFor func(nTables, nNames int) string,
	out map[catalogPair]struct{},
) error {
	// Distinct tables/names in first-seen order, so the emitted query is
	// deterministic for a given work-list (the shape the unit pins assert).
	tables := make([]string, 0, len(chunk))
	names := make([]string, 0, len(chunk))
	seenTables := make(map[string]struct{}, len(chunk))
	seenNames := make(map[string]struct{}, len(chunk))
	for _, p := range chunk {
		if _, ok := seenTables[p.table]; !ok {
			seenTables[p.table] = struct{}{}
			tables = append(tables, p.table)
		}
		if _, ok := seenNames[p.name]; !ok {
			seenNames[p.name] = struct{}{}
			names = append(names, p.name)
		}
	}
	args := make([]any, 0, 1+len(tables)+len(names))
	args = append(args, schema)
	for _, t := range tables {
		args = append(args, t)
	}
	for _, n := range names {
		args = append(args, n)
	}
	rows, err := db.QueryContext(ctx, queryFor(len(tables), len(names)), args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var table, name string
		if err := rows.Scan(&table, &name); err != nil {
			return err
		}
		out[foldCatalogPair(table, name)] = struct{}{}
	}
	return rows.Err()
}

// statisticsPairsQuery renders one chunk of the batched index-existence probe
// for [probeCatalogPairs]. DISTINCT because information_schema.statistics
// carries one row per indexed COLUMN. Narrowing by index_name too (not just
// table_name) keeps the fetch proportional to the expected set — a wide table
// full of unrelated indexes contributes nothing — and mirrors the bounds the
// per-object probe applied DB-side.
func statisticsPairsQuery(nTables, nNames int) string {
	return "SELECT DISTINCT table_name, index_name FROM information_schema.statistics" +
		" WHERE table_schema = ? AND table_name IN (" + sqlPlaceholders(nTables) + ")" +
		" AND index_name IN (" + sqlPlaceholders(nNames) + ")"
}

// foreignKeyPairsQuery renders one chunk of the batched FOREIGN-KEY-existence
// probe for [probeCatalogPairs] (TABLE_CONSTRAINTS is one row per constraint,
// so no DISTINCT needed).
func foreignKeyPairsQuery(nTables, nNames int) string {
	return "SELECT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS" +
		" WHERE CONSTRAINT_SCHEMA = ? AND CONSTRAINT_TYPE = 'FOREIGN KEY'" +
		" AND TABLE_NAME IN (" + sqlPlaceholders(nTables) + ")" +
		" AND CONSTRAINT_NAME IN (" + sqlPlaceholders(nNames) + ")"
}

// sqlPlaceholders returns n comma-joined `?` placeholders for an IN list.
func sqlPlaceholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// columnNullable returns the IS_NULLABLE attribute of the named
// column ("YES" / "NO"). Used by the ADR-0054 Phase 2c nullability
// applier for idempotency.
func columnNullable(ctx context.Context, db *sql.DB, schema, table, col string) (nullable string, found bool, err error) {
	const q = `SELECT IS_NULLABLE FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_name = ?`
	var v string
	switch scanErr := db.QueryRowContext(ctx, q, schema, table, col).Scan(&v); {
	case errors.Is(scanErr, sql.ErrNoRows):
		return "", false, nil
	case scanErr != nil:
		return "", false, scanErr
	}
	return v, true, nil
}

// AlterDropColumn implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0054 Phase 2c). Detect-then-ALTER for idempotency — the
// portable pattern across 8.0.x; `DROP COLUMN IF EXISTS` landed in
// 8.0.29 and isn't universally available.
func (w *SchemaWriter) AlterDropColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) error {
	if len(cols) == 0 {
		return nil
	}
	for _, col := range cols {
		exists, err := columnExists(ctx, w.db, w.schema, table.Name, col.Name)
		if err != nil {
			return fmt.Errorf("alter drop column: probe %q: %w", col.Name, err)
		}
		if !exists {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
			quoteIdent(table.Name), quoteIdent(col.Name))
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter drop column %q on %s: %w",
				col.Name, table.Name, err)
		}
	}
	return nil
}

// CreateShapeIndex implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0054 Phase 2c). Reuses the existing emitCreateIndex emitter
// (which emits an ALTER TABLE … ADD INDEX form). Idempotency is via
// detect-then-DDL on information_schema.statistics.
func (w *SchemaWriter) CreateShapeIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) error {
	if len(indexes) == 0 {
		return nil
	}
	for _, idx := range indexes {
		if idx == nil || strings.EqualFold(idx.Name, "PRIMARY") {
			continue
		}
		exists, err := indexExists(ctx, w.db, w.schema, table.Name, idx.Name)
		if err != nil {
			return fmt.Errorf("create shape index: probe %q: %w", idx.Name, err)
		}
		if exists {
			continue
		}
		stmt, err := emitCreateIndex(table.Name, idx)
		if err != nil {
			return fmt.Errorf("create shape index: emit %q: %w", idx.Name, err)
		}
		// Empty stmt = a non-portable SQLite index was WARN-skipped.
		if stmt == "" {
			continue
		}
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create shape index %q on %s: %w",
				idx.Name, table.Name, err)
		}
	}
	return nil
}

// DropShapeIndex implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0054 Phase 2c). Detect-then-DROP for idempotency.
func (w *SchemaWriter) DropShapeIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) error {
	if len(indexes) == 0 {
		return nil
	}
	for _, idx := range indexes {
		if idx == nil || strings.EqualFold(idx.Name, "PRIMARY") {
			continue
		}
		exists, err := indexExists(ctx, w.db, w.schema, table.Name, idx.Name)
		if err != nil {
			return fmt.Errorf("drop shape index: probe %q: %w", idx.Name, err)
		}
		if !exists {
			continue
		}
		stmt := fmt.Sprintf("DROP INDEX %s ON %s",
			quoteIdent(idx.Name), quoteIdent(table.Name))
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("drop shape index %q on %s: %w",
				idx.Name, table.Name, err)
		}
	}
	return nil
}

// AlterColumnType implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0054 Phase 2c). Uses `MODIFY COLUMN` — the MySQL form of
// changing a column's type. The emitted column-def carries the
// type + nullability; for same-nullability type-widening we still
// emit the full def so MySQL's column-rewrite path runs cleanly.
//
// Idempotency: MySQL accepts a MODIFY that yields the same column
// shape as a fast no-op; the engine short-circuits when the catalog
// already matches the def.
func (w *SchemaWriter) AlterColumnType(ctx context.Context, table *ir.Table, want *ir.Column) error {
	if want == nil {
		return errors.New("mysql: alter column type: want column is nil")
	}
	def, err := w.emitter.emitColumnDef(table.Name, want)
	if err != nil {
		return fmt.Errorf("alter column type: emit %q: %w", want.Name, err)
	}
	// Cross-engine collation-drop WARN, per column (mirrors
	// AlterAddColumn — emitColumnType omits foreign collations).
	warnDroppedForeignCollation(table, want.Name, want.Type)
	stmt := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s",
		quoteIdent(table.Name), def)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter column type %q on %s: %w",
			want.Name, table.Name, err)
	}
	return nil
}

// AlterRenameColumn implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0054 v0.78.0 — task #22 RENAME COLUMN sub-task). MySQL 8.0+
// `ALTER TABLE ... RENAME COLUMN <old> TO <new>` preserves type,
// nullability, default, comment, identity, and collation; only the
// name changes. (MySQL 5.7 lacks RENAME COLUMN; sluice supports
// 8.0+ so this is fine.)
//
// Idempotency on the post-state via detect-then-RENAME: MySQL does
// not support `RENAME COLUMN IF EXISTS`, so we probe both names
// first against information_schema.columns and only emit the DDL
// when oldName is present + newName is absent.
func (w *SchemaWriter) AlterRenameColumn(ctx context.Context, table *ir.Table, oldName, newName string) error {
	if oldName == "" || newName == "" {
		return errors.New("mysql: alter rename column: oldName and newName must be non-empty")
	}
	if oldName == newName {
		return nil
	}
	oldPresent, err := columnExists(ctx, w.db, w.schema, table.Name, oldName)
	if err != nil {
		return fmt.Errorf("alter rename column: probe %q: %w", oldName, err)
	}
	newPresent, err := columnExists(ctx, w.db, w.schema, table.Name, newName)
	if err != nil {
		return fmt.Errorf("alter rename column: probe %q: %w", newName, err)
	}
	switch {
	case !oldPresent && newPresent:
		// Idempotent post-state.
		return nil
	case oldPresent && !newPresent:
		// Apply.
	case oldPresent && newPresent:
		return fmt.Errorf(
			"mysql: alter rename column %s.%s: both %q and %q exist — "+
				"target schema cannot be reconciled without operator intervention",
			w.schema, table.Name, oldName, newName,
		)
	default: // !oldPresent && !newPresent
		return fmt.Errorf(
			"mysql: alter rename column %s.%s: neither %q nor %q exists",
			w.schema, table.Name, oldName, newName,
		)
	}
	stmt := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		quoteIdent(table.Name), quoteIdent(oldName), quoteIdent(newName))
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter rename column %s.%s -> %s: %w",
			table.Name, oldName, newName, err)
	}
	return nil
}

// AlterColumnNullability implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0054 Phase 2c). Detect-then-MODIFY for idempotency: read the
// catalog's IS_NULLABLE first, skip the MODIFY if it already matches.
func (w *SchemaWriter) AlterColumnNullability(ctx context.Context, table *ir.Table, want *ir.Column) error {
	if want == nil {
		return errors.New("mysql: alter column nullability: want column is nil")
	}
	currentNullable, ok, err := columnNullable(ctx, w.db, w.schema, table.Name, want.Name)
	if err != nil {
		return fmt.Errorf("alter column nullability: probe %q: %w", want.Name, err)
	}
	if !ok {
		return fmt.Errorf("mysql: alter column nullability: column %q absent on %s", want.Name, table.Name)
	}
	wantYes := want.Nullable
	currentYes := currentNullable == "YES"
	if wantYes == currentYes {
		return nil
	}
	def, err := w.emitter.emitColumnDef(table.Name, want)
	if err != nil {
		return fmt.Errorf("alter column nullability: emit %q: %w", want.Name, err)
	}
	stmt := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s",
		quoteIdent(table.Name), def)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("alter column nullability %q on %s: %w",
			want.Name, table.Name, err)
	}
	return nil
}
