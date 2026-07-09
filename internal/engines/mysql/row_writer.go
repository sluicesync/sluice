// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// defaultMaxRowsPerBatch caps how many rows go into a single INSERT
// statement — a SAFETY CEILING, not the primary flush trigger. Since
// ADR-0150 the batched bulk paths flush on the ~1 MiB estimated
// statement byte target (defaultStatementByteTarget, the pscale-dumper
// size — see row_writer_bytebatch.go), so this cap only binds for rows
// so narrow that ten thousand of them still sit under the byte target;
// it bounds per-batch heap and statement parse cost. It was 500 —
// the pre-ADR-0150 primary trigger — which capped narrow-row
// statements at 50–100 KB and left 10–20× of WAN round-trip
// amortization unused on PlanetScale (the flavor with no LOAD DATA,
// where batched INSERT IS the bulk-load path). The placeholder bound
// (maxBulkInsertPlaceholders / columns) clamps it further for wide
// tables; tests can override via maxRowsPerBatch.
const defaultMaxRowsPerBatch = 10000

// defaultMaxBufferBytes is the soft per-batch byte cap when the
// caller doesn't set one explicitly. Bounds heap usage at ~64 MiB
// for wide-row workloads; tunable via --max-buffer-bytes. See
// ADR-0028. The batched bulk-INSERT paths now flush far earlier on
// the ADR-0150 ~1 MiB statement byte target; this cap remains the
// accumulation bound for the CDC apply-batch paths
// (change_applier_batch.go / change_applier_concurrent.go).
const defaultMaxBufferBytes int64 = 64 << 20 // 64 MiB

// RowWriter performs bulk inserts into MySQL tables. It implements
// [ir.RowWriter].
//
// The writer chooses a backend strategy at construction time based on
// the engine's declared [ir.BulkLoadMethod]:
//
//   - BulkLoadLoadDataInfile: streams rows to MySQL via LOAD DATA
//     LOCAL INFILE using go-sql-driver/mysql's RegisterReaderHandler
//     mechanism (no real file is written). Typically 5–10x faster than
//     batched INSERT. Falls back to BatchedInsert on a per-call basis
//     when the server has `local_infile=OFF` or the table contains an
//     ir.Geometry column. See `load_data_writer.go`.
//   - BulkLoadBatchedInsert: accumulates rows into multi-row INSERT
//     statements via prepared parameter placeholders. Used by
//     PlanetScale (which doesn't allow LOAD DATA LOCAL INFILE) and as
//     the per-call fallback path for vanilla MySQL.
//
// The writer holds an open *sql.DB; callers should call Close when
// finished to release the connection pool.
type RowWriter struct {
	db       *sql.DB
	schema   string
	bulkLoad ir.BulkLoadMethod

	// sqlMode is the engine's resolved --mysql-sql-mode override (task 2.5).
	// [reportBulkWriteWarnings] keys its WARN-vs-refuse decision off whether the
	// operator opted into the relaxed "" mode ([resolveSessionSQLMode]), replacing
	// the former sessionSQLMode package global. nil (the direct-API / unit
	// construction default) resolves to the strict-by-default mode, so a bare
	// RowWriter refuses on a LOAD-DATA warning exactly as before.
	sqlMode *string

	// maxRowsPerBatch caps the number of rows folded into a single
	// INSERT statement. Tests can override it; callers typically
	// leave it as the zero value, in which case defaultMaxRowsPerBatch
	// is used.
	maxRowsPerBatch int

	// maxBufferBytes is the operator's byte-size cap on per-batch
	// buffered row values for the BatchedInsert path. Implements
	// [ir.MaxBufferBytesSetter] via [SetMaxBufferBytes]. Since
	// ADR-0150 the batched paths flush on the ~1 MiB statement byte
	// target (defaultStatementByteTarget); a value here BELOW that
	// target lowers the effective per-statement byte trigger, while
	// zero/negative or a larger value leaves the target in charge
	// (the target already sits far under the ADR-0028 64 MiB
	// accumulation default). The LOAD DATA path is already streaming
	// (rows go through an io.Pipe to the driver) and is unaffected.
	maxBufferBytes int64

	// tierCPUBoundTarget marks a hosted-PlanetScale-flavor target;
	// the first batched bulk flush per writer emits the ADR-0150
	// tier-CPU-ceiling operator hint (see [noteTierCPUBoundTarget]).
	// Set at [Engine.OpenRowWriter]; false on every other path, which
	// makes the zero value the safe/silent default.
	tierCPUBoundTarget bool
	tierHintOnce       sync.Once

	// copyDurableProgress is the durable-write reporter the cold-start
	// COPY path wires (v0.99.9). When set, the idempotent batch writer
	// calls it after each successful flush with the per-flush row delta
	// so the snapshot reader's checkpoint stays at-or-behind the
	// durably-written frontier. Implements [ir.CopyDurableProgressReporter]
	// via [SetCopyDurableProgress]. nil on every non-cold-start path.
	copyDurableProgress ir.CopyDurableProgressFunc

	// warnedClamp tracks tables already warned about a relaxed-sql_mode
	// silent coercion (Vector B), so reportBulkWriteWarnings emits at
	// most one WARN per table rather than one per flushed batch. Keyed
	// by table name; values are struct{}. Zero value is ready to use.
	warnedClamp sync.Map

	// growGate is the shared cold-copy coordinated-pause primitive
	// (ADR-0110). The pipeline threads ONE gate per cold-copy run onto
	// every writer it opens (via [SetGrowGate]); flushWithReparentRetry
	// Awaits it at the top of each flush attempt and Trips it on a
	// classified grow-transient so all sibling lanes quiesce together for
	// the grow window. nil ⇒ pre-ADR-0110 behaviour, byte-for-byte (Await
	// returns immediately, Trip is a no-op) — the default on every path
	// the pipeline doesn't wire (serial copy, tests, the non-cold-copy
	// apply path). Implements [ir.GrowGateSetter].
	growGate ir.GrowGate

	// reparentObserver, when non-nil, is called with the table name the
	// FIRST time a flush on that table hits a classified grow/reparent
	// transient (the same point growGate is tripped). The restore wires it
	// (ADR-0113) so its reconciliation phase knows which tables a target's
	// reparent may have silently under-copied — PlanetScale's grow-reparent
	// can drop committed-but-unreplicated rows the reactive grow-gate cannot
	// recover. nil ⇒ no tracking (the default on every non-restore path).
	// Implements [ir.ReparentObserverSetter]. Guarded by reparentOnce so a
	// table is reported at most once per writer regardless of how many of
	// its batches hit the transient.
	reparentObserver func(table string)
	reparentSeen     sync.Map
}

// SetGrowGate implements [ir.GrowGateSetter] (ADR-0110). The pipeline
// wires the cold-copy run's shared [ir.GrowGate] here, right after
// OpenRowWriter, so every per-table / per-fan-out-worker writer in the run
// shares ONE pause coordinator. A nil gate disables the coordinated pause
// (pre-ADR-0110 behaviour); the per-lane reparent-retry budget remains the
// authoritative floor either way.
func (w *RowWriter) SetGrowGate(gate ir.GrowGate) {
	w.growGate = gate
}

// SetReparentObserver implements [ir.ReparentObserverSetter] (ADR-0113).
// The restore wires one run-shared observer onto every writer it opens, so
// the reconciliation phase can re-derive any reparent-touched table from
// its chunks. A nil observer disables tracking (every non-restore path).
func (w *RowWriter) SetReparentObserver(observe func(table string)) {
	w.reparentObserver = observe
}

// notifyReparent reports table to the wired reparent observer at most once
// per writer per table (the sync.Map dedup). No-op when no observer is
// wired. Called from flushWithReparentRetry at the grow-gate trip point.
func (w *RowWriter) notifyReparent(table string) {
	if w.reparentObserver == nil {
		return
	}
	if _, seen := w.reparentSeen.LoadOrStore(table, struct{}{}); seen {
		return
	}
	w.reparentObserver(table)
}

// SetCopyDurableProgress implements [ir.CopyDurableProgressReporter]
// (v0.99.9). The pipeline wires the snapshot reader's durable-progress
// sink here on the cold-start COPY path, before WriteRowsIdempotent runs,
// so each successful flush reports its row delta to the checkpoint
// watermark. A nil func disables reporting (the default on every path
// that isn't a resumable VStream cold-start).
func (w *RowWriter) SetCopyDurableProgress(report ir.CopyDurableProgressFunc) {
	w.copyDurableProgress = report
}

// SetMaxBufferBytes implements [ir.MaxBufferBytesSetter]. The
// orchestrator calls this after [Engine.OpenRowWriter] returns when
// --max-buffer-bytes is set, before WriteRows runs. A value below the
// ADR-0150 ~1 MiB statement byte target lowers the effective
// per-statement flush trigger; zero/negative or a larger value leaves
// the target in charge (see the maxBufferBytes field comment).
func (w *RowWriter) SetMaxBufferBytes(bytes int64) {
	w.maxBufferBytes = bytes
}

// Close releases the underlying connection pool.
func (w *RowWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// TruncateTable empties the target table. Used by the resume path in
// [pipeline.Migrator] to clear an `in_progress` table before
// re-running its bulk copy. Implements [ir.TableTruncator].
//
// MySQL TRUNCATE TABLE drops and recreates the table — fast on InnoDB
// but it implicitly commits any open transaction. Fine here because
// the resume path runs single-threaded and any in-flight writer is
// torn down before this call.
func (w *RowWriter) TruncateTable(ctx context.Context, table *ir.Table) error {
	if table == nil {
		return errors.New("mysql: TruncateTable: table is nil")
	}
	stmt := "TRUNCATE TABLE " + quoteIdent(table.Name)
	if w.schema != "" {
		stmt = "TRUNCATE TABLE " + quoteIdent(w.schema) + "." + quoteIdent(table.Name)
	}
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysql: truncate %q: %w", table.Name, err)
	}
	return nil
}

// DropTable drops the target table. Used by the
// `--reset-target-data` recovery path (ADR-0023). Implements
// [ir.TableDropper].
//
// MySQL's DROP TABLE does not accept the CASCADE keyword (which is
// PG-specific); InnoDB's referential cascade rules handle FK
// dependencies based on the constraint's declared ON DELETE action.
// IF EXISTS keeps the call idempotent across partial-failure retries.
// The schema readers exclude `sluice_*_state` tables, so the
// bookkeeping row is cleared via [MigrationStateStore.ClearMigration]
// / [ChangeApplier.ClearStream] rather than ever reaching this method.
func (w *RowWriter) DropTable(ctx context.Context, table *ir.Table) error {
	if table == nil {
		return errors.New("mysql: DropTable: table is nil")
	}
	stmt := "DROP TABLE IF EXISTS " + quoteIdent(table.Name)
	if w.schema != "" {
		stmt = "DROP TABLE IF EXISTS " + quoteIdent(w.schema) + "." + quoteIdent(table.Name)
	}
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysql: drop %q: %w", table.Name, err)
	}
	return nil
}

// DropTables drops every named table with one DROP TABLE statement.
// Implements [ir.BulkTableDropper] for the reset path on databases
// with many tables — collapses N round-trips into one. MySQL's
// multi-table DROP is atomic per the binlog-correctness model;
// either every named table goes away or none do. IF EXISTS preserves
// idempotency. CASCADE is not a MySQL keyword; InnoDB's referential
// cascade rules handle FK dependencies.
//
// An empty input list is a no-op; nil entries are skipped silently.
func (w *RowWriter) DropTables(ctx context.Context, tables []*ir.Table) error {
	if len(tables) == 0 {
		return nil
	}
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		if w.schema != "" {
			parts = append(parts, quoteIdent(w.schema)+"."+quoteIdent(t.Name))
		} else {
			parts = append(parts, quoteIdent(t.Name))
		}
	}
	if len(parts) == 0 {
		return nil
	}
	stmt := "DROP TABLE IF EXISTS " + strings.Join(parts, ", ")
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysql: drop %d tables: %w", len(parts), err)
	}
	return nil
}

// IsTableEmpty reports whether the target table has no rows. A
// missing table is treated as empty so the cold-start pre-flight
// doesn't double up with the subsequent CREATE TABLE IF NOT EXISTS
// step. Implements [ir.TableEmptyChecker].
//
// We use SELECT 1 ... LIMIT 1 rather than COUNT(*) so the cost is
// constant regardless of table size — the pre-flight only needs to
// know "is anything there", not "how many rows".
func (w *RowWriter) IsTableEmpty(ctx context.Context, table *ir.Table) (bool, error) {
	if table == nil {
		return false, errors.New("mysql: IsTableEmpty: table is nil")
	}
	stmt := "SELECT 1 FROM " + quoteIdent(table.Name) + " LIMIT 1"
	if w.schema != "" {
		stmt = "SELECT 1 FROM " + quoteIdent(w.schema) + "." + quoteIdent(table.Name) + " LIMIT 1"
	}
	var dummy int
	err := w.db.QueryRowContext(ctx, stmt).Scan(&dummy)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	// MySQL error 1146 ("Table 'x.y' doesn't exist") plus the Vitess
	// equivalent both contain "doesn't exist" in the message — that's
	// the simplest cross-flavor check without importing the driver's
	// error type into this package.
	if strings.Contains(err.Error(), "doesn't exist") {
		return true, nil
	}
	return false, fmt.Errorf("mysql: probe %q for emptiness: %w", table.Name, err)
}

// HasNullShardColumn reports whether the named discriminator column
// exists on the target table AND at least one existing row has it
// NULL. ADR-0048 Shape A populated-target preflight check (1);
// catalog Bug 81. Returns (false, nil) when:
//   - the column doesn't exist on the target (CompositePKLeadsWith
//     catches that case structurally), OR
//   - every row has the column NOT NULL (the legal Shape-A shape).
//
// A genuine engine error (permission denied, network drop) surfaces
// verbatim; the orchestrator wraps with operator-actionable context.
func (w *RowWriter) HasNullShardColumn(ctx context.Context, table *ir.Table, column string) (bool, error) {
	if table == nil {
		return false, errors.New("mysql: HasNullShardColumn: table is nil")
	}
	exists, err := w.columnExistsOnTarget(ctx, table.Name, column)
	if err != nil {
		return false, fmt.Errorf("mysql: HasNullShardColumn: %w", err)
	}
	if !exists {
		return false, nil
	}
	q := "SELECT 1 FROM " + quoteIdent(table.Name) +
		" WHERE " + quoteIdent(column) + " IS NULL LIMIT 1"
	var dummy int
	err = w.db.QueryRowContext(ctx, q).Scan(&dummy)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, fmt.Errorf("mysql: probe %q for NULL %q: %w", table.Name, column, err)
}

// ShardValuePresent reports whether the named discriminator column
// on the target table already has any row matching `value`. ADR-0048
// Shape A populated-target preflight check (2). Returns (false, nil)
// when the column is absent or no matching row exists. Catalog Bug 81.
func (w *RowWriter) ShardValuePresent(ctx context.Context, table *ir.Table, column string, value any) (bool, error) {
	if table == nil {
		return false, errors.New("mysql: ShardValuePresent: table is nil")
	}
	exists, err := w.columnExistsOnTarget(ctx, table.Name, column)
	if err != nil {
		return false, fmt.Errorf("mysql: ShardValuePresent: %w", err)
	}
	if !exists {
		return false, nil
	}
	q := "SELECT 1 FROM " + quoteIdent(table.Name) +
		" WHERE " + quoteIdent(column) + " = ? LIMIT 1"
	var dummy int
	err = w.db.QueryRowContext(ctx, q, value).Scan(&dummy)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, fmt.Errorf("mysql: probe %q for %q = %v: %w", table.Name, column, value, err)
}

// CompositePKLeadsWith reports whether the target table has a
// composite PRIMARY KEY (>1 column) whose leading column is the
// named discriminator. ADR-0048 Shape A populated-target preflight
// check (3) — the disjointness invariant the bypass rests on.
// Catalog Bug 81. Queries information_schema.statistics: PRIMARY
// KEY rows have INDEX_NAME='PRIMARY' and SEQ_IN_INDEX numbered from
// 1, so the row with SEQ_IN_INDEX=1 names the leading column.
func (w *RowWriter) CompositePKLeadsWith(ctx context.Context, table *ir.Table, column string) (bool, error) {
	if table == nil {
		return false, errors.New("mysql: CompositePKLeadsWith: table is nil")
	}
	const q = `
		SELECT  column_name,
		        (SELECT COUNT(*)
		         FROM   information_schema.statistics inner_s
		         WHERE  inner_s.table_schema = s.table_schema
		           AND  inner_s.table_name   = s.table_name
		           AND  inner_s.index_name   = 'PRIMARY') AS pk_len
		FROM    information_schema.statistics s
		WHERE   s.table_schema = DATABASE()
		  AND   s.table_name   = ?
		  AND   s.index_name   = 'PRIMARY'
		  AND   s.seq_in_index = 1`
	var leadName string
	var pkLen int
	err := w.db.QueryRowContext(ctx, q, table.Name).Scan(&leadName, &pkLen)
	if err == nil {
		return pkLen > 1 && leadName == column, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, fmt.Errorf("mysql: probe %q PK lead: %w", table.Name, err)
}

// columnExistsOnTarget is a small helper for the preflight probers —
// returns false when the table doesn't exist or the column doesn't
// exist on it; an unrelated query error surfaces verbatim.
func (w *RowWriter) columnExistsOnTarget(ctx context.Context, tableName, column string) (bool, error) {
	const q = `
		SELECT 1
		FROM   information_schema.columns
		WHERE  table_schema = DATABASE()
		  AND  table_name   = ?
		  AND  column_name  = ?
		LIMIT  1`
	var dummy int
	err := w.db.QueryRowContext(ctx, q, tableName, column).Scan(&dummy)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

// WriteRows consumes rows from the channel and inserts them into table
// using the strategy chosen at construction time. The method returns
// when the channel is closed (success) or when ctx is cancelled / a
// DB error occurs (failure).
//
// Caller responsibilities:
//   - Provide the channel; the writer drains it.
//   - Cancel ctx if iteration should stop early; without cancellation,
//     a writer with a partially-drained channel will block.
//   - Ensure table accurately describes the column types of the rows.
//     The writer trusts the [ir.Type] on each column to decide value
//     preparation (notably, []string-to-CSV for Set columns).
func (w *RowWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return fmt.Errorf("mysql: WriteRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("mysql: WriteRows: table %q has no columns", table.Name)
	}
	if rows == nil {
		return fmt.Errorf("mysql: WriteRows: rows channel is nil")
	}

	switch w.bulkLoad {
	case ir.BulkLoadLoadDataInfile:
		return w.writeLoadData(ctx, table, rows)
	case ir.BulkLoadBatchedInsert:
		return w.writeBatched(ctx, table, rows)
	case ir.BulkLoadNone:
		return errors.New("mysql: WriteRows: engine declares BulkLoad=None; cannot write rows")
	default:
		return fmt.Errorf("mysql: WriteRows: unknown BulkLoadMethod %v", w.bulkLoad)
	}
}

// writeBatched buffers rows and flushes them as a single multi-row
// INSERT statement using parameter placeholders. Letting the driver
// handle parameter encoding sidesteps the per-type escaping problems
// that custom SQL generation would face.
//
// The flush trigger (ADR-0150): the batch flushes when the ESTIMATED
// statement value bytes reach the ~1 MiB byte target — what
// pscale-cli's batcher does, the primary trigger — or on the row-count
// safety ceiling, whichever fires first (see [RowWriter.newInsertBatcher]
// for the full trigger derivation incl. the placeholder bound). The
// byte target is soft: a single row larger than it still ships as a
// one-row statement (the row's already in the batch when the
// post-append check fires); the target bounds *accumulation*, not
// individual rows — rows are never split and never refused for size.
func (w *RowWriter) writeBatched(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	// Pin a single connection for the whole batched write so the
	// post-flush warning check (Vector B) reads SHOW WARNINGS on the SAME
	// session that ran the INSERT — @@warning_count / SHOW WARNINGS are
	// session-scoped, and the pool could otherwise hand the probe a
	// different conn. This is the bulk path used whenever LOAD DATA isn't
	// (server local_infile=OFF, or a geometry column), so it's where a
	// relaxed-sql_mode silent clamp would otherwise go unreported. The
	// write was already sequential (one flush at a time), so pinning
	// loses no parallelism.
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: batched insert into %q: pin connection: %w", table.Name, err)
	}
	defer func() { _ = conn.Close() }()
	return w.writeBatchedConn(ctx, conn, table, rows)
}

// writeBatchedConn is the per-connection PLAIN batched-INSERT loop, shared
// by the serial [writeBatched] (one conn) and the parallel
// [WriteRowsParallel] (N conns, one per worker, ADR-0102). The caller owns
// pinning + closing conn. It is the plain-INSERT mirror of
// [writeBatchedIdempotentConn] — same batch/flush mechanics and Vector-B
// warning probe, plain INSERT instead of upsert.
//
// Concurrency note (ADR-0102): when N of these run on one RowWriter for a
// fan-out copy, they share w.warnedClamp (a sync.Map — safe). There is no
// mid-COPY durable watermark on the plain path (plain INSERT has no
// CopyDurableProgressReporter wiring), so no shared mutable progress state
// crosses workers. Each invocation keeps its own batch buffer and flush
// counter, so the per-call Vector-B sampling schedule is per-worker
// (defensible: a systematic clamp still trips every worker's first-N
// exhaustive flushes and its final flush).
func (w *RowWriter) writeBatchedConn(ctx context.Context, conn *sql.Conn, table *ir.Table, rows <-chan ir.Row) error {
	w.noteTierCPUBoundTarget(ctx)
	batch := w.newInsertBatcher(table)
	flushes := 0

	// final=true on the channel-close flush: the table's last flush is
	// always checked regardless of the sampling schedule below.
	flush := func(final bool) error {
		if batch.empty() {
			return nil
		}
		query := buildBatchInsert(table, len(batch.rows))
		args, err := flattenArgs(batch.rows, table)
		if err != nil {
			return fmt.Errorf("mysql: insert into %q: %w", table.Name, err)
		}
		// ADR-0108: ride a transient target primary-reparent / "not
		// serving". The exec + the session-scoped SHOW WARNINGS probe BOTH
		// run on the conn flushWithReparentRetry hands us — the originally
		// pinned conn on the first try, a FRESH pool conn (reconnected to
		// the new primary) on every retry. The dead post-reparent conn is
		// never reused.
		if err := w.flushWithReparentRetry(ctx, table.Name, len(batch.rows), func(c *sql.Conn, isRetry bool) error {
			if _, err := c.ExecContext(ctx, query, args...); err != nil {
				// WART: tolerate-1062-on-retry (ADR-0108). A plain cold-copy
				// batch is a SINGLE atomic multi-row INSERT. On a classified
				// transient (reparent / conn-reset) the prior attempt either
				// fully rolled back (this retry succeeds clean) OR
				// committed-but-the-ack-was-lost. In the lost-ack case the
				// retry re-applies the BYTE-IDENTICAL batch and collides with
				// the rows it already landed → Error 1062. Because cold-copy
				// is the SOLE writer onto a fresh target and the batch is
				// byte-identical, a 1062 *on the retry of the same batch*
				// PROVES those exact rows are already durable — so we treat
				// the batch as done and continue (no silent loss: the data is
				// there). This tolerance is SCOPED to isRetry only: a
				// FIRST-ATTEMPT 1062 stays terminal (a real non-PK uniqueness
				// violation / dirty target must fail loudly — unchanged
				// ADR-0038 policy). The idempotent path needs no such wart
				// (its UPSERT absorbs the collision).
				if isRetry && isMySQLDupKey(err) {
					slog.WarnContext(
						ctx, "mysql: cold-copy plain INSERT retry collided with a duplicate key (Error 1062); "+
							"the prior attempt committed but lost its ack across the transient — the rows already landed, "+
							"treating this byte-identical batch as durable (ADR-0108 tolerate-1062-on-retry)",
						slog.String("table", table.Name),
						slog.Int("rows", len(batch.rows)),
					)
					return nil
				}
				return fmt.Errorf("mysql: insert into %q (%d rows): %w", table.Name, len(batch.rows), err)
			}
			// Strict sql_mode rejects an out-of-range value at the INSERT
			// above (loud already); under --mysql-sql-mode='' MySQL silently
			// clamps/truncates and only flags it via the warning list — so
			// check after a successful flush (Vector B). Refuses (strict)
			// or WARNs once per table (relaxed). Sampled rather than
			// every-flush — see [warningsCheckDue] for the schedule and
			// what the sampling does and doesn't weaken. The flushes counter
			// drives sampling; bumped once per logical flush (here, inside
			// the attempt) so a retry of the same batch re-checks under the
			// same schedule slot rather than skewing the count.
			flushes++
			if final || warningsCheckDue(flushes) {
				return w.reportBulkWriteWarnings(ctx, c, table.Name)
			}
			return nil
		}, conn); err != nil {
			return err
		}
		if bulkFlushHookForTest != nil {
			bulkFlushHookForTest(len(batch.rows), batch.bytes)
		}
		batch.reset()
		return nil
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				return flush(true)
			}
			batch.add(row)
			if batch.full() {
				if err := flush(false); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Warning-check sampling schedule for the batched-INSERT bulk path
// (repo-audit M3.5). SHOW WARNINGS is a full round-trip per flush; on
// a cross-region PlanetScale target (~50 ms RTT) — the engine that can
// ONLY use batched INSERT — checking every flush adds up to ~2× per-
// flush latency (measured pre-ADR-0150 at ≤500-row flushes: a 19M-row
// table was ~38k flushes ≈ ~30 min of pure SHOW WARNINGS round-trips;
// the ~1 MiB statements cut the flush count ~10–20×, which shrinks the
// absolute cost but leaves the per-flush ratio). The schedule: the first
// [warningsExhaustiveFlushes] flushes are ALL checked (a systematic
// coercion — wrong column type, bad mapping — clamps on every flush
// and is caught immediately), then 1-in-[warningsSampleEvery], plus
// the table's final flush unconditionally.
//
// What sampling does NOT weaken: under strict sql_mode (the default),
// a conversion problem on this path errors the INSERT itself — the
// per-flush warning probe is defense-in-depth there, not the primary
// guard (the LOAD DATA path, where strict mode DOES downgrade errors
// to warnings, keeps its every-statement check in the load-data
// writer; so does the ≤10-flush-per-call idempotent resume path).
// What it does weaken, deliberately: under the operator's explicit
// --mysql-sql-mode=” relaxed opt-in, a SPARSE clamp confined to
// unsampled mid-table flushes can miss the advisory once-per-table
// WARN. That advisory exists to name coercions the operator already
// opted into; trading completeness of the advisory for ~30 min on
// large cross-region loads is the documented call.
const (
	warningsExhaustiveFlushes = 10
	warningsSampleEvery       = 16
)

// warningsCheckDue reports whether the flushNum-th (1-based) flush is
// on the sampling schedule. The caller additionally forces the final
// flush.
func warningsCheckDue(flushNum int) bool {
	return flushNum <= warningsExhaustiveFlushes || flushNum%warningsSampleEvery == 0
}

// buildBatchInsert returns the parameterised INSERT statement for the
// given table and row count. Generated columns are excluded — the
// reader doesn't emit values for them, and INSERT into a generated
// column is a database error. Identifiers are backtick-quoted;
// values are placeholders (`?`) for the driver to fill in.
func buildBatchInsert(table *ir.Table, rowCount int) string {
	cols := nonGeneratedColumns(table.Columns)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = quoteIdent(c.Name)
	}

	rowPart := buildRowPlaceholder(len(cols))
	rowParts := make([]string, rowCount)
	for i := range rowParts {
		rowParts[i] = rowPart
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		quoteIdent(table.Name),
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)
}

// buildRowPlaceholder returns a single row's "(?, ?, ...)" placeholder
// fragment for a row with the given column count. Returns "()" for a
// zero-column row, which is invalid SQL — the caller should validate
// upstream.
func buildRowPlaceholder(numCols int) string {
	if numCols <= 0 {
		return "()"
	}
	if numCols == 1 {
		return "(?)"
	}
	return "(" + strings.Repeat("?, ", numCols-1) + "?)"
}

// flattenArgs walks the batch column-major-by-row and produces the
// flat []any the driver expects. Values are passed through prepareValue
// to handle the IR-Set-to-string conversion and similar adjustments.
// Generated columns are skipped so the column-list and value-list
// stay in lockstep with buildBatchInsert. A prepareValue refusal (the
// SLUICE-E-VALUE-UNREPRESENTABLE float guard) surfaces here naming the
// offending row's position in the batch.
func flattenArgs(batch []ir.Row, table *ir.Table) ([]any, error) {
	cols := nonGeneratedColumns(table.Columns)
	args := make([]any, 0, len(batch)*len(cols))
	for i, row := range batch {
		for _, col := range cols {
			v, err := prepareValue(row[col.Name], col)
			if err != nil {
				return nil, fmt.Errorf("row %d of batch: %w", i+1, err)
			}
			args = append(args, v)
		}
	}
	return args, nil
}

// prepareValue adjusts a Row value to a form the driver accepts.
//
// Most IR-canonical Go values pass through to go-sql-driver/mysql
// unchanged: int64, uint64, float64, string, []byte, bool, time.Time,
// and nil all serialise correctly without intervention. The exceptions:
//
//   - [ir.Set] values are []string in IR but MySQL expects a
//     comma-separated string literal.
//   - [ir.Geometry] values are raw WKB bytes (per docs/value-types.md);
//     MySQL's wire format is `<srid uint32 LE><wkb>`. We prepend the
//     SRID using the column's declared SRID (or 0 when unset).
//   - [ir.JSON] values are []byte in IR (the raw JSON document
//     per docs/value-types.md). go-sql-driver/mysql labels []byte
//     parameters with `_binary` charset on the wire, which Vitess
//     rejects with "Cannot create a JSON value from a string with
//     CHARACTER SET 'binary'" when the destination column is JSON.
//     Convert to a Go string so the driver sends VARCHAR (no
//     charset prefix) and MySQL/Vitess parses it as JSON cleanly.
//     Real-world bug found during PlanetScale-target testing.
//   - [ir.JSON] target with a `[]any` value: PG arrays decode to
//     `[]any` in the IR, and operators sometimes use
//     `--type-override=col=jsonb` to land them as JSON on MySQL.
//     Without conversion, the driver receives the slice and bombs
//     with "Invalid JSON text"; we marshal to a JSON array literal
//     instead. (Bug 14, v0.4.0 type-override regression.)
//   - [ir.JSON] target with a PG-array-literal string ("{a,b,c}"):
//     same scenario when the source presented the array as text
//     rather than []any. We parse the literal and re-emit as JSON.
//
// The col parameter carries the column's IR descriptor. The post-
// translate Type drives the branch dispatch; [ir.Column.SourceColumnType]
// is consulted only to disambiguate the literal `{}` bytes case
// (Bug 47 vs Bug 14 — see [convertArrayLikeToJSON]). A nil col is
// tolerated: the value passes through unchanged, matching the
// pre-Bug-6 fallback shape and keeping defensive applier callers
// safe.
//
// The error return is the SLUICE-E-VALUE-UNREPRESENTABLE refusal
// ([refuseUnrepresentableFloat]) — the one value class MySQL cannot hold
// under ANY statement protocol. It fires before the driver ever sees the
// value; every other branch is infallible.
func prepareValue(v any, col *ir.Column) (any, error) {
	if err := refuseUnrepresentableFloat(v, col); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	if col == nil {
		// Type-free negative-zero encoding for the nil-descriptor path
		// (the applier's cache-miss/unknown-column branches): without the
		// column type, the ir.Float-gated wart below can't fire, and an
		// interpolated −0.0 would mangle to +0 while the binary protocol
		// preserves the sign — the exact divergence the wart exists to
		// close. Encoding '-0' type-free is safe here: for float-family
		// targets the string→double conversion preserves the sign on both
		// protocols, and for every other numeric target the string '-0'
		// converts to the same zero a bound −0.0 double does (DECIMAL/INT
		// have no signed zero). The one theoretical asymmetry — a float64
		// −0.0 bound to a JSON column through a cache miss — parses '-0'
		// as a JSON number either way (MySQL coerces string literals to
		// JSON by parsing them), so no cleaner-behaving alternative
		// exists.
		if f, ok := v.(float64); ok && f == 0 && math.Signbit(f) {
			return "-0", nil
		}
		return v, nil
	}
	t := col.Type
	// Bug 122 (v0.95.3): an ir.Domain column in the schema unwraps to
	// its base type for value-prep purposes. The MySQL retarget layer
	// downgrades the DOMAIN to its base type in the COLUMN DDL emit
	// path, but the COLUMN-list passed to prepareValue carries the
	// source's IR types verbatim. Synthesize a column whose Type is
	// the base type and recurse so every downstream branch
	// (Set / JSON / Array / Time-stripping / scalar passthrough)
	// reaches its existing case.
	if dom, isDomain := t.(ir.Domain); isDomain {
		if dom.BaseType == nil {
			return v, nil
		}
		baseCol := *col
		baseCol.Type = dom.BaseType
		return prepareValue(v, &baseCol)
	}
	if _, isSet := t.(ir.Set); isSet {
		if ss, ok := v.([]string); ok {
			return strings.Join(ss, ","), nil
		}
	}
	// ADR-0153 named wart — the negative-zero literal mangle. MySQL's SQL
	// parser reads the literal `-0` as unary minus applied to an unsigned
	// zero, which evaluates to PLAIN +0 — so client-side interpolation
	// (which renders a float64 -0.0 arg as the literal -0) silently drops
	// the IEEE sign bit that the binary protocol's raw bits preserve. That
	// is a per-value silent divergence between the two statement protocols
	// (caught by the Phase-1 family matrix of the N-15a flip). Encoding
	// negative zero as the STRING '-0' routes it through MySQL's
	// string→double DATA CONVERSION instead of the expression parser,
	// which preserves the sign — identically on BOTH protocols, keeping
	// them byte-exact. Pinned by
	// TestInterpolation_BulkWrite_FamilyMatrix_ByteExact and
	// TestInterpolation_CDCApply_FamilyMatrix_ByteExact.
	if _, isFloat := t.(ir.Float); isFloat {
		if f, ok := v.(float64); ok && f == 0 && math.Signbit(f) {
			return "-0", nil
		}
	}
	// MySQL has no native array type, so emitColumnType (ddl_emit.go)
	// renders an ir.Array source column as a MySQL `JSON` column —
	// the IR keeps the source type ir.Array, only the emitted DDL is
	// JSON. The value side must follow the same decision: a PG array
	// arrives from the PG RowReader as a Go []any (string/int64/nil
	// elements, possibly nested for multi-dim arrays), which the JSON
	// column accepts only as its JSON text form. Treat ir.Array
	// exactly like ir.JSON here (Bug 18); without this the []any falls
	// through to tsvEncode and crashes the LOAD DATA serializer.
	_, isJSON := t.(ir.JSON)
	_, isArray := t.(ir.Array)
	if isJSON || isArray {
		if converted, ok := convertArrayLikeToJSON(v, col); ok {
			return converted, nil
		}
		if b, ok := v.([]byte); ok {
			return string(b), nil
		}
	}
	// catalog Bug 71: a PG `timetz` (ir.Time{WithTimeZone:true}) value
	// arrives from the PG reader as its canonical text form including
	// the zone offset ("13:45:30+05"). MySQL has no tz-aware TIME — the
	// column was emitted as plain MySQL TIME (the documented
	// zone-flatten cross-engine policy, mirroring timestamptz→MySQL).
	// MySQL's TIME parser rejects the offset suffix (Error 1292), so we
	// strip it here, leaving the time-of-day. Plain `time`
	// (WithTimeZone:false) is untouched.
	if tt, isTime := t.(ir.Time); isTime && tt.WithTimeZone {
		if s, ok := v.(string); ok {
			return stripTimeZoneOffset(s), nil
		}
		if b, ok := v.([]byte); ok {
			return stripTimeZoneOffset(string(b)), nil
		}
	}
	// task #72 / Bug-74 family pin: cross-engine trigger-CDC temporal
	// values. The postgres-trigger CDC reader (cdc_reader.go) decodes a
	// captured timestamp/timestamptz column into an ISO TEXT string, NOT
	// the time.Time the pgoutput path's value_decode.go produces. For a
	// MySQL DATETIME (PG `timestamp`) the bare ISO string is accepted by
	// the driver as-is — but a PG `timestamptz` (ir.Timestamp{WithTimeZone})
	// carries a numeric zone offset suffix ("2026-02-02 02:02:02.020202+00")
	// that MySQL's TIMESTAMP/DATETIME parser rejects with Error 1292,
	// exactly like the timetz case above. The documented cross-engine
	// policy flattens the zone (timestamptz → MySQL TIMESTAMP stored in
	// the session zone); strip the offset so the time-of-day lands. A
	// time.Time value (same-engine / pgoutput path) is untouched. Plain
	// ir.DateTime strings have no offset, so stripTimeZoneOffset is a
	// no-op there — but we route them through it too so a stray offset on
	// a DateTime-typed column (defensive) doesn't crash the apply.
	switch t.(type) {
	case ir.Timestamp, ir.DateTime:
		if s, ok := v.(string); ok {
			return stripTimeZoneOffset(s), nil
		}
		if b, ok := v.([]byte); ok {
			return stripTimeZoneOffset(string(b)), nil
		}
	}
	// task #72 / Bug-74 family pin: cross-engine trigger-CDC bytea values.
	// The postgres-trigger CDC reader emits a captured `bytea` column as
	// PG's `\x`-hex TEXT (e.g. "\xdeadbeef") — the JSON-scalar string form
	// — NOT the raw []byte the pgoutput path's value_decode.go hex-decodes.
	// A MySQL VARBINARY / BLOB column bound with that string stores the
	// literal ASCII of the hex text (10 bytes "\xdeadbeef") instead of the
	// 4 raw bytes 0xDEADBEEF: SILENT bytea corruption, the Bug-92 class.
	// Hex-decode the `\x`-prefixed text here so the raw bytes land. Raw
	// []byte (same-engine / pgoutput path) and non-`\x` strings pass
	// through unchanged.
	switch t.(type) {
	case ir.Binary, ir.Varbinary, ir.Blob:
		if s, ok := v.(string); ok {
			if b, decoded := decodeHexByteaText(s); decoded {
				return b, nil
			}
		}
	}
	if geom, isGeom := t.(ir.Geometry); isGeom {
		if b, ok := v.([]byte); ok {
			out := make([]byte, 4+len(b))
			// Little-endian uint32 SRID prefix, matching MySQL's
			// on-wire geometry layout.
			srid := uint32(geom.SRID)
			out[0] = byte(srid)
			out[1] = byte(srid >> 8)
			out[2] = byte(srid >> 16)
			out[3] = byte(srid >> 24)
			copy(out[4:], b)
			return out, nil
		}
	}
	// ADR-0032: PG extension passthrough types. The MySQL writer's
	// emitColumnType rewrites these to a native MySQL shape (hstore →
	// JSON; citext → VARCHAR _ci). The values need matching adjustment:
	//
	//   - hstore values arrive from the PG reader as PG-canonical text
	//     ("\"key\"=>\"value\", \"k2\"=>\"v2\""). MySQL JSON columns
	//     reject that wire form — parse the hstore text and re-emit as
	//     a JSON object string. NULL values in hstore (the unquoted
	//     keyword NULL on the value side) map to JSON null.
	//   - citext values are plain strings; no value translation needed
	//     (the case-insensitive comparison is a server-side property
	//     of the collation, not the wire format).
	if ext, isExt := t.(ir.ExtensionType); isExt {
		switch ext.Extension {
		case "hstore":
			return prepareHstoreToJSON(v), nil
		case "citext":
			// Identity — string passes through.
			if b, ok := v.([]byte); ok {
				return string(b), nil
			}
		}
	}
	// catalog Bug 77 (was Bug 75): an ir.Bit value is the IR-canonical
	// '0'/'1' bit-string. Bind it to MySQL's BIT(N) column as the
	// unsigned integer value, NOT the ceil(N/8) big-endian []byte: the
	// byte form is bound by go-sql-driver as a binary string and
	// MySQL's string→BIT coercion raises 1264/22003 for some byte
	// patterns even when the value fits N bits (Bug 75's fix passed
	// review only because its pinned values were all ≤1 byte). The
	// integer form round-trips for every width 2..64 and matches what
	// the LOAD DATA writer already does (CONV(v,2,10) AS UNSIGNED). A
	// malformed bit string is an upstream decode bug; we let the engine
	// reject the raw value rather than silently substituting a wrong one.
	if _, isBit := t.(ir.Bit); isBit {
		if s, ok := v.(string); ok {
			if n, err := ir.BitStringToUint64(s); err == nil {
				return n, nil
			}
		}
	}
	return v, nil
}

// refuseUnrepresentableFloat is the SLUICE-E-VALUE-UNREPRESENTABLE guard
// (ADR-0153): MySQL FLOAT/DOUBLE cannot hold NaN or ±Inf under ANY statement
// protocol, and the two protocols fail in DIFFERENT downstream shapes — the
// binary protocol's IEEE bits draw a terminal server error, but client-side
// interpolation renders the literal `NaN`, which MySQL rejects as Error 1054
// ER_BAD_FIELD_ERROR ("Unknown column 'NaN'"), the SAME code the applier's
// schema-drift self-healing (Bug F8) deliberately classifies as RETRIABLE —
// so an unguarded NaN would spin the cold-copy reparent-retry window (~30
// min) or a CDC sync's drift backoff FOREVER instead of failing loudly.
// (PG DOUBLE PRECISION legitimately carries NaN/Infinity, so a PG→MySQL run
// can absolutely feed one in.) Refusing HERE, before the driver sees the
// value, makes both protocols fail identically, immediately, naming the
// column and the remedy.
func refuseUnrepresentableFloat(v any, col *ir.Column) error {
	f, ok := v.(float64)
	if !ok || (!math.IsNaN(f) && !math.IsInf(f, 0)) {
		return nil
	}
	colName := "(unknown column)"
	if col != nil {
		colName = col.Name
	}
	return fmt.Errorf(
		"SLUICE-E-VALUE-UNREPRESENTABLE: column %q carries the float64 value %v, which no MySQL column type "+
			"can represent (MySQL has no NaN/Infinity); refusing loudly rather than corrupting the value or "+
			"retry-looping on the server's misleading error — filter or transform the source value "+
			"(e.g. NULLIF / CASE on the source query)",
		colName, f,
	)
}

// stripTimeZoneOffset removes a trailing timezone offset from a PG
// `timetz` text value ("13:45:30+05", "08:00:00-07:30",
// "23:59:59.123456+00") so it is accepted by a MySQL TIME column
// (catalog Bug 71 — MySQL has no tz-aware TIME; the zone is dropped
// per the documented cross-engine policy). The offset sign is the
// first '+' or '-' after the "HH:MM:SS" head (offset 8+); it never
// collides with the time digits or the fractional dot. A value with no
// offset passes through unchanged.
//
// task #72 reuse: the same shape strips a `timestamptz` text value's
// trailing offset ("2026-02-02 02:02:02.020202+00"). The offset never
// appears before index 8 (a "YYYY-MM-DD HH:..." prefix is >= 11 chars),
// and the only '-' chars in the date portion are at offsets 4 and 7, so
// starting the scan at 8 skips them and finds only the zone sign.
func stripTimeZoneOffset(s string) string {
	for i := 8; i < len(s); i++ {
		if s[i] == '+' || s[i] == '-' {
			return s[:i]
		}
	}
	return s
}

// decodeHexByteaText recognises PG's `bytea_output = hex` text form (a
// `\x` prefix followed by an even-length hex string) and returns the
// decoded raw bytes. The bool is false when s is not in that form so the
// caller falls back to passing the value through unchanged.
//
// task #72 / Bug-74 bytea family pin: the postgres-trigger CDC reader
// surfaces a captured bytea value as this `\x`-hex TEXT (the JSON-scalar
// string form), which must be decoded to raw bytes before binding to a
// MySQL VARBINARY / BLOB column — otherwise the literal ASCII of the hex
// text is stored (silent corruption). Mirrors the postgres engine's
// value_decode.go decodeHexByteaText (kept duplicated; the mysql package
// does not import the postgres engine).
func decodeHexByteaText(s string) ([]byte, bool) {
	const prefix = `\x`
	if !strings.HasPrefix(s, prefix) {
		return nil, false
	}
	body := s[len(prefix):]
	if len(body)%2 != 0 {
		return nil, false
	}
	b, err := hex.DecodeString(body)
	if err != nil {
		return nil, false
	}
	return b, true
}

// prepareHstoreToJSON converts a PG hstore wire value into a JSON
// object string suitable for a MySQL JSON column. The cross-engine
// PG → MySQL translator (ADR-0032 § "Cross-engine policy") declares
// hstore's default MySQL form as JSON; this is the value side of that
// declaration.
//
// Input shapes:
//   - string / []byte in PG hstore canonical text form:
//     `"key1"=>"value1", "key2"=>"value2"`, with backslash-escaped
//     interior quotes (`"a\"b"=>"v"`). Unquoted NULL on the value
//     side is the SQL null marker, mapped to JSON null.
//   - empty string → empty JSON object `{}`.
//
// On any parse failure the input bytes pass through unchanged; the
// downstream driver will raise an unambiguous "Invalid JSON text"
// error citing the offending value. The loud-failure path is
// preferred over emitting "best-effort" malformed JSON.
func prepareHstoreToJSON(v any) any {
	var s string
	switch b := v.(type) {
	case string:
		s = b
	case []byte:
		s = string(b)
	default:
		return v
	}
	pairs, err := parseHstoreText(s)
	if err != nil {
		// Parse failed — fall through with original bytes; the driver
		// will surface its own error rather than this helper masking it.
		return v
	}
	encoded, err := json.Marshal(pairs)
	if err != nil {
		return v
	}
	return string(encoded)
}

// parseHstoreText parses a PG hstore canonical text representation
// into a key→value map suitable for JSON serialization. The grammar
// is described in PG's docs ("hstore Input and Output"):
//
//   - Each key/value pair is `"key"=>"value"` with double-quoted
//     strings on both sides.
//   - Pairs are separated by `, ` (comma + space; tolerate either).
//   - Interior quotes are backslash-escaped (`\"` is a literal quote
//     in the key/value); literal backslashes are `\\`.
//   - The unquoted keyword `NULL` (case-insensitive) on the value side
//     is the SQL null marker. Keys cannot be NULL — PG hstore enforces
//     that on insert.
//   - The empty hstore is the empty string `""` (no braces).
//
// The returned map preserves PG's "last write wins" semantics for
// duplicate keys — Go maps overwrite on assignment, which matches
// PG's hstore behaviour.
//
// Returns an error for malformed input so the caller can fall back
// to loud-failure rather than emit partial JSON. The parser is
// strict on the brace-less canonical form; hstore values inside a
// PG array (`{...}` envelope) are not supported — those would need
// the array text-form parser ahead of this one.
func parseHstoreText(s string) (map[string]any, error) {
	out := map[string]any{}
	i := 0
	skipSpace := func() {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
	}
	// readQuoted consumes a `"..."` literal with backslash escapes,
	// returning the unescaped string body and advancing i past the
	// closing quote.
	readQuoted := func() (string, error) {
		if i >= len(s) || s[i] != '"' {
			return "", fmt.Errorf("hstore: expected '\"' at offset %d in %q", i, s)
		}
		i++
		var sb []byte
		for i < len(s) {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				sb = append(sb, s[i+1])
				i += 2
				continue
			}
			if c == '"' {
				i++
				return string(sb), nil
			}
			sb = append(sb, c)
			i++
		}
		return "", fmt.Errorf("hstore: unterminated quoted string in %q", s)
	}
	for {
		skipSpace()
		if i >= len(s) {
			break
		}
		key, err := readQuoted()
		if err != nil {
			return nil, err
		}
		skipSpace()
		// Expect `=>`.
		if i+1 >= len(s) || s[i] != '=' || s[i+1] != '>' {
			return nil, fmt.Errorf("hstore: expected '=>' at offset %d in %q", i, s)
		}
		i += 2
		skipSpace()
		// Value: either an unquoted NULL (case-insensitive) or a
		// quoted string.
		switch {
		case i < len(s) && s[i] == '"':
			val, err := readQuoted()
			if err != nil {
				return nil, err
			}
			out[key] = val
		case i+4 <= len(s) && strings.EqualFold(s[i:i+4], "NULL"):
			out[key] = nil
			i += 4
		default:
			return nil, fmt.Errorf("hstore: expected '\"' or NULL at offset %d in %q", i, s)
		}
		skipSpace()
		// Either end of input or a comma separator.
		if i >= len(s) {
			break
		}
		if s[i] != ',' {
			return nil, fmt.Errorf("hstore: expected ',' or end at offset %d in %q", i, s)
		}
		i++
	}
	return out, nil
}

// convertArrayLikeToJSON detects values that look like PG arrays
// landing on a MySQL JSON column (Bug 14) and re-encodes them as a
// JSON array string the driver can hand to MySQL's JSON parser.
//
// Three input shapes are recognised:
//
//   - []any — the canonical IR shape for an [ir.Array] value
//     decoded by the postgres reader against an ir.Array column.
//     Marshalled element-by-element via [encoding/json].
//   - string of the form "{...}" — the PG array text literal that
//     surfaces when the override leaves the value as a string (e.g.
//     CDC tuple values, or some database/sql scan paths). Parsed
//     into tokens and emitted as a JSON string array; nested
//     arrays are not supported (the IR doesn't model multi-
//     dimensional arrays today and the parser refuses them).
//   - []byte starting with `{` and ending with `}` — same shape
//     as above when the source decodes the value as bytes (the
//     [decodeValue] path for [ir.JSON] returns []byte and so when
//     the column was *overridden* to JSON post-decode, the row
//     reader's bytes path produces a `[]byte` containing the PG
//     array literal verbatim). Routed through the same parser as
//     the string case.
//
// Returns (jsonString, true) on a recognised shape; (nil, false)
// when the value doesn't match either, signalling the caller to
// fall back to the next branch in [prepareValue].
//
// Errors during marshalling are swallowed: returning the un-
// converted value is safe because the next branch (the []byte →
// string branch) handles bytes correctly, and the driver's own
// error handling will report any genuinely-bad value the operator
// supplied.
//
// The col parameter carries the column's pre-override source type
// in [ir.Column.SourceColumnType]; the `[]byte` branch consults it
// to disambiguate the literal `{}` shape — see the branch comment
// for Bug 47 (round-trip empty JSON object) vs Bug 14 (override
// empty PG array landing as JSON).
func convertArrayLikeToJSON(v any, col *ir.Column) (any, bool) {
	switch shaped := v.(type) {
	case map[string]any:
		// Cross-engine trigger-CDC path (task #72 / Bug-74 family pin):
		// the postgres-trigger CDC reader decodes a jsonb capture-log
		// value into a nested map[string]any (cdc_reader.go
		// decodeJSONBRow), NOT the []byte the pgoutput path produces. A
		// bare map reaches go-sql-driver as reflect.Map, which it rejects
		// ("unsupported type map[string]interface {}, a map") — a LOUD
		// cross-engine failure the same-engine PG path never hit (pgx
		// marshals a map to jsonb natively). Marshal it to a JSON object
		// string here, the same shape the []any branch produces for a
		// top-level JSON array. json.Number leaves marshal to their exact
		// numeric text (no float widening), preserving numeric precision
		// inside the document.
		out, err := json.Marshal(shaped)
		if err != nil {
			return nil, false
		}
		return string(out), true
	case []any:
		out, err := json.Marshal(shaped)
		if err != nil {
			return nil, false
		}
		return string(out), true
	case string:
		if !looksLikePGArrayLiteral(shaped) {
			return nil, false
		}
		converted, err := pgArrayLiteralToJSON(shaped)
		if err != nil {
			return nil, false
		}
		return converted, true
	case []byte:
		// A `[]byte` for a JSON-target column is normally already
		// valid JSON (the canonical IR shape for ir.JSON values).
		// The PG-array-literal-as-bytes case only arises when the
		// column was overridden to JSON post-decode and the value
		// arrived from the source's text-form array reader.
		//
		// Disambiguation: try PG-array parsing for non-empty
		// `{...}` bytes. The PG array grammar is strict — JSON
		// objects with quoted keys and colons fail to parse — so a
		// successful parse is high signal that the bytes are an
		// array literal. For non-`{...}` JSON bytes, the PG-array
		// parse fails and we fall through to the next branch
		// (which emits the bytes as a string).
		//
		// The corner case is `{}`, which is BOTH an empty JSON
		// object and an empty PG array literal. Two converging
		// real-world paths arrive at `[]byte("{}")` on a JSON-target
		// column:
		//   - Bug 47: MySQL JSON source column with value `{}`. The
		//     correct round-trip is the JSON object `{}`. The
		//     column has no `SourceColumnType` (no override fired).
		//   - Bug 14: PG `text[]` source with `--type-override=jsonb`
		//     and an empty array value. The correct landing is the
		//     JSON array `[]`. The column's `SourceColumnType` is
		//     an [ir.Array].
		// We discriminate on `SourceColumnType`: only when the
		// pre-override type is an array do we route `{}` through
		// the array→JSON parser. Otherwise the bytes fall through
		// to the next branch in [prepareValue] which emits `"{}"`
		// (the JSON-object preservation Bug 47 demands).
		if len(shaped) < 2 || shaped[0] != '{' || shaped[len(shaped)-1] != '}' {
			return nil, false
		}
		if len(shaped) == 2 {
			// Empty `{}`. Only treat as PG empty array when an
			// override carries that intent in SourceColumnType.
			if col != nil {
				if _, fromArray := col.SourceColumnType.(ir.Array); fromArray {
					return "[]", true
				}
			}
			return nil, false
		}
		if converted, err := pgArrayLiteralToJSON(string(shaped)); err == nil {
			return converted, true
		}
		return nil, false
	}
	return nil, false
}

// looksLikePGArrayLiteral is a cheap pre-filter: a Postgres array
// text literal is wrapped in braces. Any string starting with `{`
// and ending with `}` is a candidate; the actual parse rejects
// non-array shapes (JSON objects, malformed input).
func looksLikePGArrayLiteral(s string) bool {
	return len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}'
}

// pgArrayLiteralToJSON parses a Postgres one-dimensional array text
// literal ("{a,b,c}", "{\"x\",\"y\"}") and returns its JSON-array
// equivalent. Element values are emitted as JSON strings — the
// translation is a string-array view of the array, not a typed
// re-decode. Operators who need typed-element translation (numeric
// arrays as JSON numbers, etc.) should override to `text` instead
// and parse downstream.
func pgArrayLiteralToJSON(s string) (string, error) {
	if !looksLikePGArrayLiteral(s) {
		return "", fmt.Errorf("not a PG array literal: %q", s)
	}
	body := s[1 : len(s)-1]
	if body == "" {
		return "[]", nil
	}
	tokens, err := splitPGArrayTokens(body)
	if err != nil {
		return "", err
	}
	out, err := json.Marshal(tokens)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// splitPGArrayTokens parses the inside of a PG array literal into
// per-element string tokens. Mirrors the postgres engine's
// parsePGArrayText (which lives in a different package and isn't
// imported by mysql); kept narrow because the only thing we do
// with the tokens is JSON-serialise them.
//
// Format reference: PostgreSQL "Array Input and Output Syntax".
// Quoted elements are unescaped; bare elements are taken verbatim;
// the literal NULL (case-insensitive) maps to JSON null. Multi-
// dimensional arrays (nested braces) are rejected.
func splitPGArrayTokens(body string) ([]any, error) {
	var out []any
	for i := 0; i < len(body); {
		// Skip leading whitespace.
		for i < len(body) && (body[i] == ' ' || body[i] == '\t') {
			i++
		}
		if i >= len(body) {
			break
		}
		if body[i] == '{' || body[i] == '}' {
			return nil, fmt.Errorf("nested arrays not supported in PG array literal")
		}

		var (
			tok    string
			isNull bool
		)
		if body[i] == '"' {
			i++ // opening quote
			var sb []byte
			for i < len(body) {
				c := body[i]
				if c == '\\' && i+1 < len(body) {
					sb = append(sb, body[i+1])
					i += 2
					continue
				}
				if c == '"' {
					i++ // closing quote
					break
				}
				sb = append(sb, c)
				i++
			}
			tok = string(sb)
		} else {
			start := i
			for i < len(body) && body[i] != ',' {
				i++
			}
			tok = body[start:i]
			for tok != "" && (tok[len(tok)-1] == ' ' || tok[len(tok)-1] == '\t') {
				tok = tok[:len(tok)-1]
			}
			if strings.EqualFold(tok, "NULL") {
				isNull = true
			}
		}
		// Skip trailing whitespace, then the comma.
		for i < len(body) && (body[i] == ' ' || body[i] == '\t') {
			i++
		}
		if i < len(body) {
			if body[i] != ',' {
				return nil, fmt.Errorf("expected ',' or '}' at offset %d", i)
			}
			i++
		}

		if isNull {
			out = append(out, nil)
		} else {
			out = append(out, tok)
		}
	}
	return out, nil
}
