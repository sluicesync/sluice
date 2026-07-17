// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// # Pipelined CDC apply for Postgres (ADR-0092)
//
// The shared batch loop ([appliershared.RunOneBatch]) applies a batch of
// N changes as N serial tx.ExecContext round trips, then a position
// write, then a commit — N+2 round trips. On a high-latency link
// (measured ~7ms RTT on the PlanetScale soak) that caps apply throughput
// at ~1/RTT regardless of batch size, because batching only amortises the
// commit fsync, not the per-row execs.
//
// This file replaces the serial execs on the PG BATCH path with a single
// pipelined pgx.Batch flush: every data statement AND the position upsert
// are queued onto one pgx.Batch and sent in ONE network flush; the server
// executes them in order and the results are read back together, then the
// transaction commits. Round trips per batch drop from N+2 to ~3 (begin,
// the batched flush, commit), independent of N.
//
// The load-bearing correctness invariant: pipelining changes WHEN
// statements are sent, never HOW a value is encoded. Every queued
// statement is built by the SAME build{Insert,Update,Delete}SQL builders
// and prepareApplierValue codec path the serial exec path uses; and the
// pipelined pool runs in pgx's QueryExecModeDescribeExec (see
// [openPgxDBDescribeExec]), under which SendBatch describes each DISTINCT
// queued statement FRESH via an unnamed prepare (pgx passes a nil
// statement-description cache → no client cache, never a stale OID), then
// binds + executes every statement with the real described parameter OID in
// BINARY — byte-IDENTICAL value encoding to the serial CacheStatement path
// the applier's primary pool uses. (An Exec-mode pool would instead send
// OID-0 TEXT, a *different* wire encoding; DescribeExec is what makes the
// "only WHEN, never HOW" claim literally true and inherits the serial
// path's already-trusted per-OID binary codecs.) The
// differential/value-fidelity pins (change_applier_pipelined_integration_test.go)
// are the oracle.
//
// GAP #3 (ADR-0091) interaction: because DescribeExec re-describes every
// distinct statement fresh against the live catalog within the single flush
// (and caches nothing), a column widened by a forwarded ALTER TYPE
// (int4→bigint) is bound against the live post-DDL OID, never a stale
// cached pre-DDL OID — which subsumes the schemaDirtyTables special-casing
// the serial path needs (no QueryExecModeExec arg-prepend is queued;
// pgx.Batch does not honour a per-query exec mode, the connection's default
// DescribeExec governs the whole flush).
//
// Conn lifecycle: BeginTx checks out one *sql.Conn from the dedicated
// pipelined pool and escapes it to the underlying *pgx.Conn exactly as the
// raw-COPY path does (Conn.Raw → *stdlib.Conn.Conn()). That backend is
// pinned for the batch's lifetime and released at commit/rollback — the
// same resource shape as writeViaCopy. If the escape ever fails (a
// non-pgx driver / wrapped conn), BeginTx logs a one-time WARN and falls
// back to the serial *sql.Tx path — loud, never silent, no throughput
// claim.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// queuedStmt records the change context of one statement queued onto the
// pipelined batch so a per-statement execution error read back at commit
// can be attributed to the exact change (schema.table + kind), matching
// the serial path's per-change error wrapping.
type queuedStmt struct {
	schema string
	table  string
	kind   string // "insert" / "update" / "delete" / "truncate" / "schema-snapshot" / "position"
}

// pgxBatchTx is the ADR-0092 pipelined-apply transaction handle. It pins
// one *sql.Conn (escaped to *pgx.Conn), runs a native pgx.Tx, and
// accumulates the batch's statements onto an in-handle pgx.Batch that is
// flushed in a single SendBatch at commit. It satisfies
// [appliershared.BatchTx]; the loop only ever calls Rollback on it,
// while the applier's Dispatch / WritePosition / Commit closures
// type-assert it to queue and flush.
type pgxBatchTx struct {
	a *ChangeApplier

	// ctx is the batch's context, captured at BeginTx. The shared loop's
	// Commit closure takes no ctx (the serial path's commitWithTimeout
	// doesn't either), but pgx's SendBatch / Tx.Commit require one; the
	// batch lives entirely within a single RunOneBatch call under this
	// one ctx, so storing it on the handle is the correct lifetime.
	ctx context.Context

	sqlConn *sql.Conn  // pinned backend, returned to pool on release
	conn    *pgx.Conn  // native escape of sqlConn, valid until release
	tx      pgx.Tx     // native tx on conn
	batch   *pgx.Batch // accumulates data statements + position upsert
	stmts   []queuedStmt

	released bool
}

// beginPipelinedTx opens a pipelined batch transaction: checks out a
// backend from the DescribeExec-mode pool, escapes it to *pgx.Conn, begins a
// pgx.Tx, and pins synchronous_commit = on (the ADR-0007 F7 durability
// pin, preserved). On a usable handle it returns a *pgxBatchTx; on an
// escape failure it returns (nil, errPipelineUnavailable) so the caller
// can fall back to the serial path. Any other error (pool acquire / begin
// / SET) is returned as a genuine error (already classified by the
// caller) — those are real failures, not "pipelining unsupported".
func (a *ChangeApplier) beginPipelinedTx(ctx context.Context) (*pgxBatchTx, error) {
	db, err := a.pipelinePool()
	if err != nil {
		return nil, err
	}
	return a.beginPipelinedTxOn(ctx, db)
}

// beginPipelinedTxOn opens a pipelined batch transaction on a SPECIFIC
// DescribeExec-mode pool. It is the body of [beginPipelinedTx] parameterised
// on the pool so the single-lane batch path (the shared [pipelinePool]) and
// each ADR-0105 concurrent lane (its own DescribeExec lane pool, ADR-0138)
// share one pipelined-tx lifecycle: conn escape → native pgx.Tx → F7
// synchronous_commit pin → Bug-164 FK bypass → the same errPipelineUnavailable
// fallback contract. db MUST be a DescribeExec-mode *sql.DB (openPgxDBDescribeExec)
// for the "only WHEN, never HOW" byte-identical encoding guarantee to hold.
func (a *ChangeApplier) beginPipelinedTxOn(ctx context.Context, db *sql.DB) (*pgxBatchTx, error) {
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: pipelined acquire conn: %w", err)
	}

	// Escape database/sql to the native *pgx.Conn. We capture the pointer
	// inside Raw and keep using it for the batch's lifetime: the *sql.Conn
	// stays checked out (never returned to the pool until release), so the
	// underlying backend — and the *pgx.Conn pointer — remain valid, the
	// same lifetime the raw-COPY path relies on (raw_copy.go / writeViaCopy).
	var conn *pgx.Conn
	rawErr := sqlConn.Raw(func(driverConn any) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return errPipelineUnavailable
		}
		conn = stdlibConn.Conn()
		return nil
	})
	if rawErr != nil {
		_ = sqlConn.Close()
		if errors.Is(rawErr, errPipelineUnavailable) {
			return nil, errPipelineUnavailable
		}
		return nil, fmt.Errorf("postgres: applier: pipelined conn escape: %w", rawErr)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		_ = sqlConn.Close()
		return nil, fmt.Errorf("postgres: applier: pipelined begin tx: %w", err)
	}

	// F7: pin synchronous_commit on for this tx so a role/db-level default
	// of `off` can't silently break ADR-0007's "position + data lands
	// durably together" contract — identical to the serial BeginTx.
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		_ = tx.Rollback(ctx)
		_ = sqlConn.Close()
		return nil, fmt.Errorf("postgres: applier: pipelined force synchronous_commit=on: %w", err)
	}

	// Bug 164: bypass target FK + user-trigger enforcement for this apply tx
	// (a CDC stream is not FK-dependency-ordered) — identical semantics to the
	// serial bypassForeignKeyEnforcement, applied here on the native pgx.Tx.
	// SET LOCAL scopes it to this tx; no-op without privilege.
	if a.foreignKeyBypassAvailable(ctx) {
		if _, err := tx.Exec(ctx, replicaRoleSQL); err != nil {
			_ = tx.Rollback(ctx)
			_ = sqlConn.Close()
			return nil, fmt.Errorf("postgres: applier: pipelined bypass FK enforcement (session_replication_role=replica): %w", err)
		}
	}

	return &pgxBatchTx{
		a:       a,
		ctx:     ctx,
		sqlConn: sqlConn,
		conn:    conn,
		tx:      tx,
		batch:   &pgx.Batch{},
	}, nil
}

// errPipelineUnavailable signals that the database/sql conn could not be
// escaped to a *pgx.Conn (a non-pgx driver / wrapped conn). The caller
// falls back to the serial *sql.Tx path with a one-time WARN.
var errPipelineUnavailable = errors.New("postgres: applier: pipelined apply unavailable (conn is not pgx)")

// pipelinePool returns the lazily-opened ADR-0092 pipelined pool
// (DescribeExec mode). nil pipelineCfg (direct-API / unit constructions)
// yields errPipelineUnavailable so the batch path falls back to serial exec.
func (a *ChangeApplier) pipelinePool() (*sql.DB, error) {
	if a.pipelineDB != nil {
		return a.pipelineDB, nil
	}
	if a.pipelineCfg == nil {
		return nil, errPipelineUnavailable
	}
	// Register the PostGIS geometry codec on every pipelined backend so a
	// geometry column applies as BINARY EWKB under DescribeExec rather than
	// being TEXT-refused (see [afterConnectRegisterGeometry]); a no-op when
	// PostGIS isn't installed on the target.
	db, err := openPgxDBDescribeExec(a.pipelineCfg.dsn, roleApplier, a.pipelineCfg.appID,
		stdlib.OptionAfterConnect(composeAfterConnect(afterConnectSessionPins, afterConnectRegisterGeometry)))
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: open pipelined pool: %w", err)
	}
	a.pipelineDB = db
	return a.pipelineDB, nil
}

// Rollback aborts the pipelined tx and releases the pinned backend. It
// satisfies [appliershared.BatchTx]; the shared loop calls it on every
// error path (dispatch failure, ctx cancel, position-write failure). The
// batch was never sent / committed, so this discards both data and
// position atomically.
func (b *pgxBatchTx) Rollback() error {
	if b.released {
		return nil
	}
	rbErr := b.tx.Rollback(context.Background())
	b.release()
	if rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
		return rbErr
	}
	return nil
}

// release returns the pinned *sql.Conn to the pool. Idempotent.
func (b *pgxBatchTx) release() {
	if b.released {
		return
	}
	b.released = true
	_ = b.sqlConn.Close()
}

// queue appends a built (sql, args) statement onto the batch and records
// its change context for error attribution. The args were produced by the
// SAME prepareApplierValue codec path the serial exec uses, so encoding
// fidelity is identical — pipelining only changes when they are sent.
func (b *pgxBatchTx) queue(stmt string, args []any, ctxStmt queuedStmt) {
	b.batch.Queue(stmt, args...)
	b.stmts = append(b.stmts, ctxStmt)
}

// dispatchPipelined builds one change's SQL via the shared builders and
// queues it onto the batch instead of executing it. It mirrors
// [ChangeApplier.dispatch] arm-for-arm — same builders, same
// prepareApplierValue path, same unknown-table skip — but Queue replaces
// txExec. The schemaDirtyTables / execDMLArgs QueryExecMode prepend the
// serial path uses is intentionally NOT applied here: the pipelined pool
// already runs in QueryExecModeDescribeExec, so SendBatch re-describes every
// distinct statement fresh (binary, live-OID) and GAP #3 is subsumed (see
// file header).
func (a *ChangeApplier) dispatchPipelined(ctx context.Context, b *pgxBatchTx, streamID string, c ir.Change) error {
	switch v := c.(type) {
	case ir.Insert:
		schema := a.routedSchema(v.Schema)
		diagApplierInsertReceived(ctx, schema, v)
		key, err := a.conflictKeyForPipelined(ctx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("postgres: applier: conflict-key lookup for %s.%s: %w", schema, v.Table, err)
		}
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if errors.Is(err, errUnknownTable) {
			logUnknownTable(ctx, "insert", schema, v.Table)
			return nil
		}
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildInsertSQL(schema, v.Table, v.Row, key, colTypes)
		if err != nil {
			return fmt.Errorf("postgres: applier: build insert for %s.%s: %w", schema, v.Table, err)
		}
		b.queue(stmt, args, queuedStmt{schema: schema, table: v.Table, kind: "insert"})
		return nil

	case ir.Update:
		schema := a.routedSchema(v.Schema)
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if errors.Is(err, errUnknownTable) {
			logUnknownTable(ctx, "update", schema, v.Table)
			return nil
		}
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildUpdateSQL(schema, v.Table, v.Before, v.After, colTypes)
		if err != nil {
			return fmt.Errorf("postgres: applier: build update for %s.%s: %w", schema, v.Table, err)
		}
		b.queue(stmt, args, queuedStmt{schema: schema, table: v.Table, kind: "update"})
		return nil

	case ir.Delete:
		schema := a.routedSchema(v.Schema)
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if errors.Is(err, errUnknownTable) {
			logUnknownTable(ctx, "delete", schema, v.Table)
			return nil
		}
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildDeleteSQL(schema, v.Table, v.Before, colTypes)
		if err != nil {
			return fmt.Errorf("postgres: applier: build delete for %s.%s: %w", schema, v.Table, err)
		}
		b.queue(stmt, args, queuedStmt{schema: schema, table: v.Table, kind: "delete"})
		return nil

	case ir.Truncate:
		// A Truncate flushes as a 1-change batch (the shared loop returns
		// immediately after dispatching it on the TransactionalDDL=true
		// path), so a 1-statement pipelined batch is fine. The missing-table
		// benign skip is detected at SendBatch result-read time (commit),
		// the same place every other exec error surfaces; see flushAndCommit.
		schema := a.routedSchema(v.Schema)
		stmt := buildTruncateSQL(schema, v.Table, v.Cascade, v.RestartIdentity)
		b.queue(stmt, nil, queuedStmt{schema: schema, table: v.Table, kind: "truncate"})
		return nil

	case ir.SchemaSnapshot:
		// Persist the boundary's IR schema onto the SAME tx as the position
		// write (ADR-0049 locked decision #4a). writeSchemaVersionPgx queues
		// the history upsert onto the batch. A nil IR is a hard error,
		// identical to the serial dispatch.
		if v.IR == nil {
			return errors.New("postgres: applier: schema snapshot has nil IR table")
		}
		stmt, args, err := buildWriteSchemaVersionSQL(a.controlSchema, streamID, v.Schema, v.Table, v.Position, v.IR)
		if err != nil {
			return fmt.Errorf("postgres: applier: write schema version for %s.%s: %w", v.Schema, v.Table, err)
		}
		b.queue(stmt, args, queuedStmt{schema: v.Schema, table: v.Table, kind: "schema-snapshot"})
		return nil
	}
	return fmt.Errorf("postgres: applier: unknown change type %T", c)
}

// conflictKeyForPipelined resolves the Insert ON CONFLICT key for the
// pipelined path. Cache hits (the steady state) cost nothing; a cold cache
// runs the same loadConflictKey query the serial path uses, but on a
// short-lived *sql.Tx from the applier's primary pool rather than the
// pinned pipelined backend — the lookup is metadata-only and idempotent,
// and keeping it off the pinned conn avoids interleaving a query with the
// open pipelined tx. The result is cached in the shared conflictKeyCache,
// so the keyless guard (isKeylessInsert) reads the same entry the serial
// path would have populated.
func (a *ChangeApplier) conflictKeyForPipelined(ctx context.Context, schema, table string) ([]string, error) {
	qn := schemaTableKey(schema, table)
	if cached, ok := a.cachedConflictKey(qn); ok {
		return cached, nil
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: pipelined conflict-key begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	key, err := loadConflictKey(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}
	a.storeConflictKey(qn, key)
	return key, nil
}

// writePositionPipelined queues the position upsert onto the batch (built
// via the shared buildWritePositionSQL so the row shape / COALESCE
// semantics are byte-identical to the serial writePositionTx). It is the
// last statement queued before the SendBatch flush in flushAndCommit.
func (a *ChangeApplier) writePositionPipelined(b *pgxBatchTx, streamID, token string, rowsApplied int64) {
	stmt, args := buildWritePositionSQL(a.controlSchema, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema, rowsApplied)
	b.queue(stmt, args, queuedStmt{schema: a.controlSchema, table: controlTableName, kind: "position"})
}

// flushAndCommit sends the accumulated batch in one round trip, reads
// every queued statement's result IN ORDER (surfacing the first
// per-statement error wrapped with its change context and run through
// classifyApplierError), then commits the pgx.Tx under the Bug-56 commit
// watchdog deadline. The pinned backend is released on every path.
//
// A Truncate of a missing table (SQLSTATE 42P01) is the one benign exec
// outcome the serial path tolerates as a skip-with-warning; flushAndCommit
// reproduces that here when the only queued data statement is a truncate.
func (a *ChangeApplier) flushAndCommit(b *pgxBatchTx) (err error) {
	defer b.release()

	if execErr := a.sendBatchUnderDeadline(b); execErr != nil {
		_ = b.tx.Rollback(context.Background())
		return classifyApplierError(execErr)
	}

	// Commit under the same per-exec watchdog the serial path uses (Bug 56
	// / v0.52.1): a half-closed destination connection can otherwise stall
	// the apply goroutine inside the commit's TLS write.
	commitErr := appliershared.RunWithDeadline(a.execTimeout, func() error {
		return b.tx.Commit(b.ctx)
	})
	if commitErr != nil {
		return classifyApplierError(fmt.Errorf("postgres: applier: commit: %w", commitErr))
	}
	return nil
}

// sendBatchUnderDeadline flushes the accumulated batch (data statements +
// the position upsert that the WritePosition closure already queued as the
// final statement, ADR-0007 + ADR-0092) in ONE round trip, then reads
// every queued result IN ORDER inside the per-exec watchdog so a stalled
// flush is bounded the same way txExec bounds a serial exec. The first
// per-statement error wins, is attributed to its queued change, and is
// returned; a benign missing-table truncate is absorbed as a
// skip-with-warning (matching the serial dispatch's tolerance).
func (a *ChangeApplier) sendBatchUnderDeadline(b *pgxBatchTx) error {
	return appliershared.RunWithDeadline(a.execTimeout, func() error {
		br := b.tx.SendBatch(b.ctx, b.batch)
		// br.Exec must be called once per queued statement to drain the
		// pipeline; the server executed them in queue order.
		var firstErr error
		for i := range b.stmts {
			_, execErr := br.Exec()
			if execErr != nil && firstErr == nil {
				if b.stmts[i].kind == "truncate" && isMissingTableErr(execErr) {
					logUnknownTable(b.ctx, "truncate", b.stmts[i].schema, b.stmts[i].table)
					continue
				}
				firstErr = a.attributeQueuedError(b.stmts[i], execErr)
			}
		}
		if closeErr := br.Close(); closeErr != nil && firstErr == nil {
			firstErr = fmt.Errorf("postgres: applier: pipelined batch close: %w", closeErr)
		}
		return firstErr
	})
}

// attributeQueuedError wraps a per-statement execution error with the same
// "<op> into <schema>.<table>" context the serial dispatch arms produce,
// so retriable/fatal classification and operator-facing messages are
// equivalent to the serial path.
func (a *ChangeApplier) attributeQueuedError(s queuedStmt, err error) error {
	switch s.kind {
	case "insert":
		return fmt.Errorf("postgres: applier: insert into %s.%s: %w", s.schema, s.table, err)
	case "update":
		return fmt.Errorf("postgres: applier: update %s.%s: %w", s.schema, s.table, err)
	case "delete":
		return fmt.Errorf("postgres: applier: delete from %s.%s: %w", s.schema, s.table, err)
	case "truncate":
		return fmt.Errorf("postgres: applier: truncate %s.%s: %w", s.schema, s.table, err)
	case "position":
		return fmt.Errorf("postgres: write position: %w", err)
	default:
		return fmt.Errorf("postgres: applier: pipelined %s %s.%s: %w", s.kind, s.schema, s.table, err)
	}
}

// warnPipelineFallbackOnce logs a single WARN the first time the pipelined
// path is unavailable and the applier falls back to serial exec, so an
// operator sees the lost-throughput condition once (not per batch) and no
// throughput claim is made silently.
func (a *ChangeApplier) warnPipelineFallbackOnce(ctx context.Context, cause error) {
	// CompareAndSwap so the WARN fires exactly once even when multiple ADR-0138
	// concurrent apply lanes hit the fallback simultaneously (race-free).
	if !a.pipelineWarnedFallback.CompareAndSwap(false, true) {
		return
	}
	slog.WarnContext(ctx,
		"postgres: applier: pipelined CDC apply unavailable — falling back to serial per-row exec "+
			"(no throughput improvement on high-latency links; ADR-0092)",
		slog.String("cause", cause.Error()))
}
