// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// # Batched apply for MySQL CDC throughput
//
// The default per-change Apply path commits one source change per
// target transaction. Each commit is a binlog flush (sync_binlog)
// + InnoDB log flush (innodb_flush_log_at_trx_commit) round-trip,
// and bulk-traffic workloads (one source transaction with thousands
// of INSERTs) bottom out at the per-commit fsync latency rather than
// row-application work — measured at ~6.5 rows/sec on PG → MySQL CDC
// during v0.3.0 robustness testing.
//
// ApplyBatch amortises that overhead by running up to N source
// changes inside a single target transaction along with the position
// write of the last applied change in the batch. Two invariants:
//
//   - **Idempotency (ADR-0010).** Insert uses ON DUPLICATE KEY
//     UPDATE (row-alias form, MySQL 8.0.20+); Update / Delete
//     tolerate zero-rows-affected. Replay of any prefix of the
//     change stream produces the same final state, so the position
//     written at the end of a batch can be the position of the
//     *last* applied change — replay from that position via
//     idempotency reproduces the missed work.
//
//   - **Position-and-data atomicity (ADR-0007).** The position
//     write happens inside the same target transaction as the
//     batch's data writes. A crash mid-batch rolls back both;
//     replay starts from the previous batch boundary.
//
// The batch loop itself — accumulation, AIMD consult/observe, the
// idle-grace flush, byte-cap, commit ordering — is the shared control
// plane in [appliershared.RunBatchLoop] (ADR-0081); this file wires
// the MySQL dialect into its [appliershared.BatchConfig] seam.
//
// The load-bearing MySQL divergence is TransactionalDDL=false:
// MySQL's TRUNCATE TABLE is DDL that implicit-commits any open
// transaction, so it would silently break the batch's atomicity if
// mixed with row changes. Schema-changing events therefore flush the
// in-progress batch and then apply alone via the per-change applyOne
// path. (applyOne has the same implicit-commit rough edge — see the
// comment there — but the resume idempotency story makes it tolerable
// for both.)
//
// # Pipelined apply (ADR-0104 Phase 1)
//
// When an operator wires an apply-pipeline depth W > 1
// ([SetApplyPipelineDepth]) the batch path runs each batch on a
// dedicated [mysqlPipeline] backend and commits batches strictly in
// submission order across a bounded in-flight window — overlapping
// cross-region commit RTTs to lift apply throughput toward W/RTT while
// keeping the commit linearization point in source order (the
// correctness). The BeginTx / Dispatch / WritePosition / Commit closures
// below type-switch on the handle (*mysqlBatchTx pipelined vs *sql.Tx
// serial); everything else — the builders, the prepareApplierValue codec,
// the keyless guard, the AIMD row cap — is shared with the serial path
// byte-for-byte. Depth 0/1 (every non-CLI construction's zero value)
// stays on the serial *sql.Tx path, byte-identical to the pre-ADR-0104
// behaviour. See change_applier_pipelined.go.
//
// See ADR-0017 for the original design choice.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// defaultIdleFlushPeriod aliases the shared loop's idle-flush grace
// (item 18 Fix B — see [appliershared.DefaultIdleFlushPeriod] for the
// full 100ms reasoning). The alias keeps this engine's Fix-B pin
// (TestDefaultIdleFlushPeriod_IsShortGrace) guarding the value from
// the MySQL side. MySQL has no slot-ack equivalent of PG's
// confirmed_flush_lsn (binlog retention is server-wide, not
// per-consumer), but the bounded-replay-window argument applies the
// same way.
const defaultIdleFlushPeriod = appliershared.DefaultIdleFlushPeriod

// ApplyBatch implements [ir.BatchedChangeApplier]. See the file-
// header comment for the invariants and
// [appliershared.RunBatchLoop] for the shared control flow: the loop
// draws changes one at a time, accumulates dispatch calls on a shared
// open transaction, and commits when one of the flush conditions
// fires (row cap, byte cap, idle grace, channel close, TxCommit
// boundary, or a schema event — which flushes first, then applies
// alone via applyOne).
//
// maxBatchSize <= 1 falls through to the per-change Apply path so
// the field's "0 means default" semantics work without callers
// special-casing.
//
// ADR-0104: when the pipelined path is engaged (depth W > 1) the loop
// runs over the dedicated-pool handles; ApplyBatch drains the pipeline
// to empty (committing every in-flight batch in order) before returning,
// so a clean shutdown / channel close never strands an un-committed
// batch and any deferred async commit error surfaces here.
func (a *ChangeApplier) ApplyBatch(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize int) error {
	if streamID == "" {
		return errors.New("mysql: applier: streamID is empty (Streamer is responsible for resolving it)")
	}
	if maxBatchSize <= 1 {
		return a.Apply(ctx, streamID, changes)
	}
	// ADR-0104 item 23(c): when the operator wired --apply-pipeline-depth
	// (the key-hash apply LANE count W) > 1 and a dedicated pool can be
	// opened, route to the concurrent key-hash apply path — W in-order
	// lanes committing concurrently, with the resume position advanced only
	// to a fully-durable source-tx boundary. This SUPERSEDES the Phase-1
	// commit-pipeline (kept below as the serial fallback when no dedicated
	// pool is available; slated for removal once the concurrent path is
	// validated). depth 0/1 stays on the byte-identical serial loop.
	if a.applyPipelineDepth > 1 && a.pipelineCfg != nil {
		return a.applyBatchConcurrent(ctx, streamID, changes, maxBatchSize, a.applyPipelineDepth)
	}
	loopErr := appliershared.RunBatchLoop(ctx, a.batchConfig(), streamID, changes, maxBatchSize)
	// ADR-0104: drain the in-flight window. drainPipeline returns the first
	// async commit error observed across the whole run; a loop error takes
	// precedence (it is the earlier, root failure) but we MUST still drain
	// so committed-but-unwaited batches finish and the worker goroutine
	// exits. nil pipeline (serial path) makes this a no-op.
	drainErr := a.drainPipeline()
	if loopErr != nil {
		return loopErr
	}
	return drainErr
}

// drainPipeline drains the lazily-started pipeline if one exists, so
// every in-flight batch commits in order and the commit worker exits.
// No-op on the serial path (no pipeline opened). Returns the first async
// commit error. After drain a fresh pipeline is started on the next
// ApplyBatch run, mirroring the lazy-open contract (a warm-resume / retry
// re-opens cleanly rather than reusing a drained worker).
func (a *ChangeApplier) drainPipeline() error {
	if a.pipelinePool == nil {
		return nil
	}
	p := a.pipelinePool
	a.pipelinePool = nil
	a.pipelineWarnedEngaged = false
	return p.closePool()
}

// applyOneBatch runs one batch-accumulation cycle of the shared loop.
// Kept as a named engine-side method (rather than inlining the
// appliershared call where needed) because the item-18 AIMD unit pins
// (change_applier_aimd_test.go) drive a single cycle directly to
// assert the apply-only latency timing and the pre-tx-cancel IsZero
// guard.
func (a *ChangeApplier) applyOneBatch(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize int) (n int, lastPos ir.Position, channelClosed bool, err error) {
	return appliershared.RunOneBatch(ctx, a.batchConfig(), streamID, changes, maxBatchSize)
}

// batchConfig assembles the ADR-0081 dialect seam for this applier.
// Built per call (a cheap struct of closures) so unit tests that
// construct ChangeApplier literals see the applier's current field
// values; all setters (SetMaxBufferBytes, SetBatchSizeProvider, …)
// run before ApplyBatch per the applier's single-goroutine contract.
func (a *ChangeApplier) batchConfig() *appliershared.BatchConfig {
	byteCap := a.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}
	cfg := &appliershared.BatchConfig{
		EngineName: "mysql",
		// MySQL DDL implicit-commits the open tx — schema events must
		// flush the batch and apply alone via ApplyOne (see the file-
		// header comment).
		TransactionalDDL:  false,
		ByteCap:           byteCap,
		BatchSizeProvider: a.batchSizeProvider,
		BatchObserver:     a.batchObserver,
		// BeginTx returns the ADR-0104 pipelined handle (*mysqlBatchTx) when
		// depth W > 1 is wired and the dedicated pool opens; otherwise it
		// falls back to the serial *sql.Tx path with a one-time WARN — loud,
		// never silent, no throughput claim. The Dispatch / WritePosition /
		// Commit closures below type-switch on which handle came back.
		BeginTx: func(ctx context.Context) (appliershared.BatchTx, error) {
			b, err := a.beginPipelinedBatchTx(ctx)
			if err == nil {
				return b, nil
			}
			if !errors.Is(err, errPipelineUnavailable) {
				// A genuine failure (pool acquire / begin / pending async
				// commit error) — surface it classified, exactly as the
				// serial BeginTx would.
				return nil, classifyApplierError(err)
			}
			// Serial path. Depth 0/1 is the silent zero-value default and
			// must NOT warn (no degradation occurred); only an operator who
			// asked for depth > 1 but whose pool failed to open sees the
			// one-time fallback WARN.
			if a.pipelineEnabled() {
				a.warnPipelineFallbackOnce(ctx, err)
			}
			return a.beginSerialBatchTx(ctx)
		},
		Dispatch: func(ctx context.Context, tx appliershared.BatchTx, streamID string, c ir.Change) error {
			if b, ok := tx.(*mysqlBatchTx); ok {
				b.rows++
				return a.dispatch(ctx, b.tx, streamID, c)
			}
			return a.dispatch(ctx, tx.(*sql.Tx), streamID, c)
		},
		ApplyOne:   a.applyOne,
		Redact:     a.redactChange,
		StampShard: a.stampShardChange,
		Classify:   classifyApplierError,
		WritePosition: func(ctx context.Context, tx appliershared.BatchTx, streamID, token string) error {
			posCtx, posCancel := a.execTimeoutCtx(ctx)
			defer posCancel()
			if b, ok := tx.(*mysqlBatchTx); ok {
				b.token = token
				return writePositionTx(posCtx, b.tx, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
			}
			return writePositionTx(posCtx, tx.(*sql.Tx), streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
		},
		Commit: func(tx appliershared.BatchTx) error {
			if b, ok := tx.(*mysqlBatchTx); ok {
				// ADR-0104 keyless guard: if the last IsKeylessTable verdict
				// for this batch was keyless, force the synchronous,
				// fully-drained commit so the keyless single-row commit
				// linearizes as --apply-batch-size=1 (consumed once).
				if a.pendingKeylessFlush {
					a.pendingKeylessFlush = false
					b.syncCommit = true
				}
				// Hand the (dispatched + position-written) tx to the ordered
				// commit pipeline. Returns once the hand-off is recorded
				// (commit RTT overlaps the next batch's dispatch), or after a
				// full drain for a keyless sync-commit batch.
				return b.p.handCommit(b)
			}
			return a.commitWithTimeout(tx.(*sql.Tx))
		},
		// AfterCommit stays nil (MySQL has no slot-ack tracker).
		// CacheSchemaSnapshot stays nil: SchemaSnapshots route through
		// applyOne (TransactionalDDL=false), which owns the ADR-0049
		// cache-after-commit update itself.
		IsKeylessTable: a.isKeylessInsertGuard,
	}
	// ADR-0104 Consequences: on the pipelined path the AIMD observation
	// MUST be the per-transaction COMMIT latency, measured in the commit
	// worker — NOT the shared loop's near-zero hand-off wall time, which
	// would poison the controller (it would read "apply is instant" and
	// grow the batch unboundedly into the cross-region tx-killer). So we
	// suppress the loop's observer and let mysqlPipeline.commitOne observe.
	// The serial path keeps the loop observer (commit is synchronous there,
	// so the loop's wall time IS the commit latency).
	if a.pipelineEnabled() {
		cfg.BatchObserver = nil
	}
	return cfg
}

// beginPipelinedBatchTx opens a pipelined batch transaction on the
// dedicated pool, acquiring a window slot (blocking until the in-flight
// count drops below W). Returns errPipelineUnavailable when depth is
// serial or the pool can't open, so BeginTx falls back to the serial
// path.
func (a *ChangeApplier) beginPipelinedBatchTx(ctx context.Context) (*mysqlBatchTx, error) {
	p, err := a.pipeline(ctx)
	if err != nil {
		return nil, err
	}
	return p.begin(ctx)
}

// beginSerialBatchTx opens the legacy serial *sql.Tx batch transaction —
// the pre-ADR-0104 path, also the ADR-0104 fall-back when depth is serial
// or the dedicated pool is unavailable. Returned as the
// [appliershared.BatchTx] the seam requires.
func (a *ChangeApplier) beginSerialBatchTx(ctx context.Context) (appliershared.BatchTx, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("mysql: applier: begin tx: %w", err)
	}
	return tx, nil
}

// isKeylessInsertGuard is the ADR-0089 keyless-guard predicate the shared
// batch loop consults to decide whether a change must apply as a batch of
// 1. It delegates to [isKeylessInsert] and, on a keyless verdict, records
// pendingKeylessFlush so the imminent Commit closure marks the pipelined
// batch tx for a synchronous fully-drained commit (ADR-0104: the pipeline
// never widens a keyless table's at-least-once replay window past the
// --apply-batch-size=1 serial baseline). The applier is single-goroutine,
// so the flag is set (here) and read+cleared (in Commit) on the same
// goroutine with no lock; on the serial path the flag is harmlessly unread
// (commit is already a per-batch boundary there).
func (a *ChangeApplier) isKeylessInsertGuard(ctx context.Context, c ir.Change) bool {
	keyless := a.isKeylessInsert(ctx, c)
	if keyless {
		a.pendingKeylessFlush = true
	}
	return keyless
}

// isKeylessInsert is the ADR-0089 keyless-guard predicate (MySQL). It
// returns true for an Insert into a TRULY-KEYLESS table — no PRIMARY KEY
// and no UNIQUE index — for which MySQL's ON DUPLICATE KEY UPDATE clause
// is inert and the Insert is effectively a plain, non-idempotent INSERT
// (Bug 125 class 3); such a change must apply as a batch of 1 so a
// crash-replay can't amplify the per-source-transaction duplicate
// window past what `--apply-batch-size=1` already exposes (keyless CDC
// is at-least-once — the in-flight source transaction's keyless rows
// still replay on resume; see warnKeylessOnce). Only Inserts are gated —
// Update/Delete on a keyless table do not create duplicate rows on
// replay. Unlike Postgres (which computes a conflict key during
// dispatch), MySQL does not, so this runs a one-time information_schema
// probe per table, cached.
func (a *ChangeApplier) isKeylessInsert(ctx context.Context, c ir.Change) bool {
	ins, ok := c.(ir.Insert)
	if !ok {
		return false
	}
	schema := a.routedSchema(ins.Schema)
	keyless := a.tableIsKeyless(ctx, schema, ins.Table)
	if keyless {
		a.warnKeylessOnce(ctx, qualifiedName(schema, ins.Table))
	}
	return keyless
}

// tableIsKeyless reports whether the table has neither a PRIMARY KEY nor
// any UNIQUE index (information_schema.statistics has no non_unique=0
// index for it), caching the verdict per qualified name. A probe error
// returns true (conservative — apply single-row rather than risk a
// batched keyless replay) and is NOT cached, so a later probe can correct
// it once the transient condition clears.
func (a *ChangeApplier) tableIsKeyless(ctx context.Context, schema, table string) bool {
	qn := qualifiedName(schema, table)
	if v, ok := a.cachedKeyless(qn); ok {
		return v
	}
	const q = `SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND non_unique = 0`
	var n int
	if err := a.db.QueryRowContext(ctx, q, schema, table).Scan(&n); err != nil {
		slog.DebugContext(ctx, "mysql: applier: keyless probe failed; applying single-row (ADR-0089)",
			slog.String("table", qn), slog.String("err", err.Error()))
		return true
	}
	keyless := n == 0
	a.storeKeyless(qn, keyless)
	return keyless
}

// warnKeylessOnce logs a single WARN per keyless table the first time the
// ADR-0089 guard holds it at single-row apply, so an operator sees why
// that table is not getting batched throughput.
func (a *ChangeApplier) warnKeylessOnce(ctx context.Context, qn string) {
	if !a.markWarnedKeyless(qn) {
		return
	}
	slog.WarnContext(ctx,
		"mysql: applier: table has no PRIMARY KEY or unique index — its INSERTs are not "+
			"idempotent, so keyless CDC is at-least-once: a crash before the source "+
			"transaction's commit checkpoint re-inserts this table's rows from the interrupted "+
			"transaction on resume (keyed tables are exactly-once). Each change is applied as its "+
			"own transaction to bound the window, but rows in the same source transaction still "+
			"replay together. Add a PRIMARY KEY (or a UNIQUE index) for exactly-once, batched "+
			"throughput (ADR-0089)",
		slog.String("table", qn))
}
