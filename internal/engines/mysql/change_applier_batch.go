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
// See ADR-0017 for the original design choice.

import (
	"context"
	"errors"
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
func (a *ChangeApplier) ApplyBatch(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize int) error {
	if streamID == "" {
		return errors.New("mysql: applier: streamID is empty (Streamer is responsible for resolving it)")
	}
	if maxBatchSize <= 1 {
		return a.Apply(ctx, streamID, changes)
	}
	// ADR-0104 item 23(c): when the operator wired the key-hash apply LANE
	// count (--apply-concurrency W) > 1 and a dedicated pool can be opened,
	// route to the concurrent key-hash apply path — W in-order lanes
	// committing concurrently, with the resume position advanced only to a
	// fully-durable source boundary. 0/1 stays on the serial loop below.
	if a.applyConcurrency > 1 && a.pipelineCfg != nil {
		return a.applyBatchConcurrent(ctx, streamID, changes, maxBatchSize, a.applyConcurrency)
	}
	return appliershared.RunBatchLoop(ctx, a.batchConfig(), streamID, changes, maxBatchSize)
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
		TransactionalDDL: false,
		// MySQL file/pos resume cannot start mid-transaction (go-mysql seeks
		// to the byte offset; a ROWS event whose TABLE_MAP was earlier in the
		// same tx fails with "no corresponding table map event"). Persist the
		// resume position ONLY at a source-tx / DDL boundary so a row-cap /
		// byte-cap / idle / keyless / channel-close flush that lands mid-tx
		// commits its data but does NOT advance the persisted position — the
		// interrupted tx re-reads whole and idempotently re-applies on resume
		// (ADR-0010). The concurrent laneapply path already enforces this; the
		// serial loop must match it. Found live on the large-scale program: a
		// slow cross-region target drove AIMD to batch-size 1, split a large
		// multi-row source tx across batches, and persisted a mid-tx position
		// that crash-looped every warm-resume.
		CheckpointOnlyAtTxBoundary: true,
		ByteCap:                    byteCap,
		BatchSizeProvider:          a.batchSizeProvider,
		BatchObserver:              a.batchObserver,
		// ADR-0139: BeginTx returns the coalescing handle (*mysqlBatchTx) that
		// buffers consecutive same-table/same-shape keyed inserts and emits
		// them as one multi-row INSERT at flush; the Dispatch / WritePosition /
		// Commit closures drive it (flush-before-position so the data is durable
		// before the position write, flush-on-commit for the mid-tx
		// CheckpointOnly path). Value encoding is byte-identical to the serial
		// single-row path (same prepareApplierValue → `?` binding).
		BeginTx: a.beginCoalescingBatchTx,
		Dispatch: func(ctx context.Context, tx appliershared.BatchTx, streamID string, c ir.Change) error {
			return tx.(*mysqlBatchTx).dispatch(ctx, streamID, c)
		},
		ApplyOne:   a.applyOne,
		Redact:     a.redactChange,
		StampShard: a.stampShardChange,
		Classify:   classifyApplierError,
		WritePosition: func(ctx context.Context, tx appliershared.BatchTx, streamID, token string, rowsApplied int64) error {
			return tx.(*mysqlBatchTx).writePosition(ctx, streamID, token, rowsApplied)
		},
		Commit: func(tx appliershared.BatchTx) error {
			return tx.(*mysqlBatchTx).commit()
		},
		// AfterCommit stays nil (MySQL has no slot-ack tracker).
		// CacheSchemaSnapshot stays nil: SchemaSnapshots route through
		// applyOne (TransactionalDDL=false), which owns the ADR-0049
		// cache-after-commit update itself.
		IsKeylessTable: a.isKeylessInsert,
	}
	return cfg
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
	// MySQL's ON DUPLICATE KEY UPDATE keys off any unique index, so the
	// diagnosis and advice name a plain UNIQUE index (PG needs NOT NULL —
	// see appliershared.WarnKeyless).
	appliershared.WarnKeyless(ctx, "mysql", qn, "unique index", "a UNIQUE index")
}
