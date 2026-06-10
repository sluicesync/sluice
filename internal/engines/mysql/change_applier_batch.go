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
	"database/sql"
	"errors"
	"fmt"

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
	return &appliershared.BatchConfig{
		EngineName: "mysql",
		// MySQL DDL implicit-commits the open tx — schema events must
		// flush the batch and apply alone via ApplyOne (see the file-
		// header comment).
		TransactionalDDL:  false,
		ByteCap:           byteCap,
		BatchSizeProvider: a.batchSizeProvider,
		BatchObserver:     a.batchObserver,
		BeginTx: func(ctx context.Context) (*sql.Tx, error) {
			tx, err := a.db.BeginTx(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("mysql: applier: begin tx: %w", err)
			}
			return tx, nil
		},
		Dispatch:   a.dispatch,
		ApplyOne:   a.applyOne,
		Redact:     a.redactChange,
		StampShard: a.stampShardChange,
		Classify:   classifyApplierError,
		WritePosition: func(ctx context.Context, tx *sql.Tx, streamID, token string) error {
			posCtx, posCancel := a.execTimeoutCtx(ctx)
			defer posCancel()
			return writePositionTx(posCtx, tx, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
		},
		Commit: a.commitWithTimeout,
		// AfterCommit and CacheSchemaSnapshot stay nil: MySQL has no
		// slot-ack tracker, and SchemaSnapshots route through applyOne
		// (TransactionalDDL=false), which owns the ADR-0049
		// cache-after-commit update itself.
	}
}
