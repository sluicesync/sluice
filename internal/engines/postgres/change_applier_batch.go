// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// # Batched apply for Postgres CDC throughput
//
// The default per-change Apply path commits one source change per
// target transaction. Each commit is a wal-flush + fsync round-trip,
// and bulk-traffic workloads (one source transaction with thousands
// of INSERTs) bottom out at the per-commit fsync latency rather than
// row-application work — measured at ~6.5 rows/sec on PG → MySQL CDC
// during v0.3.0 robustness testing.
//
// ApplyBatch amortises that overhead by running up to N source
// changes inside a single target transaction along with the position
// write of the last applied change in the batch. Two invariants:
//
//   - **Idempotency (ADR-0010).** Insert uses ON CONFLICT (PK) DO
//     UPDATE; Update / Delete tolerate zero-rows-affected. Replay
//     of any prefix of the change stream produces the same final
//     state, so the position written at the end of a batch can be
//     the position of the *last* applied change — replay from that
//     position via idempotency reproduces the missed work.
//
//   - **Position-and-data atomicity (ADR-0007).** The position
//     write happens inside the same target transaction as the
//     batch's data writes. A crash mid-batch rolls back both;
//     replay starts from the previous batch boundary.
//
// The batch loop itself — accumulation, AIMD consult/observe, the
// idle-grace flush, byte-cap, commit ordering — is the shared control
// plane in [appliershared.RunBatchLoop] (ADR-0081); this file wires
// the Postgres dialect into its [appliershared.BatchConfig] seam.
//
// PG's divergences from the MySQL wiring: TransactionalDDL=true (PG
// DDL is transactional, so a Truncate / SchemaSnapshot dispatches
// onto the batch tx and flushes as its own batch — the event's
// position write rides the SAME tx, ADR-0049 locked decision #4a);
// BeginTx pins `SET LOCAL synchronous_commit = on` (F7); AfterCommit
// reports the applied LSN to the slot-ack feedback tracker (Bug 15,
// ADR-0020). Schema-changing events flush the in-progress batch and
// apply alone so the applier's column-type cache invalidation stays
// contained: "everything before the schema event is durable; the
// schema event itself is its own transaction; everything after is a
// fresh batch". The cache is keyed per qualified table name and is
// *not* invalidated mid-stream, so a Truncate followed by INSERTs
// into a redefined table is the operator's problem to coordinate.
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
// the PG side. The PG-specific stake: without the idle flush, a
// partial batch would sit in memory and the slot's
// confirmed_flush_lsn would never advance past the last *full* batch
// (the original ADR-0020 purpose, served faster since item 18).
const defaultIdleFlushPeriod = appliershared.DefaultIdleFlushPeriod

// ApplyBatch implements [ir.BatchedChangeApplier]. See the file-
// header comment for the invariants and
// [appliershared.RunBatchLoop] for the shared control flow: the loop
// draws changes one at a time, accumulates dispatch calls on a shared
// open transaction, and commits when one of the flush conditions
// fires (row cap, byte cap, idle grace, channel close, TxCommit
// boundary, or a schema event flushing as its own batch).
//
// maxBatchSize <= 1 falls through to the per-change Apply path so
// the field's "0 means default" semantics work without callers
// special-casing.
func (a *ChangeApplier) ApplyBatch(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize int) error {
	if streamID == "" {
		return errors.New("postgres: applier: streamID is empty (Streamer is responsible for resolving it)")
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
		EngineName: "postgres",
		// PG DDL is transactional — a schema event joins the batch tx
		// and flushes as its own batch (ADR-0049 locked decision #4a).
		TransactionalDDL:  true,
		ByteCap:           byteCap,
		BatchSizeProvider: a.batchSizeProvider,
		BatchObserver:     a.batchObserver,
		BeginTx: func(ctx context.Context) (*sql.Tx, error) {
			tx, err := a.db.BeginTx(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("postgres: applier: begin tx: %w", err)
			}
			// F7: pin synchronous_commit on for the duration of this tx
			// so a role/db-level default of `off` can't silently break
			// ADR-0007's "position + data lands durably together"
			// contract.
			if err := a.forceSynchronousCommitOn(ctx, tx); err != nil {
				_ = tx.Rollback()
				return nil, classifyApplierError(err)
			}
			return tx, nil
		},
		Dispatch: a.dispatch,
		// ApplyOne is unreachable while TransactionalDDL is true (PG
		// schema events ride the batch tx); filled so the seam stays
		// total.
		ApplyOne:   a.applyOne,
		Redact:     a.redactChange,
		StampShard: a.stampShardChange,
		Classify:   classifyApplierError,
		WritePosition: func(ctx context.Context, tx *sql.Tx, streamID, token string) error {
			posCtx, posCancel := a.execTimeoutCtx(ctx)
			defer posCancel()
			return writePositionTx(posCtx, tx, a.controlSchema, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
		},
		Commit: a.commitWithTimeout,
		// AfterCommit reports the just-committed LSN to the slot-ack
		// feedback tracker (Bug 15, ADR-0020). It runs AFTER tx.Commit
		// succeeds, so a crash between the data commit and the report
		// only loses one tracker update — the next batch's commit will
		// report a higher LSN that supersedes it. The slot retains the
		// WAL until that next report in exchange. Crash before
		// tx.Commit means the data isn't durable either, and the
		// reader's keepalive will keep ack'ing the previous floor.
		AfterCommit:         a.reportAppliedToken,
		CacheSchemaSnapshot: a.cacheActiveSchemaAfterCommit,
	}
}
