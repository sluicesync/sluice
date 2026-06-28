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
// fresh batch". After a SchemaSnapshot boundary commits, the per-table
// target-side caches (colType / pk / conflict-key) are invalidated for
// that table (invalidateTargetCachesForBoundary) so the next DML
// re-reads the live post-DDL catalog — without that, a forwarded ALTER
// COLUMN TYPE applies the DDL but the applier keeps encoding against the
// stale pre-DDL column OID (ADR-0091 F7a GAP #3). A bare Truncate
// carries no schema delta, so its post-redefinition coordination remains
// the operator's responsibility.
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
	// ADR-0105 item 26: when the operator wired the key-hash apply LANE count
	// (--apply-concurrency W) > 1 and a dedicated pool can be opened, route to
	// the shared concurrent key-hash apply path — W in-order lanes committing
	// concurrently, with the resume position advanced only to a fully-durable
	// source boundary (the seq-frontier). 0/1 stays on the ADR-0092 batch loop
	// below. Matches the MySQL applier's routing precedence exactly.
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
	return &appliershared.BatchConfig{
		EngineName: "postgres",
		// PG DDL is transactional — a schema event joins the batch tx
		// and flushes as its own batch (ADR-0049 locked decision #4a).
		TransactionalDDL:  true,
		ByteCap:           byteCap,
		BatchSizeProvider: a.batchSizeProvider,
		BatchObserver:     a.batchObserver,
		// BeginTx returns the ADR-0092 pipelined handle (*pgxBatchTx) for
		// the batch path; if the raw-conn escape is unavailable it falls
		// back to the serial *sql.Tx path with a one-time WARN — loud,
		// never silent, no throughput claim. The Dispatch / WritePosition
		// / Commit closures below type-switch on which handle came back.
		BeginTx: func(ctx context.Context) (appliershared.BatchTx, error) {
			b, err := a.beginPipelinedTx(ctx)
			if err == nil {
				return b, nil
			}
			if !errors.Is(err, errPipelineUnavailable) {
				// A genuine failure (pool acquire / begin / SET) — surface
				// it classified, exactly as the serial BeginTx would.
				return nil, classifyApplierError(err)
			}
			a.warnPipelineFallbackOnce(ctx, err)
			return a.beginSerialBatchTx(ctx)
		},
		Dispatch: func(ctx context.Context, tx appliershared.BatchTx, streamID string, c ir.Change) error {
			if b, ok := tx.(*pgxBatchTx); ok {
				return a.dispatchPipelined(ctx, b, streamID, c)
			}
			return a.dispatch(ctx, tx.(*sql.Tx), streamID, c)
		},
		// ApplyOne is unreachable while TransactionalDDL is true (PG
		// schema events ride the batch tx); filled so the seam stays
		// total.
		ApplyOne:   a.applyOne,
		Redact:     a.redactChange,
		StampShard: a.stampShardChange,
		Classify:   classifyApplierError,
		WritePosition: func(ctx context.Context, tx appliershared.BatchTx, streamID, token string) error {
			if b, ok := tx.(*pgxBatchTx); ok {
				// Queue the position upsert onto the batch; it flushes with
				// the data in Commit's single SendBatch (ADR-0092).
				a.writePositionPipelined(b, streamID, token)
				return nil
			}
			posCtx, posCancel := a.execTimeoutCtx(ctx)
			defer posCancel()
			return writePositionTx(posCtx, tx.(*sql.Tx), a.controlSchema, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
		},
		Commit: func(tx appliershared.BatchTx) error {
			if b, ok := tx.(*pgxBatchTx); ok {
				return a.flushAndCommit(b)
			}
			return a.commitWithTimeout(tx.(*sql.Tx))
		},
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
		IsKeylessTable:      a.isKeylessInsert,
	}
}

// beginSerialBatchTx opens the legacy serial *sql.Tx batch transaction —
// the ADR-0092 fall-back when the pipelined raw-conn escape is
// unavailable. Byte-identical to the pre-ADR-0092 PG BeginTx (a tx on the
// primary pool with the F7 synchronous_commit pin); returned as the
// [appliershared.BatchTx] the seam now requires.
func (a *ChangeApplier) beginSerialBatchTx(ctx context.Context) (appliershared.BatchTx, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: begin tx: %w", err)
	}
	// F7: pin synchronous_commit on for the duration of this tx so a
	// role/db-level default of `off` can't silently break ADR-0007's
	// "position + data lands durably together" contract.
	if err := a.forceSynchronousCommitOn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return nil, classifyApplierError(err)
	}
	// Bug 164: bypass target FK + user-trigger enforcement for this apply tx
	// (a CDC stream is not FK-dependency-ordered). No-op without privilege.
	if err := a.bypassForeignKeyEnforcement(ctx, tx); err != nil {
		_ = tx.Rollback()
		return nil, classifyApplierError(err)
	}
	return tx, nil
}

// isKeylessInsert is the ADR-0089 keyless-guard predicate the shared
// batch loop consults to decide whether a change must apply as a batch
// of 1. It returns true only for an Insert into a TRULY-KEYLESS table —
// no PRIMARY KEY and no usable non-null UNIQUE index — for which the
// dispatch path falls back to a plain, non-idempotent INSERT
// (conflictKeyFor → empty key; Bug 125 class 3). Only Inserts are gated:
// Update/Delete on a keyless table do not create duplicate rows on
// replay, so they need no flush boundary.
//
// The loop calls this AFTER the change has dispatched, and the Insert
// dispatch always populates conflictKeyCache via conflictKeyFor, so this
// is a plain map read keyed identically to the dispatch site
// (routedSchema(Schema)+"."+Table). An unexpected cache miss returns
// false (do not flush): the change already applied, and a missing entry
// cannot prove keylessness, so the safe-by-omission choice is to not
// special-case it.
func (a *ChangeApplier) isKeylessInsert(ctx context.Context, c ir.Change) bool {
	ins, ok := c.(ir.Insert)
	if !ok {
		return false
	}
	qn := schemaTableKey(a.routedSchema(ins.Schema), ins.Table)
	key, ok := a.cachedConflictKey(qn)
	if !ok || len(key) > 0 {
		return false
	}
	a.warnKeylessOnce(ctx, qn)
	return true
}

// warnKeylessOnce logs a single WARN per keyless table the first time the
// ADR-0089 guard holds it at single-row apply, so an operator sees why
// that table is not getting batched throughput. The check-and-set is atomic
// under the cacheMu write lock (markWarnedKeyless) so the WARN fires exactly
// once even when concurrent lanes touch the same keyless table.
func (a *ChangeApplier) warnKeylessOnce(ctx context.Context, qn string) {
	if !a.markWarnedKeyless(qn) {
		return
	}
	slog.WarnContext(ctx,
		"postgres: applier: table has no PRIMARY KEY or usable unique index — its INSERTs are "+
			"not idempotent, so keyless CDC is at-least-once: a crash before the source "+
			"transaction's commit checkpoint re-inserts this table's rows from the interrupted "+
			"transaction on resume (keyed tables are exactly-once). Each change is applied as its "+
			"own transaction to bound the window, but rows in the same source transaction still "+
			"replay together. Add a PRIMARY KEY (or NOT NULL UNIQUE index) for exactly-once, "+
			"batched throughput (ADR-0089)",
		slog.String("table", qn))
}
