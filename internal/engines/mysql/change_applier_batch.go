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
// Schema-changing events flush the in-progress batch and then
// apply alone via the per-change applyOne path. MySQL's TRUNCATE
// TABLE is DDL that implicit-commits any open transaction, so it
// would silently break the batch's atomicity if mixed with row
// changes; the explicit flush-before keeps the contract clean.
// (The per-change applyOne path has the same issue — see the
// comment in applyOne — but the resume idempotency story makes the
// rough edge tolerable for both.)
//
// See ADR-0017 for the design choice.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// defaultIdleFlushPeriod bounds the wait between the last applied
// change in an in-flight batch and that batch's commit. Without an
// idle flush, a partial batch (n < maxBatchSize) would sit in memory
// until either the channel closes or the next event arrives — which
// on a quiet stream means the persisted source_position never
// advances past the last *full* batch, lengthening the replay
// window on warm-resume.
//
// MySQL has no slot-ack equivalent of PG's confirmed_flush_lsn (the
// binlog retention is server-wide, not per-consumer), but the
// replay-window argument applies the same way. 5s matches PG for
// consistency.
const defaultIdleFlushPeriod = 5 * time.Second

// ApplyBatch implements [ir.BatchedChangeApplier]. See the file-
// header comment for the design and invariants.
//
// The loop draws changes one at a time, accumulates dispatch calls
// on a shared open transaction, and commits when one of the flush
// conditions fires:
//
//   - the batch's row count reaches maxBatchSize;
//   - the change channel closes;
//   - a Truncate event arrives — the in-flight batch flushes first
//     (so the previous changes' position-write lands before
//     TRUNCATE's implicit commit), then the truncate applies via
//     the existing per-change applyOne path;
//   - ctx is cancelled (the in-flight batch rolls back; the
//     remaining changes replay on resume via idempotency);
//   - a target write fails (ditto).
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

	for {
		// ADR-0052: when an AIMD controller is wired via
		// SetBatchSizeProvider, consult it before each batch so the
		// controller's current target drives the row cap. The static
		// maxBatchSize remains the absolute ceiling — provider returns
		// are clamped to it so an operator-supplied --apply-batch-size=N
		// remains a hard cap the controller can never exceed.
		effective := maxBatchSize
		if a.batchSizeProvider != nil {
			next := a.batchSizeProvider.NextBatchSize()
			if next > 0 && next < effective {
				effective = next
			}
		}
		batchN, lastPos, channelClosed, err := a.applyOneBatch(ctx, streamID, changes, effective)
		if err != nil {
			return err
		}
		if batchN > 0 {
			slog.DebugContext(
				ctx, "mysql: applier: batch committed",
				slog.String("stream_id", streamID),
				slog.Int("rows", batchN),
				slog.String("position_token", truncateBatchToken(lastPos.Token, 80)),
			)
		}
		if channelClosed {
			return nil
		}
	}
}

// applyOneBatch consumes up to maxBatchSize changes from the channel
// and applies them in a single target transaction along with the
// position write of the last applied change. Returns the number of
// changes applied, the last applied change's position, whether the
// channel closed (signalling clean shutdown to the caller), and any
// error.
//
// Truncate handling: when a Truncate arrives as the first change of
// a batch, it's dispatched alone via the existing per-change
// applyOne path (which has the same implicit-commit-on-DDL behaviour
// the batched path does, just at smaller scale). When a Truncate
// arrives mid-batch, the in-flight batch flushes first, then the
// truncate applies via applyOne — ensuring the previous changes'
// position-write lands before TRUNCATE's implicit commit destroys
// the open tx.
//
// Memory-bounded batching (ADR-0028): the in-flight batch's
// accumulated row-value bytes are tracked via
// [ir.ApproximateChangeBytes]; when the running total reaches
// [ChangeApplier.maxBufferBytes] the batch flushes early even if the
// row cap hasn't fired. The byte cap is a soft target — a single
// change larger than the cap still applies (the dispatch already
// happened before the post-dispatch check fires); the cap bounds
// *accumulation*, not individual changes.
//
// On error the open transaction is rolled back. On clean exit (row
// cap, byte cap, channel close, or Truncate flush) the transaction is
// committed.
func (a *ChangeApplier) applyOneBatch(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize int) (n int, lastPos ir.Position, channelClosed bool, err error) {
	// GitHub #18 Phase 1: batch-latency telemetry. Measure wall-clock
	// from "batch start" through "position write + tx commit returns"
	// so a downstream auto-tuner (or operator log inspection) can
	// see per-batch apply cost. DEBUG-only and elided on n==0 (idle
	// flush of an empty batch); typical operators run INFO and never
	// see this; --log-level=debug surfaces it for telemetry runs.
	//
	// ADR-0052: if a BatchObserver is wired, the same wall-clock
	// duration feeds the AIMD controller via ObserveBatch — success
	// and failure paths both call it so the controller sees retry
	// signals.
	batchStart := time.Now()
	defer func() {
		latency := time.Since(batchStart)
		if n > 0 {
			slog.DebugContext(
				ctx, "applier: batch latency",
				slog.String("stream_id", streamID),
				slog.Int("rows", n),
				slog.Int64("millis", latency.Milliseconds()),
			)
		}
		if a.batchObserver != nil && (n > 0 || err != nil) {
			a.batchObserver.ObserveBatch(ctx, latency, n, err)
		}
	}()

	// Wait for the first row-bearing change before opening the tx.
	// Opening the tx then blocking with no work to do would hold a
	// connection idle from the pool for arbitrarily long; we'd
	// rather wait on the channel. TxBegin / TxCommit boundary
	// events received before any row event are consumed in this
	// pre-tx loop (ADR-0027 — they're useful to the inner loop
	// where they bracket actual row work).
	var first ir.Change
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return 0, ir.Position{}, true, nil
			}
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit:
				// Empty source tx (BEGIN → XID with no row events)
				// or a TxBegin / TxCommit that arrived after a
				// previous batch flushed — both are no-ops.
				continue
			}
			first = c
		case <-ctx.Done():
			return 0, ir.Position{}, false, ctx.Err()
		}
		break
	}

	// Truncate as the first change of the batch: dispatch via the
	// per-change applyOne path so the implicit-commit-on-DDL is
	// handled by the same code that handles it for the
	// per-change Apply loop. The "batch of 1" shape is logged
	// uniformly by the outer loop.
	if _, isTruncate := first.(ir.Truncate); isTruncate {
		if err := a.applyOne(ctx, streamID, first); err != nil {
			return 0, ir.Position{}, false, err
		}
		return 1, first.Pos(), false, nil
	}

	// ADR-0049 Chunk B: a SchemaSnapshot as the first change applies
	// alone via applyOne — which writes the version AND a position in
	// ONE tx (locked decision #4a). Treating it as its own "batch of
	// 1" keeps the version write atomically paired with a position
	// write without entangling it with row-change rollback semantics.
	if _, isSnap := first.(ir.SchemaSnapshot); isSnap {
		if err := a.applyOne(ctx, streamID, first); err != nil {
			return 0, ir.Position{}, false, err
		}
		return 1, first.Pos(), false, nil
	}

	byteCap := a.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

	// PII Phase 1.5: redact the first change before dispatch.
	// Subsequent batch members are redacted in the loop below.
	if err := a.redactChange(ctx, first); err != nil {
		return 0, ir.Position{}, false, classifyApplierError(fmt.Errorf("mysql: applier: redact: %w", err))
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, ir.Position{}, false, fmt.Errorf("mysql: applier: begin tx: %w", err)
	}

	if err := a.dispatch(ctx, tx, streamID, first); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(
			ctx, "mysql: applier: batch rollback on error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", 1),
			slog.String("err", err.Error()),
		)
		return 0, ir.Position{}, false, classifyApplierError(err)
	}
	n = 1
	lastPos = first.Pos()
	batchBytes := ir.ApproximateChangeBytes(first)

	// Idle-flush timer: commit a partial batch if no further change
	// arrives within defaultIdleFlushPeriod, so the persisted
	// source_position keeps current on quiet streams (matches PG;
	// see PG's change_applier_batch.go for the reasoning).
	idle := time.NewTimer(defaultIdleFlushPeriod)
	defer idle.Stop()

	for n < maxBatchSize {
		select {
		case c, ok := <-changes:
			if !ok {
				channelClosed = true
				return n, lastPos, channelClosed, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
			}
			// Source-tx boundary handling (ADR-0027). TxCommit
			// flushes the in-flight target tx so the apply aligns
			// with the source's XIDEvent. TxBegin observed mid-
			// batch is a no-op: flushing on TxCommit is sufficient
			// to keep alignment.
			if _, isTxCommit := c.(ir.TxCommit); isTxCommit {
				lastPos = c.Pos()
				return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
			}
			if _, isTxBegin := c.(ir.TxBegin); isTxBegin {
				if !idle.Stop() {
					select {
					case <-idle.C:
					default:
					}
				}
				idle.Reset(defaultIdleFlushPeriod)
				continue
			}
			if _, isTruncate := c.(ir.Truncate); isTruncate {
				// Flush the in-flight non-DDL changes first so the
				// position write of the *previous* change lands
				// before TRUNCATE's implicit commit destroys our
				// tx. The truncate itself runs in a separate apply
				// in the next batch iteration; we re-deliver it by
				// applying it inline here, then return.
				if err := a.commitBatch(ctx, tx, streamID, lastPos.Token, n); err != nil {
					return 0, ir.Position{}, false, err
				}
				slog.DebugContext(
					ctx, "mysql: applier: batch committed",
					slog.String("stream_id", streamID),
					slog.Int("rows", n),
					slog.String("position_token", truncateBatchToken(lastPos.Token, 80)),
				)
				// Apply the truncate alone via the per-change path.
				if err := a.applyOne(ctx, streamID, c); err != nil {
					return 0, ir.Position{}, false, err
				}
				// Return rows=1 and the truncate's position so the
				// outer loop logs the truncate as its own batch.
				return 1, c.Pos(), false, nil
			}
			if _, isSnap := c.(ir.SchemaSnapshot); isSnap {
				// ADR-0049 Chunk B: flush the in-flight row batch first
				// so the previous changes' position-write commits, THEN
				// apply the snapshot alone via applyOne (version write +
				// position write in one tx, locked decision #4a). The
				// snapshot must land durably before the post-DDL rows
				// that follow it on the channel are applied; flushing
				// here keeps that ordering and keeps the version write
				// off the row-batch's rollback path.
				if err := a.commitBatch(ctx, tx, streamID, lastPos.Token, n); err != nil {
					return 0, ir.Position{}, false, err
				}
				slog.DebugContext(
					ctx, "mysql: applier: batch committed",
					slog.String("stream_id", streamID),
					slog.Int("rows", n),
					slog.String("position_token", truncateBatchToken(lastPos.Token, 80)),
				)
				if err := a.applyOne(ctx, streamID, c); err != nil {
					return 0, ir.Position{}, false, err
				}
				return 1, c.Pos(), false, nil
			}
			// PII Phase 1.5: redact each subsequent batch member
			// before dispatch. nil/empty redactor is a no-op.
			if err := a.redactChange(ctx, c); err != nil {
				_ = tx.Rollback()
				return 0, ir.Position{}, false, classifyApplierError(fmt.Errorf("mysql: applier: redact: %w", err))
			}
			if err := a.dispatch(ctx, tx, streamID, c); err != nil {
				_ = tx.Rollback()
				slog.WarnContext(
					ctx, "mysql: applier: batch rollback on error",
					slog.String("stream_id", streamID),
					slog.Int("rows_attempted", n+1),
					slog.String("err", err.Error()),
				)
				return 0, ir.Position{}, false, classifyApplierError(err)
			}
			n++
			lastPos = c.Pos()
			batchBytes += ir.ApproximateChangeBytes(c)
			// Byte-cap flush (ADR-0028): bounds the in-flight tx's
			// buffered parameter memory on wide-row streams. Checked
			// after the dispatch so the just-dispatched change is
			// included in the count we commit.
			if batchBytes >= byteCap {
				slog.DebugContext(
					ctx, "mysql: applier: byte-cap flush",
					slog.String("stream_id", streamID),
					slog.Int("rows", n),
					slog.Int64("bytes", batchBytes),
					slog.Int64("byte_cap", byteCap),
				)
				// ADR-0052 DP-4 (b): when the byte-cap fires before the
				// row-cap on a sustained shape, AI-ing rows can't help
				// (the binding constraint is bytes). The controller logs
				// the advisory at most once per cool-off period.
				if a.batchSizeProvider != nil && n < maxBatchSize {
					if hinter, ok := a.batchSizeProvider.(interface {
						NoteByteCapDominant(ctx context.Context, rows int, bytes, byteCap int64)
					}); ok {
						hinter.NoteByteCapDominant(ctx, n, batchBytes, byteCap)
					}
				}
				return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
			}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(defaultIdleFlushPeriod)
		case <-idle.C:
			slog.DebugContext(
				ctx, "mysql: applier: idle flush",
				slog.String("stream_id", streamID),
				slog.Int("rows", n),
				slog.Duration("idle", defaultIdleFlushPeriod),
			)
			return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
		case <-ctx.Done():
			_ = tx.Rollback()
			return 0, ir.Position{}, false, ctx.Err()
		}
	}

	return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
}

// commitBatch writes the position then commits the open tx.
// Returns a wrapped error on either failure with a rollback already
// attempted on the position-write path.
func (a *ChangeApplier) commitBatch(ctx context.Context, tx *sql.Tx, streamID, token string, rows int) error {
	posCtx, posCancel := a.execTimeoutCtx(ctx)
	err := writePositionTx(posCtx, tx, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
	posCancel()
	if err != nil {
		_ = tx.Rollback()
		slog.WarnContext(
			ctx, "mysql: applier: batch rollback on position-write error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return err
	}
	if err := a.commitWithTimeout(tx); err != nil {
		slog.WarnContext(
			ctx, "mysql: applier: batch commit error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return classifyApplierError(fmt.Errorf("mysql: applier: commit: %w", err))
	}
	return nil
}

// truncateBatchToken trims a position token to maxLen characters
// with an ellipsis when longer. Mirrors the streamer's
// truncateDryRunToken helper; kept local so the applier doesn't
// import the pipeline package. Position tokens are JSON blobs that
// can run hundreds of bytes; the debug log line stays scannable.
func truncateBatchToken(token string, maxLen int) string {
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen-1] + "…"
}
