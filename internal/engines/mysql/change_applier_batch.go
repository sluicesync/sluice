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

	"github.com/orware/sluice/internal/ir"
)

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
		batchN, lastPos, channelClosed, err := a.applyOneBatch(ctx, streamID, changes, maxBatchSize)
		if err != nil {
			return err
		}
		if batchN > 0 {
			slog.DebugContext(ctx, "mysql: applier: batch committed",
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
// On error the open transaction is rolled back. On clean exit (row
// cap, channel close, or Truncate flush) the transaction is
// committed.
func (a *ChangeApplier) applyOneBatch(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize int) (n int, lastPos ir.Position, channelClosed bool, err error) {
	// Wait for the first change before opening the tx. Opening the
	// tx then blocking with no work to do would hold a connection
	// idle from the pool for arbitrarily long; we'd rather wait on
	// the channel.
	var first ir.Change
	select {
	case c, ok := <-changes:
		if !ok {
			return 0, ir.Position{}, true, nil
		}
		first = c
	case <-ctx.Done():
		return 0, ir.Position{}, false, ctx.Err()
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

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, ir.Position{}, false, fmt.Errorf("mysql: applier: begin tx: %w", err)
	}

	if err := a.dispatch(ctx, tx, first); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(ctx, "mysql: applier: batch rollback on error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", 1),
			slog.String("err", err.Error()),
		)
		return 0, ir.Position{}, false, err
	}
	n = 1
	lastPos = first.Pos()

	for n < maxBatchSize {
		select {
		case c, ok := <-changes:
			if !ok {
				channelClosed = true
				return n, lastPos, channelClosed, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
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
				slog.DebugContext(ctx, "mysql: applier: batch committed",
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
			if err := a.dispatch(ctx, tx, c); err != nil {
				_ = tx.Rollback()
				slog.WarnContext(ctx, "mysql: applier: batch rollback on error",
					slog.String("stream_id", streamID),
					slog.Int("rows_attempted", n+1),
					slog.String("err", err.Error()),
				)
				return 0, ir.Position{}, false, err
			}
			n++
			lastPos = c.Pos()
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
	if err := writePositionTx(ctx, tx, streamID, token); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(ctx, "mysql: applier: batch rollback on position-write error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return err
	}
	if err := tx.Commit(); err != nil {
		slog.WarnContext(ctx, "mysql: applier: batch commit error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("mysql: applier: commit: %w", err)
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
