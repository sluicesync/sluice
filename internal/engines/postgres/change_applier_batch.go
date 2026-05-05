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
// Schema-changing events (Truncate today; AddColumn / DropColumn
// when the IR grows them) flush the in-progress batch and apply
// alone. The applier's column-type cache is keyed per qualified
// table name and is *not* invalidated mid-stream, so a Truncate
// followed by INSERTs into a redefined table is the operator's
// problem to coordinate. Flushing the batch around the schema
// event keeps the contract local: "everything before the schema
// event is durable; the schema event itself is its own
// transaction; everything after is a fresh batch".
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
//   - a Truncate event arrives (it joins the in-flight batch as
//     the final change so the truncate's position is the batch's
//     position write, then the batch commits and a fresh one
//     starts);
//   - ctx is cancelled (the in-flight batch rolls back; the
//     remaining changes replay on resume via idempotency);
//   - a target write fails (ditto).
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

	for {
		batchN, lastPos, channelClosed, err := a.applyOneBatch(ctx, streamID, changes, maxBatchSize)
		if err != nil {
			return err
		}
		if batchN > 0 {
			slog.DebugContext(ctx, "postgres: applier: batch committed",
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

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, ir.Position{}, false, fmt.Errorf("postgres: applier: begin tx: %w", err)
	}

	if err := a.dispatch(ctx, tx, first); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(ctx, "postgres: applier: batch rollback on error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", 1),
			slog.String("err", err.Error()),
		)
		return 0, ir.Position{}, false, err
	}
	n = 1
	lastPos = first.Pos()

	// Truncate flushes the batch — schema-changing events apply
	// alone so cache invalidation is contained.
	if _, isTruncate := first.(ir.Truncate); isTruncate {
		return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
	}

	for n < maxBatchSize {
		select {
		case c, ok := <-changes:
			if !ok {
				channelClosed = true
				return n, lastPos, channelClosed, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
			}
			if err := a.dispatch(ctx, tx, c); err != nil {
				_ = tx.Rollback()
				slog.WarnContext(ctx, "postgres: applier: batch rollback on error",
					slog.String("stream_id", streamID),
					slog.Int("rows_attempted", n+1),
					slog.String("err", err.Error()),
				)
				return 0, ir.Position{}, false, err
			}
			n++
			lastPos = c.Pos()
			if _, isTruncate := c.(ir.Truncate); isTruncate {
				return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
			}
		case <-ctx.Done():
			_ = tx.Rollback()
			return 0, ir.Position{}, false, ctx.Err()
		}
	}

	return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
}

// commitBatch writes the position then commits the open tx, then
// reports the just-committed LSN to the slot-ack feedback tracker
// (Bug 15, ADR-0020). Returns a wrapped error on either failure
// with a rollback already attempted on the position-write path.
//
// The tracker report happens AFTER tx.Commit succeeds, so a crash
// between the data commit and the report only loses one tracker
// update — the next batch's commit will report a higher LSN that
// supersedes it. The slot retains the WAL until that next report
// in exchange. Crash before tx.Commit means the data isn't durable
// either, and the reader's keepalive will keep ack'ing the
// previous floor.
func (a *ChangeApplier) commitBatch(ctx context.Context, tx *sql.Tx, streamID, token string, rows int) error {
	if err := writePositionTx(ctx, tx, a.schema, streamID, token); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(ctx, "postgres: applier: batch rollback on position-write error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return err
	}
	if err := tx.Commit(); err != nil {
		slog.WarnContext(ctx, "postgres: applier: batch commit error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("postgres: applier: commit: %w", err)
	}
	a.reportAppliedToken(ctx, token)
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
