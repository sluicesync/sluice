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
	"time"

	"github.com/orware/sluice/internal/ir"
)

// defaultIdleFlushPeriod is the maximum time a partial batch
// (n < maxBatchSize) waits after the last applied change before it
// commits. It exists to bound trailing-row latency on a quiet stream
// (ADR-0020): without it, a partial batch would sit in memory until
// either the channel closes or the next event arrives, and the slot's
// confirmed_flush_lsn would never advance past the last *full* batch.
//
// Why 100ms (the roadmap item-18 latency fix):
//
//   - When the channel is being fed (a burst / sustained high-
//     throughput drain) the next change arrives well within 100ms, so
//     batches still fill to maxBatchSize. The poller's adaptive
//     immediate-repoll on full batches keeps the channel fed, so there
//     is NO throughput regression — the grace only ever fires once the
//     producer genuinely pauses.
//   - When the stream drains / pauses, the partial batch flushes within
//     ~100ms instead of the old 5s, so single-change apply latency on a
//     sparse stream drops by ~5s and the slot's confirmed_flush_lsn
//     advances promptly (the original ADR-0020 purpose, now served
//     faster).
//   - 100ms comfortably rides producer/scheduler jitter within a burst
//     while staying negligible against the ~1s poll interval, so it
//     never truncates a burst that is still in flight.
//
// The pre-item-18 value was 5s, which made a single sparse change cost
// ~5s of pure trailing latency — the dominant component of the 5.9s
// measured on the postgres-trigger CDC path under
// --apply-batch-size=auto.
const defaultIdleFlushPeriod = 100 * time.Millisecond

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
				ctx, "postgres: applier: batch committed",
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
// Source-transaction boundary handling (ADR-0027): TxBegin /
// TxCommit events surface the source's transaction boundaries.
// TxBegin is a no-op (the applier opens its target tx lazily on the
// first row event). TxCommit flushes the in-flight batch so the
// target tx commits as a single unit aligned to the source tx; an
// empty source tx (TxBegin → TxCommit with no row events) is a no-
// op since no target tx was opened.
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
// cap, byte cap, channel close, Truncate flush, or TxCommit flush)
// the transaction is committed.
func (a *ChangeApplier) applyOneBatch(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize int) (n int, lastPos ir.Position, channelClosed bool, err error) {
	// GitHub #18 Phase 1 + roadmap item 18: batch-latency telemetry
	// (PG-side mirror of the MySQL applier). Measure wall-clock for the
	// APPLY WORK only — begin-tx → dispatch(es) → position write →
	// commit — NOT the time spent blocked in the pre-tx wait loop below
	// waiting for the first row-bearing change. batchStart is therefore
	// declared here (so the closure captures it) but assigned only after
	// the pre-tx wait loop, immediately before BeginTx.
	//
	// Why this matters (ADR-0052): the same wall-clock duration feeds
	// the AIMD controller via ObserveBatch. Including the wait-for-work
	// made a sparse/bursty stream's first batch report ~31s of latency
	// (~0.4s apply + ~31s blocked), which the controller read as "apply
	// is catastrophically slow" and collapsed batch size 1000→1 —
	// throttling drain throughput ~2x. Timing apply-only un-collapses it.
	//
	// IsZero guard: the n==0 early-return paths inside the pre-tx wait
	// loop leave batchStart at its zero value. The DEBUG log is already
	// elided on n==0, but the ctx.Done path there returns n==0 with a
	// non-nil err, which would otherwise feed ObserveBatch a bogus
	// (~now-since-zero-Time) latency. Guarding both the log and the
	// observe call on !batchStart.IsZero() keeps a pre-tx-wait
	// cancellation from poisoning the controller's window.
	var batchStart time.Time
	defer func() {
		if batchStart.IsZero() {
			return
		}
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
	// pre-tx loop.
	var first ir.Change
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return 0, ir.Position{}, true, nil
			}
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit:
				// Boundary observed before any row event for this
				// batch. TxBegin is a no-op; TxCommit closes an
				// empty source tx (or the boundary that follows the
				// previous batch's flush) — in both cases continue
				// waiting for the next row event.
				continue
			}
			first = c
		case <-ctx.Done():
			return 0, ir.Position{}, false, ctx.Err()
		}
		break
	}

	byteCap := a.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

	// PII Phase 1.5: redact the first change before dispatch.
	if err := a.redactChange(ctx, first); err != nil {
		return 0, ir.Position{}, false, classifyApplierError(fmt.Errorf("postgres: applier: redact: %w", err))
	}
	// ADR-0048 Shape A: stamp the operator-supplied discriminator
	// onto the first change before dispatch (sibling-tier to the
	// redact call above). Empty shardColumn is a no-op fast path.
	a.stampShardChange(first)

	// Item 18 Fix A: start the apply-latency clock here — after the
	// pre-tx wait loop has returned the first change — so the measured
	// latency reflects apply work (begin-tx → dispatch → position write
	// → commit) and not the blocked wait for the first change.
	batchStart = time.Now()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, ir.Position{}, false, fmt.Errorf("postgres: applier: begin tx: %w", err)
	}
	// F7: pin synchronous_commit on for the duration of this tx so a
	// role/db-level default of `off` can't silently break ADR-0007's
	// "position + data lands durably together" contract.
	if err := a.forceSynchronousCommitOn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return 0, ir.Position{}, false, classifyApplierError(err)
	}

	if err := a.dispatch(ctx, tx, streamID, first); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(
			ctx, "postgres: applier: batch rollback on error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", 1),
			slog.String("err", err.Error()),
		)
		return 0, ir.Position{}, false, classifyApplierError(err)
	}
	n = 1
	lastPos = first.Pos()
	batchBytes := ir.ApproximateChangeBytes(first)

	// Truncate flushes the batch — schema-changing events apply
	// alone so cache invalidation is contained.
	if _, isTruncate := first.(ir.Truncate); isTruncate {
		return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
	}

	// ADR-0049 Chunk B3: a SchemaSnapshot was just dispatched onto
	// `tx` (the version write). Commit it as a 1-change batch so the
	// commitBatch position write rides the SAME tx (locked decision
	// #4a) and the version lands durably before the post-DDL rows
	// that follow it on the channel are applied.
	if snap, isSnap := first.(ir.SchemaSnapshot); isSnap {
		if err := a.commitBatch(ctx, tx, streamID, lastPos.Token, n); err != nil {
			return 0, ir.Position{}, false, err
		}
		// ADR-0049 Chunk C cache-after-commit: only after commitBatch
		// reports nil do we mutate the in-memory active-version cache.
		// A failed commit short-circuits above; the cache is never
		// updated on the rolled-back path.
		a.cacheActiveSchemaAfterCommit(snap)
		return n, lastPos, false, nil
	}

	// Idle-flush timer: if no further change arrives within
	// defaultIdleFlushPeriod, commit the partial batch so the slot's
	// confirmed_flush_lsn can advance past the in-flight work
	// (ADR-0020 trailing-row latency footnote).
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
			// flushes the batch so the target tx aligns with the
			// source tx; the position written is the source
			// commit's LSN, which is the right resume point under
			// ADR-0007 (the position of the last durably-applied
			// work) and ADR-0010 idempotency. TxBegin is a no-op:
			// when it follows a TxCommit-driven flush we land here
			// with the previous tx already committed and the loop
			// has restarted; a TxBegin observed mid-batch (no
			// preceding TxCommit) means the source produced
			// adjacent transactions whose row events the applier
			// hasn't separated — flushing on TxCommit is sufficient
			// to keep alignment, so we ignore TxBegin here.
			if _, isTxCommit := c.(ir.TxCommit); isTxCommit {
				lastPos = c.Pos()
				return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
			}
			if _, isTxBegin := c.(ir.TxBegin); isTxBegin {
				// Reset idle timer so a TxBegin observed mid-batch
				// doesn't make the next idle window expire from the
				// previous row event's timestamp; otherwise an
				// otherwise-quiet stream would idle-flush
				// inconsistently.
				if !idle.Stop() {
					select {
					case <-idle.C:
					default:
					}
				}
				idle.Reset(defaultIdleFlushPeriod)
				continue
			}
			// PII Phase 1.5: redact subsequent batch members.
			if err := a.redactChange(ctx, c); err != nil {
				_ = tx.Rollback()
				return 0, ir.Position{}, false, classifyApplierError(fmt.Errorf("postgres: applier: redact: %w", err))
			}
			// ADR-0048 Shape A: stamp the operator-supplied
			// discriminator onto each subsequent batch member
			// before dispatch (sibling-tier to the redact call
			// above). Empty shardColumn is a no-op fast path.
			a.stampShardChange(c)
			if err := a.dispatch(ctx, tx, streamID, c); err != nil {
				_ = tx.Rollback()
				slog.WarnContext(
					ctx, "postgres: applier: batch rollback on error",
					slog.String("stream_id", streamID),
					slog.Int("rows_attempted", n+1),
					slog.String("err", err.Error()),
				)
				return 0, ir.Position{}, false, classifyApplierError(err)
			}
			n++
			lastPos = c.Pos()
			batchBytes += ir.ApproximateChangeBytes(c)
			if _, isTruncate := c.(ir.Truncate); isTruncate {
				return n, lastPos, false, a.commitBatch(ctx, tx, streamID, lastPos.Token, n)
			}
			// ADR-0049 Chunk B3: the SchemaSnapshot's version write
			// just landed on `tx`; flush now so the commitBatch
			// position write rides the SAME tx (locked decision #4a)
			// and the version is durable before the post-DDL rows
			// (which arrive in later batches) are applied.
			if snap, isSnap := c.(ir.SchemaSnapshot); isSnap {
				if err := a.commitBatch(ctx, tx, streamID, lastPos.Token, n); err != nil {
					return 0, ir.Position{}, false, err
				}
				// ADR-0049 Chunk C cache-after-commit: see the first-
				// change branch above for the rolled-back-tx rationale.
				a.cacheActiveSchemaAfterCommit(snap)
				return n, lastPos, false, nil
			}
			// Byte-cap flush (ADR-0028): bounds the in-flight tx's
			// buffered parameter memory on wide-row streams. Checked
			// after the dispatch so the just-dispatched change is
			// included in the count we commit.
			if batchBytes >= byteCap {
				slog.DebugContext(
					ctx, "postgres: applier: byte-cap flush",
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
			// Reset the idle timer for each successful change so the
			// timer measures gaps between events, not absolute time
			// since first.
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(defaultIdleFlushPeriod)
		case <-idle.C:
			slog.DebugContext(
				ctx, "postgres: applier: idle flush",
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
	posCtx, posCancel := a.execTimeoutCtx(ctx)
	err := writePositionTx(posCtx, tx, a.controlSchema, streamID, token, a.slotName, a.sourceFingerprint, a.targetSchema)
	posCancel()
	if err != nil {
		_ = tx.Rollback()
		slog.WarnContext(
			ctx, "postgres: applier: batch rollback on position-write error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return err
	}
	if err := a.commitWithTimeout(tx); err != nil {
		slog.WarnContext(
			ctx, "postgres: applier: batch commit error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return classifyApplierError(fmt.Errorf("postgres: applier: commit: %w", err))
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
