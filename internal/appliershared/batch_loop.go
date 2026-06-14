// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

// # Shared batched-apply control plane (ADR-0081)
//
// ApplyBatch's control flow — batch accumulation, the AIMD controller
// interaction (ADR-0052), the idle-grace flush timer (item 18 Fix B),
// memory-bounded flushing (ADR-0028), source-tx boundary alignment
// (ADR-0027), and the position-write-then-commit ordering (ADR-0007 /
// ADR-0010) — used to be maintained twice, once per engine, and 16 of
// 19 historical commits to either copy had to touch both (the item-18
// latency fix landed twice, line for line). This file is that loop,
// hoisted once, behind the [BatchConfig] dialect seam: each engine
// fills the genuinely divergent leaves (engine name for logs, the
// fallible dispatch, error classification, the position write, DDL
// transactionality) and keeps every piece of dialect knowledge in its
// own package. Mirrors the exprident.Config precedent (ADR-0045):
// shared skeleton, per-engine config.
//
// Two invariants this file owns — do not move them (the engine unit
// pins in change_applier_aimd_test.go ×2 and the integration pins in
// change_applier_batch_integration_test.go ×2 are the behaviour
// oracle):
//
//   - **Apply-only latency + timer ownership (item 18).** batchStart
//     is assigned only after the pre-tx wait loop returns the first
//     row-bearing change, immediately before BeginTx, so the AIMD
//     controller observes apply work — never the blocked wait. The
//     idle timer is created after the first dispatched change and
//     reset only after each subsequent successful dispatch (and on a
//     mid-batch TxBegin), so it measures gaps between events, not
//     absolute time since the batch opened.
//
//   - **Position-write-then-commit ordering (ADR-0007 / ADR-0010).**
//     commitBatch writes the position on the SAME tx as the batch's
//     data writes, then commits, then fires the optional post-commit
//     hooks (PG's slot-ack report, the ADR-0049 schema-version cache).
//     A crash mid-batch rolls back both position and data; replay
//     from the previous batch boundary reproduces the missed work via
//     idempotent writes.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// DefaultIdleFlushPeriod is the maximum time a partial batch
// (n < maxBatchSize) waits after the last applied change before it
// commits. It exists to bound the replay window on a quiet stream:
// without it, a partial batch would sit in memory until either the
// channel closes or the next event arrives, and the persisted
// source_position would never advance past the last *full* batch
// (on PG, the slot's confirmed_flush_lsn would likewise stall —
// ADR-0020; MySQL has no slot-ack equivalent but the replay-window
// argument applies the same way).
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
//     sparse stream drops by ~5s and the persisted source_position
//     advances promptly.
//   - 100ms comfortably rides producer/scheduler jitter within a burst
//     while staying negligible against the ~1s poll interval, so it
//     never truncates a burst that is still in flight.
//
// The pre-item-18 value was 5s, which made a single sparse change cost
// ~5s of pure trailing latency — the dominant component of the 5.9s
// measured on the postgres-trigger CDC path under
// --apply-batch-size=auto. Each engine aliases this constant as its
// package-local defaultIdleFlushPeriod so the per-engine item-18 Fix B
// pins keep guarding the value from both sides.
const DefaultIdleFlushPeriod = 100 * time.Millisecond

// BatchConfig is the dialect seam for the shared batched-apply loop
// (ADR-0081). Each engine's ChangeApplier assembles one per ApplyBatch
// call (a cheap struct of closures over the applier's fields) and
// hands it to [RunBatchLoop] / [RunOneBatch]. Everything NOT in this
// struct — accumulation, AIMD consult/observe, the idle-grace timer,
// flush triggers, rollback handling, commit ordering — is shared
// control flow that lives in this package.
//
// All func fields except AfterCommit and CacheSchemaSnapshot are
// required; ApplyOne is required only when TransactionalDDL is false.
type BatchConfig struct {
	// EngineName prefixes every log line and error this loop emits
	// ("mysql: applier: …", "postgres: applier: …") so operator-facing
	// output is byte-identical to the pre-extraction per-engine loops.
	EngineName string

	// TransactionalDDL names the one structural divergence between the
	// engines' batch loops: whether a schema-changing event (Truncate /
	// SchemaSnapshot) can ride the open batch transaction.
	//
	//   - true (Postgres): DDL is transactional, so the event is
	//     dispatched onto the batch tx and the batch flushes
	//     immediately after — the event's position write rides the
	//     same tx (ADR-0049 locked decision #4a).
	//   - false (MySQL): DDL implicit-commits the open transaction, so
	//     mixing it into the batch would silently break the batch's
	//     atomicity. The in-flight batch flushes FIRST (the previous
	//     changes' position-write lands before the DDL's implicit
	//     commit can destroy the tx), then the event applies alone via
	//     ApplyOne — the per-change path that already owns the
	//     implicit-commit rough edge.
	TransactionalDDL bool

	// ByteCap is the resolved ADR-0028 soft byte cap on the in-flight
	// batch's accumulated row-value bytes. Engines resolve their
	// defaultMaxBufferBytes fallback before filling this; the loop
	// flushes early once the running ApproximateChangeBytes total
	// reaches it. The cap bounds *accumulation*, not individual
	// changes — a single change larger than the cap still applies.
	ByteCap int64

	// BatchSizeProvider is the optional AIMD controller's batch-size
	// surface (ADR-0052); nil means "no controller — the static
	// maxBatchSize from the caller is the cap". Captured once per
	// ApplyBatch call (the streamer wires it before Run; the applier is
	// single-goroutine, so mid-flight rewiring was never supported).
	BatchSizeProvider ir.BatchSizeProvider

	// BatchObserver is the optional AIMD controller's batch-outcome
	// surface (ADR-0052); nil means "no observation".
	BatchObserver ir.BatchObserver

	// BeginTx opens the batch transaction, returning an error already
	// in its final caller-facing shape (wrapped + classified as the
	// engine requires; the loop does not rollback or re-wrap). PG's
	// hook also pins `SET LOCAL synchronous_commit = on` (F7) so the
	// ADR-0007 durability contract holds per-tx.
	BeginTx func(ctx context.Context) (*sql.Tx, error)

	// Dispatch routes one change to its SQL form on the open tx — the
	// engine's existing dispatch method. The loop owns rollback,
	// logging, and classification of a dispatch failure.
	Dispatch func(ctx context.Context, tx *sql.Tx, streamID string, c ir.Change) error

	// ApplyOne is the engine's per-change apply path (own tx, position
	// write included). The loop calls it only on the
	// TransactionalDDL=false branches, where a schema event must apply
	// outside the batch tx.
	ApplyOne func(ctx context.Context, streamID string, c ir.Change) error

	// Redact applies the operator's PII rules to a change's row data
	// before dispatch (Phase 1.5); the loop wraps a refusal as
	// "<engine>: applier: redact: …" and classifies it.
	Redact func(ctx context.Context, c ir.Change) error

	// StampShard stamps the ADR-0048 Shape-A discriminator onto a
	// row-bearing change before dispatch; empty wiring is the engine's
	// no-op fast path.
	StampShard func(c ir.Change)

	// Classify is the engine's applier-error classifier (ADR-0038).
	Classify func(error) error

	// WritePosition upserts the stream's position row on the open tx —
	// the first half of the ADR-0007 position-and-data atomicity
	// contract. The engine hook owns its control-table addressing and
	// per-exec timeout; errors come back unwrapped (the loop returns
	// them as-is after rolling back, matching the pre-extraction
	// loops).
	WritePosition func(ctx context.Context, tx *sql.Tx, streamID, token string) error

	// Commit commits the batch tx under the engine's Bug-56 watchdog
	// (commitWithTimeout).
	Commit func(tx *sql.Tx) error

	// AfterCommit, when non-nil, runs after a successful commit with
	// the batch's position token — PG's slot-ack feedback report
	// (Bug 15, ADR-0020). Deliberately after Commit: a crash between
	// the two only loses one tracker update, which the next batch's
	// report supersedes.
	AfterCommit func(ctx context.Context, token string)

	// CacheSchemaSnapshot, when non-nil, runs after a SchemaSnapshot
	// batch commits on the TransactionalDDL=true path (ADR-0049
	// Chunk C cache-after-commit invariant: never on a rolled-back
	// tx). On the TransactionalDDL=false path the snapshot routes
	// through ApplyOne, which owns the cache update itself.
	CacheSchemaSnapshot func(snap ir.SchemaSnapshot)

	// IsKeylessTable, when non-nil, reports whether a row-bearing change
	// targets a TRULY-KEYLESS table — one with no PRIMARY KEY and no
	// usable unique index, so the engine's Insert falls back to a plain
	// (non-idempotent) INSERT (ADR-0010 / Bug 125 class 3). Such a change
	// is treated as a flush boundary (ADR-0089 keyless guard): it is
	// dispatched and then the batch commits immediately, so a keyless
	// table's crash-replay blast radius stays at exactly 1 duplicate per
	// change — identical to --apply-batch-size=1 — even when the default
	// adaptive batch size would otherwise group many changes. PK and
	// unique-keyed tables (idempotent) are unaffected and batch normally.
	// nil (or an engine without the signal) means "never keyless" — no
	// flush boundary, behaviour unchanged. The predicate is consulted
	// only for row-bearing changes, AFTER they dispatch (so an engine
	// that populates its key cache during dispatch sees a cache hit); ctx
	// is supplied because an engine without a dispatch-populated signal
	// (MySQL) may run a one-time metadata query to classify the table.
	IsKeylessTable func(ctx context.Context, c ir.Change) bool
}

// RunBatchLoop is the shared ApplyBatch outer loop: consult the AIMD
// controller for the effective row cap, run one batch, log it, repeat
// until the channel closes or a batch fails. Callers (each engine's
// ApplyBatch) validate streamID and handle the maxBatchSize <= 1
// fall-through to the per-change Apply path before calling.
func RunBatchLoop(ctx context.Context, cfg *BatchConfig, streamID string, changes <-chan ir.Change, maxBatchSize int) error {
	for {
		// ADR-0052: when an AIMD controller is wired via
		// SetBatchSizeProvider, consult it before each batch so the
		// controller's current target drives the row cap. The static
		// maxBatchSize remains the absolute ceiling — provider returns
		// are clamped to it so an operator-supplied --apply-batch-size=N
		// remains a hard cap the controller can never exceed.
		effective := maxBatchSize
		if cfg.BatchSizeProvider != nil {
			next := cfg.BatchSizeProvider.NextBatchSize()
			if next > 0 && next < effective {
				effective = next
			}
		}
		batchN, lastPos, channelClosed, err := RunOneBatch(ctx, cfg, streamID, changes, effective)
		if err != nil {
			return err
		}
		if batchN > 0 {
			logBatchCommitted(ctx, cfg.EngineName, streamID, batchN, lastPos.Token)
		}
		if channelClosed {
			return nil
		}
	}
}

// RunOneBatch consumes up to maxBatchSize changes from the channel
// and applies them in a single target transaction along with the
// position write of the last applied change. Returns the number of
// changes applied, the last applied change's position, whether the
// channel closed (signalling clean shutdown to the caller), and any
// error.
//
// Exported (rather than folded into [RunBatchLoop]) because each
// engine keeps a one-line applyOneBatch delegate over it — the
// item-18 AIMD unit pins drive a single batch cycle directly to
// assert the apply-only latency timing.
//
// On error the open transaction is rolled back. On clean exit (row
// cap, byte cap, idle grace, channel close, TxCommit flush, or a
// schema-event flush) the transaction is committed.
func RunOneBatch(ctx context.Context, cfg *BatchConfig, streamID string, changes <-chan ir.Change, maxBatchSize int) (n int, lastPos ir.Position, channelClosed bool, err error) {
	// GitHub #18 Phase 1 + roadmap item 18: batch-latency telemetry.
	// Measure wall-clock for the APPLY WORK only — begin-tx →
	// dispatch(es) → position write → commit — NOT the time spent
	// blocked in the pre-tx wait loop below waiting for the first
	// row-bearing change. batchStart is therefore declared here (so the
	// closure captures it) but assigned only after the pre-tx wait loop,
	// immediately before BeginTx. DEBUG-only and elided on n==0; typical
	// operators run INFO and never see this; --log-level=debug surfaces
	// it for telemetry runs.
	//
	// Why this matters (ADR-0052): the same wall-clock duration feeds
	// the AIMD controller via ObserveBatch. Including the wait-for-work
	// made a sparse/bursty stream's first batch report tens of seconds
	// of latency (a fraction apply, the rest blocked), which the
	// controller read as "apply is catastrophically slow" and collapsed
	// batch size 1000→1 — throttling drain throughput ~2x. Timing
	// apply-only un-collapses it.
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
		if cfg.BatchObserver != nil && (n > 0 || err != nil) {
			cfg.BatchObserver.ObserveBatch(ctx, latency, n, err)
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
				// Empty source tx (BEGIN → COMMIT with no row events)
				// or a boundary that arrived after a previous batch
				// flushed — both are no-ops; keep waiting for the
				// next row event.
				continue
			}
			first = c
		case <-ctx.Done():
			return 0, ir.Position{}, false, ctx.Err()
		}
		break
	}

	// TransactionalDDL=false (MySQL): a schema event as the first
	// change of a batch dispatches alone via the per-change ApplyOne
	// path BEFORE any tx opens — DDL implicit-commits, so it can never
	// share the batch tx. The "batch of 1" shape is logged uniformly
	// by the outer loop. batchStart stays zero here, so the defer's
	// IsZero guard skips observing these rare schema-event paths (they
	// were never meaningful AIMD-row-throughput signals).
	if !cfg.TransactionalDDL && isSchemaEvent(first) {
		if err := cfg.ApplyOne(ctx, streamID, first); err != nil {
			return 0, ir.Position{}, false, err
		}
		return 1, first.Pos(), false, nil
	}

	// PII Phase 1.5: redact the first change before dispatch.
	// Subsequent batch members are redacted in the loop below.
	if err := cfg.Redact(ctx, first); err != nil {
		return 0, ir.Position{}, false, cfg.Classify(fmt.Errorf("%s: applier: redact: %w", cfg.EngineName, err))
	}
	// ADR-0048 Shape A: stamp the operator-supplied discriminator
	// onto the first change before dispatch (sibling-tier to the
	// redact call above). Empty shard wiring is a no-op fast path.
	cfg.StampShard(first)

	// Item 18 Fix A: start the apply-latency clock here — after the
	// pre-tx wait loop has returned the first change — so the measured
	// latency reflects apply work (begin-tx → dispatch → position write
	// → commit) and not the blocked wait for the first change.
	batchStart = time.Now()
	tx, err := cfg.BeginTx(ctx)
	if err != nil {
		return 0, ir.Position{}, false, err
	}

	if err := cfg.Dispatch(ctx, tx, streamID, first); err != nil {
		_ = tx.Rollback()
		logBatchRollback(ctx, cfg.EngineName, streamID, 1, err)
		return 0, ir.Position{}, false, cfg.Classify(err)
	}
	n = 1
	lastPos = first.Pos()
	batchBytes := ir.ApproximateChangeBytes(first)

	// TransactionalDDL=true (PG): a schema event was just dispatched
	// onto `tx`. Flush it as a 1-change batch so the commitBatch
	// position write rides the SAME tx (ADR-0049 locked decision #4a)
	// and a Truncate's cache-invalidation blast radius stays contained
	// to its own transaction. The post-commit cache hook runs ONLY
	// after commitBatch reports nil (Chunk C cache-after-commit).
	if cfg.TransactionalDDL {
		if _, isTruncate := first.(ir.Truncate); isTruncate {
			return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
		}
		if snap, isSnap := first.(ir.SchemaSnapshot); isSnap {
			if err := commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n); err != nil {
				return 0, ir.Position{}, false, err
			}
			cfg.CacheSchemaSnapshot(snap)
			return n, lastPos, false, nil
		}
	}

	// ADR-0089 keyless guard: a change to a truly-keyless (non-idempotent)
	// table must not batch — flush it as a batch of 1 so a crash-replay
	// can't amplify duplicates past the single-row baseline. `first` is a
	// row-bearing change here (schema events returned above) and has just
	// dispatched, so the engine's key cache is populated for the lookup.
	if cfg.IsKeylessTable != nil && cfg.IsKeylessTable(ctx, first) {
		return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
	}

	// Idle-flush timer: commit a partial batch if no further change
	// arrives within DefaultIdleFlushPeriod, so the persisted
	// source_position keeps current on quiet streams (item 18 Fix B;
	// see the constant's doc for the 100ms reasoning).
	idle := time.NewTimer(DefaultIdleFlushPeriod)
	defer idle.Stop()

	for n < maxBatchSize {
		select {
		case c, ok := <-changes:
			if !ok {
				channelClosed = true
				return n, lastPos, channelClosed, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
			}
			// Source-tx boundary handling (ADR-0027). TxCommit flushes
			// the in-flight target tx so the apply aligns with the
			// source's commit boundary; the position written is the
			// source commit's position, which is the right resume
			// point under ADR-0007 (the position of the last durably-
			// applied work) and ADR-0010 idempotency. TxBegin observed
			// mid-batch is a no-op: flushing on TxCommit is sufficient
			// to keep alignment.
			if _, isTxCommit := c.(ir.TxCommit); isTxCommit {
				lastPos = c.Pos()
				return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
			}
			if _, isTxBegin := c.(ir.TxBegin); isTxBegin {
				// Reset the idle timer so a TxBegin observed mid-batch
				// doesn't make the next idle window expire from the
				// previous row event's timestamp; otherwise an
				// otherwise-quiet stream would idle-flush
				// inconsistently.
				resetIdleTimer(idle)
				continue
			}
			// TransactionalDDL=false (MySQL): flush the in-flight
			// non-DDL changes first so the position write of the
			// *previous* change lands before the DDL's implicit commit
			// destroys our tx, then apply the schema event alone via
			// the per-change path. Return rows=1 and the event's
			// position so the outer loop logs it as its own batch.
			if !cfg.TransactionalDDL && isSchemaEvent(c) {
				if err := commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n); err != nil {
					return 0, ir.Position{}, false, err
				}
				logBatchCommitted(ctx, cfg.EngineName, streamID, n, lastPos.Token)
				if err := cfg.ApplyOne(ctx, streamID, c); err != nil {
					return 0, ir.Position{}, false, err
				}
				return 1, c.Pos(), false, nil
			}
			// PII Phase 1.5: redact each subsequent batch member
			// before dispatch, then stamp the ADR-0048 Shape-A
			// discriminator (sibling-tier, same as the first change).
			if err := cfg.Redact(ctx, c); err != nil {
				_ = tx.Rollback()
				return 0, ir.Position{}, false, cfg.Classify(fmt.Errorf("%s: applier: redact: %w", cfg.EngineName, err))
			}
			cfg.StampShard(c)
			if err := cfg.Dispatch(ctx, tx, streamID, c); err != nil {
				_ = tx.Rollback()
				logBatchRollback(ctx, cfg.EngineName, streamID, n+1, err)
				return 0, ir.Position{}, false, cfg.Classify(err)
			}
			n++
			lastPos = c.Pos()
			batchBytes += ir.ApproximateChangeBytes(c)
			// TransactionalDDL=true (PG): the schema event's write just
			// landed on `tx`; flush now so the commitBatch position
			// write rides the SAME tx (ADR-0049 locked decision #4a)
			// and the event is durable before the post-DDL rows (which
			// arrive in later batches) are applied.
			if cfg.TransactionalDDL {
				if _, isTruncate := c.(ir.Truncate); isTruncate {
					return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
				}
				if snap, isSnap := c.(ir.SchemaSnapshot); isSnap {
					if err := commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n); err != nil {
						return 0, ir.Position{}, false, err
					}
					// ADR-0049 Chunk C cache-after-commit: see the
					// first-change branch above for the rolled-back-tx
					// rationale.
					cfg.CacheSchemaSnapshot(snap)
					return n, lastPos, false, nil
				}
			}
			// ADR-0089 keyless guard (mid-batch): flush the batch
			// (including the just-dispatched keyless change `c`) so a
			// keyless table never rides a multi-change replay window. The
			// prior in-batch changes are PK/unique-keyed (idempotent on
			// replay); only `c` could duplicate, bounding the blast radius
			// to 1 — the same as --apply-batch-size=1. See the first-change
			// branch above.
			if cfg.IsKeylessTable != nil && cfg.IsKeylessTable(ctx, c) {
				return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
			}
			// Byte-cap flush (ADR-0028): bounds the in-flight tx's
			// buffered parameter memory on wide-row streams. Checked
			// after the dispatch so the just-dispatched change is
			// included in the count we commit.
			if batchBytes >= cfg.ByteCap {
				slog.DebugContext(
					ctx, cfg.EngineName+": applier: byte-cap flush",
					slog.String("stream_id", streamID),
					slog.Int("rows", n),
					slog.Int64("bytes", batchBytes),
					slog.Int64("byte_cap", cfg.ByteCap),
				)
				// ADR-0052 DP-4 (b): when the byte-cap fires before the
				// row-cap on a sustained shape, AI-ing rows can't help
				// (the binding constraint is bytes). The controller logs
				// the advisory at most once per cool-off period.
				if cfg.BatchSizeProvider != nil && n < maxBatchSize {
					if hinter, ok := cfg.BatchSizeProvider.(interface {
						NoteByteCapDominant(ctx context.Context, rows int, bytes, byteCap int64)
					}); ok {
						hinter.NoteByteCapDominant(ctx, n, batchBytes, cfg.ByteCap)
					}
				}
				return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
			}
			// Reset the idle timer for each successful change so the
			// timer measures gaps between events, not absolute time
			// since first.
			resetIdleTimer(idle)
		case <-idle.C:
			slog.DebugContext(
				ctx, cfg.EngineName+": applier: idle flush",
				slog.String("stream_id", streamID),
				slog.Int("rows", n),
				slog.Duration("idle", DefaultIdleFlushPeriod),
			)
			return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
		case <-ctx.Done():
			_ = tx.Rollback()
			return 0, ir.Position{}, false, ctx.Err()
		}
	}

	return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n)
}

// commitBatch writes the position then commits the open tx, then
// fires the optional AfterCommit hook (PG's slot-ack report — see the
// [BatchConfig.AfterCommit] doc for the crash-window reasoning).
// Returns a wrapped error on either failure with a rollback already
// attempted on the position-write path. This ordering IS the ADR-0007
// position-and-data atomicity contract; do not reorder.
func commitBatch(ctx context.Context, cfg *BatchConfig, tx *sql.Tx, streamID, token string, rows int) error {
	if err := cfg.WritePosition(ctx, tx, streamID, token); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(
			ctx, cfg.EngineName+": applier: batch rollback on position-write error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return err
	}
	if err := cfg.Commit(tx); err != nil {
		slog.WarnContext(
			ctx, cfg.EngineName+": applier: batch commit error",
			slog.String("stream_id", streamID),
			slog.Int("rows_attempted", rows),
			slog.String("err", err.Error()),
		)
		return cfg.Classify(fmt.Errorf("%s: applier: commit: %w", cfg.EngineName, err))
	}
	if cfg.AfterCommit != nil {
		cfg.AfterCommit(ctx, token)
	}
	return nil
}

// isSchemaEvent reports whether c is a schema-changing event the
// batch loop must flush around (Truncate) or persist atomically with
// a position write (SchemaSnapshot). The TransactionalDDL flag picks
// which of the two handling shapes applies.
func isSchemaEvent(c ir.Change) bool {
	switch c.(type) {
	case ir.Truncate, ir.SchemaSnapshot:
		return true
	}
	return false
}

// resetIdleTimer re-arms the idle-flush timer using the
// stop-drain-reset idiom: Stop may return false because the timer
// already fired, in which case the stale tick is drained so Reset
// arms a clean window rather than leaving a buffered expiry that
// would idle-flush the next batch member instantly.
func resetIdleTimer(idle *time.Timer) {
	if !idle.Stop() {
		select {
		case <-idle.C:
		default:
		}
	}
	idle.Reset(DefaultIdleFlushPeriod)
}

// logBatchCommitted is the shared "batch committed" DEBUG line —
// emitted by the outer loop after every successful batch and by the
// TransactionalDDL=false mid-batch flush before a schema event
// applies alone.
func logBatchCommitted(ctx context.Context, engineName, streamID string, rows int, token string) {
	slog.DebugContext(
		ctx, engineName+": applier: batch committed",
		slog.String("stream_id", streamID),
		slog.Int("rows", rows),
		slog.String("position_token", TruncateToken(token, 80)),
	)
}

// logBatchRollback is the shared "batch rollback on error" WARN line
// for dispatch failures.
func logBatchRollback(ctx context.Context, engineName, streamID string, rowsAttempted int, err error) {
	slog.WarnContext(
		ctx, engineName+": applier: batch rollback on error",
		slog.String("stream_id", streamID),
		slog.Int("rows_attempted", rowsAttempted),
		slog.String("err", err.Error()),
	)
}
