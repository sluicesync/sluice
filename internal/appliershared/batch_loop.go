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
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// BatchTx is the minimal transaction handle the shared batch loop needs:
// it opens a tx (via [BatchConfig.BeginTx]), dispatches writes onto it,
// writes the position + commits via [BatchConfig.WritePosition] /
// [BatchConfig.Commit], and rolls it back on any error. The loop itself
// touches the handle ONLY to roll it back — every other operation is
// delegated to the engine's closures, which know the concrete type.
//
// ADR-0092: generalizing the seam from a concrete `*sql.Tx` to this
// interface is what lets the Postgres engine return a pipelined
// `*pgxBatchTx` (accumulating onto a pgx.Batch flushed in one round trip
// at commit) while MySQL and the PG non-pipelined fall-back keep using
// `*sql.Tx`. `*sql.Tx` already satisfies BatchTx, so the existing engines
// type-assert inside their closures with zero behaviour change.
type BatchTx interface {
	Rollback() error
}

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

	// CheckpointOnlyAtTxBoundary, when true, makes the loop persist the
	// resume position ONLY on a source-transaction boundary flush
	// (TxCommit) or a DDL flush (Truncate / SchemaSnapshot, which
	// implicit-commit on MySQL and ride their own tx on PG) — never on a
	// row-cap / byte-cap / idle / keyless / channel-close flush that lands
	// mid-transaction. The batch's DATA still commits on every flush
	// (memory stays bounded, ADR-0028); only the *position write* is gated.
	//
	// This exists because MySQL file/pos resume is NOT valid mid-transaction:
	// go-mysql seeks to the persisted byte offset and starts reading there,
	// so a position pointing at a ROWS event whose TABLE_MAP was earlier in
	// the same transaction fails with "no corresponding table map event" and
	// the stream cannot warm-resume (found live on the large-scale program:
	// the serial applier, driven to AIMD batch-size 1 by a slow cross-region
	// target, split a large multi-row source transaction across batches and
	// persisted a mid-tx position; every restart then crash-looped). The
	// concurrent laneapply path already enforces this invariant via its
	// frontier ("must NOT persist a partial point"); this flag gives the
	// serial loop the same guarantee. On a mid-tx flush the position row
	// simply retains its last boundary value, so a crash re-reads the whole
	// in-flight transaction from the previous boundary and idempotently
	// re-applies it (ADR-0010) — at-least-once for the interrupted tx, the
	// same model the keyless guard and the concurrent frontier already use.
	//
	// false (Postgres): unchanged. PG logical-replication resume is by LSN
	// and the walsender resends whole transactions from restart_lsn, so a
	// mid-tx position is a valid restart point; persisting lastPos every
	// flush keeps the slot's confirmed_flush_lsn advancing promptly
	// (ADR-0020). Engines whose AfterCommit advances a slot ack must leave
	// this false.
	CheckpointOnlyAtTxBoundary bool

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
	BeginTx func(ctx context.Context) (BatchTx, error)

	// Dispatch routes one change to its SQL form on the open tx — the
	// engine's existing dispatch method. The loop owns rollback,
	// logging, and classification of a dispatch failure. The engine
	// type-asserts the concrete tx (e.g. *sql.Tx or *pgxBatchTx) it
	// returned from BeginTx.
	Dispatch func(ctx context.Context, tx BatchTx, streamID string, c ir.Change) error

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
	WritePosition func(ctx context.Context, tx BatchTx, streamID, token string) error

	// Commit commits the batch tx under the engine's Bug-56 watchdog
	// (commitWithTimeout). For the PG pipelined path, Commit is where
	// the accumulated pgx.Batch is flushed in a single round trip
	// (ADR-0092) before the underlying pgx.Tx commits.
	Commit func(tx BatchTx) error

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
	// dispatched and then the batch commits immediately, so the guard does
	// not amplify keyless duplication past what --apply-batch-size=1 already
	// exposes, even when the default adaptive batch size would otherwise
	// group many changes. It does NOT make keyless CDC exactly-once: resume
	// granularity is the SOURCE TRANSACTION (the GTID/LSN only advances at
	// its commit), so every keyless row in one interrupted source
	// transaction shares the same pre-transaction resume position and
	// replays together on crash-recovery — keyless CDC is at-least-once
	// (Bug 143). PK and unique-keyed tables (idempotent) are unaffected and
	// batch normally.
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

	// Wait for the first row-bearing change before opening the tx
	// (see waitForFirstChange). A closed channel here is the clean
	// shutdown signal; a ctx cancel or a boundary-only position-write
	// failure surfaces as an error — batchStart is still zero on both,
	// so the defer's IsZero guard keeps them out of the AIMD window.
	first, chClosed, werr := waitForFirstChange(ctx, cfg, streamID, changes)
	if werr != nil {
		return 0, ir.Position{}, false, werr
	}
	if chClosed {
		return 0, ir.Position{}, true, nil
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
			return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, true)
		}
		if snap, isSnap := first.(ir.SchemaSnapshot); isSnap {
			if err := commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, true); err != nil {
				return 0, ir.Position{}, false, err
			}
			cfg.CacheSchemaSnapshot(snap)
			return n, lastPos, false, nil
		}
	}

	// ADR-0089 keyless guard: a change to a truly-keyless (non-idempotent)
	// table must not batch — flush it as a batch of 1 so a crash-replay
	// can't amplify duplicates past the single-row (--apply-batch-size=1)
	// baseline. This does not make keyless CDC exactly-once — the in-flight
	// source transaction still replays on resume (see IsKeylessTable doc;
	// Bug 143). `first` is a row-bearing change here (schema events returned
	// above) and has just dispatched, so the engine's key cache is populated
	// for the lookup.
	if cfg.IsKeylessTable != nil && cfg.IsKeylessTable(ctx, first) {
		return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, false)
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
				return n, lastPos, channelClosed, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, false)
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
				return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, true)
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
				if err := commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, false); err != nil {
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
					return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, true)
				}
				if snap, isSnap := c.(ir.SchemaSnapshot); isSnap {
					if err := commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, true); err != nil {
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
			// keyless table never rides a larger batch's replay window. The
			// prior in-batch changes are PK/unique-keyed (idempotent on
			// replay); the guard bounds the batch the same as
			// --apply-batch-size=1 — it does NOT make `c` exactly-once
			// (keyless CDC is at-least-once; see the IsKeylessTable doc and
			// the first-change branch above; Bug 143).
			if cfg.IsKeylessTable != nil && cfg.IsKeylessTable(ctx, c) {
				return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, false)
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
				return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, false)
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
			return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, false)
		case <-ctx.Done():
			_ = tx.Rollback()
			return 0, ir.Position{}, false, ctx.Err()
		}
	}

	return n, lastPos, false, commitBatch(ctx, cfg, tx, streamID, lastPos.Token, n, false)
}

// waitForFirstChange blocks until the first row-bearing change of a
// batch arrives, returning it. Opening the batch tx then blocking with
// no work would hold a pool connection idle arbitrarily long, so the
// wait happens on the channel with no tx open (ADR-0027). A closed
// channel returns channelClosed=true (clean shutdown); a ctx cancel
// returns its error. TxBegin / TxCommit boundary events observed on the
// empty batch are consumed here:
//
//   - TxBegin: a source-tx start with no batch open yet — a no-op; keep
//     waiting for the transaction's first row event.
//   - TxCommit: an empty tx (BEGIN → COMMIT with no rows, e.g. a
//     heartbeat) or — the load-bearing case under
//     CheckpointOnlyAtTxBoundary — the trailing COMMIT of a tx whose
//     rows already committed in prior batches. When mid-tx flushes skip
//     the position write, this is the ONLY place the boundary can be
//     persisted, so writeBoundaryOnly advances it in a dedicated
//     position-only tx (the rows are already durable via serial in-order
//     apply). Without the flag (PG) this is a pure no-op — PG already
//     advanced its position on the rows' own flushes.
func waitForFirstChange(ctx context.Context, cfg *BatchConfig, streamID string, changes <-chan ir.Change) (first ir.Change, channelClosed bool, err error) {
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return nil, true, nil
			}
			switch c.(type) {
			case ir.TxBegin:
				continue
			case ir.TxCommit:
				if cfg.CheckpointOnlyAtTxBoundary {
					if err := writeBoundaryOnly(ctx, cfg, streamID, c.Pos().Token); err != nil {
						return nil, false, err
					}
				}
				continue
			}
			return c, false, nil
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

// commitBatch writes the position then commits the open tx, then
// fires the optional AfterCommit hook (PG's slot-ack report — see the
// [BatchConfig.AfterCommit] doc for the crash-window reasoning).
// Returns a wrapped error on either failure with a rollback already
// attempted on the position-write path. This ordering IS the ADR-0007
// position-and-data atomicity contract; do not reorder.
//
// atBoundary reports whether `token` is a safe source-transaction /
// DDL boundary resume point. When [BatchConfig.CheckpointOnlyAtTxBoundary]
// is set and atBoundary is false, the position write (and the AfterCommit
// slot-ack) is SKIPPED: the batch's data still commits, but the persisted
// position retains its last boundary value so warm-resume re-reads the
// whole in-flight transaction (see the CheckpointOnlyAtTxBoundary doc for
// why MySQL file/pos cannot resume mid-transaction). When the flag is
// false (Postgres) every flush writes the position as before.
func commitBatch(ctx context.Context, cfg *BatchConfig, tx BatchTx, streamID, token string, rows int, atBoundary bool) error {
	skipPosition := cfg.CheckpointOnlyAtTxBoundary && !atBoundary
	if !skipPosition {
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
	if !skipPosition && cfg.AfterCommit != nil {
		cfg.AfterCommit(ctx, token)
	}
	return nil
}

// writeBoundaryOnly persists a source-transaction boundary position in
// its own tiny transaction (begin → WritePosition → commit), with no row
// data. It exists for the CheckpointOnlyAtTxBoundary path: when a tx's
// rows committed across earlier batches and the trailing TxCommit arrives
// on an empty batch (the pre-tx wait loop), this is the only place the
// boundary checkpoint can advance. The rows are already durable (serial
// in-order apply), so persisting the boundary afterward never moves the
// position ahead of durable data (ADR-0007). Mirrors commitBatch's
// position-then-commit-then-AfterCommit ordering for the one-row-less case.
func writeBoundaryOnly(ctx context.Context, cfg *BatchConfig, streamID, token string) error {
	tx, err := cfg.BeginTx(ctx)
	if err != nil {
		return err
	}
	if err := cfg.WritePosition(ctx, tx, streamID, token); err != nil {
		_ = tx.Rollback()
		slog.WarnContext(
			ctx, cfg.EngineName+": applier: boundary-position rollback on write error",
			slog.String("stream_id", streamID),
			slog.String("err", err.Error()),
		)
		return err
	}
	if err := cfg.Commit(tx); err != nil {
		return cfg.Classify(fmt.Errorf("%s: applier: boundary commit: %w", cfg.EngineName, err))
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
