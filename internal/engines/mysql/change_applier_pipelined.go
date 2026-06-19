// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// # Pipelined CDC apply for MySQL (ADR-0104 Phase 1)
//
// The shared batch loop ([appliershared.RunOneBatch]) applies a batch of
// N changes as N serial tx.ExecContext round trips, then a position
// write, then a commit. Batching amortises the commit fsync over N
// changes (the Bug-141 win), but it does nothing for the per-batch
// round trips: BEGIN → N execs → position → COMMIT are serial, so on a
// cross-region link apply throughput is bounded by ~1/RTT regardless of
// batch size. On the live Track-B PlanetScale-MySQL link this is the
// measured cause of the item-23 per-shard MinimizeSkew wedge.
//
// MySQL has no pgx.Batch statement-pipelining (the ADR-0092 PG lever):
// go-sql-driver/mysql's wire protocol is request-response per statement.
// So the MySQL lever is DIFFERENT — overlap the cross-region commit RTTs
// across a bounded window of W INDEPENDENT in-flight transactions, each
// on its own dedicated backend, committed STRICTLY in submission (source)
// order. While transaction i is blocked on its commit RTT, transactions
// i+1…i+W-1 are already dispatching their data; the aggregate apply
// ceiling rises toward W/RTT while the commit LINEARIZATION POINT stays
// in source order (the correctness).
//
// The load-bearing correctness invariants (each pinned):
//
//   - **Strict submission-order commit.** Two consecutive batches can
//     carry dependent rows (an INSERT in batch i, its UPDATE in batch
//     i+1). Committing i+1 before i would apply the UPDATE before the
//     INSERT. A single commit-worker goroutine drains ONE FIFO channel
//     in receive order, which is the order BeginTx assigned sequence
//     numbers (the applier loop is single-goroutine), so out-of-order
//     commit is structurally impossible.
//
//   - **Value encoding unchanged.** Every statement is built by the
//     SAME build{Insert,Update,Delete,Truncate}SQL builders and the
//     prepareApplierValue / prepareValue codec the serial path uses, and
//     executed by the SAME txExec on a plain *sql.Tx. The Bug-6 JSON
//     CAST(? AS JSON) WHERE form, the _binary-charset avoidance, and the
//     row-alias UPSERT all carry over byte-for-byte. The pipeline changes
//     only WHEN a tx commits, never HOW a value is encoded.
//
//   - **Position + data atomic per batch (ADR-0007).** The position
//     write rides each batch's own tx via the unchanged writePositionTx;
//     a crash rolls back both. The PERSISTED position is always the
//     highest CONTIGUOUSLY-committed batch's token — never a gap — because
//     strict-order commit means batch i durably commits (position + data)
//     before i+1's commit is even attempted.
//
//   - **Zero-value-safe depth (the v0.99.51 trap).** Pipelining engages
//     ONLY when an explicit operator-set depth > 1 is wired
//     (SetApplyPipelineDepth). The Go zero value (0) and 1 both resolve to
//     serial — byte-identical to the pre-ADR-0104 path, no pool opened, no
//     WARN. Every non-CLI construction (tests, broker, future callers)
//     gets the safe default for free.
//
//   - **Loud fallback.** If the dedicated pool cannot open, the path
//     falls back to the serial *sql.Tx batch loop with a one-time WARN —
//     no throughput claim made silently (mirrors ADR-0092's
//     warnPipelineFallbackOnce).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
)

// mysqlPipeline is the ADR-0104 Phase-1 ordered commit pipeline. It owns
// the dedicated W-backend pool, the bounded in-flight window, and the
// single commit-worker goroutine that commits batches strictly in
// submission order. One pipeline per applier, lazily started on the first
// pipelined BeginTx; nil until then.
//
// Concurrency ownership (race-clean by construction):
//   - The applier loop goroutine is the ONLY producer: it calls begin()
//     (acquire a window slot + sequence) then handCommit() (enqueue onto
//     commitQ) in strict source order. No other goroutine produces.
//   - The commit worker is the ONLY consumer of commitQ and the ONLY
//     goroutine that touches a handed-off tx after handCommit — it
//     commits each in receive order and reports the outcome. There is no
//     shared mutable batch state between producer and consumer beyond the
//     bounded channels and the mutex-guarded firstErr cell.
type mysqlPipeline struct {
	a       *ChangeApplier
	db      *sql.DB // dedicated pool, MaxOpenConns == depth
	dep     int     // resolved window depth W (> 1)
	nextSeq uint64  // producer-owned submission counter

	// slots bounds the in-flight window to W: begin() sends (acquires),
	// the worker receives (releases) after each commit completes. A
	// buffered channel of capacity W is the window.
	slots chan struct{}

	// commitQ carries handed-off batches to the single commit worker in
	// submission order. Buffered to W so a producer that already holds a
	// slot never blocks enqueuing (the slot acquire is the real bound).
	commitQ chan *mysqlBatchTx

	wg sync.WaitGroup // tracks the commit worker

	mu       sync.Mutex // guards firstErr + closed
	firstErr error      // first async commit error, surfaced fail-fast
	closed   bool       // drain() has run; no further hand-offs
}

// mysqlBatchTx is the ADR-0104 pipelined-apply transaction handle. It
// pins one *sql.Tx on a dedicated backend, accumulates the batch's data
// writes + the position write on it exactly as the serial path does, and
// — instead of committing inline — hands itself to the commit pipeline at
// Commit() so the next batch can begin while this one's commit RTT is in
// flight. It satisfies [appliershared.BatchTx]; the shared loop only ever
// calls Rollback on it, while the applier's Dispatch / WritePosition /
// Commit closures type-assert it.
type mysqlBatchTx struct {
	p   *mysqlPipeline
	ctx context.Context //nolint:containedctx // batch lives within one RunOneBatch call under this ctx; the async commit worker needs it and the shared Commit closure takes none

	tx    *sql.Tx
	seq   uint64 // submission sequence (debug/assertions)
	token string // position token written on this tx (set by WritePosition)
	rows  int    // change count in this batch (for the AIMD observer)

	// handedOff is set once Commit hands the tx to the commit worker, so a
	// late Rollback from the loop's error path is a no-op (the worker owns
	// the tx from that point).
	handedOff bool
	// slotHeld tracks whether this handle still holds a window slot, so a
	// Rollback before hand-off releases it.
	slotHeld bool
	// syncCommit forces this batch to commit SYNCHRONOUSLY with the window
	// fully drained before and after — the ADR-0089 keyless guard: a
	// truly-keyless Insert must linearize exactly as --apply-batch-size=1
	// so the pipeline never widens its at-least-once replay window past
	// the serial baseline. Set during dispatch of a keyless Insert.
	syncCommit bool
}

// errPipelineUnavailable signals the dedicated pool could not be opened
// (or depth resolved to serial); the batch path falls back to the serial
// *sql.Tx loop with a one-time WARN.
var errPipelineUnavailable = errors.New("mysql: applier: pipelined apply unavailable")

// pipelineEnabled reports whether the operator wired an explicit
// apply-pipeline depth > 1. Zero/1 (every non-CLI construction's zero
// value) is serial — the v0.99.51 zero-value-safe default.
func (a *ChangeApplier) pipelineEnabled() bool {
	return a.applyPipelineDepth > 1
}

// pipeline returns the lazily-started pipeline, or errPipelineUnavailable
// if depth is serial or the dedicated pool cannot open. The pool is sized
// to exactly W backends so the in-flight window can never starve and W is
// an honest connection-budget statement (there is no MySQL
// TargetConnectionBudgetProber to clamp against — ADR-0104 Consequences).
func (a *ChangeApplier) pipeline(ctx context.Context) (*mysqlPipeline, error) {
	if !a.pipelineEnabled() {
		return nil, errPipelineUnavailable
	}
	if a.pipelinePool != nil {
		return a.pipelinePool, nil
	}
	if a.pipelineCfg == nil {
		// Direct-API / unit constructions never set pipelineCfg, so they
		// cannot open a dedicated pool — fall back to serial.
		return nil, errPipelineUnavailable
	}
	db, err := openDB(ctx, a.pipelineCfg)
	if err != nil {
		return nil, fmt.Errorf("mysql: applier: open pipelined pool: %w", err)
	}
	// Cap the pool at exactly W: the window never needs more than W
	// concurrent backends, and a hard cap keeps the WARN's "opens W
	// backends" claim honest. SetMaxIdleConns matches so idle backends
	// are not churned between batches on a steady stream.
	db.SetMaxOpenConns(a.applyPipelineDepth)
	db.SetMaxIdleConns(a.applyPipelineDepth)

	p := &mysqlPipeline{
		a:       a,
		db:      db,
		dep:     a.applyPipelineDepth,
		slots:   make(chan struct{}, a.applyPipelineDepth),
		commitQ: make(chan *mysqlBatchTx, a.applyPipelineDepth),
	}
	p.wg.Add(1)
	go p.commitWorker()
	a.pipelinePool = p
	a.logPipelineEngagedOnce(ctx, a.applyPipelineDepth)
	return p, nil
}

// commitWorker is the SINGLE consumer of commitQ. It commits each
// handed-off batch in receive order (== submission order) under the
// Bug-56 commit watchdog, records the first error, and releases the
// batch's window slot. Being the only goroutine that commits is what
// makes out-of-order commit structurally impossible.
func (p *mysqlPipeline) commitWorker() {
	defer p.wg.Done()
	for b := range p.commitQ {
		p.commitOne(b)
	}
}

// commitOne commits one handed-off batch under the Bug-56 watchdog,
// records the first error, observes the AIMD per-transaction commit
// latency (item-18 discipline: the controller must see real apply work,
// here the actual cross-region commit RTT — NOT the loop's near-zero
// hand-off wall time), and releases the window slot. The sole caller is
// the single commit worker, so it is also the single observation site —
// the source-order, no-overlap-poisoning property the controller needs.
func (p *mysqlPipeline) commitOne(b *mysqlBatchTx) {
	start := time.Now()
	err := appliershared.RunWithDeadline(p.a.execTimeout, b.tx.Commit)
	latency := time.Since(start)
	if err != nil {
		err = classifyApplierError(fmt.Errorf("mysql: applier: pipelined commit (seq %d): %w", b.seq, err))
		p.recordErr(err)
	}
	// Feed the AIMD controller the per-transaction commit latency. The
	// shared loop's BatchObserver is suppressed on the pipelined path (see
	// batchConfig) precisely so this is the single observation site and the
	// overlap does not poison the controller (ADR-0104 Consequences).
	if p.a.batchObserver != nil && (b.rows > 0 || err != nil) {
		p.a.batchObserver.ObserveBatch(b.ctx, latency, b.rows, err)
	}
	// Release the window slot AFTER the commit fully completes, so the
	// in-flight count reflects real durability, not just dispatch.
	<-p.slots
}

// recordErr stores the first async commit error fail-fast. Subsequent
// errors are dropped — the first failure stops the stream and the rest
// are consequential.
func (p *mysqlPipeline) recordErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.firstErr == nil {
		p.firstErr = err
	}
}

// pendingErr returns any async commit error observed so far without
// blocking, so the producing loop can fail fast on the next Commit/BeginTx
// rather than waiting for drain.
func (p *mysqlPipeline) pendingErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstErr
}

// begin acquires a window slot (blocking until the in-flight count drops
// below W) and opens a fresh tx on a dedicated backend. The slot is
// released by the commit worker after the batch commits (or by Rollback
// before hand-off). A pending async error short-circuits so a failed
// stream stops promptly instead of opening more work.
func (p *mysqlPipeline) begin(ctx context.Context) (*mysqlBatchTx, error) {
	if err := p.pendingErr(); err != nil {
		return nil, err
	}
	// Acquire a window slot. Honour ctx cancellation so a shutdown does
	// not block on a saturated window.
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	// Re-check after the (possibly blocking) acquire: an in-flight commit
	// may have failed while we waited.
	if err := p.pendingErr(); err != nil {
		<-p.slots
		return nil, err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		<-p.slots
		return nil, fmt.Errorf("mysql: applier: pipelined begin tx: %w", err)
	}
	p.nextSeq++
	return &mysqlBatchTx{p: p, ctx: ctx, tx: tx, seq: p.nextSeq, slotHeld: true}, nil
}

// handCommit hands a dispatched+position-written batch to the commit
// worker. For an ordinary (keyed) batch it returns immediately — the
// commit RTT overlaps the next batch's dispatch — surfacing any PRIOR
// async commit error so the stream fails fast. For a keyless batch
// (b.syncCommit, the ADR-0089 guard) it hands the batch off IN ORDER and
// then waits for the entire window to drain to empty, so the keyless
// single-row commit linearizes exactly as --apply-batch-size=1 (its
// at-least-once replay window is never widened by the pipeline). The
// position write already rode b.tx (ADR-0007); the worker's Commit makes
// position + data durable atomically.
func (p *mysqlPipeline) handCommit(b *mysqlBatchTx) error {
	b.handedOff = true
	b.slotHeld = false // ownership of the slot transfers to the worker
	p.commitQ <- b
	if b.syncCommit {
		// The worker commits b after every lower-numbered batch (FIFO), so
		// waiting for the window to empty guarantees b — and all priors —
		// are durably committed before the next batch begins. After-drain
		// is implicit: the next batch's begin() finds an empty window.
		p.waitWindowEmpty()
	}
	return p.pendingErr()
}

// waitWindowEmpty blocks until the in-flight window is empty (every slot
// released by the commit worker). It acquires all W slots — only possible
// once the window has drained — then releases them, restoring the empty
// window. Used by the keyless-guard sync-commit path. Single-producer
// safe: only the applier loop goroutine ever calls begin()/handCommit, so
// no other producer can re-fill the window between the drain and the
// release.
func (p *mysqlPipeline) waitWindowEmpty() {
	for i := 0; i < p.dep; i++ {
		p.slots <- struct{}{}
	}
	for i := 0; i < p.dep; i++ {
		<-p.slots
	}
}

// drain closes the commit queue, waits for every in-flight batch to
// commit, and returns the first error observed across the whole pipeline.
// Called when the applier loop exits (channel close) and on Close. After
// drain the pipeline accepts no further hand-offs.
func (p *mysqlPipeline) drain() error {
	p.mu.Lock()
	if p.closed {
		err := p.firstErr
		p.mu.Unlock()
		return err
	}
	p.closed = true
	p.mu.Unlock()

	close(p.commitQ)
	p.wg.Wait()
	return p.pendingErr()
}

// closePool drains then closes the dedicated pool. Idempotent.
func (p *mysqlPipeline) closePool() error {
	drainErr := p.drain()
	closeErr := p.db.Close()
	if drainErr != nil {
		return drainErr
	}
	return closeErr
}

// Rollback aborts the pipelined tx and releases its window slot. It
// satisfies [appliershared.BatchTx]; the shared loop calls it on every
// error path (dispatch failure, ctx cancel, position-write failure)
// BEFORE Commit hands the tx off. After hand-off the worker owns the tx,
// so a late Rollback is a no-op (handedOff guard).
func (b *mysqlBatchTx) Rollback() error {
	if b.handedOff {
		return nil
	}
	err := b.tx.Rollback()
	if b.slotHeld {
		<-b.p.slots
		b.slotHeld = false
	}
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		return err
	}
	return nil
}

// warnPipelineFallbackOnce logs a single WARN the first time the pipelined
// path is unavailable and the applier falls back to serial exec, so an
// operator sees the lost-throughput condition once (not per batch) and no
// throughput claim is made silently. Mirrors ADR-0092's PG counterpart.
func (a *ChangeApplier) warnPipelineFallbackOnce(ctx context.Context, cause error) {
	if a.pipelineWarnedFallback {
		return
	}
	a.pipelineWarnedFallback = true
	slog.WarnContext(ctx,
		"mysql: applier: pipelined CDC apply unavailable — falling back to serial per-batch commit "+
			"(no throughput improvement on high-latency links; ADR-0104)",
		slog.String("cause", cause.Error()))
}

// logPipelineEngagedOnce logs a single INFO the first time the pipeline
// opens its dedicated pool, naming the W backends it will use. This is the
// honest connection-budget statement ADR-0104 requires — no silent
// auto-clamp claim; the operator asked for depth W and gets W backends.
func (a *ChangeApplier) logPipelineEngagedOnce(ctx context.Context, depth int) {
	if a.pipelineWarnedEngaged {
		return
	}
	a.pipelineWarnedEngaged = true
	slog.InfoContext(ctx,
		"mysql: applier: pipelined CDC apply engaged — overlapping commit RTTs across an in-flight "+
			"window of W ordered transactions on a dedicated pool (ADR-0104 Phase 1; commits stay "+
			"strictly in source order)",
		slog.Int("depth_W", depth),
		slog.Int("dedicated_backends", depth))
}
