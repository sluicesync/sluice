// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package laneapply is the engine-neutral concurrent key-hash CDC apply core.
//
// # Concurrent key-hashed CDC apply (ADR-0104, item 23(c); shared core, ADR-0105)
//
// This package is the ENGINE-NEUTRAL correctness core of the concurrent
// key-hash CDC apply, extracted verbatim-in-behavior from the GA MySQL
// implementation (internal/engines/mysql) so a second engine (Postgres,
// ADR-0105) can reuse it without a second copy of the exactly-once
// landmark. The engine-specific decode / dispatch / position-write /
// error-classification live behind the [LaneApplier] seam; everything in
// this package is database-free except via that seam.
//
// Phase 1 (in-order pipelined COMMIT) was live-proven INEFFECTIVE on the
// cross-region Track-B link: it overlaps only the commit RTT while the
// per-batch data execs stay serial on the single producer goroutine, so
// depth=8 ≈ depth=1 (7/8 backends idle, ~21 rows/s < ~44 rows/s source).
// The throughput lever is concurrent DISPATCH — but naive concurrent
// dispatch of consecutive batches is a SILENT-LOSS hazard: an INSERT in
// batch i and a DELETE/UPDATE of the same key in batch i+1, dispatched on
// two transactions at once, let the later op exec against a snapshot
// without the uncommitted INSERT (0 rows affected → the row that should
// have been deleted survives), and in-order COMMIT does not save it
// because the damage is at exec time. So concurrent dispatch is only safe
// when same-key operations are guaranteed onto a single in-order lane.
//
// This package is the correctness core of that safe partitioning: the
// **key-hash router** (every change for a given primary key lands on the
// same lane, dispatched in source order there) and the **contiguous
// checkpoint frontier** (the resume position advances only to a source
// transaction boundary all of whose changes are durable across every
// lane). Both are pure, lock-disciplined, and unit-tested independently of
// any database or goroutine wiring; the lane orchestration that consumes
// them is layered on top (and carries the -race integration gate before
// any tag — concurrency chunk).
//
// Why key-hash and not per-shard: the source shard is not on ir.Change
// (the engine-neutral IR carries no Vitess concept, and the merged
// VStream position is a []shardGtid snapshot, not an originating shard),
// and a key-hash lane gives the identical same-key-closed guarantee a
// shard would — same key → same hash → same lane — while generalizing to
// an unsharded source (where per-shard degenerates to one lane). See
// ADR-0104 "Plumbing constraint discovered during Phase 2 design".
//
// ## The position relaxation (deliberate, safe, documented)
//
// The serial/Phase-1 path writes the position INSIDE each batch's own
// transaction (ADR-0007: position + data atomic). Key-hash lanes commit
// independently, so no single lane's transaction owns the merged position.
// Instead the checkpoint coordinator persists the merged position in a
// SEPARATE transaction (via [LaneApplier.WriteCheckpoint]), and ONLY up to
// a source-transaction boundary whose every change is durably committed
// across all lanes (the contiguous frontier). This RELAXES ADR-0007's
// per-batch atomicity to the weaker — but still exactly-once-preserving —
// invariant:
//
//	persisted_position ≤ all-durably-committed-data, always.
//
// The position can lag the data (a crash between a lane's data commit and
// the next checkpoint loses only the checkpoint, not the data) but can
// NEVER lead it (the frontier never passes an uncommitted change). On
// resume, re-streaming from the persisted (lagging) position re-delivers
// every change after it; keyed tables re-apply idempotently (ADR-0010
// UPSERT) → exactly-once across crash+resume; keyless tables keep their
// at-least-once guarantee (ADR-0089/Bug-143), unchanged. The cost is a
// larger crash-replay window (bounded by the checkpoint interval), the
// same trade ADR-0089 already accepted for larger batches — never a
// silent-loss or skip.
//
// Source-transaction cohesion (ADR-0027) is also relaxed on this path: a
// single source transaction's rows scatter across lanes and commit in
// separate target transactions, so a mid-recovery observer can see a
// partially-applied source transaction. The FINAL state is correct
// (resume re-applies the whole transaction idempotently, because the
// frontier only checkpoints at a fully-committed tx boundary), which is
// the guarantee a migration/continuous-sync tool makes — the target is
// not read-consistent mid-stream regardless.
package laneapply

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// LaneApplier is the minimal per-engine surface the shared concurrent
// key-hash [Orchestrator] drives. One method family applies a batch on a
// dedicated backend; the rest is PK routing, error classification, the
// barrier-path apply, and the position checkpoint write. The orchestrator
// owns the router, frontier, lane scheduling, recursive shrink-and-retry,
// the lane-local read cap, and all concurrency; the engine owns the
// database contact and value encoding.
type LaneApplier interface {
	// PKValuesForRouting returns the source-qualified table name and the
	// ordered primary-key values of a row change for lane hashing.
	//
	// ok=false routes the change to the BARRIER path (drain all lanes, apply
	// single-row in global order). The contract: ok=false covers EVERY case
	// the GA MySQL routeRow fell to barrier for —
	//
	//   - a non-row event (TxBegin/TxCommit/Truncate/SchemaSnapshot reach the
	//     orchestrator's own dispatch, not this method, but a defensively
	//     non-routable change still degrades safely),
	//   - a keyless table (no primary key — ADR-0089 at-least-once guard),
	//   - a malformed change (a key column absent from the row image),
	//   - a PK-CHANGING update (After's key differs from Before's — a key
	//     migration whose old/new effects could land on different lanes and
	//     must stay globally ordered).
	//
	// All four are barriered identically, preserving the GA behavior exactly.
	// err is for a genuine engine error (e.g. a PK-metadata lookup failure),
	// already classified, and aborts the run.
	PKValuesForRouting(ctx context.Context, c ir.Change) (qualified string, pkVals []any, ok bool, err error)

	// ApplyLaneBatch applies the (sub-)batch on lane `lane`'s dedicated
	// backend in one transaction (idempotent UPSERT per ADR-0010) and
	// commits, returning the number of rows durably committed (len(batch) on
	// success, 0 on error). The orchestrator handles the recursive
	// split-on-retriable-error and the frontier advance; this method applies
	// one (sub-)batch atomically and returns the RAW (unclassified) error so
	// the orchestrator's retriable/split decision inspects the original.
	ApplyLaneBatch(ctx context.Context, lane int, batch []ir.Change) (committed int, err error)

	// ClassifyError maps a raw driver error to a classified error exposing
	// the [ir.RetriableError] surface (Retriable() → split-and-retry vs
	// fatal). MySQL: tx-killer (1105) + lock-wait. Postgres: serialization
	// (40001) + deadlock (40P01). The orchestrator derives retriability
	// solely from this (errors.As on the classified value), so the in-lane
	// retry semantics stay byte-identical to the engine's streamer-side
	// classification.
	ClassifyError(error) error

	// WriteCheckpoint persists the merged position at a durable frontier
	// boundary in its own transaction (the ADR-0007 relaxation above). The
	// orchestrator owns the frontier read + the seq-monotone guard; this
	// method does only the durable write and returns an already-classified
	// error.
	WriteCheckpoint(ctx context.Context, pos ir.Position) error

	// ApplyBarrierChange applies one barrier-path change on the coordinator
	// backend (writing its position + data atomically per ADR-0007), and —
	// for a schema change — performs any engine-side metadata-cache
	// invalidation needed AFTER the apply commits. The orchestrator
	// additionally calls [LaneApplier.InvalidateMetadataCaches] on a
	// SchemaSnapshot for the same effect; engines may implement the
	// invalidation in either place (MySQL does it here, inside applyOne's
	// after-commit hook, AND exposes the explicit invalidator).
	ApplyBarrierChange(ctx context.Context, c ir.Change) error

	// InvalidateMetadataCaches drops the engine's PK + column-type cache for
	// the table named by a SchemaSnapshot, so lanes re-probe on the next
	// change for that table. Called by the orchestrator on a SchemaSnapshot
	// barrier AFTER ApplyBarrierChange returns (preserving the GA
	// apply-then-invalidate order). schema/table are the snapshot's RAW
	// source schema+table; the engine applies its own routing/qualification.
	InvalidateMetadataCaches(schema, table string)
}

// retriable reports whether the raw lane error is one the ADR-0038 streamer
// retry loop would treat as transient, derived SOLELY from the engine's
// [LaneApplier.ClassifyError] (its single source of truth) — classify the
// raw error, then check the [ir.RetriableError] surface the streamer's
// classifyRetriable inspects. A Vitess tx-killer abort (MySQL) or a
// serialization/deadlock abort (Postgres) is retriable here, which is what
// makes the in-lane shrink-and-retry converge instead of dropping the run.
func retriable(la LaneApplier, err error) bool {
	var re ir.RetriableError
	return errors.As(la.ClassifyError(err), &re) && re.Retriable()
}

// checkpointEveryChanges is how many routed row-changes the coordinator
// processes between persisted-position checkpoints on a barrier-free run.
// Smaller = shorter crash-replay window; larger = fewer position-write
// round trips. Barriers (TxCommit-driven boundaries) also trigger a
// checkpoint, so on a transactional stream the real cadence is finer.
const checkpointEveryChanges = 2000

// retrySameBeforeSplit is how many times a MULTI-change lane batch is
// re-applied at its current size before the in-lane recovery re-chunks
// (splits it in half). A TRANSIENT tx-killer — a momentary target overload —
// usually clears within a retry or two, so retrying the same batch avoids the
// cost of splitting a batch that would have committed anyway. Must be ≥ 2 so
// a tx-killer that recovers on the second attempt is caught without splitting
// (the TxKillerShrinkAndRetry pin). Once these attempts are exhausted the
// failure is treated as PERSISTENT (the batch is too large to commit under
// the target's tx-killer timeout) and the batch is split — see applyLaneBatch.
const retrySameBeforeSplit = 2

// laneReadCapGrowth is the factor applied to a lane's largest just-committed
// (sub-)batch size to bound its NEXT read (see laneApplyLoop's readCap). After
// a tx-killer storm splits a batch down to a committable size S, the next read
// is capped at S×laneReadCapGrowth — so the lane climbs back toward the
// controller's size gradually (doubling per success) rather than immediately
// re-reading an over-large ceiling and re-triggering the killer. >1 so the cap
// always allows growth; on the happy path (whole batches commit) the cap
// exceeds the controller's size and never binds.
const laneReadCapGrowth = 2

// maxInLaneRetries bounds the SINGLE-change retry loop (the recursion's base
// case in applyLaneBatch): a lone change is re-applied idempotently (ADR-0010)
// up to this many times. A transient single-row tx-killer recovers within the
// budget; a target that tx-kills even a single row exhausts it and FAILS THE
// RUN LOUDLY (→ ctx cancel → the streamer's warm-resume re-streams from the
// last durable boundary) rather than spinning forever. Multi-change batches
// converge by SPLITTING (retrySameBeforeSplit → halve), not by this cap.
const maxInLaneRetries = 10

// LaneChange is the {seq, change} envelope the coordinator pushes onto a
// lane's feed. Pairing the source sequence with its change on one channel
// is the FIFO-alignment fix: the lane reads the seq and the change
// together, so the frontier advance can never drift out of step with the
// change it accounts for. Exported only so engines/tests in this package
// can construct it; production callers never see it (the orchestrator owns
// the envelope).
type LaneChange struct {
	Seq    uint64
	Change ir.Change
}

// Config configures an [Orchestrator]. Zero values are safe: Lanes < 1 is
// clamped to serial, MaxBatchSize < 1 to 1, and a zero MaxBufferBytes /
// IdleFlushPeriod falls back to the package defaults. There are no
// default-on bools (the v0.99.51 zero-value trap): the only behavioral
// switch is the lane count itself.
type Config struct {
	// Lanes is the lane count W (--apply-concurrency). < 1 clamps to 1.
	Lanes int

	// MaxBatchSize is the static per-lane read size used when a lane has no
	// AIMD controller (or as the initial size before the controller sizes a
	// read). < 1 clamps to 1.
	MaxBatchSize int

	// LaneControllers are the per-lane AIMD controllers (one per lane, in
	// lane-index order). A nil slice (or a nil element) makes that lane run
	// at the static MaxBatchSize with bounded in-lane retry but no adaptive
	// sizing. Each lane drives its own controller from its single goroutine,
	// so a tx-killer shrink stays local to the affected lane.
	LaneControllers []ir.BatchSizeController

	// MaxBufferBytes is the soft per-lane-batch byte cap (ADR-0028). 0 falls
	// back to defaultMaxBufferBytes. Pass the engine's resolved cap to keep
	// behavior identical to the serial path.
	MaxBufferBytes int64

	// IdleFlushPeriod is the partial-batch idle-flush grace (item 18 Fix B)
	// so a quiet lane still drains within the grace. 0 falls back to
	// defaultIdleFlushPeriod.
	IdleFlushPeriod time.Duration
}

// defaultMaxBufferBytes is the fallback soft per-batch byte cap when the
// Config leaves MaxBufferBytes zero. Matches the engines' 64 MiB default
// (kept here so the orchestrator is self-contained; engines pass their own
// resolved value through Config to stay byte-identical to their serial
// path).
const defaultMaxBufferBytes int64 = 64 << 20 // 64 MiB

// defaultIdleFlushPeriod is the fallback idle-flush grace when the Config
// leaves IdleFlushPeriod zero. Matches appliershared.DefaultIdleFlushPeriod;
// engines pass their own value through Config.
const defaultIdleFlushPeriod = 100 * time.Millisecond

// Orchestrator is the ADR-0104/ADR-0105 key-hash concurrent apply
// coordinator. The single coordinator goroutine (the one running [Run])
// reads the merged change stream in source order, assigns each event a
// monotonic sequence, and either routes a keyed row-change to its key-hash
// lane or handles a barrier event (Tx*, Truncate, SchemaSnapshot, keyless,
// PK-changing update) by draining all lanes first. W lane goroutines each
// apply their routed changes in-order on a dedicated backend (no position
// write) and report committed sequences to the [Frontier]; the coordinator
// persists the resume position only up to a fully-durable source-tx
// boundary, via the [LaneApplier] seam.
type Orchestrator struct {
	la           LaneApplier
	maxBatchSize int
	lanes        int
	byteCap      int64
	idlePeriod   time.Duration

	router   *Router
	frontier *Frontier

	// laneIn is the per-lane change feed (coordinator → lane). Each element
	// is a {seq, change} envelope so a lane reads the sequence and its change
	// inherently paired — there is no separate seq channel to keep
	// FIFO-aligned.
	laneIn []chan LaneChange

	// laneControllers are the per-lane AIMD controllers (one per lane, in
	// lane-index order). A nil slice (or a nil element) makes that lane run
	// at the static maxBatchSize with bounded in-lane retry but no adaptive
	// sizing. Each lane drives its own controller from its single goroutine.
	laneControllers []ir.BatchSizeController

	nextSeq         uint64 // coordinator-owned monotonic sequence
	sinceCheckpoint int    // routed changes since the last checkpoint
	lastWrittenSeq  uint64 // seq of the last persisted boundary (monotone guard)
	lastWrittenTok  string // token of the last persisted boundary (skip redundant)

	// prevSeq / prevPos track the most-recent event for position-change
	// boundary detection: a checkpoint boundary is the highest seq sharing a
	// given source position, detected when the NEXT event carries a
	// different position. This generalizes TxCommit boundaries to any stream
	// (incl. boundary-less streams where every change has a distinct
	// position), and guarantees the persisted position never names a
	// position that an uncommitted later change also carries. prevSeq == 0
	// means "no prior event".
	prevSeq uint64
	prevPos ir.Position

	cancel context.CancelFunc

	wg       sync.WaitGroup
	errMu    sync.Mutex
	firstErr error
}

// NewOrchestrator builds an [Orchestrator] over the [LaneApplier] seam. The
// engine owns the dedicated lane backend pool (the lane count must match
// cfg.Lanes); this constructor sets up the router, frontier, per-lane
// channels, and the AIMD controllers. Call [Orchestrator.Run] exactly once.
func NewOrchestrator(cfg Config, la LaneApplier) *Orchestrator {
	lanes := cfg.Lanes
	if lanes < 1 {
		lanes = 1
	}
	maxBatchSize := cfg.MaxBatchSize
	if maxBatchSize < 1 {
		maxBatchSize = 1
	}
	byteCap := cfg.MaxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}
	idlePeriod := cfg.IdleFlushPeriod
	if idlePeriod <= 0 {
		idlePeriod = defaultIdleFlushPeriod
	}
	o := &Orchestrator{
		la:              la,
		maxBatchSize:    maxBatchSize,
		lanes:           lanes,
		byteCap:         byteCap,
		idlePeriod:      idlePeriod,
		router:          NewRouter(lanes),
		frontier:        NewFrontier(),
		laneIn:          make([]chan LaneChange, lanes),
		laneControllers: cfg.LaneControllers,
	}
	// Buffer each lane a batch's worth so the coordinator's routing isn't
	// gated on a lane's per-change commit latency (the whole point — lanes
	// overlap their cross-region commit RTTs).
	buf := maxBatchSize
	if buf < 1 {
		buf = 1
	}
	for i := range o.laneIn {
		o.laneIn[i] = make(chan LaneChange, buf)
	}
	return o
}

// Run reads the merged change stream in source order and drives the
// concurrent key-hash apply to completion (channel close) or first error.
// On any lane or coordinator error the whole run stops (ctx cancel + drain)
// and the error is returned; the persisted position reflects only
// fully-durable work, so warm-resume re-streams + idempotently re-applies
// the remainder.
func (o *Orchestrator) Run(ctx context.Context, changes <-chan ir.Change) error {
	ctx, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	defer cancel()

	o.wg.Add(o.lanes)
	for i := 0; i < o.lanes; i++ {
		go o.laneApplyLoop(ctx, i)
	}
	slog.InfoContext(ctx,
		"laneapply: concurrent key-hash CDC apply engaged — routing row changes to W in-order "+
			"lanes by primary-key hash, committing each lane concurrently on a dedicated pool; the "+
			"resume position advances only to a source-tx boundary durable across all lanes (ADR-0104)",
		slog.Int("lanes_W", o.lanes),
		slog.Int("dedicated_backends", o.lanes))

	var loopErr error
	for c := range changes {
		if err := o.getErr(); err != nil {
			loopErr = err
			break
		}
		o.nextSeq++
		if err := o.handle(ctx, o.nextSeq, c); err != nil {
			loopErr = err
			break
		}
	}

	if loopErr != nil {
		// Abort: unblock any lane stuck on a slow commit and any pending
		// barrier drain, then collect.
		cancel()
	}
	for _, ch := range o.laneIn {
		close(ch)
	}
	o.wg.Wait()
	if e := o.getErr(); e != nil && loopErr == nil {
		loopErr = e
	}
	if loopErr != nil {
		return loopErr
	}
	// Clean end-of-stream: the final position run never saw a differing
	// successor, so record its boundary now, then persist the final
	// fully-durable checkpoint (frontier is at its max after wg.Wait).
	if o.prevSeq != 0 {
		o.frontier.RecordTxBoundary(o.prevSeq, o.prevPos)
	}
	return o.writeCheckpoint(ctx)
}

// handle dispatches one source event by kind: boundary markers advance the
// frontier directly, keyed row-changes route to a lane, everything else
// (Truncate / SchemaSnapshot / keyless / PK-changing update) takes the
// barrier path.
func (o *Orchestrator) handle(ctx context.Context, seq uint64, c ir.Change) error {
	// Position-change boundary detection (see prevSeq/prevPos): when this
	// event's position differs from the previous event's, the previous
	// event was the last of its position run — a safe checkpoint boundary.
	o.noteBoundary(seq, c.Pos())

	switch c.(type) {
	case ir.TxBegin:
		// Boundary marker, no lane work — mark committed so the contiguous
		// frontier can advance past it once the tx's rows (lower seqs) land.
		o.frontier.MarkCommitted(seq)
		return nil
	case ir.TxCommit:
		o.frontier.MarkCommitted(seq)
		return o.maybeCheckpoint(ctx)
	case ir.Insert, ir.Update, ir.Delete:
		return o.routeRow(ctx, seq, c)
	default:
		// Truncate, SchemaSnapshot, or any future barrier-class event.
		return o.barrier(ctx, seq, c)
	}
}

// noteBoundary records the previous event as a checkpoint boundary when the
// current event's position differs from it, then advances the prev cursor.
// Coordinator-goroutine-only (no lock on prev* needed). A boundary at seq S
// means: once the frontier reaches S, the position at S is a safe resume
// point (no later change carries that same position). Recording happens
// when the position CHANGES, so the highest seq of each position run is the
// boundary — exactly the safe point.
func (o *Orchestrator) noteBoundary(seq uint64, pos ir.Position) {
	if o.prevSeq != 0 && pos.Token != o.prevPos.Token {
		o.frontier.RecordTxBoundary(o.prevSeq, o.prevPos)
	}
	o.prevSeq = seq
	o.prevPos = pos
}

// routeRow routes a keyed row-change to its key-hash lane, or falls to the
// barrier path when the change is keyless, malformed, or a PK-changing
// update (where the old and new keys could land on different lanes and the
// old/new ordering must be preserved globally). All of those distinctions
// are made by the engine's [LaneApplier.PKValuesForRouting] returning
// ok=false (see its contract); the orchestrator only hashes + pushes.
func (o *Orchestrator) routeRow(ctx context.Context, seq uint64, c ir.Change) error {
	qualified, vals, ok, err := o.la.PKValuesForRouting(ctx, c)
	if err != nil {
		return err
	}
	if !ok {
		// Keyless / malformed / PK-changing update → single-row barrier
		// (preserves the ADR-0089 keyless at-least-once bound; never silently
		// mis-routed).
		return o.barrier(ctx, seq, c)
	}
	lane := o.router.LaneFor(qualified, vals)
	// Push the {seq, change} envelope so the lane reads the sequence and its
	// change inherently paired (the FIFO-alignment fix — no sibling seq
	// channel to drift out of step). The select honours ctx cancel so a
	// stalled lane during shutdown doesn't wedge the coordinator.
	select {
	case o.laneIn[lane] <- LaneChange{Seq: seq, Change: c}:
	case <-ctx.Done():
		return ctx.Err()
	}
	o.sinceCheckpoint++
	return o.maybeCheckpoint(ctx)
}

// barrier applies a globally-ordered event (Truncate / SchemaSnapshot /
// keyless or PK-changing row change) after draining EVERY lane to the
// barrier's predecessor, so it lands in correct order relative to the row
// changes around it. It first persists a checkpoint (so the barrier's own
// position write, via ApplyBarrierChange, is monotone), then applies the
// change on the coordinator backend (which writes the barrier's position +
// data atomically — ADR-0007), then advances the frontier past it and
// records it as a tx boundary so subsequent checkpoints stay monotone from
// here.
func (o *Orchestrator) barrier(ctx context.Context, seq uint64, c ir.Change) error {
	if err := o.frontier.WaitForFrontier(ctx, seq-1); err != nil {
		return err
	}
	if err := o.writeCheckpoint(ctx); err != nil {
		return err
	}
	if err := o.la.ApplyBarrierChange(ctx, c); err != nil {
		return err
	}
	// SchemaSnapshot changed the table shape — drop the metadata caches so
	// lanes re-probe on the next change for that table. The apply (above)
	// has already committed; this preserves the GA apply-then-invalidate
	// order.
	if snap, ok := c.(ir.SchemaSnapshot); ok {
		o.la.InvalidateMetadataCaches(snap.Schema, snap.Table)
	}
	// ApplyBarrierChange persisted the barrier's own position + data
	// atomically and it is now the highest durable point. Mark it on the
	// frontier and advance the persisted-checkpoint cursor to it so a later
	// checkpoint can never regress below the barrier (the seq-monotone guard).
	o.frontier.MarkCommitted(seq)
	o.lastWrittenSeq = seq
	o.lastWrittenTok = c.Pos().Token
	o.sinceCheckpoint = 0
	return nil
}

// laneApplyLoop runs one lane (ADR-0104 graduation): it reads a batch of
// the lane's routed {seq, change} envelopes, applies them on the lane's
// dedicated backend in one target transaction (via the seam), and — ONLY
// after that transaction durably commits — advances the contiguous frontier
// past each committed seq. It owns per-lane AIMD sizing (its own
// controller) AND the in-lane shrink-and-retry that graduates
// --apply-concurrency out of preview: a retriable commit failure (a Vitess
// tx-killer, a PG serialization abort, a transient) re-applies the SAME
// buffered batch idempotently (ADR-0010) at the controller's freshly-shrunk
// size, so a loaded cross-region target converges in-lane instead of
// dropping the whole run on the first abort.
//
// A lane writes NO position — the coordinator's seq-frontier owns the
// merged resume point (the ADR-0104 position relaxation). The lane never
// sees keyless / schema / Tx-boundary events (the coordinator's
// routing/barrier handles those), so this loop is deliberately lean.
//
// ## Exactly-once invariants (do not reorder — the review focus)
//
//   - the frontier advance for a seq fires ONLY after the lane's target
//     transaction durably commits. A retriable retry re-applies the SAME buf
//     and does NOT advance any seq until a commit succeeds, so the frontier
//     — and thus the persisted resume position — only ever passes durable
//     work.
//   - Value encoding is byte-identical to the serial path: the seam's
//     ApplyLaneBatch redacts + shard-stamps each change in the SAME order
//     RunOneBatch uses then dispatches via the SAME dispatch. In-lane retry
//     changes only WHETHER/WHEN a batch is re-applied, never HOW a value is
//     encoded.
//   - A genuinely un-committable batch (the target tx-kills even at the
//     controller floor of 1) fails the run LOUDLY after maxInLaneRetries —
//     ctx cancel → warm-resume — rather than looping forever.
func (o *Orchestrator) laneApplyLoop(ctx context.Context, i int) {
	defer o.wg.Done()
	var ctrl ir.BatchSizeController
	if i < len(o.laneControllers) {
		ctrl = o.laneControllers[i]
	}
	// readCap is a lane-local learned bound on the read size, derived from the
	// largest batch this lane has recently COMMITTED. It exists for the
	// over-large-ceiling case: the AIMD controller shrinks only one
	// multiplicative-decrease per tx-killer (each costing a full target
	// tx-killer timeout), so from an absurd ceiling it takes many timeouts to
	// reach a committable size — but applyLaneBatch's re-chunk discovers that
	// size in-memory in one storm. Capping the next read at the just-committed
	// size (× a gentle growth factor) lets the lane SNAP to the committable
	// band after one storm instead of waiting out the controller's slow
	// descent. 0 = no cap yet (first read). It is happy-path-neutral: when
	// batches commit whole, readCap = len(buf)×growth ≥ the controller's size,
	// so min(NextBatchSize, readCap) == NextBatchSize and the cap never binds.
	readCap := 0
	for {
		size := o.maxBatchSize
		if ctrl != nil {
			size = ctrl.NextBatchSize()
		}
		if readCap > 0 && readCap < size {
			size = readCap
		}
		buf, closed, err := o.readLaneBatch(ctx, i, size)
		if err != nil {
			// Only ctx cancellation reaches here (the read has no other
			// failure mode); the coordinator already owns the run error.
			return
		}
		if len(buf) == 0 {
			if closed {
				return
			}
			continue
		}
		committed, err := o.applyLaneBatch(ctx, i, ctrl, buf)
		if err != nil {
			o.recordErr(err)
			o.cancel()
			return
		}
		// Learn the committable size: cap the next read at the largest
		// just-committed (sub-)batch grown by laneReadCapGrowth, so the read
		// tracks the proven-committable band and can climb back gradually.
		if committed > 0 {
			readCap = committed * laneReadCapGrowth
		}
		if closed {
			return
		}
	}
}

// readLaneBatch reads up to `size` {seq, change} envelopes from lane i's
// feed into a fresh slice, returning early when: the channel closes
// (closed=true; the caller drains+returns once buf is empty), the running
// ApproximateChangeBytes total reaches the byte cap (ADR-0028 — the same
// cap the serial path enforces), the idle-flush grace elapses with a
// partial buffer (so the frontier/position stays current on a quiet lane —
// item 18 Fix B), or ctx is cancelled. A non-nil error is ONLY ctx.Err();
// the read itself cannot otherwise fail.
func (o *Orchestrator) readLaneBatch(ctx context.Context, i, size int) (buf []LaneChange, closed bool, err error) {
	if size < 1 {
		size = 1
	}
	var batchBytes int64
	// The idle timer is created only after the first envelope lands, so a
	// quiet lane blocks indefinitely on the read (no spin) until work or
	// shutdown arrives, then flushes a partial batch within the grace.
	var idle *time.Timer
	defer func() {
		if idle != nil {
			idle.Stop()
		}
	}()
	for len(buf) < size {
		var idleC <-chan time.Time
		if idle != nil {
			idleC = idle.C
		}
		select {
		case lc, ok := <-o.laneIn[i]:
			if !ok {
				return buf, true, nil
			}
			buf = append(buf, lc)
			batchBytes += ir.ApproximateChangeBytes(lc.Change)
			if batchBytes >= o.byteCap {
				return buf, false, nil
			}
			if idle == nil {
				idle = time.NewTimer(o.idlePeriod)
			} else {
				o.resetLaneIdleTimer(idle)
			}
		case <-idleC:
			return buf, false, nil
		case <-ctx.Done():
			return buf, false, ctx.Err()
		}
	}
	return buf, false, nil
}

// applyLaneBatch applies one buffered batch on the lane's dedicated backend
// with in-lane shrink-and-retry, advancing the frontier past every
// committed seq ONLY after a durable commit. On a retriable failure of a
// MULTI-change batch it RE-CHUNKS — splits the batch in half and applies
// each half recursively — because a batch large enough to exceed the target's
// transaction-killer timeout can NEVER commit if re-applied whole; the
// controller's multiplicative-decrease only sizes the NEXT read, so the
// stuck batch must itself be broken down ("the shrink IS the split",
// matching serial #54). Halving guarantees convergence to committable
// sub-batches (and, in the limit, to a single change). A single change that
// still fails retriably uses a bounded retry — a transient single-row
// tx-killer recovers; persistent failure (a target that can't accept even
// one row) is fatal after the budget. The frontier advance fires per
// envelope ONLY after that envelope's sub-batch durably commits, in seq
// order across the splits, so exactly-once + same-lane ordering hold. See
// laneApplyLoop's invariant block.
//
// It returns the size of the LARGEST (sub-)batch that durably committed —
// the lane's "proven-committable size" — which laneApplyLoop uses to cap its
// next read (so an over-large ceiling snaps to the committable band after one
// storm instead of waiting out the controller's slow per-tx-killer descent).
// committed is 0 on any error path (the run is cancelling; the cap is moot).
func (o *Orchestrator) applyLaneBatch(ctx context.Context, lane int, ctrl ir.BatchSizeController, buf []LaneChange) (committed int, err error) {
	// Single change: bounded retry-in-place. A transient single-row tx-killer
	// recovers within the budget; persistent failure (the target cannot accept
	// even one row) is fatal — surface loudly so warm-resume / the operator can
	// act. There is nothing left to split, so this is the recursion's base case.
	if len(buf) == 1 {
		var rawErr error
		// attempt 0 is the initial try; 1..maxInLaneRetries are the retries.
		for attempt := 0; attempt <= maxInLaneRetries; attempt++ {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			rawErr = o.commitObserve(ctx, lane, ctrl, buf)
			if rawErr == nil {
				o.frontier.MarkCommitted(buf[0].Seq) // advance only on durable commit
				return 1, nil
			}
			if !retriable(o.la, rawErr) {
				return 0, o.la.ClassifyError(rawErr)
			}
		}
		return 0, o.la.ClassifyError(rawErr)
	}

	// Multi-change: retry the SAME batch a few times first — a TRANSIENT
	// tx-killer (a momentary target overload) recovers on retry-same without
	// the cost of splitting. Only when it PERSISTS do we re-chunk.
	var rawErr error
	for attempt := 1; attempt <= retrySameBeforeSplit; attempt++ {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		rawErr = o.commitObserve(ctx, lane, ctrl, buf)
		if rawErr == nil {
			for _, e := range buf { // advance only on durable commit
				o.frontier.MarkCommitted(e.Seq)
			}
			return len(buf), nil
		}
		if !retriable(o.la, rawErr) {
			return 0, o.la.ClassifyError(rawErr) // non-retriable → fatal
		}
	}

	// Persistent retriable failure on a multi-change batch ⇒ it is too large to
	// commit under the target's tx-killer timeout, and re-applying it whole can
	// NEVER converge (the controller's MD only sizes the NEXT read). RE-CHUNK:
	// split in half and apply each half recursively until the sub-batches are
	// small enough to commit ("the shrink IS the split", matching serial #54).
	// The first half commits + advances the frontier before the second is
	// attempted; a fatal second half leaves the first durable (warm-resume
	// re-applies the rest idempotently). The frontier advance therefore still
	// fires per envelope only on a durable commit, in seq order across the splits.
	mid := len(buf) / 2
	slog.WarnContext(ctx,
		"laneapply: concurrent lane batch persistently tx-killed — splitting to converge in-lane",
		slog.Int("rows", len(buf)),
		slog.Int("split_at", mid),
		slog.String("err", o.la.ClassifyError(rawErr).Error()))
	lc, e := o.applyLaneBatch(ctx, lane, ctrl, buf[:mid])
	if e != nil {
		return 0, e
	}
	rc, e := o.applyLaneBatch(ctx, lane, ctrl, buf[mid:])
	if e != nil {
		return 0, e
	}
	if rc > lc {
		return rc, nil
	}
	return lc, nil
}

// commitObserve runs one commit attempt of buf and feeds the per-lane AIMD
// controller the per-transaction latency + the ENGINE-CLASSIFIED error
// (only the classified wrapper carries the TransactionKilled() / Retriable()
// surfaces a raw driver error lacks — observing the classified error is what
// drives the tx-killer multiplicative decrease). Returns the RAW commit
// error so the caller's retriable/split decision inspects the original.
func (o *Orchestrator) commitObserve(ctx context.Context, lane int, ctrl ir.BatchSizeController, buf []LaneChange) error {
	start := time.Now()
	rawErr := o.applyOnce(ctx, lane, buf)
	if ctrl != nil {
		ctrl.ObserveBatch(ctx, time.Since(start), len(buf), o.la.ClassifyError(rawErr))
	}
	return rawErr
}

// applyOnce drives the seam's ApplyLaneBatch with the envelope buffer
// converted to the raw []ir.Change the engine dispatches. It returns the raw
// (unclassified) error so the caller's retry predicate inspects the original.
func (o *Orchestrator) applyOnce(ctx context.Context, lane int, buf []LaneChange) error {
	changes := make([]ir.Change, len(buf))
	for i, e := range buf {
		changes[i] = e.Change
	}
	_, err := o.la.ApplyLaneBatch(ctx, lane, changes)
	return err
}

// resetLaneIdleTimer re-arms the idle-flush timer using the
// stop-drain-reset idiom (same as appliershared.resetIdleTimer): a stale
// tick is drained so the reset arms a clean grace window rather than
// firing instantly on the next read.
func (o *Orchestrator) resetLaneIdleTimer(idle *time.Timer) {
	if !idle.Stop() {
		select {
		case <-idle.C:
		default:
		}
	}
	idle.Reset(o.idlePeriod)
}

// maybeCheckpoint persists a checkpoint once enough changes have been
// routed since the last one. Called on the coordinator goroutine only, so
// all position writes are serialized (no race on the cdc-state row).
func (o *Orchestrator) maybeCheckpoint(ctx context.Context) error {
	if o.sinceCheckpoint < checkpointEveryChanges {
		return nil
	}
	o.sinceCheckpoint = 0
	return o.writeCheckpoint(ctx)
}

// writeCheckpoint persists the highest source-tx-boundary position that is
// durable across all lanes (the contiguous frontier), via the seam's
// WriteCheckpoint (its own transaction on the coordinator's primary pool).
// It is a no-op when no new boundary is durable, or when the boundary equals
// the last one written (idempotent / monotone). This is the ADR-0104
// position relaxation: the persisted position lags the durable data but can
// never lead it.
func (o *Orchestrator) writeCheckpoint(ctx context.Context) error {
	pos, seq, ok := o.frontier.CheckpointPosition()
	// Seq-monotone guard: never write a boundary at or below the last one
	// persisted (prevents regression below a barrier's direct apply write,
	// and skips redundant re-writes of the same point).
	if !ok || seq <= o.lastWrittenSeq {
		return nil
	}
	if err := o.la.WriteCheckpoint(ctx, pos); err != nil {
		return err
	}
	o.lastWrittenSeq = seq
	o.lastWrittenTok = pos.Token
	return nil
}

func (o *Orchestrator) recordErr(err error) {
	o.errMu.Lock()
	defer o.errMu.Unlock()
	if o.firstErr == nil {
		o.firstErr = err
	}
}

func (o *Orchestrator) getErr() error {
	o.errMu.Lock()
	defer o.errMu.Unlock()
	return o.firstErr
}
