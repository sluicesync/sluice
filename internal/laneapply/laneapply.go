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
// **key-hash router** (every change sharing a [Route] lands on the same
// lane, dispatched in source order there) and the **contiguous checkpoint
// frontier** (the resume position advances only to a source transaction
// boundary all of whose changes are durable across every lane). Both are
// pure, lock-disciplined, and unit-tested independently of any database or
// goroutine wiring; the lane orchestration that consumes them is layered on
// top (and carries the -race integration gate before any tag — concurrency
// chunk).
//
// A primary key is not always a wide enough route. Two changes to DIFFERENT
// primary keys that collide on a table's SECONDARY UNIQUE index are
// dependent on each other in exactly the way above, and hashing the PK puts
// them on different lanes — silently losing a row on a MySQL-family target,
// where every lane INSERT's ON DUPLICATE KEY UPDATE fires on any unique
// index (roadmap item 131 / audit 2026-08-05 A-1). [RouteScope] is how the
// engine widens the route to the whole table for those, keeping cross-table
// concurrency but ordering the table against itself.
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
// across all lanes (the contiguous frontier). (With --exactly-once-lanes, a
// mark fence's position rides a lane's own transaction instead — a
// [FoldTicket] — but it is the same frontier-chosen boundary, durable before
// the fold began, so nothing below changes; ADR-0190 amendment D.) This
// RELAXES ADR-0007's per-batch atomicity to the weaker — but still
// exactly-once-preserving — invariant:
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
	"slices"
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
	// RouteForChange returns the lane [Route] for a row change: the
	// source-qualified table, the ordered primary-key values, and the
	// [RouteScope] the lane hash must use.
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
	//
	// The SCOPE is the item-131 half: an implementation may return
	// [RouteScopeKey] only where it has proven the target table carries no
	// non-PK uniqueness constraint, because a secondary UNIQUE index makes
	// changes to DIFFERENT primary keys dependent on each other. Everything
	// else — including "the metadata probe failed" — leaves the zero value
	// [RouteScopeTable] and gets whole-table ordering. The zero-value default
	// is what makes a new implementor safe before anyone reviews it.
	RouteForChange(ctx context.Context, c ir.Change) (route Route, ok bool, err error)

	// ApplyLaneBatch applies the (sub-)batch on lane `lane`'s dedicated
	// backend in one transaction (idempotent UPSERT per ADR-0010) and
	// commits, returning the number of rows durably committed (len(batch) on
	// success, 0 on error). The orchestrator handles the recursive
	// split-on-retriable-error and the frontier advance; this method applies
	// one (sub-)batch atomically and returns the RAW (unclassified) error so
	// the orchestrator's retriable/split decision inspects the original.
	//
	// fold, when non-nil, is ADR-0190 amendment D's [FoldTicket]: the batch
	// carries the fenced transaction's first marked change, and the engine
	// writes, in THIS transaction and in this order, the data, the marks
	// (closing fold.ClosedTxs: its own upserts and their deletes, the plan
	// WriteCheckpoint would run), and then fold.Pos with fold.RowsApplied —
	// last. Once the commit lands, and before returning, it records the marks
	// durable and anchors fold.Tx on its lane fence, so a later batch on any
	// lane may write that transaction's marks. A COMMIT-step error of a fold
	// batch must be wrapped with [CommitOutcomeUnknown].
	ApplyLaneBatch(ctx context.Context, lane int, batch []ir.Change, fold *FoldTicket) (committed int, err error)

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
	//
	// rowsApplied is the number of row-level DML changes ([ir.Insert] /
	// [ir.Update] / [ir.Delete]) that became durable across all lanes
	// between the previously-persisted checkpoint boundary and this one — the
	// increment added to the control row's cumulative rows_applied IN THE
	// SAME checkpoint write, so the counter advances together with the
	// position (never counting a routed change until its boundary is durable
	// across every lane; see the orchestrator's boundaryRowDML tracking). 0
	// when the boundary advanced with no intervening DML (e.g. a Truncate
	// boundary).
	//
	// closedTxs names every source transaction (its [ir.ApplyID] TxID) whose
	// commit this checkpoint passes and that the orchestrator has not handed
	// to an earlier checkpoint — the ADR-0190 garbage collection: those
	// transactions are never re-delivered once this position is durable, so
	// the engine deletes their apply marks IN THE SAME transaction as the
	// position. Empty on a stream whose reader stamps no identity.
	WriteCheckpoint(ctx context.Context, pos ir.Position, rowsApplied int64, closedTxs []string) error

	// ApplyBarrierChange applies one barrier-path change on the coordinator
	// backend (writing its position + data atomically per ADR-0007), and —
	// for a SchemaSnapshot — performs ALL engine-side metadata-cache
	// invalidation needed after the apply commits, using the SAME guarded
	// apply-then-invalidate the serial path uses: invalidate ONLY on a real
	// signature-changing boundary, NEVER on a first-touch baseline /
	// identical re-send. Both engine implementations already do this inside
	// applyOne's after-commit hook (cacheActiveSchemaAfterCommit). The
	// orchestrator does NOT independently invalidate — a separate
	// unconditional invalidation bypassed the first-touch guard and forced
	// the PG lane DML onto the text-encode path, silently dropping
	// non-text-round-trippable values (Bug 158); deferring entirely to this
	// method keeps the concurrent path's invalidation byte-identical to
	// serial.
	ApplyBarrierChange(ctx context.Context, c ir.Change) error

	// SkipsRowChange reports whether a row-level DML change (Insert/Update/
	// Delete) would be DROPPED at apply time because its target table is
	// absent on the destination — the audit-C-11 errUnknownTable skip. The
	// orchestrator consults it at ROUTE time so a skipped change does not
	// advance rows_applied (PG-2): a skip writes zero rows, so counting it
	// inflates the counter with phantom progress (a bulk load into a missing
	// target would show rows_applied climbing with nothing applied). The true
	// skip count already lives durably in sluice_cdc_skipped_tables; this
	// keeps the rows_applied observability counter from double-telling it.
	//
	// It shares the SAME target-table metadata cache the lane/barrier dispatch
	// skip consults, so the route-time verdict and the later apply-time skip
	// agree in the steady state (the probe caches the verdict; dispatch reads
	// it back). The only disagreement window is a metadata-cache TTL expiry
	// that straddles one change's route→dispatch gap — sub-second, at most a
	// handful of in-flight changes, and it perturbs ONLY this observability
	// counter, never the resume position (the frontier owns that; see
	// boundaryRowDML). Implementations MUST return false for a non-row change
	// and for any probe error (fail toward counting; never abort the run from
	// here — a genuine metadata error surfaces loudly on the apply path).
	SkipsRowChange(ctx context.Context, c ir.Change) bool

	// ApplyMarkTx reports the source transaction (its [ir.ApplyID] TxID)
	// whose ADR-0190 apply mark a lane would write when it applies c, or ""
	// when c writes none (no identity, marks disabled, an idempotent class, or
	// a change the marks prove already applied). It is asked at ROUTE time and
	// has no side effect; an implementation that cannot tell (a metadata probe
	// failed) answers the TxID, because the fence it triggers costs only a
	// drain. See [Orchestrator.fenceApplyMarks].
	ApplyMarkTx(ctx context.Context, c ir.Change) string

	// ApplyMarksFenced clears the lanes to write txID's apply marks: the
	// orchestrator has drained every lane to the transaction's first marked
	// change (ADR-0190 amendment A). A lane must write NO mark of any other
	// transaction — it applies such a change unmarked instead.
	//
	// anchored reports whether the persisted position already sits at the
	// transaction's start. When it does not, the anchor rides a [FoldTicket]
	// on the batch carrying the transaction's first marked change (amendment
	// D), and until that batch commits only it may write the transaction's
	// marks — the "anchored" rule. A lane batch admitting them earlier would
	// make them durable while the position still sits at the previous
	// transaction's start: marks for two transactions at once.
	ApplyMarksFenced(txID string, anchored bool)
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

// checkpointIdlePeriod bounds how long the persisted resume position may
// lag the durable frontier on a LOW-VOLUME stream. The count-based
// checkpointEveryChanges cadence only fires every 2000 routed changes, so
// a sparse stream (e.g. a postgres-trigger source applying a handful of
// changes) would otherwise leave source_position frozen at the cold-start
// anchor until 2000 changes accrued — the data converges idempotently but
// the consumed watermark never persists, so the capture log is never
// reclaimable and every warm-resume re-reads from the start (Bug 159).
// A periodic coordinator-side checkpoint mirrors the serial path's item-18
// idle flush (appliershared.DefaultIdleFlushPeriod = 100ms), which already
// persists the position on every quiet-stream flush. 1s keeps the position
// current within a poll interval while staying negligible against the
// count-based cadence on a busy stream (where 2000 changes arrive well
// inside 1s and the count path fires first).
const checkpointIdlePeriod = 1 * time.Second

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

	// flush marks the coordinator's drain sentinel (see
	// [Orchestrator.drainLanes]): no change, no seq — it tells the lane to
	// commit the partial batch it holds now instead of waiting out the idle
	// grace. Never applied, never counted on the frontier.
	flush bool

	// fold is the [FoldTicket] the coordinator attached to a fenced
	// transaction's first marked change (ADR-0190 amendment D): whichever
	// (sub-)batch carries this envelope writes the fenced checkpoint in its
	// own transaction. nil on every other envelope.
	fold *FoldTicket
}

// Config configures an [Orchestrator]. Zero values are safe: Lanes < 1 is
// clamped to serial, MaxBatchSize < 1 to 1, and a zero MaxBufferBytes /
// IdleFlushPeriod falls back to the package defaults. There are no
// default-on bools (the v0.99.51 zero-value trap): ExactlyOnceLanes is
// opt-in, so its zero value is the default.
type Config struct {
	// ExactlyOnceLanes turns on ADR-0190's apply marks for the changes the
	// LANES apply (amendment C, operator 2026-09-29): the mark fence before
	// a transaction's first marked lane change, and the lanes' mark writes.
	// Off (the zero value, the default), the lanes write no marks and the
	// coordinator never fences — a lane change still CHECKS the marks the
	// barrier and serial paths wrote, and a crash mid-transaction can still
	// stop on the loud GC-38 (l) collision for a secondary-unique table, as
	// before ADR-0190. See [Orchestrator.fenceApplyMarks] for the cost it
	// buys back and the soundness argument for the partial coverage.
	ExactlyOnceLanes bool

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

	// LookAheadCap bounds how far the coordinator may run AHEAD of the durable
	// frontier: the router blocks before dispatching a change whose sequence is
	// more than LookAheadCap past the frontier. This caps Frontier.pending +
	// txBoundaries under the stalled-lane pathology (a hot table pinned to one
	// lane via RouteScopeTable stalls while other lanes keep committing —
	// per-lane channel backpressure does NOT bound the global pending set there).
	// 0 falls back to defaultFrontierLookAheadCap; it is a safety bound, not a
	// tuning knob — the default has ample headroom so a healthy stream never
	// blocks here (a small value is for tests).
	LookAheadCap int
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

// defaultFrontierLookAheadCap bounds how far the coordinator runs ahead of the
// durable frontier (see [Config.LookAheadCap]). ~1M in-flight sequences: a
// healthy stream keeps the frontier within lanes×buffer of the router, far
// under this, so it never throttles legitimate throughput — it only bites the
// stalled-lane pathology, converting an UNBOUNDED pending set (10M+ entries,
// hundreds of MB–GB over a long storm) into a bounded one (~tens of MB).
const defaultFrontierLookAheadCap = 1 << 20 // 1,048,576

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
	la               LaneApplier
	maxBatchSize     int
	lanes            int
	byteCap          int64
	idlePeriod       time.Duration
	lookAheadCap     uint64 // coordinator-side frontier look-ahead bound (Config.LookAheadCap)
	exactlyOnceLanes bool   // Config.ExactlyOnceLanes: fence and write lane marks

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

	// --- Cumulative rows-applied accounting (ADR-0156 phase 2) ---
	//
	// cumRowDML is the running count of row-level DML changes
	// (Insert/Update/Delete — routed to a lane OR barriered) the
	// coordinator has PROCESSED so far, keyed by source sequence. It counts
	// at ROUTE time (before the lane commits), but the counter is only ever
	// REALIZED at a checkpoint (writeCheckpoint), whose boundary seq is by
	// definition durable across every lane — so a delta derived from it
	// reflects only DML that is both committed AND covered by the persisted
	// position. Coordinator-goroutine-only (all of handle / noteBoundary /
	// flushPendingBoundary / writeCheckpoint run on the coordinator), so no
	// lock is needed — mirroring sinceCheckpoint / lastWrittenSeq.
	cumRowDML uint64

	// boundaryRowDML maps a recorded checkpoint-boundary seq → cumRowDML as
	// of that seq (the count of DML with source seq ≤ that boundary). Every
	// frontier.RecordTxBoundary call site records the matching cum here so
	// writeCheckpoint can look up the chosen boundary's cumulative count and
	// emit the delta since the last persisted boundary. Pruned as the
	// persisted checkpoint advances. Coordinator-goroutine-only.
	boundaryRowDML map[uint64]uint64

	// lastWrittenCum is cumRowDML as of the last persisted checkpoint
	// boundary; the WriteCheckpoint delta is boundaryRowDML[chosenSeq] −
	// lastWrittenCum. Coordinator-goroutine-only.
	lastWrittenCum uint64

	// prevSeq / prevPos drive the position-run boundary heuristic used ONLY on
	// marker-LESS streams (the trigger sources — see sawTxMarker): a checkpoint
	// boundary is the highest seq sharing a given source position, detected
	// when the NEXT event carries a different position. This is safe ONLY when
	// within-tx events share a position token or every event is its own
	// transaction (the trigger sources: one change-log id per change; VStream,
	// whose VGTID is stable within a source transaction, was marker-less too
	// until ADR-0190 phase 4); it must NOT be used for MySQL file/pos, where every binlog
	// event has a distinct LogPos so a mid-transaction ROW position would be
	// recorded as a "boundary" yet is unresumable ("no corresponding table map
	// event" on warm-resume — the bug this heuristic caused on the concurrent
	// path). prevSeq == 0 means "no prior event".
	prevSeq uint64
	prevPos ir.Position

	// prevCum is cumRowDML as of prevSeq — the cumulative row-DML count to
	// record for prevSeq when noteBoundary / flushPendingBoundary settle it
	// as a checkpoint boundary on a marker-LESS stream. Kept in lockstep
	// with prevSeq / prevPos. Coordinator-goroutine-only.
	prevCum uint64
	// prevTx is the ADR-0190 TxID of the change at prevSeq ("" without one).
	// On a marker-less stream each identity-carrying change is its own
	// transaction (the trigger sources), so the boundary that settles prevSeq
	// closes prevTx — the lane path's stand-in for a TxCommit. Kept in
	// lockstep with prevSeq. Coordinator-goroutine-only.
	prevTx string

	// lastNotedSeq is the highest prevSeq already handed to the frontier as a
	// boundary (by noteBoundary or the idle-checkpoint flush). It dedups the
	// idle flush against noteBoundary so the same (seq,pos) isn't recorded
	// twice: the idle tick records the trailing prevSeq/prevPos as a boundary
	// on a marker-LESS stream (the position-run heuristic otherwise records a
	// run's boundary only when a DIFFERENT-token successor arrives — which
	// never happens on a quiet stream, leaving the last change's watermark
	// unpersisted). Coordinator-goroutine-only (no lock). 0 = nothing noted.
	lastNotedSeq uint64

	// sawTxMarker latches true once an [ir.TxBegin] or [ir.TxCommit] is seen on
	// the stream. It selects the boundary-detection strategy (see handle):
	// marker streams (binlog-MySQL, Postgres) record a checkpoint boundary ONLY
	// at the real boundary events (TxCommit, and the DDL-boundary Truncate),
	// while marker-LESS streams (the trigger sources) fall back to the prevSeq/prevPos
	// position-run heuristic above. Coordinator-goroutine-only (no lock).
	sawTxMarker bool

	// curTx is the ADR-0190 TxID of the source transaction the coordinator
	// is inside (the identity its row changes carry), and closedTx maps each
	// TxCommit's seq to the transaction it closed. writeCheckpoint hands every
	// closed transaction at or below its boundary to the engine, whose apply
	// marks it then deletes with the position. Coordinator-goroutine-only.
	curTx    string
	closedTx map[uint64]string

	// fencedTx is the transaction the last ADR-0190 mark fence cleared the
	// lanes for (see fenceApplyMarks). foldSeq is the seq of the envelope
	// carrying its [FoldTicket] — 0 when it needed none, being anchored at the
	// fence — and foldLane the lane that envelope was routed to.
	// Coordinator-goroutine-only.
	fencedTx string
	foldSeq  uint64
	foldLane int

	// folds counts the fold tickets this run issued — the evidence, logged
	// when the run ends, that amendment D's path ran at all.
	// Coordinator-goroutine-only.
	folds int

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
	lookAheadCap := cfg.LookAheadCap
	if lookAheadCap <= 0 {
		lookAheadCap = defaultFrontierLookAheadCap
	}
	o := &Orchestrator{
		la:               la,
		maxBatchSize:     maxBatchSize,
		lanes:            lanes,
		byteCap:          byteCap,
		idlePeriod:       idlePeriod,
		lookAheadCap:     uint64(lookAheadCap),
		exactlyOnceLanes: cfg.ExactlyOnceLanes,
		router:           NewRouter(lanes),
		frontier:         NewFrontier(),
		laneIn:           make([]chan LaneChange, lanes),
		laneControllers:  cfg.LaneControllers,
		boundaryRowDML:   make(map[uint64]uint64),
		closedTx:         make(map[uint64]string),
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
	defer o.logFolds(ctx)

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

	// Idle-checkpoint ticker (Bug 159): on a LOW-VOLUME stream the count-based
	// checkpointEveryChanges cadence (every 2000 routed changes) rarely fires,
	// so without a time-based flush the persisted resume position lags the
	// durable frontier indefinitely (frozen at the cold-start anchor on a
	// sparse postgres-trigger source). The tick persists the current durable
	// frontier — mirroring the serial path's item-18 idle flush — so a quiet
	// stream's watermark stays current within ~checkpointIdlePeriod. It only
	// ever writes a fully-durable boundary (writeCheckpoint consults the
	// frontier), so it never advances the position ahead of committed data.
	ticker := time.NewTicker(checkpointIdlePeriod)
	defer ticker.Stop()

	var loopErr error
loop:
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				break loop
			}
			if err := o.getErr(); err != nil {
				loopErr = err
				break loop
			}
			o.nextSeq++
			if err := o.handle(ctx, o.nextSeq, c); err != nil {
				loopErr = err
				break loop
			}
		case <-ticker.C:
			if err := o.getErr(); err != nil {
				loopErr = err
				break loop
			}
			// Record the trailing marker-LESS boundary (no-op on a marker
			// stream or when already noted) so the last applied change's
			// watermark becomes checkpointable on a quiet stream, then persist
			// whatever the frontier has made durable.
			o.flushPendingBoundary()
			if err := o.writeCheckpoint(ctx); err != nil {
				loopErr = err
				break loop
			}
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
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
	// The RECORDED error is the authoritative run error. A failing lane (or the
	// coordinator) records the real error and THEN calls o.cancel(), so the
	// coordinator loop frequently observes that internal abort as loopErr ==
	// context.Canceled — and the dead lane stops draining, so the coordinator
	// also blocks routing same-key changes and unblocks only via this same
	// internal cancel. Preferring the recorded error over loopErr is therefore
	// load-bearing: without it a real target-side apply failure (e.g.
	// "connection refused") is masked as a clean context.Canceled, which the
	// streamer/supervisor read as a graceful drain and PARK the sync (stopped,
	// no restart, no last_error) on an uncommitted target outage. A genuine
	// OUTER cancel (operator stop) records no lane error, so getErr() is nil and
	// loopErr (ctx.Err) is returned unchanged. See the Run-level pins in
	// lane_apply_test.go.
	if e := o.getErr(); e != nil {
		loopErr = e
	}
	if loopErr != nil {
		return loopErr
	}
	// Clean end-of-stream. On a marker-LESS stream the final position run never
	// saw a differing successor, so record its boundary now (dedup-aware via
	// flushPendingBoundary so an idle tick that already recorded it doesn't
	// double-append). On a marker stream the boundary was already recorded at
	// each TxCommit / Truncate (noteBoundary is never used there, so prevSeq
	// stays 0); never record prevPos, which could be a mid-transaction row
	// position. Then persist the final fully-durable checkpoint (frontier is at
	// its max after wg.Wait).
	if !o.sawTxMarker {
		o.flushPendingBoundary()
	}
	return o.writeCheckpoint(ctx)
}

// handle dispatches one source event by kind: boundary markers advance the
// frontier directly, keyed row-changes route to a lane, everything else
// (Truncate / SchemaSnapshot / keyless / PK-changing update) takes the
// barrier path.
func (o *Orchestrator) handle(ctx context.Context, seq uint64, c ir.Change) error {
	// Checkpoint-boundary tracking. Which source positions are safe to resume
	// FROM depends on the stream's shape:
	//
	//   - MARKER streams — those emitting TxBegin/TxCommit (the binlog-MySQL
	//     and Postgres CDC readers) — record a checkpoint boundary ONLY at the
	//     real boundary events: TxCommit (the transaction boundary) and the
	//     DDL-boundary Truncate. This is LOAD-BEARING for MySQL file/pos: every
	//     binlog event has a distinct LogPos, so a mid-transaction ROW position
	//     points INTO a transaction (after its TABLE_MAP) and is NOT a valid
	//     warm-resume point — persisting one crash-loops the stream with "no
	//     corresponding table map event" (the concurrent-path counterpart of
	//     the serial item-29 / v0.99.89 fix; found live on a native-MySQL
	//     file/pos run). For Postgres the mid-tx LSN is independently resumable,
	//     but a TxCommit boundary is equally valid and the canonical restart
	//     point, so the same rule applies — and never persisting a mid-tx
	//     position is strictly safer.
	//
	//   - MARKER-LESS streams — the trigger sources — emit only row changes,
	//     each with its own distinct change-log id and no Tx* markers to anchor
	//     on. For these the prevSeq/prevPos position-run heuristic
	//     (noteBoundary) finds the boundary as the last change of each run —
	//     every change. (VStream was the other marker-less stream, its VGTID
	//     stable within a transaction, until ADR-0190 phase 4 made its reader
	//     emit each shard transaction's BEGIN/COMMIT.)
	//
	// sawTxMarker latches once a TxBegin/TxCommit is seen, selecting the marker
	// path. SchemaSnapshot is excluded from BOTH: its token is metadata-anchored
	// (pgoutput's first-touch RelationMessage carries WAL 0/0, before any real
	// anchor exists), NOT a resumable position; recording a (seq, 0/0) boundary
	// pinned the persisted position at 0/0 forever (Bug 158, the position half).
	switch c.(type) {
	case ir.TxBegin:
		o.sawTxMarker = true
		o.curTx = ""
		// Boundary marker, no lane work — mark committed so the contiguous
		// frontier can advance past it as soon as this seq (and all lower) are
		// committed. C-2 (Tier-3 audit): the tx's OWN rows carry HIGHER seqs and
		// land AFTER this TxBegin (nextSeq increments per event); the correct
		// "lower seqs are the tx's rows" reasoning belongs to TxCommit below,
		// where the rows precede the commit — this parenthetical had them
		// inverted.
		o.frontier.MarkCommitted(seq)
		return nil
	case ir.TxCommit:
		o.sawTxMarker = true
		// The source-transaction boundary: its position is the resume-safe
		// restart point. Record it — the frontier returns it as the checkpoint
		// once every lower seq (this tx's rows) is durably committed across all
		// lanes. cumRowDML here already counts every DML change in this tx (they
		// had lower seqs and were processed first); TxCommit itself is not DML.
		o.frontier.RecordTxBoundary(seq, c.Pos())
		o.boundaryRowDML[seq] = o.cumRowDML
		if o.curTx != "" {
			o.closedTx[seq] = o.curTx
			o.curTx = ""
		}
		o.frontier.MarkCommitted(seq)
		return o.maybeCheckpoint(ctx)
	case ir.Insert, ir.Update, ir.Delete:
		if id := ir.ApplyIDOf(c); !id.IsZero() {
			o.curTx = id.TxID
		}
		// Count the row-level DML change (routed OR — via routeRow's ok=false
		// fall-through — barriered; both are counted here exactly once, before
		// the lane/barrier applies it). The counter is only realized at a
		// durable checkpoint, so counting at route time can never over-count a
		// change the target didn't commit.
		//
		// PG-2: EXCEPT a change whose target table is absent — it dispatches to
		// recordSkippedTable, writes zero rows, and must NOT advance
		// rows_applied even though it is an Insert/Update/Delete. SkipsRowChange
		// consults the SAME metadata cache the lane/barrier apply-time skip
		// uses, so the route-time verdict matches what dispatch will do (a
		// bulk load into a missing target would otherwise climb rows_applied
		// with nothing written).
		if !o.la.SkipsRowChange(ctx, c) {
			o.cumRowDML++
		}
		// Marker-less (trigger sources) only: the position-run heuristic. On a marker
		// stream a row position is mid-transaction and must NOT be a boundary.
		if !o.sawTxMarker {
			o.noteBoundary(seq, c.Pos(), ir.ApplyIDOf(c).TxID)
		}
		return o.routeRow(ctx, seq, c)
	default:
		// Truncate, SchemaSnapshot, or any future barrier-class event. Not
		// row-level DML — cumRowDML is unchanged.
		if o.sawTxMarker {
			// A Truncate is a DDL statement boundary (auto-committed, not
			// wrapped in BEGIN/XID), so its position IS a resume-safe restart
			// point — record it. SchemaSnapshot is NOT (metadata-anchored).
			if _, isTrunc := c.(ir.Truncate); isTrunc {
				o.frontier.RecordTxBoundary(seq, c.Pos())
				o.boundaryRowDML[seq] = o.cumRowDML
			}
		} else if !isSchemaSnapshot(c) {
			o.noteBoundary(seq, c.Pos(), "")
		}
		return o.barrier(ctx, seq, c)
	}
}

// isSchemaSnapshot reports whether c is an [ir.SchemaSnapshot]. A
// SchemaSnapshot's position is metadata-anchored (pgoutput's first-touch
// RelationMessage carries WAL 0/0), NOT a resumable source position, so it is
// excluded from boundary/checkpoint tracking and never written as the resume
// position on the concurrent path (Bug 158, the position half).
func isSchemaSnapshot(c ir.Change) bool {
	_, ok := c.(ir.SchemaSnapshot)
	return ok
}

// noteBoundary records the previous event as a checkpoint boundary when the
// current event's position differs from it, then advances the prev cursor.
// Coordinator-goroutine-only (no lock on prev* needed). A boundary at seq S
// means: once the frontier reaches S, the position at S is a safe resume
// point (no later change carries that same position). Recording happens
// when the position CHANGES, so the highest seq of each position run is the
// boundary — exactly the safe point.
// TOKEN MONOTONICITY IS NOT CHECKED HERE, AND THAT IS A DECISION (2026-09-04).
// [Orchestrator.writeCheckpoint]'s guard is `seq <= lastWrittenSeq` — monotone
// in the orchestrator's own arrival counter, NOT in the position token. A
// reader that delivers changes out of order therefore advances seq while the
// TOKEN goes backwards, and the orchestrator persists the lower one. Bug 268
// is exactly that: the d1-trigger poll sorted lexicographically, the last
// change delivered carried `{"last_id":9}` after ids up to 42 had been
// applied, and that 9 is what landed in the control row.
//
// Why there is no assertion here, having costed it:
//
//   - A generic non-decreasing-token check needs ordering semantics for an
//     opaque string. The capability exists ([ir.PositionOrderer]) but the
//     engines that REACH this function do not implement it: sqlite-trigger
//     (and d1-trigger) and pgtrigger are marker-less and have no orderer,
//     while postgres and binlog-MySQL have one and take the marker path,
//     where noteBoundary is never called — as does VStream (which has one)
//     since ADR-0190 phase 4 made it a marker stream. A gate built
//     on the orderer would therefore miss the two engines that produced the
//     defect while reading as though it covered the marker-less path — the
//     coverage-narrower-than-its-name shape.
//   - The orderer is also SOURCE-side, and this orchestrator is constructed
//     inside the TARGET engine's applier, which holds no source-engine
//     handle. Plumbing one through is a cross-cutting change, not a guard.
//   - The cheap version that needs no ordering — refuse a token already
//     recorded — would NOT have caught Bug 268, whose 42 tokens were all
//     distinct and each seen once. A gate that misses its own motivating
//     defect is worse than none.
//
// So the check lives in the READER, which is the only component that knows
// its token semantics: sqlite-trigger's `CHANGE-LOG-PAGE-UNORDERED` refuses a
// non-ascending page before the pump can advance past it. Sibling checked at
// the same time and clean: pgtrigger's poll selects `id` unaliased in both the
// CTE and the outer query (`cdc_reader.go` pollQuery), so nothing shadows the
// bigint, and its watermark is the end of the contiguous committed run rather
// than the max id fetched — a strictly stronger contract than the one Bug 268
// broke.
//
// What would change this: an engine that reaches noteBoundary AND implements
// [ir.PositionOrderer] on the source side. Then the assertion is worth adding,
// and it would have real coverage.
func (o *Orchestrator) noteBoundary(seq uint64, pos ir.Position, txID string) {
	if o.prevSeq != 0 && pos.Token != o.prevPos.Token && o.prevSeq > o.lastNotedSeq {
		o.settlePrev()
	}
	o.prevSeq = seq
	o.prevPos = pos
	o.prevTx = txID
	// Snapshot the cum for THIS change; the caller has already incremented
	// cumRowDML for a DML change, so prevCum = count of DML with seq ≤ seq.
	o.prevCum = o.cumRowDML
}

// flushPendingBoundary records the trailing prevSeq/prevPos of a marker-LESS
// stream as a checkpoint boundary if it hasn't been recorded yet. The
// position-run heuristic in noteBoundary records a run's boundary only when a
// DIFFERENT-token successor arrives; on a quiet / low-volume stream no such
// successor comes, so the last applied change's watermark would never persist
// (Bug 159). The idle-checkpoint tick and clean end-of-stream call this to
// record that trailing boundary. The safety of recording a settled prevSeq as
// a resume point differs by marker-less source, and C-1 (Tier-3 audit) found
// this doc previously overstated it for VStream:
//
//   - pgtrigger: every row carries a DISTINCT, monotone position token (the
//     change-log id), so a settled prevSeq IS a resume-safe point — no
//     in-flight successor shares its token.
//   - VStream — a marker stream since ADR-0190 phase 4, so this no longer
//     applies to it; kept because the premise below is still pinned. The
//     VGTID is STABLE within a source transaction, so a settled prevSeq could sit MID-transaction on
//     a shared token. That is still safe ONLY because of a load-bearing wire-
//     ordering premise: the reader advances r.currentVgtid to the tx's VGTID
//     only on the TRAILING VGTID event (`mysql/cdc_vstream.go:1383`), so rows
//     carry the PRE-transaction VGTID and a resume from a mid-tx checkpoint
//     REPLAYS the partially-applied tx idempotently rather than skipping its
//     tail. This premise is now BOUND (audit backlog C-1) by
//     TestVStream_MidTxCheckpointReplaysNotSkips (internal/engines/mysql,
//     `//go:build integration && vstream`): against a real vttestserver it
//     asserts a 3-row tx's rows all carry one token DISTINCT from a later tx's
//     (so a row's position excludes its own tx), and that reopening the stream
//     FROM a mid-tx row position replays the whole tx rather than skipping it.
//     If a reader refactor ever stamped rows from the trailing VGTID — a crash
//     after an idle-tick checkpoint would then resume PAST a partly-applied tx
//     and silently drop its tail — that test fails. The concurrent path engages
//     by default on PlanetScale (ADR-0106), which is why it is worth the pin.
//
// Idempotent via lastNotedSeq. Coordinator-goroutine-only. No-op on a marker
// stream (prevSeq stays 0).
func (o *Orchestrator) flushPendingBoundary() {
	if o.prevSeq != 0 && o.prevSeq > o.lastNotedSeq {
		o.settlePrev()
	}
}

// settlePrev records prevSeq as a checkpoint boundary on a marker-less
// stream, and closes the ADR-0190 transaction of the change there (see
// prevTx): the checkpoint that persists this boundary deletes its marks.
func (o *Orchestrator) settlePrev() {
	o.frontier.RecordTxBoundary(o.prevSeq, o.prevPos)
	o.boundaryRowDML[o.prevSeq] = o.prevCum
	if o.prevTx != "" {
		o.closedTx[o.prevSeq] = o.prevTx
	}
	o.lastNotedSeq = o.prevSeq
}

// routeRow routes a keyed row-change to its lane, or falls to the barrier
// path when the change is keyless, malformed, or a PK-changing update (where
// the old and new keys could land on different lanes and the old/new ordering
// must be preserved globally). All of those distinctions are made by the
// engine's [LaneApplier.RouteForChange] returning ok=false (see its
// contract); the orchestrator only hashes + pushes. The returned [Route]'s
// [RouteScope] decides whether the hash covers the row or the whole table
// (item 131) — resolved in exactly one place, [Router.LaneForRoute].
func (o *Orchestrator) routeRow(ctx context.Context, seq uint64, c ir.Change) error {
	// Look-ahead cap (audit-2026-08-19): block before running more than
	// lookAheadCap ahead of the durable frontier, so Frontier.pending +
	// txBoundaries stay bounded when one lane stalls while others commit ahead
	// (per-lane channel backpressure alone does not bound the global pending
	// set — the RouteScopeTable hot-table-pinned-to-one-lane pathology). No
	// deadlock: the frontier advances from the LOWER-seq changes already routed
	// into the lanes, independent of this wait; a permanently-stuck lane wedges
	// the apply regardless. The barrier path's full drain (drainLanes to
	// seq-1) is a stricter special case of the same wait. This one
	// deliberately does NOT go through drainLanes: the cap fires because a lane
	// is BUSY (backpressure behind a hot table), not idle-grace-bound, and in
	// the row-28 hot-table case it recurs about once per committed batch — a
	// flush sentinel each time could cut the hot lane's batches below what its
	// AIMD controller would choose (unmeasured). So it keeps v0.156.4's plain
	// wait; it is the one named exemption in
	// TestOrchestrator_FrontierWaitsGoThroughDrainLanes.
	if seq > o.lookAheadCap {
		if err := o.frontier.WaitForFrontier(ctx, seq-o.lookAheadCap); err != nil {
			return err
		}
	}
	route, ok, err := o.la.RouteForChange(ctx, c)
	if err != nil {
		return err
	}
	if !ok {
		// Keyless / malformed / PK-changing update → single-row barrier
		// (preserves the ADR-0089 keyless at-least-once bound; never silently
		// mis-routed).
		return o.barrier(ctx, seq, c)
	}
	lane := o.router.LaneForRoute(route)
	fold, err := o.fenceApplyMarks(ctx, seq, c, lane)
	if err != nil {
		return err
	}
	// Push the {seq, change} envelope so the lane reads the sequence and its
	// change inherently paired (the FIFO-alignment fix — no sibling seq
	// channel to drift out of step). The select honours ctx cancel so a
	// stalled lane during shutdown doesn't wedge the coordinator.
	select {
	case o.laneIn[lane] <- LaneChange{Seq: seq, Change: c, fold: fold}:
	case <-ctx.Done():
		return ctx.Err()
	}
	o.sinceCheckpoint++
	return o.maybeCheckpoint(ctx)
}

// fenceApplyMarks is ADR-0190 amendment A (operator-approved 2026-09-28):
// before the FIRST change of a source transaction that will write an apply
// mark reaches a lane, drain every lane to that change's predecessor, move
// the persisted position to the transaction's start — the barrier's own
// prefix — and clear the lanes to write the transaction's marks. No EARLIER
// transaction can then ever be re-delivered alongside its marks; and the
// next transaction's fence drains this one and moves the position past it,
// deleting its marks in that same write. Marks therefore only ever exist for
// the first transaction after the persisted position, which is what makes the
// same-transaction skip rule sufficient.
//
// Without it a lane batch could commit T1's update of a key and T2's delete
// of it together, T2's mark on the key; a crash before the checkpoint passed
// T1 replays T1's update (a mark of another transaction proves nothing, so it
// re-applies and recreates the key) and then skips T2's delete on its own
// mark — the key left on the target at exit 0. applymarks.LaneFence carries
// the full counterexample.
//
// The cost is one drain per source transaction that sends a marked change to
// a lane — none for an idempotent-only transaction (the PK-only steady state)
// or a stream whose reader stamps no identity.
//
// # The position rides the marked batch (amendment D, 2026-10-01)
//
// Amendment A wrote that position in its own synchronous checkpoint, then the
// lane committed the marks: two target commits per marked transaction, which
// the 2026-09-29 benchmark measured at ~0.49x the serial path. The fence now
// CLAIMS the anchor instead ([Orchestrator.claimAnchor]) and hands it to the
// lane as a [FoldTicket] on the marked change's own envelope; the lane writes
// it last in the transaction that commits the marks, so "the marks name T"
// and "the position sits at T's start" become durable in ONE commit. Two
// conditions make that sound, and both are kept here:
//
//   - the drain stays: the anchor is a legal position only once every change
//     before the marked one is durable on every lane (without it the fold
//     would persist a lagging position under the marks — amendment D's V1);
//   - the anchored rule: until the fold commits, no lane but the fold's may
//     commit the transaction's marks. The coordinator waits for the fold
//     before routing a later marked change of the transaction to another lane
//     (step 8 below; operator decision D-Q1), and the lane re-checks at apply
//     time ([LaneApplier.ApplyMarksFenced]'s anchored flag) — without it a
//     second lane could make the transaction's marks durable beside the
//     previous transaction's (amendment D's V2).
//
// It returns the ticket the marked change's envelope carries, or nil when the
// transaction needed none: it was already anchored at the fence — the run's
// first transaction, or a position a barrier or an idle checkpoint already
// wrote — so the lanes may write its marks at once.
//
// # Opt-in (amendment C, operator 2026-09-29)
//
// That cost is paid per TRANSACTION, and a secondary-unique-heavy workload
// is one marked transaction after another: measured, the default lane path
// fell from 7557 to ~10 transactions/s (MySQL → Postgres). So the fence runs
// only with [Config.ExactlyOnceLanes]; off, it is a no-op, the lane fence
// never opens, and the lanes write no marks. That partial coverage is sound
// because the invariant this fence exists for — marks only ever exist for
// the first transaction after the persisted position — is kept by every path
// that still writes marks WITHOUT this fence:
//
//   - a barrier (keyless change, PK-changing update) drains every lane to its
//     predecessor and checkpoints BEFORE it applies, so the position sits at
//     its transaction's start when its marks commit — the fence's own prefix;
//   - the serial paths never move the position inside a source transaction
//     (ADR-0027 cohesion, CDCPOS-2, schemaEventAtBoundary) and write it with
//     the data at its commit, so their marks name the transaction starting at
//     the persisted position (phases 1–3, pinned by the crash suite);
//   - every checkpoint that passes a transaction deletes its marks in the
//     same write (closedTx), whichever path wrote them.
//
// And no mark is ever evidence for another path's change unless it names the
// same transaction and key with a HIGHER-or-equal ordinal: the barrier's
// drain means every earlier change of its transaction (lower seq, lower
// ordinal) is durable when its mark commits, so the per-key prefix the skip
// rule relies on holds; an unmarked lane change later in the transaction
// carries a higher ordinal and is never skipped by it.
func (o *Orchestrator) fenceApplyMarks(ctx context.Context, seq uint64, c ir.Change, lane int) (*FoldTicket, error) {
	if !o.exactlyOnceLanes {
		return nil, nil
	}
	tx := o.la.ApplyMarkTx(ctx, c)
	if tx == "" {
		return nil, nil
	}
	if tx == o.fencedTx {
		// A later marked change of the fenced transaction (step 8). On the
		// fold's own lane it needs no wait — a lane commits its batches in
		// order — but on any other lane it waits for the fold to commit: once
		// the drain to seq-1 made the frontier foldSeq-1, reaching foldSeq
		// means exactly that.
		if o.foldSeq != 0 && lane != o.foldLane && !o.foldAnchored() {
			return nil, o.drainLanes(ctx, o.foldSeq, o.foldLane)
		}
		return nil, nil
	}
	// Every change before this one durable on every lane: the precondition
	// for the anchor to be a legal position (position ≤ durable data).
	if err := o.drainLanes(ctx, seq-1); err != nil {
		return nil, err
	}
	fold := o.claimAnchor()
	o.fencedTx, o.foldSeq, o.foldLane = tx, 0, lane
	if fold != nil {
		fold.Tx = tx
		o.foldSeq = seq
		o.folds++
	}
	o.la.ApplyMarksFenced(tx, fold == nil)
	return fold, nil
}

// logFolds reports, once per run, how many fence checkpoints the run folded
// into lane transactions (ADR-0190 amendment D). Silent on a run with none —
// every run without --exactly-once-lanes.
func (o *Orchestrator) logFolds(ctx context.Context) {
	if o.folds > 0 {
		slog.InfoContext(ctx, "laneapply: --exactly-once-lanes wrote each mark fence's position in the marked lane batch's own transaction (ADR-0190 amendment D)",
			slog.Int("folded_fences", o.folds))
	}
}

// drainLanes blocks until every change at or below target is durable across
// the lanes — the shared prefix of [Orchestrator.barrier] and
// [Orchestrator.fenceApplyMarks]: the barrier and the fence wake the lanes.
// The look-ahead cap in [Orchestrator.routeRow] deliberately does not (it is
// busy-lane backpressure, not an idle stall — see there);
// TestOrchestrator_FrontierWaitsGoThroughDrainLanes holds that it is the only
// other wait on the frontier.
//
// It first hands a flush sentinel to each lane in only — every lane when only
// is empty; the fence's wait for one fold names just the fold's lane —
// queued behind everything the coordinator has already routed to it, so a
// lane holding a partial batch commits it on reaching the sentinel instead
// of waiting out the idle grace.
// Without that the drain costs a full idle period (100 ms) whenever the last
// lane to finish is holding a partial batch — which, on a stream that drains
// once per marked transaction, capped the lane path at ~10 transactions/s
// (the 2026-09-29 benchmark). Correctness-neutral: a lane batch's extent was
// already arbitrary (size, byte cap, idle), the frontier advances per seq
// only after its commit, and a checkpoint reads only the frontier — nothing
// depends on where a batch ends. A lane with nothing buffered just reads the
// sentinel and waits again.
func (o *Orchestrator) drainLanes(ctx context.Context, target uint64, only ...int) error {
	if o.frontier.FrontierSeq() >= target {
		return nil
	}
	for i := range o.laneIn {
		if len(only) > 0 && !slices.Contains(only, i) {
			continue
		}
		select {
		case o.laneIn[i] <- LaneChange{flush: true}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return o.frontier.WaitForFrontier(ctx, target)
}

// barrier applies a globally-ordered event (Truncate / SchemaSnapshot /
// keyless or PK-changing row change) after draining EVERY lane to the
// barrier's predecessor, so it lands in correct order relative to the row
// changes around it. It first persists a checkpoint (advancing the resume
// position to the now-fully-durable predecessor), then applies the change on
// the coordinator backend POSITION-FREE (ApplyBarrierChange writes the data +
// any schema-history row but NOT the position — the frontier checkpoint owns
// the resume position on this path), then marks the barrier's seq durable and
// persists the checkpoint again.
//
// Metadata-cache invalidation on a SchemaSnapshot is owned ENTIRELY by
// ApplyBarrierChange (whose engine implementation runs the SAME guarded
// apply-then-invalidate the serial path uses — invalidate ONLY on a real
// signature-changing boundary, NOT on the first-touch baseline). The
// orchestrator must NOT independently force-invalidate here: doing so
// bypassed that guard and marked even the first post-cold-start baseline
// SchemaSnapshot schema-dirty, which on PG forced every subsequent lane DML
// onto the QueryExecModeExec text-encode path and silently dropped every
// value that doesn't round-trip as text (Bug 158: json/jsonb → SQLSTATE
// 22P02 → lane fatal → run wedged at position 0/0). Serial never hit this
// because its boundary invalidation (cacheActiveSchemaAfterCommit) is
// guarded; deferring entirely to ApplyBarrierChange makes the concurrent
// path's invalidation byte-identical to serial.
func (o *Orchestrator) barrier(ctx context.Context, seq uint64, c ir.Change) error {
	if err := o.drainLanes(ctx, seq-1); err != nil {
		return err
	}
	if err := o.writeCheckpoint(ctx); err != nil {
		return err
	}
	if err := o.la.ApplyBarrierChange(ctx, c); err != nil {
		return err
	}
	// ApplyBarrierChange applied the barrier's data + (for a SchemaSnapshot)
	// the ADR-0049 history row atomically, but did NOT write the position —
	// the frontier checkpoint owns the resume position exclusively on this
	// path (the ADR-0104 relaxation). The barrier's seq is now durable, so
	// mark it on the frontier and persist the checkpoint. Whether the barrier's
	// own position becomes the checkpoint depends on its kind + the stream
	// shape (see handle): a Truncate is a DDL boundary and IS recorded (marker
	// streams) or token-run-recorded (marker-less); a keyless / PK-changing ROW
	// barrier is a mid-transaction position and is NOT recorded on a marker
	// stream (it is unresumable on MySQL file/pos) — the resume position stays
	// at the last TxCommit boundary and warm-resume + idempotent re-apply
	// replays the in-flight tx (at-least-once for keyless, as on the serial
	// path). A SchemaSnapshot is never a resume point (metadata-anchored — 0/0
	// at first-touch), excluded from boundary tracking; warm-resume + idempotent
	// re-apply replays the schema event from the prior boundary (Bug 158).
	o.frontier.MarkCommitted(seq)
	o.sinceCheckpoint = 0
	return o.writeCheckpoint(ctx)
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
// A lane writes no position of its own — the coordinator's seq-frontier owns
// the merged resume point (the ADR-0104 position relaxation). The one
// position a lane ever writes is one the coordinator chose and claimed: a
// mark fence's anchor, folded into the batch carrying the fenced
// transaction's first marked change (a [FoldTicket], ADR-0190 amendment D).
// The lane never
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
			if lc.flush {
				// The coordinator is draining (drainLanes): commit what we hold.
				return buf, false, nil
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
	// The (sub-)batch's fold ticket, if it carries one (ADR-0190 amendment D;
	// a split hands it to the half holding its envelope). A COMMIT-step error
	// of a fold batch is never retried in place — see CommitOutcomeUnknown.
	fold, err := foldOf(buf)
	if err != nil {
		return 0, err
	}
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
			rawErr = o.commitObserve(ctx, lane, ctrl, buf, fold)
			if rawErr == nil {
				o.frontier.MarkCommitted(buf[0].Seq) // advance only on durable commit
				return 1, nil
			}
			if fold != nil && IsCommitOutcomeUnknown(rawErr) {
				return 0, o.foldCommitUnknownFatal(fold, rawErr)
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
		rawErr = o.commitObserve(ctx, lane, ctrl, buf, fold)
		if rawErr == nil {
			for _, e := range buf { // advance only on durable commit
				o.frontier.MarkCommitted(e.Seq)
			}
			return len(buf), nil
		}
		if fold != nil && IsCommitOutcomeUnknown(rawErr) {
			return 0, o.foldCommitUnknownFatal(fold, rawErr)
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
func (o *Orchestrator) commitObserve(ctx context.Context, lane int, ctrl ir.BatchSizeController, buf []LaneChange, fold *FoldTicket) error {
	start := time.Now()
	rawErr := o.applyOnce(ctx, lane, buf, fold)
	if ctrl != nil {
		ctrl.ObserveBatch(ctx, time.Since(start), len(buf), o.la.ClassifyError(rawErr))
	}
	return rawErr
}

// applyOnce drives the seam's ApplyLaneBatch with the envelope buffer
// converted to the raw []ir.Change the engine dispatches, and buf's fold
// ticket. It returns the raw (unclassified) error so the caller's retry
// predicate inspects the original.
func (o *Orchestrator) applyOnce(ctx context.Context, lane int, buf []LaneChange, fold *FoldTicket) error {
	changes := make([]ir.Change, len(buf))
	for i, e := range buf {
		changes[i] = e.Change
	}
	_, err := o.la.ApplyLaneBatch(ctx, lane, changes, fold)
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
	ck, ok := o.nextCheckpoint()
	if !ok {
		return nil
	}
	if err := o.la.WriteCheckpoint(ctx, ck.pos, ck.rowsApplied, ck.closedTxs); err != nil {
		return err
	}
	o.checkpointWritten(ck)
	return nil
}

// checkpoint is one position write the frontier allows: the boundary, the
// rows_applied increment it carries, and the transactions it closes.
type checkpoint struct {
	seq         uint64
	pos         ir.Position
	rowsApplied int64
	closedTxs   []string
	cum         uint64
	hasCum      bool
}

// nextCheckpoint computes the position write the frontier allows now, or
// ok=false when there is nothing new to persist. Shared by writeCheckpoint
// and a mark fence's fold ([Orchestrator.claimAnchor]), so the two writers
// compute the same thing for the same boundary.
func (o *Orchestrator) nextCheckpoint() (checkpoint, bool) {
	pos, seq, ok := o.frontier.CheckpointPosition()
	// Seq-monotone guard: never write a boundary at or below the last one
	// persisted (prevents regression below a barrier's direct apply write,
	// and skips redundant re-writes of the same point). It is also what holds
	// every coordinator checkpoint off while a claimed fold is in flight.
	if !ok || seq <= o.lastWrittenSeq {
		return checkpoint{}, false
	}
	// rows_applied delta: the DML that became durable (frontier ≥ seq) and is
	// now covered by the persisted position, since the last checkpoint. cum is
	// recorded at every RecordTxBoundary call site, so a chosen boundary seq
	// always has an entry. If it were ever missing (defensive; unreachable in
	// production), leave BOTH the delta at 0 AND lastWrittenCum untouched, so
	// the next checkpoint still computes its delta from the true baseline
	// rather than re-counting from 0 — a missed boundary defers its rows, it
	// never over-counts. cum is monotone in seq, so delta is never negative.
	cum, hasCum := o.boundaryRowDML[seq]
	ck := checkpoint{seq: seq, pos: pos, closedTxs: o.closedTxsUpTo(seq), cum: cum, hasCum: hasCum}
	if hasCum && cum > o.lastWrittenCum {
		ck.rowsApplied = int64(cum - o.lastWrittenCum)
	}
	return ck, true
}

// checkpointWritten advances the persisted-checkpoint bookkeeping past ck —
// after its write committed, or, for a fold, when the fence claims it.
func (o *Orchestrator) checkpointWritten(ck checkpoint) {
	o.lastWrittenSeq = ck.seq
	if ck.hasCum {
		o.lastWrittenCum = ck.cum
	}
	o.pruneBoundaryRowDML(ck.seq)
	for s := range o.closedTx {
		if s <= ck.seq {
			delete(o.closedTx, s)
		}
	}
}

// closedTxsUpTo lists, in seq order, the transactions whose commit lies at or
// below seq and has not been handed to a persisted checkpoint yet — the
// ADR-0190 marks this checkpoint deletes. Coordinator-goroutine-only.
func (o *Orchestrator) closedTxsUpTo(seq uint64) []string {
	var seqs []uint64
	for s := range o.closedTx {
		if s <= seq {
			seqs = append(seqs, s)
		}
	}
	slices.Sort(seqs)
	txs := make([]string, 0, len(seqs))
	for _, s := range seqs {
		txs = append(txs, o.closedTx[s])
	}
	return txs
}

// pruneBoundaryRowDML drops boundaryRowDML entries at or below the just-
// persisted boundary seq — they can never be chosen again (the frontier's
// CheckpointPosition and the seq-monotone guard both advance past them), so
// the map stays bounded by the in-flight window. Coordinator-goroutine-only.
func (o *Orchestrator) pruneBoundaryRowDML(upTo uint64) {
	for s := range o.boundaryRowDML {
		if s <= upTo {
			delete(o.boundaryRowDML, s)
		}
	}
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
