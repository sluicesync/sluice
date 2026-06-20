// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// # Concurrent key-hashed CDC apply (ADR-0104, item 23(c))
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
// This file is the correctness core of that safe partitioning: the
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
// Instead the checkpoint coordinator persists the merged VGTID in a
// SEPARATE transaction, and ONLY up to a source-transaction boundary whose
// every change is durably committed across all lanes (the contiguous
// frontier). This RELAXES ADR-0007's per-batch atomicity to the weaker —
// but still exactly-once-preserving — invariant:
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

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// laneRouter maps each row-bearing change to one of `lanes` apply lanes by
// a stable hash of the change's (qualified table, ordered primary-key
// values). The mapping is deterministic and total: the same logical key
// always resolves to the same lane, which is the load-bearing
// same-key-closed property — all changes to one row are applied in source
// order on a single lane, so the dependent-row hazard (INSERT then
// DELETE/UPDATE of the same key racing on two transactions) cannot occur.
//
// The router is pure and immutable; it holds no state and is safe to call
// from the single routing goroutine. Keyless changes (no primary key) are
// NOT routed here — they take the barrier path (drain all lanes, apply
// single-row), so laneFor is only ever called with a non-empty pkCols.
type laneRouter struct {
	lanes int
}

// newLaneRouter returns a router over `lanes` lanes. lanes < 1 is clamped
// to 1 (serial) so a misconfigured caller degrades to correct-but-serial
// rather than panicking on a modulo-by-zero.
func newLaneRouter(lanes int) laneRouter {
	if lanes < 1 {
		lanes = 1
	}
	return laneRouter{lanes: lanes}
}

// laneFor returns the lane index in [0, lanes) for a change to `qualified`
// (schema.table) whose ordered primary-key column values are pkVals. The
// hash is FNV-1a over the qualified name and a canonical, type-tagged
// encoding of each key value, so two values that are equal-but-typed
// differently (int64(5) vs "5") never alias — a correctness requirement,
// not just balance: the SAME row must always hash identically, and the
// decode path guarantees a given column yields the same Go type across
// Insert/Update/Delete, so a per-value type tag keeps distinct keys
// distinct without depending on cross-type coincidences.
func (r laneRouter) laneFor(qualified string, pkVals []any) int {
	if r.lanes <= 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(qualified))
	_, _ = h.Write([]byte{0}) // table/value domain separator
	for _, v := range pkVals {
		writeCanonicalKeyValue(h, v)
		_, _ = h.Write([]byte{0}) // value separator (so ["a","b"] ≠ ["ab"])
	}
	return int(h.Sum64() % uint64(r.lanes))
}

// writeCanonicalKeyValue writes a deterministic, type-tagged byte encoding
// of a single primary-key value to h. The tag prefix ensures values of
// different kinds never collide on identical byte content (int64(49) vs
// the string "1"). The set of kinds mirrors what the VStream/binlog decode
// path can place in a key column; an unrecognised kind falls back to the
// fmt-style %v rendering under a generic tag — deterministic for the
// scalar/byte-slice kinds that reach a primary key.
func writeCanonicalKeyValue(h io.Writer, v any) {
	switch t := v.(type) {
	case nil:
		_, _ = h.Write([]byte{'N'})
	case int64:
		_, _ = h.Write([]byte{'i'})
		_, _ = h.Write([]byte(strconv.FormatInt(t, 10)))
	case int:
		_, _ = h.Write([]byte{'i'})
		_, _ = h.Write([]byte(strconv.FormatInt(int64(t), 10)))
	case uint64:
		_, _ = h.Write([]byte{'u'})
		_, _ = h.Write([]byte(strconv.FormatUint(t, 10)))
	case string:
		_, _ = h.Write([]byte{'s'})
		_, _ = h.Write([]byte(t))
	case []byte:
		_, _ = h.Write([]byte{'b'})
		_, _ = h.Write(t)
	case bool:
		if t {
			_, _ = h.Write([]byte{'B', '1'})
		} else {
			_, _ = h.Write([]byte{'B', '0'})
		}
	default:
		// Float/decimal/temporal keys are rare but legal; render under a
		// generic tag. The encoding only needs determinism (same value →
		// same bytes), which the standard formatter provides for these.
		_, _ = h.Write([]byte{'?'})
		_, _ = fmt.Fprintf(h, "%v", t)
	}
}

// pkValuesForRouting extracts the ordered primary-key values from a change
// for routing, reading from the map appropriate to the change kind:
// Insert.Row, Update.After (the post-image — the row's current identity),
// Delete.Before. Returns ok=false when the change is not a routable
// row-change (TxBegin/TxCommit/Truncate/SchemaSnapshot — all barrier
// events) or when any key column is absent from the row (a malformed
// change that must take the safe barrier path rather than be silently
// mis-routed).
//
// pkCols is the table's ordered primary-key column list (from the
// applier's pkCache). An empty pkCols means a keyless table → ok=false →
// barrier path (the keyless guard applies single-row regardless).
//
// PK-changing UPDATEs (After's key differs from Before's) are a key
// migration, not a same-key op: routing on the After image keeps the new
// identity's lane consistent, but a concurrent op on the OLD key could be
// on a different lane. Such updates are rare and the caller treats a
// detected key change as a barrier (see the lane orchestrator) so the
// old/new ordering is preserved; this helper reports the After-image key
// and leaves that detection to the caller.
func pkValuesForRouting(c ir.Change, pkCols []string) (vals []any, ok bool) {
	if len(pkCols) == 0 {
		return nil, false
	}
	var row ir.Row
	switch v := c.(type) {
	case ir.Insert:
		row = v.Row
	case ir.Update:
		row = v.After
	case ir.Delete:
		row = v.Before
	default:
		return nil, false
	}
	if row == nil {
		return nil, false
	}
	vals = make([]any, len(pkCols))
	for i, col := range pkCols {
		val, present := row[col]
		if !present {
			return nil, false
		}
		vals[i] = val
	}
	return vals, true
}

// checkpointFrontier tracks, across all lanes, which source-sequence
// numbers have durably committed and computes the contiguous frontier: the
// highest sequence S such that EVERY change with sequence ≤ S is committed.
// Because each change is routed to exactly one lane and a lane commits its
// changes in increasing sequence order, "all ≤ S committed" is exactly the
// out-of-order-completion → contiguous-prefix problem, solved here with a
// pending-set + advancing watermark (memory bounded by the in-flight
// window, since lane backpressure prevents unbounded look-ahead).
//
// Separately it records source-transaction boundary positions (the VGTID
// captured at each TxCommit). checkpointPosition returns the position of
// the highest tx boundary whose sequence ≤ the frontier — the safe resume
// point under the position relaxation documented at the top of this file.
//
// Concurrency: lanes call markCommitted from W goroutines; the router
// goroutine calls recordTxBoundary; the coordinator calls
// checkpointPosition. All three take the mutex, so the tracker is the
// single synchronization point for cross-lane frontier state.
type checkpointFrontier struct {
	mu sync.Mutex

	frontier uint64          // highest seq with all ≤ it committed
	pending  map[uint64]bool // committed seqs strictly above frontier, awaiting contiguity

	// txBoundaries holds, in ascending sequence order, the (seq, position)
	// of each source-transaction commit not yet superseded by a persisted
	// checkpoint. Pruned as the persisted checkpoint advances.
	txBoundaries []txBoundary

	// notify is closed-and-replaced on every frontier advance so
	// waitForFrontier can block until a target seq is reached without
	// busy-polling. Guarded by mu.
	notify chan struct{}
}

type txBoundary struct {
	seq uint64
	pos ir.Position
}

func newCheckpointFrontier() *checkpointFrontier {
	return &checkpointFrontier{
		pending: make(map[uint64]bool),
		notify:  make(chan struct{}),
	}
}

// waitForFrontier blocks until the contiguous frontier reaches target (all
// changes with seq ≤ target are durably committed across every lane) or ctx
// is cancelled. The coordinator uses it to drain all lanes before a barrier
// event (Truncate / SchemaSnapshot / keyless / PK-changing update) so the
// barrier applies in correct global order relative to the row changes
// around it. Progress is guaranteed by the lanes' idle-flush, so a quiet
// lane still drains within DefaultIdleFlushPeriod.
func (f *checkpointFrontier) waitForFrontier(ctx context.Context, target uint64) error {
	for {
		f.mu.Lock()
		if f.frontier >= target {
			f.mu.Unlock()
			return nil
		}
		ch := f.notify
		f.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// markCommitted records that the change at sequence seq has durably
// committed (called by a lane after its target transaction commits), then
// advances the contiguous frontier across any now-contiguous pending seqs.
// Sequences may arrive out of order across lanes; within a lane they
// arrive in order. Marking a barrier marker's own seq (TxBegin/TxCommit,
// which do no lane work) is the coordinator's responsibility via
// markCommitted too, so the frontier can advance past boundary markers.
func (f *checkpointFrontier) markCommitted(seq uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if seq <= f.frontier {
		// Already subsumed (e.g. a duplicate report) — no-op.
		return
	}
	f.pending[seq] = true
	advanced := false
	for f.pending[f.frontier+1] {
		delete(f.pending, f.frontier+1)
		f.frontier++
		advanced = true
	}
	if advanced {
		// Wake any waitForFrontier waiters: close the current notify
		// channel and install a fresh one for the next wait cycle.
		close(f.notify)
		f.notify = make(chan struct{})
	}
}

// recordTxBoundary records the position to persist when the frontier
// reaches a source-transaction commit at sequence seq. Boundaries must be
// recorded in ascending seq order (the router assigns seqs monotonically),
// which keeps txBoundaries sorted for the binary search in
// checkpointPosition.
func (f *checkpointFrontier) recordTxBoundary(seq uint64, pos ir.Position) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txBoundaries = append(f.txBoundaries, txBoundary{seq: seq, pos: pos})
}

// checkpointPosition returns the position of the highest source-transaction
// boundary whose sequence is ≤ the current frontier — the newest resume
// point at which every change is durable across all lanes — and ok=true
// when such a boundary exists. It prunes superseded boundaries so memory
// stays bounded. ok=false means no tx boundary has fully committed yet
// (nothing safe to persist); the caller must NOT persist a partial point.
func (f *checkpointFrontier) checkpointPosition() (pos ir.Position, seq uint64, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	front := f.frontier
	// Highest boundary with seq ≤ front. txBoundaries is ascending by seq.
	idx := sort.Search(len(f.txBoundaries), func(i int) bool {
		return f.txBoundaries[i].seq > front
	})
	if idx == 0 {
		return ir.Position{}, 0, false
	}
	chosen := f.txBoundaries[idx-1]
	// Prune every boundary strictly below the chosen one; keep the chosen
	// one so a repeat call with no further progress is idempotent (returns
	// the same boundary, ok=true) rather than spuriously false.
	if idx-1 > 0 {
		f.txBoundaries = append([]txBoundary(nil), f.txBoundaries[idx-1:]...)
	}
	return chosen.pos, chosen.seq, true
}

// frontierSeq returns the current contiguous frontier sequence. Test and
// observability helper; not on the hot path.
func (f *checkpointFrontier) frontierSeq() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frontier
}

// --- Guarded metadata-cache accessors (ADR-0104 concurrency safety) ---
//
// Every read/write of pkCache, colTypeCache, keylessCache and
// warnedKeyless funnels through these so the concurrent key-hash lanes
// (which call dispatch from W goroutines) never touch a map unguarded. The
// load-on-miss callers use the RLock-check → unlock → DB-load → Lock-store
// pattern so a cache miss does NOT serialize every lane on the DB
// round-trip; the double-store on a concurrent miss is idempotent.

func (a *ChangeApplier) cachedPK(qn string) ([]string, bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.pkCache[qn]
	return v, ok
}

func (a *ChangeApplier) storePK(qn string, pk []string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.pkCache == nil {
		a.pkCache = make(map[string][]string)
	}
	a.pkCache[qn] = pk
}

func (a *ChangeApplier) cachedColTypes(qn string) (map[string]*ir.Column, bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.colTypeCache[qn]
	return v, ok
}

func (a *ChangeApplier) storeColTypes(qn string, cols map[string]*ir.Column) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.colTypeCache == nil {
		a.colTypeCache = make(map[string]map[string]*ir.Column)
	}
	a.colTypeCache[qn] = cols
}

func (a *ChangeApplier) cachedKeyless(qn string) (keyless, ok bool) {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	v, ok := a.keylessCache[qn]
	return v, ok
}

func (a *ChangeApplier) storeKeyless(qn string, keyless bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.keylessCache == nil {
		a.keylessCache = make(map[string]bool)
	}
	a.keylessCache[qn] = keyless
}

// markWarnedKeyless records that the keyless WARN has been emitted for qn
// and reports whether THIS call was the one that recorded it — so the
// caller logs the WARN exactly once even under concurrent lanes (the
// check-and-set is atomic under the write lock).
func (a *ChangeApplier) markWarnedKeyless(qn string) (firstTime bool) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	if a.warnedKeyless == nil {
		a.warnedKeyless = make(map[string]bool)
	}
	if a.warnedKeyless[qn] {
		return false
	}
	a.warnedKeyless[qn] = true
	return true
}

// invalidateMetadataCaches drops the PK + column-type cache entries for qn
// (the ADR-0049 schema-change cache invalidation). Guarded so a
// barrier-path invalidation is safe against concurrent lane reads.
func (a *ChangeApplier) invalidateMetadataCaches(qn string) {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	delete(a.colTypeCache, qn)
	delete(a.pkCache, qn)
}

// --- Concurrent apply orchestration (ADR-0104, item 23(c)) ---

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

// maxInLaneRetries bounds the SINGLE-change retry loop (the recursion's base
// case in applyLaneBatch): a lone change is re-applied idempotently (ADR-0010)
// up to this many times. A transient single-row tx-killer recovers within the
// budget; a target that tx-kills even a single row exhausts it and FAILS THE
// RUN LOUDLY (→ ctx cancel → the streamer's warm-resume re-streams from the
// last durable boundary) rather than spinning forever. Multi-change batches
// converge by SPLITTING (retrySameBeforeSplit → halve), not by this cap.
const maxInLaneRetries = 10

// laneCommitHookForTest, when non-nil, is copied onto every
// concurrentApplyManager's laneCommitHook so integration tests can force a
// deterministic lane-commit failure (the in-lane retry / tx-killer-convergence
// pin). Production leaves it nil — the apply path is byte-identical. Set only
// by single-test fixtures (set then reset in the same test), so no concurrent
// mutation across tests.
var laneCommitHookForTest func(buf []laneChange) error

// laneChange is the {seq, change} envelope the coordinator pushes onto a
// lane's feed. Pairing the source sequence with its change on one channel
// is the FIFO-alignment fix: the lane reads the seq and the change
// together, so markCommitted(seq) can never drift out of step with the
// change it accounts for.
type laneChange struct {
	seq    uint64
	change ir.Change
}

// concurrentApplyManager is the ADR-0104 key-hash concurrent apply
// coordinator. The single coordinator goroutine (the one running
// [ChangeApplier.applyBatchConcurrent]) reads the merged change stream in
// source order, assigns each event a monotonic sequence, and either routes
// a keyed row-change to its key-hash lane or handles a barrier event
// (Tx*, Truncate, SchemaSnapshot, keyless, PK-changing update) by draining
// all lanes first. W lane goroutines each apply their routed changes
// in-order on a dedicated backend (no position write) and report committed
// sequences to the [checkpointFrontier]; the coordinator persists the
// resume position only up to a fully-durable source-tx boundary.
type concurrentApplyManager struct {
	a            *ChangeApplier
	streamID     string
	maxBatchSize int
	lanes        int

	laneDB   *sql.DB // dedicated pool for lane backends (MaxOpenConns == lanes)
	router   laneRouter
	frontier *checkpointFrontier

	// laneIn is the per-lane change feed (coordinator → lane). Each element
	// is a {seq, change} envelope so a lane reads the sequence and its change
	// inherently paired — there is no separate seq channel to keep
	// FIFO-aligned (the prior two-channel design was fragile: a lane that read
	// N changes had to trust N seqs were queued in lock-step on a sibling
	// channel; the envelope removes that coupling).
	laneIn []chan laneChange

	// laneControllers are the per-lane AIMD controllers (one per lane, in
	// lane-index order), copied from the applier at construction. A nil slice
	// (or a nil element) makes that lane run at the static maxBatchSize with
	// bounded in-lane retry but no adaptive sizing. Each lane drives its own
	// controller from its single goroutine, so a tx-killer shrink stays local
	// to the affected lane. See laneApplyLoop.
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
	// position that an uncommitted later change also carries (the unsafe
	// mid-transaction-resume case). prevSeq == 0 means "no prior event".
	prevSeq uint64
	prevPos ir.Position

	cancel context.CancelFunc

	wg       sync.WaitGroup
	errMu    sync.Mutex
	firstErr error

	// laneCommitHook, when non-nil, is invoked just before a lane's commit
	// with the batch about to commit; a non-nil return forces the commit to
	// fail (rolled back), driving the in-lane retry path deterministically.
	// Test-only seam (the lane analogue of the serial path's removed
	// pipelineTestCommitHook); nil in production. Wired by the integration
	// pins, which run a single lane, so no cross-goroutine access concern.
	laneCommitHook func(buf []laneChange) error

	// commitBatchFn is the lane-batch commit seam: production uses
	// [commitLaneBatch] (a real lane transaction). Unit tests substitute a
	// no-DB fake so the in-lane shrink-and-retry / markCommitted ordering can
	// be pinned deterministically without testcontainers. Defaults to
	// commitLaneBatch in [run]; nil only transiently before that.
	commitBatchFn func(ctx context.Context, buf []laneChange) error
}

// applyBatchConcurrent is the ADR-0104 concurrent key-hash apply entry,
// invoked from ApplyBatch when an operator wires --apply-concurrency
// (the lane count W) > 1 and a dedicated pool can be opened. It owns the
// lane pool for the call's lifetime. On any lane or coordinator error the
// whole run stops (ctx cancel + drain) and the error is returned; the
// persisted position reflects only fully-durable work, so warm-resume
// re-streams + idempotently re-applies the remainder (exactly-once for
// keyed tables; the keyless at-least-once guarantee is unchanged because
// keyless changes take the single-row barrier path).
func (a *ChangeApplier) applyBatchConcurrent(ctx context.Context, streamID string, changes <-chan ir.Change, maxBatchSize, lanes int) error {
	laneDB, err := openDB(ctx, a.pipelineCfg)
	if err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: open concurrent lane pool: %w", err))
	}
	defer func() { _ = laneDB.Close() }()
	laneDB.SetMaxOpenConns(lanes)
	laneDB.SetMaxIdleConns(lanes)

	m := &concurrentApplyManager{
		a:               a,
		streamID:        streamID,
		maxBatchSize:    maxBatchSize,
		lanes:           lanes,
		laneDB:          laneDB,
		router:          newLaneRouter(lanes),
		frontier:        newCheckpointFrontier(),
		laneIn:          make([]chan laneChange, lanes),
		laneControllers: a.laneControllers,
		laneCommitHook:  laneCommitHookForTest, // nil in production
	}
	// Buffer each lane a batch's worth so the coordinator's routing isn't
	// gated on a lane's per-change commit latency (the whole point — lanes
	// overlap their cross-region commit RTTs).
	buf := maxBatchSize
	if buf < 1 {
		buf = 1
	}
	for i := range m.laneIn {
		m.laneIn[i] = make(chan laneChange, buf)
	}
	return m.run(ctx, changes)
}

func (m *concurrentApplyManager) run(ctx context.Context, changes <-chan ir.Change) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	defer cancel()

	if m.commitBatchFn == nil {
		m.commitBatchFn = m.commitLaneBatch
	}

	m.wg.Add(m.lanes)
	for i := 0; i < m.lanes; i++ {
		go m.laneApplyLoop(ctx, i)
	}
	slog.InfoContext(ctx,
		"mysql: applier: concurrent key-hash CDC apply engaged — routing row changes to W in-order "+
			"lanes by primary-key hash, committing each lane concurrently on a dedicated pool; the "+
			"resume position advances only to a source-tx boundary durable across all lanes (ADR-0104)",
		slog.Int("lanes_W", m.lanes),
		slog.Int("dedicated_backends", m.lanes))

	var loopErr error
	for c := range changes {
		if err := m.getErr(); err != nil {
			loopErr = err
			break
		}
		m.nextSeq++
		if err := m.handle(ctx, m.nextSeq, c); err != nil {
			loopErr = err
			break
		}
	}

	if loopErr != nil {
		// Abort: unblock any lane stuck on a slow commit and any pending
		// barrier drain, then collect.
		cancel()
	}
	for _, ch := range m.laneIn {
		close(ch)
	}
	m.wg.Wait()
	if e := m.getErr(); e != nil && loopErr == nil {
		loopErr = e
	}
	if loopErr != nil {
		return loopErr
	}
	// Clean end-of-stream: the final position run never saw a differing
	// successor, so record its boundary now, then persist the final
	// fully-durable checkpoint (frontier is at its max after wg.Wait).
	if m.prevSeq != 0 {
		m.frontier.recordTxBoundary(m.prevSeq, m.prevPos)
	}
	return m.writeCheckpoint(ctx)
}

// handle dispatches one source event by kind: boundary markers advance the
// frontier directly, keyed row-changes route to a lane, everything else
// (Truncate / SchemaSnapshot / keyless / PK-changing update) takes the
// barrier path.
func (m *concurrentApplyManager) handle(ctx context.Context, seq uint64, c ir.Change) error {
	// Position-change boundary detection (see prevSeq/prevPos): when this
	// event's position differs from the previous event's, the previous
	// event was the last of its position run — a safe checkpoint boundary.
	m.noteBoundary(seq, c.Pos())

	switch c.(type) {
	case ir.TxBegin:
		// Boundary marker, no lane work — mark committed so the contiguous
		// frontier can advance past it once the tx's rows (lower seqs) land.
		m.frontier.markCommitted(seq)
		return nil
	case ir.TxCommit:
		m.frontier.markCommitted(seq)
		return m.maybeCheckpoint(ctx)
	case ir.Insert, ir.Update, ir.Delete:
		return m.routeRow(ctx, seq, c)
	default:
		// Truncate, SchemaSnapshot, or any future barrier-class event.
		return m.barrier(ctx, seq, c)
	}
}

// noteBoundary records the previous event as a checkpoint boundary when the
// current event's position differs from it, then advances the prev cursor.
// Coordinator-goroutine-only (no lock on prev* needed). A boundary at seq S
// means: once the frontier reaches S, the position at S is a safe resume
// point (no later change carries that same position). Recording happens
// when the position CHANGES, so the highest seq of each position run is the
// boundary — exactly the safe point.
func (m *concurrentApplyManager) noteBoundary(seq uint64, pos ir.Position) {
	if m.prevSeq != 0 && pos.Token != m.prevPos.Token {
		m.frontier.recordTxBoundary(m.prevSeq, m.prevPos)
	}
	m.prevSeq = seq
	m.prevPos = pos
}

// routeRow routes a keyed row-change to its key-hash lane, or falls to the
// barrier path when the change is keyless, malformed, or a PK-changing
// update (where the old and new keys could land on different lanes and the
// old/new ordering must be preserved globally).
func (m *concurrentApplyManager) routeRow(ctx context.Context, seq uint64, c ir.Change) error {
	schema, table := rowChangeSchemaTable(c)
	routed := m.a.routedSchema(schema)
	pkCols, err := m.a.pkForRedact(ctx, schema, table)
	if err != nil {
		return classifyApplierError(err)
	}
	vals, ok := pkValuesForRouting(c, pkCols)
	if !ok {
		// Keyless / malformed → single-row barrier (preserves the ADR-0089
		// keyless at-least-once bound; never silently mis-routed).
		return m.barrier(ctx, seq, c)
	}
	if u, isUpd := c.(ir.Update); isUpd && pkChangedUpdate(u, pkCols) {
		return m.barrier(ctx, seq, c)
	}
	lane := m.router.laneFor(qualifiedName(routed, table), vals)
	// Push the {seq, change} envelope so the lane reads the sequence and its
	// change inherently paired (the FIFO-alignment fix — no sibling seq
	// channel to drift out of step). The select honours ctx cancel so a
	// stalled lane during shutdown doesn't wedge the coordinator.
	select {
	case m.laneIn[lane] <- laneChange{seq: seq, change: c}:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.sinceCheckpoint++
	return m.maybeCheckpoint(ctx)
}

// barrier applies a globally-ordered event (Truncate / SchemaSnapshot /
// keyless or PK-changing row change) after draining EVERY lane to the
// barrier's predecessor, so it lands in correct order relative to the row
// changes around it. It first persists a checkpoint (so the barrier's own
// position write, via applyOne, is monotone), then applies the change on
// the coordinator backend (applyOne writes the barrier's position + data
// atomically — ADR-0007), then advances the frontier past it and records
// it as a tx boundary so subsequent checkpoints stay monotone from here.
func (m *concurrentApplyManager) barrier(ctx context.Context, seq uint64, c ir.Change) error {
	if err := m.frontier.waitForFrontier(ctx, seq-1); err != nil {
		return err
	}
	if err := m.writeCheckpoint(ctx); err != nil {
		return err
	}
	if err := m.a.applyOne(ctx, m.streamID, c); err != nil {
		return err
	}
	// SchemaSnapshot changed the table shape — drop the metadata caches so
	// lanes re-probe on the next change for that table.
	if snap, ok := c.(ir.SchemaSnapshot); ok {
		m.a.invalidateMetadataCaches(qualifiedName(m.a.routedSchema(snap.Schema), snap.Table))
	}
	// applyOne persisted the barrier's own position + data atomically and
	// it is now the highest durable point. Mark it on the frontier and
	// advance the persisted-checkpoint cursor to it so a later checkpoint
	// can never regress below the barrier (the seq-monotone guard).
	m.frontier.markCommitted(seq)
	m.lastWrittenSeq = seq
	m.lastWrittenTok = c.Pos().Token
	m.sinceCheckpoint = 0
	return nil
}

// laneApplyLoop runs one lane (ADR-0104 graduation): it reads a batch of
// the lane's routed {seq, change} envelopes, applies them on the lane's
// dedicated backend in one target transaction, and — ONLY after that
// transaction durably commits — advances the contiguous frontier past each
// committed seq. It owns per-lane AIMD sizing (its own controller) AND the
// in-lane shrink-and-retry that graduates --apply-concurrency out of
// preview: a retriable commit failure (a Vitess tx-killer, a transient)
// re-applies the SAME buffered batch idempotently (ADR-0010) at the
// controller's freshly-shrunk size, so a loaded cross-region target
// converges in-lane instead of dropping the whole run on the first abort.
//
// A lane writes NO position — the coordinator's seq-frontier owns the
// merged []shardGtid resume point (the ADR-0104 position relaxation). The
// lane never sees keyless / schema / Tx-boundary events (the coordinator's
// routing/barrier handles those), so this loop is deliberately lean: no
// keyless guard, no schema handling, no Tx-commit flush, no applyOne.
//
// ## Exactly-once invariants (do not reorder — the review focus)
//
//   - markCommitted(seq) fires ONLY after the lane's target transaction
//     durably commits. A retriable retry re-applies the SAME buf and does
//     NOT advance any seq until a commit succeeds, so the frontier — and
//     thus the persisted resume position — only ever passes durable work.
//   - Value encoding is byte-identical to the serial path: each change is
//     redacted + shard-stamped (m.a.redactChange + m.a.stampShardChange, in
//     the SAME order RunOneBatch uses) then dispatched via the SAME
//     m.a.dispatch. In-lane retry changes only WHETHER/WHEN a batch is
//     re-applied, never HOW a value is encoded.
//   - A genuinely un-committable batch (the target tx-kills even at the
//     controller floor of 1) fails the run LOUDLY after maxInLaneRetries —
//     ctx cancel → warm-resume — rather than looping forever.
func (m *concurrentApplyManager) laneApplyLoop(ctx context.Context, i int) {
	defer m.wg.Done()
	var ctrl ir.BatchSizeController
	if i < len(m.laneControllers) {
		ctrl = m.laneControllers[i]
	}
	for {
		size := m.maxBatchSize
		if ctrl != nil {
			size = ctrl.NextBatchSize()
		}
		buf, closed, err := m.readLaneBatch(ctx, i, size)
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
		if err := m.applyLaneBatch(ctx, ctrl, buf); err != nil {
			m.recordErr(err)
			m.cancel()
			return
		}
		if closed {
			return
		}
	}
}

// readLaneBatch reads up to `size` {seq, change} envelopes from lane i's
// feed into a fresh slice, returning early when: the channel closes
// (closed=true; the caller drains+returns once buf is empty), the running
// ApproximateChangeBytes total reaches the applier's byte cap (ADR-0028 —
// the same cap the serial path enforces), the idle-flush grace elapses with
// a partial buffer (so the frontier/position stays current on a quiet
// lane — item 18 Fix B), or ctx is cancelled. A non-nil error is ONLY
// ctx.Err(); the read itself cannot otherwise fail.
func (m *concurrentApplyManager) readLaneBatch(ctx context.Context, i, size int) (buf []laneChange, closed bool, err error) {
	if size < 1 {
		size = 1
	}
	byteCap := m.a.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
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
		case lc, ok := <-m.laneIn[i]:
			if !ok {
				return buf, true, nil
			}
			buf = append(buf, lc)
			batchBytes += ir.ApproximateChangeBytes(lc.change)
			if batchBytes >= byteCap {
				return buf, false, nil
			}
			if idle == nil {
				idle = time.NewTimer(defaultIdleFlushPeriod)
			} else {
				resetLaneIdleTimer(idle)
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
// one row) is fatal after the budget. markCommitted fires per envelope ONLY
// after that envelope's sub-batch durably commits, in seq order across the
// splits, so exactly-once + same-lane ordering hold. See laneApplyLoop's
// invariant block.
func (m *concurrentApplyManager) applyLaneBatch(ctx context.Context, ctrl ir.BatchSizeController, buf []laneChange) error {
	// Single change: bounded retry-in-place. A transient single-row tx-killer
	// recovers within the budget; persistent failure (the target cannot accept
	// even one row) is fatal — surface loudly so warm-resume / the operator can
	// act. There is nothing left to split, so this is the recursion's base case.
	if len(buf) == 1 {
		var rawErr error
		// attempt 0 is the initial try; 1..maxInLaneRetries are the retries.
		for attempt := 0; attempt <= maxInLaneRetries; attempt++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			rawErr = m.commitObserve(ctx, ctrl, buf)
			if rawErr == nil {
				m.frontier.markCommitted(buf[0].seq) // advance only on durable commit
				return nil
			}
			if !isApplierErrorRetriable(rawErr) {
				return classifyApplierError(rawErr)
			}
		}
		return classifyApplierError(rawErr)
	}

	// Multi-change: retry the SAME batch a few times first — a TRANSIENT
	// tx-killer (a momentary target overload) recovers on retry-same without
	// the cost of splitting. Only when it PERSISTS do we re-chunk.
	var rawErr error
	for attempt := 1; attempt <= retrySameBeforeSplit; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rawErr = m.commitObserve(ctx, ctrl, buf)
		if rawErr == nil {
			for _, e := range buf { // advance only on durable commit
				m.frontier.markCommitted(e.seq)
			}
			return nil
		}
		if !isApplierErrorRetriable(rawErr) {
			return classifyApplierError(rawErr) // non-retriable → fatal
		}
	}

	// Persistent retriable failure on a multi-change batch ⇒ it is too large to
	// commit under the target's tx-killer timeout, and re-applying it whole can
	// NEVER converge (the controller's MD only sizes the NEXT read). RE-CHUNK:
	// split in half and apply each half recursively until the sub-batches are
	// small enough to commit ("the shrink IS the split", matching serial #54).
	// The first half commits + advances the frontier before the second is
	// attempted; a fatal second half leaves the first durable (warm-resume
	// re-applies the rest idempotently). markCommitted therefore still fires
	// per envelope only on a durable commit, in seq order across the splits.
	mid := len(buf) / 2
	slog.WarnContext(ctx,
		"mysql: applier: concurrent lane batch persistently tx-killed — splitting to converge in-lane",
		slog.Int("rows", len(buf)),
		slog.Int("split_at", mid),
		slog.String("err", classifyApplierError(rawErr).Error()))
	if e := m.applyLaneBatch(ctx, ctrl, buf[:mid]); e != nil {
		return e
	}
	return m.applyLaneBatch(ctx, ctrl, buf[mid:])
}

// commitObserve runs one commit attempt of buf and feeds the per-lane AIMD
// controller the per-transaction latency + the ENGINE-CLASSIFIED error
// (only the classified wrapper carries the TransactionKilled() / Retriable()
// surfaces a raw *MySQLError lacks — observing the classified error is what
// drives the tx-killer multiplicative decrease). Returns the RAW commit
// error so the caller's retriable/split decision inspects the original.
func (m *concurrentApplyManager) commitObserve(ctx context.Context, ctrl ir.BatchSizeController, buf []laneChange) error {
	start := time.Now()
	rawErr := m.commitBatchFn(ctx, buf)
	if ctrl != nil {
		ctrl.ObserveBatch(ctx, time.Since(start), len(buf), classifyApplierError(rawErr))
	}
	return rawErr
}

// commitLaneBatch dispatches every change in buf onto a single lane
// transaction and commits it. Redaction + shard-stamp happen FIRST, in the
// SAME order the serial RunOneBatch path uses, so value encoding is
// byte-identical; the lane writes NO position (the coordinator's frontier
// owns it). Returns the raw (unclassified) error so the caller's retry
// predicate can inspect it; on any failure the tx is rolled back.
//
// The *sql.Tx is rolled back on every error path and committed via
// commitWithTimeout on success; sqlclosecheck can't track that
// commit-or-rollback discipline across the dispatch loop, so it's suppressed.
//
//nolint:sqlclosecheck
func (m *concurrentApplyManager) commitLaneBatch(ctx context.Context, buf []laneChange) error {
	tx, err := m.laneDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: applier: lane begin tx: %w", err)
	}
	for _, e := range buf {
		if err := m.a.redactChange(ctx, e.change); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("mysql: applier: redact: %w", err)
		}
		m.a.stampShardChange(e.change)
		if err := m.a.dispatch(ctx, tx, m.streamID, e.change); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	// Test seam: force a commit-path failure deterministically (the lane
	// analogue of the serial path's removed pipelineTestCommitHook). nil in
	// production. Returns the error to take the same rollback+retry path a
	// real commit failure would.
	if m.laneCommitHook != nil {
		if herr := m.laneCommitHook(buf); herr != nil {
			_ = tx.Rollback()
			return herr
		}
	}
	if err := m.a.commitWithTimeout(tx); err != nil {
		return fmt.Errorf("mysql: applier: lane commit: %w", err)
	}
	return nil
}

// resetLaneIdleTimer re-arms the idle-flush timer using the
// stop-drain-reset idiom (same as appliershared.resetIdleTimer): a stale
// tick is drained so the reset arms a clean grace window rather than
// firing instantly on the next read.
func resetLaneIdleTimer(idle *time.Timer) {
	if !idle.Stop() {
		select {
		case <-idle.C:
		default:
		}
	}
	idle.Reset(defaultIdleFlushPeriod)
}

// isApplierErrorRetriable reports whether the raw lane error is one the
// ADR-0038 streamer retry loop would treat as transient. It REUSES the
// engine's single source of truth — [classifyApplierError] — rather than
// re-deriving the MySQL/Vitess transient set: classify the raw error, then
// check the same [ir.RetriableError] surface the streamer's
// classifyRetriable inspects. A Vitess tx-killer abort is retriable here
// (it satisfies RetriableError), which is exactly what makes the in-lane
// shrink-and-retry converge instead of dropping the run.
func isApplierErrorRetriable(err error) bool {
	var re ir.RetriableError
	return errors.As(classifyApplierError(err), &re) && re.Retriable()
}

// maybeCheckpoint persists a checkpoint once enough changes have been
// routed since the last one. Called on the coordinator goroutine only, so
// all position writes are serialized (no race on the sluice_cdc_state row).
func (m *concurrentApplyManager) maybeCheckpoint(ctx context.Context) error {
	if m.sinceCheckpoint < checkpointEveryChanges {
		return nil
	}
	m.sinceCheckpoint = 0
	return m.writeCheckpoint(ctx)
}

// writeCheckpoint persists the highest source-tx-boundary position that is
// durable across all lanes (the contiguous frontier), in its own
// transaction on the coordinator's primary pool. It is a no-op when no new
// boundary is durable, or when the boundary equals the last one written
// (idempotent / monotone). This is the ADR-0104 position relaxation: the
// persisted position lags the durable data but can never lead it.
func (m *concurrentApplyManager) writeCheckpoint(ctx context.Context) error {
	pos, seq, ok := m.frontier.checkpointPosition()
	// Seq-monotone guard: never write a boundary at or below the last one
	// persisted (prevents regression below a barrier's direct applyOne
	// write, and skips redundant re-writes of the same point).
	if !ok || seq <= m.lastWrittenSeq {
		return nil
	}
	posCtx, cancel := m.a.execTimeoutCtx(ctx)
	defer cancel()
	tx, err := m.a.db.BeginTx(posCtx, nil)
	if err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: checkpoint begin: %w", err))
	}
	if err := writePositionTx(posCtx, tx, m.streamID, pos.Token, m.a.slotName, m.a.sourceFingerprint, m.a.targetSchema); err != nil {
		_ = tx.Rollback()
		return classifyApplierError(fmt.Errorf("mysql: applier: checkpoint position write: %w", err))
	}
	if err := m.a.commitWithTimeout(tx); err != nil {
		return classifyApplierError(fmt.Errorf("mysql: applier: checkpoint commit: %w", err))
	}
	m.lastWrittenSeq = seq
	m.lastWrittenTok = pos.Token
	return nil
}

func (m *concurrentApplyManager) recordErr(err error) {
	m.errMu.Lock()
	defer m.errMu.Unlock()
	if m.firstErr == nil {
		m.firstErr = err
	}
}

func (m *concurrentApplyManager) getErr() error {
	m.errMu.Lock()
	defer m.errMu.Unlock()
	return m.firstErr
}

// rowChangeSchemaTable returns the source schema + table of a row-bearing
// change (Insert/Update/Delete). Barrier-class events never reach here.
func rowChangeSchemaTable(c ir.Change) (schema, table string) {
	switch v := c.(type) {
	case ir.Insert:
		return v.Schema, v.Table
	case ir.Update:
		return v.Schema, v.Table
	case ir.Delete:
		return v.Schema, v.Table
	}
	return "", ""
}

// pkChangedUpdate reports whether an Update changes any primary-key column
// value (Before vs After). A nil Before image (source without before-rows)
// cannot be compared, so it returns false (route by the After key). Such
// PK-changing updates are rare; the caller routes them through the barrier
// path so the old-key and new-key effects stay globally ordered.
func pkChangedUpdate(u ir.Update, pkCols []string) bool {
	if u.Before == nil || u.After == nil {
		return false
	}
	for _, col := range pkCols {
		b, bok := u.Before[col]
		a, aok := u.After[col]
		if bok != aok || !valuesEqualForKey(b, a) {
			return true
		}
	}
	return false
}

// valuesEqualForKey compares two primary-key values for the PK-change
// check. Byte slices ([]byte keys) need content comparison; everything else
// is a comparable scalar the decode path produces, so == is correct.
func valuesEqualForKey(a, b any) bool {
	ab, aIsBytes := a.([]byte)
	bb, bIsBytes := b.([]byte)
	if aIsBytes || bIsBytes {
		if !aIsBytes || !bIsBytes {
			return false
		}
		return bytes.Equal(ab, bb)
	}
	return a == b
}
