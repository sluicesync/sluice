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
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"sync"

	"sluicesync.dev/sluice/internal/appliershared"
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

	laneIn  []chan ir.Change // per-lane change feed (coordinator → lane)
	laneSeq []chan uint64    // per-lane seq FIFO, pushed before each change

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
}

// applyBatchConcurrent is the ADR-0104 concurrent key-hash apply entry,
// invoked from ApplyBatch when an operator wires --apply-pipeline-depth
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
		a:            a,
		streamID:     streamID,
		maxBatchSize: maxBatchSize,
		lanes:        lanes,
		laneDB:       laneDB,
		router:       newLaneRouter(lanes),
		frontier:     newCheckpointFrontier(),
		laneIn:       make([]chan ir.Change, lanes),
		laneSeq:      make([]chan uint64, lanes),
	}
	// Buffer each lane a batch's worth so the coordinator's routing isn't
	// gated on a lane's per-change commit latency (the whole point — lanes
	// overlap their cross-region commit RTTs). The seq FIFO matches.
	buf := maxBatchSize
	if buf < 1 {
		buf = 1
	}
	for i := range m.laneIn {
		m.laneIn[i] = make(chan ir.Change, buf)
		m.laneSeq[i] = make(chan uint64, buf)
	}
	return m.run(ctx, changes)
}

func (m *concurrentApplyManager) run(ctx context.Context, changes <-chan ir.Change) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	defer cancel()

	m.wg.Add(m.lanes)
	for i := 0; i < m.lanes; i++ {
		go m.laneLoop(ctx, i)
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
	// Push the seq BEFORE the change so the lane, after committing N
	// changes, always finds N seqs waiting (the FIFO-alignment invariant).
	select {
	case m.laneSeq[lane] <- seq:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case m.laneIn[lane] <- c:
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

// laneLoop runs one lane: repeatedly apply a batch of its routed changes
// (reusing the shared batch loop, so AIMD-free static batching + byte cap +
// the Bug-56 watchdog all apply) and, after each successful commit, pop the
// committed changes' sequences and advance the frontier. A lane writes NO
// position (WritePosition is a no-op in laneBatchConfig); the coordinator
// owns the merged position. Any error stops the whole run.
func (m *concurrentApplyManager) laneLoop(ctx context.Context, i int) {
	defer m.wg.Done()
	cfg := m.a.laneBatchConfig(m.laneDB)
	for {
		n, _, closed, err := appliershared.RunOneBatch(ctx, cfg, m.streamID, m.laneIn[i], m.maxBatchSize)
		if err != nil {
			m.recordErr(err)
			m.cancel()
			return
		}
		for k := 0; k < n; k++ {
			select {
			case seq := <-m.laneSeq[i]:
				m.frontier.markCommitted(seq)
			case <-ctx.Done():
				return
			}
		}
		if closed {
			return
		}
	}
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

// laneBatchConfig builds the shared-batch-loop seam for one lane: serial
// in-order dispatch+commit on the lane's dedicated pool, the SAME builders
// / codec / redaction / shard-stamp / keyless-guard the serial path uses,
// but with NO position write (the coordinator owns the merged position) and
// NO AIMD observer (lanes use static batch sizing in this first cut; per-
// lane AIMD + tx-killer convergence is a tracked follow-up — a tx-killer
// abort on a lane currently stops the run and warm-resume recovers).
func (a *ChangeApplier) laneBatchConfig(laneDB *sql.DB) *appliershared.BatchConfig {
	byteCap := a.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}
	return &appliershared.BatchConfig{
		EngineName:       "mysql",
		TransactionalDDL: false,
		ByteCap:          byteCap,
		BeginTx: func(ctx context.Context) (appliershared.BatchTx, error) {
			tx, err := laneDB.BeginTx(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("mysql: applier: lane begin tx: %w", err)
			}
			return tx, nil
		},
		Dispatch: func(ctx context.Context, tx appliershared.BatchTx, streamID string, c ir.Change) error {
			return a.dispatch(ctx, tx.(*sql.Tx), streamID, c)
		},
		ApplyOne:   a.applyOne,
		Redact:     a.redactChange,
		StampShard: a.stampShardChange,
		Classify:   classifyApplierError,
		// Lanes write NO position — the coordinator's frontier checkpoint
		// owns the merged []shardGtid resume point (ADR-0104 relaxation).
		WritePosition: func(_ context.Context, _ appliershared.BatchTx, _, _ string) error { return nil },
		Commit:        func(tx appliershared.BatchTx) error { return a.commitWithTimeout(tx.(*sql.Tx)) },
		// Inert on lanes (keyless changes take the barrier path, never a
		// lane), wired for defensive parity with the serial config.
		IsKeylessTable: a.isKeylessInsert,
	}
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
