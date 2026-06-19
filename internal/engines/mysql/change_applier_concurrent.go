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
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strconv"
	"sync"

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
}

type txBoundary struct {
	seq uint64
	pos ir.Position
}

func newCheckpointFrontier() *checkpointFrontier {
	return &checkpointFrontier{pending: make(map[uint64]bool)}
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
	for f.pending[f.frontier+1] {
		delete(f.pending, f.frontier+1)
		f.frontier++
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
func (f *checkpointFrontier) checkpointPosition() (ir.Position, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	front := f.frontier
	// Highest boundary with seq ≤ front. txBoundaries is ascending by seq.
	idx := sort.Search(len(f.txBoundaries), func(i int) bool {
		return f.txBoundaries[i].seq > front
	})
	if idx == 0 {
		return ir.Position{}, false
	}
	pos := f.txBoundaries[idx-1].pos
	// Prune every boundary strictly below the chosen one; keep the chosen
	// one so a repeat call with no further progress is idempotent (returns
	// the same position, ok=true) rather than spuriously false.
	if idx-1 > 0 {
		f.txBoundaries = append([]txBoundary(nil), f.txBoundaries[idx-1:]...)
	}
	return pos, true
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
