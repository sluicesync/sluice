// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"sort"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// Frontier tracks, across all lanes, which source-sequence numbers have
// durably committed and computes the contiguous frontier: the highest
// sequence S such that EVERY change with sequence ≤ S is committed. Because
// each change is routed to exactly one lane and a lane commits its changes
// in increasing sequence order, "all ≤ S committed" is exactly the
// out-of-order-completion → contiguous-prefix problem, solved here with a
// pending-set + advancing watermark (memory bounded by the in-flight
// window, since lane backpressure prevents unbounded look-ahead).
//
// Separately it records source-transaction boundary positions (the position
// captured at each TxCommit). CheckpointPosition returns the position of the
// highest tx boundary whose sequence ≤ the frontier — the safe resume point
// under the position relaxation documented in laneapply.go.
//
// Concurrency: lanes call MarkCommitted from W goroutines; the router
// goroutine calls RecordTxBoundary; the coordinator calls
// CheckpointPosition. All three take the mutex, so the tracker is the
// single synchronization point for cross-lane frontier state.
type Frontier struct {
	mu sync.Mutex

	frontier uint64          // highest seq with all ≤ it committed
	pending  map[uint64]bool // committed seqs strictly above frontier, awaiting contiguity

	// txBoundaries holds, in ascending sequence order, the (seq, position)
	// of each source-transaction commit not yet superseded by a persisted
	// checkpoint. Pruned as the persisted checkpoint advances.
	txBoundaries []txBoundary

	// notify is closed-and-replaced on every frontier advance so
	// WaitForFrontier can block until a target seq is reached without
	// busy-polling. Guarded by mu.
	notify chan struct{}
}

type txBoundary struct {
	seq uint64
	pos ir.Position
}

// NewFrontier returns an empty Frontier ready for use.
func NewFrontier() *Frontier {
	return &Frontier{
		pending: make(map[uint64]bool),
		notify:  make(chan struct{}),
	}
}

// WaitForFrontier blocks until the contiguous frontier reaches target (all
// changes with seq ≤ target are durably committed across every lane) or ctx
// is cancelled. The coordinator uses it to drain all lanes before a barrier
// event (Truncate / SchemaSnapshot / keyless / PK-changing update) so the
// barrier applies in correct global order relative to the row changes
// around it. Progress is guaranteed by the lanes' idle-flush, so a quiet
// lane still drains within the idle-flush period.
func (f *Frontier) WaitForFrontier(ctx context.Context, target uint64) error {
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

// MarkCommitted records that the change at sequence seq has durably
// committed (called by a lane after its target transaction commits), then
// advances the contiguous frontier across any now-contiguous pending seqs.
// Sequences may arrive out of order across lanes; within a lane they
// arrive in order. Marking a barrier marker's own seq (TxBegin/TxCommit,
// which do no lane work) is the coordinator's responsibility via
// MarkCommitted too, so the frontier can advance past boundary markers.
func (f *Frontier) MarkCommitted(seq uint64) {
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
		// Wake any WaitForFrontier waiters: close the current notify
		// channel and install a fresh one for the next wait cycle.
		close(f.notify)
		f.notify = make(chan struct{})
	}
}

// RecordTxBoundary records the position to persist when the frontier
// reaches a source-transaction commit at sequence seq. Boundaries must be
// recorded in ascending seq order (the router assigns seqs monotonically),
// which keeps txBoundaries sorted for the binary search in
// CheckpointPosition.
func (f *Frontier) RecordTxBoundary(seq uint64, pos ir.Position) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txBoundaries = append(f.txBoundaries, txBoundary{seq: seq, pos: pos})
}

// CheckpointPosition returns the position of the highest source-transaction
// boundary whose sequence is ≤ the current frontier — the newest resume
// point at which every change is durable across all lanes — and ok=true
// when such a boundary exists. It prunes superseded boundaries so memory
// stays bounded. ok=false means no tx boundary has fully committed yet
// (nothing safe to persist); the caller must NOT persist a partial point.
func (f *Frontier) CheckpointPosition() (pos ir.Position, seq uint64, ok bool) {
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

// FrontierSeq returns the current contiguous frontier sequence. Test and
// observability helper; not on the hot path.
func (f *Frontier) FrontierSeq() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frontier
}
