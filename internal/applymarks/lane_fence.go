// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"log/slog"
	"sync/atomic"
)

// LaneFence is a lane applier's clearance to write apply marks — ADR-0190
// amendment A (operator-approved 2026-09-28), with amendment D's anchored
// rule (2026-10-01).
//
// # Why a lane may not simply write its marks
//
// A lane batch can commit the changes of SEVERAL source transactions to one
// key while the checkpoint that passes the earlier ones is still pending. Say
// T1 updates key k (an idempotent upsert) and T2 deletes k, both committed by
// one lane, and the process dies before any checkpoint passes T1. The restart
// re-delivers T1 then T2. k's mark names T2, so T1's update finds a mark of
// ANOTHER transaction — which proves nothing about T1 — and re-applies,
// recreating k; T2's delete then finds its own mark and is skipped. k is left
// on the target though the source deleted it, at exit 0: a loud collision
// turned into a silent divergence.
//
// # The fence
//
// The coordinator fences before the FIRST change of a transaction that
// writes a mark reaches a lane: it drains every lane to that change's
// predecessor, so every earlier transaction is durable, and moves the
// persisted position to the transaction's start — so no earlier transaction
// is ever re-delivered with its marks. Only then does it [LaneFence.Open] the
// transaction. Every later transaction's marked change fences again, which
// drains this one and moves the position past it (deleting its marks in the
// same write). So marks only ever exist for the first transaction after the
// persisted position — the invariant the restart sweep ([Tracker]
// sweepLocked) rests on — and the same-transaction skip rule is all a restart
// needs.
//
// # Anchored (amendment D)
//
// The position move rides the lane batch that carries the transaction's first
// marked change — a fold, written in that batch's own target transaction —
// unless the position already sat at the transaction's start when it was
// fenced (the run's first transaction, or one a barrier already checkpointed).
// Until the fold commits the transaction is not ANCHORED, and only the fold's
// batch may write its marks: any other batch that made them durable first
// would leave them beside the previous transaction's, under a position at that
// transaction's start — marks for two transactions at once. The fold's lane
// calls [LaneFence.Anchor] once its commit lands, before it reports the batch
// committed, and from then on every lane may write them.
//
// # Why the lane checks the fence too
//
// The coordinator decides at ROUTE time (it never routes a later marked
// change of an unanchored transaction to another lane before the fold
// commits); the lane decides again at APPLY time, from caches that could in
// principle have moved in between. A lane therefore writes a change's marks
// only when [LaneFence.Admits] them, and otherwise drops them. A dropped mark
// is the pre-ADR behaviour for that change (applied, unmarked: a loud
// collision on a crash at worst), never a skip, so the fallback is safe.
//
// The zero value admits nothing. Safe for concurrent use: the coordinator
// opens, the fold's lane anchors, the W lanes read.
type LaneFence struct {
	state atomic.Pointer[fenceState]
}

// fenceState is one fenced transaction and whether it is anchored. Replaced,
// never mutated, so a reader sees a consistent pair.
type fenceState struct {
	tx       string
	anchored bool
}

// Open clears txID's marks for the lanes; anchored reports whether the
// persisted position already sits at the transaction's start. The coordinator
// calls it after the fence's drain.
func (f *LaneFence) Open(txID string, anchored bool) {
	f.state.Store(&fenceState{tx: txID, anchored: anchored})
}

// Anchor records that txID's fold committed: the position sits at its start,
// so every lane may now write its marks. A no-op when the fence has moved to
// another transaction (it cannot, while txID's fold is uncommitted — the next
// fence drains it — but the check keeps a late call from anchoring the wrong
// one).
func (f *LaneFence) Anchor(txID string) {
	for {
		cur := f.state.Load()
		if cur == nil || cur.tx != txID || cur.anchored {
			return
		}
		if f.state.CompareAndSwap(cur, &fenceState{tx: txID, anchored: true}) {
			return
		}
	}
}

// Admits reports whether ms may be written by a lane batch whose fold ticket
// names foldTx ("" for a batch carrying none): every mark belongs to the
// fenced transaction, and that transaction is anchored or this batch is its
// fold. An empty ms is trivially admitted.
func (f *LaneFence) Admits(ms []Mark, foldTx string) bool {
	p := f.state.Load()
	for _, m := range ms {
		if p == nil || m.TxID != p.tx || (!p.anchored && foldTx != p.tx) {
			return false
		}
	}
	return true
}

// Admitted is the lane's mark set for one applied change in a batch whose
// fold ticket names foldTx: ms when the fence [LaneFence.Admits] them, nil
// otherwise. A drop means the route-time and apply-time verdicts disagreed;
// the change then applies unmarked — the pre-ADR behaviour, never a skip —
// and the drop is logged at DEBUG.
func (f *LaneFence) Admitted(ms []Mark, foldTx string) []Mark {
	if f.Admits(ms, foldTx) {
		return ms
	}
	slog.Debug("apply-marks: a lane dropped the marks of a transaction that was not fenced, or not yet anchored; the change applies unmarked",
		slog.String("tx_id", ms[0].TxID), slog.String("table", ms[0].Table))
	return nil
}
