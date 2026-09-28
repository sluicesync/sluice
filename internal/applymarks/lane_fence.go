// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"log/slog"
	"sync/atomic"
)

// LaneFence is a lane applier's clearance to write apply marks — ADR-0190
// amendment A (operator-approved 2026-09-28).
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
// predecessor and persists the checkpoint — the barrier path's own prefix —
// so the persisted position sits at the transaction's start and no earlier
// transaction is ever re-delivered with it. Only then does it [LaneFence.Open]
// the transaction. Every later transaction's marked change fences again,
// which drains this one and moves the position past it (deleting its marks
// in the same checkpoint). So marks only ever exist for the first
// transaction after the persisted position — the invariant the restart sweep
// ([Tracker] sweepLocked) rests on — and the same-transaction skip rule is
// all a restart needs.
//
// # Why the lane checks the fence too
//
// The coordinator decides at ROUTE time; the lane decides again at APPLY
// time, from caches that could in principle have moved in between. A lane
// therefore writes a change's marks only when [LaneFence.Admits] them — the
// transaction was fenced — and otherwise drops them. A dropped mark is
// today's behaviour for that change (applied, unmarked: a loud collision on
// a crash at worst), never a skip, so the fallback is safe.
//
// The zero value admits nothing. Safe for concurrent use: the coordinator
// opens, the W lanes read.
type LaneFence struct {
	tx atomic.Pointer[string]
}

// Open clears txID's marks for the lanes. The coordinator calls it after the
// fence's checkpoint persisted.
func (f *LaneFence) Open(txID string) {
	f.tx.Store(&txID)
}

// Admits reports whether ms may be written by a lane: every mark belongs to
// the fenced transaction. An empty ms is trivially admitted.
func (f *LaneFence) Admits(ms []Mark) bool {
	p := f.tx.Load()
	for _, m := range ms {
		if p == nil || m.TxID != *p {
			return false
		}
	}
	return true
}

// Admitted is the lane's mark set for one applied change: ms when the fence
// [LaneFence.Admits] them, nil otherwise. A drop means the route-time and
// apply-time verdicts disagreed; the change then applies unmarked — today's
// behaviour, never a skip — and the drop is logged at DEBUG.
func (f *LaneFence) Admitted(ms []Mark) []Mark {
	if f.Admits(ms) {
		return ms
	}
	slog.Debug("apply-marks: a lane dropped the marks of a transaction the coordinator did not fence; the change applies unmarked",
		slog.String("tx_id", ms[0].TxID), slog.String("table", ms[0].Table))
	return nil
}
