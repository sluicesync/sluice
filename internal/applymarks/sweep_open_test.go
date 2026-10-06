// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"slices"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTracker_SweepSparesAnOpenTransaction pins the restart sweep against a
// fold that is the run's first close (review of ADR-0190 amendment E, F2).
// Run 1 left a durable mark of barrier B (transaction txB); run 2 delivers
// txA first, so B's loaded mark is untrusted and B re-applies, writing a new
// mark. The fold in B's own target transaction closes txA — the run's first
// close — and its gc plan must still UPSERT B's new mark and must not delete
// txB's marks: txB is open (re-delivered right now), not stale. The control
// arm is the shape the sweep exists for: a loaded mark of a transaction this
// run never decided is still closed and deleted at the first close.
func TestTracker_SweepSparesAnOpenTransaction(t *testing.T) {
	barrierB := ins("keyless", 1, txB, ir.Row{"v": "b"})
	tr := loaded(t, markFor(t, barrierB, keyless))
	if _, err := tr.Decide(ins("keyless", 1, txA, ir.Row{"v": "a"}), keyless); err != nil { // txA delivered first
		t.Fatal(err)
	}
	d, err := tr.Decide(barrierB, keyless)
	if err != nil {
		t.Fatal(err)
	}
	if d.Skip {
		t.Fatal("B was skipped on its own loaded mark although txA was delivered first: the untrusted path this pin is about was not reached")
	}
	var p Pending
	p.Add(d.Marks)
	tr.CloseTxs([]string{txA}) // the fold's close: the run's first, so it sweeps
	pl := tr.Plan(&p, true)
	if len(pl.Upserts) != 1 || pl.Upserts[0].TxID != txB {
		t.Errorf("the fold's plan upserts %v; want B's own new mark (%s) — the sweep closed the open transaction and dropped it", pl.Upserts, txB)
	}
	if slices.Contains(pl.Deletes, txB) {
		t.Errorf("the fold's plan deletes %v, which includes the open transaction %s: its marks would vanish in the commit that re-applies it", pl.Deletes, txB)
	}

	// Control: nothing re-delivers txB, so its loaded mark is stale and the
	// first close deletes it.
	stale := loaded(t, markFor(t, barrierB, keyless))
	if _, err := stale.Decide(ins("keyless", 1, txA, ir.Row{"v": "a"}), keyless); err != nil {
		t.Fatal(err)
	}
	stale.CloseTxs([]string{txA})
	if pl := stale.Plan(nil, true); !slices.Contains(pl.Deletes, txB) {
		t.Errorf("the first close kept a loaded mark of %s that no transaction of the run re-delivered (deletes %v): the sweep no longer sweeps", txB, pl.Deletes)
	}
}
