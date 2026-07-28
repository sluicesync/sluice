// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 101: a smart-compacted incremental must not END below its
// own recorded EndPosition.
//
// chain_restore.go's F1 tail-truncation backstop asserts that replaying
// an incremental's change chunks reaches the manifest's EndPosition, and
// refuses the chain (SLUICE-E-BACKUP-INCOMPLETE) when it does not. Smart
// compaction rewrites those chunks: it buffers row events into per-PK
// accumulators and flushes them at end-of-incremental, and a collapsed
// event deliberately carries its chain's FIRST position. Flushing after
// the closing TxCommit therefore ended the stream BELOW the closing
// position on the most ordinary shape smart compaction exists for — one
// row touched in more than one transaction inside one incremental — and
// the compacted chain stopped restoring.
//
// These pins are the property the restore path actually needs, stated
// directly: the last position-bearing event of the rewritten stream is
// the incremental's closing position.

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// lastPosOf returns the position of the last position-bearing event, the
// same rule chain_restore.go's `lastApplied` uses.
func lastPosOf(events []ir.Change) ir.Position {
	var last ir.Position
	for _, e := range events {
		if p := e.Pos(); p.Engine != "" || p.Token != "" {
			last = p
		}
	}
	return last
}

// smartTx wraps one row event in its transactional envelope, the shape
// both real CDC readers emit (and, on Postgres, every event of a
// transaction carries that transaction's commit position).
func smartTx(at uint64, row ir.Change) []ir.Change {
	return []ir.Change{ir.TxBegin{Position: pos(at)}, row, ir.TxCommit{Position: pos(at)}}
}

func smartUpdate(at uint64, id int64, from, to string) ir.Change {
	return ir.Update{
		Position: pos(at), Schema: "public", Table: "users",
		Before: ir.Row{"id": id, "name": from},
		After:  ir.Row{"id": id, "name": to},
	}
}

// TestSmart_RewrittenStreamEndsAtTheClosingPosition is the item-101 gate.
// Every shape here collapses something; every shape must still close at
// the incremental's last transaction's position.
func TestSmart_RewrittenStreamEndsAtTheClosingPosition(t *testing.T) {
	cases := []struct {
		name    string
		events  []ir.Change
		closing uint64
		// wantKinds pins the emitted shape so a future change that
		// "fixes" the position by reordering into something else is
		// caught here rather than in an integration suite.
		wantKinds string
	}{
		{
			// The shape that broke the Bug-139 resume-heal chain: one
			// row updated in two transactions. Pre-fix the collapsed
			// UPDATE (carrying tx 1's position) was emitted after tx 2's
			// TxCommit, so the stream ended at 100 while the manifest
			// recorded 200.
			name:      "one row touched in two transactions",
			events:    append(smartTx(100, smartUpdate(100, 1, "v0", "v1")), smartTx(200, smartUpdate(200, 1, "v1", "v2"))...),
			closing:   200,
			wantKinds: "BCBUC",
		},
		{
			// Insertion order is not position order: the row first seen
			// LAST need not be the row touched last.
			name: "two rows, the last-flushed one first touched earliest",
			events: concatChanges(
				smartTx(100, smartUpdate(100, 1, "a0", "a1")),
				smartTx(200, smartUpdate(200, 2, "b0", "b1")),
				smartTx(300, smartUpdate(300, 1, "a1", "a2")),
			),
			closing:   300,
			wantKinds: "BCBCBUUC",
		},
		{
			// INSERT then UPDATE keeps the INSERT's position by policy
			// (ADR-0064 §2) — which is exactly why the closing commit
			// has to be held back rather than the position raised.
			name:      "insert then update in a later transaction",
			events:    append(smartTx(100, ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}}), smartTx(200, smartUpdate(200, 1, "v0", "v1"))...),
			closing:   200,
			wantKinds: "BCBIC",
		},
		{
			// A row that collapses to NOTHING still must not strand the
			// stream below the closing position.
			name: "insert then delete collapses away entirely",
			events: append(
				smartTx(100, ir.Insert{Position: pos(100), Schema: "public", Table: "users", Row: ir.Row{"id": int64(9), "name": "tmp"}}),
				smartTx(200, ir.Delete{Position: pos(200), Schema: "public", Table: "users", Before: ir.Row{"id": int64(9), "name": "tmp"}})...,
			),
			closing:   200,
			wantKinds: "BCBC",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			emitted, _ := runPolicy(t, usersSchema(), c.events)
			if got := kindsOf(emitted); got != c.wantKinds {
				t.Errorf("emitted kinds = %q; want %q", got, c.wantKinds)
			}
			if got := lastPosOf(emitted); got != pos(c.closing) {
				t.Errorf("rewritten stream's last position-bearing event = %+v; want the incremental's closing position %+v — chain restore's F1 backstop refuses a change-list that ends short of EndPosition (roadmap item 101)",
					got, pos(c.closing))
			}
		})
	}
}

// TestSmart_NoTransactionEnvelopeKeepsFlushingAtTheEnd is the
// non-vacuity half: an engine whose CDC reader emits no TxBegin/TxCommit
// has no closing commit to hold back, and must keep the previous shape
// rather than lose its collapsed events.
func TestSmart_NoTransactionEnvelopeKeepsFlushingAtTheEnd(t *testing.T) {
	emitted, res := runPolicy(t, usersSchema(), []ir.Change{
		smartUpdate(100, 1, "v0", "v1"),
		smartUpdate(200, 1, "v1", "v2"),
	})
	if got := kindsOf(emitted); got != "U" {
		t.Fatalf("emitted kinds = %q; want %q (the two updates still collapse to one)", got, "U")
	}
	if res.eventsBefore != 2 || res.eventsAfter != 1 {
		t.Errorf("eventsBefore/After = %d/%d; want 2/1 — the collapse must still happen", res.eventsBefore, res.eventsAfter)
	}
	if got := lastPosOf(emitted); got != pos(100) {
		t.Errorf("collapsed UPDATE position = %+v; want %+v (the policy keeps the chain's FIRST position; this pin exists so a future change to that rule is a deliberate one)", got, pos(100))
	}
}

// concatChanges joins change slices; append-with-spread reads badly past
// two operands.
func concatChanges(groups ...[]ir.Change) []ir.Change {
	var out []ir.Change
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}
