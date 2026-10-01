// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestWouldMark_AgreesWithDecide pins the route-time question the lane mark
// fence asks against the apply-time answer, for every class and every
// verdict: a change that would write a mark answers true, one the marks prove
// applied (a skip) or of an idempotent class answers false, and one that
// would refuse answers true (fence first; the apply path refuses).
func TestWouldMark_AgreesWithDecide(t *testing.T) {
	uIns := ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"})
	uMark := markFor(t, uIns, uniq)
	kIns := ins("keyless", 1, txA, ir.Row{"k": int64(1)})
	pkChange := upd("pk_only", 2, txA, ir.Row{"id": int64(1), "v": "a"}, ir.Row{"id": int64(2), "v": "a"})

	cases := []struct {
		name  string
		tr    *Tracker
		c     ir.Change
		s     Subject
		want  bool
		skips bool
	}{
		{name: "secondary-unique, nothing on record", tr: loaded(t), c: uIns, s: uniq, want: true},
		{name: "keyless, nothing on record", tr: loaded(t), c: kIns, s: keyless, want: true},
		{name: "pk change, nothing on record", tr: loaded(t), c: pkChange, s: pkOnly, want: true},
		{name: "pk-only, idempotent", tr: loaded(t), c: ins("pk_only", 1, txA, ir.Row{"id": int64(1)}), s: pkOnly, want: false},
		{name: "no identity", tr: loaded(t), c: ir.Insert{Schema: "app", Table: "uniq", Row: ir.Row{"id": int64(1), "u": "x"}}, s: uniq, want: false},
		{name: "the marks prove it applied", tr: loaded(t, uMark), c: uIns, s: uniq, want: false, skips: true},
		{name: "refusal (tripwire)", tr: loaded(t, Mark{
			Table: uMark.Table, KeyDigest: uMark.KeyDigest, TxID: txA, Seq: 1,
			ChangeDigest: "other", ScopeDigest: testScope,
		}), c: uIns, s: uniq, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.tr.WouldMark(tc.c, tc.s); got != tc.want {
				t.Errorf("WouldMark = %v, want %v", got, tc.want)
			}
			if got := tc.tr.Skips(tc.c, tc.s); got != tc.skips {
				t.Errorf("Skips = %v, want %v", got, tc.skips)
			}
			d, err := tc.tr.Decide(tc.c, tc.s)
			if decideMarks := err != nil || len(d.Marks) > 0; decideMarks != tc.want {
				t.Errorf("WouldMark disagrees with Decide: Decide wrote %d marks, err %v", len(d.Marks), err)
			}
		})
	}
	disabled := &Tracker{}
	if disabled.WouldMark(uIns, uniq) {
		t.Error("a disabled tracker must never ask for a fence")
	}
}

// TestLaneFence_AdmitsOnlyWithTicketOrAnchored pins amendment A's fence and
// amendment D's anchored rule on the lane side: nothing before a fence; the
// fenced transaction's marks only; and, until its fold commits, only in the
// batch carrying the fold ticket — then in every batch once it is anchored,
// or at once when the fence opened it anchored.
func TestLaneFence_AdmitsOnlyWithTicketOrAnchored(t *testing.T) {
	var f LaneFence
	a := []Mark{{TxID: txA}, {TxID: txA}}
	if f.Admits(a, "") || f.Admits(a, txA) {
		t.Fatal("the zero fence admitted marks; it must admit nothing until a transaction is fenced")
	}
	if !f.Admits(nil, "") {
		t.Fatal("an empty mark set is trivially admitted")
	}

	// Fenced, not anchored: the fold's batch only.
	f.Open(txA, false)
	if !f.Admits(a, txA) {
		t.Fatal("the fold's own batch was refused the fenced transaction's marks")
	}
	if f.Admits(a, "") {
		t.Fatal("a batch with no fold ticket admitted an unanchored transaction's marks (the anchored rule)")
	}
	if f.Admits(a, txB) {
		t.Fatal("a batch carrying ANOTHER transaction's ticket admitted an unanchored transaction's marks")
	}
	if f.Admits([]Mark{{TxID: txA}, {TxID: txB}}, txA) {
		t.Fatal("a set carrying another transaction's mark was admitted")
	}

	// Anchored by the fold's commit: every batch.
	f.Anchor(txB) // the wrong transaction: no effect
	if f.Admits(a, "") {
		t.Fatal("anchoring another transaction anchored the fenced one")
	}
	f.Anchor(txA)
	if !f.Admits(a, "") {
		t.Fatal("an anchored transaction's marks were refused to a batch without its ticket")
	}

	// The next fence moves on; opened anchored, it admits at once.
	f.Open(txB, true)
	if f.Admits(a, "") || f.Admits(a, txA) {
		t.Fatal("a transaction stayed admitted after the fence moved to the next one")
	}
	if !f.Admits([]Mark{{TxID: txB}}, "") {
		t.Fatal("a transaction fenced already anchored was refused")
	}
	// A late Anchor of the previous transaction must not disturb it.
	f.Anchor(txA)
	if !f.Admits([]Mark{{TxID: txB}}, "") || f.Admits(a, "") {
		t.Fatal("a late Anchor of the previous transaction changed the fence")
	}
}
