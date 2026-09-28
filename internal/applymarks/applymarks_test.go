// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package applymarks

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The subjects the tests decide against: a PK-only table (idempotent class),
// a table with a secondary unique index, and a keyless table.
var (
	pkOnly    = Subject{Table: "app.pk_only", PK: []string{"id"}}
	uniq      = Subject{Table: "app.uniq", PK: []string{"id"}, SecondaryUnique: true}
	keyless   = Subject{Table: "app.keyless"}
	txA       = "gtid:aaaa:1"
	txB       = "gtid:bbbb:2"
	testScope = "scope-1"
)

func ins(table string, seq uint64, tx string, row ir.Row) ir.Insert {
	return ir.Insert{Schema: "app", Table: table, Row: row, ApplyID: ir.ApplyID{TxID: tx, Seq: seq}}
}

func upd(table string, seq uint64, tx string, before, after ir.Row) ir.Update {
	return ir.Update{Schema: "app", Table: table, Before: before, After: after, ApplyID: ir.ApplyID{TxID: tx, Seq: seq}}
}

func del(table string, seq uint64, tx string, before ir.Row) ir.Delete {
	return ir.Delete{Schema: "app", Table: table, Before: before, ApplyID: ir.ApplyID{TxID: tx, Seq: seq}}
}

func loaded(t *testing.T, marks ...Mark) *Tracker {
	t.Helper()
	tr := &Tracker{}
	tr.Load("s1", testScope, marks)
	return tr
}

// markFor is the mark the tracker itself would write for c.
func markFor(t *testing.T, c ir.Change, s Subject) Mark {
	t.Helper()
	d, err := loaded(t).Decide(c, s)
	if err != nil || len(d.Marks) == 0 {
		t.Fatalf("expected %T to write a mark on %s: marks %v, err %v", c, s.Table, d.Marks, err)
	}
	return d.Marks[0]
}

func TestDecide_ZeroIdentityNeverSkipsOrMarks(t *testing.T) {
	c := ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"})
	m := markFor(t, c, uniq)
	tr := loaded(t, m)
	c.ApplyID = ir.ApplyID{}
	d, err := tr.Decide(c, uniq)
	if err != nil || d.Skip || len(d.Marks) != 0 {
		t.Fatalf("a change with no identity must apply and write no mark; got %+v, %v", d, err)
	}
}

func TestDecide_DisabledTrackerIsANoOp(t *testing.T) {
	c := ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"})
	tr := loaded(t, markFor(t, c, uniq))
	tr.Disable()
	d, err := tr.Decide(c, uniq)
	if err != nil || d.Skip || len(d.Marks) != 0 {
		t.Fatalf("a disabled tracker must apply everything and mark nothing; got %+v, %v", d, err)
	}
	var zero Tracker
	if d, _ := zero.Decide(c, uniq); d.Skip || len(d.Marks) != 0 {
		t.Fatalf("the zero Tracker must be disabled; got %+v", d)
	}
}

// TestDecide_MarkedClasses pins operator decision 1: only the non-idempotent
// classes write marks — a table with a secondary unique index, a
// primary-key change (both keys), a keyless table — and a PK-only table's
// ordinary change writes none.
func TestDecide_MarkedClasses(t *testing.T) {
	tr := loaded(t)
	row := ir.Row{"id": int64(1), "u": "x", "v": "a"}
	cases := []struct {
		name  string
		c     ir.Change
		s     Subject
		marks int
	}{
		{"pk-only insert", ins("pk_only", 1, txA, row), pkOnly, 0},
		{"pk-only update", upd("pk_only", 2, txA, ir.Row{"id": int64(1)}, row), pkOnly, 0},
		{"pk-only delete", del("pk_only", 3, txA, ir.Row{"id": int64(1)}), pkOnly, 0},
		{"secondary-unique insert", ins("uniq", 1, txA, row), uniq, 1},
		{"secondary-unique update", upd("uniq", 2, txA, ir.Row{"id": int64(1)}, row), uniq, 1},
		{"secondary-unique delete", del("uniq", 3, txA, ir.Row{"id": int64(1)}), uniq, 1},
		{"pk-changing update", upd("pk_only", 4, txA, ir.Row{"id": int64(1)}, ir.Row{"id": int64(2), "v": "a"}), pkOnly, 2},
		{"keyless insert", ins("keyless", 1, txA, ir.Row{"v": "a"}), keyless, 1},
		{"keyless delete", del("keyless", 2, txA, ir.Row{"v": "a"}), keyless, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := tr.Decide(tc.c, tc.s)
			if err != nil || d.Skip {
				t.Fatalf("unexpected %+v, %v", d, err)
			}
			if len(d.Marks) != tc.marks {
				t.Fatalf("marks = %d, want %d: %+v", len(d.Marks), tc.marks, d.Marks)
			}
			for _, m := range d.Marks {
				if m.TxID != txA || m.Seq != ir.ApplyIDOf(tc.c).Seq || m.Table != tc.s.Table || m.ScopeDigest != testScope {
					t.Errorf("mark does not name the change: %+v", m)
				}
				if (len(tc.s.PK) == 0) != (m.KeyDigest == "") {
					t.Errorf("keyless tables mark the table (empty key), keyed ones a key; got %q", m.KeyDigest)
				}
			}
		})
	}
	if pk := cases[6]; true {
		d, _ := tr.Decide(pk.c, pk.s)
		if d.Marks[0].KeyDigest == d.Marks[1].KeyDigest {
			t.Fatal("a primary-key change must mark its before-key AND its after-key")
		}
	}
}

// TestDecide_SkipRule pins the same-transaction skip rule on every relation
// between a replayed change and the mark on its key.
func TestDecide_SkipRule(t *testing.T) {
	row := func(u string) ir.Row { return ir.Row{"id": int64(7), "u": u} }
	mark := markFor(t, ins("uniq", 5, txA, row("x")), uniq)
	cases := []struct {
		name string
		c    ir.Change
		skip bool
	}{
		{"earlier ordinal of the marking transaction", ins("uniq", 3, txA, row("w")), true},
		{"the marked change itself", ins("uniq", 5, txA, row("x")), true},
		{"later ordinal of the marking transaction", ins("uniq", 6, txA, row("y")), false},
		{"another transaction, lower ordinal", ins("uniq", 3, txB, row("w")), false},
		{"another transaction, higher ordinal", ins("uniq", 9, txB, row("w")), false},
		{"another key", ins("uniq", 3, txA, ir.Row{"id": int64(8), "u": "x"}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := loaded(t, mark)
			d, err := tr.Decide(tc.c, uniq)
			if err != nil {
				t.Fatal(err)
			}
			if d.Skip != tc.skip {
				t.Fatalf("skip = %v, want %v", d.Skip, tc.skip)
			}
			if d.Skip && len(d.Marks) != 0 {
				t.Fatal("a skipped change writes no mark")
			}
			if tr.Skips(tc.c, uniq) != tc.skip {
				t.Fatal("Skips disagrees with Decide")
			}
		})
	}
}

// TestDecide_TripwireRefusesADifferentChangeAtTheMarkedOrdinal pins the
// tripwire: the same ordinal naming a different change is a refusal, never a
// skip — for every digest-bearing field (operation, key, after-image).
func TestDecide_TripwireRefusesADifferentChangeAtTheMarkedOrdinal(t *testing.T) {
	marked := upd("uniq", 4, txA, ir.Row{"id": int64(7)}, ir.Row{"id": int64(7), "u": "x", "v": "a"})
	mark := markFor(t, marked, uniq)
	for name, replay := range map[string]ir.Change{
		"a different after-image": upd("uniq", 4, txA, ir.Row{"id": int64(7)}, ir.Row{"id": int64(7), "u": "x", "v": "b"}),
		"a different operation":   del("uniq", 4, txA, ir.Row{"id": int64(7)}),
		"an insert of the key":    ins("uniq", 4, txA, ir.Row{"id": int64(7), "u": "x", "v": "a"}),
	} {
		t.Run(name, func(t *testing.T) {
			tr := loaded(t, mark)
			d, err := tr.Decide(replay, uniq)
			if err == nil || d.Skip {
				t.Fatalf("want an %s refusal and no skip; got %+v, %v", MismatchMarker, d, err)
			}
			if !errors.Is(err, ErrMismatch) || !ir.IsTerminal(err) || !strings.Contains(err.Error(), MismatchMarker) {
				t.Fatalf("the refusal must be terminal and carry the marker: %v", err)
			}
			if tr.Skips(replay, uniq) {
				t.Fatal("Skips must answer false on a refusal")
			}
		})
	}
	// …and the identical change still skips.
	if d, err := loaded(t, mark).Decide(marked, uniq); err != nil || !d.Skip {
		t.Fatalf("the marked change itself must skip: %+v, %v", d, err)
	}
}

func TestDecide_ScopeChangeRefuses(t *testing.T) {
	c := ins("uniq", 5, txA, ir.Row{"id": int64(7), "u": "x"})
	m := markFor(t, c, uniq)
	tr := &Tracker{}
	tr.Load("s1", "another-where", []Mark{m})
	d, err := tr.Decide(ins("uniq", 3, txA, ir.Row{"id": int64(7), "u": "w"}), uniq)
	if err == nil || d.Skip || !errors.Is(err, ErrMismatch) || !strings.Contains(err.Error(), "row-filter scope") {
		t.Fatalf("a mark written under another --where must refuse, not skip: %+v, %v", d, err)
	}
	// A mark of ANOTHER transaction under another scope proves nothing and
	// refuses nothing.
	if d, err := tr.Decide(ins("uniq", 3, txB, ir.Row{"id": int64(7), "u": "w"}), uniq); err != nil || d.Skip {
		t.Fatalf("a foreign transaction's mark must be inert: %+v, %v", d, err)
	}
}

// TestDecide_ResurrectionCaseSkipsTheEarlierIdempotentChange is ADR-0190 §2's
// motivating case: on a PK-only table, `UPDATE k1 SET v=5; UPDATE k1→k2`
// fully applied, then replayed — the first update writes no mark of its own
// but the PK change marked k1 at a higher ordinal, so the replay skips it
// instead of re-creating k1.
func TestDecide_ResurrectionCaseSkipsTheEarlierIdempotentChange(t *testing.T) {
	first := upd("pk_only", 1, txA, ir.Row{"id": int64(1)}, ir.Row{"id": int64(1), "v": "5"})
	move := upd("pk_only", 2, txA, ir.Row{"id": int64(1)}, ir.Row{"id": int64(2), "v": "5"})
	d, err := loaded(t).Decide(move, pkOnly)
	if err != nil || len(d.Marks) != 2 {
		t.Fatalf("the PK change must mark both keys: %+v, %v", d, err)
	}
	tr := loaded(t, d.Marks...)
	if d, err := tr.Decide(first, pkOnly); err != nil || !d.Skip {
		t.Fatalf("the earlier idempotent update of k1 must skip on the PK change's k1 mark: %+v, %v", d, err)
	}
	if d, err := tr.Decide(move, pkOnly); err != nil || !d.Skip {
		t.Fatalf("the PK change itself must skip: %+v, %v", d, err)
	}
	after := upd("pk_only", 3, txA, ir.Row{"id": int64(2)}, ir.Row{"id": int64(2), "v": "7"})
	if d, err := tr.Decide(after, pkOnly); err != nil || d.Skip {
		t.Fatalf("a later change of the new key must apply: %+v, %v", d, err)
	}
}

func TestDecide_UncomputableKeyAppliesAndMarksNothing(t *testing.T) {
	c := ins("uniq", 5, txA, ir.Row{"u": "x"}) // the primary-key column is absent
	d, err := loaded(t).Decide(c, uniq)
	if err != nil || d.Skip || len(d.Marks) != 0 {
		t.Fatalf("a change whose key cannot be computed must apply and mark nothing: %+v, %v", d, err)
	}
}

// TestDecide_PKOnlyTableWithNothingOnRecordTakesTheFastPath pins the cost
// claim: an idempotent change to a table with no loaded mark touches no lock
// and records no open transaction.
func TestDecide_PKOnlyTableWithNothingOnRecordTakesTheFastPath(t *testing.T) {
	tr := loaded(t)
	if d, err := tr.Decide(ins("pk_only", 1, txA, ir.Row{"id": int64(1)}), pkOnly); err != nil || d.Skip || len(d.Marks) != 0 {
		t.Fatalf("unexpected %+v, %v", d, err)
	}
	if len(tr.open) != 0 {
		t.Fatalf("the fast path must not record the transaction: open = %v", tr.open)
	}
}

func TestPlanAndGC(t *testing.T) {
	a1 := markFor(t, ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"}), uniq)
	b1 := markFor(t, ins("uniq", 1, txB, ir.Row{"id": int64(2), "u": "y"}), uniq)

	tr := loaded(t)
	var p Pending
	p.Add([]Mark{a1})
	// Mid-transaction flush: the mark is written, nothing is deleted.
	pl := tr.Plan(&p, false)
	if !reflect.DeepEqual(pl.Upserts, []Mark{a1}) || len(pl.Deletes) != 0 {
		t.Fatalf("mid-transaction plan = %+v", pl)
	}
	tr.Committed(pl)
	if !tr.dirty[txA] {
		t.Fatal("a committed mark's transaction must be dirty")
	}

	// txA commits while txB's mark is pending in the same target transaction
	// as the position: txA's marks are deleted, txB's written.
	if _, err := tr.Decide(ins("uniq", 2, txA, ir.Row{"id": int64(1), "u": "z"}), uniq); err != nil {
		t.Fatal(err)
	}
	tr.CloseOpen()
	var p2 Pending
	p2.Add([]Mark{b1})
	pl = tr.Plan(&p2, true)
	if !reflect.DeepEqual(pl.Deletes, []string{txA}) || !reflect.DeepEqual(pl.Upserts, []Mark{b1}) {
		t.Fatalf("gc plan = %+v", pl)
	}
	tr.Committed(pl)
	if tr.dirty[txA] || tr.closed[txA] || !tr.dirty[txB] {
		t.Fatalf("after commit: dirty %v closed %v", tr.dirty, tr.closed)
	}

	// A closed transaction's still-pending marks are dropped, never written:
	// the position in the same transaction passes it.
	var p3 Pending
	p3.Add([]Mark{b1})
	tr.CloseTxs([]string{txB})
	pl = tr.Plan(&p3, true)
	if len(pl.Upserts) != 0 || !reflect.DeepEqual(pl.Deletes, []string{txB}) {
		t.Fatalf("a closed transaction's pending mark must be dropped and its durable ones deleted: %+v", pl)
	}
}

// TestSweep_FirstCloseRetiresEveryLoadedMark pins the restart sweep: the first
// transaction close of a run closes every loaded mark with it (see
// sweepLocked for the invariant), once.
func TestSweep_FirstCloseRetiresEveryLoadedMark(t *testing.T) {
	a1 := markFor(t, ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"}), uniq)
	stale := markFor(t, ins("uniq", 1, "gtid:stale:9", ir.Row{"id": int64(3), "u": "q"}), uniq)
	for name, closeFn := range map[string]func(*Tracker){
		"serial":          func(tr *Tracker) { tr.CloseOpen() },
		"lane checkpoint": func(tr *Tracker) { tr.CloseTxs([]string{txA}) },
	} {
		t.Run(name, func(t *testing.T) {
			tr := loaded(t, a1, stale)
			tr.CloseTxs(nil) // an empty checkpoint closes nothing and sweeps nothing
			if pl := tr.Plan(nil, true); len(pl.Deletes) != 0 {
				t.Fatalf("an empty close must not sweep: %+v", pl)
			}
			closeFn(tr)
			pl := tr.Plan(nil, true)
			if !reflect.DeepEqual(pl.Deletes, []string{txA, "gtid:stale:9"}) {
				t.Fatalf("first close must retire every loaded mark: %+v", pl)
			}
			tr.Committed(pl)
			if len(tr.dirty) != 0 {
				t.Fatalf("dirty after sweep = %v", tr.dirty)
			}
		})
	}
}

func TestPendingCoalescesPerKeyLastWins(t *testing.T) {
	m1 := markFor(t, ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"}), uniq)
	m2 := markFor(t, upd("uniq", 4, txA, ir.Row{"id": int64(1)}, ir.Row{"id": int64(1), "u": "y"}), uniq)
	var p Pending
	p.Add([]Mark{m1})
	p.Add([]Mark{m2})
	if p.Len() != 1 {
		t.Fatalf("one key, one pending mark; got %d", p.Len())
	}
	pl := loaded(t).Plan(&p, false)
	if len(pl.Upserts) != 1 || pl.Upserts[0].Seq != 4 {
		t.Fatalf("the last change to a key must win: %+v", pl.Upserts)
	}
}

func TestTxMarksPlansOnce(t *testing.T) {
	tr := loaded(t)
	var m TxMarks
	m.Add([]Mark{markFor(t, ins("uniq", 1, txA, ir.Row{"id": int64(1), "u": "x"}), uniq)})
	pl, first := m.Plan(tr, true)
	if !first || len(pl.Upserts) != 1 {
		t.Fatalf("first plan = %+v, %v", pl, first)
	}
	if _, again := m.Plan(tr, false); again {
		t.Fatal("a transaction's marks are planned once")
	}
	m.Committed(tr)
	if !tr.dirty[txA] {
		t.Fatal("Committed must retire the executed plan")
	}
}

func TestKeyDigest(t *testing.T) {
	pk := []string{"a", "b"}
	d1, ok1 := KeyDigest(ir.Row{"a": int64(5), "b": "x", "c": 1}, pk)
	d2, ok2 := KeyDigest(ir.Row{"a": uint64(5), "b": []byte("x")}, pk)
	if !ok1 || !ok2 || d1 != d2 {
		t.Fatal("the key digest is canonical by VALUE, like the lane router (int64 5 ≡ uint64 5, string ≡ []byte)")
	}
	if d3, _ := KeyDigest(ir.Row{"a": int64(5), "b": "y"}, pk); d3 == d1 {
		t.Fatal("different keys must digest differently")
	}
	if _, ok := KeyDigest(ir.Row{"a": int64(5)}, pk); ok {
		t.Fatal("an absent key column makes the key uncomputable")
	}
	if _, ok := KeyDigest(nil, pk); ok {
		t.Fatal("a nil image has no key")
	}
}

// TestChangeDigest_ValueFamilyMatrix pins the tripwire's codec over every
// family of the IR value contract (docs/value-types.md): for each, two
// independently constructed equal values digest alike (a re-delivery decodes
// fresh values) and a different value digests differently (a renumbered
// change must not pass the tripwire).
func TestChangeDigest_ValueFamilyMatrix(t *testing.T) {
	ts := time.Date(2026, 9, 28, 1, 2, 3, 456789000, time.UTC)
	families := []struct {
		name       string
		same, diff func() any
	}{
		{"nil vs value", func() any { return nil }, func() any { return "" }},
		{"bool", func() any { return true }, func() any { return false }},
		{"int64", func() any { return int64(-42) }, func() any { return int64(42) }},
		{"uint64", func() any { return uint64(1 << 63) }, func() any { return uint64(1<<63 + 1) }},
		{"float64", func() any { return 1.5 }, func() any { return 1.25 }},
		{"decimal string", func() any { return "1.50" }, func() any { return "1.5" }},
		{"text", func() any { return "héllo" }, func() any { return "hello" }},
		{"bytes", func() any { return []byte{0, 1, 2} }, func() any { return []byte{0, 1, 3} }},
		{"time", func() any { return ts }, func() any { return ts.Add(time.Microsecond) }},
		{"json.Number", func() any { return json.Number("12.0") }, func() any { return json.Number("12") }},
		{"array", func() any { return []any{int64(1), "a", nil} }, func() any { return []any{int64(1), "a"} }},
		{"nested array", func() any { return []any{[]any{int64(1)}, []any{int64(2)}} }, func() any { return []any{[]any{int64(1), int64(2)}} }},
		{"set", func() any { return []string{"a", "b"} }, func() any { return []string{"a"} }},
		{"map", func() any { return map[string]any{"k": int64(1), "j": "x"} }, func() any { return map[string]any{"k": int64(2), "j": "x"} }},
	}
	digest := func(v any) string {
		return ChangeDigest(ins("uniq", 1, txA, ir.Row{"id": int64(1), "c": v}), uniq.Table, uniq.PK)
	}
	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			first, second := digest(f.same()), digest(f.same())
			if first != second {
				t.Fatal("equal values must digest alike")
			}
			if first == digest(f.diff()) {
				t.Fatal("different values must digest differently")
			}
		})
	}
	// Column names are part of the image, and adjacent fields cannot run
	// together.
	a := ChangeDigest(ins("uniq", 1, txA, ir.Row{"id": int64(1), "ab": "c"}), uniq.Table, uniq.PK)
	b := ChangeDigest(ins("uniq", 1, txA, ir.Row{"id": int64(1), "a": "bc"}), uniq.Table, uniq.PK)
	if a == b {
		t.Fatal("column/value boundaries must be unambiguous")
	}
}

func TestSequencer(t *testing.T) {
	var s Sequencer
	if id := s.Next("t"); !id.IsZero() {
		t.Fatalf("outside a transaction there is no identity: %+v", id)
	}
	s.Begin("tx1")
	want := []ir.ApplyID{{TxID: "tx1", Seq: 1}, {TxID: "tx1", Seq: 1}, {TxID: "tx1", Seq: 2}}
	got := []ir.ApplyID{s.Next("a"), s.Next("b"), s.Next("a")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("per-table ordinals = %+v, want %+v", got, want)
	}
	s.Begin("tx2")
	if id := s.Next("a"); id != (ir.ApplyID{TxID: "tx2", Seq: 1}) {
		t.Fatalf("a new transaction restarts the ordinals: %+v", id)
	}
	s.End()
	if id := s.Next("a"); !id.IsZero() {
		t.Fatalf("after End there is no identity: %+v", id)
	}
	s.Begin("")
	if id := s.Next("a"); !id.IsZero() {
		t.Fatalf("an unnameable transaction stamps nothing: %+v", id)
	}
}
