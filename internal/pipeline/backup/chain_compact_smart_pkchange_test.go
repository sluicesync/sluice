// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Smart compaction mis-buckets a PK-changing UPDATE, so a compacted backup can
// RESURRECT A DELETED ROW with `backup verify` green (audit 2026-08-05 A-3).
//
// The comment justifying After-key-only bucketing said key-only changes "are
// represented as DELETE+INSERT in MySQL and as a separate event in PG". Both
// halves are contradicted by sluice's own readers at HEAD: the MySQL CDC
// reader emits ONE ir.Update whose narrowed Before carries the OLD key and
// whose After carries the new one (its own comment says so), Postgres emits one
// UpdateMessage with an OldTuple, and laneapply.PKChangedUpdate exists
// precisely because the appliers receive them.
//
// The consequence, on a window of INSERT(1) / UPDATE(1→2) / DELETE(2):
// the INSERT buckets under key 1, the UPDATE and DELETE both bucket under
// key 2 (the After key), so key 2's chain collapses to just the DELETE and
// key 1's chain emits its INSERT untouched. The compacted stream is
// INSERT(1) + DELETE(2) — replaying it leaves row 1 ALIVE where the source has
// no rows at all. And `backup verify` cannot see it, because compaction
// re-stamps the very hashes verify checks.
//
// This is also the shared-evidence shape: the test asserts compacted replay
// converges with UNCOMPACTED replay of the same events, so the expected value
// comes from the source stream rather than from the compactor's own arithmetic.

package backup

import (
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func pkChangeSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}
}

func pkPos(tok string) ir.Position {
	return ir.Position{Engine: "mysql", Token: tok}
}

// replayToRowState applies an event stream the way a restore would and returns
// the surviving rows keyed by primary key. This is the independent oracle:
// the same function is run over the UNCOMPACTED stream to get the expected
// value, so nothing here derives from the compactor.
func replayToRowState(evs []ir.Change) map[string]ir.Row {
	state := map[string]ir.Row{}
	for _, e := range evs {
		switch ev := e.(type) {
		case ir.Insert:
			state[fmt.Sprint(ev.Row["id"])] = ev.Row
		case ir.Update:
			// A PK-changing update moves the row: the old key goes away.
			if ev.Before != nil {
				if ob, ok := ev.Before["id"]; ok {
					if nb, ok2 := ev.After["id"]; !ok2 || fmt.Sprint(ob) != fmt.Sprint(nb) {
						delete(state, fmt.Sprint(ob))
					}
				}
			}
			k := fmt.Sprint(ev.After["id"])
			state[k] = mergeAfterImage(state[k], ev.After)
		case ir.Delete:
			delete(state, fmt.Sprint(ev.Before["id"]))
		}
	}
	return state
}

func runCompactor(t *testing.T, evs []ir.Change) []ir.Change {
	t.Helper()
	s := newSmartCompactor(PKStrategyPK, pkChangeSchema())
	for _, e := range evs {
		if err := s.process(e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	out, _ := finalizeCollected(t, s)
	return out
}

func assertConverges(t *testing.T, name string, source []ir.Change) {
	t.Helper()
	want := replayToRowState(source)
	got := replayToRowState(runCompactor(t, source))

	if len(got) != len(want) {
		t.Errorf("%s: compacted replay leaves %d row(s) %v; uncompacted leaves %d %v.\n\n"+
			"A compacted backup must converge with the events it replaced. Extra rows mean the "+
			"compaction RESURRECTED a row the source deleted — and `backup verify` cannot see it, "+
			"because compaction re-stamps the hashes verify checks.",
			name, len(got), keysOf(got), len(want), keysOf(want))
		return
	}
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("%s: row %s is missing after compacted replay but present uncompacted", name, k)
		}
	}
}

func keysOf(m map[string]ir.Row) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The headline shape: a row inserted, re-keyed, then deleted must not survive.
func TestSmartCompact_PKChangeUpdate_NotMisBucketed(t *testing.T) {
	assertConverges(t, "insert/rekey/delete", []ir.Change{
		ir.Insert{Position: pkPos("1"), Table: "t", Row: ir.Row{"id": int64(1), "body": "a"}},
		ir.Update{
			Position: pkPos("2"), Table: "t",
			Before: ir.Row{"id": int64(1)},
			After:  ir.Row{"id": int64(2), "body": "a"},
		},
		ir.Delete{Position: pkPos("3"), Table: "t", Before: ir.Row{"id": int64(2)}},
	})
}

// The re-keyed row survives when it is not deleted — the control that keeps the
// barrier from simply dropping PK-changing updates.
func TestSmartCompact_PKChangeUpdate_SurvivingRowKeepsNewKey(t *testing.T) {
	assertConverges(t, "insert/rekey", []ir.Change{
		ir.Insert{Position: pkPos("1"), Table: "t", Row: ir.Row{"id": int64(1), "body": "a"}},
		ir.Update{
			Position: pkPos("2"), Table: "t",
			Before: ir.Row{"id": int64(1)},
			After:  ir.Row{"id": int64(2), "body": "b"},
		},
	})
}

// A PK change onto a key that already has a pending chain, and a chain that
// continues after the re-key — the two shapes where a per-key barrier could
// still get the ordering wrong.
func TestSmartCompact_PKChangeUpdate_AdjacentChains(t *testing.T) {
	assertConverges(t, "delete-then-rekey-onto-it", []ir.Change{
		ir.Insert{Position: pkPos("1"), Table: "t", Row: ir.Row{"id": int64(2), "body": "old"}},
		ir.Delete{Position: pkPos("2"), Table: "t", Before: ir.Row{"id": int64(2)}},
		ir.Insert{Position: pkPos("3"), Table: "t", Row: ir.Row{"id": int64(1), "body": "new"}},
		ir.Update{
			Position: pkPos("4"), Table: "t",
			Before: ir.Row{"id": int64(1)},
			After:  ir.Row{"id": int64(2), "body": "new"},
		},
	})

	assertConverges(t, "rekey-then-update-new-key", []ir.Change{
		ir.Insert{Position: pkPos("1"), Table: "t", Row: ir.Row{"id": int64(1), "body": "a"}},
		ir.Update{
			Position: pkPos("2"), Table: "t",
			Before: ir.Row{"id": int64(1)},
			After:  ir.Row{"id": int64(2), "body": "a"},
		},
		ir.Update{
			Position: pkPos("3"), Table: "t",
			Before: ir.Row{"id": int64(2)},
			After:  ir.Row{"id": int64(2), "body": "c"},
		},
	})

	assertConverges(t, "double-rekey", []ir.Change{
		ir.Insert{Position: pkPos("1"), Table: "t", Row: ir.Row{"id": int64(1), "body": "a"}},
		ir.Update{Position: pkPos("2"), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2), "body": "a"}},
		ir.Update{Position: pkPos("3"), Table: "t", Before: ir.Row{"id": int64(2)}, After: ir.Row{"id": int64(3), "body": "a"}},
		ir.Delete{Position: pkPos("4"), Table: "t", Before: ir.Row{"id": int64(3)}},
	})
}

// An UNRELATED row's chain must still collapse across a PK-change barrier —
// otherwise the fix would quietly turn smart compaction off for any window
// containing one.
func TestSmartCompact_PKChangeUpdate_UnrelatedChainsStillCollapse(t *testing.T) {
	evs := []ir.Change{
		ir.Insert{Position: pkPos("1"), Table: "t", Row: ir.Row{"id": int64(9), "body": "a"}},
		ir.Update{Position: pkPos("2"), Table: "t", Before: ir.Row{"id": int64(9)}, After: ir.Row{"id": int64(9), "body": "b"}},
		ir.Insert{Position: pkPos("3"), Table: "t", Row: ir.Row{"id": int64(1), "body": "x"}},
		ir.Update{Position: pkPos("4"), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2), "body": "x"}},
		ir.Update{Position: pkPos("5"), Table: "t", Before: ir.Row{"id": int64(9)}, After: ir.Row{"id": int64(9), "body": "c"}},
	}
	assertConverges(t, "unrelated-collapse", evs)

	out := runCompactor(t, evs)
	// Row 9's three events (INSERT + 2 UPDATEs, no PK change) must still
	// collapse to one.
	nine := 0
	for _, e := range out {
		switch ev := e.(type) {
		case ir.Insert:
			if fmt.Sprint(ev.Row["id"]) == "9" {
				nine++
			}
		case ir.Update:
			if fmt.Sprint(ev.After["id"]) == "9" {
				nine++
			}
		}
	}
	if nine != 1 {
		t.Errorf("row 9's chain emitted %d events; want 1.\n\n"+
			"A PK-change barrier must be scoped to the keys it actually moves — scoping it to the "+
			"whole table would stop unrelated rows collapsing whenever one row is re-keyed.", nine)
	}
}

// B-5: pkValueKey must be INJECTIVE. It renders values with %v and joins with
// \x00, so ("a\x00","b") and ("a","\x00b") produce the SAME key — an INSERT and
// a DELETE of two DIFFERENT rows net to nothing in the compacted chain. The
// collation precondition is real (utf8mb4_bin makes NUL significant).
func TestSmartCompact_PKValueKeyIsInjective(t *testing.T) {
	cases := [][]any{
		{"a\x00", "b"},
		{"a", "\x00b"},
		{"a\x00b"},
		{"a", "b"},
		{"ab", ""},
		{"", "ab"},
		{int64(1), "2"},
		{"1", int64(2)},
	}
	seen := map[string][]any{}
	for _, tuple := range cases {
		cols := make([]string, len(tuple))
		row := ir.Row{}
		for i, v := range tuple {
			cols[i] = fmt.Sprintf("c%d", i)
			row[cols[i]] = v
		}
		key, err := pkValueKey(cols, row, "", "t")
		if err != nil {
			t.Fatalf("pkValueKey(%v): %v", tuple, err)
		}
		if prev, clash := seen[key]; clash {
			t.Errorf("DISTINCT key tuples %v and %v produce the SAME accumulator key %q.\n\n"+
				"Two different rows share one chain, so an INSERT of one and a DELETE of the other "+
				"net to nothing and the compacted backup silently loses a row.", prev, tuple, key)
			continue
		}
		seen[key] = tuple
	}
}
