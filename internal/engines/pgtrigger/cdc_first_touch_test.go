// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func mustPos(t *testing.T, id int64) ir.Position {
	t.Helper()
	p, err := encodePos(pgTriggerPos{LastID: id})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func posID(t *testing.T, p ir.Position) int64 {
	t.Helper()
	d, ok, err := decodePos(p)
	if err != nil || !ok {
		t.Fatalf("decodePos(%+v) = %v, %v", p, ok, err)
	}
	return d.LastID
}

// TestFirstTouch_PrecedesEachTablesFirstChangeOnce pins the boundary's shape:
// one snapshot per table per session, immediately ahead of that table's first
// change of every kind the reader emits, anchored one id below it, and read
// with the capturing transaction's txid; later changes of a touched table, in
// this batch or the next, get none.
func TestFirstTouch_PrecedesEachTablesFirstChangeOnce(t *testing.T) {
	type read struct {
		table string
		txid  int64
	}
	var reads []read
	f := firstTouch{shape: func(_ context.Context, schema, table string, txid int64) (*ir.Table, error) {
		reads = append(reads, read{schema + "." + table, txid})
		return &ir.Table{Schema: schema, Name: table}, nil
	}}
	batch := []ir.Change{
		ir.Insert{Position: mustPos(t, 7), Schema: "public", Table: "a"},
		ir.Update{Position: mustPos(t, 8), Schema: "public", Table: "a"},
		ir.Delete{Position: mustPos(t, 9), Schema: "public", Table: "b"},
		ir.Truncate{Position: mustPos(t, 10), Schema: "other", Table: "a"},
	}
	got, err := f.precede(context.Background(), batch, []int64{70, 80, 90, 100})
	if err != nil {
		t.Fatal(err)
	}
	type want struct {
		snapshot bool
		table    string
		id       int64
	}
	wants := []want{
		{true, "public.a", 6},
		{false, "public.a", 7},
		{false, "public.a", 8},
		{true, "public.b", 8},
		{false, "public.b", 9},
		{true, "other.a", 9},
		{false, "other.a", 10},
	}
	if len(got) != len(wants) {
		t.Fatalf("got %d changes, want %d: %+v", len(got), len(wants), got)
	}
	for i, w := range wants {
		s, isSnap := got[i].(ir.SchemaSnapshot)
		if isSnap != w.snapshot || got[i].QualifiedName() != w.table || posID(t, got[i].Pos()) != w.id {
			t.Errorf("change %d = %T %s @%d; want snapshot=%v %s @%d", i, got[i], got[i].QualifiedName(), posID(t, got[i].Pos()), w.snapshot, w.table, w.id)
		}
		if isSnap && (s.IR == nil || s.IR.Name != s.Table) {
			t.Errorf("change %d: snapshot IR = %+v; want the shape read for %s", i, s.IR, w.table)
		}
	}
	wantReads := []read{{"public.a", 70}, {"public.b", 90}, {"other.a", 100}}
	if len(reads) != len(wantReads) {
		t.Fatalf("shape reads = %+v; want %+v", reads, wantReads)
	}
	for i := range wantReads {
		if reads[i] != wantReads[i] {
			t.Errorf("shape read %d = %+v; want %+v", i, reads[i], wantReads[i])
		}
	}

	// The next batch of the same session: every table already touched.
	next := []ir.Change{ir.Insert{Position: mustPos(t, 11), Schema: "public", Table: "a"}}
	got, err = f.precede(context.Background(), next, []int64{110})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(reads) != 3 {
		t.Errorf("a touched table's later batch = %+v (%d reads); want it unchanged and unread", got, len(reads))
	}
}

// TestFirstTouch_ShapeFailureSendsNothing: a failed catalog read returns the
// error and none of the batch, and leaves the table untouched so the retried
// session reads it again.
func TestFirstTouch_ShapeFailureSendsNothing(t *testing.T) {
	boom := errors.New("catalog unreachable")
	f := firstTouch{shape: func(context.Context, string, string, int64) (*ir.Table, error) { return nil, boom }}
	got, err := f.precede(context.Background(), []ir.Change{
		ir.Insert{Position: mustPos(t, 3), Schema: "public", Table: "a"},
	}, []int64{1})
	if !errors.Is(err, boom) || got != nil {
		t.Fatalf("precede = %v, %v; want nothing and the read's error", got, err)
	}
	if f.touched["public.a"] {
		t.Error("a table whose shape read failed was marked touched")
	}
	if !strings.Contains(err.Error(), "public.a") {
		t.Errorf("error %q does not name the table", err)
	}
}

// TestFirstTouch_RefusesMisalignedTxidsAndUnbuiltReaders: the two decode-bug
// shapes refuse instead of emitting an unarmed or misread boundary.
func TestFirstTouch_RefusesMisalignedTxidsAndUnbuiltReaders(t *testing.T) {
	ev := []ir.Change{ir.Insert{Position: mustPos(t, 3), Schema: "public", Table: "a"}}
	f := firstTouch{shape: func(context.Context, string, string, int64) (*ir.Table, error) { return &ir.Table{}, nil }}
	if _, err := f.precede(context.Background(), ev, nil); err == nil {
		t.Error("precede accepted 1 change with 0 txids")
	}
	if _, err := (&firstTouch{}).precede(context.Background(), ev, []int64{1}); err == nil {
		t.Error("precede with no shape reader emitted the batch unguarded")
	}
}

func TestAnchorBefore(t *testing.T) {
	got, err := anchorBefore(mustPos(t, 1))
	if err != nil || posID(t, got) != 0 {
		t.Fatalf("anchorBefore(1) = %+v, %v; want id 0", got, err)
	}
	if _, err := anchorBefore(ir.Position{}); err == nil {
		t.Error("anchorBefore accepted the empty position")
	}
	if _, err := anchorBefore(ir.Position{Engine: "postgres", Token: "0/16B3748"}); err == nil {
		t.Error("anchorBefore accepted a foreign position")
	}
}

func TestXidOf(t *testing.T) {
	for _, c := range []struct {
		txid int64
		want uint32
	}{
		{0, 0},
		{-1, 0},
		{745, 745},
		{3<<32 | 745, 745}, // epoch 3: the low word is the xid
		{1<<32 | 0xFFFFFFFF, 0xFFFFFFFF},
	} {
		if got := xidOf(c.txid); got != c.want {
			t.Errorf("xidOf(%d) = %d; want %d", c.txid, got, c.want)
		}
	}
}

func TestPositionAtOrAfter(t *testing.T) {
	var e Engine
	for _, c := range []struct {
		p, anchor int64
		want      bool
	}{
		{5, 5, true}, {6, 5, true}, {4, 5, false}, {0, 0, true}, {10, 9, true},
	} {
		got, err := e.PositionAtOrAfter(mustPos(t, c.p), mustPos(t, c.anchor))
		if err != nil || got != c.want {
			t.Errorf("PositionAtOrAfter(%d, %d) = %v, %v; want %v", c.p, c.anchor, got, err, c.want)
		}
	}
	for name, bad := range map[string]ir.Position{
		"empty":   {},
		"foreign": {Engine: "postgres", Token: `{"last_id":5}`},
		"garbled": {Engine: EngineName, Token: "not json"},
	} {
		if _, err := e.PositionAtOrAfter(bad, mustPos(t, 1)); err == nil {
			t.Errorf("%s p: no error", name)
		}
		if _, err := e.PositionAtOrAfter(mustPos(t, 1), bad); err == nil {
			t.Errorf("%s anchor: no error", name)
		}
	}
}
