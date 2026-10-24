// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// intOrderer orders positions whose tokens are decimal integers (a row's
// pre-transaction position, as the window's sources stamp them); "bad"
// cannot be ordered.
type intOrderer struct{}

func (intOrderer) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	a, errA := strconv.Atoi(p.Token)
	b, errB := strconv.Atoi(anchor.Token)
	if errA != nil || errB != nil {
		return false, errors.New("unorderable")
	}
	return a >= b, nil
}

func pos(n int) ir.Position { return ir.Position{Engine: "t", Token: strconv.Itoa(n)} }

// windowOn builds an open window on table "t" keyed by key, copy complete at
// position copyAt.
func windowOn(copyAt int, key ...string) *replayWindows {
	k := map[string]bool{}
	for _, c := range key {
		k[strings.ToLower(c)] = true
	}
	return &replayWindows{orderer: intOrderer{}, byTable: map[string]*replayWindow{
		"t": {AddedTableReplayWindow: ir.AddedTableReplayWindow{Table: "t", CopyPosition: pos(copyAt), Key: key}, key: k},
	}}
}

// TestAddedTableReplayWindow_Rewrite pins the idempotent form of every row
// change kind inside a window, the pass-through outside it, and the
// refusals (ADDED-TABLE-REPLAY-IMAGE-INCOMPLETE, -COPY-POSITION-UNORDERED).
func TestAddedTableReplayWindow_Rewrite(t *testing.T) {
	ctx := context.Background()
	at := time.Unix(1700000000, 0)
	id := ir.ApplyID{TxID: "tx", Seq: 3}
	cases := []struct {
		name string
		in   ir.Change
		want []ir.Change
	}{
		{
			"an INSERT stays an INSERT (every applier upserts it)",
			ir.Insert{Position: pos(5), Table: "t", Row: ir.Row{"id": int64(1), "v": "a"}},
			[]ir.Change{ir.Insert{Position: pos(5), Table: "t", Row: ir.Row{"id": int64(1), "v": "a"}}},
		},
		{
			"an UPDATE that keeps the key is an upsert of its after-image",
			ir.Update{Position: pos(5), Table: "t", Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(1), "v": "b"}, CommitTime: at, ApplyID: id},
			[]ir.Change{ir.Insert{Position: pos(5), Table: "t", Row: ir.Row{"id": int64(1), "v": "b"}, CommitTime: at, ApplyID: id}},
		},
		{
			"an UPDATE with a PK-narrowed before-image (the binlog lane) keeps the key",
			ir.Update{Position: pos(5), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "v": "b"}},
			[]ir.Change{ir.Insert{Position: pos(5), Table: "t", Row: ir.Row{"id": int64(1), "v": "b"}}},
		},
		{
			"a key change is a delete of the old key and an upsert of the new row",
			ir.Update{Position: pos(5), Schema: "s", Table: "t", Before: ir.Row{"id": int64(7), "v": "a"}, After: ir.Row{"id": int64(-7), "v": "kc"}, CommitTime: at, ApplyID: id},
			[]ir.Change{
				ir.Delete{Position: pos(5), Schema: "s", Table: "t", Before: ir.Row{"id": int64(7)}, CommitTime: at},
				ir.Insert{Position: pos(5), Schema: "s", Table: "t", Row: ir.Row{"id": int64(-7), "v": "kc"}, CommitTime: at, ApplyID: id},
			},
		},
		{
			"a DELETE is addressed by the key alone",
			ir.Delete{Position: pos(5), Table: "t", Before: ir.Row{"id": int64(9), "v": "x", "f": float32(1.5)}, ApplyID: id},
			[]ir.Change{ir.Delete{Position: pos(5), Table: "t", Before: ir.Row{"id": int64(9)}, ApplyID: id}},
		},
		{
			"a change past the copy's position is untouched",
			ir.Update{Position: pos(10), Table: "t", Before: ir.Row{"id": int64(7), "v": "a"}, After: ir.Row{"id": int64(-7), "v": "kc"}},
			[]ir.Change{ir.Update{Position: pos(10), Table: "t", Before: ir.Row{"id": int64(7), "v": "a"}, After: ir.Row{"id": int64(-7), "v": "kc"}}},
		},
		{
			"another table is untouched",
			ir.Delete{Position: pos(5), Table: "other", Before: ir.Row{"id": int64(9), "v": "x"}},
			[]ir.Change{ir.Delete{Position: pos(5), Table: "other", Before: ir.Row{"id": int64(9), "v": "x"}}},
		},
		{
			"a change with no position stays in the window",
			ir.Delete{Table: "t", Before: ir.Row{"id": int64(9), "v": "x"}},
			[]ir.Change{ir.Delete{Table: "t", Before: ir.Row{"id": int64(9)}}},
		},
		{
			"a boundary passes through",
			ir.TxCommit{Position: pos(5)},
			[]ir.Change{ir.TxCommit{Position: pos(5)}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := windowOn(10, "id").route(ctx, c.in)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got  %#v\nwant %#v", got, c.want)
			}
		})
	}

	t.Run("the key is matched whatever the image's casing, and composite keys compare every column", func(t *testing.T) {
		w := windowOn(10, "Tenant", "ID")
		same := ir.Update{Position: pos(1), Table: "t", Before: ir.Row{"tenant": "a", "id": []byte{1}}, After: ir.Row{"TENANT": "a", "Id": []byte{1}, "v": "n"}}
		if got, _ := w.route(ctx, same); len(got) != 1 {
			t.Fatalf("an unchanged composite key (bytes by content) was treated as a key change: %#v", got)
		}
		moved := ir.Update{Position: pos(1), Table: "t", Before: ir.Row{"tenant": "a", "id": []byte{1}}, After: ir.Row{"tenant": "b", "id": []byte{1}, "v": "n"}}
		got, _ := w.route(ctx, moved)
		if len(got) != 2 {
			t.Fatalf("a change in one key column was not a key change: %#v", got)
		}
		if del := got[0].(ir.Delete); !reflect.DeepEqual(del.Before, ir.Row{"tenant": "a", "id": []byte{1}}) {
			t.Fatalf("delete addressed %#v", del.Before)
		}
	})
	t.Run("times compare by instant, a different Go type reads as a key change", func(t *testing.T) {
		w := windowOn(10, "at")
		t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		same := ir.Update{Position: pos(1), Table: "t", Before: ir.Row{"at": t0}, After: ir.Row{"at": t0.In(time.FixedZone("x", 3600))}}
		if got, _ := w.route(ctx, same); len(got) != 1 {
			t.Fatalf("one instant in two zones was treated as a key change")
		}
		typed := ir.Update{Position: pos(1), Table: "t", Before: ir.Row{"at": int64(1)}, After: ir.Row{"at": int32(1)}}
		if got, _ := w.route(ctx, typed); len(got) != 2 {
			t.Fatalf("a doubtful comparison was not treated as a key change")
		}
	})
	t.Run("an image without the key refuses", func(t *testing.T) {
		for _, c := range []ir.Change{
			ir.Insert{Position: pos(1), Table: "t", Row: ir.Row{"v": "a"}},
			ir.Update{Position: pos(1), Table: "t", Before: ir.Row{"v": "a"}, After: ir.Row{"id": int64(1)}},
			ir.Update{Position: pos(1), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"v": "b"}},
			ir.Update{Position: pos(1), Table: "t", After: ir.Row{"id": int64(1)}},
			ir.Delete{Position: pos(1), Table: "t"},
		} {
			_, err := windowOn(10, "id").route(ctx, c)
			if err == nil || !strings.Contains(err.Error(), AddedTableReplayImageIncompleteMarker) {
				t.Errorf("%#v: got %v, want %s", c, err, AddedTableReplayImageIncompleteMarker)
			}
		}
	})
	t.Run("a partial after-image (the unchanged-TOAST omission)", func(t *testing.T) {
		w := windowOn(10, "id")
		*w.byTable["t"] = *newReplayWindow(ir.AddedTableReplayWindow{Table: "t", CopyPosition: pos(10), Key: []string{"id"}, Columns: []string{"id", "v", "Big"}})
		kept := ir.Update{Position: pos(5), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "v": "n"}}
		got, err := w.route(ctx, kept)
		if err != nil || !reflect.DeepEqual(got, []ir.Change{kept}) {
			t.Fatalf("a key-preserving partial UPDATE must stay a key-addressed UPDATE of what it carries: %#v, %v", got, err)
		}
		full := ir.Update{Position: pos(5), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "v": "n", "BIG": "x"}}
		if got, _ := w.route(ctx, full); len(got) != 1 || reflect.TypeOf(got[0]) != reflect.TypeOf(ir.Insert{}) {
			t.Fatalf("a complete after-image (any casing) must be an upsert: %#v", got)
		}
		moved := ir.Update{Position: pos(5), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2), "v": "n"}}
		if _, err := w.route(ctx, moved); err == nil || !strings.Contains(err.Error(), AddedTableReplayImageIncompleteMarker) ||
			!strings.Contains(err.Error(), "big") {
			t.Fatalf("a key-changing partial UPDATE must refuse naming the missing column: %v", err)
		}
	})
	t.Run("an unorderable position refuses", func(t *testing.T) {
		_, err := windowOn(10, "id").route(ctx, ir.Insert{Position: ir.Position{Engine: "t", Token: "bad"}, Table: "t", Row: ir.Row{"id": int64(1)}})
		if err == nil || !strings.Contains(err.Error(), AddedTableCopyPositionUnorderedMarker) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("the window stays closed once a change is past the copy", func(t *testing.T) {
		w := windowOn(10, "id")
		_, _ = w.route(ctx, ir.Insert{Position: pos(11), Table: "t", Row: ir.Row{"id": int64(1)}})
		del := ir.Delete{Position: pos(3), Table: "t", Before: ir.Row{"id": int64(1), "v": "x"}}
		if got, _ := w.route(ctx, del); !reflect.DeepEqual(got, []ir.Change{del}) {
			t.Fatalf("a closed window rewrote %#v", got)
		}
	})
}

// replaySim is a target table under one apply semantics: rows by id.
type replaySim struct {
	rows map[int64]string
	// fullRowBefore: an UPDATE/DELETE matches the full before-image (the
	// VStream lane); otherwise the key alone (the binlog lane).
	fullRowBefore bool
}

// apply applies c as the target applier would, returning a wedge error on a
// duplicate key. A non-matching UPDATE/DELETE is the tolerated zero-row miss.
func (s *replaySim) apply(c ir.Change) error {
	id := func(r ir.Row) int64 { return r["id"].(int64) }
	matches := func(before ir.Row) bool {
		v, ok := s.rows[id(before)]
		if !ok {
			return false
		}
		if bv, has := before["v"]; s.fullRowBefore && has {
			return v == bv
		}
		return true
	}
	switch v := c.(type) {
	case ir.Insert:
		s.rows[id(v.Row)] = v.Row["v"].(string)
	case ir.Update:
		if !matches(v.Before) {
			return nil
		}
		from, to := id(v.Before), id(v.After)
		if _, taken := s.rows[to]; taken && to != from {
			return fmt.Errorf("duplicate key %d", to)
		}
		delete(s.rows, from)
		s.rows[to] = v.After["v"].(string)
	case ir.Delete:
		if matches(v.Before) {
			delete(s.rows, id(v.Before))
		}
	}
	return nil
}

// TestAddedTableReplayWindow_ConvergesOnTheMeasuredCells replays each measured
// Bug 333 history onto the target state the add's copy left, once as the
// plain replay (the harm, reproduced) and once through the window (the fix):
// the window must land on the source's final state in every cell, on both
// lanes' before-image shapes. The integration pins drive the same cells
// through real servers; this one isolates the rule.
func TestAddedTableReplayWindow_ConvergesOnTheMeasuredCells(t *testing.T) {
	ins := func(p int, id int64, v string) ir.Change {
		return ir.Insert{Position: pos(p), Table: "t", Row: ir.Row{"id": id, "v": v}}
	}
	upd := func(p int, from, to int64, bv, av string) ir.Change {
		return ir.Update{Position: pos(p), Table: "t", Before: ir.Row{"id": from, "v": bv}, After: ir.Row{"id": to, "v": av}}
	}
	del := func(p int, id int64, bv string) ir.Change {
		return ir.Delete{Position: pos(p), Table: "t", Before: ir.Row{"id": id, "v": bv}}
	}
	cells := []struct {
		name    string
		copy    map[int64]string // what the add's copy left on the target
		history []ir.Change      // every change from the stream's resume position up to the copy's
		source  map[int64]string // the source at the copy's position
		harm    string           // what the plain replay does (on the VStream lane)
	}{
		{
			// VStream order 1: the copy upserted the reuse INSERT and deferred
			// the key change.
			"key change then reuse of the old key, inside the copy (the silent arm)",
			map[int64]string{7: "reuse"},
			[]ir.Change{upd(1, 7, -7, "v1", "kc"), ins(2, 7, "reuse")},
			map[int64]string{7: "reuse", -7: "kc"},
			"loss",
		},
		{
			"key change without reuse",
			map[int64]string{7: "v1"},
			[]ir.Change{upd(1, 7, -7, "v1", "kc")},
			map[int64]string{-7: "kc"},
			"",
		},
		{
			// Created while stopped: the replay resurrects the seed row.
			"table created while stopped, key change before the copy read the row (the wedge)",
			map[int64]string{5000007: "kc"},
			[]ir.Change{ins(1, 7, "v1"), upd(2, 7, 5000007, "v1", "kc")},
			map[int64]string{5000007: "kc"},
			"wedge",
		},
		{
			"order 1, old key reused before the copy read it",
			map[int64]string{7: "reuse", -7: "kc"},
			[]ir.Change{upd(1, 7, -7, "v1", "kc"), ins(2, 7, "reuse")},
			map[int64]string{7: "reuse", -7: "kc"},
			"",
		},
		{
			"delete then reinsert",
			map[int64]string{7: "again"},
			[]ir.Change{del(1, 7, "v1"), ins(2, 7, "again")},
			map[int64]string{7: "again"},
			"",
		},
	}
	for _, c := range cells {
		for _, fullRow := range []bool{true, false} {
			lane := map[bool]string{true: "vstream", false: "binlog"}[fullRow]
			t.Run(c.name+"/"+lane, func(t *testing.T) {
				w := windowOn(100, "id")
				fixed := &replaySim{rows: clone(c.copy), fullRowBefore: fullRow}
				for _, ch := range c.history {
					if !fullRow {
						ch = narrowToID(ch)
					}
					out, err := w.route(context.Background(), ch)
					if err != nil {
						t.Fatal(err)
					}
					for _, o := range out {
						if err := fixed.apply(o); err != nil {
							t.Fatalf("the window's replay wedged: %v", err)
						}
					}
				}
				if !reflect.DeepEqual(fixed.rows, c.source) {
					t.Fatalf("the window's replay left %v, the source holds %v", fixed.rows, c.source)
				}
				plain := &replaySim{rows: clone(c.copy), fullRowBefore: fullRow}
				var wedge error
				for _, ch := range c.history {
					if !fullRow {
						ch = narrowToID(ch)
					}
					if wedge = plain.apply(ch); wedge != nil {
						break
					}
				}
				t.Logf("plain replay: %v (wedge=%v); the source: %v", plain.rows, wedge, c.source)
				if fullRow && c.harm == "loss" && (wedge != nil || len(plain.rows) >= len(c.source)) {
					t.Fatalf("RIG: the plain replay no longer reproduces the measured loss")
				}
			})
		}
	}
}

func clone(m map[int64]string) map[int64]string {
	out := make(map[int64]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// narrowToID is the binlog lane's before-image (Bug 88: PK only).
func narrowToID(c ir.Change) ir.Change {
	switch v := c.(type) {
	case ir.Update:
		v.Before = ir.Row{"id": v.Before["id"]}
		return v
	case ir.Delete:
		v.Before = ir.Row{"id": v.Before["id"]}
		return v
	}
	return c
}

// TestInterceptAddedTableReplay_StoresARefusalAndCloses pins the stage's
// wiring: no window is a verbatim pass-through (the same channel), and a
// refusal reaches the sink and closes the channel, which
// [Streamer.interceptErrorPending] and phaseSettleDispatch then read.
func TestInterceptAddedTableReplay_StoresARefusalAndCloses(t *testing.T) {
	in := make(chan ir.Change, 2)
	var sink atomic.Pointer[error]
	var recv <-chan ir.Change = in
	if out := interceptAddedTableReplay(context.Background(), in, nil, &sink); out != recv {
		t.Fatal("no window must be a verbatim pass-through")
	}
	in <- ir.Insert{Position: pos(1), Table: "t", Row: ir.Row{"v": "no key"}}
	close(in)
	out := interceptAddedTableReplay(context.Background(), in, windowOn(10, "id"), &sink)
	for range out {
		t.Fatal("a refused change was forwarded")
	}
	if p := sink.Load(); p == nil || !strings.Contains((*p).Error(), AddedTableReplayImageIncompleteMarker) {
		t.Fatalf("sink: %v", p)
	}
	s := &Streamer{}
	s.replayWindowErr.Store(sink.Load())
	if !s.interceptErrorPending() {
		t.Fatal("interceptErrorPending does not read the replay window's sink")
	}
}

// TestAddedTableReplayKey pins the key the window records: the primary key,
// else the deterministic non-null unique key the copy's writer upserts on.
func TestAddedTableReplayKey(t *testing.T) {
	col := func(name string, nullable bool) *ir.Column { return &ir.Column{Name: name, Nullable: nullable} }
	idx := func(name string, unique bool, pred string, cols ...string) *ir.Index {
		ic := make([]ir.IndexColumn, len(cols))
		for i, c := range cols {
			ic[i] = ir.IndexColumn{Column: c}
		}
		return &ir.Index{Name: name, Unique: unique, Predicate: pred, Columns: ic}
	}
	pk := &ir.Table{Columns: []*ir.Column{col("a", false), col("b", false)}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "b"}, {Column: "a"}}}}
	if got := addedTableReplayKey(pk); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("pk: %v", got)
	}
	uk := &ir.Table{
		Columns: []*ir.Column{col("a", false), col("b", false), col("c", true), col("d", false)},
		Indexes: []*ir.Index{
			idx("z_two", true, "", "a", "b"),
			idx("nullable", true, "", "c"),
			idx("partial", true, "d > 0", "d"),
			idx("plain", false, "", "d"),
			idx("y_two", true, "", "b", "d"),
		},
	}
	if got := addedTableReplayKey(uk); !reflect.DeepEqual(got, []string{"b", "d"}) {
		t.Fatalf("unique key: %v (want the two-column one with the smaller name, skipping nullable/partial/non-unique)", got)
	}
	if got := addedTableReplayKey(&ir.Table{Columns: []*ir.Column{col("a", true)}}); got != nil {
		t.Fatalf("keyless: %v", got)
	}
}

// windowSourceEngine is a source that declares the replay window.
type windowSourceEngine struct {
	stubEngine
	intOrderer
}

// AddedTableCopyPosition bounds the window one past the snapshot (as the
// Postgres engine does) and refuses a resume position it cannot read.
func (windowSourceEngine) AddedTableCopyPosition(snapshot, resumeFrom ir.Position) (ir.Position, error) {
	n, err := strconv.Atoi(snapshot.Token)
	if err != nil || resumeFrom.Token == "" {
		return ir.Position{}, errors.New("unboundable")
	}
	return pos(n + 1), nil
}

// windowStore is a target applier keeping windows in memory.
type windowStore struct {
	ir.ChangeApplier
	windows   []ir.AddedTableReplayWindow
	prepErr   error
	recordErr error
	reset     []ir.AddedTableReplayWindow
	resume    ir.Position
}

func (w *windowStore) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return w.resume, w.resume.Token != "", nil
}

func (w *windowStore) PrepareAddedTableReplayWindows(context.Context, string) error { return w.prepErr }

func (w *windowStore) RecordAddedTableReplayWindows(_ context.Context, _ string, ws []ir.AddedTableReplayWindow) error {
	if w.recordErr != nil {
		return w.recordErr
	}
	for _, n := range ws {
		kept := make([]ir.AddedTableReplayWindow, 0, len(w.windows)+1)
		for _, o := range w.windows {
			if o.Table != n.Table {
				kept = append(kept, o)
			}
		}
		kept = append(kept, n)
		w.windows = kept
	}
	return nil
}

func (w *windowStore) ResetAddedTableReplayWindows(_ context.Context, _ string, ws []ir.AddedTableReplayWindow) error {
	w.reset, w.windows = ws, append([]ir.AddedTableReplayWindow(nil), ws...)
	return nil
}

func (w *windowStore) ReadAddedTableReplayWindows(context.Context, string) ([]ir.AddedTableReplayWindow, error) {
	return w.windows, nil
}

// TestAddTable_RecordReplayWindow pins every way the add's record can go:
// recorded with the copy's position and key; refused (never skipped) when the
// target cannot keep it, the copy reported no position, the table has no key
// or the write fails; and nothing at all for a source that needs no window.
func TestAddTable_RecordReplayWindow(t *testing.T) {
	ctx := context.Background()
	table := &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{{Name: "id"}, {Name: "big"}, {Name: "g", GeneratedExpr: "id * 2"}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	stream := &ir.SnapshotStream{Position: pos(42)}
	resume := pos(7)

	store := &windowStore{resume: resume}
	a := &AddTable{Source: windowSourceEngine{}, Target: stubEngine{}, StreamID: "s", TableName: "t"}
	if err := a.recordReplayWindow(ctx, store, stream, table); err != nil {
		t.Fatal(err)
	}
	// The engine's bound (one past the snapshot here), the key, and every
	// column but the generated one (no lane's after-image carries it).
	want := []ir.AddedTableReplayWindow{{Table: "t", CopyPosition: pos(43), Key: []string{"id"}, Columns: []string{"id", "big"}}}
	if !reflect.DeepEqual(store.windows, want) {
		t.Fatalf("recorded %+v", store.windows)
	}

	for name, run := range map[string]func() error{
		"a target without the store": func() error { return a.recordReplayWindow(ctx, stubApplier{}, stream, table) },
		"a copy with no position": func() error {
			return a.recordReplayWindow(ctx, &windowStore{resume: resume}, &ir.SnapshotStream{}, table)
		},
		"a stream with no position to resume from": func() error {
			return a.recordReplayWindow(ctx, &windowStore{}, stream, table)
		},
		"a bound the engine cannot compute": func() error {
			return a.recordReplayWindow(ctx, &windowStore{resume: resume}, &ir.SnapshotStream{Position: ir.Position{Engine: "t", Token: "x"}}, table)
		},
		"a keyless table": func() error {
			return a.recordReplayWindow(ctx, &windowStore{resume: resume}, stream, &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "v", Nullable: true}}})
		},
		"a failed write": func() error {
			return a.recordReplayWindow(ctx, &windowStore{resume: resume, recordErr: errors.New("boom")}, stream, table)
		},
		"a target that cannot prepare": func() error { return a.prepareReplayWindow(ctx, &windowStore{prepErr: errors.New("1105")}) },
		"prepare without the store":    func() error { return a.prepareReplayWindow(ctx, stubApplier{}) },
	} {
		if err := run(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}

	plain := &AddTable{Source: stubEngine{}, Target: stubEngine{}, StreamID: "s", TableName: "t"}
	if err := plain.recordReplayWindow(ctx, stubApplier{}, &ir.SnapshotStream{}, table); err != nil {
		t.Fatalf("a source without the capability: %v", err)
	}
	if err := plain.prepareReplayWindow(ctx, stubApplier{}); err != nil {
		t.Fatalf("a source without the capability (prepare): %v", err)
	}
}

// TestStreamer_LoadAddedTableReplayWindows pins the warm resume's load: a
// window the persisted position already covers stays closed, an open one is
// kept, an entry with no window opens nothing, an unorderable pair refuses,
// a cold start loads nothing, and a live-added table with no entry at all is
// reported once (ADDED-TABLE-COPY-POSITION-UNRECORDED) and recorded as
// having none.
func TestStreamer_LoadAddedTableReplayWindows(t *testing.T) {
	ctx := context.Background()
	store := &windowStore{windows: []ir.AddedTableReplayWindow{
		{Table: "open", CopyPosition: pos(50), Key: []string{"id"}},
		{Table: "done", CopyPosition: pos(10), Key: []string{"id"}},
		{Table: "none"},
	}}
	logs := captureSlog(t)

	s := &Streamer{Source: windowSourceEngine{}, startLiveAdded: []string{"open", "done", "none", "legacy_b", "legacy_a"}}
	if err := s.phaseLoadAddedTableReplayWindows(ctx, store, "s", true, pos(20)); err != nil {
		t.Fatal(err)
	}
	open := make([]string, 0, len(s.replayWindows.byTable))
	for name := range s.replayWindows.byTable {
		open = append(open, name)
	}
	if !reflect.DeepEqual(open, []string{"open"}) {
		t.Fatalf("open windows: %v", open)
	}
	if !strings.Contains(logs.String(), AddedTableCopyPositionUnrecordedMarker) || !strings.Contains(logs.String(), "legacy_a, legacy_b") {
		t.Fatalf("no unrecorded report naming the legacy tables:\n%s", logs.String())
	}
	var recorded []string
	for _, w := range store.windows {
		if !w.HasWindow() {
			recorded = append(recorded, w.Table)
		}
	}
	sort.Strings(recorded)
	if !reflect.DeepEqual(recorded, []string{"legacy_a", "legacy_b", "none"}) {
		t.Fatalf("tables recorded with no window: %v", recorded)
	}

	logs.Reset()
	if err := s.phaseLoadAddedTableReplayWindows(ctx, store, "s", true, pos(20)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), AddedTableCopyPositionUnrecordedMarker) {
		t.Fatal("the unrecorded report repeated on the next start")
	}

	if err := s.phaseLoadAddedTableReplayWindows(ctx, store, "s", false, ir.Position{}); err != nil || s.replayWindows != nil {
		t.Fatalf("a cold start loaded windows: %v %v", s.replayWindows, err)
	}
	bad := &windowStore{windows: []ir.AddedTableReplayWindow{{Table: "x", CopyPosition: ir.Position{Engine: "t", Token: "bad"}, Key: []string{"id"}}}}
	if err := s.phaseLoadAddedTableReplayWindows(ctx, bad, "s", true, pos(20)); err == nil ||
		!strings.Contains(err.Error(), AddedTableCopyPositionUnorderedMarker) {
		t.Fatalf("an unorderable window: %v", err)
	}
	plain := &Streamer{Source: stubEngine{}, startLiveAdded: []string{"x"}}
	if err := plain.phaseLoadAddedTableReplayWindows(ctx, store, "s", true, pos(20)); err != nil || plain.replayWindows != nil {
		t.Fatalf("a source without the capability: %v %v", plain.replayWindows, err)
	}
}

// TestStreamer_ResetReplayWindowsAfterColdStart pins the cold start's reset:
// every live-added table it copied is recorded as having no window.
func TestStreamer_ResetReplayWindowsAfterColdStart(t *testing.T) {
	store := &windowStore{windows: []ir.AddedTableReplayWindow{{Table: "old", CopyPosition: pos(5), Key: []string{"id"}}}}
	s := &Streamer{Source: windowSourceEngine{}, startLiveAdded: []string{"a"}}
	s.resetReplayWindowsAfterColdStart(context.Background(), store, "s")
	if !reflect.DeepEqual(store.reset, []ir.AddedTableReplayWindow{{Table: "a"}}) {
		t.Fatalf("reset to %+v", store.reset)
	}
}
