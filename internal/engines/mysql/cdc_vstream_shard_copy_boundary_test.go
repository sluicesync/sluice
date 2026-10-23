// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// Unit pins for VSTREAM-SHARD-SKEW-COPY-CHANGES-DROPPED (Bug 331) and the
// VSTREAM-COPY-ROW-CHANGE-REFUSED refusal (cdc_vstream_shard_copy_boundary.go).
// The end-to-end pin, on a real two- and four-shard vttestserver, is
// TestStreamer_VStreamShardSkewCopy (internal/pipeline).

const scbUUID = "61bd25c9-ced2-11f1-8cb8-e2cd2f250018"

func scbGtid(hi string) string { return "MySQL56/" + scbUUID + ":1-" + hi }

func scbFields() []*query.Field {
	return []*query.Field{
		{Name: "id", Type: query.Type_INT64, Flags: uint32(query.MySqlFlag_PRI_KEY_FLAG | query.MySqlFlag_NOT_NULL_FLAG)},
		{Name: "v", Type: query.Type_VARCHAR, Charset: 255},
	}
}

func scbField(shard string, fields []*query.Field) *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_FIELD, FieldEvent: &binlogdata.FieldEvent{
		TableName: "t", Keyspace: "main", Shard: shard, Fields: fields,
	}}
}

func scbRow(shard string, rc *binlogdata.RowChange) *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, RowEvent: &binlogdata.RowEvent{
		TableName: "t", Keyspace: "main", Shard: shard, RowChanges: []*binlogdata.RowChange{rc},
	}}
}

func scbVgtid(lo, hi string) *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_VGTID, Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{
		{Keyspace: "main", Shard: "-80", Gtid: scbGtid(lo)},
		{Keyspace: "main", Shard: "80-", Gtid: scbGtid(hi)},
	}}}
}

func scbDone(shard string) *binlogdata.VEvent {
	ks := "main"
	if shard == "" {
		ks = ""
	}
	return &binlogdata.VEvent{Type: binlogdata.VEventType_COPY_COMPLETED, Keyspace: ks, Shard: shard}
}

// TestCopyRowChangeRefusal_Shapes: on a shard still copying, the copy
// applies an after-image row, and an UPDATE whose key cells are unchanged;
// every other shape refuses with the marker. The DELETE row is the one the
// replaced skip dropped silently.
func TestCopyRowChangeRefusal_Shapes(t *testing.T) {
	keyed := scbFields()
	keyless := []*query.Field{{Name: "id", Type: query.Type_INT64}, {Name: "v", Type: query.Type_VARCHAR}}
	for _, tc := range []struct {
		name   string
		rc     *binlogdata.RowChange
		fields []*query.Field
		refuse string // "" = applied
	}{
		{"copy row", &binlogdata.RowChange{After: makeRow([]string{"1", "a"})}, keyed, ""},
		{"update, key unchanged", &binlogdata.RowChange{Before: makeRow([]string{"1", "a"}), After: makeRow([]string{"1", "b"})}, keyed, ""},
		{"update, key changed", &binlogdata.RowChange{Before: makeRow([]string{"1", "a"}), After: makeRow([]string{"2", "a"})}, keyed, "changes the primary key"},
		{"update, keyless table", &binlogdata.RowChange{Before: makeRow([]string{"1", "a"}), After: makeRow([]string{"1", "b"})}, keyless, "changes the primary key"},
		{"update, key NULL to empty", &binlogdata.RowChange{Before: makeRowOmitting([]*string{nil, ptr("a")}), After: makeRow([]string{"", "a"})}, keyed, "changes the primary key"},
		{"update, malformed before", &binlogdata.RowChange{Before: &query.Row{Lengths: []int64{1}}, After: makeRow([]string{"1", "b"})}, keyed, "changes the primary key"},
		{"delete", &binlogdata.RowChange{Before: makeRow([]string{"1", "a"})}, keyed, "a DELETE"},
		{"neither image", &binlogdata.RowChange{}, keyed, "neither image"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := copyRowChangeRefusal("main", "-80", "t", tc.rc, tc.fields)
			if tc.refuse == "" {
				if err != nil {
					t.Fatalf("refused an appliable change: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("applied a %s; want %s", tc.name, copyRowChangeRefusedMarker)
			}
			if !errors.Is(err, errCopyRowChangeRefused) || !strings.Contains(err.Error(), copyRowChangeRefusedMarker) ||
				!strings.Contains(err.Error(), tc.refuse) || !strings.Contains(err.Error(), "shard -80") {
				t.Fatalf("refusal = %v; want the marker, %q and the shard", err, tc.refuse)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// TestShardCopyBoundary_SnapshotPosition: a finished shard moves back to the
// GTID it finished at (cursor dropped), an unfinished one keeps the final
// GTID, and a recorded GTID the final one does not contain, or a missing one
// on a shard that held changes, refuses.
func TestShardCopyBoundary_SnapshotPosition(t *testing.T) {
	final := []shardGtid{
		{Keyspace: "main", Shard: "-80", Gtid: scbGtid("300")},
		{Keyspace: "main", Shard: "80-", Gtid: scbGtid("278"), TablePKs: []encodedTablePK{{TableName: "t", Lastpk: "cursor"}}},
	}
	t.Run("finished shard rewound", func(t *testing.T) {
		var b shardCopyBoundary
		b.markFinished("-80", []shardGtid{{Keyspace: "main", Shard: "-80", Gtid: scbGtid("278"), TablePKs: []encodedTablePK{{TableName: "t", Lastpk: "x"}}}})
		b.hold("-80", 46)
		got, moved, err := b.snapshotPosition(final)
		if err != nil || !moved {
			t.Fatalf("moved=%v err=%v; want moved", moved, err)
		}
		if got[0].Gtid != scbGtid("278") || len(got[0].TablePKs) != 0 {
			t.Errorf("-80 = %+v; want %s with no cursor", got[0], scbGtid("278"))
		}
		if !reflect.DeepEqual(got[1], final[1]) {
			t.Errorf("80- = %+v; want it unchanged %+v", got[1], final[1])
		}
		if final[0].Gtid != scbGtid("300") {
			t.Error("snapshotPosition mutated its input")
		}
	})
	t.Run("finished at the final position", func(t *testing.T) {
		var b shardCopyBoundary
		b.markFinished("-80", final)
		b.markFinished("80-", final)
		got, moved, err := b.snapshotPosition(final)
		if err != nil || moved || !reflect.DeepEqual(got[0], final[0]) || !reflect.DeepEqual(got[1], final[1]) {
			t.Fatalf("got %+v moved=%v err=%v; want the final position unchanged", got, moved, err)
		}
	})
	t.Run("not contained refuses", func(t *testing.T) {
		var b shardCopyBoundary
		b.markFinished("-80", []shardGtid{{Shard: "-80", Gtid: "MySQL56/" + scbUUID + ":1-400"}})
		if _, _, err := b.snapshotPosition(final); err == nil || !strings.Contains(err.Error(), "VSTREAM-SHARD-SKEW-COPY-CHANGES-DROPPED") {
			t.Fatalf("err = %v; want a refusal naming the marker", err)
		}
	})
	t.Run("no recorded position with held changes refuses", func(t *testing.T) {
		var b shardCopyBoundary
		b.markFinished("-80", nil)
		b.hold("-80", 1)
		if _, _, err := b.snapshotPosition(final); err == nil {
			t.Fatal("resumed past a held change with no recorded copy position")
		}
	})
	t.Run("no recorded position, nothing held", func(t *testing.T) {
		var b shardCopyBoundary
		b.markFinished("-80", nil)
		got, moved, err := b.snapshotPosition(final)
		if err != nil || moved || !reflect.DeepEqual(got[0], final[0]) {
			t.Fatalf("got %+v moved=%v err=%v; want the final position", got, moved, err)
		}
	})
}

// scbTwoShardCopy drives a two-shard copy through dispatch: copy rows on
// both shards, -80 finishes at GTID 1-278, then -80 sends a delete, an
// update, a key change and an insert (the binlog after its snapshot) while
// 80- is still copying, then 80- finishes and the stream completes.
func scbTwoShardCopy(t *testing.T, dispatch func(*binlogdata.VEvent) (bool, error)) {
	t.Helper()
	for _, ev := range []*binlogdata.VEvent{
		scbField("-80", scbFields()),
		scbField("80-", scbFields()),
		scbRow("-80", &binlogdata.RowChange{After: makeRow([]string{"1", "a"})}),
		scbRow("80-", &binlogdata.RowChange{After: makeRow([]string{"2", "b"})}),
		scbVgtid("278", "278"),
		scbDone("-80"),
		scbRow("-80", &binlogdata.RowChange{Before: makeRow([]string{"1", "a"})}),
		scbRow("-80", &binlogdata.RowChange{Before: makeRow([]string{"3", "c"}), After: makeRow([]string{"3", "d"})}),
		scbRow("-80", &binlogdata.RowChange{Before: makeRow([]string{"5", "e"}), After: makeRow([]string{"6", "e"})}),
		scbRow("-80", &binlogdata.RowChange{After: makeRow([]string{"7", "new"})}),
		scbVgtid("300", "278"),
		scbRow("80-", &binlogdata.RowChange{After: makeRow([]string{"4", "d"})}),
		scbDone("80-"),
	} {
		if done, err := dispatch(ev); err != nil || done {
			t.Fatalf("dispatch %v: done=%v err=%v", ev.GetType(), done, err)
		}
	}
	if done, err := dispatch(scbDone("")); err != nil || !done {
		t.Fatalf("global COPY_COMPLETED: done=%v err=%v", done, err)
	}
}

func scbIDs(rows []ir.Row) []int64 {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		id, _ := r["id"].(int64)
		out = append(out, id)
	}
	return out
}

// TestVStreamSnapshot_FinishedShardChangesAreHeldAndRewound: the
// sequential dispatcher (single-stream and per-table paths) buffers only the
// copy rows, holds the four changes -80 sent after it finished, and
// finishCopy's position keeps -80 at 1-278 and marks the CDC tail to reopen
// there. Before the fix the delete was skipped, the update, key change and
// insert were buffered as copy rows, and the position was -80 at 1-300.
func TestVStreamSnapshot_FinishedShardChangesAreHeldAndRewound(t *testing.T) {
	s := newTestSnapshotStream()
	scbTwoShardCopy(t, s.dispatchCopyEvent)

	if got := scbIDs(s.rowBuffer["t"]); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 4 {
		t.Errorf("buffered copy rows %v; want [1 2 4] (the finished shard's changes are not copy rows)", got)
	}
	if s.copyBoundary.held["-80"] != 4 {
		t.Errorf("held %v; want 4 changes on -80", s.copyBoundary.held)
	}
	stream := &ir.SnapshotStream{}
	s.finishCopy(stream)
	if s.err != nil {
		t.Fatalf("finishCopy: %v", s.err)
	}
	shards, _, err := decodeVStreamPos(stream.Position)
	if err != nil {
		t.Fatalf("decode position: %v", err)
	}
	if shards[0].Gtid != scbGtid("278") || shards[1].Gtid != scbGtid("278") {
		t.Errorf("position %+v; want -80 back at %s (where it finished) and 80- at %s", shards, scbGtid("278"), scbGtid("278"))
	}
	if !s.cdcResumeRewound {
		t.Error("cdcResumeRewound = false; the CDC tail would continue the copy stream past the held changes")
	}
	if err := s.Err(); err != nil {
		t.Errorf("Err = %v; a finished shard's changes are not a refusal", err)
	}
}

// TestVStreamSnapshot_OneShardPositionUnchanged: a single shard's per-scope
// and stream-wide COPY_COMPLETED come together, nothing is held, and the
// position and in-place CDC handoff are unchanged.
func TestVStreamSnapshot_OneShardPositionUnchanged(t *testing.T) {
	s := newTestSnapshotStream()
	for _, ev := range []*binlogdata.VEvent{
		scbField("0", scbFields()),
		scbRow("0", &binlogdata.RowChange{After: makeRow([]string{"1", "a"})}),
		{Type: binlogdata.VEventType_VGTID, Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{{Keyspace: "main", Shard: "0", Gtid: scbGtid("50")}}}},
		scbDone("0"),
	} {
		if _, err := s.dispatchCopyEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	if done, err := s.dispatchCopyEvent(scbDone("")); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	stream := &ir.SnapshotStream{}
	s.finishCopy(stream)
	shards, _, err := decodeVStreamPos(stream.Position)
	if err != nil || len(shards) != 1 || shards[0].Gtid != scbGtid("50") || s.cdcResumeRewound {
		t.Fatalf("position %+v err=%v rewound=%v; want %s, continued in place", shards, err, s.cdcResumeRewound, scbGtid("50"))
	}
}

// TestVStreamSnapshot_CopyPhaseDeleteRefusesItsTable: a DELETE on a shard
// still copying refuses the table it belongs to, through Err once that
// table is read, and the read closes without waiting for the copy. A table
// nobody reads (the keyspace-wide add-table stream) is not refused.
func TestVStreamSnapshot_CopyPhaseDeleteRefusesItsTable(t *testing.T) {
	s := newTestSnapshotStream()
	for _, ev := range []*binlogdata.VEvent{
		scbField("-80", scbFields()),
		scbRow("-80", &binlogdata.RowChange{After: makeRow([]string{"1", "a"})}),
		scbRow("-80", &binlogdata.RowChange{Before: makeRow([]string{"1", "a"})}),
	} {
		if done, err := s.dispatchCopyEvent(ev); err != nil || done {
			t.Fatalf("dispatch: done=%v err=%v (the refusal is the table's, not the stream's)", done, err)
		}
	}
	if err := s.Err(); err != nil {
		t.Fatalf("Err before the table is read = %v; want nil (an unread table is not refused)", err)
	}
	rr := &vstreamSnapshotRows{snap: s}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := rr.ReadRows(ctx, &ir.Table{Name: "t"})
	if err != nil {
		t.Fatal(err)
	}
	delivered := 0
	for range ch {
		delivered++
	}
	if delivered != 0 {
		t.Errorf("the refused table delivered %d row(s); want none", delivered)
	}
	if ctx.Err() != nil {
		t.Fatal("the refused table's read waited for the copy instead of closing")
	}
	if err := rr.Err(); !errors.Is(err, errCopyRowChangeRefused) {
		t.Fatalf("Err after reading the refused table = %v; want %s", err, copyRowChangeRefusedMarker)
	}
}

// TestVStreamConcurrentCopy_FinishedShardChangesAreHeld: the concurrent
// copy's per-stream dispatcher holds the same changes, buffers the same copy
// rows, and its boundary yields the same rewound position.
func TestVStreamConcurrentCopy_FinishedShardChangesAreHeld(t *testing.T) {
	s := newTestSnapshotStream()
	s.perStreamBytes = make([]int64, 1)
	s.perStreamCap = defaultSnapshotMaxBufferBytes
	cs := &copyStream{parent: s, fields: map[string][]*query.Field{}}
	scbTwoShardCopy(t, func(ev *binlogdata.VEvent) (bool, error) { return cs.dispatchCopyEvent("t", ev) })

	if got := scbIDs(s.rowBuffer["t"]); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 4 {
		t.Errorf("buffered copy rows %v; want [1 2 4]", got)
	}
	snap, moved, err := cs.boundary.snapshotPosition(cs.cur)
	if err != nil || !moved || snap[0].Gtid != scbGtid("278") {
		t.Fatalf("snapshot %+v moved=%v err=%v; want -80 back at %s", snap, moved, err, scbGtid("278"))
	}
}

// TestVStreamConcurrentCopy_CopyPhaseDeleteRefusesItsTable: the concurrent
// dispatcher refuses the same way.
func TestVStreamConcurrentCopy_CopyPhaseDeleteRefusesItsTable(t *testing.T) {
	s := newTestSnapshotStream()
	s.perStreamBytes = make([]int64, 1)
	s.perStreamCap = defaultSnapshotMaxBufferBytes
	cs := &copyStream{parent: s, fields: map[string][]*query.Field{}}
	for _, ev := range []*binlogdata.VEvent{
		scbField("-80", scbFields()),
		scbRow("-80", &binlogdata.RowChange{Before: makeRow([]string{"1", "a"})}),
	} {
		if _, err := cs.dispatchCopyEvent("t", ev); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	s.readTables = map[string]bool{"t": true}
	s.mu.Unlock()
	if err := s.Err(); !errors.Is(err, errCopyRowChangeRefused) {
		t.Fatalf("Err = %v; want %s", err, copyRowChangeRefusedMarker)
	}
}
