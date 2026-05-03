package mysql

import (
	"context"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/orware/sluice/internal/ir"
)

// TestVStreamSnapshot_CopyPhaseBuffering walks the dispatcher
// through the canonical COPY-phase event stream (FIELD → ROW × N →
// VGTID → COPY_COMPLETED-per-table → COPY_COMPLETED-global) and
// asserts:
//
//   - rowBuffer fills under the unqualified table name,
//   - currentVgtid tracks the last VGTID,
//   - the global COPY_COMPLETED returns done=true,
//   - per-shard COPY_COMPLETED events do NOT terminate the drain.
func TestVStreamSnapshot_CopyPhaseBuffering(t *testing.T) {
	s := &vstreamSnapshotStream{
		keyspace:  "main",
		fields:    make(map[string][]*query.Field),
		rowBuffer: make(map[string][]ir.Row),
	}

	// FIELD event for users(id, email).
	fieldsEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			Fields: []*query.Field{
				{Name: "id", Type: query.Type_INT64},
				{Name: "email", Type: query.Type_VARCHAR},
			},
		},
	}
	if done, err := s.dispatchCopyEvent(fieldsEv); err != nil || done {
		t.Fatalf("FIELD: done=%v err=%v", done, err)
	}

	// Two ROW events (snapshot rows have After only).
	for _, vals := range [][]string{
		{"1", "alice@example.com"},
		{"2", "bob@example.com"},
	} {
		rowEv := &binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName: "users",
				Keyspace:  "main",
				Shard:     "-",
				RowChanges: []*binlogdata.RowChange{
					{After: makeRow(vals)},
				},
			},
		}
		if done, err := s.dispatchCopyEvent(rowEv); err != nil || done {
			t.Fatalf("ROW: done=%v err=%v", done, err)
		}
	}

	// VGTID — captures the snapshot-consistent position.
	vgtidEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_VGTID,
		Vgtid: &binlogdata.VGtid{
			ShardGtids: []*binlogdata.ShardGtid{
				{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-100"},
			},
		},
	}
	if done, err := s.dispatchCopyEvent(vgtidEv); err != nil || done {
		t.Fatalf("VGTID: done=%v err=%v", done, err)
	}

	// Per-table COPY_COMPLETED — does NOT terminate.
	perTableDone := &binlogdata.VEvent{
		Type:     binlogdata.VEventType_COPY_COMPLETED,
		Keyspace: "main",
		Shard:    "-",
	}
	if done, err := s.dispatchCopyEvent(perTableDone); err != nil || done {
		t.Fatalf("per-table COPY_COMPLETED: done=%v err=%v (should not terminate)", done, err)
	}

	// Global COPY_COMPLETED — terminates.
	globalDone := &binlogdata.VEvent{Type: binlogdata.VEventType_COPY_COMPLETED}
	done, err := s.dispatchCopyEvent(globalDone)
	if err != nil {
		t.Fatalf("global COPY_COMPLETED: %v", err)
	}
	if !done {
		t.Fatal("global COPY_COMPLETED should return done=true")
	}

	// Buffer assertions.
	usersRows := s.rowBuffer["users"]
	if len(usersRows) != 2 {
		t.Fatalf("rowBuffer[users] = %d rows; want 2", len(usersRows))
	}
	if id, _ := usersRows[0]["id"].(int64); id != 1 {
		t.Errorf("rowBuffer[users][0][id] = %#v; want int64(1)", usersRows[0]["id"])
	}
	if email, _ := usersRows[1]["email"].(string); email != "bob@example.com" {
		t.Errorf("rowBuffer[users][1][email] = %#v; want bob@example.com", usersRows[1]["email"])
	}

	// Position state.
	if len(s.currentVgtid) != 1 || s.currentVgtid[0].Gtid != "MySQL56/abc:1-100" {
		t.Errorf("currentVgtid = %v; want one entry with Gtid MySQL56/abc:1-100", s.currentVgtid)
	}
}

// TestVStreamSnapshot_CopyRowWithoutField rejects a ROW event that
// arrives before its FIELD event. Same loud-failure behaviour as the
// CDC reader — a missing FIELD means we can't decode the row, and
// silently dropping would mask an upstream protocol violation.
func TestVStreamSnapshot_CopyRowWithoutField(t *testing.T) {
	s := &vstreamSnapshotStream{
		keyspace:  "main",
		fields:    make(map[string][]*query.Field),
		rowBuffer: make(map[string][]ir.Row),
	}
	rowEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "users",
			Shard:     "-",
			RowChanges: []*binlogdata.RowChange{
				{After: makeRow([]string{"1"})},
			},
		},
	}
	if _, err := s.dispatchCopyEvent(rowEv); err == nil {
		t.Fatal("expected error for ROW event without preceding FIELD")
	}
}

// TestVStreamSnapshot_CDCRoutingAfterCopy seeds the dispatcher with
// a captured VGTID (i.e., we're "post-COPY") and confirms a ROW
// event becomes an ir.Insert with the expected position. Mirrors
// TestVStreamReader_BasicEventDispatch but on the snapshot type so
// we know the post-COPY routing is wired the same way.
func TestVStreamSnapshot_CDCRoutingAfterCopy(t *testing.T) {
	pos, err := encodeVStreamPos([]shardGtid{
		{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-100"},
	})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	s := &vstreamSnapshotStream{
		keyspace: "main",
		fields:   make(map[string][]*query.Field),
		currentVgtid: []shardGtid{
			{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-100"},
		},
	}

	out := make(chan ir.Change, 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fieldsEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			Fields: []*query.Field{
				{Name: "id", Type: query.Type_INT64},
				{Name: "email", Type: query.Type_VARCHAR},
			},
		},
	}
	if err := s.dispatchCDCEvent(ctx, fieldsEv, out); err != nil {
		t.Fatalf("dispatchCDCEvent FIELD: %v", err)
	}

	insertEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			RowChanges: []*binlogdata.RowChange{
				{After: makeRow([]string{"7", "carol@example.com"})},
			},
		},
	}
	if err := s.dispatchCDCEvent(ctx, insertEv, out); err != nil {
		t.Fatalf("dispatchCDCEvent ROW: %v", err)
	}
	close(out)

	got := drainChannel(out)
	if len(got) != 1 {
		t.Fatalf("got %d changes; want 1 (insert)", len(got))
	}
	ins, ok := got[0].(ir.Insert)
	if !ok {
		t.Fatalf("got[0] = %T; want ir.Insert", got[0])
	}
	if ins.Table != "users" {
		t.Errorf("insert.Table = %q; want users", ins.Table)
	}
	if ins.Position.Token != pos.Token || ins.Position.Engine != pos.Engine {
		t.Errorf("insert.Position = %+v; want %+v", ins.Position, pos)
	}
}

// TestVStreamSnapshot_RowsReadDrainsBuffer confirms ReadRows drains
// the buffer for the requested table and clears it (a second
// ReadRows on the same table returns an empty channel). This is the
// "in-memory snapshot rows" contract.
func TestVStreamSnapshot_RowsReadDrainsBuffer(t *testing.T) {
	s := &vstreamSnapshotStream{
		keyspace: "main",
		fields:   make(map[string][]*query.Field),
		rowBuffer: map[string][]ir.Row{
			"users": {
				{"id": int64(1), "email": "alice@example.com"},
				{"id": int64(2), "email": "bob@example.com"},
			},
		},
	}
	rr := &vstreamSnapshotRows{snap: s}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tbl := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}},
	}}

	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	rows := make([]ir.Row, 0, 2)
	for r := range ch {
		rows = append(rows, r)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows; want 2", len(rows))
	}
	if email, _ := rows[1]["email"].(string); email != "bob@example.com" {
		t.Errorf("rows[1][email] = %#v; want bob@example.com", rows[1]["email"])
	}

	// Buffer should be cleared after read.
	if remaining, ok := s.rowBuffer["users"]; ok {
		t.Errorf("rowBuffer[users] still present after drain: %v", remaining)
	}

	// Second read returns empty channel cleanly.
	ch2, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows (second call): %v", err)
	}
	count := 0
	for range ch2 {
		count++
	}
	if count != 0 {
		t.Errorf("second ReadRows returned %d rows; want 0", count)
	}
}

// TestVStreamSnapshot_ChangesPositionMismatchRejected confirms the
// snapshot's CDCReader half rejects a from-position that doesn't
// match the captured VGTID. This catches misconfigured callers who
// pass a stale position into the snapshot's StreamChanges (the
// intended contract is "use stream.Position or pass the zero
// Position to mean the same thing").
func TestVStreamSnapshot_ChangesPositionMismatchRejected(t *testing.T) {
	s := &vstreamSnapshotStream{
		keyspace: "main",
		fields:   make(map[string][]*query.Field),
		currentVgtid: []shardGtid{
			{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-100"},
		},
	}
	c := &vstreamSnapshotChanges{snap: s}

	stale, err := encodeVStreamPos([]shardGtid{
		{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-50"},
	})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.StreamChanges(ctx, stale); err == nil {
		t.Fatal("expected error for position mismatch; got nil")
	}
}

// TestSameVgtid covers the equality helper used by the position
// mismatch check. Same shape and contents → equal; any difference →
// not equal.
func TestSameVgtid(t *testing.T) {
	a := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "g1"}}
	b := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "g1"}}
	if !sameVgtid(a, b) {
		t.Error("identical slices reported not equal")
	}

	c := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "g2"}}
	if sameVgtid(a, c) {
		t.Error("different Gtids reported equal")
	}

	d := []shardGtid{
		{Keyspace: "main", Shard: "-", Gtid: "g1"},
		{Keyspace: "main", Shard: "-80", Gtid: "g3"},
	}
	if sameVgtid(a, d) {
		t.Error("different lengths reported equal")
	}

	if !sameVgtid(nil, nil) {
		t.Error("nil slices reported not equal")
	}
}
