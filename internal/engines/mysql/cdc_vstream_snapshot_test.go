// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// newTestSnapshotStream builds a vstreamSnapshotStream wired the way
// the constructor does (cond over mu, default byte cap) so the
// dispatcher/enqueue/ReadRows helpers behave as in production when a
// test drives them directly. Tests that need copy-completion or a
// smaller cap mutate the returned struct's fields before use.
func newTestSnapshotStream() *vstreamSnapshotStream {
	s := &vstreamSnapshotStream{
		keyspace:            "main",
		fields:              make(map[string][]*query.Field),
		rowBuffer:           make(map[string][]ir.Row),
		copyCompletedShards: make(map[string]bool),
		maxBufferBytes:      defaultSnapshotMaxBufferBytes,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

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
	s := newTestSnapshotStream()

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
	s := newTestSnapshotStream()
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
		// Streaming ReadRows (ADR-0071) blocks until a row is available
		// or the COPY phase has completed; this test pre-stages the
		// queue and marks copy complete so ReadRows drains-and-closes.
		copyComplete:   true,
		maxBufferBytes: defaultSnapshotMaxBufferBytes,
	}
	s.cond = sync.NewCond(&s.mu)
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

// TestVStreamSnapshot_MultiShardCopyCompletedRouting walks the
// dispatcher through a multi-shard COPY phase: each shard emits its
// own FIELD/ROW/per-scope COPY_COMPLETED stream, and only the
// final global COPY_COMPLETED (Keyspace+Shard empty) terminates the
// drain. Asserts:
//
//   - per-scope events for shards "-80" and "80-" both register in
//     copyCompletedShards but do NOT terminate,
//   - rows from both shards merge into the same unqualified-table
//     slice in rowBuffer (the Rows.ReadRows consumer sees a unified
//     view per logical table),
//   - only the global event flips done=true.
func TestVStreamSnapshot_MultiShardCopyCompletedRouting(t *testing.T) {
	s := newTestSnapshotStream()

	for _, shard := range []string{"-80", "80-"} {
		fieldsEv := &binlogdata.VEvent{
			Type: binlogdata.VEventType_FIELD,
			FieldEvent: &binlogdata.FieldEvent{
				TableName: "users",
				Keyspace:  "main",
				Shard:     shard,
				Fields: []*query.Field{
					{Name: "id", Type: query.Type_INT64},
					{Name: "email", Type: query.Type_VARCHAR},
				},
			},
		}
		if done, err := s.dispatchCopyEvent(fieldsEv); err != nil || done {
			t.Fatalf("FIELD %s: done=%v err=%v", shard, done, err)
		}

		// Each shard emits one row for the same logical table.
		rowVals := []string{"1", "alice@example.com"}
		if shard == "80-" {
			rowVals = []string{"2", "bob@example.com"}
		}
		rowEv := &binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName: "users",
				Keyspace:  "main",
				Shard:     shard,
				RowChanges: []*binlogdata.RowChange{
					{After: makeRow(rowVals)},
				},
			},
		}
		if done, err := s.dispatchCopyEvent(rowEv); err != nil || done {
			t.Fatalf("ROW %s: done=%v err=%v", shard, done, err)
		}

		// Per-scope COPY_COMPLETED — must not terminate.
		perScope := &binlogdata.VEvent{
			Type:     binlogdata.VEventType_COPY_COMPLETED,
			Keyspace: "main",
			Shard:    shard,
		}
		if done, err := s.dispatchCopyEvent(perScope); err != nil || done {
			t.Fatalf("per-scope COPY_COMPLETED %s: done=%v err=%v (must not terminate)", shard, done, err)
		}
	}

	// Both shards' per-scope completions should be tracked.
	if len(s.copyCompletedShards) != 2 {
		t.Errorf("copyCompletedShards = %v; want 2 entries (one per shard)", s.copyCompletedShards)
	}
	if !s.copyCompletedShards[shardScopeKey("main", "-80")] {
		t.Errorf("copyCompletedShards missing main/-80")
	}
	if !s.copyCompletedShards[shardScopeKey("main", "80-")] {
		t.Errorf("copyCompletedShards missing main/80-")
	}

	// Multi-shard row buffering: both shards' rows merge into the
	// users slice under the unqualified table name.
	usersRows := s.rowBuffer["users"]
	if len(usersRows) != 2 {
		t.Fatalf("rowBuffer[users] = %d rows; want 2 (one per shard)", len(usersRows))
	}

	// Global COPY_COMPLETED — terminates the drain.
	globalDone := &binlogdata.VEvent{Type: binlogdata.VEventType_COPY_COMPLETED}
	done, err := s.dispatchCopyEvent(globalDone)
	if err != nil {
		t.Fatalf("global COPY_COMPLETED: %v", err)
	}
	if !done {
		t.Fatal("global COPY_COMPLETED should terminate the drain")
	}
}

// TestVStreamSnapshot_JournalDuringCopyReturnsShardLayoutErr asserts
// that a JOURNAL event during the COPY phase surfaces the typed
// [ShardLayoutChangedError] (matchable with errors.Is against
// [ErrShardLayoutChanged]) rather than a generic string error. v1
// of multi-shard snapshot punts on in-place reshard recovery — the
// caller drops the stream and reopens against the new layout.
func TestVStreamSnapshot_JournalDuringCopyReturnsShardLayoutErr(t *testing.T) {
	s := newTestSnapshotStream()

	journalEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_JOURNAL,
		Journal: &binlogdata.Journal{
			Participants: []*binlogdata.KeyspaceShard{
				{Keyspace: "main", Shard: "-"},
			},
			ShardGtids: []*binlogdata.ShardGtid{
				{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/abcd:1-200"},
				{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/abcd:1-200"},
			},
		},
	}
	done, err := s.dispatchCopyEvent(journalEv)
	if err == nil {
		t.Fatal("expected error for JOURNAL during COPY; got nil")
	}
	if done {
		t.Error("dispatchCopyEvent on JOURNAL: done=true; want false (drain ends via error, not clean termination)")
	}
	if !errors.Is(err, ErrShardLayoutChanged) {
		t.Errorf("err = %v; want errors.Is(err, ErrShardLayoutChanged) = true", err)
	}

	// And the typed error carries the new layout.
	var resh *ShardLayoutChangedError
	if !errors.As(err, &resh) {
		t.Fatalf("err = %v; want errors.As → *ShardLayoutChangedError", err)
	}
	if len(resh.NewShards) != 2 {
		t.Errorf("ShardLayoutChangedError.NewShards has %d entries; want 2", len(resh.NewShards))
	}
}

// TestVStreamSnapshot_MultiShardRowBufferMerge confirms
// vstreamSnapshotRows.ReadRows returns rows from BOTH shards for the
// same logical table in arrival order. The per-shard distinction is
// invisible to the consumer — the orchestrator's bulk-copy phase
// applies rows by (table, value); shard origin is irrelevant on the
// target side.
func TestVStreamSnapshot_MultiShardRowBufferMerge(t *testing.T) {
	s := newTestSnapshotStream()

	// Two FIELD events (one per shard) — same table, same column
	// shape but distinct field-cache entries (per-shard FIELD events
	// can theoretically diverge during a vschema rollout).
	for _, shard := range []string{"-80", "80-"} {
		fieldsEv := &binlogdata.VEvent{
			Type: binlogdata.VEventType_FIELD,
			FieldEvent: &binlogdata.FieldEvent{
				TableName: "users",
				Keyspace:  "main",
				Shard:     shard,
				Fields: []*query.Field{
					{Name: "id", Type: query.Type_INT64},
					{Name: "email", Type: query.Type_VARCHAR},
				},
			},
		}
		if _, err := s.dispatchCopyEvent(fieldsEv); err != nil {
			t.Fatalf("FIELD %s: %v", shard, err)
		}
	}

	// Interleaved row events from both shards (the order vtgate
	// would have emitted them with MinimizeSkew enabled).
	for i, ev := range []struct {
		shard string
		vals  []string
	}{
		{"-80", []string{"1", "alice@example.com"}},
		{"80-", []string{"2", "bob@example.com"}},
		{"-80", []string{"3", "carol@example.com"}},
		{"80-", []string{"4", "dan@example.com"}},
	} {
		rowEv := &binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName: "users",
				Keyspace:  "main",
				Shard:     ev.shard,
				RowChanges: []*binlogdata.RowChange{
					{After: makeRow(ev.vals)},
				},
			},
		}
		if _, err := s.dispatchCopyEvent(rowEv); err != nil {
			t.Fatalf("ROW[%d] %s: %v", i, ev.shard, err)
		}
	}

	rows := s.rowBuffer["users"]
	if len(rows) != 4 {
		t.Fatalf("rowBuffer[users] = %d rows; want 4 (rows from both shards merged)", len(rows))
	}

	// Streaming ReadRows blocks until copy completes; mark it so the
	// merged queue drains-and-closes for the consumer assertions below.
	s.mu.Lock()
	s.copyComplete = true
	s.mu.Unlock()

	rr := &vstreamSnapshotRows{snap: s}
	tbl := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := make([]string, 0, 4)
	for r := range ch {
		s, _ := r["email"].(string)
		got = append(got, s)
	}
	want := []string{"alice@example.com", "bob@example.com", "carol@example.com", "dan@example.com"}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("got[%d] email = %q; want %q (arrival order)", i, got[i], want[i])
		}
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

// TestSameVgtid_IgnoresTablePKs confirms the equality helper compares
// only (keyspace, shard, gtid) and not the transient TablePKs cursor —
// two positions with the same GTID but different in-flight COPY cursors
// are "the same" for the snapshot-anchor mismatch guard (and shardGtid
// is no longer comparable with == now that it carries a slice).
func TestSameVgtid_IgnoresTablePKs(t *testing.T) {
	a := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "g1", TablePKs: []encodedTablePK{{TableName: "users", Lastpk: "AAAA"}}}}
	b := []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "g1"}}
	if !sameVgtid(a, b) {
		t.Error("positions with same GTID but differing TablePKs reported not equal; TablePKs must not affect identity")
	}
}

// seedBreadcrumb is a white-box helper mirroring what the pump does when
// a VGTID arrives: it records a durable-watermark breadcrumb pairing the
// encoded position (carrying a TablePKs cursor) with the rows enqueued so
// far. enqueuedRows is the monotonic count the breadcrumb covers.
func seedBreadcrumb(t *testing.T, s *vstreamSnapshotStream, gtid string, enqueuedRows int64) {
	t.Helper()
	cur := []shardGtid{{
		Keyspace: "main", Shard: "-", Gtid: gtid,
		TablePKs: []encodedTablePK{{TableName: "users", Lastpk: "QkFTRTY0"}},
	}}
	pos, err := encodeVStreamPos(cur)
	if err != nil {
		t.Fatalf("encodeVStreamPos(%s): %v", gtid, err)
	}
	s.currentVgtid = cur
	s.enqueuedRows = enqueuedRows
	s.recordBreadcrumbLocked(pos)
}

// gtidOf decodes a checkpointed position back to its single shard GTID.
func gtidOf(t *testing.T, pos ir.Position) string {
	t.Helper()
	decoded, ok, err := decodeVStreamPos(pos)
	if err != nil || !ok {
		t.Fatalf("position did not decode: ok=%v err=%v", ok, err)
	}
	if len(decoded) != 1 {
		t.Fatalf("position has %d shards; want 1", len(decoded))
	}
	if len(decoded[0].TablePKs) != 1 || decoded[0].TablePKs[0].TableName != "users" {
		t.Fatalf("checkpointed position lost the TablePKs cursor: %+v", decoded)
	}
	return decoded[0].Gtid
}

// TestVStreamSnapshot_CheckpointCadence pins ADR-0072 Phase B + the
// v0.99.9 durable-write watermark: the COPY pump persists the resume
// cursor on the bounded N-rows cadence, the persisted position carries
// the per-shard TablePKs, the row counter resets after each checkpoint,
// and — the load-bearing v0.99.9 invariant — the persisted position never
// runs ahead of the durably-written frontier (it persists the DURABLE
// breadcrumb, not the latest RECEIVED VGTID).
func TestVStreamSnapshot_CheckpointCadence(t *testing.T) {
	s := newTestSnapshotStream()
	s.checkpointRows = 3
	s.checkpointInterval = time.Hour // disable the time half for determinism

	var got []ir.Position
	s.checkpointFn = func(_ context.Context, pos ir.Position) error {
		got = append(got, pos)
		return nil
	}

	// The pump has RECEIVED two VGTID boundaries: position g50 covers the
	// first 3 rows, g100 covers the first 6. But the consumer has durably
	// written NOTHING yet.
	seedBreadcrumb(t, s, "MySQL56/abc:1-50", 3)
	seedBreadcrumb(t, s, "MySQL56/abc:1-100", 6)

	last := time.Now()

	// Cadence is due (rowsSinceCheckpoint >= 3) but NO row is durable yet:
	// the checkpoint must persist NOTHING rather than the received cursor.
	s.rowsSinceCheckpoint = 3
	last = s.maybeCheckpoint(context.Background(), last)
	if len(got) != 0 {
		t.Fatalf("checkpoint persisted a position ahead of the durable frontier (durableRows=0): %d writes", len(got))
	}

	// Consumer durably writes the first 3 rows. Now g50 (covers 3) is safe
	// but g100 (covers 6) is NOT. The checkpoint must persist g50, never
	// g100.
	(&vstreamSnapshotRows{snap: s}).AdvanceDurableRows(3)
	last = s.maybeCheckpoint(context.Background(), last)
	if len(got) != 1 {
		t.Fatalf("expected exactly one checkpoint once 3 rows are durable; got %d", len(got))
	}
	if g := gtidOf(t, got[0]); g != "MySQL56/abc:1-50" {
		t.Fatalf("checkpoint persisted %q; want the DURABLE breadcrumb MySQL56/abc:1-50 (not the received frontier MySQL56/abc:1-100)", g)
	}
	if s.rowsSinceCheckpoint != 0 {
		t.Errorf("rowsSinceCheckpoint not reset after a durable checkpoint: %d", s.rowsSinceCheckpoint)
	}

	// Consumer durably writes the remaining 3 (6 total). Now g100 is safe.
	(&vstreamSnapshotRows{snap: s}).AdvanceDurableRows(3)
	s.rowsSinceCheckpoint = 3
	_ = s.maybeCheckpoint(context.Background(), last)
	if len(got) != 2 {
		t.Fatalf("expected a second checkpoint once all 6 rows are durable; got %d", len(got))
	}
	if g := gtidOf(t, got[1]); g != "MySQL56/abc:1-100" {
		t.Errorf("second checkpoint persisted %q; want MySQL56/abc:1-100 (the now-durable frontier)", g)
	}
}

// TestVStreamSnapshot_CheckpointNeverAheadOfDurable is the structural
// v0.99.9 pin: across an interleaving of received-VGTID boundaries and
// durable-write acks, EVERY persisted checkpoint position is one whose
// covered rows are all durably written — the checkpoint is never ahead of
// the durable frontier. This is the exact condition the real-PlanetScale
// repro caught (persisted cursor 5.1M ids ahead of the durable MAX).
func TestVStreamSnapshot_CheckpointNeverAheadOfDurable(t *testing.T) {
	s := newTestSnapshotStream()
	s.checkpointRows = 1 // checkpoint at every opportunity
	s.checkpointInterval = time.Hour

	// breadcrumbCoverage maps a GTID to the durable-row count required for
	// it to be safe (its rowsCovered). Any persisted GTID must have
	// coverage <= the durable frontier at persist time.
	coverage := map[string]int64{
		"MySQL56/abc:1-10": 10,
		"MySQL56/abc:1-20": 20,
		"MySQL56/abc:1-30": 30,
		"MySQL56/abc:1-40": 40,
	}

	durable := int64(0)
	s.checkpointFn = func(_ context.Context, pos ir.Position) error {
		g := gtidOf(t, pos)
		cov, known := coverage[g]
		if !known {
			t.Fatalf("checkpoint persisted an unknown position %q", g)
		}
		if cov > durable {
			t.Fatalf("INVARIANT VIOLATION: persisted %q (covers %d rows) but only %d rows are durable — checkpoint ahead of durable frontier", g, cov, durable)
		}
		return nil
	}

	// Pump receives all four VGTID boundaries up front (rows buffered, not
	// yet durable) — the worst case where the received frontier is far
	// ahead of the consumer.
	seedBreadcrumb(t, s, "MySQL56/abc:1-10", 10)
	seedBreadcrumb(t, s, "MySQL56/abc:1-20", 20)
	seedBreadcrumb(t, s, "MySQL56/abc:1-30", 30)
	seedBreadcrumb(t, s, "MySQL56/abc:1-40", 40)

	last := time.Now()
	// Advance the durable frontier in irregular steps, checkpointing after
	// each. The checkpointFn assertion enforces the invariant every time.
	for _, step := range []int64{5, 10, 8, 7, 10} { // cumulative: 5,15,23,30,40
		(&vstreamSnapshotRows{snap: s}).AdvanceDurableRows(step)
		durable += step
		s.rowsSinceCheckpoint = 1
		last = s.maybeCheckpoint(context.Background(), last)
	}
}

// TestVStreamSnapshot_CheckpointNilFnNoOp confirms a stream with no
// wired checkpoint sink (the non-cold-start / pre-ADR-0072 path) never
// checkpoints — position is then persisted only at COPY_COMPLETED.
func TestVStreamSnapshot_CheckpointNilFnNoOp(t *testing.T) {
	s := newTestSnapshotStream()
	s.checkpointRows = 1
	s.currentVgtid = []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "g"}}
	s.rowsSinceCheckpoint = 100
	// checkpointFn is nil.
	if got := s.maybeCheckpoint(context.Background(), time.Now()); got.IsZero() {
		t.Error("maybeCheckpoint returned a zero time for a nil-fn no-op; expected the lastCheckpoint passthrough")
	}
	if s.rowsSinceCheckpoint != 100 {
		t.Errorf("nil-fn checkpoint mutated the row counter: %d", s.rowsSinceCheckpoint)
	}
}

// TestVStreamSnapshot_CheckpointWriteFailureNonFatal confirms a failing
// checkpoint sink does NOT abort the copy — the cursor is re-attempted
// next cadence tick and COPY_COMPLETED persists the final position
// regardless. maybeCheckpoint swallows the write error (logs + continues)
// and never calls failCopy for it.
func TestVStreamSnapshot_CheckpointWriteFailureNonFatal(t *testing.T) {
	s := newTestSnapshotStream()
	s.checkpointRows = 1
	s.checkpointFn = func(_ context.Context, _ ir.Position) error {
		return errors.New("control table write failed")
	}
	s.currentVgtid = []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "g"}}
	s.rowsSinceCheckpoint = 1
	_ = s.maybeCheckpoint(context.Background(), time.Now())
	if err := s.Err(); err != nil {
		t.Errorf("a failed checkpoint write set a terminal copy error (should be non-fatal): %v", err)
	}
}
