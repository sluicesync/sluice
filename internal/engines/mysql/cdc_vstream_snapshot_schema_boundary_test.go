// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// F7c regression pins for the cold-start→CDC snapshot-stream path.
//
// The standalone [vstreamCDCReader] emits an ir.SchemaSnapshot on a
// true-delta FIELD signature change (cdc_vstream_schema_history_test.go
// pins that). But a VStream source that COLD-STARTS runs its post-COPY
// CDC through [vstreamSnapshotStream.dispatchCDCEvent], NOT through the
// standalone reader. Before F7c that FIELD branch only cached the field
// list and dropped the boundary signal entirely — so an online
// ADD/DROP/MODIFY COLUMN after a cold-start never reached the ADR-0091
// schema-forward intercept (nor the ADR-0049 schema-history write). The
// post-DDL ROW then decoded with the new field set against a target
// whose schema was never altered: SQLSTATE 42703 on PG / 1054 on MySQL.
//
// These pins drive dispatchCDCEvent directly and assert it now emits the
// SAME SchemaSnapshot boundary the standalone reader does.

// drainSnapshotStreamSchemas runs the snapshot stream's CDC dispatch
// over evs and returns every ir.SchemaSnapshot emitted.
func drainSnapshotStreamSchemas(t *testing.T, s *vstreamSnapshotStream, evs []*binlogdata.VEvent) []ir.SchemaSnapshot {
	t.Helper()
	out := make(chan ir.Change, 64)
	for _, ev := range evs {
		if err := s.dispatchCDCEvent(context.Background(), ev, out); err != nil {
			t.Fatalf("dispatchCDCEvent(%v): %v", ev.GetType(), err)
		}
	}
	close(out)
	var snaps []ir.SchemaSnapshot
	for c := range out {
		if sn, ok := c.(ir.SchemaSnapshot); ok {
			snaps = append(snaps, sn)
		}
	}
	return snaps
}

func newVStreamSnapshotTestStream() *vstreamSnapshotStream {
	return &vstreamSnapshotStream{
		keyspace: "ks",
		fields:   make(map[string][]*query.Field),
	}
}

// TestVStreamSnapshotCDC_TrueDelta_RealAlter is the F7c headline pin: an
// ADD COLUMN (a true-delta FIELD signature change) on the cold-start→CDC
// path emits exactly one SchemaSnapshot, anchored at the FIELD event's
// own position, carrying the new column. This is the boundary the
// pre-F7c dispatchCDCEvent silently dropped.
func TestVStreamSnapshotCDC_TrueDelta_RealAlter(t *testing.T) {
	s := newVStreamSnapshotTestStream()
	v1 := []*query.Field{
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "email", Type: query.Type_VARCHAR, ColumnType: "varchar(255)"},
	}
	v2 := []*query.Field{ // ADD COLUMN country
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "email", Type: query.Type_VARCHAR, ColumnType: "varchar(255)"},
		{Name: "country", Type: query.Type_VARCHAR, ColumnType: "varchar(2)"},
	}
	snaps := drainSnapshotStreamSchemas(t, s, []*binlogdata.VEvent{
		vgtidEvent("gtid-pre"),
		fieldEvent(v1),
		// DDL boundary: VStream advances the VGTID then re-emits FIELD.
		vgtidEvent("gtid-post-ddl"),
		fieldEvent(v2),
	})
	if len(snaps) != 2 {
		t.Fatalf("want 2 versions (initial + post-ALTER), got %d "+
			"(pre-F7c: the cold-start→CDC path emitted 0 — the boundary "+
			"was silently dropped, the soak's 42703/1054 root cause)", len(snaps))
	}
	if snaps[0].QualifiedName() != "ks.users" {
		t.Errorf("snapshot[0] qualified name = %q, want ks.users", snaps[0].QualifiedName())
	}
	post := snaps[1]
	decoded, ok, err := decodeVStreamPos(post.Position)
	if err != nil || !ok {
		t.Fatalf("post-DDL anchor decode: ok=%v err=%v", ok, err)
	}
	if len(decoded) != 1 || decoded[0].Gtid != "gtid-post-ddl" {
		t.Errorf("post-DDL version anchored at %+v, want gtid-post-ddl "+
			"(the FIELD event's own position)", decoded)
	}
	if post.IR == nil || len(post.IR.Columns) != 3 {
		t.Errorf("post-ALTER snapshot has %d columns, want 3 (country added)",
			len(columnsOf(post.IR)))
	}
}

// TestVStreamSnapshotCDC_TrueDelta_NoOpReEmit pins the dedup contract on
// the cold-start→CDC path: VStream re-emits a byte-identical FIELD on
// stream (re)start / first-touch with no DDL; that must NOT produce a
// second SchemaSnapshot (retention ∝ DDL count; ADR-0049 DP-1). Without
// this dedup the fix would over-emit phantom versions.
func TestVStreamSnapshotCDC_TrueDelta_NoOpReEmit(t *testing.T) {
	s := newVStreamSnapshotTestStream()
	cols := []*query.Field{
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "email", Type: query.Type_VARCHAR, ColumnType: "varchar(255)"},
	}
	snaps := drainSnapshotStreamSchemas(t, s, []*binlogdata.VEvent{
		vgtidEvent("gtid-1"),
		fieldEvent(cols),
		// identical FIELD re-emit (reader restart / first-touch).
		vgtidEvent("gtid-1"),
		fieldEvent(cols),
	})
	if len(snaps) != 1 {
		t.Fatalf("want exactly 1 schema version (initial), got %d (no-op re-emit bloat)", len(snaps))
	}
}

// TestVStreamSnapshotCDC_LoudFloorPreserved: a ROW event for a table
// with no preceding FIELD event must STILL be the existing loud hard
// error on the snapshot-stream CDC path — F7c must not weaken the floor.
func TestVStreamSnapshotCDC_LoudFloorPreserved(t *testing.T) {
	s := newVStreamSnapshotTestStream()
	out := make(chan ir.Change, 4)
	rowEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName:  "users",
			Keyspace:   "ks",
			Shard:      "-",
			RowChanges: []*binlogdata.RowChange{{After: &query.Row{}}},
		},
	}
	if err := s.dispatchCDCEvent(context.Background(), rowEv, out); err == nil {
		t.Fatal("ROW without preceding FIELD: want loud error, got nil (loud floor broken)")
	}
}

// columnsOf is a nil-safe column-slice accessor for the failure message
// above (post.IR may be nil on a regression).
func columnsOf(t *ir.Table) []*ir.Column {
	if t == nil {
		return nil
	}
	return t.Columns
}
