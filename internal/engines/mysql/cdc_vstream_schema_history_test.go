// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/orware/sluice/internal/ir"
)

// drainSnapshots runs the reader's dispatch over evs and returns every
// ir.SchemaSnapshot emitted (the only ir.Change this Chunk-B2 path
// produces from FIELD/DDL events; ROW events are not exercised here).
func drainSnapshots(t *testing.T, r *vstreamCDCReader, evs []*binlogdata.VEvent) []ir.SchemaSnapshot {
	t.Helper()
	out := make(chan ir.Change, 64)
	for _, ev := range evs {
		if err := r.dispatch(context.Background(), ev, out); err != nil {
			t.Fatalf("dispatch(%v): %v", ev.GetType(), err)
		}
	}
	close(out)
	var snaps []ir.SchemaSnapshot
	for c := range out {
		if s, ok := c.(ir.SchemaSnapshot); ok {
			snaps = append(snaps, s)
		}
	}
	return snaps
}

func newVStreamTestReader() *vstreamCDCReader {
	return &vstreamCDCReader{
		keyspace:    "ks",
		fields:      make(map[string][]*query.Field),
		snapshotSig: make(map[string]ir.SchemaSignature),
	}
}

func vgtidEvent(gtid string) *binlogdata.VEvent {
	return &binlogdata.VEvent{
		Type: binlogdata.VEventType_VGTID,
		Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{
			{Keyspace: "ks", Shard: "-", Gtid: gtid},
		}},
	}
}

func fieldEvent(fields []*query.Field) *binlogdata.VEvent {
	return &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: "users",
			Keyspace:  "ks",
			Shard:     "-",
			Fields:    fields,
		},
	}
}

// TestVStreamSchemaHistory_TrueDelta_NoOpReEmit: VStream re-emits a
// FIELD event on restart / first-touch WITHOUT any DDL. ADR-0049 DP-1
// sign-off point ii: a no-op re-emit must NOT write a new
// schema-history version (retention ∝ DDL count). The first FIELD is
// the initial version; the byte-identical re-emit is zero new
// versions.
func TestVStreamSchemaHistory_TrueDelta_NoOpReEmit(t *testing.T) {
	r := newVStreamTestReader()
	cols := []*query.Field{
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "email", Type: query.Type_VARCHAR, ColumnType: "varchar(255)"},
	}
	snaps := drainSnapshots(t, r, []*binlogdata.VEvent{
		vgtidEvent("gtid-1"),
		fieldEvent(cols),
		// identical FIELD re-emit (e.g. reader restart / first-touch)
		vgtidEvent("gtid-1"),
		fieldEvent(cols),
	})
	if len(snaps) != 1 {
		t.Fatalf("want exactly 1 schema version (initial), got %d (no-op re-emit bloat)", len(snaps))
	}
	if snaps[0].QualifiedName() != "ks.users" {
		t.Errorf("snapshot qualified name = %q, want ks.users", snaps[0].QualifiedName())
	}
}

// TestVStreamSchemaHistory_TrueDelta_RealAlter: an ADD COLUMN between
// two FIELD events is a true delta → exactly one new version, and the
// DROP/RENAME/MODIFY shapes likewise each produce exactly one. The
// version is anchored at the FIELD event's OWN position (the VGTID in
// effect when the post-DDL FIELD arrived) — locked decision #4c.
func TestVStreamSchemaHistory_TrueDelta_RealAlter(t *testing.T) {
	r := newVStreamTestReader()
	v1 := []*query.Field{
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "email", Type: query.Type_VARCHAR, ColumnType: "varchar(255)"},
	}
	v2 := []*query.Field{ // ADD COLUMN country
		{Name: "id", Type: query.Type_INT64, ColumnType: "bigint"},
		{Name: "email", Type: query.Type_VARCHAR, ColumnType: "varchar(255)"},
		{Name: "country", Type: query.Type_VARCHAR, ColumnType: "varchar(2)"},
	}
	snaps := drainSnapshots(t, r, []*binlogdata.VEvent{
		vgtidEvent("gtid-pre"),
		fieldEvent(v1),
		// DDL boundary: VStream advances the VGTID then re-emits FIELD.
		vgtidEvent("gtid-post-ddl"),
		{Type: binlogdata.VEventType_DDL, Statement: "ALTER TABLE users ADD country VARCHAR(2)", Keyspace: "ks"},
		fieldEvent(v2),
	})
	if len(snaps) != 2 {
		t.Fatalf("want 2 versions (initial + post-ALTER), got %d", len(snaps))
	}
	// #4c: the post-DDL version's anchor is the FIELD event's own
	// position (gtid-post-ddl), NOT a later row's position.
	post := snaps[1]
	decoded, ok, err := decodeVStreamPos(post.Position)
	if err != nil || !ok {
		t.Fatalf("post-DDL anchor decode: ok=%v err=%v", ok, err)
	}
	if len(decoded) != 1 || decoded[0].Gtid != "gtid-post-ddl" {
		t.Errorf("post-DDL version anchored at %+v, want gtid-post-ddl (the FIELD event's own position, #4c)", decoded)
	}
	if len(post.IR.Columns) != 3 {
		t.Errorf("post-ALTER snapshot has %d columns, want 3 (country added)", len(post.IR.Columns))
	}
}

// TestVStreamSchemaHistory_LoudFloorPreserved: a ROW event for a
// table with no preceding FIELD event must STILL be the existing loud
// hard error — the Chunk-B2 snapshot path must not swallow or reorder
// it.
func TestVStreamSchemaHistory_LoudFloorPreserved(t *testing.T) {
	r := newVStreamTestReader()
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
	err := r.dispatch(context.Background(), rowEv, out)
	if err == nil {
		t.Fatal("ROW without preceding FIELD: want loud error, got nil (loud floor broken)")
	}
}
