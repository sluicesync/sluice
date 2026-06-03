// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

func collectPGSnapshots(out chan ir.Change) []ir.SchemaSnapshot {
	close(out)
	var snaps []ir.SchemaSnapshot
	for c := range out {
		if s, ok := c.(ir.SchemaSnapshot); ok {
			snaps = append(snaps, s)
		}
	}
	return snaps
}

func pgRelV1() *relationCacheEntry {
	return &relationCacheEntry{
		Schema: "public",
		Name:   "users",
		Columns: []relationColumn{
			{Name: "id", OID: 20, Type: ir.Integer{Width: 64}, KeyColumn: true},
			{Name: "email", OID: 1043, Type: ir.Varchar{Length: 255}},
		},
	}
}

// TestPGSchemaHistory_TrueDelta_NoOpReEmit: pgoutput re-sends a
// RelationMessage on reconnect / first-touch WITHOUT any DDL.
// ADR-0049 DP-1 sign-off point ii — the byte-identical re-emit must
// NOT write a new schema-history version.
func TestPGSchemaHistory_TrueDelta_NoOpReEmit(t *testing.T) {
	r := &CDCReader{schema: "public", slotName: "s"}
	sig := map[uint32]ir.SchemaSignature{}
	out := make(chan ir.Change, 16)

	if err := r.maybeSnapshotSchema(context.Background(), pgRelV1(), 100, pglogrepl.LSN(0x10), sig, out); err != nil {
		t.Fatalf("first relation: %v", err)
	}
	// Identical relation re-emit at a later LSN (reconnect).
	if err := r.maybeSnapshotSchema(context.Background(), pgRelV1(), 100, pglogrepl.LSN(0x99), sig, out); err != nil {
		t.Fatalf("re-emit relation: %v", err)
	}
	snaps := collectPGSnapshots(out)
	if len(snaps) != 1 {
		t.Fatalf("want 1 version (initial), got %d (no-op re-emit bloat)", len(snaps))
	}
}

// TestPGSchemaHistory_TrueDelta_RealAlter: an ADD COLUMN re-sends a
// Relation with a different column set → exactly one new version,
// anchored at the RelationMessage's OWN WAL position (relLSN) — not a
// later row's LSN (locked decision #4c).
func TestPGSchemaHistory_TrueDelta_RealAlter(t *testing.T) {
	r := &CDCReader{schema: "public", slotName: "s"}
	sig := map[uint32]ir.SchemaSignature{}
	out := make(chan ir.Change, 16)

	_ = r.maybeSnapshotSchema(context.Background(), pgRelV1(), 100, pglogrepl.LSN(0x10), sig, out)

	v2 := pgRelV1()
	v2.Columns = append(v2.Columns, relationColumn{Name: "country", OID: 1043, Type: ir.Varchar{Length: 2}})
	const ddlLSN = pglogrepl.LSN(0x4242)
	if err := r.maybeSnapshotSchema(context.Background(), v2, 100, ddlLSN, sig, out); err != nil {
		t.Fatalf("post-ALTER relation: %v", err)
	}

	snaps := collectPGSnapshots(out)
	if len(snaps) != 2 {
		t.Fatalf("want 2 versions (initial + post-ALTER), got %d", len(snaps))
	}
	decoded, ok, err := decodePGPos(snaps[1].Position)
	if err != nil || !ok {
		t.Fatalf("post-ALTER anchor decode: ok=%v err=%v", ok, err)
	}
	if decoded.LSN != ddlLSN.String() {
		t.Errorf("post-ALTER version anchored at LSN %q, want %q (the Relation's own WAL pos, #4c)",
			decoded.LSN, ddlLSN.String())
	}
	if len(snaps[1].IR.Columns) != 3 {
		t.Errorf("post-ALTER snapshot has %d columns, want 3", len(snaps[1].IR.Columns))
	}
}

// TestPGSchemaHistory_OutOfScopeSchemaSkipped: a relation in a schema
// the reader isn't bound to has no schema-history row to host on the
// target — it must not produce a version (mirrors the emit-side
// rel.Schema != r.schema gate).
func TestPGSchemaHistory_OutOfScopeSchemaSkipped(t *testing.T) {
	r := &CDCReader{schema: "public", slotName: "s"}
	sig := map[uint32]ir.SchemaSignature{}
	out := make(chan ir.Change, 4)
	rel := pgRelV1()
	rel.Schema = "other_schema"
	if err := r.maybeSnapshotSchema(context.Background(), rel, 7, pglogrepl.LSN(1), sig, out); err != nil {
		t.Fatalf("out-of-scope relation: %v", err)
	}
	if got := len(collectPGSnapshots(out)); got != 0 {
		t.Errorf("out-of-scope schema produced %d versions, want 0", got)
	}
}
