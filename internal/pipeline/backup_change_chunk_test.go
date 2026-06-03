// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeChunk_RoundTrip is the load-bearing codec test for the
// Phase 3 change-chunk format: write a representative mix of Change
// kinds, read them back, and verify each variant survives intact
// (kind, table, row maps, position).
func TestChangeChunk_RoundTrip(t *testing.T) {
	pos := func(token string) ir.Position {
		return ir.Position{Engine: "postgres", Token: token}
	}
	in := []ir.Change{
		ir.TxBegin{Position: pos(`{"slot":"sluice_slot","lsn":"0/100"}`)},
		ir.Insert{
			Position: pos(`{"slot":"sluice_slot","lsn":"0/110"}`),
			Schema:   "public",
			Table:    "users",
			Row: ir.Row{
				"id":         int64(1),
				"name":       "Alice",
				"created_at": time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
				"avatar":     []byte{0x01, 0x02, 0x03},
				"active":     true,
			},
		},
		ir.Update{
			Position: pos(`{"slot":"sluice_slot","lsn":"0/120"}`),
			Schema:   "public",
			Table:    "users",
			Before:   ir.Row{"id": int64(1), "name": "Alice"},
			After:    ir.Row{"id": int64(1), "name": "Alice2"},
		},
		ir.Delete{
			Position: pos(`{"slot":"sluice_slot","lsn":"0/130"}`),
			Schema:   "public",
			Table:    "users",
			Before:   ir.Row{"id": int64(1)},
		},
		ir.Truncate{
			Position: pos(`{"slot":"sluice_slot","lsn":"0/140"}`),
			Schema:   "public",
			Table:    "ephemera",
		},
		ir.TxCommit{Position: pos(`{"slot":"sluice_slot","lsn":"0/150"}`)},
	}

	// Encode.
	buf := &bytes.Buffer{}
	w, err := newChangeChunkWriter(buf, nil, CodecGzip)
	if err != nil {
		t.Fatalf("newChangeChunkWriter: %v", err)
	}
	for _, c := range in {
		if err := w.WriteChange(c); err != nil {
			t.Fatalf("WriteChange(%T): %v", c, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if w.ChangeCount() != int64(len(in)) {
		t.Errorf("ChangeCount = %d; want %d", w.ChangeCount(), len(in))
	}
	hash := w.Hash()
	if len(hash) != 64 {
		t.Errorf("hash length = %d; want 64-hex", len(hash))
	}

	// Decode and compare.
	r, err := newChangeChunkReader(nopReadCloserFromBytes(buf.Bytes()), hash, nil, CodecGzip)
	if err != nil {
		t.Fatalf("newChangeChunkReader: %v", err)
	}
	var got []ir.Change
	for {
		c, err := r.ReadChange()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadChange: %v", err)
		}
		got = append(got, c)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("decoded %d changes; want %d", len(got), len(in))
	}
	for i, c := range got {
		// Compare via valuesEquivalent for row maps so int64/JSON-
		// number drift doesn't fail spuriously, and direct compare
		// for the other fields.
		if reflect.TypeOf(c) != reflect.TypeOf(in[i]) {
			t.Errorf("[%d]: type mismatch got %T want %T", i, c, in[i])
			continue
		}
		switch want := in[i].(type) {
		case ir.Insert:
			gotI := c.(ir.Insert)
			if gotI.Schema != want.Schema || gotI.Table != want.Table {
				t.Errorf("[%d] insert id mismatch: got %s.%s want %s.%s",
					i, gotI.Schema, gotI.Table, want.Schema, want.Table)
			}
			if !rowsEquivalent(gotI.Row, want.Row) {
				t.Errorf("[%d] insert row drift: got %+v want %+v", i, gotI.Row, want.Row)
			}
			if gotI.Position != want.Position {
				t.Errorf("[%d] insert pos: got %+v want %+v", i, gotI.Position, want.Position)
			}
		case ir.Update:
			gotU := c.(ir.Update)
			if !rowsEquivalent(gotU.Before, want.Before) || !rowsEquivalent(gotU.After, want.After) {
				t.Errorf("[%d] update row drift", i)
			}
		case ir.Delete:
			gotD := c.(ir.Delete)
			if !rowsEquivalent(gotD.Before, want.Before) {
				t.Errorf("[%d] delete row drift", i)
			}
		case ir.Truncate:
			gotT := c.(ir.Truncate)
			if gotT.Schema != want.Schema || gotT.Table != want.Table {
				t.Errorf("[%d] truncate id mismatch", i)
			}
		case ir.TxBegin:
			gotB := c.(ir.TxBegin)
			if gotB.Position != want.Position {
				t.Errorf("[%d] txbegin pos: got %+v want %+v", i, gotB.Position, want.Position)
			}
		case ir.TxCommit:
			gotC := c.(ir.TxCommit)
			if gotC.Position != want.Position {
				t.Errorf("[%d] txcommit pos: got %+v want %+v", i, gotC.Position, want.Position)
			}
		}
	}
}

// TestChangeChunk_HashMismatch confirms a corrupted chunk surfaces
// ErrChunkHashMismatch on Close.
func TestChangeChunk_HashMismatch(t *testing.T) {
	buf := &bytes.Buffer{}
	w, _ := newChangeChunkWriter(buf, nil, CodecGzip)
	_ = w.WriteChange(ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: `{"slot":"x","lsn":"0/1"}`},
		Schema:   "public",
		Table:    "users",
		Row:      ir.Row{"id": int64(1)},
	})
	_ = w.Close()

	bogusHash := "00000000000000000000000000000000000000000000000000000000deadbeef"
	r, err := newChangeChunkReader(nopReadCloserFromBytes(buf.Bytes()), bogusHash, nil, CodecGzip)
	if err != nil {
		t.Fatalf("newChangeChunkReader: %v", err)
	}
	for {
		_, err := r.ReadChange()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadChange: %v", err)
		}
	}
	if err := r.Close(); !errors.Is(err, ErrChunkHashMismatch) {
		t.Errorf("Close: got %v; want ErrChunkHashMismatch", err)
	}
}

// rowsEquivalent compares two ir.Rows tolerating int / int64 drift
// (the IR row map carries int64s after JSON round-trip; comparing
// to test fixtures via reflect.DeepEqual would fail on plain ints).
func rowsEquivalent(a, b ir.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if !valuesEquivalent(av, bv) {
			return false
		}
	}
	return true
}

// TestChangeChunk_SchemaSnapshot_CollectedNotEncoded pins the ADR-0049
// Chunk D contract that SUPERSEDES the Chunk-B scope-fence skip: the
// CDC reader emits ir.SchemaSnapshot on the same stream the incremental-
// backup writer consumes. Chunk D's invariants:
//
//  1. WriteChange(SchemaSnapshot) returns nil — a DDL during a backup
//     window must not abort the backup via encodeChange's unknown-type
//     loud error.
//  2. ChangeCount() is NOT incremented — snapshots ride the manifest
//     envelope, not the per-row JSONL stream; the chunk bytes remain
//     byte-identical to pre-Chunk-B.
//  3. Snapshots() returns the snapshot exactly once — the new Chunk D
//     surface the incremental-backup orchestrator drains at finalisation
//     to populate manifest.SchemaHistory (where pre-Chunk-D had a
//     silent skip, leaving the restore + resume path to fall to the
//     loud ADR-0022 cold-start floor).
//
// This is the supersession test for the Chunk-B
// TestChangeChunk_SchemaSnapshot_SkippedNotErrored regression guard:
// the "no error, no count" invariants stay, the "no collection"
// invariant flips to "collected for the manifest envelope".
func TestChangeChunk_SchemaSnapshot_CollectedNotEncoded(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := newChangeChunkWriter(buf, nil, CodecGzip)
	if err != nil {
		t.Fatalf("newChangeChunkWriter: %v", err)
	}
	snap := ir.SchemaSnapshot{
		Position: ir.Position{Engine: "mysql", Token: "gtid:1-9"},
		Schema:   "app",
		Table:    "users",
		IR:       &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}
	if err := w.WriteChange(snap); err != nil {
		t.Fatalf("WriteChange(SchemaSnapshot) must succeed, not error; got %v", err)
	}
	if w.ChangeCount() != 0 {
		t.Fatalf("SchemaSnapshot must not be counted (rides the Manifest envelope, not the JSONL stream); count=%d", w.ChangeCount())
	}
	got := w.Snapshots()
	if len(got) != 1 {
		t.Fatalf("Snapshots() must return the snapshot exactly once; got %d entries", len(got))
	}
	if got[0].Position != snap.Position || got[0].Schema != snap.Schema || got[0].Table != snap.Table {
		t.Errorf("Snapshots()[0] identity mismatch; got %+v want %+v", got[0], snap)
	}
	// A real change after a collected snapshot still works + counts.
	if err := w.WriteChange(ir.Insert{
		Position: ir.Position{Engine: "mysql", Token: "gtid:1-10"},
		Schema:   "app", Table: "users", Row: ir.Row{"id": int64(1)},
	}); err != nil {
		t.Fatalf("WriteChange(Insert) after collected snapshot: %v", err)
	}
	if w.ChangeCount() != 1 {
		t.Fatalf("post-snapshot Insert must count; count=%d", w.ChangeCount())
	}
	// Snapshot count does NOT grow with an Insert.
	if len(w.Snapshots()) != 1 {
		t.Fatalf("Snapshots() must not grow with a row-shaped change; got %d entries", len(w.Snapshots()))
	}
}
