// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestSmartCompaction_PGTriggerChunks_KeepNumericDigits pins the
// compaction reader site: smart compaction DECODES a change chunk and
// RE-ENCODES the collapsed result, so a float64 decode there writes the
// rounded value into the compacted chunk's BYTES — a loss no later reader
// can undo. A postgres-trigger incremental's chunk (json.Number values: a
// long numeric, a jsonb document with an integer past int64, a numeric[]
// with a NULL element) is compacted, then read back with the
// number-preserving reader; every value must equal what was written. The
// independent expected value is the input row.
func TestSmartCompaction_PGTriggerChunks_KeepNumericDigits(t *testing.T) {
	store := newMemStore()
	im := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  blobcodec.PreservedNumberEngine,
		CreatedAt:     time.Now().UTC(),
		Kind:          irbackup.BackupKindIncremental,
		Schema: &ir.Schema{Tables: []*ir.Table{{
			Schema: "public", Name: "nums",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "nu", Type: ir.Decimal{}},
				{Name: "doc", Type: ir.JSON{}},
				{Name: "na", Type: ir.Array{Element: ir.Decimal{}}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}}},
		ChangeChunks: []*irbackup.ChunkInfo{{File: "chunks/_changes/incr-000.jsonl.gz"}},
	}
	after := ir.Row{
		"id":  int64(1),
		"nu":  json.Number("123456789012345678.123456789012"),
		"doc": map[string]any{"big": json.Number("12345678901234567890"), "dec": json.Number("0.100")},
		"na":  []any{json.Number("22222222222222222222.2"), nil},
	}
	passthrough := ir.Row{"id": int64(2), "nu": json.Number("-0.1234567890123456789012345678901"), "doc": nil, "na": nil}
	in := []ir.Change{
		ir.Insert{Position: pos(1), Schema: "public", Table: "nums", Row: ir.Row{"id": int64(1), "nu": json.Number("1"), "doc": nil, "na": nil}},
		ir.Update{Position: pos(2), Schema: "public", Table: "nums", Before: ir.Row{"id": int64(1)}, After: after},
		ir.Insert{Position: pos(3), Schema: "public", Table: "nums", Row: passthrough},
	}
	var buf bytes.Buffer
	ch := im.ChangeChunks[0]
	w, err := blobcodec.NewChangeChunkWriter(&buf, nil, blobcodec.CodecGzip, irbackup.ChangeChunkAADFor(im, ch, 0))
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	for _, c := range in {
		if err := w.WriteChange(c); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Put(context.Background(), ch.File, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("put: %v", err)
	}
	ch.RowCount, ch.SHA256 = w.ChangeCount(), w.Hash()

	res, err := applySmartCompactionToIncrementalSized(context.Background(), store, im, blobcodec.CodecGzip, nil, PKStrategyPK, smartCompactUnboundedChunkBudget)
	if err != nil {
		t.Fatalf("smart compaction: %v", err)
	}
	if res.eventsBefore != 3 || res.eventsAfter != 2 {
		t.Fatalf("events %d → %d; want 3 → 2 (the insert+update must collapse, or the reader site was never re-encoded)", res.eventsBefore, res.eventsAfter)
	}

	rows := map[int64]ir.Row{}
	for i, c := range im.ChangeChunks {
		rc, err := store.Get(context.Background(), c.File)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		cr, err := blobcodec.NewChangeChunkReader(rc, c.SHA256, nil, blobcodec.CodecGzip, irbackup.ChangeChunkAADFor(im, c, i))
		if err != nil {
			t.Fatalf("reader: %v", err)
		}
		cr.PreserveNumbers()
		for {
			chg, err := cr.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if ins, ok := chg.(ir.Insert); ok {
				rows[ins.Row["id"].(int64)] = ins.Row
			}
		}
		if err := cr.Close(); err != nil {
			t.Fatalf("reader close: %v", err)
		}
	}
	for id, want := range map[int64]ir.Row{1: after, 2: passthrough} {
		if got := rows[id]; !reflect.DeepEqual(got, want) {
			t.Errorf("row %d after smart compaction:\n got %#v\nwant %#v", id, got, want)
		}
	}
}
