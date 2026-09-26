// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestStampExactNumbersOnSeal_StreamAndFillLane pins the format bump at
// [changeChunkBuffer.flushTo] — the seal site `backup stream` rollovers and
// the ADD COLUMN fill share. A postgres-trigger segment whose chunk carried
// a json.Number (top level, list element, map leaf) is stamped
// FormatVersionExactNumbers; the controls — the same engine with no
// json.Number, another engine with one, and a FULL — keep their version.
// The independent expected value is the stated rule, graded per cell.
func TestStampExactNumbersOnSeal_StreamAndFillLane(t *testing.T) {
	const start = irbackup.FormatVersionLegacy
	cases := []struct {
		name   string
		engine string
		kind   string
		row    ir.Row
		want   int
	}{
		{"pgtrigger/scalar", blobcodec.PreservedNumberEngine, irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "n": json.Number("10.50")}, irbackup.FormatVersionExactNumbers},
		{"pgtrigger/array-element", blobcodec.PreservedNumberEngine, irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "a": []any{int64(1), json.Number("1e-400")}}, irbackup.FormatVersionExactNumbers},
		{"pgtrigger/jsonb-leaf", blobcodec.PreservedNumberEngine, irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "j": map[string]any{"u": json.Number("1.500")}}, irbackup.FormatVersionExactNumbers},
		{"pgtrigger/jsonb-16-digit-integer", blobcodec.PreservedNumberEngine, irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "j": map[string]any{"b": json.Number("9007199254740993")}}, irbackup.FormatVersionExactNumbers},
		{"pgtrigger/jsonb-negative-zero", blobcodec.PreservedNumberEngine, irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "j": map[string]any{"z": json.Number("-0")}}, irbackup.FormatVersionExactNumbers},
		{"pgtrigger/jsonb-small-integers", blobcodec.PreservedNumberEngine, irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "j": map[string]any{"n": json.Number("5"), "m": json.Number("-999999999999999"), "z": json.Number("0"), "l": []any{json.Number("7")}}}, start},
		{"pgtrigger/no-number", blobcodec.PreservedNumberEngine, irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "s": "10.50", "f": 1.5}, start},
		{"postgres/with-number", "postgres", irbackup.BackupKindIncremental, ir.Row{"id": int64(1), "n": json.Number("10.50")}, start},
		{"pgtrigger/full", blobcodec.PreservedNumberEngine, irbackup.BackupKindFull, ir.Row{"id": int64(1), "n": json.Number("10.50")}, start},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := &irbackup.Manifest{FormatVersion: start, SourceEngine: c.engine, CreatedAt: time.Unix(0, 0).UTC(), Kind: c.kind}
			stream := &BackupStream{segStore: newMemStore(), segCodec: blobcodec.CodecGzip}
			cb := newFillChunkBuffer(stream, m, nil)
			out := &captureOutcome{}
			if err := cb.appendChange(context.Background(), ir.Insert{Schema: "public", Table: "t", Row: c.row}, 100, out); err != nil {
				t.Fatalf("appendChange: %v", err)
			}
			if err := cb.flushTo(context.Background(), out); err != nil {
				t.Fatalf("flushTo: %v", err)
			}
			if m.FormatVersion != c.want {
				t.Errorf("FormatVersion after seal = %d; want %d", m.FormatVersion, c.want)
			}
		})
	}
}
