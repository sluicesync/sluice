// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The chunk writer rolls on ROW COUNT only (audit 2026-08-01 P3).
//
// backupChunkStreamer.writeRow ends with
//
//	if s.writer.RowCount() >= int64(s.chunkRows) { return s.flush(ctx) }
//
// and that is the only roll condition. The chunk accumulates in an in-memory
// bytes.Buffer until it fires, so the peak buffer is
//
//	chunkRows x average serialized row size
//
// with the row size unbounded and unconsulted. At the default 100,000 rows a
// table of wide rows — a mediumtext or a large json column, the shape a real
// field report just landed on — reaches gigabytes for a single chunk.
//
// Every other buffering path in the tree has a byte cap beside its row cap:
// the batched row writers take ir.MaxBufferBytesSetter, the applier batch loop
// has ByteCap, the PG chunked COPY has pgCopyChunkBytes. The backup chunk
// writer is the one that does not, which is what makes this a gap rather than
// a design choice.
//
// This measures the SHAPE — buffered bytes scale with row width at a fixed row
// count — rather than asserting a threshold, so it documents the exposure
// without becoming a brittle size gate. It is deliberately NOT a fix: adding a
// byte ceiling changes chunk boundaries, and chunk boundaries are load-bearing
// for resume and for the content-addressed same-path upload skip. See roadmap
// item 116.

package backup

import (
	"bytes"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestChunkRoll_IgnoresRowWidth measures how the buffered chunk grows with row
// width at a FIXED row count — the quantity the roll condition never consults.
func TestChunkRoll_IgnoresRowWidth(t *testing.T) {
	const rowsPerChunk = 500

	measure := func(payload int) int {
		table := &ir.Table{
			Schema: "public", Name: "wide",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "body", Type: ir.Text{}},
			},
		}
		rows := make([]ir.Row, 0, rowsPerChunk)
		body := strings.Repeat("x", payload)
		for i := range rowsPerChunk {
			rows = append(rows, ir.Row{"id": int64(i), "body": body})
		}
		return serializedChunkBytes(t, table, rows)
	}

	narrow := measure(64)
	wide := measure(64 << 10) // 64 KiB per row

	t.Logf("%d rows x 64 B  -> buffered %d bytes", rowsPerChunk, narrow)
	t.Logf("%d rows x 64 KiB -> buffered %d bytes", rowsPerChunk, wide)
	if narrow > 0 {
		t.Logf("1024x the row width buffers %.0fx the bytes at the SAME row count — "+
			"the roll condition consults neither", float64(wide)/float64(narrow))
	}

	// The finding: the same number of rows buffers vastly more when rows are
	// wider, because nothing in the roll condition looks at bytes. If this
	// ever stops holding, a byte ceiling has been added and item 116 is done.
	if wide <= narrow*100 {
		t.Errorf("wider rows did not scale the buffer (%d -> %d bytes for 1024x the width); either a byte "+
			"ceiling now exists — in which case close roadmap item 116 and delete this test — or the "+
			"measurement is not reaching the chunk buffer", narrow, wide)
	}

	// Extrapolation, logged rather than asserted so it cannot rot: at the
	// SHIPPED default the same shape is three orders of magnitude larger.
	perRow := float64(wide) / float64(rowsPerChunk)
	t.Logf("extrapolated to the shipped default of %d rows/chunk at this row width: ~%.1f GiB buffered "+
		"for ONE chunk", DefaultBackupChunkRows, perRow*float64(DefaultBackupChunkRows)/(1<<30))
}

// serializedChunkBytes writes rows through the REAL chunk writer and returns
// the buffered byte count — the same bytes.Buffer backupChunkStreamer holds
// open until its row-count roll fires.
func serializedChunkBytes(t *testing.T, table *ir.Table, rows []ir.Row) int {
	t.Helper()
	names := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		names[i] = c.Name
	}
	var buf bytes.Buffer
	w, err := blobcodec.NewChunkWriter(&buf, names, nil, blobcodec.CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, table.Columns); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Len()
}
