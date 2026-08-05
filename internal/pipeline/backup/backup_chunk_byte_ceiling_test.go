// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The chunk writer rolls on ROW COUNT **and** BYTES (roadmap item 116 P3).
//
// Until v0.111.x, backupChunkStreamer.writeRow ended with
//
//	if s.writer.RowCount() >= int64(s.chunkRows) { return s.flush(ctx) }
//
// and that was the only roll condition. The chunk accumulates in an in-memory
// bytes.Buffer until it fires, so the peak buffer was
//
//	chunkRows x average serialized row size
//
// with the row size unbounded and unconsulted. Measured below: at a fixed 500
// rows, widening rows from 64 B to 64 KiB grew the buffered chunk 644x, which
// extrapolates to ~6.1 GiB for ONE chunk at the shipped 100,000-row default —
// the shape a real field report landed on, a wide mediumtext or json column.
//
// Every other buffering path in the tree already had a byte cap beside its row
// cap: the batched row writers take ir.MaxBufferBytesSetter, the applier batch
// loop has ByteCap, the PG chunked COPY has pgCopyChunkBytes. The backup chunk
// writer was the one that did not, which is what made it a gap rather than a
// design choice.
//
// # What these tests pin, and why the ceiling is on UNCOMPRESSED bytes
//
// Chunk boundaries are load-bearing beyond memory — resume keys on them, and
// flush's content-addressed same-path upload skip compares a chunk's SHA at its
// allocated path. So where a chunk ends must depend only on the ROWS. The
// ceiling therefore counts the JSON Lines handed to the writer, before
// compression and encryption: a compressed-size trigger would move every
// boundary the day the codec or its level changed, silently invalidating the
// re-run skip for every chunk already in the store. TestChunkByteCeiling_
// BoundariesAreCodecIndependent and TestChunkByteCeiling_RerunSkipsReuploading
// IdenticalChunks are those pins.
//
// The roadmap named RESUME as the other thing chunk boundaries are load-bearing
// for. Checked rather than pinned, because on inspection it is not: resume
// works at TABLE granularity (Backup's doc comment, and Bug 135) — a partially
// written table is RE-STREAMED FROM SCRATCH and its prior chunks are never
// reused, precisely because chunk contents already depended on scan order,
// which is not repeatable across runs. Nothing keys on where a chunk ends, so
// moving a boundary cannot affect it. A test asserting that here would be
// asserting the absence of a coupling that was never there; the coupling that
// IS real is the same-path upload skip, above.

package backup

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// wideRowTable is a two-column table whose second column carries an
// arbitrarily wide payload — the shape whose serialized size the row count
// says nothing about.
func wideRowTable() *ir.Table {
	return &ir.Table{
		Schema: "public", Name: "wide",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
	}
}

// streamWideRows drives the REAL streamer — the production writeRow, its
// roll condition and its flush — over n rows of the given payload width, and
// returns the manifest entry the flushes recorded plus the store they landed
// in. codec selects the chunk codec so a caller can vary compression without
// varying anything else.
func streamWideRows(
	t *testing.T, chunkRows int, chunkBytes int64, n, payload int, codec blobcodec.Codec,
) (*irbackup.TableManifest, *blobcodec.LocalStore) {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	b := &Backup{
		Source: stubEngine{}, SourceDSN: "dsn", Store: store,
		ChunkBytes: chunkBytes, Codec: codec,
	}
	manifest := &irbackup.Manifest{PartialState: irbackup.BackupStateInProgress, Kind: irbackup.BackupKindFull}
	committer := &manifestCommitter{store: store, manifest: manifest}
	table := wideRowTable()
	entry := &irbackup.TableManifest{Name: table.Name, Partial: true}
	committer.stageTable(entry)

	var chunkIdx, rowsTotal atomic.Int64
	s := b.newBackupChunkStreamer(table, entry, chunkRows, committer, nil, &chunkIdx, &rowsTotal)
	body := strings.Repeat("x", payload)
	for i := range n {
		if err := s.writeRow(context.Background(), ir.Row{"id": int64(i), "body": body}); err != nil {
			t.Fatalf("writeRow %d: %v", i, err)
		}
	}
	if err := s.flush(context.Background()); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if got := rowsTotal.Load(); got != int64(n) {
		t.Fatalf("rowsTotal = %d; want %d — the streamer did not see every row", got, n)
	}
	return entry, store
}

// TestChunkByteCeiling_RollsBeforeTheRowCount is the fix itself: wide rows
// must roll on BYTES long before the row count is anywhere near reached.
func TestChunkByteCeiling_RollsBeforeTheRowCount(t *testing.T) {
	const (
		rows      = 200
		payload   = 4 << 10 // 4 KiB per row
		chunkRows = 100_000 // the shipped default: unreachable here
		ceiling   = 64 << 10
	)
	entry, _ := streamWideRows(t, chunkRows, ceiling, rows, payload, blobcodec.CodecNone)

	// 200 rows x ~4 KiB = ~800 KiB against a 64 KiB ceiling: at least a
	// dozen chunks. Asserted as a floor rather than an exact count so the
	// test does not encode the JSON overhead per row.
	if len(entry.Chunks) < 10 {
		t.Fatalf("entry.Chunks = %d; want >= 10.\n\n"+
			"%d rows of %d bytes against a %d-byte ceiling must roll on BYTES — the row cap of %d is "+
			"three orders of magnitude away and can never fire. This is roadmap item 116 P3: a chunk "+
			"buffers in memory until it rolls, so with no byte ceiling this table is ONE chunk.",
			len(entry.Chunks), rows, payload, ceiling, chunkRows)
	}
	var total int64
	for _, c := range entry.Chunks {
		total += c.RowCount
	}
	if total != rows {
		t.Errorf("chunk row counts sum to %d; want %d — rolling must not lose or duplicate a row", total, rows)
	}
}

// TestChunkByteCeiling_NarrowRowsStillRollOnRowCount is the control, and the
// half that matters most: the ceiling must not move a boundary on the ordinary
// shape. Every chunk already in every existing store was written by the row
// cap, and a ceiling that fired early on narrow rows would repartition them.
func TestChunkByteCeiling_NarrowRowsStillRollOnRowCount(t *testing.T) {
	const (
		rows      = 30
		payload   = 8
		chunkRows = 10
	)
	entry, _ := streamWideRows(t, chunkRows, DefaultBackupChunkBytes, rows, payload, blobcodec.CodecNone)

	if len(entry.Chunks) != rows/chunkRows {
		t.Fatalf("entry.Chunks = %d; want %d — narrow rows must still roll on the ROW count, at exactly "+
			"the boundaries they always did. A ceiling that fires here repartitions every existing backup.",
			len(entry.Chunks), rows/chunkRows)
	}
	for i, c := range entry.Chunks {
		if c.RowCount != chunkRows {
			t.Errorf("chunk %d holds %d rows; want %d", i, c.RowCount, chunkRows)
		}
	}
}

// TestChunkByteCeiling_ZeroMeansTheDefaultNotUnlimited pins the v0.99.51
// zero-value rule for this field. Every construction that does not set
// ChunkBytes — every test, the broker paths, the chain paths, any future
// caller — gets the Go zero value, so zero must be the SAFE answer. A field
// where zero meant "no ceiling" would leave the protection off everywhere
// except the one call site that remembered it.
func TestChunkByteCeiling_ZeroMeansTheDefaultNotUnlimited(t *testing.T) {
	if got := (&Backup{}).chunkByteCeiling(); got != DefaultBackupChunkBytes {
		t.Errorf("zero-value Backup.chunkByteCeiling() = %d; want %d (the DEFAULT). Zero must never mean "+
			"unlimited: it is what every caller that never heard of this field gets.", got, DefaultBackupChunkBytes)
	}
	if got := (&Backup{ChunkBytes: -1}).chunkByteCeiling(); got != DefaultBackupChunkBytes {
		t.Errorf("negative ChunkBytes resolved to %d; want the default %d", got, DefaultBackupChunkBytes)
	}
	if got := (&Backup{ChunkBytes: 4096}).chunkByteCeiling(); got != 4096 {
		t.Errorf("explicit ChunkBytes resolved to %d; want 4096", got)
	}
}

// TestChunkByteCeiling_BoundariesAreCodecIndependent is why the ceiling counts
// UNCOMPRESSED bytes. The same rows under two codecs must produce the same
// chunk boundaries: a compressed-size trigger would put them in different
// places, and since flush's re-run skip compares a chunk's SHA at its
// allocated path, every boundary that moved would re-upload.
//
// The payload is deliberately highly compressible, so a compressed-size
// trigger would differ wildly here rather than subtly.
func TestChunkByteCeiling_BoundariesAreCodecIndependent(t *testing.T) {
	const (
		rows      = 100
		payload   = 2 << 10
		chunkRows = 100_000
		ceiling   = 32 << 10
	)
	plain, _ := streamWideRows(t, chunkRows, ceiling, rows, payload, blobcodec.CodecNone)
	gzipped, _ := streamWideRows(t, chunkRows, ceiling, rows, payload, blobcodec.CodecGzip)

	if len(plain.Chunks) != len(gzipped.Chunks) {
		t.Fatalf("codec changed the chunk COUNT: none=%d gzip=%d.\n\n"+
			"The ceiling must count bytes BEFORE compression. Otherwise the day a codec or its level "+
			"changes, every boundary moves and the content-addressed same-path upload skip is invalidated "+
			"for every chunk already in the store.", len(plain.Chunks), len(gzipped.Chunks))
	}
	for i := range plain.Chunks {
		if plain.Chunks[i].RowCount != gzipped.Chunks[i].RowCount {
			t.Errorf("chunk %d holds %d rows under CodecNone and %d under CodecGzip — the boundary moved "+
				"with the codec", i, plain.Chunks[i].RowCount, gzipped.Chunks[i].RowCount)
		}
	}
}

// TestChunkByteCeiling_RerunSkipsReuploadingIdenticalChunks is the first of
// the two gotchas the roadmap named. flush skips the upload when the chunk's
// SHA already matches what sits at its allocated path, so a byte-ceiling roll
// must be DETERMINISTIC — the same rows must land in the same chunks with the
// same bytes on a re-run, or the skip stops skipping.
func TestChunkByteCeiling_RerunSkipsReuploadingIdenticalChunks(t *testing.T) {
	const (
		rows      = 60
		payload   = 2 << 10
		chunkRows = 100_000
		ceiling   = 16 << 10
	)
	first, _ := streamWideRows(t, chunkRows, ceiling, rows, payload, blobcodec.CodecNone)
	second, _ := streamWideRows(t, chunkRows, ceiling, rows, payload, blobcodec.CodecNone)

	if len(first.Chunks) != len(second.Chunks) {
		t.Fatalf("re-run produced %d chunks where the first produced %d — the roll is not deterministic",
			len(second.Chunks), len(first.Chunks))
	}
	for i := range first.Chunks {
		if first.Chunks[i].File != second.Chunks[i].File {
			t.Errorf("chunk %d path moved between runs: %q -> %q", i, first.Chunks[i].File, second.Chunks[i].File)
		}
		if first.Chunks[i].SHA256 != second.Chunks[i].SHA256 {
			t.Errorf("chunk %d SHA moved between runs (%s -> %s).\n\n"+
				"The content-addressed same-path upload skip compares exactly this. A non-deterministic "+
				"byte roll makes every re-run re-upload the whole table.",
				i, first.Chunks[i].SHA256, second.Chunks[i].SHA256)
		}
	}
}

// TestChunkRoll_MeasuresRowWidth is the original measurement (audit P3),
// kept because it is the WHY. It records the exposure the ceiling closes: at a
// fixed row count, the buffered bytes scale with row width without limit. What
// changed is that the streamer no longer lets that reach the roll condition.
func TestChunkRoll_MeasuresRowWidth(t *testing.T) {
	const rowsPerChunk = 500

	measure := func(payload int) int {
		table := wideRowTable()
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
	if wide <= narrow*100 {
		t.Errorf("wider rows did not scale the buffer (%d -> %d bytes for 1024x the width) — the "+
			"measurement is no longer reaching the chunk buffer", narrow, wide)
	}

	perRow := float64(wide) / float64(rowsPerChunk)
	t.Logf("at the shipped default of %d rows/chunk and this row width the ROW cap alone would buffer "+
		"~%.1f GiB for ONE chunk; the %d MiB ceiling now rolls it into ~%.0f chunks instead",
		DefaultBackupChunkRows, perRow*float64(DefaultBackupChunkRows)/(1<<30),
		DefaultBackupChunkBytes>>20, perRow*float64(DefaultBackupChunkRows)/float64(DefaultBackupChunkBytes))
}

// serializedChunkBytes writes rows through the REAL chunk writer and returns
// the buffered byte count — the same bytes.Buffer backupChunkStreamer holds
// open between rolls.
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
