// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The change-chunk lane had NO byte ceiling (audit 2026-08-05 C-3).
//
// Item 116 P3 added a byte ceiling to the DATA-chunk writer, and both its
// commit message and its roadmap entry enumerated the buffering paths as
// covered. That enumeration was false for this lane: `backup incremental`
// rolled its change chunks on EVENT COUNT alone while the chunk accumulated in
// an in-memory buffer, so the shipped default of 100,000 events could buffer
// arbitrarily much before rolling. With the per-row limit at 64 MiB, a wide
// TEXT/JSON/BLOB column arriving over CDC is exactly the shape that reaches it.
//
// The rolling DECISION lives in the caller (internal/pipeline), but it can only
// be made if the writer reports its accumulated size — which it did not. This
// pins the reporting half: BytesWritten must track the uncompressed JSON Lines
// the writer accepted, so a caller's ceiling is comparing against something
// real rather than against a number that never moves.

package blobcodec

import (
	"bytes"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func bytesWrittenInsert(id int64, width int) ir.Insert {
	return ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: "0/1"},
		Schema:   "public",
		Table:    "wide",
		Row:      ir.Row{"id": id, "body": strings.Repeat("x", width)},
	}
}

// TestChangeChunkWriter_BytesWrittenTracksPayload is the reporting half of the
// fix. The expected value is INDEPENDENT of the writer's own accounting: it is
// derived from the payload widths the test wrote, not from a second call to
// the writer.
func TestChangeChunkWriter_BytesWrittenTracksPayload(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewChangeChunkWriter(&buf, nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	if got := w.BytesWritten(); got != 0 {
		t.Errorf("fresh writer reports %d bytes written; want 0", got)
	}

	const (
		width  = 4 << 10
		events = 16
	)
	var prev int64
	for i := range events {
		if err := w.WriteChange(bytesWrittenInsert(int64(i), width)); err != nil {
			t.Fatalf("WriteChange: %v", err)
		}
		got := w.BytesWritten()
		if got <= prev {
			t.Fatalf("BytesWritten did not grow after event %d: %d then %d.\n\n"+
				"A caller rolling on this number would never roll — which is the defect: the "+
				"change-chunk lane rolled on event count alone and buffered without bound.",
				i, prev, got)
		}
		prev = got
	}

	// It must be in the right ORDER OF MAGNITUDE, not merely monotonic — a
	// counter that ticked 1 per event would also pass a monotonicity check and
	// would still leave the ceiling unreachable.
	wantAtLeast := int64(events * width)
	if prev < wantAtLeast {
		t.Errorf("BytesWritten reports %d after %d events of %d-byte payloads; want at least %d.\n\n"+
			"The number must track the actual payload, or a byte ceiling compares against a "+
			"counter that never reaches it.", prev, events, width, wantAtLeast)
	}
	// And it must not wildly overstate — the envelope is small relative to a
	// 4 KiB payload.
	if prev > wantAtLeast*2 {
		t.Errorf("BytesWritten reports %d, more than double the %d bytes of payload written",
			prev, wantAtLeast)
	}
}

// UNCOMPRESSED, deliberately — the same property the data-chunk lane pins.
// Chunk boundaries feed the content-addressed same-path upload skip, so where a
// chunk ends must not depend on how well it happened to compress; otherwise the
// day a codec or its level changed, every boundary would move and every
// existing chunk would re-upload.
func TestChangeChunkWriter_BytesWrittenIsPreCompression(t *testing.T) {
	// Highly compressible payload: gzip will shrink it enormously.
	const width = 32 << 10
	measure := func(codec Codec) int64 {
		var buf bytes.Buffer
		w, err := NewChangeChunkWriter(&buf, nil, codec, nil)
		if err != nil {
			t.Fatalf("NewChangeChunkWriter(%v): %v", codec, err)
		}
		for i := range 8 {
			if err := w.WriteChange(bytesWrittenInsert(int64(i), width)); err != nil {
				t.Fatalf("WriteChange: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return w.BytesWritten()
	}

	plain := measure(CodecNone)
	gzipped := measure(CodecGzip)
	if plain != gzipped {
		t.Errorf("BytesWritten differs by codec: %d (none) vs %d (gzip).\n\n"+
			"It must count bytes BEFORE compression, or chunk boundaries move when a codec or "+
			"its level changes — and every existing chunk re-uploads.", plain, gzipped)
	}
}
