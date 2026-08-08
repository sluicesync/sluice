// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChunkReader_DeclaredCodecMismatchIsLoud is the decode-mismatch pin the
// 2026-08-08 invariant sweep found missing.
//
// # The claim it grades
//
// This package's doc says the per-segment codec is READ from lineage.json and
// "NEVER inferred from the chunk bytes", and gives the reason: a restore that
// guessed would mis-decode a `none` chunk whose first byte happened to look
// like a gzip magic prefix. The refuse-to-sniff half is a real design decision
// and it is honoured. What nothing checked is the OTHER side of the same coin —
// what happens when the recorded codec is wrong. Every existing test in this
// package pairs a writer and a reader on the SAME codec constant, so the entire
// mismatch surface was writer-agreeing-with-writer.
//
// That matters because a wrong-but-VALID recorded codec is not a hypothetical:
// it is what a truncated or hand-edited lineage.json, a partially-rebuilt
// catalog, or a `--rebuild-catalog` over a mixed-codec chain produces. The
// existing refusals cover an UNKNOWN codec string (ValidateRecordedCodec) and a
// disagreeing SNIFF (TestRebuildCatalog_MixedCodecRefused); neither is this.
//
// # Why the matrix and not one cell
//
// The three codecs do not share a failure mechanism, so one representative
// proves nothing about the others (the Bug 74 lesson, applied to a compression
// codec rather than a value codec):
//
//   - gzip and zstd VALIDATE a magic header, so a mismatch fails inside
//     newCodecReader, before a byte of payload is read.
//   - CodecNone does NOT. nopCodecReadCloser is a pass-through that validates
//     nothing, so compressed bytes flow straight into the line scanner and the
//     failure surfaces later and differently — at the JSON header decode or the
//     MaxChunkLineBytes limit. Before this test, "it fails loudly" on that arm
//     was an inference from reading nopCodecReadCloser, not an observation.
//
// The 3x3 is therefore the pin, with the diagonal as the over-refusal control:
// a matching codec must still round-trip, or "everything fails" would satisfy
// the mismatch cells for the wrong reason.
//
// # Mutation record, including the one that did NOT bite
//
// The mutation that proves this test's power is a SNIFF FALLBACK in
// NewChunkReader — buffer the chunk, SniffCodec it, use that instead of the
// recorded codec. That is exactly the behaviour codec.go's package doc forbids,
// and it fails all six off-diagonal cells (each reports 64 rows silently
// assembled).
//
// Recorded because it surprised: a NAIVE lenient fallback — newCodecReader
// returning nopCodecReadCloser when gzip.NewReader errors — leaves this test
// GREEN. It is an EQUIVALENT MUTANT, not a coverage gap: gzip.NewReader has
// already consumed the ten header bytes off the shared io.Reader by the time it
// fails, so the fallback reads a truncated stream and the JSON header decode
// refuses anyway. The lesson is that this test measures "a mismatch does not
// silently assemble", not "newCodecReader returns an error" — which is the
// property worth having, but is not what the first mutation appeared to check.
func TestChunkReader_DeclaredCodecMismatchIsLoud(t *testing.T) {
	codecs := []Codec{CodecNone, CodecGzip, CodecZstd}

	colNames := []string{"id", "name"}
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "name", Type: ir.Text{}},
	}
	// Enough rows that a mis-decoded stream cannot accidentally yield the
	// right row count from a short read.
	rows := make([]ir.Row, 0, 64)
	for i := range 64 {
		rows = append(rows, ir.Row{"id": int64(i), "name": "row-payload-that-compresses-well"})
	}

	for _, wrote := range codecs {
		for _, declared := range codecs {
			t.Run(string(wrote)+"-read-as-"+string(declared), func(t *testing.T) {
				var buf bytes.Buffer
				w, err := NewChunkWriter(&buf, colNames, nil, wrote, nil)
				if err != nil {
					t.Fatalf("NewChunkWriter(%s): %v", wrote, err)
				}
				for _, row := range rows {
					if err := w.WriteRow(row, cols); err != nil {
						t.Fatalf("WriteRow: %v", err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatalf("writer Close: %v", err)
				}
				hash := w.Hash()

				src := io.NopCloser(bytes.NewReader(buf.Bytes()))
				rdr, rerr := NewChunkReader(src, hash, nil, declared, nil)

				if wrote == declared {
					// Control: the diagonal must still work end to end.
					if rerr != nil {
						t.Fatalf("matching codec %s failed to open: %v — the mismatch cells below would "+
							"then be passing for the wrong reason", wrote, rerr)
					}
					n := 0
					for {
						got, err := rdr.ReadRow()
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							t.Fatalf("matching codec %s: ReadRow: %v", wrote, err)
						}
						if got["id"] != rows[n]["id"] {
							t.Fatalf("row %d: id = %v; want %v", n, got["id"], rows[n]["id"])
						}
						n++
					}
					if n != len(rows) {
						t.Fatalf("matching codec %s: read %d rows; want %d", wrote, n, len(rows))
					}
					return
				}

				// Mismatch. Loud is the requirement; WHERE it is loud differs
				// by arm, which is why the failure is allowed to surface at
				// open OR at the first read rather than being pinned to one.
				if rerr != nil {
					return // gzip/zstd magic rejected the payload at construction
				}
				sawErr := false
				decoded := 0
				for {
					_, err := rdr.ReadRow()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						sawErr = true
						break
					}
					decoded++
				}
				if !sawErr {
					t.Fatalf("a chunk written with %s and declared %s decoded %d rows and ended at EOF with "+
						"NO error. A wrong recorded codec must fail loudly, never assemble silently — that "+
						"is the whole reason the codec is recorded rather than sniffed",
						wrote, declared, decoded)
				}
				if decoded > 0 {
					t.Fatalf("a chunk written with %s and declared %s yielded %d rows BEFORE failing. "+
						"Partial silent assembly is the loss shape: a restore would have written those "+
						"rows to the target", wrote, declared, decoded)
				}
			})
		}
	}
}
