//go:build compressbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package compressbench

import (
	"bytes"
	"os"
	"strconv"
	"testing"
)

// TestRoundTrip is a correctness sanity check — for every
// (corpus, algorithm) pair, encode and decode should produce
// byte-identical output. A regression here would invalidate any
// throughput / ratio number the harness emits.
func TestRoundTrip(t *testing.T) {
	corpora, err := generateCorpora(2_000) // small for sanity speed
	if err != nil {
		t.Fatalf("generate corpora: %v", err)
	}
	for _, c := range corpora {
		c := c
		for _, a := range allAlgos {
			a := a
			t.Run(c.Name+"/"+a.Name, func(t *testing.T) {
				var enc bytes.Buffer
				w, err := a.NewWriter(&enc)
				if err != nil {
					t.Fatalf("NewWriter: %v", err)
				}
				if _, err := w.Write(c.Data); err != nil {
					t.Fatalf("write: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatalf("close writer: %v", err)
				}
				r, err := a.NewReader(bytes.NewReader(enc.Bytes()))
				if err != nil {
					t.Fatalf("NewReader: %v", err)
				}
				dec := new(bytes.Buffer)
				if _, err := dec.ReadFrom(r); err != nil {
					t.Fatalf("read: %v", err)
				}
				if err := r.Close(); err != nil {
					t.Fatalf("close reader: %v", err)
				}
				if !bytes.Equal(dec.Bytes(), c.Data) {
					t.Fatalf("round-trip mismatch: got %d bytes, want %d",
						dec.Len(), len(c.Data))
				}
			})
		}
	}
}

// TestRunAllAndEmit drives a single end-to-end pass through
// RunAll + FormatMarkdown and writes the result to a file in a
// temp dir. This is the harness's own self-test — it makes sure
// the markdown-emitter shape is rendered without panicking and the
// row count matches (corpora × algos). It does NOT pin throughput
// or ratio numbers (which vary by hardware).
//
// To emit to a real file for the decision doc, run:
//
//	SLUICE_COMPRESSBENCH_OUT=docs/dev/notes/compression-benchmark.md \
//	go test -tags=compressbench -run TestRunAllAndEmit ./internal/pipeline/internal/compressbench/
func TestRunAllAndEmit(t *testing.T) {
	rows := CorpusRowCount
	if v := os.Getenv("SLUICE_COMPRESSBENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rows = n
		}
	}
	results, err := RunAll(rows)
	if err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	wantPairs := 4 * len(allAlgos) // 4 corpora × N algos
	if len(results) != wantPairs {
		t.Errorf("result count = %d; want %d", len(results), wantPairs)
	}

	var buf bytes.Buffer
	if err := FormatMarkdown(&buf, results); err != nil {
		t.Fatalf("FormatMarkdown: %v", err)
	}
	if buf.Len() == 0 {
		t.Errorf("FormatMarkdown wrote nothing")
	}

	out := os.Getenv("SLUICE_COMPRESSBENCH_OUT")
	if out == "" {
		// In normal `go test` runs, dump to the test's temp dir so a
		// developer can inspect it without ceremony.
		out = t.TempDir() + "/compression-benchmark.md"
	}
	if err := os.WriteFile(out, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write %s: %v", out, err)
	}
	t.Logf("wrote benchmark report to %s (%d bytes)", out, buf.Len())
}

// BenchmarkEncode drives Go's testing.B harness across the cartesian
// product. `go test -tags=compressbench -bench=BenchmarkEncode` gives
// allocator-stable, multi-iteration numbers when the single-pass
// numbers from RunAll need tightening.
func BenchmarkEncode(b *testing.B) {
	corpora, err := generateCorpora(2_000)
	if err != nil {
		b.Fatalf("generate corpora: %v", err)
	}
	for _, c := range corpora {
		c := c
		for _, a := range allAlgos {
			a := a
			b.Run(c.Name+"/"+a.Name, func(b *testing.B) {
				b.SetBytes(int64(len(c.Data)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var enc bytes.Buffer
					w, err := a.NewWriter(&enc)
					if err != nil {
						b.Fatalf("NewWriter: %v", err)
					}
					if _, err := w.Write(c.Data); err != nil {
						b.Fatalf("write: %v", err)
					}
					if err := w.Close(); err != nil {
						b.Fatalf("close: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkDecode is the inverse of BenchmarkEncode — precomputes
// encoded bytes once outside the timer, then measures decode-only
// throughput.
func BenchmarkDecode(b *testing.B) {
	corpora, err := generateCorpora(2_000)
	if err != nil {
		b.Fatalf("generate corpora: %v", err)
	}
	for _, c := range corpora {
		c := c
		for _, a := range allAlgos {
			a := a
			// Encode once outside the timer.
			var enc bytes.Buffer
			w, err := a.NewWriter(&enc)
			if err != nil {
				b.Fatalf("NewWriter: %v", err)
			}
			if _, err := w.Write(c.Data); err != nil {
				b.Fatalf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				b.Fatalf("close writer: %v", err)
			}
			encBytes := enc.Bytes()

			b.Run(c.Name+"/"+a.Name, func(b *testing.B) {
				b.SetBytes(int64(len(c.Data)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r, err := a.NewReader(bytes.NewReader(encBytes))
					if err != nil {
						b.Fatalf("NewReader: %v", err)
					}
					if _, err := bytes.NewBuffer(nil).ReadFrom(r); err != nil {
						b.Fatalf("read: %v", err)
					}
					if err := r.Close(); err != nil {
						b.Fatalf("close reader: %v", err)
					}
				}
			})
		}
	}
}
