//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestFidelityGate is the load-bearing correctness check. For every
// registered library it encodes the sluice tagged-value envelope and
// decodes it back through sluice's decodeValue contract, asserting
// bit-exact / semantically-identical recovery of int64 (no float64
// coercion), uint64, []byte, decimal-as-string, RFC3339 timestamps,
// bools, NULLs, and nested envelopes. A lossy or divergent library
// FAILS the test (and is reported DISQUALIFIED in the markdown) — speed
// is irrelevant for a candidate that can't satisfy the value contract.
func TestFidelityGate(t *testing.T) {
	for _, l := range allLibs {
		l := l
		t.Run(l.Name, func(t *testing.T) {
			verdict := checkOne(l)
			if verdict != "PASS" {
				t.Errorf("%s (%s): %s", l.Name, l.Surface, verdict)
			}
		})
	}
}

// TestHTMLEscapeBehaviour records (not asserts) each candidate's
// HTML-escaping behaviour relative to stdlib. sluice's production chunk
// path uses stdlib `encoding/json` which HTML-escapes; a candidate that
// does not produces format-observable / SHA-256-different bytes even
// though round-trip correctness is unaffected. The report needs this
// distinction, so it is surfaced as a logged table here and folded into
// the markdown's fidelity section.
func TestHTMLEscapeBehaviour(t *testing.T) {
	rep := htmlEscapeReport()
	for _, l := range allLibs {
		t.Logf("%-16s %s", l.Name, rep[l.Name])
	}
}

// TestRunAllAndEmit drives RunAll + FormatMarkdown end-to-end and
// writes the report. Mirrors compressbench's entrypoint.
//
// Default (~50k rows/corpus, ~30s):
//
//	go test -tags=jsonbench -run TestRunAllAndEmit \
//	    ./internal/pipeline/internal/jsonbench/
//
// Decision-grade 1M-row pass to a file (Windows: use an absolute path
// or the system temp dir — `go test` ignores /tmp on Windows):
//
//	SLUICE_JSONBENCH_ROWS=1000000 \
//	SLUICE_JSONBENCH_OUT=C:\Temp\json-benchmark.md \
//	go test -tags=jsonbench -timeout=30m -run TestRunAllAndEmit \
//	    ./internal/pipeline/internal/jsonbench/
func TestRunAllAndEmit(t *testing.T) {
	rows := CorpusRowCount
	if v := os.Getenv("SLUICE_JSONBENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rows = n
		}
	}
	results, err := RunAll(rows)
	if err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	wantPairs := len(corpusGens) * len(allLibs)
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

	out := os.Getenv("SLUICE_JSONBENCH_OUT")
	if out == "" {
		out = filepath.Join(t.TempDir(), "json-benchmark.md")
	}
	if err := os.WriteFile(out, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write %s: %v", out, err)
	}
	t.Logf("wrote benchmark report to %s (%d bytes)", out, buf.Len())
}

// BenchmarkEncode / BenchmarkDecode give Go-testing.B allocator-stable
// numbers when the warm-median path in RunAll needs tightening. Uses a
// small corpus for harness speed; the decision numbers come from
// TestRunAllAndEmit at scale.
func BenchmarkEncode(b *testing.B) {
	corpora := generateCorpora(2_000)
	for _, c := range corpora {
		c := c
		for _, l := range allLibs {
			l := l
			b.Run(c.Name+"/"+l.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range c.Records {
						if _, err := l.Marshal(c.Records[j]); err != nil {
							b.Fatalf("marshal: %v", err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	corpora := generateCorpora(2_000)
	for _, c := range corpora {
		c := c
		for _, l := range allLibs {
			l := l
			enc := make([][]byte, len(c.Records))
			for j := range c.Records {
				bb, err := l.Marshal(c.Records[j])
				if err != nil {
					b.Fatalf("pre-encode: %v", err)
				}
				cp := make([]byte, len(bb))
				copy(cp, bb)
				enc[j] = cp
			}
			b.Run(c.Name+"/"+l.Name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range enc {
						if _, err := decodeLine(enc[j], l); err != nil {
							b.Fatalf("decode: %v", err)
						}
					}
				}
			})
		}
	}
}
