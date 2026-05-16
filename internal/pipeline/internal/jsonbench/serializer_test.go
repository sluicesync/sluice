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

// TestSerializerFidelityGate is the load-bearing correctness check for
// the EXTENDED (msgpack + JSON) matrix. Same bar as TestFidelityGate:
// every serializer must round-trip the value contract or fail here and
// be reported DISQUALIFIED in the markdown.
func TestSerializerFidelityGate(t *testing.T) {
	for _, s := range allSerializers() {
		s := s
		t.Run(s.Name, func(t *testing.T) {
			// PASS = value contract met, format self-describing.
			// PASS* = value contract met bit-exact but the native model
			// requires out-of-band schema for timestamp/decimal typing
			// (a documented format-redesign cost, NOT data loss — see
			// serializer_fidelity.go). Both are non-disqualifying; any
			// other verdict (a real lossy/divergent round-trip) fails.
			verdict := checkOneSerializer(s)
			if verdict != "PASS" && verdict != "PASS*" {
				t.Errorf("%s (%s): %s", s.Name, s.Surface, verdict)
			}
		})
	}
}

// TestRunSerializersAndEmit drives RunAllSerializers +
// FormatSerializerMarkdown end-to-end and writes the evidence report.
//
// Default (~50k rows/corpus):
//
//	go test -tags=jsonbench -run TestRunSerializersAndEmit \
//	    ./internal/pipeline/internal/jsonbench/
//
// Decision-grade pass to a file (Windows: absolute path; go test
// ignores /tmp on Windows):
//
//	SLUICE_JSONBENCH_ROWS=200000 \
//	SLUICE_JSONBENCH_OUT=C:\Temp\msgpack-vs-json.md \
//	go test -tags=jsonbench -timeout=90m -run TestRunSerializersAndEmit \
//	    ./internal/pipeline/internal/jsonbench/
func TestRunSerializersAndEmit(t *testing.T) {
	rows := CorpusRowCount
	if v := os.Getenv("SLUICE_JSONBENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rows = n
		}
	}
	results, err := RunAllSerializers(rows)
	if err != nil {
		t.Fatalf("RunAllSerializers: %v", err)
	}
	wantPairs := len(corpusGens) * len(allSerializers())
	if len(results) != wantPairs {
		t.Errorf("result count = %d; want %d", len(results), wantPairs)
	}

	var buf bytes.Buffer
	if err := FormatSerializerMarkdown(&buf, results); err != nil {
		t.Fatalf("FormatSerializerMarkdown: %v", err)
	}
	if buf.Len() == 0 {
		t.Errorf("FormatSerializerMarkdown wrote nothing")
	}

	out := os.Getenv("SLUICE_JSONBENCH_OUT")
	if out == "" {
		out = filepath.Join(t.TempDir(), "msgpack-vs-json.md")
	}
	if err := os.WriteFile(out, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write %s: %v", out, err)
	}
	t.Logf("wrote evidence report to %s (%d bytes)", out, buf.Len())
}
