// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A backup you can take, verify clean, and never restore (Bug 226).
//
// The chunk reader capped a line at 64 MiB; the writer had no cap. A row
// whose serialized form exceeded it was written happily, so:
//
//	backup full    rc=0
//	backup verify  rc=0        <-- reports the backup fine
//	restore        rc=1, 0 rows   "bufio.Scanner: token too long"
//
// The operator finds out at restore time. `verify` cannot see it because it
// rehashes the chunk BYTES and never parses a row — its evidence is the same
// artifact, so it confirms the bytes are intact while saying nothing about
// whether they can be read back. That is the shared-evidence class the
// project's own rule names: a verification whose "all clear" derives from the
// same artifact as the thing it verifies.
//
// Found by the v0.111.1 regression cycle while BOUNDING item 116's new chunk
// byte ceiling — the neighbouring fix is what led someone to push on the
// limit next door. Pre-existing on v0.111.0 and earlier.
//
// The pin that matters most is TestChunkLineLimit_WriterAndReaderShareOneLimit:
// the defect was two independent numbers where there should have been one, so
// the durable fix is that they cannot drift apart again.

package blobcodec

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func lineLimitColumns() []*ir.Column {
	return []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "body", Type: ir.Text{}},
	}
}

// TestChunkLineLimit_WriterRefusesAnUnreadableRow is the fix: a row the
// reader could never scan must not reach the artifact.
func TestChunkLineLimit_WriterRefusesAnUnreadableRow(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewChunkWriter(&buf, []string{"id", "body"}, nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	// Comfortably past the limit once JSON-encoded.
	row := ir.Row{"id": int64(1), "body": strings.Repeat("x", MaxChunkLineBytes+1024)}

	err = w.WriteRow(row, lineLimitColumns())
	if err == nil {
		t.Fatal("WriteRow ACCEPTED a row larger than the reader's line limit.\n\n" +
			"That produces a backup `backup verify` reports as OK and `restore` cannot read — the " +
			"operator discovers it at the moment they need the backup. Bug 226.")
	}
	for _, want := range []string{"exceeds", "restore", "verify"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — it must say what would have happened, not just "+
				"that a number was exceeded.\ngot: %v", want, err)
		}
	}
}

// The control, and the half that keeps the refusal from being a regression: a
// large-but-readable row must still round-trip. Wide rows are the workload
// this format is for.
func TestChunkLineLimit_LargeButReadableRowRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewChunkWriter(&buf, []string{"id", "body"}, nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	// 1 MiB — large, ordinary, and nowhere near the limit.
	body := strings.Repeat("y", 1<<20)
	if err := w.WriteRow(ir.Row{"id": int64(1), "body": body}, lineLimitColumns()); err != nil {
		t.Fatalf("WriteRow refused an ordinary wide row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewChunkReader(io.NopCloser(bytes.NewReader(buf.Bytes())), "", nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkReader: %v", err)
	}
	defer func() { _ = r.Close() }()
	rows := 0
	for {
		got, err := r.ReadRow()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadRow: %v", err)
		}
		if s, _ := got["body"].(string); len(s) != len(body) {
			t.Fatalf("body round-tripped at %d bytes; want %d", len(s), len(body))
		}
		rows++
	}
	if rows != 1 {
		t.Fatalf("read %d rows; want 1", rows)
	}
}

// TestChunkLineLimit_WriterAndReaderShareOneLimit is the structural pin, and
// the reason this bug was possible at all: the reader's scanner cap and the
// writer's refusal were two independent numbers, one of which did not exist.
//
// It asserts by SOURCE that the scanner is configured from the same exported
// constant the writer refuses on — a value-equality test would pass happily
// against two literals that agree today and drift tomorrow, which is exactly
// the failure being closed.
func TestChunkLineLimit_WriterAndReaderShareOneLimit(t *testing.T) {
	src := readOwnSource(t, "backup_chunk.go")

	if strings.Contains(src, "64*1024*1024") {
		t.Error("the chunk reader still configures its scanner from a LITERAL.\n\n" +
			"The writer's refusal and the reader's capacity must be the same named constant, or a " +
			"future edit to one silently re-opens Bug 226: rows the writer accepts that the reader " +
			"cannot scan.")
	}
	if !strings.Contains(src, "sc.Buffer(make([]byte, 0, 64*1024), MaxChunkLineBytes)") {
		t.Error("the scanner is no longer sized from MaxChunkLineBytes — writer and reader can drift")
	}
}

// Both write cores must refuse. The fast append-based encoder and the legacy
// reflection marshal are two separate paths to the same artifact, and a
// refusal on only one leaves the defect reachable through any row the fast
// path declines to encode.
func TestChunkLineLimit_BothWriteCoresRefuse(t *testing.T) {
	src := readOwnSource(t, "backup_chunk.go")
	if got := strings.Count(src, "checkChunkLineLength(len(b))"); got != 2 {
		t.Errorf("checkChunkLineLength appears on %d write path(s); want 2 (the fast encoder and "+
			"writeRowLegacy).\n\nA refusal on one core only is the sibling gap this project keeps "+
			"paying for.", got)
	}
}

// readOwnSource reads a source file from this package for the
// writer/reader-share-one-limit assertion above.
func readOwnSource(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}
