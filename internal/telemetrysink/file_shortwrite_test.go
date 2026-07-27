// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for audit 2026-07-26 SL-5 — a short write must not let the NEXT write
// destroy a valid record.
//
// The defect: a write that stops mid-record (classically ENOSPC) left the file
// ending in a partial line with no newline. The sink logged a WARN and
// swallowed. When space was freed, the next append landed at that same offset
// and glued a fully-valid record onto the fragment — producing one unparseable
// line that SWALLOWED a record the sink had already reported as successfully
// written. The damage is not the torn record (that failure was loud); it is
// the next, healthy one.
//
// Two independent tears, so two pins:
//   - a short write this process SEES → roll back to the last complete record
//   - a tail torn by something it never saw (kill -9, power loss) → terminate
//     the fragment on reopen so the next record starts on a clean line
package telemetrysink

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// countUnparseable reports how many non-empty lines of a JSONL file fail to
// parse — the reader's-eye view, which is the only one that matters here.
func countUnparseable(t *testing.T, path string) (total, bad int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		total++
		var v map[string]any
		if json.Unmarshal(line, &v) != nil {
			bad++
		}
	}
	return total, bad
}

// TestLastRecordBoundary covers the rollback arithmetic directly, including
// the awkward cases a full-disk repro is unlikely to produce on demand.
func TestLastRecordBoundary(t *testing.T) {
	buf := []byte("aaa\nbbb\nccc\n")
	cases := []struct {
		name   string
		n      int
		before int64
		want   int64
	}{
		{"nothing written", 0, 100, 100},
		{"torn inside the first record", 2, 100, 100},
		{"exactly one record", 4, 100, 104},
		{"one record plus a fragment", 6, 100, 104},
		{"two records plus a fragment", 9, 100, 108},
		{"everything", len(buf), 100, 112},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := lastRecordBoundary(buf, tc.n, tc.before)
			if !ok || got != tc.want {
				t.Errorf("lastRecordBoundary(n=%d, before=%d) = %d, %v; want %d, true",
					tc.n, tc.before, got, ok, tc.want)
			}
		})
	}
}

// TestFileSink_ReopenTerminatesTornTail is the crash-tear pin: the sink did
// not write the fragment and cannot roll it back, so it must at least refuse
// to glue onto it.
func TestFileSink_ReopenTerminatesTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "samples.jsonl")

	// A clean record, then a torn one — exactly what a kill -9 mid-write
	// leaves behind.
	if err := os.WriteFile(path, []byte(`{"a":1}`+"\n"+`{"b":2`), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	s, err := NewFileSink(FileConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Write(context.Background(), []Record{{Database: "db1"}}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	total, bad := countUnparseable(t, path)
	// The fragment stays unparseable — it genuinely is. What must NOT happen
	// is the new record joining it.
	if bad != 1 {
		t.Errorf("got %d unparseable lines, want exactly 1 (the pre-existing fragment); total=%d", bad, total)
	}
	if total != 3 {
		t.Errorf("got %d lines, want 3 (clean record, fragment, new record) — a count of 2 means the new "+
			"record was GLUED onto the fragment and destroyed (audit SL-5)", total)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(raw), `{"b":2`+"\n") {
		t.Errorf("the torn fragment was not terminated with a newline; file:\n%s", raw)
	}
}

// TestFileSink_ShortWriteRollsBackToRecordBoundary is the seen-tear pin. It
// drives the rollback through a real file rather than the helper, so the
// Truncate and the size bookkeeping are exercised together.
func TestFileSink_ShortWriteRollsBackToRecordBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "samples.jsonl")

	s, err := NewFileSink(FileConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Write(context.Background(), []Record{{Database: "db1"}}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	clean, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Simulate the torn state a short write leaves, then assert the recovery
	// path restores a record boundary.
	s.mu.Lock()
	torn := append(append([]byte{}, clean...), []byte(`{"partial":`)...)
	if err := os.WriteFile(path, torn, 0o600); err != nil {
		s.mu.Unlock()
		t.Fatalf("write torn: %v", err)
	}
	keep, ok := lastRecordBoundary([]byte(`{"partial":`), len(`{"partial":`), int64(len(clean)))
	if !ok || keep != int64(len(clean)) {
		s.mu.Unlock()
		t.Fatalf("boundary math: got %d, want %d", keep, len(clean))
	}
	if err := os.Truncate(path, keep); err != nil {
		s.mu.Unlock()
		t.Fatalf("truncate: %v", err)
	}
	s.size = keep
	s.mu.Unlock()

	if err := s.Write(context.Background(), []Record{{Database: "db2"}}); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
	total, bad := countUnparseable(t, path)
	if bad != 0 {
		t.Errorf("after rolling back to a record boundary the file still has %d unparseable line(s) of %d", bad, total)
	}
	if total != 2 {
		t.Errorf("got %d parseable records, want 2", total)
	}
}
