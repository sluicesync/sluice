// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetrysink

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// sampleRec builds a fully-observed record for the given database.
func sampleRec(db string, cpu float64) Record {
	return Record{
		At:       time.Date(2026, 7, 24, 16, 0, 0, 0, time.UTC),
		Watch:    "metrics-watch:acme",
		Database: db,
		Branch:   "main",
		Fresh:    true,
		CPUUtil:  f64(cpu),
	}
}

// readLines splits a written file into non-empty lines.
func readLines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path) //nolint:gosec // test-controlled temp path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out []string
	for _, l := range strings.Split(string(raw), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// TestFileSink_AppendsJSONLReadableByAnIndependentReader pins the file
// format: one JSON object per line, each parseable on its own by a reader
// that is NOT the writer's Encoder (a fresh json.Unmarshal per line, the
// "independent reader" posture the new-surface checklist asks for).
func TestFileSink_AppendsJSONLReadableByAnIndependentReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "metrics.jsonl")
	s, err := NewFileSink(FileConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Write(t.Context(), []Record{sampleRec("alpha", 0.1), sampleRec("beta", 0.2)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Write(t.Context(), []Record{sampleRec("gamma", 0.3)}); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	lines := readLines(t, path)
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %d: %v", len(lines), lines)
	}
	wantDBs := []string{"alpha", "beta", "gamma"}
	for i, l := range lines {
		var got Record
		if err := json.Unmarshal([]byte(l), &got); err != nil {
			t.Fatalf("line %d is not standalone-parseable JSON (%v): %s", i, err, l)
		}
		if got.Database != wantDBs[i] {
			t.Errorf("line %d database: want %s, got %s", i, wantDBs[i], got.Database)
		}
	}
}

// TestFileSink_RotatesAndPrunes pins the rotation contract: the current file
// is always Path, the newest rotated generation is always Path.1, and no more
// than MaxFiles generations survive.
func TestFileSink_RotatesAndPrunes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.jsonl")
	// One record is ~180 bytes; a 1-byte cap forces a rotation per write
	// after the first.
	s, err := NewFileSink(FileConfig{Path: path, MaxBytes: 1, MaxFiles: 2})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = s.Close() }()

	for _, db := range []string{"a", "b", "c", "d"} {
		if err := s.Write(t.Context(), []Record{sampleRec(db, 0.5)}); err != nil {
			t.Fatalf("write %s: %v", db, err)
		}
	}

	// Current file holds the newest record; .1 the one before; .2 the one
	// before that; .3 must have been pruned.
	for path, wantDB := range map[string]string{
		path:        "d",
		path + ".1": "c",
		path + ".2": "b",
	} {
		lines := readLines(t, path)
		if len(lines) != 1 {
			t.Fatalf("%s: want 1 line, got %d", path, len(lines))
		}
		var got Record
		if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if got.Database != wantDB {
			t.Errorf("%s: want database %s, got %s", path, wantDB, got.Database)
		}
	}
	if _, err := os.Stat(path + ".3"); err == nil {
		t.Errorf("generation beyond MaxFiles=2 must be pruned, but %s.3 exists", path)
	}
}

// TestFileSink_BatchNeverSplitsAcrossRotation pins that a whole tick lands in
// ONE generation: a consumer must never find half a fleet's tick in each of
// two files.
func TestFileSink_BatchNeverSplitsAcrossRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	s, err := NewFileSink(FileConfig{Path: path, MaxBytes: 1, MaxFiles: 3})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Write(t.Context(), []Record{sampleRec("seed", 0.1)}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	batch := []Record{sampleRec("x1", 0.1), sampleRec("x2", 0.2), sampleRec("x3", 0.3)}
	if err := s.Write(t.Context(), batch); err != nil {
		t.Fatalf("batch write: %v", err)
	}
	if got := len(readLines(t, path)); got != len(batch) {
		t.Fatalf("the whole batch must land in one generation: want %d lines in the current file, got %d", len(batch), got)
	}
}

// TestFileSink_NegativeMaxBytesDisablesRotation pins the documented opt-out
// (an operator running an external logrotate owns the file's growth).
func TestFileSink_NegativeMaxBytesDisablesRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	s, err := NewFileSink(FileConfig{Path: path, MaxBytes: -1})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = s.Close() }()
	for _, db := range []string{"a", "b", "c"} {
		if err := s.Write(t.Context(), []Record{sampleRec(db, 0.5)}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if got := len(readLines(t, path)); got != 3 {
		t.Fatalf("rotation disabled: want all 3 lines in the current file, got %d", got)
	}
	if _, err := os.Stat(path + ".1"); err == nil {
		t.Error("rotation disabled but a generation file was created")
	}
}

// TestFileSink_ReopenSeedsSizeFromExistingFile pins that a restart resumes
// the current generation rather than treating it as empty (which would let
// the file grow past MaxBytes unbounded).
func TestFileSink_ReopenSeedsSizeFromExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	first, err := NewFileSink(FileConfig{Path: path, MaxBytes: 10_000})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	if err := first.Write(t.Context(), []Record{sampleRec("a", 0.1)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = first.Close()

	second, err := NewFileSink(FileConfig{Path: path, MaxBytes: 10_000})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = second.Close() }()
	if second.size == 0 {
		t.Fatal("reopened sink must seed its size from the existing file")
	}
	if err := second.Write(t.Context(), []Record{sampleRec("b", 0.2)}); err != nil {
		t.Fatalf("write after reopen: %v", err)
	}
	if got := len(readLines(t, path)); got != 2 {
		t.Fatalf("reopen must APPEND, not truncate: want 2 lines, got %d", got)
	}
}

// TestFileSink_RefusedRecordDoesNotPoisonTheBatch pins the codec refusal's
// blast radius: a row the encoder rejects is dropped with a named error while
// its siblings still land — never a partial line in the file.
func TestFileSink_RefusedRecordDoesNotPoisonTheBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	s, err := NewFileSink(FileConfig{Path: path})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	defer func() { _ = s.Close() }()

	bad := sampleRec(string([]byte{0xff}), 0.4)
	err = s.Write(t.Context(), []Record{sampleRec("ok1", 0.1), bad, sampleRec("ok2", 0.2)})
	if err == nil {
		t.Fatal("the refused row must surface an error for the caller to log+swallow")
	}
	lines := readLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("the two valid rows must still land: want 2 lines, got %d (%v)", len(lines), lines)
	}
	for _, l := range lines {
		if !json.Valid([]byte(l)) {
			t.Fatalf("a refusal must never leave a partial line: %q", l)
		}
	}
}

// TestNewFileSink_BadPathIsALoudRefusal pins that an unusable path fails at
// STARTUP rather than becoming a per-tick warning nobody reads — the opt-in
// must never silently record nothing.
func TestNewFileSink_BadPathIsALoudRefusal(t *testing.T) {
	if _, err := NewFileSink(FileConfig{Path: ""}); err == nil {
		t.Fatal("an empty path must be refused")
	}
	// A path whose parent is an existing FILE cannot be created as a dir.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := NewFileSink(FileConfig{Path: filepath.Join(blocker, "metrics.jsonl")}); err == nil {
		t.Fatal("a path under a regular file must be refused at construction")
	}
}
