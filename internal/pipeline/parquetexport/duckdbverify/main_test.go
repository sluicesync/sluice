// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Smoke tests for the duckdbverify generator/checker plumbing. The
// REAL gate — reading the files back with DuckDB — runs in
// .github/workflows/duckdb-verify.yml; these only keep `gen`/`check`
// from bitrotting in unit CI (files written, readable, checks/script
// consistent, and the checker loud on a mismatch).

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestGenerate_ProducesReadableMatrix(t *testing.T) {
	dir := t.TempDir()
	if err := generate(dir); err != nil {
		t.Fatalf("generate: %v", err)
	}

	// Every spec's parquet file opens (under parquet-go — a smoke, not
	// the external gate) with the declared row/row-group counts.
	for _, spec := range tableSpecs() {
		data, err := os.ReadFile(filepath.Join(dir, spec.name+".parquet"))
		if err != nil {
			t.Fatalf("%s: %v", spec.name, err)
		}
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("%s: OpenFile: %v", spec.name, err)
		}
		var rows int64
		for _, chunk := range spec.chunks {
			rows += int64(len(chunk))
		}
		if f.NumRows() != rows {
			t.Errorf("%s: NumRows = %d; want %d", spec.name, f.NumRows(), rows)
		}
		if got := len(f.RowGroups()); got != len(spec.chunks) {
			t.Errorf("%s: row groups = %d; want %d (one per chunk)", spec.name, got, len(spec.chunks))
		}
	}

	// checks.json parses, and every check's query names a generated
	// table file (script/checks drift guard).
	cb, err := os.ReadFile(filepath.Join(dir, checksFileName))
	if err != nil {
		t.Fatal(err)
	}
	var checks []check
	if err := json.Unmarshal(cb, &checks); err != nil {
		t.Fatalf("parse %s: %v", checksFileName, err)
	}
	if len(checks) == 0 {
		t.Fatal("no checks generated")
	}
	script, err := os.ReadFile(filepath.Join(dir, scriptFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(script), "SET TimeZone='UTC';") {
		t.Error("script.sql must SET TimeZone='UTC' first (the TIMESTAMPTZ renderings depend on it)")
	}
	for _, c := range checks {
		if c.Name == "" || c.Query == "" || len(c.Want) == 0 {
			t.Errorf("check %+v: missing name/query/want", c)
		}
		if !strings.Contains(string(script), c.Query) {
			t.Errorf("check %q: query missing from script.sql", c.Name)
		}
		table, _, ok := strings.Cut(c.Name, "/")
		if !ok {
			t.Errorf("check %q: name is not table/check-shaped", c.Name)
			continue
		}
		if !strings.Contains(c.Query, "'"+table+".parquet'") {
			t.Errorf("check %q: query does not read %s.parquet", c.Name, table)
		}
	}
}

func TestCompare_FailsLoudlyOnMismatch(t *testing.T) {
	dir := t.TempDir()
	checks := []check{{
		Name:  "t/one",
		Query: "SELECT 1 AS v FROM read_parquet('t.parquet');",
		Want:  []map[string]any{{"v": 1}},
	}}
	cb, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checksFileName), cb, 0o644); err != nil {
		t.Fatal(err)
	}
	actual := filepath.Join(dir, "actual.json")

	// Matching output passes.
	if err := os.WriteFile(actual, []byte(`[{"v":1}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := compare(dir, actual); err != nil {
		t.Fatalf("compare on matching output: %v", err)
	}

	// A value mismatch is a loud failure.
	if err := os.WriteFile(actual, []byte(`[{"v":2}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := compare(dir, actual); err == nil || !strings.Contains(err.Error(), "FAILED") {
		t.Fatalf("compare on mismatch = %v; want a FAILED error", err)
	}

	// Truncated output (fewer result sets than checks) is loud too.
	if err := os.WriteFile(actual, []byte(``), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := compare(dir, actual); err == nil || !strings.Contains(err.Error(), "ended early") {
		t.Fatalf("compare on truncated output = %v; want the ended-early error", err)
	}

	// Extra result sets mean script/checks drift: loud.
	if err := os.WriteFile(actual, []byte(`[{"v":1}] [{"v":9}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := compare(dir, actual); err == nil || !strings.Contains(err.Error(), "more result sets") {
		t.Fatalf("compare on extra output = %v; want the drift error", err)
	}
}

// TestWant_Uint64SurvivesTheJSONRoundTrip pins the checker's UseNumber
// discipline: uint64 max in a want must not be float64-mangled on
// either side of the comparison (the first cut decoded checks.json
// with plain Unmarshal and 18446744073709551615 became …552000).
func TestWant_Uint64SurvivesTheJSONRoundTrip(t *testing.T) {
	dir := t.TempDir()
	checks := []check{{
		Name:  "t/u64",
		Query: "SELECT u FROM read_parquet('t.parquet');",
		Want:  []map[string]any{{"u": uint64(18446744073709551615)}},
	}}
	cb, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checksFileName), cb, 0o644); err != nil {
		t.Fatal(err)
	}
	actual := filepath.Join(dir, "actual.json")
	if err := os.WriteFile(actual, []byte(`[{"u":18446744073709551615}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := compare(dir, actual); err != nil {
		t.Fatalf("uint64 max want mangled in comparison: %v", err)
	}
	// And the near-miss (float64(MaxUint64)'s rounding) must FAIL.
	if err := os.WriteFile(actual, []byte(`[{"u":18446744073709552000}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := compare(dir, actual); err == nil {
		t.Fatal("float64-rounded uint64 compared equal; the checker lost exactness")
	}
}
