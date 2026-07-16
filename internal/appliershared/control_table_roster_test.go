// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// controlTableConstPattern matches a table-name constant definition —
// an identifier ending in Table or TableName assigned a sluice_*
// string literal, e.g.
//
//	const ControlTableName = "sluice_cdc_state"
//	ChangeLogTable     = "sluice_change_log"
//
// Deliberately anchored on the Table/TableName suffix so
// non-table sluice_* constants (slot names, publication names,
// trigger-name prefixes, KMS context keys) don't match. New control
// tables MUST follow this naming convention — that's what lets this
// test catch them.
var controlTableConstPattern = regexp.MustCompile(`(?:Table|TableName)\s*=\s*"(sluice_[a-z0-9_]+)"`)

// TestControlTableRoster_SourceSync enforces ControlTableNames against
// the codebase IN BOTH DIRECTIONS (the error-code doc-sync pattern —
// see sluicecode.TestRegistryDocSync): every table-name constant
// matching the convention above, anywhere under internal/, must be in
// the roster; and every roster entry must be backed by at least one
// such constant. A future control table added without a roster entry
// fails here instead of silently re-appearing as a "user table" in
// the schema readers (roadmap item 65b).
func TestControlTableRoster_SourceSync(t *testing.T) {
	roster := map[string]bool{}
	for _, name := range ControlTableNames() {
		if roster[name] {
			t.Errorf("roster lists %q twice", name)
		}
		roster[name] = true
	}

	declared := map[string][]string{} // table name → declaring files
	root := filepath.Join("..", "..", "internal")
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// testdata holds fixtures, not sluice source.
			if info.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range controlTableConstPattern.FindAllStringSubmatch(string(raw), -1) {
			declared[m[1]] = append(declared[m[1]], path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(declared) == 0 {
		t.Fatal("no control-table constants found under internal/ — the scan pattern is broken")
	}

	names := make([]string, 0, len(declared))
	for name := range declared {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !roster[name] {
			t.Errorf("table-name constant %q (declared in %v) is missing from ControlTableNames — "+
				"add it so the schema readers exclude it from user-table enumeration", name, declared[name])
		}
	}
	for name := range roster {
		if _, ok := declared[name]; !ok {
			t.Errorf("roster entry %q has no backing table-name constant under internal/ — "+
				"remove it or fix the constant to follow the *Table/*TableName naming convention", name)
		}
	}
}

// TestIsControlTable pins the membership predicate every schema-reader
// door consults: true for every roster entry, false for near-misses.
func TestIsControlTable(t *testing.T) {
	for _, name := range ControlTableNames() {
		if !IsControlTable(name) {
			t.Errorf("IsControlTable(%q) = false; every roster entry must match", name)
		}
	}
	for _, name := range []string{"", "users", "sluice_", "sluice_cdc_state_backup", "SLUICE_CDC_STATE"} {
		if IsControlTable(name) {
			t.Errorf("IsControlTable(%q) = true; want false", name)
		}
	}
}

// TestControlTableRoster_AllSchemaReaderDoors enforces that EVERY
// schema-reader door applies the roster (audit-2026-07-15 MED-D0-6:
// item 65b reached the live mysql/postgres readers but not the
// mydumper/flatfile doors — the same-schema-different-door class).
// Doors are DISCOVERED, not hand-listed: every engine package under
// internal/engines with a non-test `ReadSchema(ctx context.Context)`
// implementation must reference IsControlTable somewhere in its
// non-test sources; the flatfile package (whose door is
// deriveTableName — it stages into sqlite under a filename-derived
// table name, so it has no ReadSchema of its own) is required
// explicitly. A future engine that adds a ReadSchema without
// consulting the roster fails here.
func TestControlTableRoster_AllSchemaReaderDoors(t *testing.T) {
	root := filepath.Join("..", "engines")
	readSchemaPattern := regexp.MustCompile(`\)\s+ReadSchema\(ctx context\.Context\)`)

	doorPkgs := map[string]bool{filepath.Join(root, "flatfile"): true}
	usesRoster := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		pkg := filepath.Dir(path)
		if readSchemaPattern.Match(raw) {
			doorPkgs[pkg] = true
		}
		if strings.Contains(string(raw), "IsControlTable") {
			usesRoster[pkg] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(doorPkgs) < 5 { // mysql, postgres, sqlite, mydumper, flatfile at minimum
		t.Fatalf("discovered only %d schema-reader door packages (%v) — the ReadSchema scan pattern is broken",
			len(doorPkgs), doorPkgs)
	}
	pkgs := make([]string, 0, len(doorPkgs))
	for pkg := range doorPkgs {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	for _, pkg := range pkgs {
		if !usesRoster[pkg] {
			t.Errorf("engine package %s enumerates tables (ReadSchema door) but never consults "+
				"appliershared.IsControlTable — apply the control-table roster at that door", pkg)
		}
	}
}
