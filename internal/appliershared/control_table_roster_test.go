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

// TestControlTableSQLList pins the NOT IN body shape both engine
// readers embed: single-quoted names, comma-separated, one per roster
// entry, no operator-controllable input.
func TestControlTableSQLList(t *testing.T) {
	list := ControlTableSQLList()
	names := ControlTableNames()
	parts := strings.Split(list, ", ")
	if len(parts) != len(names) {
		t.Fatalf("SQL list has %d entries; roster has %d:\n%s", len(parts), len(names), list)
	}
	for i, name := range names {
		if want := "'" + name + "'"; parts[i] != want {
			t.Errorf("entry %d = %s; want %s", i, parts[i], want)
		}
	}
}
