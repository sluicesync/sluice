// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every engine package that writes a stream's persisted position must stamp
// its liveness in the same statement — and the roster of such packages is
// derived, not remembered.
//
// # Why this gate exists (DRAINED-ADD-TABLE-STREAM-NOT-STOPPED)
//
// A drained `schema add-table` passes only on a stream that has recorded its
// stop, and the record it reads (`sluice_cdc_state.stream_alive_ms`) is kept
// honest by one rule: every write of `source_position` stamps the stream
// running in the same statement, so a stream that advances its position can
// never read as stopped. Inside the Postgres and MySQL packages that rule is
// held by their own AST rosters (TestPositionWritesStampLiveness and
// TestSourcePositionWrittenOnlyByTheBuilder in each package's
// position_write_liveness_test.go). Those rosters reach only their own
// package. A new engine with its own change applier would write positions
// under neither of them, its streams would keep a stopped marker while
// applying, and a drained add against them would pass at exit 0 while the
// stream dropped the new table's changes.
//
// So this gate walks every package under internal/engines, finds each
// function or package-level const/var block whose string literals write
// `source_position` (a literal naming it, and a literal that — whitespace
// collapsed, case folded — opens INSERT [INTO|IGNORE] / REPLACE / UPDATE /
// MERGE / UPSERT, or assigns `source_position =`), and requires each such
// package to carry both per-package rosters.
//
// WHAT IT REACHES, and what it does not: SQL a package spells in its own
// string literals, inside one declaration. It does NOT see a statement whose
// verb and column name are assembled from different declarations (a verb in
// one package-level const, the column in another), SQL built in another
// package (seen where it lives — every source_position mention outside
// internal/engines is a comment or a reader today), or a write through an
// ORM/driver API with no SQL text. The floor below pins the two packages
// known to write today, so a walk gone blind fails rather than passes.
package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestPositionWriterEngines_EveryOneCarriesTheLivenessRosters(t *testing.T) {
	root := filepath.Join("..", "engines")
	writers := map[string][]string{} // package dir -> writing functions
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		pkg := filepath.Dir(path)
		for _, decl := range f.Decls {
			// A function, or a package-level const/var block (SQL hoisted
			// into a package-level string is still a write).
			name := "a package-level declaration"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				name = fn.Name.Name
			}
			if writesSourcePosition(decl) {
				writers[pkg] = append(writers[pkg], name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	pkgs := make([]string, 0, len(writers))
	for pkg := range writers {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	for _, pkg := range pkgs {
		roster, rerr := os.ReadFile(filepath.Join(pkg, "position_write_liveness_test.go"))
		for _, gate := range []string{"func TestPositionWritesStampLiveness(", "func TestSourcePositionWrittenOnlyByTheBuilder("} {
			if rerr != nil || !strings.Contains(string(roster), gate) {
				t.Errorf("%s writes sluice_cdc_state.source_position (in %s) but has no %s roster in "+
					"position_write_liveness_test.go: its position writes are not held to stamping stream_alive_ms, "+
					"and a drained add-table could read its running streams as stopped",
					pkg, strings.Join(writers[pkg], ", "), strings.TrimSuffix(strings.TrimPrefix(gate, "func "), "("))
			}
		}
	}
	// Anti-vacuity: the Postgres and MySQL appliers write positions today
	// (postgres-trigger delegates to the Postgres applier). Fewer means the
	// walk or its write test has gone blind.
	for _, want := range []string{"mysql", "postgres"} {
		if _, ok := writers[filepath.Join(root, want)]; !ok {
			t.Errorf("the walk found no position write in internal/engines/%s (found: %v); it has gone blind", want, pkgs)
		}
	}
}

// writesSourcePosition is the per-package rosters' test, at function scope:
// a literal names source_position, and a literal writes.
func writesSourcePosition(decl ast.Node) bool {
	names, writes := false, false
	ast.Inspect(decl, func(n ast.Node) bool {
		bl, ok := n.(*ast.BasicLit)
		if !ok || bl.Kind != token.STRING {
			return true
		}
		v, err := strconv.Unquote(bl.Value)
		if err != nil {
			return true
		}
		names = names || strings.Contains(v, "source_position")
		writes = writes || sqlWrites(v)
		return true
	})
	return names && writes
}

// sqlWriteVerbs are the statement openings that write a row, after
// whitespace is collapsed and case folded.
var sqlWriteVerbs = []string{"INSERT INTO ", "INSERT IGNORE ", "INSERT ", "REPLACE INTO ", "REPLACE ", "UPDATE ", "MERGE INTO ", "MERGE ", "UPSERT "}

// sqlWrites reports whether a literal opens a write statement or assigns
// source_position, with runs of whitespace (spaces, tabs, newlines) collapsed
// so a multi-line or oddly spaced statement is seen.
func sqlWrites(v string) bool {
	norm := strings.ToUpper(strings.Join(strings.Fields(v), " "))
	for _, verb := range sqlWriteVerbs {
		if strings.HasPrefix(norm+" ", verb) {
			return true
		}
	}
	return strings.Contains(norm, "SOURCE_POSITION =") || strings.Contains(norm, "SOURCE_POSITION=")
}
