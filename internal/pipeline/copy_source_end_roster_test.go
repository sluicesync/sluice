// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// copyWriteMethods are the calls that move a source's rows to the target:
// the four bulk RowWriter surfaces, the raw byte-pipe's export side (the
// stage that reads its source), and the float repair's patch writer. A
// function calling one is a copy entry point.
var copyWriteMethods = map[string]bool{
	"WriteRows":                   true,
	"WriteRowsIdempotent":         true,
	"WriteRowsParallel":           true,
	"WriteRowsIdempotentParallel": true,
	"ExportRawCopy":               true,
	"UpdateFloatColumnsByPK":      true,
}

// knownCopyEntryPoints is the anti-vacuity floor: entry points the walker
// MUST find, so a walker that stopped seeing calls (a renamed method, a
// parse that skipped files) fails here instead of passing on an empty set.
// It is a floor, not the universe — a NEW entry point is found by the walk
// and held to the rule without being listed.
var knownCopyEntryPoints = []string{
	"copyChunk",
	"copyChunkFast",
	"copyTable",
	"copyTableColdStartIdempotent",
	"copyTableColdStartIdempotentParallel",
	"copyTableIdempotent",
	"copyTablePlainParallel",
	"copyTableWithCursor",
	"repairFloatTable",
	"runRawCopyChunk",
}

// TestCopyEntryPointRoster_EveryWriteConsultsTheSourceEnd is the GC-41 (i)
// gate: every function in package pipeline that moves rows with one of
// [copyWriteMethods] must ask a [migcore.SourceEnd] for its verdict AFTER the
// write — `.Confirm(` positioned after the first write call — and every tee
// that produces a SourceEnd must have it bound, not discarded to `_`.
// Without that, a writer's nil on a stopped copy reads as a finished table
// and its caller records it COMPLETE.
//
// Reach, stated so the name cannot be read as broader: non-test files of
// package internal/pipeline only. A function that IS one of those methods
// (a writer wrapper delegating to an inner writer) is not an entry point
// and is skipped. Outside the walk: internal/pipeline/backup (backupTable
// reads its stream directly with no writer; it carries its own end-of-
// stream ctx check, pinned by nothing here) and the engine packages (the
// writers themselves, whose select race this signal makes irrelevant).
// The gate checks presence and order, not that the Confirm's SourceEnd is
// the one the function's own source fed — the unit cells in
// copy_source_end_test.go are the behavioural half.
func TestCopyEntryPointRoster_EveryWriteConsultsTheSourceEnd(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || copyWriteMethods[fn.Name.Name] {
				continue
			}
			firstWrite, confirmAfter := token.NoPos, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					sel, ok := n.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if copyWriteMethods[sel.Sel.Name] && firstWrite == token.NoPos {
						firstWrite = n.Pos()
					}
					if sel.Sel.Name == "Confirm" && firstWrite != token.NoPos && n.Pos() > firstWrite {
						confirmAfter = true
					}
				case *ast.AssignStmt:
					checkSourceEndBound(t, fset, fn.Name.Name, n)
				}
				return true
			})
			if firstWrite == token.NoPos {
				continue
			}
			found[fn.Name.Name] = true
			if !confirmAfter {
				t.Errorf("%s (%s) moves rows with a bulk write but never asks a migcore.SourceEnd to Confirm "+
					"the source drained: a stop closes the stream exactly as end-of-table does, the writer "+
					"returns nil, and the caller records the table COMPLETE with its tail unread (GC-41 (i))",
					fn.Name.Name, fset.Position(firstWrite))
			}
		}
	}
	for _, name := range knownCopyEntryPoints {
		if !found[name] {
			t.Errorf("anti-vacuity: the walk did not find copy entry point %q — renamed, removed, or the "+
				"walker stopped seeing its write call; update knownCopyEntryPoints only if it truly moved", name)
		}
	}
	names := make([]string, 0, len(found))
	for n := range found {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("copy entry points held to the source-end rule: %v", names)
}

// checkSourceEndBound fails an assignment from teeRows / teePKAndCount
// that discards the SourceEnd: a tee whose end nobody reads is the
// pre-GC-41 (i) shape with extra steps.
func checkSourceEndBound(t *testing.T, fset *token.FileSet, fn string, as *ast.AssignStmt) {
	t.Helper()
	if len(as.Rhs) != 1 || len(as.Lhs) != 2 {
		return
	}
	call, ok := as.Rhs[0].(*ast.CallExpr)
	if !ok {
		return
	}
	id, ok := call.Fun.(*ast.Ident)
	if !ok || (id.Name != "teeRows" && id.Name != "teePKAndCount") {
		return
	}
	if end, ok := as.Lhs[1].(*ast.Ident); ok && end.Name == "_" {
		t.Errorf("%s (%s) discards the SourceEnd %s returns", fn, fset.Position(as.Pos()), id.Name)
	}
}
