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

// closeExemptRowWriterWrappers names the test-local [ir.RowWriter] wrappers
// that are deliberately NOT [io.Closer], each with the reason. The map is
// fail-by-default: a wrapper that is neither closable nor listed here fails
// the roster below, so adding one is a decision somebody writes down rather
// than an omission nobody notices.
//
// The distinction that matters is whether the wrapper ever holds a writer
// the pipeline OPENED. A stub that embeds a nil ir.RowWriter to satisfy the
// interface owns no pool and has nothing to release.
var closeExemptRowWriterWrappers = map[string]string{
	"emptinessWriter": "embeds a nil ir.RowWriter to answer one probe; never holds an opened writer, so there is no pool to release",
	"bareRowWriter":   "embeds a nil ir.RowWriter as a do-nothing stand-in; never holds an opened writer",
}

// TestRowWriterTestWrapperRoster_EveryWrapperClosesItsInner requires every
// test-local [ir.RowWriter] wrapper in THIS package to implement Close.
//
// # What it is for (TESTFRAGILE-3)
//
// The pipeline releases the writers it opens through [migcore.CloseIf],
// which is an [io.Closer] type assertion and nothing more. A wrapper that
// forwards WriteRows / WriteRowsIdempotent / IsTableEmpty / TruncateTable
// but not Close therefore satisfies every surface the copy path needs while
// making every CloseIf against it a silent no-op — the real engine writer
// underneath is never closed and its pool's backends survive the test.
// Measured at 1-8 leaked Postgres backends per run of
// migrate_fastloader_integration_test.go; nine iterations against one
// container reach `FATAL: sorry, too many clients already`, which then
// surfaces as an unrelated-looking mid-copy failure in whatever test is
// running at the time.
//
// # Exactly what this gate reaches, stated rather than implied
//
// It walks the *_test.go files of internal/pipeline only — tagged files
// included, since go/parser ignores build constraints — and its universe is
// struct types declared there with a field (named OR embedded) of type
// ir.RowWriter. It does NOT reach: wrappers in other packages' tests,
// wrappers that hold an engine writer behind a concrete engine type (the
// pipeline package cannot import one, so none exist here), production code,
// or a wrapper that stores its inner writer as `any`. If one of those
// shapes ever appears, this gate is silent about it and the name above is
// wider than the truth; extend the walker rather than assuming it covered
// you.
//
// Nor does it check that Close forwards to the inner writer — only that the
// type is a Closer. A Close that returns nil without forwarding passes here
// and leaks exactly as before.
//
// Mutation run 2026-09-14, both directions: deleting trackingWriter.Close
// fails with that type named; adding a struct with an ir.RowWriter field
// and no Close fails with the new type named.
func TestRowWriterTestWrapperRoster_EveryWrapperClosesItsInner(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fset := token.NewFileSet()
	wrappers := map[string]string{}  // type name -> position
	closers := map[string]struct{}{} // type name with a Close method
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.TypeSpec:
				st, ok := node.Type.(*ast.StructType)
				if !ok || st.Fields == nil {
					return true
				}
				for _, field := range st.Fields.List {
					if isRowWriterType(field.Type) {
						wrappers[node.Name.Name] = fset.Position(node.Pos()).String()
						break
					}
				}
			case *ast.FuncDecl:
				if node.Name.Name != "Close" || node.Recv == nil || len(node.Recv.List) != 1 {
					return true
				}
				if recv := receiverTypeName(node.Recv.List[0].Type); recv != "" {
					closers[recv] = struct{}{}
				}
			}
			return true
		})
	}

	var graded int
	names := make([]string, 0, len(wrappers))
	for name := range wrappers {
		names = append(names, name)
	}
	sort.Strings(names)

	// Violations first, floor second — a real miss must be the thing the
	// output leads with, not a vacuity complaint that reads as "the walker
	// is broken".
	for _, name := range names {
		if reason, exempt := closeExemptRowWriterWrappers[name]; exempt {
			if _, ok := closers[name]; ok {
				t.Errorf("%s is listed in closeExemptRowWriterWrappers (%q) but DOES implement Close; "+
					"drop the exemption so the list keeps meaning what it says", name, reason)
			}
			continue
		}
		graded++
		if _, ok := closers[name]; !ok {
			t.Errorf("%s (%s) wraps an ir.RowWriter but implements no Close, so migcore.CloseIf against it "+
				"is a silent no-op and the inner engine writer's pool leaks for the life of the test binary. "+
				"Add `func (w *%s) Close() error { migcore.CloseIf(w.<inner>); return nil }`, or — if it never "+
				"holds an opened writer — add it to closeExemptRowWriterWrappers with the reason",
				name, wrappers[name], name)
		}
	}

	// Anti-vacuity floor. Four wrappers hold opened writers today
	// (trackingWriter, failingRowWriter, failingIdempotentRowWriter,
	// writeConcurrencyProbe); if the walker sees fewer it has stopped
	// reaching the code it grades — a renamed field type, a moved file, a
	// parser change — and its green means nothing.
	if graded < 4 {
		t.Errorf("the walker graded only %d ir.RowWriter wrappers; it is not reaching the four this gate "+
			"was built for, so a passing run proves nothing", graded)
	}
}

// isRowWriterType reports whether a struct field's declared type is
// ir.RowWriter, embedded (`ir.RowWriter`) or named (`inner ir.RowWriter`).
// A pointer to it counts too; nothing declares one today.
func isRowWriterType(expr ast.Expr) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "ir" && sel.Sel.Name == "RowWriter"
}
