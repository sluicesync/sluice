// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestBoundaryRefusals_ClaimNothingWritten fails when any string literal in
// this package's non-test sources says "nothing has been written". Every
// boundary refusal said it, and it was false wherever rows reached the
// target before the boundary was judged — measured under
// binlog_row_metadata=FULL, where a row typed by a later change landed
// rounded and the refusal then claimed nothing had been written
// (BUG301-DEFERRED-RETYPE). Those refusals now share
// refusedBeforeBoundary. Comments are not scanned, only literals an
// operator could read; the floor keeps a broken walk from passing
// vacuously.
//
// SCOPE, stated so the name is not read as broader: this directory only
// (not engines/, not the CLI), and this one phrase. It pins the WORDING —
// that the old false claim does not come back here — not the truth of the
// sentence that replaced it, which rests on the reader-side deferred-shape
// refusal and its own pins (TestHoldDecodedOrder_DeferredShapeMatrix,
// rsfRunDeferredRetypeCell).
func TestBoundaryRefusals_ClaimNothingWritten(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	literals := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			literals++
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				s = lit.Value
			}
			if strings.Contains(strings.ToLower(s), "nothing has been written") {
				t.Errorf("%s: a literal claims %q; say what was written instead (refusedBeforeBoundary)", fset.Position(lit.Pos()), "nothing has been written")
			}
			return true
		})
	}
	if literals < 1000 {
		t.Fatalf("walked only %d string literals in this package; the scan is not reaching the sources", literals)
	}
}
