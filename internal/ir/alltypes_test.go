// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

// TestAllTypes_CoversEveryIsTypeImplementor keeps [AllTypes] honest by
// deriving the ground truth from the package SOURCE: every type with an
// `isType()` method (the sealed-interface marker) must appear in
// AllTypes() exactly once, and AllTypes() must not list anything that
// is not an implementor. Without this, the registry would be one more
// hand-maintained mirror of the type universe — exactly the rot class
// it exists to gate (audit 2026-07-23 TEST-2 / G-12).
func TestAllTypes_CoversEveryIsTypeImplementor(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	implementors := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "isType" || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			switch r := fn.Recv.List[0].Type.(type) {
			case *ast.Ident:
				implementors[r.Name] = true
			case *ast.StarExpr:
				if id, ok := r.X.(*ast.Ident); ok {
					implementors[id.Name] = true
				}
			}
		}
	}
	// Vacuity guard: empty discovery means the parse broke, not that the
	// interface has no implementors.
	if len(implementors) < 10 {
		t.Fatalf("source scan found only %d isType() implementors — discovery is broken", len(implementors))
	}

	registered := map[string]bool{}
	for _, typ := range AllTypes() {
		name := reflect.TypeOf(typ).Name()
		if registered[name] {
			t.Errorf("AllTypes() lists %s twice", name)
		}
		registered[name] = true
	}

	for name := range implementors {
		if !registered[name] {
			t.Errorf("ir.%s implements isType() but is missing from AllTypes() — add it so every type-family exhaustiveness gate sees it", name)
		}
	}
	for name := range registered {
		if !implementors[name] {
			t.Errorf("AllTypes() lists %s, which does not implement isType() — remove the stale entry", name)
		}
	}
}
