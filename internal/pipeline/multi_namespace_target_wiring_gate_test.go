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

// EVERY FAN-OUT THAT BRANCHES ON THE TARGET'S NAMESPACING MUST FIRST REFUSE A
// TARGET THAT HAS NONE (roadmap item 148, route 2).
//
// # Why a DERIVED roster rather than a hand-maintained one
//
// The sibling gate next door (TestIndexEmitPreflightReachesEveryCopyEntryPoint)
// says in its own scope note that its roster is hand-maintained, and that a new
// entry point nothing adds to it is invisible. That limit is exactly how this
// defect survived: `migrate`'s fan-out and `sync`'s fan-out are two functions
// in two files that make the SAME type assertion and took the SAME false
// premise, and the second was written by copying the first. So this gate
// derives its subject set from the code — every function that type-asserts a
// `.Target` to [ir.DatabaseDSNDeriver] — and requires each to call
// [migcore.ValidateMultiNamespaceTarget]. A third fan-out written the same way
// is caught the day it is written, with no roster to remember to update.
//
// # What it reaches, stated so the name cannot be read as broader
//
// It finds the assertion by SHAPE: `<expr>.Target.(ir.DatabaseDSNDeriver)`. A
// future fan-out that reached the same decision by another route — a helper
// that takes the deriver as a parameter, a capability lookup, an interface
// stored in a struct field — is outside it. The anti-vacuity floor below is
// what keeps that limitation visible: if the assertion shape changes and the
// walk starts matching nothing, this fails rather than passing on an empty set.
func TestMultiNamespaceFanOutRefusesAFlatTarget(t *testing.T) {
	const dir = "."

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	// asserters: declaration identity -> file, for every func that asserts a
	// .Target to ir.DatabaseDSNDeriver. validators: the same identities that
	// also call migcore.ValidateMultiNamespaceTarget.
	asserters := map[string]string{}
	validators := map[string]bool{}
	parsed := 0
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		parsed++
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.TypeAssertExpr:
					if !isTargetDeriverAssertion(node) {
						return true
					}
					asserters[declIdentity(fn)] = name
				case *ast.CallExpr:
					sel, ok := node.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "ValidateMultiNamespaceTarget" {
						return true
					}
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "migcore" {
						validators[declIdentity(fn)] = true
					}
				}
				return true
			})
		}
	}

	if parsed < 5 {
		t.Fatalf("parsed only %d non-test files in %q — the walk is not seeing the package", parsed, dir)
	}
	// Anti-vacuity floor. Two fan-outs exist today (migrate + sync cold start);
	// a walk that found fewer has stopped matching the assertion shape, and an
	// empty set would otherwise satisfy every assertion below forever.
	if len(asserters) < 2 {
		got := make([]string, 0, len(asserters))
		for d := range asserters {
			got = append(got, d)
		}
		sort.Strings(got)
		t.Fatalf("found %d function(s) asserting a .Target to ir.DatabaseDSNDeriver (%v); at least the "+
			"migrate and sync multi-namespace fan-outs do. The assertion shape this gate matches has "+
			"changed — re-point it, do not lower the floor.", len(asserters), got)
	}

	for decl, file := range asserters {
		if validators[decl] {
			continue
		}
		t.Errorf("%s (%s) branches on whether the TARGET can derive a per-namespace DSN, but never calls "+
			"migcore.ValidateMultiNamespaceTarget.\n\n"+
			"Its non-deriver arm then runs against a target that may have NO namespacing at all — the arm "+
			"whose comment used to claim it was \"unreachable with today's engines\". On a flat target "+
			"(sqlite/d1) it sets a TargetSchema the engine discards, and every source namespace writes "+
			"bare, unqualified table names into ONE target namespace: two namespaces carrying a same-named "+
			"table merge, the second's rows landing in the first, at exit 0. That is roadmap item 148 "+
			"route 2, re-opened for this path.", decl, file)
	}
}

// isTargetDeriverAssertion reports whether the expression is
// `<expr>.Target.(ir.DatabaseDSNDeriver)` — the shape both fan-outs use to
// decide how to route each source namespace.
func isTargetDeriverAssertion(node *ast.TypeAssertExpr) bool {
	sel, ok := node.X.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Target" {
		return false
	}
	typ, ok := node.Type.(*ast.SelectorExpr)
	if !ok || typ.Sel.Name != "DatabaseDSNDeriver" {
		return false
	}
	pkg, ok := typ.X.(*ast.Ident)
	return ok && pkg.Name == "ir"
}
