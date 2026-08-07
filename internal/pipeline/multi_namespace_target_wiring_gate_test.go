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
// TARGET THAT HAS NONE (roadmap item 148, route 2) — AND MUST ASK WHETHER THE
// TARGET FOLDS THE NAMESPACE NAMES IT IS ABOUT TO ROUTE TO (item 149).
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
	// .Target to ir.DatabaseDSNDeriver. calls: declaration identity -> the
	// package-local declarations it calls, so "does this fan-out reach the
	// check?" is answered over the call graph rather than by direct call only.
	// checkers: for each required check, the declarations that call it directly.
	asserters := map[string]string{}
	calls := map[string]map[string]bool{}
	checkers := map[string]map[string]bool{
		"preflightNamespaceFoldCollisions":     {},
		"migcore.ValidateMultiNamespaceTarget": {},
	}
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
			self := declIdentity(fn)
			ast.Inspect(fn, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.TypeAssertExpr:
					if !isTargetDeriverAssertion(node) {
						return true
					}
					asserters[self] = name
				case *ast.CallExpr:
					switch callee := node.Fun.(type) {
					case *ast.Ident:
						// A package-level function: the fold preflight is one.
						if _, required := checkers[callee.Name]; required {
							checkers[callee.Name][self] = true
						}
						if calls[self] == nil {
							calls[self] = map[string]bool{}
						}
						calls[self][callee.Name] = true
					case *ast.SelectorExpr:
						x, ok := callee.X.(*ast.Ident)
						if !ok {
							return true
						}
						if qualified := x.Name + "." + callee.Sel.Name; checkers[qualified] != nil {
							checkers[qualified][self] = true
						}
						// A call on this function's OWN receiver is a call to a
						// method of the same type, which is how both fan-outs
						// reach their shared helpers.
						if x.Name == receiverName(fn) {
							if calls[self] == nil {
								calls[self] = map[string]bool{}
							}
							calls[self][receiverType(fn)+"."+callee.Sel.Name] = true
						}
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

	// Each required check must be REACHED, not necessarily called in the
	// asserting function itself. The sync fan-out reaches the fold preflight
	// through the resolver it shares with the warm-resume path, and a gate that
	// demanded a direct call would have reported that correct code as a defect
	// — the false-positive direction of the same narrowness this file's header
	// warns about in the false-negative one.
	for _, check := range []struct{ name, why string }{
		{
			// The DATABASE-name axis of the same fan-out (ADR-0075 decision #1),
			// added with roadmap item 149. Item 149 closed the TABLE-name axis of
			// the identical server setting and found this one held by nothing but
			// two hand-written call sites. Same defect one level up: two source
			// namespaces that fold to one MySQL database merge silently, and the
			// fan-out is the only place that can see it.
			name: "preflightNamespaceFoldCollisions",
			why: "So it routes each source namespace to a same-named target database without ever asking " +
				"the server whether it FOLDS those names. On MySQL under lower_case_table_names != 0 it " +
				"does: `Sales` and `sales` become ONE database, and two namespaces' rows merge into it at " +
				"exit 0 (ADR-0075 resolved decision #1 — the DATABASE-name axis of roadmap item 149).",
		},
		{
			name: "migcore.ValidateMultiNamespaceTarget",
			why: "Its non-deriver arm then runs against a target that may have NO namespacing at all — " +
				"the arm whose comment used to claim it was \"unreachable with today's engines\". On a " +
				"flat target (sqlite — the only one, since `d1` is a migrate source only) it sets a " +
				"TargetSchema the engine discards, and every source " +
				"namespace writes bare, unqualified table names into ONE target namespace: two namespaces " +
				"carrying a same-named table merge, the second's rows landing in the first, at exit 0. " +
				"That is roadmap item 148 route 2, re-opened for this path.",
		},
	} {
		callers := checkers[check.name]
		if len(callers) == 0 {
			t.Fatalf("nothing in %q calls %s. Either it was renamed (re-point this gate) or the check is "+
				"gone entirely, in which case every assertion below is satisfied by an empty set.",
				dir, check.name)
		}
		for decl, file := range asserters {
			if reachesCall(decl, callers, calls) {
				continue
			}
			t.Errorf("%s (%s) branches on whether the TARGET can derive a per-namespace DSN, but neither it "+
				"nor anything it calls reaches %s.\n\n%s", decl, file, check.name, check.why)
		}
	}
}

// receiverName returns the NAME the function gives its receiver ("m", "s"), or
// "" for a plain function or an unnamed receiver.
func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 || len(fn.Recv.List[0].Names) == 0 {
		return ""
	}
	return fn.Recv.List[0].Names[0].Name
}

// receiverType renders the receiver the way [declIdentity] does ("(*Streamer)"),
// so a call on the receiver keys the same string the callee declares itself as.
func receiverType(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	star := ""
	if s, ok := typ.(*ast.StarExpr); ok {
		typ, star = s.X, "*"
	}
	id, ok := typ.(*ast.Ident)
	if !ok {
		return ""
	}
	return "(" + star + id.Name + ")"
}

// reachesCall reports whether decl is one of callers, or transitively calls one
// through the package-local call graph in calls.
//
// # Scope, stated so the name cannot be read as broader
//
// The graph holds two edge kinds: a call to a package-level function, and a
// call on the function's OWN receiver. Those are the two shapes both fan-outs
// use. A check reached through a function VALUE, a call on another type's
// value, or an interface method is INVISIBLE here and would be reported as
// missing — a false positive that a reviewer resolves by reading the code, not
// a silent pass. The floor on len(callers) above is what keeps a renamed check
// from turning every one of these questions vacuous.
func reachesCall(decl string, callers map[string]bool, calls map[string]map[string]bool) bool {
	seen := map[string]bool{}
	var walk func(string) bool
	walk = func(at string) bool {
		if callers[at] {
			return true
		}
		if seen[at] {
			return false
		}
		seen[at] = true
		for callee := range calls[at] {
			if walk(callee) {
				return true
			}
		}
		return false
	}
	return walk(decl)
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
