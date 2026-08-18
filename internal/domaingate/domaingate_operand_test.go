// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package domaingate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestClassifyOperand_FollowsSingleAssignmentLocals pins the T-5 capability:
// a column-type dispatch routed through a LOCAL variable is classified by the
// variable's source expression (so `t := col.Type; switch t.(type)` grades as
// raw, exactly as the direct form does), and an ambiguously-assigned local is
// never followed (so following can only ever REVEAL a reading the code has,
// never invent one). Before T-5 every local-routed dispatch classified as
// "not a dispatch at all" — invisible to both the finding set and the
// anti-vacuity floor, which is the gate-coverage hole this closes.
func TestClassifyOperand_FollowsSingleAssignmentLocals(t *testing.T) {
	const src = `package p
type C struct{ Type any; SourceColumnType any }
func direct(col C)         { switch col.Type.(type) { case int: } }
func localRaw(col C)       { t := col.Type; switch t.(type) { case int: } }
func localAssert(col C)    { t := col.Type; _ = t.(int) }
func localUnwrapped(col C) { t := ir.UnwrapDomain(col.Type); switch t.(type) { case int: } }
func chainedRaw(col C)     { a := col.Type; b := a; switch b.(type) { case int: } }
func twiceAssigned(col C, x any) { t := col.Type; t = x; switch t.(type) { case int: } }
func unrelatedLocal(x any) { t := x; switch t.(type) { case int: } }
`

	want := map[string]struct {
		kind string
		ok   bool
	}{
		"direct":         {operandRaw, true},
		"localRaw":       {operandRaw, true},
		"localAssert":    {operandRaw, true},
		"localUnwrapped": {operandUnwrapped, true},
		"chainedRaw":     {operandRaw, true},
		"twiceAssigned":  {"", false}, // reassigned → ambiguous → not followed
		"unrelatedLocal": {"", false}, // source is a param, not a .Type read
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	got := map[string]struct {
		kind string
		ok   bool
	}{}
	for _, scope := range dispatchScopes(f) {
		locals := localTypeSources(scope.node)
		ast.Inspect(scope.node, func(n ast.Node) bool {
			operand, ok := dispatchOperand(n)
			if !ok {
				return true
			}
			kind, classified := classifyOperand(operand, locals)
			// One dispatch per function; record the first.
			if _, seen := got[scope.name]; !seen {
				got[scope.name] = struct {
					kind string
					ok   bool
				}{kind, classified}
			}
			return true
		})
	}

	for name, exp := range want {
		g, ok := got[name]
		if !ok {
			t.Errorf("%s: no dispatch found (dispatchOperand missed it)", name)
			continue
		}
		if g.ok != exp.ok || g.kind != exp.kind {
			t.Errorf("%s: got (%q, %v), want (%q, %v)", name, g.kind, g.ok, exp.kind, exp.ok)
		}
	}
}
