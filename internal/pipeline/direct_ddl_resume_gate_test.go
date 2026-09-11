// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"strings"
	"testing"
)

// The direct-DDL preflight must not run on `--resume`, and this gate holds
// the guard because the failure it prevents is a PERMANENT dead end rather
// than a wasted run.
//
// What happened (sluice-testing Bug 284, found by the v0.151.0 regression
// cycle on a vttestserver booted with ENABLE_DIRECT_DDL=false):
//
//   - the probe is placed after the ADR-0166 pre-create gate so a run that
//     needs no DDL is not refused for being unable to do DDL;
//   - but that gate is skipped on --resume, so `createSchema` is the WHOLE
//     schema regardless of what the target already holds;
//   - so the probe fired on every resumed run into a DDL-refusing target,
//     however completely the schema had been pre-created;
//   - and the partial-migration refusal names `--resume` as its remedy, so
//     the operator was routed into a loop whose only exits were deleting
//     the migration-state row or inventing a new --migration-id.
//
// The feature exists to spare an operator exactly one wasted `--resume`
// cycle. It was manufacturing a permanent one.
//
// A source-level gate rather than a behavioural one because the condition
// is a property of the call site: the probe needs a live DDL-refusing
// target to exercise, which no unit test has. This asserts the guard is
// present and syntactically binding; the regression cycle asserts the
// behaviour against a real server.
func TestDirectDDLPreflightIsNotRunOnResume(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "migrate.go", nil, 0)
	if err != nil {
		t.Fatalf("parse migrate.go: %v", err)
	}

	var (
		found       bool
		guardedByNo bool
	)
	ast.Inspect(f, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Cond == nil {
			return true
		}
		// Does this if-statement's body call PreflightDirectDDL?
		var calls bool
		ast.Inspect(ifs.Body, func(m ast.Node) bool {
			call, ok := m.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "PreflightDirectDDL" {
				calls = true
			}
			return true
		})
		if !calls {
			return true
		}
		found = true
		// The condition must contain a `!resuming` term. Rendering the
		// expression and looking for the token is deliberate: a structural
		// match would accept `resuming` (without the negation), which is the
		// exact inversion that would reintroduce the dead end.
		if strings.Contains(exprText(fset, ifs.Cond), "!resuming") {
			guardedByNo = true
		}
		return true
	})

	if !found {
		t.Fatal("no `if` statement in migrate.go guards a call to PreflightDirectDDL — either the probe moved, " +
			"or it is now called unconditionally. If it moved, move this gate with it; if it is unconditional, " +
			"that is Bug 284 returning: a resumed migrate into a DDL-refusing target is refused forever, and the " +
			"partial-migration refusal names --resume as the remedy, so there is no way out but deleting state.")
	}
	if !guardedByNo {
		t.Error("the guard around PreflightDirectDDL does not test `!resuming`. On --resume the ADR-0166 " +
			"pre-create gate does not run, so createSchema is the whole schema and the probe fires however " +
			"completely the target was pre-created — a permanent dead end (sluice-testing Bug 284).")
	}
}

// exprText renders an expression back to source text.
func exprText(fset *token.FileSet, e ast.Expr) string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, e); err != nil {
		return ""
	}
	return buf.String()
}
