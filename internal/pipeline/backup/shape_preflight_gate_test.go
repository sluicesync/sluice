// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestBackupRunReachesTheShapePreflights is the backup-package half of the
// audit 2026-09-06 W4 H3 gate.
//
// THE DEFECT. `backup full` is a row-emitting entry point and ran none of the
// three source-shape preflights. RLS is the one that bites: a table with RLS
// enabled, read by a role without BYPASSRLS, returns only the rows the policy
// admits — so the ARCHIVE silently holds a partial table, and every restore
// taken from it is short. Nothing in the chain records that it happened.
//
// WHY THIS GATE LIVES HERE rather than beside its add-table twin. `pipeline`
// imports `pipeline/backup`, so `backup` can never import `pipeline`, and the
// roster walker in that package only scans its own directory. The audit
// proposed "widen the consumer set to (*Backup).Run", which was not a wiring
// change at all: the three preflights had to MOVE into migcore — beside the
// eight preflights already there — before this call could exist. A gate that
// cannot see the consumer it names is worse than no gate, so this one sits
// where it can see it.
//
// WHAT IT REACHES: calls to the three exported migcore preflights from
// `(*Backup).Run`, by AST. It does NOT prove ordering, that they run
// post-filter, or that the reader is still open — those are properties of the
// call site, pinned by the preflights' own behaviour.
func TestBackupRunReachesTheShapePreflights(t *testing.T) {
	want := []string{
		"PreflightRLS",
		"PreflightPartitionedTables",
		"PreflightInheritanceTables",
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "backup.go", nil, 0)
	if err != nil {
		t.Fatalf("parse backup.go: %v", err)
	}

	// BOTH Run and the helper it delegates to. Run outgrew the funlen limit,
	// so the three calls were extracted — and a gate that greps only Run
	// would have gone green having lost sight of all three. The add-table
	// twin hit exactly this, which is why this one was written as a set from
	// the start.
	wantFuncs := map[string]bool{"Run": true, "preflightSourceShape": true}
	var targets []*ast.FuncDecl
	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || !wantFuncs[fn.Name.Name] {
			return true
		}
		targets = append(targets, fn)
		return false
	})
	if len(targets) != len(wantFuncs) {
		t.Fatalf("found %d of %d target functions %v in backup.go — one was renamed or moved, and this gate "+
			"is now grading less than it claims. Re-anchor it rather than deleting it.", len(targets), len(wantFuncs), wantFuncs)
	}

	seen := map[string]bool{}
	total := 0
	for _, target := range targets {
		ast.Inspect(target.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if strings.HasPrefix(strings.ToLower(sel.Sel.Name), "preflight") {
				seen[sel.Sel.Name] = true
				total++
			}
			return true
		})
	}

	// Anti-vacuity: Run calls more preflights than these three (redaction,
	// and the chain-posture doors). A walk finding almost nothing has drifted.
	if total < 4 {
		t.Fatalf("anti-vacuity: (*Backup).Run reaches only %d preflight call(s) %v; it runs more than that — "+
			"the AST walk has drifted, fix the walk rather than the floor", total, seen)
	}

	for _, w := range want {
		if !seen[w] {
			t.Errorf("(*Backup).Run does not call migcore.%s.\n\n"+
				"backup full copies rows into an archive, so it owes the same source-shape checks migrate and "+
				"sync cold start run. Without the RLS one, a policy-filtered read produces an archive that is "+
				"silently short — and every restore from it inherits the loss with nothing recording why.", w)
		}
	}
}
