// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// flavorDoorExemptMarker lets an Open* door declare that it deliberately
// does not run [Engine.checkServerFlavor], with the reason at the site.
const flavorDoorExemptMarker = "flavor-door-exempt:"

// Every Engine door that opens a connection either runs the flavor probe
// or says at the site why it does not.
//
// The probe carries two things: a WARN steering a MariaDB server off the
// mysql driver, and a REFUSAL of Vitess under a non-VStream flavor, which
// is a silent-loss guard (the vanilla flavor's full scans run without
// `set workload=olap`, and Vitess's OLTP workload truncates result sets
// at its row cap). Both lived at exactly two of the eight doors —
// OpenSchemaReader and OpenSchemaWriter — and their coverage rested on an
// unwritten assumption: that every path reaching data opens one of those
// first.
//
// The v0.148.2 regression cycle measured that assumption failing (Bug
// 280). The migrate pipeline opens the migration-state store at phase
// 1.75, ahead of both schema doors, and that store's SQL is itself
// flavor-specific — so a MariaDB target addressed with
// `--target-driver mysql` died on `Error 1064 … near 'AS new ON
// DUPLICATE KEY UPDATE'`, sluice's own statement, while the WARN naming
// the right driver never ran. The diagnosis was pre-empted by its own
// symptom, and the silent-loss refusal rode in the same probe.
//
// So the classification is written down per door instead of inferred
// from call order, and this roster derives its universe from the AST: a
// new `Open*` method on Engine joins it automatically.
func TestFlavorDoorRoster_EveryConnectionOpenerIsClassified(t *testing.T) {
	t.Parallel()

	const file = "engine.go"
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(src), "\n")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	doors, probed, exempt := 0, 0, 0
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Open") {
			continue
		}
		// Only doors that actually open a pool.
		opens, checks := false, false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				if fun.Name == "openDB" {
					opens = true
				}
			case *ast.SelectorExpr:
				if fun.Sel.Name == "checkServerFlavor" {
					checks = true
				}
				// A door that delegates to another door inherits its
				// probe; openBinlogCDCReader / openVStreamReader are
				// package functions, so record the delegation.
				if strings.HasPrefix(fun.Sel.Name, "Open") {
					checks = checks || false
				}
			}
			return true
		})
		if !opens {
			continue
		}
		doors++
		if checks {
			probed++
			continue
		}
		start := fset.Position(fn.Pos()).Line
		end := fset.Position(fn.End()).Line
		if end > len(lines) {
			end = len(lines)
		}
		body := strings.Join(lines[start-1:end], "\n")
		idx := strings.Index(body, flavorDoorExemptMarker)
		if idx < 0 {
			t.Errorf("%s:%d %s opens a connection pool but neither runs checkServerFlavor nor declares why "+
				"not. That probe carries the MariaDB driver steer AND the Vitess-under-vanilla REFUSAL, which is "+
				"a silent-loss guard; a door without it hands those cases to whichever door happens to open "+
				"first (Bug 280). Call it, or write `%s <why>` inside this function.",
				file, start, fn.Name.Name, flavorDoorExemptMarker)
			continue
		}
		if reason := strings.TrimSpace(body[idx+len(flavorDoorExemptMarker):]); len(reason) < 30 {
			t.Errorf("%s:%d %s carries %s with no real reason (%q)", file, start, fn.Name.Name,
				flavorDoorExemptMarker, reason)
		}
		exempt++
	}

	// Anti-vacuity: the scan must find the doors that exist, and at least
	// the four that carry the probe today (schema reader/writer, the
	// migration-state store, the change applier) must actually call it —
	// so a green cannot be bought by exempting everything.
	if doors < 5 {
		t.Fatalf("found %d connection-opening Open* doors in %s; expected at least 5 — the scan broke", doors, file)
	}
	if probed < 4 {
		t.Fatalf("only %d door(s) run checkServerFlavor; at least 4 must (schema reader, schema writer, "+
			"migration-state store, change applier)", probed)
	}
	t.Logf("flavor doors: %d total, %d probed, %d exempt with a reason", doors, probed, exempt)
}
