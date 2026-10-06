// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestRegistryPositionOrder pins what `backup compact` hands smart compaction
// for shape (B): Postgres orders positions; a registered engine without an
// order is KNOWN with no order (smart compaction proceeds and says so); an
// engine this build does not register is UNKNOWN (smart compaction refuses).
func TestRegistryPositionOrder(t *testing.T) {
	for _, tc := range []struct {
		engine    string
		wantOrder bool
		wantKnown bool
	}{
		{"postgres", true, true},
		{"mysql", false, true},
		{"not-an-engine", false, false},
	} {
		t.Run(tc.engine, func(t *testing.T) {
			cmp, known := registryPositionOrder(tc.engine)
			if (cmp != nil) != tc.wantOrder || known != tc.wantKnown {
				t.Fatalf("registryPositionOrder(%q) = (order %v, known %v); want (order %v, known %v)", tc.engine, cmp != nil, known, tc.wantOrder, tc.wantKnown)
			}
		})
	}
}

// TestBackupCompactPassesThePositionOrder is the wiring half: every
// backup.CompactOpts literal in the CLI sets PositionOrder to
// registryPositionOrder. Without it the resolver above is dead code and
// smart compaction's (B) gates are off (said at INFO, but off). Floor: at
// least one CompactOpts literal must be found.
func TestBackupCompactPassesThePositionOrder(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			sel, ok := lit.Type.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "CompactOpts" {
				return true
			}
			found++
			wired := false
			for _, el := range lit.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				k, ok := kv.Key.(*ast.Ident)
				if !ok || k.Name != "PositionOrder" {
					continue
				}
				if v, ok := kv.Value.(*ast.Ident); ok && v.Name == "registryPositionOrder" {
					wired = true
				}
			}
			if !wired {
				t.Errorf("%s: a backup.CompactOpts literal does not set PositionOrder: registryPositionOrder", fset.Position(lit.Pos()))
			}
			return true
		})
	}
	if found == 0 {
		t.Fatal("anti-vacuity: no backup.CompactOpts literal found in cmd/sluice — re-anchor this gate")
	}
}
