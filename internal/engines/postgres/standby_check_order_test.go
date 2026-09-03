// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// Bug 263 (v0.139.0 regression cycle, on a real streaming standby): the
// standby refusal reached a standby running `wal_level=logical` and NOT one
// running `wal_level=replica` — which is Postgres's default, and what a
// plain read replica actually runs. Both conditions hold on such a server,
// so whichever check runs first decides the error the caller sees, and
// wal_level ran first.
//
// The consequences were worse than a mislabelled error. On the CDC paths it
// told the operator to set `wal_level=logical` on a server that inherits
// the setting from its primary and cannot change it. On the backup path it
// also meant the CODED standby refusal never reached the fallback door that
// v0.139.0 added for Bug 260, so `backup full` copied the entire database
// and then died at the position capture — exactly the waste that fix
// existed to prevent, still reachable through the other setting.
//
// This gate derives its own universe: every function in the package that
// calls BOTH checks must call the standby one first. A new opener that
// gets the order wrong fails here, and so does a re-ordering of an existing
// one. It is deliberately a source-order gate rather than a behavioural
// one — reproducing it behaviourally needs a real `pg_basebackup` standby,
// which the sluice-testing regression cycle builds and which is where the
// live differential lives.
func TestStandbyCheckRunsBeforeWALLevel(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	type site struct{ file, fn string }
	var checked []site

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			standbyAt, walAt := token.NoPos, token.NoPos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch id.Name {
				case "checkNotStandby":
					if !standbyAt.IsValid() {
						standbyAt = call.Pos()
					}
				case "checkWALLevel":
					if !walAt.IsValid() {
						walAt = call.Pos()
					}
				}
				return true
			})
			if !standbyAt.IsValid() || !walAt.IsValid() {
				continue // only sites that run BOTH are in scope
			}
			checked = append(checked, site{name, fn.Name.Name})
			if standbyAt > walAt {
				t.Errorf("%s: %s calls checkWALLevel before checkNotStandby (%s vs %s) — on a standby at "+
					"wal_level=replica both hold, so the wal_level error wins and the operator is told to change a "+
					"setting the server inherits from its primary (Bug 263)",
					name, fn.Name.Name, fset.Position(walAt), fset.Position(standbyAt))
			}
		}
	}

	// Anti-vacuity: this package HAS sites that run both. If the roster
	// finds none, the matcher broke or the checks were renamed, and a
	// silent pass would be worse than a failure.
	if len(checked) < 3 {
		t.Fatalf("only %d site(s) run both checks (%v); expected at least 3 (the backup snapshot opener, the CDC "+
			"reader, and the snapshot-stream opener) — the roster is not seeing what it claims to", len(checked), checked)
	}
	t.Logf("ordering verified at %d call sites: %v", len(checked), checked)
}
