// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every return site in this engine whose diagnosis is "different lineage"
// must carry ir.ErrPositionForeignLineage — or be listed here with the
// reason it cannot (audit 2026-09-09 A0909-MYSQL-HIGH-1: the GTID arm
// wrapped ErrPositionInvalid and the automatic re-copy dropped the
// target). The roster derives its universe from the source text rather
// than a hand list, and a floor keeps it from going green on an empty
// scan.
//
// Scope, stated: non-test .go files in this package; a site is any
// string literal containing "different lineage" (the phrase every such
// verdict uses), graded by whether the enclosing return statement also
// names ErrPositionForeignLineage.
func TestForeignLineageVerdictsCarryTheSentinel(t *testing.T) {
	t.Parallel()

	// Sites that say "different lineage" and deliberately keep
	// ir.ErrPositionInvalid, each with the reason.
	exempt := map[string]string{
		"reader_errors.go": "the MariaDB REACTIVE 1236 classification cannot tell a reset from a replacement from " +
			"the server's error text; the pipeline's reactive door asks the reader before it drops anything",
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	graded, carrying := 0, 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(src), "different lineage") && !strings.Contains(string(src), "different server") {
			continue
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok {
				return true
			}
			var mentions, carries bool
			ast.Inspect(ret, func(m ast.Node) bool {
				switch x := m.(type) {
				case *ast.BasicLit:
					if x.Kind == token.STRING && (strings.Contains(x.Value, "different lineage") || strings.Contains(x.Value, "different server")) {
						mentions = true
					}
				case *ast.SelectorExpr:
					if x.Sel.Name == "ErrPositionForeignLineage" {
						carries = true
					}
				}
				return true
			})
			if !mentions {
				return true
			}
			graded++
			if carries {
				carrying++
				return true
			}
			if why, ok := exempt[filepath.Base(path)]; ok {
				t.Logf("%s:%d exempt: %s", path, fset.Position(ret.Pos()).Line, why)
				return true
			}
			t.Errorf("%s:%d returns a \"different lineage\" verdict without ir.ErrPositionForeignLineage — it "+
				"wraps the sentinel that routes the automatic re-copy, which drops the target and refills it from "+
				"the other database (A0909-MYSQL-HIGH-1). Carry the sentinel, or list the site in this test's "+
				"exempt map with the reason it cannot.", path, fset.Position(ret.Pos()).Line)
			return true
		})
	}
	// Anti-vacuity: the four lanes' six known sites (GTID continuity,
	// file/pos identity, MariaDB anchor ×2 + domain, VStream foreign) plus
	// the one exempt reactive site.
	if graded < 6 {
		t.Fatalf("graded only %d \"different lineage\" return sites; the roster expects at least 6 — the phrase "+
			"was reworded or the scan broke", graded)
	}
	if carrying < 5 {
		t.Fatalf("only %d sites carry the sentinel; at least 5 must", carrying)
	}
}
