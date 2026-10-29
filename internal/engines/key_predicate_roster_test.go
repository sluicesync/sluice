// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

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

// TestKeyPredicateRoster_EveryUpdateDeleteBuilderGetsTheNarrowedImage holds
// the Bug 339 fix to every UPDATE/DELETE statement the appliers build: each
// call of buildUpdateSQL / buildDeleteSQL under internal/engines/{mysql,
// postgres} must pass, as its before-image argument, a plain identifier
// assigned in the same function from the key narrowing —
// appliershared.KeyPredicate or the Postgres applier's keyPredicateFor — and
// never a change's raw `.Before`.
//
// A raw before-image reaching a builder is the defect: its WHERE compares
// every column of a full image (VStream, a trigger source's full capture
// mode, a replayed chain), so a row whose non-key column the added-column
// backfill or anything else moved ahead of the stream matches nothing and the
// change is dropped at exit 0. The roster derives its universe from the AST,
// so a new apply path that builds an UPDATE or DELETE is graded the moment it
// exists. Its floor: the six call sites the fix covers (Postgres serial and
// pipelined UPDATE and DELETE, MySQL per-change UPDATE and DELETE).
//
// Reach, stated: the builders only. The MySQL batch path's coalesced forms
// (the ADR-0140 upsert and the DELETE … IN) never take a before-image WHERE —
// they key on the primary key by construction — so they are outside it.
func TestKeyPredicateRoster_EveryUpdateDeleteBuilderGetsTheNarrowedImage(t *testing.T) {
	const floor = 6
	builders := map[string]bool{"buildUpdateSQL": true, "buildDeleteSQL": true}
	narrowers := map[string]bool{"KeyPredicate": true, "keyPredicateFor": true}
	var sites, bad []string
	for _, dir := range []string{"mysql", "postgres"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				narrowed := narrowedIdents(fn.Body, narrowers)
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					id, ok := call.Fun.(*ast.Ident)
					if !ok || !builders[id.Name] || len(call.Args) < 4 {
						return true
					}
					where := fset.Position(call.Pos()).String()
					sites = append(sites, where)
					arg, ok := call.Args[3].(*ast.Ident)
					if !ok || !narrowed[arg.Name] {
						bad = append(bad, where+" ("+fn.Name.Name+"): "+id.Name+" is handed a before-image that was not "+
							"narrowed by appliershared.KeyPredicate / keyPredicateFor")
					}
					return true
				})
			}
		}
	}
	sort.Strings(sites)
	if len(sites) < floor {
		t.Fatalf("found %d UPDATE/DELETE builder call sites, want at least %d — the walk lost its universe:\n  %s",
			len(sites), floor, strings.Join(sites, "\n  "))
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		t.Errorf("Bug 339: %d builder call site(s) build a WHERE from a raw before-image:\n  %s",
			len(bad), strings.Join(bad, "\n  "))
	}
}

// narrowedIdents returns the identifiers assigned (`:=` or `=`) from a call
// whose function is one of narrowers, anywhere in body.
func narrowedIdents(body *ast.BlockStmt, narrowers map[string]bool) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 || len(as.Lhs) == 0 {
			return true
		}
		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		var fname string
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			fname = fun.Name
		case *ast.SelectorExpr:
			fname = fun.Sel.Name
		}
		if !narrowers[fname] {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); ok {
			out[id.Name] = true
		}
		return true
	})
	return out
}
