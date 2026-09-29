// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestOrchestrator_FrontierWaitsGoThroughDrainLanes holds the claim that every
// coordinator wait on the frontier wakes the lanes (ADR-0190 amendment C): in
// this package's non-test sources, WaitForFrontier is called only inside
// drainLanes, which queues the flush sentinel first. A new direct caller
// would wait out the lanes' idle grace — the ~100 ms-per-wait stall the
// 2026-09-29 benchmark measured — so it must go through drainLanes or be
// added here as a reasoned exemption. It reaches every .go file of the
// package by AST, not by name; the floor keeps it from passing on an empty walk.
func TestOrchestrator_FrontierWaitsGoThroughDrainLanes(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	waits, drains := 0, 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "WaitForFrontier":
					waits++
					if fn.Name.Name != "drainLanes" {
						t.Errorf("%s: %s calls WaitForFrontier directly; route it through drainLanes so the wait wakes the lanes",
							fset.Position(call.Pos()), fn.Name.Name)
					}
				case "drainLanes":
					drains++
				}
				return true
			})
		}
	}
	if waits == 0 || drains < 3 {
		t.Fatalf("found %d WaitForFrontier and %d drainLanes calls; want ≥1 and ≥3 (barrier, mark fence, look-ahead cap) — the walk missed a caller, or one was removed without updating this floor", waits, drains)
	}
}
