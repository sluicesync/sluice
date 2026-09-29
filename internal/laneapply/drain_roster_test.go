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

// directFrontierWaitExempt names the functions allowed to call
// WaitForFrontier WITHOUT going through drainLanes, each with its reason.
// drainLanes itself is the waking wait; routeRow is the look-ahead cap.
var directFrontierWaitExempt = map[string]string{
	"drainLanes": "the waking wait itself: it queues the flush sentinel, then waits",
	"routeRow": "the look-ahead cap: busy-lane backpressure, not an idle stall — it recurs about once per " +
		"committed batch behind a hot table (row 28), where a sentinel each time could cut the hot lane's " +
		"batches below its AIMD size; kept as v0.156.4's plain wait (ADR-0190 amendment C)",
}

// TestOrchestrator_FrontierWaitsGoThroughDrainLanes holds which coordinator
// waits on the frontier wake the lanes (ADR-0190 amendment C): in this
// package's non-test sources, WaitForFrontier is called only by the functions
// in directFrontierWaitExempt. A new direct caller would wait out the lanes'
// idle grace — the ~100 ms-per-wait stall the 2026-09-29 benchmark measured
// — so it must go through drainLanes or be added to the map with a reason.
// Both directions fail: a caller outside the map, and an exemption whose
// function no longer calls WaitForFrontier (a stale reason documents
// nothing). It walks every .go file of the package by AST; the floor on
// drainLanes calls (barrier, fence) keeps an empty walk from passing.
func TestOrchestrator_FrontierWaitsGoThroughDrainLanes(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	waitsIn := map[string]int{}
	drains := 0
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
					waitsIn[fn.Name.Name]++
					if _, ok := directFrontierWaitExempt[fn.Name.Name]; !ok {
						t.Errorf("%s: %s calls WaitForFrontier directly; route it through drainLanes so the wait wakes "+
							"the lanes, or exempt it in directFrontierWaitExempt with a reason",
							fset.Position(call.Pos()), fn.Name.Name)
					}
				case "drainLanes":
					drains++
				}
				return true
			})
		}
	}
	for fn := range directFrontierWaitExempt {
		if waitsIn[fn] == 0 {
			t.Errorf("directFrontierWaitExempt names %s, which no longer calls WaitForFrontier; drop the stale exemption", fn)
		}
	}
	if drains < 2 {
		t.Fatalf("found %d drainLanes calls; want ≥2 (barrier, mark fence) — the walk missed a caller, or one was "+
			"removed without updating this floor", drains)
	}
}
