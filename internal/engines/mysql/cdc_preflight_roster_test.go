// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The CDC-open preflight roster (M2 capture-completeness remediation,
// 2026-08-26). Five preflights guard every binlog CDC open, bundled in
// ONE combined opener so no path can adopt a subset:
//
//	preflightBinlogRowImage         Bug 193   partial row images
//	preflightBinlogFormat           item 68e  STATEMENT/MIXED format
//	preflightReplicaSource          M2 G5     replica source, log_replica_updates=OFF
//	preflightBinlogDBFilter         M2 G6     --binlog-ignore-db / --binlog-do-db
//	preflightFKReferentialActions   M2 G9     FK referential-action capture WARN
//
// The historical failure shape this gate closes is the moved/narrowed
// door: a preflight added at one chokepoint and not its siblings (the
// row-image and format preflights each needed all three sites; G5's
// filing re-derived the chokepoint list because line references had
// drifted). Three assertions, all derived from the source rather than
// promised:
//
//  1. the combined opener [preflightBinlogCDCOpen] calls ALL five
//     preflights — dropping one from it un-gates every chokepoint at
//     once, and this is the line that reddens;
//  2. the three known chokepoints all call the combined opener — a
//     rename or a removed call fails loudly (staleness + door-removal
//     guard), and any NEW function calling it is simply fully gated;
//  3. NO other function calls an individual preflight directly — the
//     subset-adoption shape is refused at the gate, not at review.
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader
// than the truth: functions in internal/engines/mysql and their calls
// to the named preflight identifiers. It does NOT reach the VStream
// lane (vtgate owns the row-event contract; the one preflight whose
// class DOES span both lanes — the G9 FK-action WARN — has its own
// cross-lane roster, TestFKReferentialActionWarnRoster_BothLanes) or
// the bulk-only backup-full openers (deliberately
// ungated — they never read the binlog; the chain's first incremental
// runs StreamChanges and is gated there). A brand-new CDC-open path
// that calls NO preflight at all is out of reach too — catching that
// requires a caller graph over the binlog syncer itself; the three
// known sites are the only ones that construct one today.

package mysql

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
)

// combinedOpener is the one function allowed (and required) to call the
// individual preflights.
const combinedOpener = "preflightBinlogCDCOpen"

// cdcOpenPreflights is the full preflight set the combined opener owes.
// Adding a preflight here makes the opener owe it immediately.
var cdcOpenPreflights = []string{
	"preflightBinlogRowImage",
	"preflightBinlogFormat",
	"preflightReplicaSource",
	"preflightBinlogDBFilter",
	"preflightFKReferentialActions",
}

// knownChokepoints are the three binlog CDC-open sites. The walker must
// DISCOVER each calling the combined opener; anything else it discovers
// is a fourth fully-gated site and passes.
var knownChokepoints = []string{
	"CDCReader.StreamChanges",
	"Engine.openBinlogSnapshotStreamShared",
	"Engine.openBinlogSnapshotStreamConcurrent",
}

// TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights derives
// the roster from the source (see the file comment for the three
// assertions and the stated reach). The independent expected value is
// the source itself — the gate reads call sites, not anything the code
// reports about itself.
//
// Mutation-verified in both directions (2026-08-26): deleting the
// preflightReplicaSource call from preflightBinlogCDCOpen fails
// assertion 1; deleting the combined call from StreamChanges fails
// assertion 2; calling preflightBinlogFormat directly from a snapshot
// opener fails assertion 3.
func TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	type site struct {
		name  string
		calls map[string]bool
	}
	var sites []site
	declared := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			declared[fn.Name.Name] = true
			calls := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok {
						calls[id.Name] = true
					}
				}
				return true
			})
			sites = append(sites, site{name: qualifiedFuncName(fn), calls: calls})
		}
	}
	if len(sites) == 0 {
		t.Fatal("parsed no functions from the package — the walker cannot see the source it grades")
	}
	for _, p := range append([]string{combinedOpener}, cdcOpenPreflights...) {
		if !declared[p] {
			t.Fatalf("preflight %q is no longer a declared function in this package — the roster's marker set "+
				"is stale and the gate would grade nothing", p)
		}
	}

	// (1) The combined opener carries the FULL set.
	var chokepoints []string
	for _, s := range sites {
		if s.name == combinedOpener {
			for _, p := range cdcOpenPreflights {
				if !s.calls[p] {
					t.Errorf("%s does not call %s — every binlog CDC open just lost that door at once "+
						"(the moved-door shape, wholesale)", combinedOpener, p)
				}
			}
			continue
		}
		if s.calls[combinedOpener] {
			chokepoints = append(chokepoints, s.name)
		}
		// (3) Nothing else calls an individual preflight directly — a
		// direct call is the subset-adoption shape (a path gated by SOME
		// preflights reads as gated by all of them).
		for _, p := range cdcOpenPreflights {
			if s.calls[p] {
				t.Errorf("%s calls %s directly; binlog CDC-open paths must run the COMBINED %s so no path "+
					"can adopt a subset of the preflight set", s.name, p, combinedOpener)
			}
		}
	}

	// (2) The three known chokepoints are all discovered (anti-vacuity +
	// staleness guard).
	sort.Strings(chokepoints)
	for _, want := range knownChokepoints {
		if !slices.Contains(chokepoints, want) {
			t.Errorf("known chokepoint %q does not call %s (found callers: %v) — either it was renamed "+
				"(update knownChokepoints) or its preflights were removed (the Bug-193/68e/G5/G6 doors just "+
				"came off a CDC-open path)", want, combinedOpener, chokepoints)
		}
	}
}

// qualifiedFuncName renders "Recv.Name" for methods and "Name" for
// plain functions, so same-named methods on different receivers (e.g.
// the several StreamChanges implementations in this package) do not
// collapse into one roster entry.
func qualifiedFuncName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return fmt.Sprintf("%s.%s", receiverTypeName(fn.Recv.List[0].Type), fn.Name.Name)
}

func receiverTypeName(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return receiverTypeName(v.X)
	case *ast.IndexExpr: // generic receiver
		return receiverTypeName(v.X)
	}
	return "?"
}
