// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The G9 FK referential-action WARN's CROSS-LANE roster. The capture
// gap is a cross-engine CLASS sibling (the M2 structural insight): the
// binlog lane and the vstream lane miss cascaded child changes by the
// same mechanism through different code paths, so the WARN must reach
// BOTH lanes' open chokepoints and both must share ONE census so the
// lanes cannot drift. The binlog lane's reach is already enforced by
// TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights (the
// combined opener owes preflightFKReferentialActions at all three
// binlog chokepoints); this gate covers the half that roster cannot
// see — the vstream lane — plus the shared-census binding.
//
// WHAT THIS GATE REACHES, stated: functions in internal/engines/mysql
// and their calls to the named identifiers. The two vstream chokepoints
// it pins are [vstreamCDCReader.StreamChanges] (the standalone CDC
// open) and [Engine.openVStreamSnapshotStreamFrom] (the seedable core
// every vstream snapshot opener — fresh, filtered, COPY-resume,
// backup — funnels through). A brand-new vstream open path that calls
// neither is out of reach, exactly like the binlog roster's equivalent
// caveat.

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// fkCensusCore is the one shared census both lane wrappers must call.
const fkCensusCore = "warnFKReferentialActions"

// fkVStreamWrapper is the vstream-lane entry point.
const fkVStreamWrapper = "warnVStreamFKReferentialActions"

// fkVStreamChokepoints are the vstream-lane open sites that owe the
// WARN call.
var fkVStreamChokepoints = []string{
	"vstreamCDCReader.StreamChanges",
	"Engine.openVStreamSnapshotStreamFrom",
}

// TestFKReferentialActionWarnRoster_BothLanes derives the roster from
// the source (the independent expected value is the call graph itself).
//
// Mutation-verified in both directions (2026-08-26; mutants
// grep-confirmed, targeted-revert after): deleting the
// warnVStreamFKReferentialActions call from StreamChanges fails the
// chokepoint assertion; making preflightFKReferentialActions skip the
// shared census fails the binding assertion.
func TestFKReferentialActionWarnRoster_BothLanes(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	calls := map[string]map[string]bool{} // qualified func name -> called idents
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
			c := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok {
						c[id.Name] = true
					}
				}
				return true
			})
			calls[qualifiedFuncName(fn)] = c
		}
	}
	if len(calls) == 0 {
		t.Fatal("parsed no functions from the package — the walker cannot see the source it grades")
	}
	// Anti-vacuity: the roster's markers must still be declared
	// functions, or the assertions below would grade nothing.
	for _, marker := range []string{fkCensusCore, fkVStreamWrapper, "preflightFKReferentialActions"} {
		if _, ok := calls[marker]; !ok {
			t.Fatalf("%q is no longer a declared function in this package — the roster's marker set is stale", marker)
		}
	}

	// (1) Both vstream chokepoints call the lane wrapper.
	for _, cp := range fkVStreamChokepoints {
		c, ok := calls[cp]
		if !ok {
			t.Errorf("vstream chokepoint %q is no longer a declared function — update fkVStreamChokepoints "+
				"(a rename here is exactly how a door quietly stops covering a path)", cp)
			continue
		}
		if !c[fkVStreamWrapper] {
			t.Errorf("%s does not call %s — the vstream lane just lost the G9 FK-action WARN "+
				"(cross-engine CLASS sibling of the binlog lane's preflight)", cp, fkVStreamWrapper)
		}
	}

	// (2) Both lane entry points run the SAME census, so the two lanes'
	// detection logic cannot drift apart.
	for _, lane := range []string{"preflightFKReferentialActions", fkVStreamWrapper} {
		if !calls[lane][fkCensusCore] {
			t.Errorf("%s does not call the shared census %s — the two lanes' G9 detection can now drift",
				lane, fkCensusCore)
		}
	}
}
