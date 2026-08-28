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
//  4. NO function REFERENCES a rostered identifier outside a direct
//     call (assignment, argument, &-reference) — `f := preflightX;
//     f(…)` would otherwise evade assertions 1–3, which see only
//     *ast.Ident call sites (the alias-evasion LOW, 2026-08-27;
//     detection self-tested by TestRosterAliasDetection_SelfTest).
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader
// than the truth: functions in internal/engines/mysql and their calls
// to — or in-body alias references of — the named preflight
// identifiers. Full data-flow tracking is out of scope: a function
// value smuggled through a struct field, a factory return, or another
// package would not be seen (no such shape exists in the package
// today, and the in-body reference check makes creating one a loud
// failure at the point of aliasing). It does NOT reach the VStream
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

	watched := map[string]bool{combinedOpener: true}
	for _, p := range cdcOpenPreflights {
		watched[p] = true
	}

	type site struct {
		name    string
		calls   map[string]bool
		aliased []string
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
			sites = append(sites, site{
				name:    qualifiedFuncName(fn),
				calls:   calls,
				aliased: nonCallRefIdents(fn, watched),
			})
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

	// (4) No alias references anywhere — assigning a rostered function to
	// a variable and invoking the alias would evade assertions 1–3, which
	// only see direct-call idents.
	for _, s := range sites {
		for _, name := range s.aliased {
			t.Errorf("%s references %s outside a direct call (assignment/argument/&-reference) — an aliased "+
				"invocation is invisible to this roster's call-site walker, so the gate cannot grade it; call "+
				"the function directly (or extend the walker if a function value is genuinely needed)",
				s.name, name)
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

// nonCallRefIdents returns every occurrence of a watched name in fn's body
// that is NOT the function position of a direct call — an assignment,
// argument, or &-reference through which the function could be invoked as an
// alias the call-site walkers cannot see. Shared by this file's roster and
// the FK cross-lane roster; self-tested (anti-vacuity) by
// TestRosterAliasDetection_SelfTest. Deliberately name-based: a LOCAL
// variable coincidentally named like a preflight would be flagged too, which
// is loud and fixed by renaming — the safe direction.
func nonCallRefIdents(fn *ast.FuncDecl, watched map[string]bool) []string {
	callFuns := map[*ast.Ident]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok {
				callFuns[id] = true
			}
		}
		return true
	})
	var out []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && watched[id.Name] && !callFuns[id] {
			out = append(out, id.Name)
		}
		return true
	})
	return out
}

// TestRosterAliasDetection_SelfTest is the anti-vacuity floor for the alias
// detection: a planted alias in a parsed fixture MUST be flagged, and a
// direct call MUST NOT be — so a walker change that stops seeing aliases
// cannot green the roster gates silently.
func TestRosterAliasDetection_SelfTest(t *testing.T) {
	t.Parallel()
	const fixture = `package p

func direct() { preflightBinlogFormat() }

func evade() {
	f := preflightBinlogFormat
	f()
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "fixture.go", fixture, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	watched := map[string]bool{"preflightBinlogFormat": true}
	byName := map[string]*ast.FuncDecl{}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok {
			byName[fn.Name.Name] = fn
		}
	}
	if refs := nonCallRefIdents(byName["direct"], watched); len(refs) != 0 {
		t.Errorf("direct call flagged as alias reference: %v — the detection over-fires and the roster gates would refuse legitimate code", refs)
	}
	if refs := nonCallRefIdents(byName["evade"], watched); len(refs) != 1 {
		t.Errorf("planted alias produced %d finding(s), want 1 — the alias detection went vacuous and the roster gates are evadable again", len(refs))
	}
}
