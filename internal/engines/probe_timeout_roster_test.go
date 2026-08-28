// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The open-path probe-timeout roster (audit 2026-08-27 A5).
//
// # The class
//
// A probe added at a CDC-open chokepoint runs BEFORE the stream exists,
// so nothing downstream can time it out — an unbounded probe converts a
// wedged read (a queued ACCESS EXCLUSIVE parking a user-table count, a
// half-dead pooled connection) into a silent, indefinite hang of the very
// open the probe was added to protect. The convention — derive a
// context.WithTimeout at the top of the probe function — was applied to
// every MySQL binlog preflight (30s) and the Postgres slot-create probe
// (15s), and to none of pgtrigger's five doors; A5 is that missed sweep.
// This gate makes the convention mechanical so the next probe cannot ship
// without its cap.
//
// # How the universe is derived (not hand-listed)
//
// Per engine package, the probes are the direct callees of that package's
// CDC-open chokepoint function(s) that are package-level functions taking
// a context.Context AND a database handle (the package's own db-ish param
// types). Each derived probe must contain a context.WithTimeout call in
// its own body OR in the body of a same-package function it directly
// calls (ONE hop — preflightFKReferentialActions delegates its census,
// and the cap, to the lane-shared warnFKReferentialActions; the same
// one-hop lookthrough the pump-join gate uses). Anything deeper needs an
// exemption whose reason names where the cap lives — or, better, the cap
// moved to the probe's top, the siblings' proven placement.
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader
// than the truth: probes invoked BY NAME from the configured chokepoints
// in mysql, postgres, and pgtrigger. A probe invoked through an ALIAS in a
// chokepoint body would never enter the derived universe, so the walker
// additionally refuses any chokepoint-body reference to a probe-shaped
// package function (ctx+db params) outside a direct call — self-tested by
// TestProbeAliasDetection_SelfTest (the alias-evasion LOW, 2026-08-27).
// Honest residual, still out of reach: a probe function value smuggled
// through a struct field or factory return, probes aliased in NON-chokepoint
// functions and passed in, chokepoints not configured here (a brand-new open
// path owes this list its chokepoint), the sqlite/D1 engines
// (request/response transports with per-call HTTP deadlines; no lock-queue
// analogue), and general catalog helpers that are not probes at a
// chokepoint. The anti-vacuity floors below fail the gate if a walker
// change stops it seeing the probes it grades today.
//
// Mutation-verified in both directions (2026-08-27): removing the
// context.WithTimeout derivation from pgtrigger's replica-role capture
// dispatch (checkReplicaRoleCaptureShapes, né warnReplicaRoleCaptureBlindness)
// fails its roster line; pointing a chokepoint entry at a nonexistent
// function fails the staleness guard; the floors fail if a package's
// derived probe set shrinks.
var probeTimeoutRoster = []struct {
	pkg         string   // directory under internal/engines/
	chokepoints []string // functions whose probe callees form the universe
	dbishTypes  []string // param types that mark a callee as a probe
	floor       int      // anti-vacuity: minimum probes the walker must derive
	exempt      map[string]string
}{
	{
		pkg:         "mysql",
		chokepoints: []string{"preflightBinlogCDCOpen"},
		dbishTypes:  []string{"dbQuerier", "rowQuerier", "sql.DB"},
		floor:       5, // row-image, format, replica-source, db-filter, FK-actions
	},
	{
		pkg:         "postgres",
		chokepoints: []string{"createLogicalReplicationSlot"},
		dbishTypes:  []string{"sql.DB"},
		floor:       2, // serverVersionNum + warnPreparedTransactions
		exempt: map[string]string{
			"serverVersionNum": "reads one GUC (SHOW server_version_num) — no table access, so the " +
				"lock-queue wedge this gate exists for cannot reach it; it is a package-wide shared helper " +
				"predating the probe convention, and bounding every shared catalog read is a separate sweep.",
		},
	},
	{
		pkg:         "pgtrigger",
		chokepoints: []string{"openCDCReader"},
		dbishTypes:  []string{"sql.DB"},
		// change-log existence, sequence grade, capture-posture read,
		// capture-shape door, replica-role shape dispatch (WARN/echo-
		// refusal), DDL-detection WARN.
		floor: 6,
	},
}

// TestOpenPathProbesDeriveBoundedContexts is the gate over the roster
// above; see the file comment for the class, the derivation, and the
// stated reach.
func TestOpenPathProbesDeriveBoundedContexts(t *testing.T) {
	t.Parallel()
	for _, entry := range probeTimeoutRoster {
		fns := parsePackageFuncs(t, entry.pkg)
		if len(fns) == 0 {
			t.Fatalf("%s: parsed no functions — the walker cannot see the package it grades", entry.pkg)
		}

		// Every probe-shaped package function (ctx+db params) — the set an
		// alias in a chokepoint body could smuggle past the derivation.
		probeShaped := map[string]bool{}
		for name, fn := range fns {
			if takesContextAndDB(fn, entry.dbishTypes) {
				probeShaped[name] = true
			}
		}

		probes := map[string]bool{}
		for _, choke := range entry.chokepoints {
			fn, ok := fns[choke]
			if !ok {
				t.Errorf("%s: chokepoint %q is not a declared function — the roster is stale and this "+
					"package's probes are ungated; update the entry", entry.pkg, choke)
				continue
			}
			for callee := range directCallees(fn) {
				target, ok := fns[callee]
				if !ok {
					continue // method call on another package / builtin
				}
				if takesContextAndDB(target, entry.dbishTypes) {
					probes[callee] = true
				}
			}
			// Alias-evasion guard: a probe referenced in the chokepoint
			// body outside a direct call (assignment, argument,
			// &-reference) would be invoked without ever entering the
			// derived universe above — refuse the reference itself.
			for _, aliased := range nonCallFuncRefs(fn, probeShaped) {
				t.Errorf("%s: chokepoint %s references probe-shaped function %s outside a direct call — an "+
					"aliased probe invocation never enters this gate's derived universe, so its timeout would be "+
					"ungraded; call the probe directly (or extend the walker if a function value is genuinely needed)",
					entry.pkg, choke, aliased)
			}
		}
		if len(probes) < entry.floor {
			names := sortedProbeNames(probes)
			t.Errorf("%s: derived only %d probe(s) %v from chokepoints %v (floor %d) — either probes were "+
				"removed or the walker stopped seeing them; both mean this gate is grading less than it claims",
				entry.pkg, len(probes), names, entry.chokepoints, entry.floor)
		}

		for _, probe := range sortedProbeNames(probes) {
			if reason, ok := entry.exempt[probe]; ok {
				if strings.TrimSpace(reason) == "" {
					t.Errorf("%s.%s: exempt with no reason; the reason IS the gate", entry.pkg, probe)
				}
				continue
			}
			if !probeDerivesBoundedContext(fns[probe], fns) {
				t.Errorf("%s.%s: open-path probe does not derive a context.WithTimeout (in its body or one "+
					"hop into a same-package helper it calls) — an unbounded probe at a CDC-open chokepoint "+
					"hangs the open silently when the read wedges (lock queue, dead connection; audit "+
					"2026-08-27 A5). Derive the cap at the probe's top (the rowImagePreflightTimeout / "+
					"openProbeTimeout pattern), or add an exemption here whose reason says why no cap is "+
					"needed", entry.pkg, probe)
			}
		}

		// Staleness guard on the exemptions: a blessing for a function the
		// walker no longer derives is a roster row covering nothing.
		for exempted := range entry.exempt {
			if !probes[exempted] {
				t.Errorf("%s: exemption for %q matches no derived probe — remove or update it; a stale "+
					"blessing is how a roster starts covering less than its name implies", entry.pkg, exempted)
			}
		}
	}
}

// parsePackageFuncs parses every non-test .go file in the sibling engine
// package dir and returns its package-level (receiver-less) functions.
func parsePackageFuncs(t *testing.T, pkg string) map[string]*ast.FuncDecl {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(pkg)
	if err != nil {
		t.Fatalf("read %s: %v", pkg, err)
	}
	fns := map[string]*ast.FuncDecl{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(pkg, name), nil, parser.SkipObjectResolution)
		if perr != nil {
			t.Fatalf("parse %s/%s: %v", pkg, name, perr)
		}
		for _, d := range f.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Body != nil {
				fns[fn.Name.Name] = fn
			}
		}
	}
	return fns
}

// nonCallFuncRefs returns every occurrence of a watched name in fn's body
// that is NOT the function position of a direct call — the alias shapes
// (`p := probeX; p(ctx, db)`, a probe passed as an argument, &probeX)
// through which a probe could run without entering the derived universe.
// Self-tested by TestProbeAliasDetection_SelfTest. The mysql package's
// roster gates carry the same helper (nonCallRefIdents); duplicated here
// because the packages cannot share test code.
func nonCallFuncRefs(fn *ast.FuncDecl, watched map[string]bool) []string {
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

// TestProbeAliasDetection_SelfTest is the anti-vacuity floor for the alias
// detection above: a planted alias in a parsed fixture MUST be flagged and a
// direct call MUST NOT be, so a walker change cannot quietly stop seeing the
// evasion shape.
func TestProbeAliasDetection_SelfTest(t *testing.T) {
	t.Parallel()
	const fixture = `package p

func direct() { probeX() }

func evade() {
	p := probeX
	p()
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "fixture.go", fixture, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	watched := map[string]bool{"probeX": true}
	byName := map[string]*ast.FuncDecl{}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok {
			byName[fn.Name.Name] = fn
		}
	}
	if refs := nonCallFuncRefs(byName["direct"], watched); len(refs) != 0 {
		t.Errorf("direct call flagged as alias reference: %v — the detection over-fires and would refuse legitimate chokepoint code", refs)
	}
	if refs := nonCallFuncRefs(byName["evade"], watched); len(refs) != 1 {
		t.Errorf("planted alias produced %d finding(s), want 1 — the alias detection went vacuous and the probe gate is evadable again", len(refs))
	}
}

// directCallees returns the names of package-local functions fn calls by
// bare identifier.
func directCallees(fn *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok {
				out[id.Name] = true
			}
		}
		return true
	})
	return out
}

// takesContextAndDB reports whether fn's parameters include a
// context.Context and one of the package's db-ish types.
func takesContextAndDB(fn *ast.FuncDecl, dbish []string) bool {
	var hasCtx, hasDB bool
	for _, field := range fn.Type.Params.List {
		typ := renderParamType(field.Type)
		if typ == "context.Context" {
			hasCtx = true
		}
		for _, want := range dbish {
			if typ == want {
				hasDB = true
			}
		}
	}
	return hasCtx && hasDB
}

// renderParamType renders a parameter type as "Ident", "pkg.Ident", or
// the pointee's rendering for pointers; anything else renders "".
func renderParamType(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return renderParamType(v.X)
	case *ast.SelectorExpr:
		if x, ok := v.X.(*ast.Ident); ok {
			return fmt.Sprintf("%s.%s", x.Name, v.Sel.Name)
		}
	}
	return ""
}

// probeDerivesBoundedContext reports whether fn derives a
// context.WithTimeout in its own body, or — one hop — in the body of a
// same-package function it calls by name.
func probeDerivesBoundedContext(fn *ast.FuncDecl, fns map[string]*ast.FuncDecl) bool {
	if bodyDerivesContextWithTimeout(fn) {
		return true
	}
	for callee := range directCallees(fn) {
		if target, ok := fns[callee]; ok && bodyDerivesContextWithTimeout(target) {
			return true
		}
	}
	return false
}

// bodyDerivesContextWithTimeout reports whether fn's body contains a
// context.WithTimeout call.
func bodyDerivesContextWithTimeout(fn *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if x, ok := sel.X.(*ast.Ident); ok && x.Name == "context" && sel.Sel.Name == "WithTimeout" {
			found = true
			return false
		}
		return true
	})
	return found
}

func sortedProbeNames(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
