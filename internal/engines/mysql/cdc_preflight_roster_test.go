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
//  1. the combined opener [preflightBinlogCDCOpen] calls ALL of them —
//     dropping one from it un-gates every chokepoint at once, and this
//     is the line that reddens; a NEW preflight adopted by one
//     chokepoint and never added to the bundle reddens here too,
//     because the universe is derived from the package rather than
//     hand-listed (audit 2026-08-31 F-T3);
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
// # How the preflight universe is derived (not hand-listed)
//
// A preflight is a package-level function whose name carries the
// `preflight` prefix and which takes a context.Context plus a database
// handle — the handle predicate being structural over the same AST
// (*sql.DB / *sql.Tx / *sql.Conn, a named type over one of those, or an
// in-package interface declaring QueryContext / QueryRowContext /
// ExecContext, following embedded in-package interfaces so dbQuerier
// resolves through rowQuerier). The five names below are demoted to an
// anti-vacuity FLOOR the derived set must contain, not the universe
// itself.
//
// Until 2026-08-31 the universe WAS that five-name literal, which is the
// subset-adoption shape assertion 3 exists to refuse, one level up: a
// preflight adopted by `CDCReader.StreamChanges` and never added to the
// literal was watched by nothing, so it passed assertions 1, 3 and 4 in
// silence while `backup`'s incremental opener ran without it. Adding a
// preflight is no longer opt-in.
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader
// than the truth: functions in internal/engines/mysql and their calls
// to — or in-body alias references of — the DERIVED preflight
// identifiers. The derivation enforces a convention and inherits its
// blind spots: a CDC-open preflight named something other than
// `preflight…`, or one that takes no database handle (a config-only
// check), is invisible to it — as is any handle reached through a struct
// parameter, the same residual the sibling probe-timeout roster states.
// Full data-flow tracking is out of scope: a function value smuggled
// through a struct field, a factory return, or another package would not
// be seen (no such shape exists in the package today, and the in-body
// reference check makes creating one a loud failure at the point of
// aliasing). It does NOT reach the VStream lane (vtgate owns the
// row-event contract; the one preflight whose class DOES span both
// lanes — the G9 FK-action WARN — has its own cross-lane roster,
// TestFKReferentialActionWarnRoster_BothLanes) or the bulk-only
// backup-full openers (deliberately ungated — they never read the
// binlog; the chain's first incremental runs StreamChanges and is gated
// there). A brand-new CDC-open path that calls NO preflight at all is
// out of reach too — catching that requires a caller graph over the
// binlog syncer itself; the three known sites are the only ones that
// construct one today.

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

// knownCDCOpenPreflights is the ANTI-VACUITY FLOOR on the derived
// preflight universe, not the universe itself: the walker must discover
// at least these five, or its derivation has gone blind (a rename, a
// moved file, a shape change) and the gate would be grading nothing.
// The universe is [deriveCDCOpenPreflights].
var knownCDCOpenPreflights = []string{
	"preflightBinlogRowImage",
	"preflightBinlogFormat",
	"preflightReplicaSource",
	"preflightBinlogDBFilter",
	"preflightFKReferentialActions",
}

// cdcOpenPreflightExempt lists derived preflights that DELIBERATELY do
// not belong in the binlog CDC-open bundle, each with a written reason.
// Empty today: every preflight in this package guards a binlog CDC open.
// An entry is the escape hatch for a future `preflight…(ctx, db)` that
// belongs to some other lane — and the reason is what stops the escape
// hatch from becoming the hand-list again.
var cdcOpenPreflightExempt = map[string]string{}

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
// opener fails assertion 3. ADDITION-shaped, the direction F-T3 was
// about (2026-08-31): a new `preflightAuditMutantE(ctx, db dbQuerier)`
// wired into CDCReader.StreamChanges only — one of three chokepoints —
// passed every assertion while the universe was the five-name literal,
// and reddens assertions 1 and 3 now that the universe is derived.
func TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var decls []*ast.FuncDecl
	typeDecls := map[string]ast.Expr{}
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
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				decls = append(decls, d)
				declared[d.Name.Name] = true
			case *ast.GenDecl:
				if d.Tok != token.TYPE {
					continue
				}
				for _, spec := range d.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok {
						typeDecls[ts.Name.Name] = ts.Type
					}
				}
			}
		}
	}
	if len(decls) == 0 {
		t.Fatal("parsed no functions from the package — the walker cannot see the source it grades")
	}
	if !declared[combinedOpener] {
		t.Fatalf("%q is no longer a declared function in this package — the roster's chokepoint marker is "+
			"stale and the gate would grade nothing", combinedOpener)
	}

	// The preflight universe, derived from the package (see the file
	// comment). knownCDCOpenPreflights is the floor it must clear.
	cdcOpenPreflights := deriveCDCOpenPreflights(decls, mysqlDBHandleTypes(typeDecls))
	for _, p := range knownCDCOpenPreflights {
		if !slices.Contains(cdcOpenPreflights, p) {
			t.Fatalf("the derived preflight universe %v does not contain %q — the derivation went blind "+
				"(rename, shape change, or a walker regression) and every assertion below would be grading a "+
				"short set", cdcOpenPreflights, p)
		}
	}
	for exempted, reason := range cdcOpenPreflightExempt {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("cdcOpenPreflightExempt[%q] has no reason; the reason IS the gate", exempted)
		}
		if !slices.Contains(cdcOpenPreflights, exempted) {
			t.Errorf("cdcOpenPreflightExempt entry %q matches no derived preflight — a stale blessing is how "+
				"a roster starts covering less than its name implies; remove or update it", exempted)
		}
	}
	cdcOpenPreflights = slices.DeleteFunc(cdcOpenPreflights, func(p string) bool {
		_, exempt := cdcOpenPreflightExempt[p]
		return exempt
	})

	watched := map[string]bool{combinedOpener: true}
	for _, p := range cdcOpenPreflights {
		watched[p] = true
	}

	type site struct {
		name    string
		calls   map[string]bool
		aliased []string
	}
	sites := make([]site, 0, len(decls))
	for _, fn := range decls {
		sites = append(sites, site{
			name:    qualifiedFuncName(fn),
			calls:   directCallIdents(fn),
			aliased: nonCallRefIdents(fn, watched),
		})
	}

	// (1) The combined opener carries the FULL set.
	var chokepoints []string
	for _, s := range sites {
		if s.name == combinedOpener {
			for _, p := range cdcOpenPreflights {
				if !s.calls[p] {
					t.Errorf("%s does not call %s — either a door was dropped from the bundle (every binlog "+
						"CDC open loses it at once, the moved-door shape wholesale) or a new preflight was "+
						"adopted by one chokepoint without joining the bundle (audit F-T3). Wire it into %s, "+
						"or record a cdcOpenPreflightExempt reason if it belongs to another lane",
						combinedOpener, p, combinedOpener)
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

// deriveCDCOpenPreflights returns the package's preflight universe: every
// package-level function carrying the `preflight` name prefix that takes a
// context.Context and a database handle, minus the combined opener itself.
// Sorted so the failure messages are stable.
//
// The name prefix is the convention this gate enforces; the ctx+handle
// shape is what makes a function able to wedge on the server at all. See
// the file comment for the residual both leave.
func deriveCDCOpenPreflights(decls []*ast.FuncDecl, dbish map[string]bool) []string {
	var out []string
	for _, fn := range decls {
		name := fn.Name.Name
		if fn.Recv != nil || name == combinedOpener || !strings.HasPrefix(name, "preflight") {
			continue
		}
		if preflightShaped(fn, dbish) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// preflightShaped reports whether fn takes a context.Context and a
// database handle.
func preflightShaped(fn *ast.FuncDecl, dbish map[string]bool) bool {
	var hasCtx, hasDB bool
	for _, field := range fn.Type.Params.List {
		typ := renderTypeExpr(field.Type)
		if typ == "context.Context" {
			hasCtx = true
		}
		if dbish[typ] {
			hasDB = true
		}
	}
	return hasCtx && hasDB
}

// mysqlQueryShapedMethods are the method names that make an interface a
// database handle. Mirrors the sibling probe-timeout roster's set;
// duplicated because the two packages cannot share test code.
var mysqlQueryShapedMethods = map[string]bool{
	"QueryContext":    true,
	"QueryRowContext": true,
	"ExecContext":     true,
}

// mysqlDBHandleTypes derives which parameter-type renderings count as a
// database handle from the package's own type declarations: the stdlib
// handles, a package-level interface declaring a query-shaped method
// (following embedded in-package interfaces, so dbQuerier resolves via the
// rowQuerier it embeds), and a named type or alias over one of those. The
// fixpoint makes the answer independent of declaration order across files.
// Same predicate as the sibling probe-timeout roster's dbHandleTypes.
func mysqlDBHandleTypes(decls map[string]ast.Expr) map[string]bool {
	dbish := map[string]bool{"sql.DB": true, "sql.Tx": true, "sql.Conn": true}
	for changed := true; changed; {
		changed = false
		for name, typ := range decls {
			if dbish[name] {
				continue
			}
			var qualifies bool
			switch typ := typ.(type) {
			case *ast.InterfaceType:
				for _, m := range typ.Methods.List {
					if len(m.Names) == 1 {
						qualifies = qualifies || mysqlQueryShapedMethods[m.Names[0].Name]
						continue
					}
					qualifies = qualifies || dbish[renderTypeExpr(m.Type)]
				}
			default:
				qualifies = dbish[renderTypeExpr(typ)]
			}
			if qualifies {
				dbish[name] = true
				changed = true
			}
		}
	}
	return dbish
}

// renderTypeExpr renders a type expression as "Ident", "pkg.Ident", or the
// pointee's rendering for pointers; anything else renders "".
func renderTypeExpr(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.StarExpr:
		return renderTypeExpr(v.X)
	case *ast.SelectorExpr:
		if x, ok := v.X.(*ast.Ident); ok {
			return fmt.Sprintf("%s.%s", x.Name, v.Sel.Name)
		}
	}
	return ""
}

// directCallIdents returns the names fn calls by bare identifier.
func directCallIdents(fn *ast.FuncDecl) map[string]bool {
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

// TestCDCOpenPreflightDerivation_SelfTest is the anti-vacuity floor for
// the derivation above — the F-T3 defect was a universe narrower than the
// gate's name, so the derivation owes a fixture proving it sees a NEW
// preflight (the addition shape that was invisible), and does not sweep in
// a same-prefix helper that takes no handle or a non-preflight that does.
func TestCDCOpenPreflightDerivation_SelfTest(t *testing.T) {
	t.Parallel()
	const fixture = `package p

type rowQuerier interface {
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
}

type dbQuerier interface {
	rowQuerier
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
}

func preflightBinlogCDCOpen(ctx context.Context, db dbQuerier) error { return nil }

func preflightNewlyAdopted(ctx context.Context, q rowQuerier) error { return nil }

func preflightConfigOnly(ctx context.Context, cfg string) error { return nil }

func loadSomething(ctx context.Context, db dbQuerier) error { return nil }
`
	f, err := parser.ParseFile(token.NewFileSet(), "fixture.go", fixture, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	var fns []*ast.FuncDecl
	typeDecls := map[string]ast.Expr{}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			fns = append(fns, d)
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok {
					typeDecls[ts.Name.Name] = ts.Type
				}
			}
		}
	}
	got := deriveCDCOpenPreflights(fns, mysqlDBHandleTypes(typeDecls))
	want := []string{"preflightNewlyAdopted"}
	if !slices.Equal(got, want) {
		t.Errorf("derived %v, want %v — the derivation either stopped seeing a newly added preflight (the "+
			"F-T3 hole reopens) or widened past the ctx+handle shape (unrelated helpers become rostered "+
			"doors and the gate starts failing on correct code)", got, want)
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
