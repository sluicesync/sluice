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
	"strconv"
	"strings"
	"testing"
	"time"
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
// a context.Context AND a database handle. Each derived probe must
// contain a context.WithTimeout call in its own body OR in the body of a
// same-package function it directly calls (ONE hop —
// preflightFKReferentialActions delegates its census, and the cap, to the
// lane-shared warnFKReferentialActions; the same one-hop lookthrough the
// pump-join gate uses). Anything deeper needs an exemption whose reason
// names where the cap lives — or, better, the cap moved to the probe's
// top, the siblings' proven placement.
//
// "Database handle" is a STRUCTURAL predicate over the same AST, not a
// per-package list of type names ([dbHandleTypes]): *sql.DB / *sql.Tx /
// *sql.Conn, any package-level named type whose underlying type is one of
// those, and any package-level interface whose method set — following
// embedded in-package interfaces — declares QueryContext, QueryRowContext
// or ExecContext. It replaces a hand-list (audit 2026-08-31 F-T2) that
// was a transcription of today's code rather than a derivation: mysql
// listed three type names, postgres and pgtrigger one each, so a probe
// taking an IN-PACKAGE INTERFACE handle entered neither the graded
// universe nor the alias watch set — and the floors could not see the
// omission, because a floor counts what the walker FOUND, not what
// exists. That shape was not hypothetical: pgtrigger already declares
// settleQuerier for exactly this purpose ("Both *sql.DB … and a
// snapshot-pinned *sql.Conn … satisfy it").
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
// chokepoint. Two residuals belong specifically to the handle predicate:
// a handle reached through a STRUCT parameter (a probe taking *Engine, or
// a config struct carrying a *sql.DB field — there is no field
// transitivity here), and an interface declared in ANOTHER package or
// spelling its read differently (an sqlx-style Get/Select). Both are
// stated rather than closed; the predicate's own anti-vacuity floor is
// TestProbeDBHandleDetection_SelfTest.
//
// Mutation-verified in both directions: removing the context.WithTimeout
// derivation from pgtrigger's replica-role capture dispatch
// (checkReplicaRoleCaptureShapes, né warnReplicaRoleCaptureBlindness)
// fails its roster line; pointing a chokepoint entry at a nonexistent
// function fails the staleness guard; the floors fail if a package's
// derived probe set shrinks (2026-08-27). ADDITION-shaped, the direction
// F-T2 was about: a new uncapped pgtrigger probe taking the settleQuerier
// interface and called from openCDCReader passed the pre-F-T2 gate and
// reddens this one (2026-08-31).
//
// # The composed-latency half (audit 2026-08-31 C-6)
//
// Each cap bounds ONE probe; the probes run in sequence, so what an
// operator actually waits for at a wedged CDC open is count x cap. On
// pgtrigger that is seven 15s caps — 105s of worst-case added latency
// per open. That number is documented at [openProbeTimeout] and held
// here by capConst / worstCaseCeiling, so the NEXT serial probe (or a
// raised cap) has to re-justify the bound instead of adding 15s
// silently.
//
// Reach of that half, stated: pgtrigger only. mysql's preflights do not
// share one cap constant (rowImagePreflightTimeout is 30s, siblings
// differ) and postgres' chokepoint runs two, so neither composition is
// derived here. That is a stated gap, not an implied cover — a roster
// entry with no capConst is simply not latency-graded.
var probeTimeoutRoster = []struct {
	pkg         string   // directory under internal/engines/
	chokepoints []string // functions whose probe callees form the universe
	floor       int      // anti-vacuity: TODAY's exact derived count, so a drop reddens
	exempt      map[string]string

	capConst         string        // package const holding this path's per-probe cap ("" = not latency-graded)
	worstCaseCeiling time.Duration // ceiling on derived-probes x cap
}{
	{
		pkg:         "mysql",
		chokepoints: []string{"preflightBinlogCDCOpen"},
		floor:       5, // row-image, format, replica-source, db-filter, FK-actions
	},
	{
		pkg:         "postgres",
		chokepoints: []string{"createLogicalReplicationSlot"},
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
		// change-log existence, sequence grade, capture-posture read,
		// capture-shape door, replica-role shape dispatch (WARN/echo-
		// refusal), DDL-detection WARN, insecure-definer WARN (SEC-1).
		floor: 7,

		// The ceiling is TODAY'S bound, deliberately with no headroom:
		// headroom is exactly what lets one more serial probe land
		// unnoticed, which is the thing C-6 is about.
		capConst:         "openProbeTimeout",
		worstCaseCeiling: 105 * time.Second,
	},
}

// TestOpenPathProbesDeriveBoundedContexts is the gate over the roster
// above; see the file comment for the class, the derivation, and the
// stated reach.
func TestOpenPathProbesDeriveBoundedContexts(t *testing.T) {
	t.Parallel()
	for _, entry := range probeTimeoutRoster {
		fns, decls := parsePackage(t, entry.pkg)
		if len(fns) == 0 {
			t.Fatalf("%s: parsed no functions — the walker cannot see the package it grades", entry.pkg)
		}
		dbish := dbHandleTypes(decls)

		// Every probe-shaped package function (ctx+db params) — the set an
		// alias in a chokepoint body could smuggle past the derivation.
		probeShaped := map[string]bool{}
		for name, fn := range fns {
			if takesContextAndDB(fn, dbish) {
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
				if takesContextAndDB(target, dbish) {
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

		// The composed-latency ceiling (audit 2026-08-31 C-6). Only fires
		// when the bound GROWS: a gate that also failed on a REDUCTION
		// would defend the defect it exists to bound.
		if entry.capConst != "" {
			perProbe := parsePackageDurationConst(t, entry.pkg, entry.capConst)
			worst := time.Duration(len(probes)) * perProbe
			if worst > entry.worstCaseCeiling {
				t.Errorf("%s: %d serial open-path probes x %s = %s worst-case added latency per CDC open, "+
					"over the documented ceiling of %s.\n\n"+
					"Each cap bounds one probe; they run in sequence, so this is what an operator waits for "+
					"when the source is wedged — and part of it is spent by probes that can only WARN. Either "+
					"the new probe belongs off the serial open path, or the bound is genuinely larger and "+
					"both this ceiling and %s's doc owe the new number.",
					entry.pkg, len(probes), perProbe, worst, entry.worstCaseCeiling, entry.capConst)
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

// parsePackage parses every non-test .go file in the sibling engine
// package dir and returns its package-level (receiver-less) functions plus
// its package-level type declarations — the raw material [dbHandleTypes]
// needs to decide, structurally, which parameter types are database
// handles.
func parsePackage(t *testing.T, pkg string) (funcs map[string]*ast.FuncDecl, types map[string]ast.Expr) {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(pkg)
	if err != nil {
		t.Fatalf("read %s: %v", pkg, err)
	}
	funcs = map[string]*ast.FuncDecl{}
	types = map[string]ast.Expr{}
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
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Body != nil {
					funcs[d.Name.Name] = d
				}
			case *ast.GenDecl:
				if d.Tok != token.TYPE {
					continue
				}
				for _, spec := range d.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok {
						types[ts.Name.Name] = ts.Type
					}
				}
			}
		}
	}
	return funcs, types
}

// queryShapedMethods are the method names that make an interface a
// database handle: everything a probe could wedge on is issued through
// one of them. An interface spelling its read differently (an sqlx-style
// Get/Select) is the stated residual — see the file comment.
var queryShapedMethods = map[string]bool{
	"QueryContext":    true,
	"QueryRowContext": true,
	"ExecContext":     true,
}

// dbHandleTypes derives the set of parameter-type renderings that count as
// a database handle in this package, from the package's own type
// declarations rather than from a hand-list (audit 2026-08-31 F-T2).
// Three shapes qualify:
//
//   - the stdlib handles themselves — *sql.DB / *sql.Tx / *sql.Conn;
//   - a package-level interface declaring a query-shaped method, directly
//     or through an embedded in-package interface (mysql's dbQuerier
//     embeds rowQuerier and would otherwise need the transitive step);
//   - a package-level named type or alias whose underlying type is one of
//     the above (`type handle = *sql.DB`).
//
// The last two are resolved to a fixpoint so declaration ORDER across the
// package's files cannot change the answer.
func dbHandleTypes(decls map[string]ast.Expr) map[string]bool {
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
					// A named method (len(Names)==1) or an embedded
					// interface (len(Names)==0, Type is the name).
					if len(m.Names) == 1 {
						qualifies = qualifies || queryShapedMethods[m.Names[0].Name]
						continue
					}
					qualifies = qualifies || dbish[renderParamType(m.Type)]
				}
			default:
				qualifies = dbish[renderParamType(typ)]
			}
			if qualifies {
				dbish[name] = true
				changed = true
			}
		}
	}
	return dbish
}

// TestProbeDBHandleDetection_SelfTest is the anti-vacuity floor for
// [dbHandleTypes]: the F-T2 defect was a handle predicate that silently
// covered less than its name implied, so the predicate owes a fixture
// exercising each shape it claims — and one it must NOT claim, so a
// predicate that went permissive (marking every parameter db-ish, which
// would drag unrelated helpers into the graded universe and read as
// broader coverage) fails too.
func TestProbeDBHandleDetection_SelfTest(t *testing.T) {
	t.Parallel()
	const fixture = `package p

type rowQuerier interface {
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
}

type dbQuerier interface {
	rowQuerier
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
}

type aliasHandle = *sql.DB

type namedHandle *sql.Tx

type reporter interface {
	Report(msg string)
}

type settings struct{ Schema string }
`
	f, err := parser.ParseFile(token.NewFileSet(), "fixture.go", fixture, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	decls := map[string]ast.Expr{}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			if ts, ok := spec.(*ast.TypeSpec); ok {
				decls[ts.Name.Name] = ts.Type
			}
		}
	}
	dbish := dbHandleTypes(decls)
	for _, want := range []string{"sql.DB", "sql.Tx", "sql.Conn", "rowQuerier", "dbQuerier", "aliasHandle", "namedHandle"} {
		if !dbish[want] {
			t.Errorf("%s is not recognised as a database handle — a probe taking it would enter neither the "+
				"graded universe nor the alias watch set, which is exactly the F-T2 hole this predicate closed",
				want)
		}
	}
	for _, notWant := range []string{"reporter", "settings"} {
		if dbish[notWant] {
			t.Errorf("%s was recognised as a database handle — the predicate went permissive, so every "+
				"ctx-taking helper becomes a 'probe' and the gate's universe stops meaning what its name says",
				notWant)
		}
	}
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
// context.Context and a database handle, per the structural set
// [dbHandleTypes] derived from the package's own declarations.
func takesContextAndDB(fn *ast.FuncDecl, dbish map[string]bool) bool {
	var hasCtx, hasDB bool
	for _, field := range fn.Type.Params.List {
		typ := renderParamType(field.Type)
		if typ == "context.Context" {
			hasCtx = true
		}
		if dbish[typ] {
			hasDB = true
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

// parsePackageDurationConst reads a `const NAME = <n> * time.<Unit>`
// declaration out of a sibling engine package. The cap is read from the
// SOURCE rather than restated in the roster so that raising the constant
// — the other way the composed bound grows — cannot slip past the
// ceiling above.
func parsePackageDurationConst(t *testing.T, pkg, name string) time.Duration {
	t.Helper()
	units := map[string]time.Duration{
		"Nanosecond":  time.Nanosecond,
		"Microsecond": time.Microsecond,
		"Millisecond": time.Millisecond,
		"Second":      time.Second,
		"Minute":      time.Minute,
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(pkg)
	if err != nil {
		t.Fatalf("read %s: %v", pkg, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(pkg, e.Name()), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s/%s: %v", pkg, e.Name(), perr)
		}
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || len(vs.Names) != 1 || vs.Names[0].Name != name || len(vs.Values) != 1 {
					continue
				}
				bin, ok := vs.Values[0].(*ast.BinaryExpr)
				if !ok || bin.Op != token.MUL {
					t.Fatalf("%s.%s is not an `<n> * time.<Unit>` expression; the latency ceiling cannot read it",
						pkg, name)
				}
				lit, ok := bin.X.(*ast.BasicLit)
				if !ok || lit.Kind != token.INT {
					t.Fatalf("%s.%s: left operand is not an integer literal", pkg, name)
				}
				sel, ok := bin.Y.(*ast.SelectorExpr)
				if !ok {
					t.Fatalf("%s.%s: right operand is not a time.<Unit> selector", pkg, name)
				}
				unit, ok := units[sel.Sel.Name]
				if !ok {
					t.Fatalf("%s.%s: unrecognised duration unit %q", pkg, name, sel.Sel.Name)
				}
				n, cerr := strconv.Atoi(lit.Value)
				if cerr != nil {
					t.Fatalf("%s.%s: %v", pkg, name, cerr)
				}
				return time.Duration(n) * unit
			}
		}
	}
	t.Fatalf("%s: const %q not found — the roster names a cap this package does not declare", pkg, name)
	return 0
}

func sortedProbeNames(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
