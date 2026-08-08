// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package domaingate is the shared gate for the Bug 233 class: a target
// engine deciding what a column LANDS AS by dispatching on
// `col.Type`, where a Postgres `CREATE DOMAIN` wrapper matches no arm
// and the decision silently takes the default branch.
//
// # The class, in one sentence
//
// A DOMAIN is a constraint wrapper and not a storage one — Postgres
// stores and transmits a domain-typed value exactly as its base type,
// and every sluice target already downgrades the column to that base
// type in its DDL — so a target-side `switch col.Type.(type)` that
// does not unwrap is asking a question about a type the target will
// never create.
//
// # Why it is a WALKER and not a list of fixed sites
//
// Three axes of this one class have now been found, each after a fix
// that enumerated the previous ones and was believed complete: the
// array ELEMENT family (item 152), the ARRIVAL SHAPE of the same
// declared family (the item-152 value-fidelity review), and the TYPE
// PATH — this one. The first two were closed by matrices over values.
// A matrix over values cannot see this axis at all, because the defect
// is in which BRANCH runs, not in what the branch computes. So the
// check has to read the branches.
//
// It is shared rather than package-local for the reason
// internal/errclassgate is: the same defect exists once per target
// engine, and a gate that can only see the engine it was born in
// re-learns the lesson one engine at a time.
//
// # Which packages instantiate it (roadmap item 155 widened this)
//
// The original rule was "every engine that can be a TARGET for a
// Postgres-family source; an engine without a gate file is itself a
// finding". That drew the scope around WHO OWNS THE CODE, and the class
// does not respect that line: item 153 put ir.Domain dispatch in
// internal/translate, which is not an engine, and two real instances of
// this exact defect were sitting there when the gate finally arrived
// (the wide-varchar and unconstrained-numeric advisory scanners, both
// silently suppressed for a DOMAIN-wrapped column that MySQL's emitter
// down-maps regardless).
//
// The rule is therefore: **every package that DECIDES WHAT A COLUMN
// LANDS AS by dispatching on its IR type instantiates this gate** —
// each target engine, and internal/translate. A package that grows such
// a dispatch and no gate file is the finding, whether or not it is an
// engine. Current instantiations:
//
//   - internal/engines/mysql   (TestDomainTransparency_MySQLDispatchRoster)
//   - internal/engines/sqlite  (TestDomainTransparency_SQLiteDispatchRoster)
//   - internal/translate       (TestDomainTransparency_TranslateDispatchRoster)
//
// internal/engines/postgres is deliberately absent and this is the
// reason rather than an oversight: PG is the engine that HAS domains, so
// its writer creates the DOMAIN rather than downgrading to a base type,
// and "what does this column land as" is answered by the wrapper itself.
// The class is about targets that must flatten.
//
// # What a passing run proves, stated so it cannot be read as broader
//
// It proves that every `.Type` / `.SourceColumnType` type-dispatch in
// the walked package either reads the storage type through
// [ir.UnwrapDomain], reads the wrapper deliberately through
// [ir.DomainOf], or carries a written reason. It proves NOTHING about
// whether the unwrapped branch then does the right thing with the value
// — that is the family × shape matrices' job, and neither substitutes
// for the other.
//
// It reaches the two regions of a file named in [dispatchScopes]:
// function bodies AND package-level var/const initialisers. The second
// was missing until item 155, and a registry-shaped package (a
// `var rules = []rule{{match: func(...) …}}`) was entirely invisible to
// it — which the MinSites floor could not catch either, because the
// floor counts what the walk reached.
package domaingate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// unwrapFunc is the sanctioned storage-type reading. Matched on the
// selector's method name only, so an alias of package ir still
// satisfies the gate.
const unwrapFunc = "UnwrapDomain"

// typeFields are the [ir.Column] fields whose dispatch this gate grades.
// Both carry an [ir.Type] that can be an [ir.Domain] on a PG-sourced
// schema: Type on every lane, SourceColumnType on any lane a
// `--type-override` or a cross-engine retarget has passed through.
var typeFields = map[string]bool{
	"Type":             true,
	"SourceColumnType": true,
}

// Config describes one package's instantiation of the gate.
type Config struct {
	// Dir is the package directory to walk (usually ".").
	Dir string

	// Engine names the engine for failure messages.
	Engine string

	// Allowed are the deliberate exceptions, keyed
	// "file.go:funcName:operand-source" — the enclosing function is part
	// of the key so an entry cannot silently widen to a same-named
	// operand in a different function. Several dispatch sites inside ONE
	// function on ONE operand share a key and therefore one reason;
	// that is intended (they are one decision) and is why the reason
	// must describe the function, not a line.
	//
	// The value is the REASON unwrapping would be wrong or inert there.
	// An entry whose reason is not a real one is a fix that should have
	// been made.
	Allowed map[string]string

	// MinSites is the anti-vacuity floor: if the walk finds fewer
	// dispatch sites than this, the gate has probably stopped matching
	// the real dispatch shape (a rename, a refactor to a helper) and
	// would pass forever. Fail instead.
	MinSites int
}

// Assert runs the gate.
func Assert(t *testing.T, cfg Config) {
	t.Helper()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		t.Fatalf("read package dir %s: %v", cfg.Dir, err)
	}

	var offenders []string
	seen := make(map[string]bool)
	used := make(map[string]bool)
	checked := 0

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(cfg.Dir, name)
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("read %s: %v", path, rerr)
		}
		for _, scope := range dispatchScopes(f) {
			ast.Inspect(scope.node, func(n ast.Node) bool {
				operand, ok := dispatchOperand(n)
				if !ok {
					return true
				}
				kind, ok := classifyOperand(operand)
				if !ok {
					return true
				}
				// Every column-type dispatch counts toward the floor,
				// including the ones already reading the storage type —
				// otherwise fixing the package would shrink the walk to
				// the exemptions and make the floor grade nothing.
				checked++
				if kind != operandRaw {
					return true
				}
				key := name + ":" + scope.name + ":" + exprText(fset, operand, src)
				if _, allowed := cfg.Allowed[key]; allowed {
					used[key] = true
					return true
				}
				if seen[key] {
					return true
				}
				seen[key] = true
				pos := fset.Position(operand.Pos())
				offenders = append(offenders,
					name+":"+strconv.Itoa(pos.Line)+"  "+key)
				return true
			})
		}
	}

	if checked < cfg.MinSites {
		t.Fatalf("%s: only %d column-type dispatch site(s) found (floor %d); the gate has probably stopped "+
			"matching the real dispatch shape and is now vacuous — re-point it", cfg.Engine, checked, cfg.MinSites)
	}

	// A stale exemption is a finding too: it means the site was fixed or
	// deleted and the reason now shields nothing, so the next one to
	// appear under that key inherits an argument nobody made for it.
	var stale []string
	for key := range cfg.Allowed {
		if !used[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%s: %d exemption(s) match no dispatch site any more — delete them, or the next site to "+
			"take that key inherits a reason written for different code:\n  %s",
			cfg.Engine, len(stale), strings.Join(stale, "\n  "))
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%s: %d column-type dispatch site(s) read the DECLARED type instead of the STORAGE type "+
			"(the Bug 233 class — a PG `CREATE DOMAIN` column matches no arm and takes the default branch, "+
			"which is how a whole column class became un-migratable with MySQL's own charset error and no "+
			"sluice refusal):\n  %s\n\n"+
			"Wrap the operand in ir.UnwrapDomain(...) if the decision is about the storage the column lands "+
			"in; use ir.DomainOf(col) if it is genuinely about the wrapper's name or CHECKs; otherwise add "+
			"the key to Allowed with the reason unwrapping would be wrong or inert.",
			cfg.Engine, len(offenders), strings.Join(offenders, "\n  "))
	}
}

// dispatchScope is one named region of a file the walk descends into.
// The name becomes part of the exemption key, so it must be stable and
// human-recognisable.
type dispatchScope struct {
	name string
	node ast.Node
}

// dispatchScopes returns every region of a file that can contain a type
// dispatch. There are TWO, and the second was missing until roadmap item
// 155 — the gate walked function BODIES only, which is not where a
// registry-shaped package puts its predicates.
//
// `internal/translate/notes.go` declares its advisory rules as a
// package-level `var noteEntries = []noteEntry{{matches: func(...) …}}`;
// those `src.Type.(ir.JSON)` dispatches live in a GenDecl, so the walk
// never reached them and the package read as having fewer sites than it
// has. That is the gate-coverage failure mode CLAUDE.md calls worse than
// no gate: nothing about the walk's OUTPUT distinguished "this package
// dispatches correctly" from "this package's dispatches are invisible to
// me", and the MinSites floor cannot catch it either — a package whose
// FuncDecl sites are all fine clears the floor while its registry rots.
//
// Function bodies keep their existing scope name (the function's own),
// so instantiated rosters keyed on it are unaffected.
func dispatchScopes(f *ast.File) []dispatchScope {
	var scopes []dispatchScope
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body != nil {
				scopes = append(scopes, dispatchScope{d.Name.Name, d.Body})
			}
		case *ast.GenDecl:
			// var/const initialisers. A type declaration carries no
			// expressions to dispatch in, so only ValueSpecs matter.
			for _, spec := range d.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok || len(vs.Names) == 0 {
					continue
				}
				for _, val := range vs.Values {
					scopes = append(scopes, dispatchScope{vs.Names[0].Name, val})
				}
			}
		}
	}
	return scopes
}

// dispatchOperand returns the expression a type switch or type
// assertion dispatches on.
func dispatchOperand(n ast.Node) (ast.Expr, bool) {
	switch v := n.(type) {
	case *ast.TypeSwitchStmt:
		switch assign := v.Assign.(type) {
		case *ast.ExprStmt:
			if ta, ok := assign.X.(*ast.TypeAssertExpr); ok {
				return ta.X, true
			}
		case *ast.AssignStmt:
			if len(assign.Rhs) == 1 {
				if ta, ok := assign.Rhs[0].(*ast.TypeAssertExpr); ok {
					return ta.X, true
				}
			}
		}
	case *ast.TypeAssertExpr:
		// v.Type == nil is the `x.(type)` inside a type switch, already
		// handled above; skip it so the site is not counted twice.
		if v.Type != nil {
			return v.X, true
		}
	}
	return nil, false
}

// The two readings of a column's type a TYPE DISPATCH can take.
//
// The third sanctioned reading — the wrapper's own name/CHECKs — is
// spelled `ir.DomainOf(col)`, which returns a value rather than
// dispatching, so it never reaches this walk and needs no exemption.
// That is the point of giving it a name: an identity consumer written
// as `col.Type.(ir.Domain)` IS a raw dispatch and this gate holds it to
// the same account as every other one.
const (
	operandRaw       = "raw"     // `col.Type.(…)` — the declared type, wrapper and all
	operandUnwrapped = "storage" // `ir.UnwrapDomain(col.Type).(…)`
)

// classifyOperand reports how a type dispatch reads a column's type,
// and whether it reads one at all. Only [operandRaw] is a finding;
// [operandUnwrapped] still counts toward the anti-vacuity floor, so the
// floor measures "does this package still dispatch on column types"
// rather than "does it still do so wrongly".
func classifyOperand(operand ast.Expr) (string, bool) {
	switch v := operand.(type) {
	case *ast.SelectorExpr:
		if typeFields[v.Sel.Name] {
			return operandRaw, true
		}
	case *ast.CallExpr:
		sel, ok := v.Fun.(*ast.SelectorExpr)
		if !ok || len(v.Args) != 1 {
			return "", false
		}
		if sel.Sel.Name == unwrapFunc {
			return operandUnwrapped, true
		}
	}
	return "", false
}

// exprText renders an expression back to source for the key and the
// failure message.
func exprText(fset *token.FileSet, e ast.Expr, src []byte) string {
	start := fset.Position(e.Pos()).Offset
	end := fset.Position(e.End()).Offset
	if start < 0 || end > len(src) || start >= end {
		return "<unrenderable>"
	}
	return strings.TrimSpace(string(src[start:end]))
}
