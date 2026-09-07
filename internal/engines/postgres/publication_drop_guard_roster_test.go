// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// dropPublicationGuardExempt names each function whose body CONTAINS a
// `DROP PUBLICATION` string literal but needs no scope guard, with the
// reason. Fail-by-default: a new one that is neither guarded nor listed
// here fails this test.
//
// THE WALK IS DELIBERATELY BROAD — it matches any string literal, so a
// literal that is only ever printed (an operator hint) matches too. That
// over-matching is the safe direction: the failure mode worth preventing
// is a real emitter going unnoticed, and the cost of a false match is
// one entry here saying why. Narrowing the walk to "literals passed to
// Exec" would not help anyway, since a function can both execute one
// statement and print another (abandonDropSourceObjects does exactly
// that).
//
// The exemptions are otherwise narrow. "This path is only reached in
// tests" is NOT a reason — the guard costs one catalog query and the
// harm it prevents is a peer stream wedged or silently widened.
var dropPublicationGuardExempt = map[string]string{
	"dropOwnPublicationIfPerStream": "cleanup of a PER-STREAM publication, i.e. one this stream created under its " +
		"own --publication-name. No peer can be reading through a name scoped to this stream, so the " +
		"other-slots probe would always find nothing and refuse nothing. The per-stream condition is the " +
		"guard, and it is checked by the function itself rather than by a catalog probe.",
	"abandonDropSourceObjects": "NOT AN EMITTER. Its only `DROP PUBLICATION` literal is the `manual_drop` hint " +
		"in a slog field, telling the operator what to run by hand when cleanup fails. The actual drop is " +
		"delegated to dropOwnPublicationIfPerStream (exempt above, and for a real reason). Kept as an " +
		"entry rather than excluded by a cleverer walk, because the broad walk is the one that cannot " +
		"miss a real emitter.",
}

// TestPublicationDropSitesAreGuarded is the audit 2026-09-06 H2 gate.
//
// THE DEFECT IT PINS. Narrowing a publication (FOR ALL TABLES → scoped)
// ran [guardPublicationNarrowing] before its DROP, because removing
// tables from a peer's scope is silent loss. Widening (scoped → FOR ALL
// TABLES) dropped with no probe at all, on a comment asserting the drop
// was "safe because the publication is metadata only". The door faced
// one way for the whole ADR-0175 arc.
//
// It is the DROP that a peer experiences, not the resulting member set,
// and this file's own [ensurePublication] comment records both measured
// outcomes: a wedged non-zero resume (Bug 267) or a silent widening to
// database-wide with keyless tables then refusing UPDATE (Bug 270).
//
// WHAT IT REACHES: every function in this package whose body emits a
// `DROP PUBLICATION` statement, by AST. For each, it requires a call to
// one of the guard functions somewhere in the same function body — or an
// entry in [dropPublicationGuardExempt] saying why not.
//
// WHAT IT DOES NOT REACH, stated so the name cannot be read wider: it
// does not prove the guard runs BEFORE the drop, nor that the probe's
// excludeSlot is the caller's own. Those are properties of the call
// site. It proves the guard is present at all, which is the property
// that was missing.
func TestPublicationDropSitesAreGuarded(t *testing.T) {
	guards := map[string]bool{
		"guardPublicationNarrowing": true,
		"guardPublicationWidening":  true,
	}

	// An explicit glob rather than parser.ParseDir, and not only because
	// the latter is deprecated: ParseDir associates files with packages
	// WITHOUT considering build tags, which for this gate is the wrong
	// direction to be imprecise in. A build-tagged file can hold an
	// emitter, and this walk must see it.
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package files: %v", err)
	}

	fset := token.NewFileSet()
	type site struct{ fn, file string }
	var emitters []site

	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var emitsDrop, callsGuard bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if strings.Contains(strings.ToUpper(lit.Value), "DROP PUBLICATION") {
						emitsDrop = true
					}
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && guards[id.Name] {
						callsGuard = true
					}
				}
				return true
			})
			if !emitsDrop {
				continue
			}
			emitters = append(emitters, site{fn: fn.Name.Name, file: path})
			if callsGuard {
				continue
			}
			if why, exempt := dropPublicationGuardExempt[fn.Name.Name]; exempt {
				if strings.TrimSpace(why) == "" {
					t.Errorf("%s is exempt from the publication-drop guard with an EMPTY reason; "+
						"an exemption without a stated reason is indistinguishable from an oversight", fn.Name.Name)
				}
				continue
			}
			t.Errorf("%s (%s) emits DROP PUBLICATION without calling a scope guard.\n\n"+
				"A DROP is what a PEER stream experiences, whichever direction the rescope goes: with a "+
				"write in the drop→recreate window its resume dies non-zero asserting the publication does "+
				"not exist (Bug 267); with none it resumes silently database-wide and every keyless table "+
				"begins refusing UPDATE (Bug 270). Route it through guardPublicationNarrowing or "+
				"guardPublicationWidening, or add a %q entry to dropPublicationGuardExempt saying why the "+
				"peer cannot exist.", fn.Name.Name, path, fn.Name.Name)
		}
	}

	// Anti-vacuity, two-part. The first proves the AST walk still finds
	// the statement at all; the second proves the exemption map is not
	// naming functions that have been renamed away, which would let a
	// real emitter inherit a stale pass.
	if len(emitters) < 3 {
		t.Fatalf("found only %d DROP PUBLICATION emitter(s) %v; this package has more, so the AST walk "+
			"has drifted and this gate is grading almost nothing", len(emitters), emitters)
	}
	seen := map[string]bool{}
	for _, e := range emitters {
		seen[e.fn] = true
	}
	for name := range dropPublicationGuardExempt {
		if !seen[name] {
			t.Errorf("dropPublicationGuardExempt names %q, which emits no DROP PUBLICATION in this package "+
				"(renamed? removed?). A stale exemption is how a real emitter later inherits a pass it was "+
				"never granted.", name)
		}
	}
}
