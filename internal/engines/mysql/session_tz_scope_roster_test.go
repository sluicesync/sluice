// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// EVERY SITE THAT KILLS A STREAM OVER A SCHEMA CHANGE ASKS WHETHER THE
// TABLE IS IN SCOPE.
//
// # What went wrong, twice
//
// Bug 246 was the XA refusal firing on a table the sync EXCLUDED. The
// pipeline's table filter lives one stage downstream of this reader, so
// the reader decodes every table in the bound database and the filter
// drops the emitted change afterwards. A refusal that runs BEFORE that
// point therefore sees tables the stream emits nothing for, and killing
// the stream over one of them refuses a working configuration. It was
// fixed by wiring [ir.CDCScopePredicateSetter] and asking.
//
// Audit 2026-09-09 A0909-AQ-M-2 found the same defect on a SECOND
// refusal — the session-`time_zone` cast refusal — in the same file, with
// the predicate already sitting on the receiver, unconsulted. Its comment
// even claimed "the same posture as the PG reader's checkSchemaRace arm",
// and that arm IS scope-gated, so the sentence was true about where the
// check sits in the flow and false about the thing that mattered.
//
// The sweep widened it further: the two VStream lanes did not implement
// the setter AT ALL, so the pipeline's type-assert found nothing and
// their copy of the refusal was ungated by construction. The VStream tail
// request's rules end in `Match: "/.*/"` — every table in the keyspace
// arrives — so those lanes see excluded tables exactly as the binlog
// reader does.
//
// # What this roster reaches
//
// Every call to sessionTZCastRefusal in this package's non-test files,
// and it requires the enclosing function to consult a scope predicate —
// either `scopeAllowed` directly, the binlog reader's tableInScope, or
// the shared sessionTZRefusalInScope helper. It is deliberately keyed on
// the REFUSAL rather than on a list of lanes: a fourth lane, or a fourth
// call in an existing one, joins the roster by existing.
//
// It does NOT reach a different stream-killing refusal. If you add one,
// it needs its own answer to the same question; this gate will not tell
// you so. That is the honest boundary, and the reason the sibling sweep
// above is written out rather than summarised.
func TestSessionTZRefusalSitesAreScopeGated(t *testing.T) {
	const dir = "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	// Any of these, read anywhere in the enclosing function, counts as
	// asking the question. Named rather than pattern-matched so adding a
	// fourth spelling is a deliberate edit to this list.
	askers := map[string]bool{
		"scopeAllowed":            true,
		"tableInScope":            true,
		"sessionTZRefusalInScope": true,
		"relationInScope":         true,
	}

	type site struct{ fn, file string }
	var sites, gated []site
	parsed := 0
	fset := token.NewFileSet()

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		parsed++
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			refuses, asks := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.Ident:
					if node.Name == "sessionTZCastRefusal" {
						refuses = true
					}
					if askers[node.Name] {
						asks = true
					}
				case *ast.SelectorExpr:
					if node.Sel.Name == "sessionTZCastRefusal" {
						refuses = true
					}
					if askers[node.Sel.Name] {
						asks = true
					}
				}
				return true
			})
			if !refuses {
				continue
			}
			s := site{fn: fn.Name.Name, file: name}
			sites = append(sites, s)
			if asks {
				gated = append(gated, s)
			}
		}
	}

	if parsed < 10 {
		t.Fatalf("parsed only %d non-test files in %q — the walk is not seeing the package", parsed, dir)
	}
	// Anti-vacuity, at the true count. THREE lanes call this refusal: the
	// binlog reader, the VStream standalone reader, and the cold-start
	// snapshot stream's CDC half. A floor below three is a floor that
	// cannot fire, which is the shape audit A0909-TCI-H-3 was about.
	if len(sites) < 3 {
		names := make([]string, 0, len(sites))
		for _, s := range sites {
			names = append(names, s.file+":"+s.fn)
		}
		sort.Strings(names)
		t.Fatalf("found %d site(s) calling sessionTZCastRefusal (%v); this engine has THREE CDC lanes and "+
			"each carries one. The walk has broken rather than the code having shrunk — re-point it "+
			"rather than lowering this floor.", len(sites), names)
	}

	for _, s := range sites {
		found := false
		for _, g := range gated {
			if g == s {
				found = true
				break
			}
		}
		if found {
			continue
		}
		t.Errorf("%s (%s) can return sessionTZCastRefusal without consulting the table-scope predicate.\n\n"+
			"That refusal KILLS THE STREAM. The sync's table filter is applied one stage downstream of "+
			"the CDC readers, so an EXCLUDED table still reaches this code — and a TIMESTAMP/DATETIME "+
			"MODIFY on a table this stream emits nothing for cannot diverge the target, so refusing over "+
			"it refuses a working configuration (Bug 246's shape, re-found as audit 2026-09-09 "+
			"A0909-AQ-M-2). Gate it on scopeAllowed / sessionTZRefusalInScope, treating a nil predicate "+
			"as in-scope so an unwired pipeline still fails loud.", s.fn, s.file)
	}
	if len(gated) < 3 {
		t.Errorf("only %d of %d sessionTZCastRefusal site(s) consult the scope predicate; all three lanes "+
			"must", len(gated), len(sites))
	}
}

// TestEveryMySQLCDCLaneTakesTheScopePredicate is the wiring half. The
// roster above proves each refusal ASKS; this proves the pipeline can
// ANSWER, because a lane that does not implement the setter gets a
// silently-ignored type-assert and a permanently nil predicate — which
// is exactly how both VStream lanes came to be ungated.
//
// The compile-time pins in capabilities_assert.go say the same thing, but
// a pin is a line someone can delete alongside the method. This names the
// three lanes together so the SET is the assertion.
func TestEveryMySQLCDCLaneTakesTheScopePredicate(t *testing.T) {
	for _, tc := range []struct {
		lane   string
		reader any
	}{
		{"binlog", (*CDCReader)(nil)},
		{"vstream", (*vstreamCDCReader)(nil)},
		{"vstream cold-start snapshot", (*vstreamSnapshotChanges)(nil)},
	} {
		if _, ok := tc.reader.(ir.CDCScopePredicateSetter); !ok {
			t.Errorf("the %s CDC lane does not implement ir.CDCScopePredicateSetter, so the pipeline's "+
				"type-assert finds nothing and its scope predicate stays nil forever — every refusal in "+
				"that lane then fires on tables the sync excluded (audit 2026-09-09 A0909-AQ-M-2)", tc.lane)
		}
	}
}
