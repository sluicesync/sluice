// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSchemaDeltaArming_ReachesEveryReaderOpenSite is the arming twin of
// TestSchemaSeed_ReachesEveryReaderOpenSite, built 2026-09-06 after H5's
// own sibling miss.
//
// THE MISS. H5 widened [Streamer.schemaDeltaAppliesToTarget] so
// `--schema-changes=refuse` arms the session-zone refusal like every
// other mode. The two SINGLE-STREAM reader-open sites had the setter
// call INLINED, and the two MULTI-DATABASE sites called it nowhere at
// all — so the fan-out stayed unarmed after the fix that was supposed to
// cover every mode. Two of four paths, in a fix whose whole subject was
// a door reaching one path and not its sibling.
//
// WHY THE EXISTING GATES COULD NOT SEE IT. Its sibling
// TestSchemaSeed_WiredWhereverTheRefusalIsArmed rosters "sites that
// ARM" and requires each to seed — so a site that arms NOTHING is
// outside its universe by construction, which is exactly the shape the
// fan-out had. And the seed roster's own doc recorded the reason as
// settled: the fan-out "arms nothing, because
// Streamer.schemaDeltaAppliesToTarget is false in multi-database mode by
// construction". True when written; false the moment H5 landed. A
// comment asserting an invariant, outliving the code it described.
//
// So this gate takes the same wider universe the seed roster took: a
// site that opens a change stream is a site where a first boundary can
// arrive, and a first boundary at an unarmed reader is the SLM-1 window
// regardless of what else that site wires.
//
// WHAT IT REACHES, stated so the name is not read as broader: every
// function in the package's non-test files whose body calls a method
// named StreamChanges must also reach wireSchemaDeltaArming, directly or
// through a package-local helper on its own receiver. A reader opened
// through a differently-named method, or in another package, is outside
// it.
func TestSchemaDeltaArming_ReachesEveryReaderOpenSite(t *testing.T) {
	// The change-stream opens that deliberately do NOT arm, each with the
	// reason. Identical to the seed roster's list and for the identical
	// reason: these lanes write changes into chunk files, never into a
	// target, so no re-zoned value can land in a target column and the
	// refusal has nothing to protect.
	exempt := map[string]string{
		"(*BackupStream).Run": "backup chain lane: changes are written to chunk files, not applied to a " +
			"target, so no re-zoned value can land in a target column",
		"(*BackupStream).newRolloverLoop": "backup chain lane, same as (*BackupStream).Run — its pump-open half",
		"(*IncrementalBackup).Run": "backup incremental lane: changes are written to chunk files, not " +
			"applied to a target",
	}

	const dir = "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	opens := map[string]string{}
	calls := map[string]map[string]bool{}
	armers := map[string]bool{}
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
			self := declIdentity(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					if calls[self] == nil {
						calls[self] = map[string]bool{}
					}
					calls[self][callee.Name] = true
				case *ast.SelectorExpr:
					switch callee.Sel.Name {
					case "StreamChanges":
						opens[self] = name
					case "wireSchemaDeltaArming":
						armers[self] = true
					}
					if x, ok := callee.X.(*ast.Ident); ok && x.Name == receiverName(fn) {
						if calls[self] == nil {
							calls[self] = map[string]bool{}
						}
						calls[self][receiverType(fn)+"."+callee.Sel.Name] = true
					}
				}
				return true
			})
		}
	}

	if parsed < 5 {
		t.Fatalf("parsed only %d non-test files in %q — the walk is not seeing the package", parsed, dir)
	}
	// Anti-vacuity: the package has several stream opens. A walk finding
	// none must FAIL rather than pass over an empty universe.
	if len(opens) < 4 {
		t.Fatalf("found only %d change-stream open site(s) %v; the package has more, so the AST walk has "+
			"drifted and this gate grades almost nothing", len(opens), opens)
	}

	// Reachability through the package-local call graph, so a site that
	// arms via a helper on its own receiver still passes.
	reaches := func(from string) bool {
		seen := map[string]bool{}
		var walk func(string) bool
		walk = func(cur string) bool {
			if armers[cur] {
				return true
			}
			if seen[cur] {
				return false
			}
			seen[cur] = true
			for callee := range calls[cur] {
				for cand := range calls {
					if strings.HasSuffix(cand, "."+callee) || cand == callee {
						if walk(cand) {
							return true
						}
					}
				}
				if armers[callee] {
					return true
				}
			}
			return false
		}
		return walk(from)
	}

	for site, file := range opens {
		if reaches(site) {
			continue
		}
		if why, ok := exempt[site]; ok {
			if strings.TrimSpace(why) == "" {
				t.Errorf("%s is exempt from the arming roster with an EMPTY reason", site)
			}
			continue
		}
		t.Errorf("%s (%s) opens a change stream without arming the session-zone refusal.\n\n"+
			"An unarmed reader cannot refuse a source zone swap at a table's first boundary, and every "+
			"row after that boundary lands in a target column of the other zone family — measured at "+
			"nine hours on MySQL 8 at +09:00. Call s.wireSchemaDeltaArming(<reader>), or add a %q entry "+
			"to this roster's exempt map saying why this lane has no target to diverge.", site, file, site)
	}

	// The exemption map's other half: an entry that no longer opens a
	// stream fails, so the list cannot rot into a blanket.
	for name := range exempt {
		if _, ok := opens[name]; !ok {
			t.Errorf("exempt names %q, which opens no change stream in this package (renamed? removed?) — "+
				"a stale exemption is how a real site later inherits a pass it was never granted", name)
		}
	}
}
