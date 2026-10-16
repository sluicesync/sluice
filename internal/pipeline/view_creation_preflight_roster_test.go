// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// EVERY PLACE THAT CREATES VIEWS NAMES THE VIEW PREFLIGHT THAT COVERED THEM
// (Bug 317 review).
//
// TestIndexEmitPreflightReachesEveryCopyEntryPoint proves each copy ENTRY
// POINT calls migcore.PreflightViewEmit. It cannot see where views are
// actually CREATED, and since Bug 317 those are not always inside the entry
// point: both fan-outs hand every namespace's views to a deferred pass
// (createDeferredViews) that runs after the per-namespace passes return. A
// new creation site — a third fan-out, a deferred pass fed from somewhere
// the preflight never ran — would be invisible to the entry-point roster.
//
// So this roster is keyed on the CREATION side: every declaration in the
// walked packages that calls a view-creating primitive (migcore.RunViewsPhase,
// runRecordedViewsPhase, createDeferredViews, or any `.CreateViews(` method)
// must be listed, either with the declaration whose PreflightViewEmit call
// covers the views it creates, or as a wrapper whose own callers are
// listed. Fail-by-default both ways: an unlisted creator fails, and so does a
// listed one that no longer creates views.
//
// Scope, stated so the name cannot be read as broader than the truth: the
// gate proves the BINDING is written down and that the named coverer still
// calls PreflightViewEmit. It does not prove data flow (that the views
// created are the ones that were preflighted); the integration pins
// (TestMigrate_ViewsOnlyNamespace_*, the Bug 314 view pins) grade that.

const viewCreatorWrapper = "wrapper"

// viewCreationRoster: package-relative directory → creating declaration →
// the declaration (same directory) whose PreflightViewEmit covers it, or
// viewCreatorWrapper.
var viewCreationRoster = map[string]map[string]string{
	".": {
		"runRecordedViewsPhase": viewCreatorWrapper,
		"createDeferredViews":   viewCreatorWrapper,
		// migrate: views phase of the shared ladder; the fast-parallel sync
		// cold start reaches it from (*Streamer).coldStart.
		"runBulkCopyPhases": "(*Migrator).phaseTranslateAndGateSchema",
		// serial sync cold start, single namespace; the fan-out strips the
		// views before calling it.
		"runBulkCopyWithOpts": "(*Streamer).coldStart",
		// add-table: its scoped schema carries no views, but the call is there.
		"runBulkCopyForAddTable": "(*AddTable).Run",
		// stopped-cold-start resume.
		"runColdStartResumePhases": "(*Streamer).resumeTargetPreflight",
		// the deferred passes: each namespace's views were preflighted in its
		// own per-namespace pass.
		"(*Migrator).runMultiDatabase":       "(*Migrator).phaseTranslateAndGateSchema",
		"(*Streamer).coldStartMultiDatabase": "(*Streamer).coldStartCopyOneDatabase",
	},
	"backup": {
		"(*Restore).Run": "(*Restore).refuseUnrepresentableTargetShape",
	},
}

// viewCreators returns the declarations in dir that call a view-creating
// primitive.
func viewCreators(t *testing.T, dir string) (creators map[string]bool, parsed int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	creators = map[string]bool{}

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
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.Ident:
					if f.Name == "runRecordedViewsPhase" || f.Name == "createDeferredViews" {
						creators[declIdentity(fn)] = true
					}
				case *ast.SelectorExpr:
					if f.Sel.Name == "CreateViews" || f.Sel.Name == "RunViewsPhase" {
						creators[declIdentity(fn)] = true
					}
				}
				return true
			})
		}
	}
	return creators, parsed
}

func TestViewCreationRoster_EveryCreatorNamesItsPreflight(t *testing.T) {
	total := 0
	for dir, roster := range viewCreationRoster {
		creators, parsed := viewCreators(t, dir)
		if parsed < 5 {
			t.Fatalf("parsed only %d non-test files in %q — the walk is not seeing the package", parsed, dir)
		}
		total += len(creators)
		preflighters, _ := callersOfPreflight(t, dir, "PreflightViewEmit")

		var unlisted []string
		for decl := range creators {
			if _, ok := roster[decl]; !ok {
				unlisted = append(unlisted, decl)
			}
		}
		sort.Strings(unlisted)
		for _, decl := range unlisted {
			t.Errorf("%s in %q creates views but is not in viewCreationRoster. Name the declaration whose "+
				"migcore.PreflightViewEmit covers the views it creates (or mark it %q if only its callers create), "+
				"so a view refusal cannot be bypassed by a new creation path", decl, dir, viewCreatorWrapper)
		}
		for decl, coverer := range roster {
			if !creators[decl] {
				t.Errorf("%s in %q is rostered but no longer creates views — remove the stale entry", decl, dir)
				continue
			}
			if coverer == viewCreatorWrapper {
				continue
			}
			if !preflighters[coverer] {
				t.Errorf("%s in %q names %s as its view preflight, but %s does not call migcore.PreflightViewEmit",
					decl, dir, coverer, coverer)
			}
		}
	}
	// Anti-vacuity: the walk must still be finding the known creators. A
	// renamed primitive would empty the set and pass forever.
	if total < 9 {
		t.Fatalf("found only %d view-creating declarations; the matcher has gone blind (renamed primitive?)", total)
	}
}
