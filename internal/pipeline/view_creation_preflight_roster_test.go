// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"sort"
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
// So this roster is keyed on the CREATION side. Its universe is derived:
// every declaration in every non-test .go file under internal/ (minus
// internal/engines, whose writers implement CreateViews rather than
// orchestrate it) and cmd/ that REFERENCES a view-creating primitive —
// migcore.RunViewsPhase, runRecordedViewsPhase, createDeferredViews, or any
// `.CreateViews` selector. A reference, not only a call, so a function
// value or method expression (`f := sw.CreateViews`, `(*T).CreateViews`)
// counts too. Each must be listed, either with the declaration (same
// package) whose PreflightViewEmit call covers the views it creates, or as
// a wrapper whose own callers are listed. Fail-by-default both ways.
//
// Limits, stated so the name cannot be read as broader than the truth: the
// gate proves the BINDING is written down and that the named coverer still
// calls PreflightViewEmit; it does not prove data flow (that the views
// created are the ones preflighted) — the integration pins grade that. A
// primitive reached through a value created OUTSIDE the walked tree (an
// engine handing back a closure) is not seen.

const viewCreatorWrapper = "wrapper"

// viewCreationRoster: repo-relative package directory → creating
// declaration → the declaration (same directory) whose PreflightViewEmit
// covers it, or viewCreatorWrapper.
var viewCreationRoster = map[string]map[string]string{
	"internal/pipeline": {
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
		"(*Migrator).runMultiDatabase": "(*Migrator).phaseTranslateAndGateSchema",
		"(*Streamer).finishFanoutCopy": "(*Streamer).coldStartCopyOneDatabase",
	},
	"internal/pipeline/backup": {
		"(*Restore).Run": "(*Restore).refuseUnrepresentableTargetShape",
	},
	"internal/pipeline/migcore": {
		// the primitive itself; every caller above is rostered.
		"RunViewsPhase": viewCreatorWrapper,
	},
}

// referencesViewCreation reports whether fn's body references a
// view-creating primitive (call, function value or method expression).
func referencesViewCreation(fn *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Ident:
			if x.Name == "runRecordedViewsPhase" || x.Name == "createDeferredViews" {
				found = true
			}
		case *ast.SelectorExpr:
			if x.Sel.Name == "CreateViews" || x.Sel.Name == "RunViewsPhase" {
				found = true
			}
		}
		return !found
	})
	return found
}

func TestViewCreationRoster_EveryCreatorNamesItsPreflight(t *testing.T) {
	decls, dirs := walkRepoDecls(t)
	if dirs < 40 {
		t.Fatalf("walked only %d package directories; the walk is not seeing the tree", dirs)
	}
	creators := map[string]map[string]bool{}
	preflighters := map[string]map[string]bool{}
	total := 0
	for _, d := range decls {
		if referencesViewCreation(d.fn) {
			if creators[d.dir] == nil {
				creators[d.dir] = map[string]bool{}
			}
			creators[d.dir][d.name] = true
			total++
		}
		if migcoreCalls(d.fn)["PreflightViewEmit"] {
			if preflighters[d.dir] == nil {
				preflighters[d.dir] = map[string]bool{}
			}
			preflighters[d.dir][d.name] = true
		}
	}

	var unlisted []string
	for dir, names := range creators {
		for name := range names {
			if _, ok := viewCreationRoster[dir][name]; !ok {
				unlisted = append(unlisted, dir+"::"+name)
			}
		}
	}
	sort.Strings(unlisted)
	for _, key := range unlisted {
		t.Errorf("%s creates views but is not in viewCreationRoster. Name the declaration whose "+
			"migcore.PreflightViewEmit covers the views it creates (or mark it %q if only its callers create), "+
			"so a view refusal cannot be bypassed by a new creation path", key, viewCreatorWrapper)
	}
	for dir, roster := range viewCreationRoster {
		for decl, coverer := range roster {
			if !creators[dir][decl] {
				t.Errorf("%s::%s is rostered but no longer creates views — remove the stale entry", dir, decl)
				continue
			}
			if coverer != viewCreatorWrapper && !preflighters[dir][coverer] {
				t.Errorf("%s::%s names %s as its view preflight, but %s does not call migcore.PreflightViewEmit",
					dir, decl, coverer, coverer)
			}
		}
	}
	// Anti-vacuity: the walk must still be finding the known creators. A
	// renamed primitive would empty the set and pass forever.
	if total < 10 {
		t.Fatalf("found only %d view-creating declarations; the matcher has gone blind (renamed primitive?)", total)
	}
}
