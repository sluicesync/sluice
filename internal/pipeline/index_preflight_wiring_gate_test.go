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

// EVERY ENTRY POINT THAT COPIES THEN INDEXES MUST ASK THE INDEX PREFLIGHT
// FIRST (roadmap item 118).
//
// # Why a gate rather than a promise
//
// Item 118 exists because three engines' index refusals all fired after the
// bulk copy and nobody noticed for two releases. The fix is one call —
// [migcore.PreflightIndexEmit] — and the way this project loses a one-call fix
// is by landing it on one entry point and missing the siblings: the shared
// copy phase runs under `migrate`, under BOTH `sync` cold-start paths, under
// `add-table`, and under both restore paths, and each of those is a separate
// function in a separate file.
//
// The parity agreement in CLAUDE.md ("an operator-facing flag/capability
// touching a shared pipeline phase applies to BOTH migrate and sync cold-start
// unless a written reason says otherwise") is enforced mechanically for
// copy-phase FLAGS by TestCopyPhaseFlagParityMigratorStreamer, which compares
// struct fields. This is not a flag — it is an unconditional gate with no
// field to compare — so it gets its own roster, in the same fail-by-default
// shape: an entry point is either in the list and calls the preflight, or it
// is exempt with a reason.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
// This proves the CALL EXISTS in each named function. It does not prove
// ordering within the function (that it precedes the copy) — the engine-level
// tests prove the answer is right, and the cross-engine integration test
// TestMigratePartialUniqueRefusedBeforeAnyRowsCopied proves the timing
// end-to-end on a real target.

// indexPreflightEntryPoints is the roster: package-relative directory →
// declaration identity → why this entry point copies-then-indexes.
var indexPreflightEntryPoints = map[string]map[string]string{
	".": {
		"(*Migrator).phaseTranslateAndGateSchema": "`migrate`: the pre-DDL gate phase, beside the " +
			"cross-engine supportability refusal",
		"(*Streamer).coldStart": "`sync` cold-start, single database: the copy phase it shares with " +
			"migrate builds the same indexes at the same point",
		"(*Streamer).coldStartCopyOneDatabase": "`sync` cold-start, multi-database fan-out: without it, " +
			"an unrepresentable index in database N is found after N-1 databases are copied",
		"(*AddTable).Run": "`add-table`: copies the new table, then emits its indexes — mid-sync, with " +
			"the publication already extended",
	},
	"backup": {
		"(*Restore).refuseUnrepresentableTargetShape": "`restore`: writes the whole archive to the " +
			"target, then builds indexes. The call lives in the schema-shape refusal phase Run carves " +
			"out, not in Run itself",
		"(*ChainRestore).Run": "`restore` of a chain: same phase order, after the entire chain replays",
	},
}

// declIdentity renders a FuncDecl the way the roster keys it, receiver
// included — two packages here declare a `Run`, and a name-only key would
// match whichever was parsed last.
func declIdentity(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	typ := fn.Recv.List[0].Type
	star := ""
	if s, ok := typ.(*ast.StarExpr); ok {
		typ, star = s.X, "*"
	}
	id, ok := typ.(*ast.Ident)
	if !ok {
		return fn.Name.Name
	}
	return "(" + star + id.Name + ")." + fn.Name.Name
}

// callersOfPreflight parses every non-test .go file in dir and returns the set
// of declarations that call migcore.PreflightIndexEmit.
func callersOfPreflight(t *testing.T, dir string) (callers map[string]bool, parsed int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	callers = map[string]bool{}
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
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "PreflightIndexEmit" {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "migcore" {
					callers[declIdentity(fn)] = true
				}
				return true
			})
		}
	}
	return callers, parsed
}

func TestIndexEmitPreflightReachesEveryCopyEntryPoint(t *testing.T) {
	for dir, wanted := range indexPreflightEntryPoints {
		callers, parsed := callersOfPreflight(t, dir)

		// Anti-vacuity floor. A walk that saw no files, or that stopped
		// matching the call because the helper was renamed, would pass this
		// gate on an empty set forever — which is the failure mode the
		// sibling gates in this package were rebuilt to avoid.
		if parsed < 5 {
			t.Fatalf("parsed only %d non-test files in %q — the walk is not seeing the package", parsed, dir)
		}
		if len(callers) == 0 {
			t.Fatalf("no call to migcore.PreflightIndexEmit found anywhere in %q. Either the helper was "+
				"renamed (re-point this gate) or every entry point lost the call — in which case roadmap "+
				"item 118 has silently regressed and every index refusal is back to firing after the copy.",
				dir)
		}

		for decl, why := range wanted {
			if strings.TrimSpace(why) == "" {
				t.Errorf("%s/%s is rostered with an EMPTY reason", dir, decl)
			}
			if !callers[decl] {
				got := make([]string, 0, len(callers))
				for c := range callers {
					got = append(got, c)
				}
				sort.Strings(got)
				t.Errorf("%s in %q does not call migcore.PreflightIndexEmit.\n\nThis entry point %s, so "+
					"every \"this index cannot be represented on the target\" refusal it can hit fires "+
					"AFTER the copy — roadmap item 118's whole defect, re-opened for this path.\n"+
					"callers found in %q: %v", decl, dir, why, dir, got)
			}
		}

		// The reverse direction: a NEW caller is fine, but it must be
		// rostered, because the roster is the only place the entry-point list
		// is written down.
		for decl := range callers {
			if _, ok := wanted[decl]; !ok {
				t.Errorf("%s in %q calls migcore.PreflightIndexEmit but is not in "+
					"indexPreflightEntryPoints. Add it with the reason it copies-then-indexes, so the "+
					"roster stays the answer to \"which entry points are covered?\"", decl, dir)
			}
		}
	}
}
