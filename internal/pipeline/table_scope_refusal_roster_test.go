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

// EVERY CALLER THAT INSTALLS THE TABLE-SCOPE PUSH-DOWN AND THEN APPLIES THE
// TABLE FILTER OWES THE PUSH-DOWN REFUSAL (Bug 317 second review).
//
// migcore.ApplyTableFilter refuses "table filter excluded every source table"
// only when it EMPTIED a non-empty table set (a views-only namespace has no
// tables to exclude). But on an engine with the scope push-down (Postgres),
// a filtered-out table never reaches the filter at all, so a typo'd
// --include-table arrives as an empty table set and the filter's own
// refusal cannot see it. Every caller that installs the push-down and
// filters must therefore install it TALLIED (migcore.ApplyTableScopeTallied)
// and ask migcore.RefuseFilteredOutNamespace. The first cut of the fix wired
// four sites and missed `backup full`'s readScopedSchema and `schema diff`,
// so a typo'd --include-table on a Postgres backup wrote an empty backup at
// exit 0.
//
// The universe is derived, not listed: every non-test .go file under
// internal/ (minus internal/engines, which implements the push-down rather
// than consuming it) and cmd/. Fail-by-default both ways: an untallied
// scope+filter caller fails unless exempt with a reason, a tallied caller
// without the refusal fails, and a stale exemption fails. Floors on the
// packages walked and on the tallied sites keep the walk from going blind.
//
// Scope, stated so the name cannot be read as broader than the truth: this
// matches CALLS of the migcore helpers by selector within one declaration.
// A caller that installs the push-down in one function and filters in
// another (or reaches the helpers through a function value) is outside it.

// scopeFilterExempt: "<dir relative to repo root>::<decl>" → reason.
var scopeFilterExempt = map[string]string{
	"internal/pipeline::(*Migrator).applyDeferredConstraints": "the fan-out's deferred FK pass re-reads a " +
		"namespace whose filter verdict the per-namespace pass already gave; zero tables there means no FKs",
	"internal/pipeline::(*Streamer).preflightOneNamespaceReplicaIdentity": "an auxiliary per-namespace " +
		"preflight that returns on zero tables before filtering; the copy pass owns the filter verdict",
}

// goDecl is one function declaration found by walkRepoDecls.
type goDecl struct {
	dir  string // relative to the repo root, slash-separated
	name string // declIdentity
	fn   *ast.FuncDecl
}

// walkRepoDecls parses every non-test .go file under internal/ (minus
// internal/engines) and cmd/, and returns their declarations plus the
// number of directories that held at least one such file.
func walkRepoDecls(t *testing.T) (decls []goDecl, dirs int) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	seenDir := map[string]bool{}
	fset := token.NewFileSet()
	for _, top := range []string{"internal", "cmd"} {
		werr := filepath.Walk(filepath.Join(root, top), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if path == filepath.Join(root, "internal", "engines") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return perr
			}
			rel, _ := filepath.Rel(root, filepath.Dir(path))
			rel = filepath.ToSlash(rel)
			seenDir[rel] = true
			for _, d := range f.Decls {
				if fn, ok := d.(*ast.FuncDecl); ok && fn.Body != nil {
					decls = append(decls, goDecl{dir: rel, name: declIdentity(fn), fn: fn})
				}
			}
			return nil
		})
		if werr != nil {
			t.Fatalf("walk %s: %v", top, werr)
		}
	}
	return decls, len(seenDir)
}

// migcoreCalls returns the migcore.<name> selectors called in fn's body.
func migcoreCalls(fn *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "migcore" {
				out[sel.Sel.Name] = true
			}
		}
		return true
	})
	return out
}

func TestTableScopePushDownCallersOweTheRefusal(t *testing.T) {
	decls, dirs := walkRepoDecls(t)
	if dirs < 40 {
		t.Fatalf("walked only %d package directories under internal/ and cmd/; the walk is not seeing the tree", dirs)
	}
	tallied := 0
	usedExempt := map[string]bool{}
	var problems []string
	for _, d := range decls {
		calls := migcoreCalls(d.fn)
		key := d.dir + "::" + d.name
		filters := calls["ApplyTableFilter"] || calls["ApplyTableFilterQuiet"]
		if calls["ApplyTableScope"] && filters {
			if _, ok := scopeFilterExempt[key]; ok {
				usedExempt[key] = true
			} else {
				problems = append(problems, key+": installs the push-down with migcore.ApplyTableScope and then "+
					"filters — use migcore.ApplyTableScopeTallied and migcore.RefuseFilteredOutNamespace, or exempt it "+
					"here with a reason (a typo'd --include-table on Postgres otherwise exits 0 with an empty table set)")
			}
		}
		if calls["ApplyTableScopeTallied"] {
			tallied++
			if !calls["RefuseFilteredOutNamespace"] {
				problems = append(problems, key+": tallies the push-down but never asks migcore.RefuseFilteredOutNamespace")
			}
		}
	}
	for key := range scopeFilterExempt {
		if !usedExempt[key] {
			problems = append(problems, key+": stale exemption (no longer installs the push-down and filters)")
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
	if tallied < 6 {
		t.Fatalf("found only %d tallied push-down sites (want at least 6: migrate, sync cold start, sync dry run, "+
			"sync fan-out, backup, diff); the matcher has gone blind", tallied)
	}
}
