// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// The SECURITY DEFINER emitter roster (audit 2026-08-31, SEC-1).
//
// # The class
//
// A `SECURITY DEFINER` function runs with its OWNER's privileges. Without a
// `SET search_path` clause it resolves unqualified names against the
// CALLING session's search_path — which belongs to whoever fired it. Since
// PostgreSQL picks the BEST-MATCHING candidate before it considers schema
// order, an attacker-created exact-typed overload of a built-in the body
// calls (e.g. `jsonb_build_object(text,text,text,text)` against the
// built-in's `VARIADIC "any"`, which scores zero exact matches) wins
// resolution over pg_catalog's and executes as the owner. pgtrigger's DDL
// capture function is owned by a SUPERUSER — `CREATE EVENT TRIGGER`
// requires one — and fires on ANY user's DDL, so from v0.85.0 to v0.134.0
// one `CREATE TABLE` by an unprivileged user was superuser code execution.
//
// # How the universe is derived (not hand-listed)
//
// Every package-level function in every non-test .go file under the repo
// whose body's string literals spell BOTH a `CREATE … FUNCTION` and
// `SECURITY DEFINER` is an emitter, and each must also emit
// `SET search_path =`. Adding a fourth emitter anywhere in the tree enters
// this universe automatically. The CREATE conjunct is what separates an
// emitter from prose that merely names the hazard (this door's own WARN
// text does), and it correctly excludes pgtrigger's canCreateEventTrigger,
// whose probe function is SECURITY INVOKER with an empty body and is rolled
// back — verified by reading it, not assumed.
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader than
// the truth: SQL that sluice's own Go source SPELLS. Honest residuals — SQL
// assembled from fragments too far apart for the literal scan (none today:
// all three emitters spell the whole CREATE in one function), SQL read from
// a config/manifest and executed verbatim (covered instead by the
// single-statement door, SLUICE-E-DDL-EMIT-MULTI-STATEMENT), and functions
// already installed on a database by an OLDER sluice — a source file cannot
// see those, which is exactly why pgtrigger carries the runtime
// warnInsecureCaptureFunctions door as well. The floor below fails if the
// walker stops seeing the emitters it grades today.
//
// Mutation-verified in both directions (2026-08-31): deleting the
// `SET search_path` line from renderCaptureDDLFunction fails this gate;
// dropping the walker's root to a directory with no emitters fails the
// anti-vacuity floor.
func TestNoUnpinnedSecurityDefinerEmitters(t *testing.T) {
	t.Parallel()
	const repoRoot = "../.."
	found := map[string]bool{}
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			// `.claude` holds the agent worktrees (whole copies of this
			// repo); walking them would grade stale duplicates.
			case ".git", ".claude", "vendor", "node_modules", "workspace", "tmp":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if perr != nil {
			// Every .go file in this repo compiles, so a parse failure means
			// the walker is mis-reading the tree — louder than skipping.
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			sql := literalsIn(fn.Body)
			if !strings.Contains(sql, "SECURITY DEFINER") || !strings.Contains(sql, "FUNCTION ") ||
				!strings.Contains(sql, "CREATE ") {
				continue
			}
			name := filepath.ToSlash(path) + ":" + fn.Name.Name
			found[name] = true
			if !strings.Contains(sql, "SET search_path =") {
				t.Errorf("%s emits SECURITY DEFINER SQL with NO `SET search_path =` clause — the function would "+
					"resolve unqualified names against the FIRING session's search_path, letting any user who can "+
					"create a function in a reachable schema shadow a built-in and execute as the function's OWNER "+
					"(SEC-1). Add `SET search_path = pg_catalog, pg_temp` and pg_catalog-qualify the body", name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", repoRoot, err)
	}
	// Anti-vacuity floor: the three pgtrigger capture-function renderers.
	// A walker that stops seeing them grades nothing while reporting green.
	if len(found) < 3 {
		t.Fatalf("derived only %d SECURITY DEFINER emitter(s) %v (floor 3: the pgtrigger row/truncate/DDL capture "+
			"renderers) — the walker stopped seeing the emitters this gate exists to grade", len(found), sortedDefinerEmitters(found))
	}
}

// literalsIn concatenates every string literal in n, which is how an
// emitter's hand-written SQL is spelled in this repo (one CREATE per
// function, assembled from adjacent raw-string chunks).
func literalsIn(n ast.Node) string {
	var b strings.Builder
	ast.Inspect(n, func(node ast.Node) bool {
		if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			b.WriteString(lit.Value)
			b.WriteString("\n")
		}
		return true
	})
	return b.String()
}

func sortedDefinerEmitters(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
