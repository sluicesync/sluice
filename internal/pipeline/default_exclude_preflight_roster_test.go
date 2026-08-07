// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// defaultExcludeCallSites rosters every place the orchestrator consults
// [ir.DefaultTableExcluder] (via `migcore.EffectiveTableFilter`), and
// records for each whether the driver/host preflight has already run.
//
// # Why this roster exists
//
// The mysql engine's `DefaultExcludePatterns` has a branch keyed on the
// DSN HOSTNAME rather than the flavor: a non-VStream flavor (`mysql`,
// `mariadb`) pointed at a PlanetScale endpoint still gets the `_vt_*`
// shadow-table exclusion. Its comment justified that branch by calling
// the pairing "a legitimate configuration — the operator gets binlog
// CDC instead of VStream". That was true when written, and v0.100.0's
// `ValidateDSN` falsified it: `migrate` and `sync` now REFUSE the
// pairing outright, so the only entry points that can still reach the
// branch are the ones that never ask.
//
// Which ones those are is a reachability fact, and a comment asserting
// it is a hypothesis until something fails when it breaks. This is that
// something. It does not prove reachability by itself — it forces the
// question to be answered in writing whenever a consult site appears or
// moves, and [TestDSNValidationPreflightCallSites] pins the other half
// (which entry points run the preflight at all), so wiring the
// preflight into `backup` — the obvious next move — fails both rosters
// until the verdicts are re-derived.
//
// Keys are `<path under internal/pipeline>:<enclosing function>`.
// Values MUST start with "PREFLIGHTED" or "NOT PREFLIGHTED".
var defaultExcludeCallSites = map[string]string{
	"migrate_phases.go:(*Migrator).phaseReadSourceSchema": "PREFLIGHTED — Migrator.Run calls " +
		"preflightDSNValidation before the first phase, so a non-VStream flavor against a PlanetScale " +
		"host never reaches here",
	"streamer_run_phases.go:(*Streamer).phaseResolveStreamIdentity": "PREFLIGHTED — Streamer.Run calls " +
		"preflightDSNValidation once before the first runOnce, for the same reason",
	"backup/backup.go:(*Backup).Run": "NOT PREFLIGHTED — `sluice backup` runs no driver/host validation, so " +
		"a vanilla-mysql DSN aimed at a PlanetScale host reaches the excluder and gets the `_vt_*` " +
		"exclusion it needs. Whether backup SHOULD refuse that pairing up front is an open question " +
		"(audit-backlog G-10), not something this roster decides",
	"diff.go:(*Differ).Run": "NOT PREFLIGHTED — `sluice schema diff` reads information_schema over the " +
		"plain MySQL protocol, which a PlanetScale endpoint serves fine; the exclusion is what keeps " +
		"`_vt_*` out of the diff",
	"preview.go:(*Previewer).Run": "NOT PREFLIGHTED — `sluice schema preview` is the same shape as diff: " +
		"schema read only, no CDC and no bulk copy",
}

// dsnValidationPreflightCallSites rosters every place
// `preflightDSNValidation` is invoked. It is the other half of
// [defaultExcludeCallSites]: that roster's verdicts are claims about
// which entry points validate, and this one is what makes adding or
// removing a validation site fail the build.
//
// Keys are `<path under internal/pipeline>:<enclosing function>`.
var dsnValidationPreflightCallSites = map[string]string{
	"migrate.go:(*Migrator).Run":   "the migrate entry point, before any reader or writer opens",
	"streamer.go:(*Streamer).Run":  "the sync entry point, before the retry loop",
	"dsn_validation_preflight.go:": "the declaration's own package-level home (see the walker's note)",
}

// TestDefaultExcludeCallSitesRecordTheirDSNPreflight fails when a
// consult of [ir.DefaultTableExcluder] appears, moves, or disappears
// without its preflight verdict being re-derived.
func TestDefaultExcludeCallSitesRecordTheirDSNPreflight(t *testing.T) {
	found := pipelineCallSites(t, "migcore", "EffectiveTableFilter")

	// Anti-vacuity floor. A walker that finds nothing agrees with any
	// roster; sluice has five consult sites across three commands and
	// two orchestrators today.
	if len(found) < 4 {
		t.Fatalf("the AST walk found only %d migcore.EffectiveTableFilter call site(s) (%s) — the walk broke "+
			"rather than the orchestrator shrinking to one consumer", len(found), strings.Join(found, ", "))
	}

	for _, site := range found {
		verdict, rostered := defaultExcludeCallSites[site]
		if !rostered {
			t.Errorf("%s consults ir.DefaultTableExcluder and is not in defaultExcludeCallSites.\n\n"+
				"Record whether preflightDSNValidation has already run on this path. It decides whether the "+
				"mysql engine's DSN-hostname branch is reachable here, and that branch's doc comment names "+
				"this roster as its evidence.", site)
			continue
		}
		if !strings.HasPrefix(verdict, "PREFLIGHTED") && !strings.HasPrefix(verdict, "NOT PREFLIGHTED") {
			t.Errorf("%s's roster entry must start with PREFLIGHTED or NOT PREFLIGHTED; got %q", site, verdict)
		}
	}
	assertNoStaleRosterEntries(t, "defaultExcludeCallSites", defaultExcludeCallSites, found)

	// Both verdict classes must be populated: if every site answered the
	// same way the roster would separate nothing, and the engine comment
	// it backs is entirely about the difference.
	var preflighted, unpreflighted int
	for _, v := range defaultExcludeCallSites {
		if strings.HasPrefix(v, "NOT PREFLIGHTED") {
			unpreflighted++
			continue
		}
		preflighted++
	}
	if preflighted == 0 || unpreflighted == 0 {
		t.Fatalf("every consult site carries the same verdict (%d preflighted, %d not), so this roster "+
			"separates nothing. If the preflight genuinely reaches every entry point now, say so in the "+
			"mysql engine's DefaultExcludePatterns comment and delete this gate — do not delete this check",
			preflighted, unpreflighted)
	}
}

// TestDSNValidationPreflightCallSites fails when the set of entry
// points running the driver/host preflight changes.
func TestDSNValidationPreflightCallSites(t *testing.T) {
	found := pipelineCallSites(t, "", "preflightDSNValidation")
	if len(found) == 0 {
		t.Fatal("the AST walk found no preflightDSNValidation call site — either the preflight was removed " +
			"(a refusal every release since v0.100.0 has shipped) or the walk broke")
	}
	for _, site := range found {
		if _, rostered := dsnValidationPreflightCallSites[site]; !rostered {
			t.Errorf("%s invokes preflightDSNValidation and is not rostered.\n\n"+
				"Adding an entry point to the preflight changes which commands can reach the mysql engine's "+
				"DSN-hostname exclusion branch — re-derive defaultExcludeCallSites' verdicts in the same "+
				"change, and update the prose those verdicts back.", site)
		}
	}
	assertNoStaleRosterEntries(t, "dsnValidationPreflightCallSites", dsnValidationPreflightCallSites, found)
}

// assertNoStaleRosterEntries fails on a rostered site the walk no
// longer sees. A stale entry is an unexamined hole: it reads as
// coverage and grades nothing.
func assertNoStaleRosterEntries(t *testing.T, roster string, entries map[string]string, found []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, s := range found {
		seen[s] = true
	}
	stale := make([]string, 0, len(entries))
	for site := range entries {
		if !seen[site] {
			stale = append(stale, site)
		}
	}
	sort.Strings(stale)
	for _, site := range stale {
		t.Errorf("%s rosters %q, which the AST walk no longer finds. Drop or re-key the entry — a roster "+
			"line for a call site that moved grades nothing while looking like it does", roster, site)
	}
}

// pipelineCallSites walks internal/pipeline (recursively, non-test
// files) and returns `<relative path>:<enclosing function>` for every
// call to `pkg.name` — or to bare `name` when pkg is empty.
//
// A call outside any function body is reported with an empty function
// part; the only one today is the preflight's own declaration file,
// whose doc comment names it. That shape is kept visible rather than
// filtered so a package-level wiring change cannot hide in it.
func pipelineCallSites(t *testing.T, pkg, name string) []string {
	t.Helper()

	var out []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		rel := filepath.ToSlash(path)
		for _, decl := range parsed.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			hit := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if callMatches(call.Fun, pkg, name) {
					hit = true
				}
				return true
			})
			if hit {
				out = append(out, rel+":"+funcSiteName(fn))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/pipeline: %v", err)
	}

	// The declaration's own file is rostered by path so the walker's
	// "outside a function body" case stays represented; a declaration is
	// not a call, so record it explicitly rather than inferring it.
	if pkg == "" {
		for _, decl := range declaringFiles(t, fset, name) {
			out = append(out, decl+":")
		}
	}
	sort.Strings(out)
	return out
}

// callMatches reports whether a call expression names `pkg.name`, or
// bare `name` when pkg is empty.
func callMatches(fun ast.Expr, pkg, name string) bool {
	if pkg == "" {
		id, ok := fun.(*ast.Ident)
		return ok && id.Name == name
	}
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	base, ok := sel.X.(*ast.Ident)
	return ok && base.Name == pkg
}

// declaringFiles returns the files under internal/pipeline that DECLARE
// a function of the given name.
func declaringFiles(t *testing.T, fset *token.FileSet, name string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
				out = append(out, filepath.ToSlash(path))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/pipeline for %s's declaration: %v", name, err)
	}
	return out
}

// funcSiteName renders a function or method as it appears in the
// rosters: `Name` for a function, `(*Recv).Name` for a method.
func funcSiteName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return fn.Name.Name
	}
	switch rt := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := rt.X.(*ast.Ident); ok {
			return "(*" + id.Name + ")." + fn.Name.Name
		}
	case *ast.Ident:
		return "(" + rt.Name + ")." + fn.Name.Name
	}
	return fn.Name.Name
}
