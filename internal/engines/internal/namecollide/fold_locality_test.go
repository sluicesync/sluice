// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package namecollide

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// engineRoot is internal/engines, the directory this package's callers are
// confined to: [namecollide] sits under internal/engines/internal, so Go's
// internal-package rule makes internal/engines the COMPLETE set of packages
// that can import it. That is what lets the roster below claim completeness
// rather than "the call sites we knew about".
func engineRoot() string { return filepath.Join("..", "..") }

// foldArg is one `namecollide.New(...)` call site with the fold expression it
// passes, already reduced through one hop of local assignment.
type foldArg struct {
	pkgDir string // e.g. "../../sqlite"
	file   string
	line   int
	expr   ast.Expr // nil for a literal `nil` argument
}

// TestNameCollideFoldIsDeclaredByItsOwnEngine is the gate the package doc
// names: every fold handed to [New] must be the CALLING ENGINE's own rule, not
// a function borrowed from somewhere both engines can reach.
//
// # Why a gate rather than a comment
//
// Both SQLite walks passed [strings.ToLower] — Go's Unicode-aware fold, which
// is no engine's identifier rule — and shipped an over-refusal of `idx_é` /
// `idx_É` in v0.114.0 (roadmap item 150). The same borrowing in the other
// direction is worse: SQLite's ASCII-only fold handed to MySQL would UNDER-
// refuse a pair a `lower_case_table_names=1` server merges, which is the silent
// direction. Nothing about the type `func(string) string` distinguishes the
// three engines' rules, so the type system cannot catch this; the shape that
// CAN be checked mechanically is where the function is DECLARED.
//
// # WHAT THIS GATE REACHES, stated rather than implied
//
//   - Every non-test `.go` file under internal/engines, which by the internal-
//     package rule above is every possible caller of this package.
//   - It requires the fold expression to be `nil`, an identifier, or a call of
//     an identifier — and requires that identifier to name a func declared in
//     the caller's own package. A selector (`strings.ToLower`,
//     `otherengine.Fold`) and a function literal both FAIL.
//   - It resolves ONE hop of local assignment, which is what MySQL's
//     `fold := serverTableNameFold(lct)` needs. A fold laundered through two
//     locals, a struct field or a closure parameter would be reported as
//     unresolvable and fail — deliberately, because at that point the call site
//     no longer says whose rule it is.
//   - It does NOT check that the rule is CORRECT for that engine. Correctness
//     is ground-truthed against each real target
//     (`TestFoldSQLiteIdentifierMatchesRealSQLite`,
//     `TestMySQLTableNameFoldGroundTruth`); this gate only keeps the rules from
//     becoming one rule.
func TestNameCollideFoldIsDeclaredByItsOwnEngine(t *testing.T) {
	sites, funcsByPkg := scanNameCollideCallSites(t)

	// Anti-vacuity floor. A gate that found nothing to check would pass
	// forever, including after someone deleted every call site or moved the
	// package. These numbers are the four sites in three engine packages
	// present when the gate was written; raise them, never lower them.
	pkgs := map[string]bool{}
	for _, s := range sites {
		pkgs[s.pkgDir] = true
	}
	if len(sites) < 4 || len(pkgs) < 3 {
		t.Fatalf("found %d namecollide.New call site(s) in %d package(s); expected at least 4 in 3. "+
			"Either the walk broke (wrong root? %s) or the call sites moved — check before relaxing this.",
			len(sites), len(pkgs), engineRoot())
	}

	for _, s := range sites {
		where := s.file + ":" + strconv.Itoa(s.line)

		// A namecollide caller that is not an ENGINE package would be shared
		// machinery by construction, and its fold would be shared with it.
		if filepath.Dir(s.pkgDir) != filepath.Clean(engineRoot()) {
			t.Errorf("%s: namecollide.New called from %s, which is not an engine package. A fold that "+
				"lives in shared machinery is shared BETWEEN engines — that is exactly the mistake "+
				"roadmap item 150 shipped. Put the fold in the engine whose rule it is.", where, s.pkgDir)
			continue
		}

		if s.expr == nil {
			continue // literal nil: byte-exact, and it names no other engine
		}

		name, ok := foldIdentName(s.expr)
		if !ok {
			t.Errorf("%s: the fold passed to namecollide.New is %T, not a named function of this "+
				"package. The fold is ONE TARGET ENGINE's identifier-comparison rule; a literal or a "+
				"borrowed function does not say whose rule it is, and the two SQLite walks shipped an "+
				"over-refusal precisely by passing strings.ToLower here (roadmap item 150).",
				where, s.expr)
			continue
		}
		if !funcsByPkg[s.pkgDir][name] {
			t.Errorf("%s: the fold passed to namecollide.New resolves to %q, which is not a func "+
				"declared in %s. Every engine's fold must be its own: SQLite folds ASCII only, MySQL "+
				"folds what its server folds under lower_case_table_names != 0 (non-ASCII case "+
				"included), Postgres compares byte-exactly. Sharing one is an over-refusal on one "+
				"engine and a SILENT under-refusal on another.", where, name, s.pkgDir)
		}
	}
}

// TestNameCollideExportsNoFold closes the other half of the same door: the gate
// above requires the fold to be declared in the calling engine, and this one
// keeps THIS package from offering one for two engines to settle on. A
// `namecollide.ASCIIFold` would be reached by a selector and so would already
// fail above — but it would also be an invitation, and the cheapest place to
// decline it is here.
func TestNameCollideExportsNoFold(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read namecollide dir: %v", err)
	}
	parsed := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		parsed++
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !fn.Name.IsExported() {
				continue
			}
			if isStringToStringSignature(fn.Type) {
				t.Errorf("%s: %s is an exported string->string function in the SHARED collision "+
					"package. Two engines settling on it is roadmap item 150 with extra steps — "+
					"the seam is shared here, the FOLD belongs to one engine.", name, fn.Name.Name)
			}
		}
	}
	if parsed == 0 {
		t.Fatal("parsed no source files in this package; the check above asserted nothing")
	}
}

// isStringToStringSignature reports whether ft is `func(string) string`, the
// shape of an identifier fold.
func isStringToStringSignature(ft *ast.FuncType) bool {
	if ft.Params == nil || ft.Results == nil || len(ft.Params.List) != 1 || len(ft.Results.List) != 1 {
		return false
	}
	isString := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && id.Name == "string"
	}
	return isString(ft.Params.List[0].Type) && isString(ft.Results.List[0].Type)
}

// scanNameCollideCallSites parses every non-test source file under
// internal/engines and returns each `namecollide.New(...)` call site plus, per
// package directory, the set of package-level func names declared there.
func scanNameCollideCallSites(t *testing.T) (sites []foldArg, funcsByPkg map[string]map[string]bool) {
	t.Helper()
	funcsByPkg = map[string]map[string]bool{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(engineRoot(), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		pkgDir := filepath.Dir(path)
		if funcsByPkg[pkgDir] == nil {
			funcsByPkg[pkgDir] = map[string]bool{}
		}
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
				funcsByPkg[pkgDir][fn.Name.Name] = true
			}
		}
		sites = append(sites, newCallsIn(fset, f, pkgDir, path)...)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", engineRoot(), err)
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].file != sites[j].file {
			return sites[i].file < sites[j].file
		}
		return sites[i].line < sites[j].line
	})
	return sites, funcsByPkg
}

// newCallsIn returns every `namecollide.New[...](fold)` call in f, with the
// fold argument reduced through one hop of local assignment inside the function
// that contains it.
func newCallsIn(fset *token.FileSet, f *ast.File, pkgDir, path string) []foldArg {
	var out []foldArg
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isNameCollideNew(call.Fun) || len(call.Args) != 1 {
				return true
			}
			arg := call.Args[0]
			if id, isIdent := arg.(*ast.Ident); isIdent {
				if id.Name == "nil" {
					out = append(out, foldArg{pkgDir: pkgDir, file: path, line: fset.Position(call.Pos()).Line})
					return true
				}
				if rhs, found := localAssignment(fn.Body, id.Name); found {
					arg = rhs
				}
			}
			out = append(out, foldArg{
				pkgDir: pkgDir,
				file:   path,
				line:   fset.Position(call.Pos()).Line,
				expr:   arg,
			})
			return true
		})
	}
	return out
}

// isNameCollideNew reports whether fun is `namecollide.New`, with or without
// explicit type arguments (the generic instantiation parses as an IndexExpr).
func isNameCollideNew(fun ast.Expr) bool {
	switch e := fun.(type) {
	case *ast.IndexExpr:
		return isNameCollideNew(e.X)
	case *ast.IndexListExpr:
		return isNameCollideNew(e.X)
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		return ok && pkg.Name == "namecollide" && e.Sel.Name == "New"
	}
	return false
}

// localAssignment finds the single `name := rhs` (or `name = rhs`) inside body
// and returns rhs. Two assignments to the same name make the site
// unresolvable, which is reported as a failure rather than guessed at.
func localAssignment(body *ast.BlockStmt, name string) (ast.Expr, bool) {
	var found ast.Expr
	count := 0
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		if id, isIdent := as.Lhs[0].(*ast.Ident); isIdent && id.Name == name {
			found = as.Rhs[0]
			count++
		}
		return true
	})
	return found, count == 1
}

// foldIdentName reduces a fold expression to the identifier that names it: the
// identifier itself, or the identifier being CALLED by a fold factory such as
// SQLite's plain function or MySQL's `serverTableNameFold(lct)`. A selector or
// a function literal has no such name, which is the whole point.
func foldIdentName(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name, true
	case *ast.CallExpr:
		if id, ok := v.Fun.(*ast.Ident); ok {
			return id.Name, true
		}
	}
	return "", false
}
