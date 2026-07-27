// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The optional-dispatch pin guard (audit 2026-07-26 ARCH-1 / QUAL-1, gate
// G-15).
//
// # The class
//
// sluice discovers optional capabilities at runtime: `if x, ok :=
// applier.(ir.SomeSurface); ok { … }`. That idiom has one failure mode, and it
// is silent by construction — if the implementing method's receiver or
// signature drifts, the type assertion simply stops matching. `go build`, `go
// vet -tags=…` and every test stay GREEN while the feature quietly stops
// existing. Nothing fails, because "doesn't implement it" is a legitimate state
// for an OPTIONAL surface.
//
// The counter-measure the project already uses is a compile-time pin — `var _
// ir.SomeSurface = (*ChangeApplier)(nil)` in each engine's
// capabilities_assert.go — which turns that drift into a build error. It works;
// it is just applied by hand, and by hand means eventually not.
//
// Two surfaces had already fallen through when this was written:
// ir.TargetVacuumHealthReporter (implemented, dispatched, unpinned — a drift
// would have left every --notify-dead-tuple* threshold permanently inert with
// no signal) and ir.TemporalLiteralResolver (a drift would have reverted BOTH
// engines to ClientExact, re-opening the temporal-granularity defect the
// previous release had just fixed).
//
// # What this does
//
// Differences the set of ir interfaces that are runtime-asserted anywhere in
// the tree against the set that appear in a compile-time pin. Anything asserted
// but never pinned is reported.
package ir_test

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

// unpinnedByDesign are surfaces with NO implementer today — extension points
// declared ahead of a consumer. A pin would have nothing to point at, so their
// absence is deliberate rather than drift. Each needs a reason; if one gains an
// implementer, delete its entry and pin it.
var unpinnedByDesign = map[string]string{
	"CDCScopePreflighter":     "zero implementers: MariaDB's Engine.PreflightCDCScope was removed when its native-type refusal moved source-side (flavor_mariadb.go:412 records this), leaving the interface and its dispatch site (pipeline/add_table.go) as an extension point nothing satisfies today. A pin would have nothing to point at",
	"CDCUnsupportedExplainer": "zero implementers, same shape — the dispatch in pipeline/cdc_refusal.go is an extension point for an engine that wants to explain a CDC refusal; none does yet",
}

func TestEveryRuntimeDispatchedIRSurfaceHasACompileTimePin(t *testing.T) {
	root := repoRoot(t)

	// Only INTERFACES are in scope. A type switch over the concrete IR value
	// and change types (ir.Varchar, ir.Update, …) is ordinary exhaustive
	// dispatch, not capability discovery, and cannot silently stop matching:
	// those are structs, so a drift is a compile error already.
	interfaces := irInterfaceNames(t, root)
	if len(interfaces) < 20 {
		t.Fatalf("found only %d interface declarations in package ir; the declaration scan broke and this gate "+
			"would flag concrete types — re-point it", len(interfaces))
	}

	asserted := map[string][]string{} // interface -> files asserting it
	pinned := map[string]bool{}

	fset := token.NewFileSet()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Skip vendored / generated / worktree trees.
			base := info.Name()
			if base == ".git" || base == "vendor" || base == ".claude" || base == "workspace" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			// A file that does not parse standalone is not this gate's to
			// judge; skip it rather than failing the whole walk.
			return nil //nolint:nilerr // skipping an unparseable file is the intended behaviour, not a swallowed error
		}
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.TypeAssertExpr:
				// x.(ir.Surface)
				if name, ok := irSelector(node.Type); ok && interfaces[name] {
					asserted[name] = append(asserted[name], rel)
				}
			case *ast.CaseClause:
				// switch x := v.(type) { case ir.Surface: … }
				//
				// No interface case exists today — every type switch in the
				// tree dispatches on the concrete IR value and change types —
				// but the pin obligation is identical if one appears, and a
				// guard that only understands one dispatch form is a guard
				// with a hole waiting for someone to use the other.
				for _, typ := range node.List {
					if name, ok := irSelector(typ); ok && interfaces[name] {
						asserted[name] = append(asserted[name], rel)
					}
				}
			case *ast.ValueSpec:
				// Matches a compile-time pin declaration.
				if len(node.Names) != 1 || node.Names[0].Name != "_" || node.Type == nil {
					return true
				}
				if name, ok := irSelector(node.Type); ok {
					pinned[name] = true
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// Anti-vacuity: the walk must actually be finding both populations. The
	// audit that motivated this counted 153 assertion sites; a collapse to a
	// handful means the traversal broke and the gate is passing on nothing.
	if len(asserted) < 20 {
		t.Fatalf("found only %d runtime-asserted ir surfaces; the AST walk is not reaching the tree and this "+
			"gate is vacuous — re-point it", len(asserted))
	}
	if len(pinned) < 20 {
		t.Fatalf("found only %d compile-time pins; the walk is not seeing capabilities_assert.go and this gate "+
			"is vacuous — re-point it", len(pinned))
	}

	var offenders []string
	for name, files := range asserted {
		if pinned[name] {
			continue
		}
		if _, ok := unpinnedByDesign[name]; ok {
			continue
		}
		sort.Strings(files)
		seen := map[string]bool{}
		uniq := make([]string, 0, len(files))
		for _, f := range files {
			if !seen[f] {
				seen[f] = true
				uniq = append(uniq, f)
			}
		}
		if len(uniq) > 3 {
			uniq = append(uniq[:3], "…")
		}
		offenders = append(offenders, "ir."+name+"  (asserted in "+strings.Join(uniq, ", ")+")")
	}
	sort.Strings(offenders)

	if len(offenders) > 0 {
		t.Errorf("%d optional ir surface(s) are dispatched at RUNTIME but have no compile-time pin:\n  %s\n\n"+
			"A receiver or signature drift on the implementing method makes the type assertion stop matching, "+
			"and nothing fails — build, vet and tests all stay green while the feature silently stops existing. "+
			"Add `var _ ir.X = (*T)(nil)` to the implementing engine's capabilities_assert.go, or — if the "+
			"surface genuinely has no implementer yet — add it to unpinnedByDesign WITH a reason.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

// irInterfaceNames returns every interface type declared in package ir. The
// pin obligation applies to interfaces only — a concrete struct cannot silently
// stop satisfying an assertion.
func irInterfaceNames(t *testing.T, root string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	dir := filepath.Join(root, "internal", "ir")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if perr != nil {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if _, isIface := ts.Type.(*ast.InterfaceType); isIface {
				out[ts.Name.Name] = true
			}
			return true
		})
	}
	return out
}

// irSelector returns the interface name when e is `ir.Something`.
func irSelector(e ast.Expr) (string, bool) {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "ir" {
		return "", false
	}
	return sel.Sel.Name, true
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not locate the module root (no go.mod found walking up)")
	return ""
}
