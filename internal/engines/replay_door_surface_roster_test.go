// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
)

// replayDoorSurfaces are the two optional [ir.RowWriter] surfaces the F-E1
// replay-duplication doors require. migcore.FindReplayKeylessTables
// REFUSES a writer lacking either (fail closed), so an engine whose row
// writer is missing one cannot be a restore or broker target at all — and
// before the doors failed closed, it was one with the door silently open
// (SQLite: a keyless table restored twice held 200 rows for 100).
var replayDoorSurfaces = []string{"IsTableEmpty", "ProbeReplayKey"}

// registeredAtInit snapshots the registry once every engine package's init
// has run and before any test does: the package's own registry tests reset
// the registry, so reading it from inside a test would see whatever state
// the previous test left behind.
var registeredAtInit = func() map[string]ir.Engine {
	out := map[string]ir.Engine{}
	for _, name := range engines.Names() {
		out[name], _ = engines.Get(name)
	}
	return out
}()

// replayDoorDelegates names the registered engines whose OpenRowWriter
// DELEGATES to another registered engine's, so the writer they return is
// that engine's writer and is graded there. Fail-by-default: a delegating
// OpenRowWriter not listed here fails the roster, and a listed engine that
// stops delegating fails it too.
var replayDoorDelegates = map[string]string{
	"postgres-trigger": "postgres",
}

// TestReplayDoorSurfaceRoster_EveryTargetWriter derives every engine that
// can be a `restore` or `sync from-backup` target FROM THE REGISTRY, and
// requires the row-writer type its OpenRowWriter returns to implement both
// replay-door surfaces.
//
// # How it derives the universe
//
// For every registered engine, reflect gives the engine's package and type
// name. The package's non-test sources are parsed and the engine type's
// OpenRowWriter is classified by its return statements:
//
//   - every return's writer result is `nil` — not a row-writer target
//     (a source-only engine, or a flavor that refuses to be a target);
//   - some return is `&T{...}` — T, declared in that package, is the writer
//     every restore and broker door on this engine probes; it must declare
//     both methods;
//   - some return is a call to another engine's OpenRowWriter — a
//     delegation, which must be listed in replayDoorDelegates.
//
// Broker targets are a subset: the broker opens OpenChangeApplier AND the
// door's OpenRowWriter on the same engine, so an engine with a change
// applier but no graded writer is reported too.
//
// # What it does not reach, stated
//
// It checks that the methods are DECLARED on the returned type, not that
// they answer correctly — that is each engine's
// TestRowWriter_ProbeReplayKey_ShapeMatrix, graded against the engine's own
// measured re-write. An OpenRowWriter that returns a writer through a local
// variable or a helper call (rather than a composite literal or a
// delegation) is reported as unclassifiable rather than silently passed.
// An engine package that registers but is not blank-imported above is
// caught by the walk over internal/engines that requires every package
// calling engines.Register to be represented in the registry.
func TestReplayDoorSurfaceRoster_EveryTargetWriter(t *testing.T) {
	names := make([]string, 0, len(registeredAtInit))
	for name := range registeredAtInit {
		names = append(names, name)
	}
	sort.Strings(names)
	registeredDirs := map[string]bool{}
	graded := map[string]bool{} // "<dir>.<Type>"
	targets, nonTargets := 0, 0
	for _, name := range names {
		eng := registeredAtInit[name]
		typ := reflect.TypeOf(eng)
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		dir := strings.TrimPrefix(typ.PkgPath(), "sluicesync.dev/sluice/internal/engines/")
		registeredDirs[dir] = true

		pkg := parseEnginePackage(t, dir)
		rwKind, rwType := classifyOpener(pkg, typ.Name(), "OpenRowWriter")
		applierKind, _ := classifyOpener(pkg, typ.Name(), "OpenChangeApplier")
		t.Logf("%s (%s.%s): OpenRowWriter %s %s; OpenChangeApplier %s", name, dir, typ.Name(), rwKind, rwType, applierKind)
		switch rwKind {
		case openerNil:
			nonTargets++
			if applierKind != openerNil {
				t.Errorf("%s (%s.%s): OpenChangeApplier can return an applier but OpenRowWriter never returns a "+
					"writer, so the broker's F-E1 target probe has nothing to ask", name, dir, typ.Name())
			}
		case openerLiteral:
			targets++
			graded[dir+"."+rwType] = true
			for _, m := range replayDoorSurfaces {
				if !pkg.methods[rwType][m] {
					t.Errorf("%s: its row writer %s.%s does not declare %s, so every restore onto it is refused "+
						"by the F-E1 door (and, before the door failed closed, was silently unguarded)", name, dir, rwType, m)
				}
			}
			if _, listed := replayDoorDelegates[name]; listed {
				t.Errorf("%s is listed in replayDoorDelegates but its OpenRowWriter builds its own writer; drop the entry", name)
			}
		case openerDelegates:
			targets++
			to, listed := replayDoorDelegates[name]
			if !listed {
				t.Errorf("%s (%s.%s): OpenRowWriter delegates to another engine's; list it in replayDoorDelegates "+
					"with the engine it delegates to so its writer is graded there", name, dir, typ.Name())
				continue
			}
			if _, ok := registeredAtInit[to]; !ok {
				t.Errorf("%s delegates to %q, which is not registered", name, to)
			}
		default:
			t.Errorf("%s (%s.%s): cannot classify OpenRowWriter's returns (%s); extend the roster rather than "+
				"assume the writer is covered", name, dir, typ.Name(), rwKind)
		}
	}

	// Every package that registers an engine must be in the registry this
	// test sees — otherwise a new engine is invisible to it.
	for _, dir := range registeringDirs(t) {
		if !registeredDirs[dir] {
			t.Errorf("internal/engines/%s calls engines.Register but no registered engine comes from it here; "+
				"blank-import it in this test", dir)
		}
	}

	// Anti-vacuity floor: postgres, mysql and sqlite build their own
	// writers today, postgres-trigger delegates, and at least the six
	// source-only engines (flatfile ×3, mydumper, sqlite-trigger,
	// d1-trigger) plus d1 return none.
	if len(graded) < 3 || targets < 7 || nonTargets < 7 {
		t.Errorf("anti-vacuity: graded %d writer type(s) %v over %d target and %d non-target engine(s) of %d "+
			"registered; the walker is not reaching the engines it was built for", len(graded), graded,
			targets, nonTargets, len(names))
	}
}

type openerKind string

const (
	openerNil       openerKind = "always nil"
	openerLiteral   openerKind = "returns a composite literal"
	openerDelegates openerKind = "delegates"
	openerUnknown   openerKind = "unknown"
	openerMissing   openerKind = "no such method"
)

// enginePackage is the parsed non-test source of one engine package.
type enginePackage struct {
	funcs   map[string]*ast.FuncDecl   // "<RecvType>.<Method>"
	methods map[string]map[string]bool // receiver type -> method set
}

func parseEnginePackage(t *testing.T, dir string) enginePackage {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read engine package %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	pkg := enginePackage{funcs: map[string]*ast.FuncDecl{}, methods: map[string]map[string]bool{}}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s/%s: %v", dir, name, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) != 1 {
				continue
			}
			recv := recvTypeName(fd.Recv.List[0].Type)
			if pkg.methods[recv] == nil {
				pkg.methods[recv] = map[string]bool{}
			}
			pkg.methods[recv][fd.Name.Name] = true
			pkg.funcs[recv+"."+fd.Name.Name] = fd
		}
	}
	return pkg
}

func recvTypeName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// classifyOpener classifies an Open* method on the engine type by the first
// result of its return statements.
func classifyOpener(pkg enginePackage, engineType, method string) (kind openerKind, writer string) {
	fd, ok := pkg.funcs[engineType+"."+method]
	if !ok || fd.Body == nil {
		return openerMissing, ""
	}
	kind = openerNil
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, isLit := n.(*ast.FuncLit); isLit {
			return false
		}
		ret, ok := n.(*ast.ReturnStmt)
		if !ok || len(ret.Results) == 0 {
			return true
		}
		switch r := ret.Results[0].(type) {
		case *ast.Ident:
			if r.Name != "nil" {
				kind = openerUnknown
			}
		case *ast.UnaryExpr:
			lit, isLit := r.X.(*ast.CompositeLit)
			if r.Op != token.AND || !isLit {
				kind = openerUnknown
				return true
			}
			if id, isID := lit.Type.(*ast.Ident); isID && kind != openerUnknown {
				kind, writer = openerLiteral, id.Name
			}
		case *ast.CallExpr:
			if sel, isSel := r.Fun.(*ast.SelectorExpr); isSel && sel.Sel.Name == method && kind != openerUnknown {
				kind = openerDelegates
			} else {
				kind = openerUnknown
			}
		default:
			kind = openerUnknown
		}
		return true
	})
	return kind, writer
}

// registeringDirs lists the internal/engines sub-packages whose non-test
// sources call engines.Register.
func registeringDirs(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "testdata" || e.Name() == "internal" {
			continue
		}
		files, err := os.ReadDir(e.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), "_test.go") || !strings.HasSuffix(f.Name(), ".go") {
				continue
			}
			src, err := os.ReadFile(filepath.Join(e.Name(), f.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(src), "engines.Register(") {
				out = append(out, e.Name())
				break
			}
		}
	}
	sort.Strings(out)
	return out
}
