// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	// Every engine package the shipped binary blank-imports, so this
	// package's registry view matches the operator's. The set is held to
	// cmd/sluice/main.go by [TestEngineRegistryCoversTheShippedBinary].
	// Note that block is not load-bearing ON ITS OWN: several sibling
	// roster files carry the same imports, and removing a line here is
	// invisible while any of them (or any transitive import) still pulls
	// the package in. See that test's scope note.
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// Shared machinery for the engine-set doc markers in this package.
//
// # Two different questions, two different helpers
//
// A capability pin lives in a PACKAGE; an operator types an engine NAME
// (`--source-driver mariadb`). Those are not the same set, and conflating
// them is how the filtered-sync marker spent its whole life unable to fail:
// `internal/engines/mysql` registers FOUR engines — mysql, mariadb,
// planetscale, vitess (engine.go's init) — and `internal/engines/sqlite`
// registers TWO (sqlite, d1). A derivation that emits directory names can
// only ever emit `mysql, postgres`, so the marker it holds the doc to is
// correct by construction and binds the prose to nothing. MariaDB could run
// a filtered continuous sync from the day the mariadb flavor shipped and
// was absent from every engine list sluice published.
//
// So:
//
//   - [registeredEnginesImplementing] answers "which engine NAMES can do
//     this" — the operator-facing question, and the default. Use it for any
//     marker beside prose an operator reads.
//   - [enginePackagesImplementing] answers "which PACKAGES carry the pin".
//     Use it only when the capability genuinely belongs to one type inside a
//     package rather than to the package's whole flavor set, and say so at
//     the call site. [TestIdempotentCopyEngineListMatchesTheCode] is the one
//     such case today; its marker is named `…-engine-packages` so the name
//     cannot be read as broader than the truth.
//
// # What the name-level derivation does NOT check
//
// It maps a pin to every engine registered from the pin's package. That is
// exact when the pinned TYPE is reachable from every flavor of that package
// (mysql's `*SchemaWriter` and binlog `*CDCReader`, which flavor-branch
// internally on a field), and WRONG when the pin sits on a flavor-specific
// type (`*vstreamSnapshotRows`, reachable only from planetscale/vitess).
// Nothing here can tell those apart — the reader-selection dispatch is a
// runtime branch on a DSN. Before pointing a marker at this helper, read the
// pin and confirm the type is flavor-uniform; if it is not, use the package
// helper and carry the flavor sentence in prose.

// enginePackagesImplementing returns the engine PACKAGE directory names
// whose sources declare a compile-time `var _ ir.<capability>` pin.
//
// These are not engine names. See the package note above before using this
// for anything an operator reads; [registeredEnginesImplementing] is almost
// always the one you want.
func enginePackagesImplementing(t *testing.T, capability string) []string {
	t.Helper()
	root := filepath.Join("..", "..", "internal", "engines")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}

	var out []string
	fset := token.NewFileSet()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pkgDir := filepath.Join(root, e.Name())
		files, err := os.ReadDir(pkgDir)
		if err != nil {
			continue
		}
		found := false
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".go") || strings.HasSuffix(f.Name(), "_test.go") {
				continue
			}
			parsed, perr := parser.ParseFile(fset, filepath.Join(pkgDir, f.Name()), nil, 0)
			if perr != nil {
				continue
			}
			ast.Inspect(parsed, func(n ast.Node) bool {
				vs, ok := n.(*ast.ValueSpec)
				if !ok || len(vs.Names) != 1 || vs.Names[0].Name != "_" || vs.Type == nil {
					return true
				}
				sel, ok := vs.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != capability {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "ir" {
					found = true
				}
				return true
			})
			if found {
				break
			}
		}
		if found {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// registeredEnginesImplementing returns the REGISTERED ENGINE NAMES —
// the values an operator passes to `--source-driver` / `--target-driver` —
// served by a package that pins the named ir capability.
//
// The AST pass finds the packages; the live registry maps each package to
// every name registered from it, so a flavor cannot go missing from a claim
// just because it shares a directory with its siblings.
func registeredEnginesImplementing(t *testing.T, capability string) []string {
	t.Helper()
	byPkg := registeredEngineNamesByPackage(t)

	var out []string
	for _, pkg := range enginePackagesImplementing(t, capability) {
		names := byPkg[pkg]
		if len(names) == 0 {
			t.Fatalf("package internal/engines/%s pins ir.%s but registers no engine this package can see. "+
				"Either it is missing from the blank imports in engine_roster_test.go (the derived list would "+
				"silently under-report) or the pin lives in a package that registers nothing", pkg, capability)
		}
		out = append(out, names...)
	}
	sort.Strings(out)
	return out
}

// registeredEngineNamesByPackageType groups every registered engine name by
// `<package dir>.<concrete type name>` — one level finer than
// [registeredEngineNamesByPackage].
//
// Use it when the property being derived lives on a METHOD rather than on the
// package: `internal/engines/sqlite` declares two engine types with opposite
// answers — `Engine` implements the writer doors, `d1Engine` refuses them — so
// a package-keyed derivation cannot tell `sqlite` from `d1` at all. That is
// exactly the pair [TestTargetEngineListMatchesTheCode] has to separate.
//
// It stays flavor-correct for the other direction: mysql's four flavors and
// flatfile's three formats each share ONE concrete type (they differ by a
// struct field), so every name still reaches the key its methods were parsed
// from.
func registeredEngineNamesByPackageType(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() listed %q but engines.Get(%q) missed", name, name)
		}
		rt := reflect.TypeOf(e)
		for rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}
		key := path.Base(rt.PkgPath()) + "." + rt.Name()
		out[key] = append(out[key], name)
	}
	for k := range out {
		sort.Strings(out[k])
	}

	// Anti-vacuity floor, the same one [registeredEngineNamesByPackage]
	// carries and for the same reason: if every key ever held exactly one
	// name, the grouping would do nothing and a per-type marker derived from
	// it would be correct by construction.
	if len(engines.Names()) <= len(out) {
		t.Fatalf("every engine TYPE registers exactly one engine name (%d names across %d types), so mapping "+
			"type→names does nothing and the markers derived from it cannot fail. sluice has multi-flavor "+
			"types (mysql.Engine registers 4, flatfile.Engine 3); if that genuinely changed, delete this "+
			"helper — do not delete this check", len(engines.Names()), len(out))
	}
	return out
}

// registeredEngineNamesByPackage groups every registered engine name by the
// directory name of the package its value was constructed in.
//
// reflect.TypeOf on the registry's [ir.Engine] values yields the concrete
// engine struct, whose PkgPath's last element is the package directory the
// AST pass walks — that is the join between the two halves.
func registeredEngineNamesByPackage(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, name := range engines.Names() {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() listed %q but engines.Get(%q) missed", name, name)
		}
		rt := reflect.TypeOf(e)
		for rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}
		pkg := path.Base(rt.PkgPath())
		out[pkg] = append(out[pkg], name)
	}
	for pkg := range out {
		sort.Strings(out[pkg])
	}

	// Anti-vacuity floor. If the mapping ever collapses to one name per
	// package — a registry rewrite, or someone "simplifying" this back to
	// directory names — every name-level marker in this package would
	// quietly become correct-by-construction again, which is the exact
	// defect this helper was written to remove.
	if len(engines.Names()) <= len(out) {
		t.Fatalf("every engine package registers exactly one engine name (%d names across %d packages), so "+
			"mapping package→names does nothing and the markers derived from it cannot fail. sluice has "+
			"multi-flavor packages (mysql registers 4, sqlite 2); if that genuinely changed, delete this "+
			"helper — do not delete this check", len(engines.Names()), len(out))
	}
	return out
}

// TestEngineRegistryCoversTheShippedBinary holds the set of engine packages
// this TEST BINARY registers to the set cmd/sluice/main.go imports.
//
// Every name-level marker here derives its list from the registry, and the
// registry only contains what has been imported. An engine package added to
// the shipped binary but reachable from nothing in this package would make
// each derived list SHORT — and a short list is not a failure that announces
// itself: the marker would be updated to match and the new engine would
// silently vanish from the operator-facing claim, which is the same shape as
// the flavor-blindness this file exists to fix.
//
// # Scope — narrower than "holds this file's import block", deliberately
//
// It compares the EFFECTIVE registry of the whole test binary against
// main.go's engine imports. It therefore does NOT catch a drop from any one
// file's blank-import block, and two things mask that:
//
//   - Several sibling roster files here (bulk_copy_fk_enforcement,
//     cdc_guide_engines, cdc_mode_engines, index_namespace, index_preflight,
//     psdb_host_driver, table_namespace, view_namespace) carry the same import
//     list, so the union survives a deletion in any one of them.
//   - Engine packages import each other. `sqlite-trigger` and `d1-trigger`
//     both import `internal/engines/sqlite`, so sqlite (and with it the `d1`
//     engine) registers whether or not anything blank-imports it directly.
//
// Both were confirmed by mutation: deleting the sqlite import, and then the
// mydumper import, from this file alone left the gate GREEN; deleting mydumper
// from every file in the package turned it red. That is the honest boundary —
// it guards the property that matters (no engine in the binary is invisible to
// the registry here), not per-file import hygiene.
//
// It also does not check that main.go's imports are complete, nor that an
// imported package registers anything.
func TestEngineRegistryCoversTheShippedBinary(t *testing.T) {
	const enginePrefix = "sluicesync.dev/sluice/internal/engines/"

	mainPath := filepath.Join("..", "..", "cmd", "sluice", "main.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), mainPath, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", mainPath, err)
	}

	var wantPkgs []string
	for _, imp := range parsed.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		if strings.HasPrefix(p, enginePrefix) {
			wantPkgs = append(wantPkgs, path.Base(p))
		}
	}
	sort.Strings(wantPkgs)
	if len(wantPkgs) == 0 {
		t.Fatalf("parsed no %s imports from %s — the scan broke; an empty want-set would agree with any "+
			"registry", enginePrefix, mainPath)
	}

	byPkg := registeredEngineNamesByPackage(t)
	gotPkgs := make([]string, 0, len(byPkg))
	for pkg := range byPkg {
		gotPkgs = append(gotPkgs, pkg)
	}
	sort.Strings(gotPkgs)

	if !equalStringSets(wantPkgs, gotPkgs) {
		t.Errorf("this package's engine registry does not match the shipped binary's.\n"+
			"  cmd/sluice/main.go imports: %s\n"+
			"  registered here:            %s\n\n"+
			"Add the missing package to the blank imports in engine_roster_test.go. Every marker derived by "+
			"registeredEnginesImplementing reads this registry, so a package missing here silently drops its "+
			"engines from an operator-facing list instead of failing.",
			strings.Join(wantPkgs, ", "), strings.Join(gotPkgs, ", "))
	}
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
