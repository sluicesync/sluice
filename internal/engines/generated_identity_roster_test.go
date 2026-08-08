// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The fail-by-default roster over every CDC reader in an engine package,
// for the generated-row-identity class — the durable half of the
// 2026-08-07 MySQL fix and its 2026-08-08 Postgres sibling.
//
// # What the class is
//
// A source table whose ROW IDENTITY (its PRIMARY KEY, or whatever the
// engine's replica identity resolves to) includes a GENERATED column
// that the change stream does not carry. Every reader then narrows a
// before-image to an identity it does not have, and the applier renders
// a WHERE that is one of three wrong things — empty, `key IS NULL`, or a
// NON-UNIQUE PREFIX of the key. The first is a hard SQL error naming
// nothing; the other two are silent, and the third DELETES TARGET ROWS
// THE SOURCE NEVER TOUCHED.
//
// Two engines carry the shape today and each does so by its own
// mechanism, which is exactly why enumerating them mattered:
//
//   - MySQL's binlog carries the generated column, and sluice's decoder
//     drops it deliberately (the target's own GENERATED clause recomputes
//     it — correct for the INSERT/SET side, fatal for the WHERE side).
//   - PostgreSQL's WAL carries it and pgoutput does not publish it at
//     all before PG 18, so it never reaches sluice.
//
// Same consequence, disjoint causes. A reviewer looking at one would not
// have predicted the other, which is the property that makes this a
// roster and not a comment.
//
// # What this gate reaches, and what it does not
//
// It walks `internal/engines/**` for non-test declarations of
// `StreamChanges` — the [ir.CDCReader] core method, so the discriminator
// is the interface's own shape rather than a naming convention — and
// requires each declaring PACKAGE to be either
//
//	guarded — some non-test source in the package references
//	          sluicecode.CodeCDCGeneratedPrimaryKey, i.e. it refuses the
//	          shape by name; or
//	exempt  — listed below with a recorded reason.
//
// It is PACKAGE-keyed, and that is a real narrowing, stated rather than
// implied: `mysql` declares three readers (binlog, VStream, VStream
// snapshot) and one refusal in the binlog reader satisfies the package.
// The per-reader verdicts are in the exemption table's prose instead,
// and the VStream readers' safety is pinned where it actually lives —
// [TestNarrowBefore_RefusesAPartialKey] in internal/pipeline, which
// guards the one path that narrows a VStream before-image. Keying
// per-reader would need the roster to know which type each method hangs
// off and which of them is reachable, and the honest version of that is
// the pipeline pin.
//
// It also does not check that the refusal FIRES — that is what
// TestCDCReader_RefusesGeneratedIdentity (postgres) and
// TestCDCReader_MinimalDeleteCarriesThePK (mysql) do, against real
// servers. This gate checks that a NEW CDC reader cannot be added
// without someone deciding.
package engines

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// generatedIdentityExempt lists the CDC-reader packages that do NOT
// refuse the shape, each with the reason it cannot reach them. A package
// here that DOES carry the refusal fails the test too — a stale
// exemption is how a roster starts covering less than its name implies.
var generatedIdentityExempt = map[string]string{
	"pgtrigger": "cannot reach the shape: setup.go refuses ANY in-scope table carrying a stored " +
		"generated column (TableRefusal reason \"generated-stored-column\") before a capture trigger " +
		"is installed, so a generated identity column is out of scope by the time a change exists.",

	"sqlite-trigger": "cannot express the shape: SQLite rejects the DDL outright — \"generated columns " +
		"cannot be part of the PRIMARY KEY\", for STORED and VIRTUAL, sole and composite keys. Ground- " +
		"truthed rather than asserted by TestPremise_SQLiteForbidsAGeneratedPrimaryKey in that package, " +
		"which fails if a future SQLite relaxes it. Also covers d1-trigger, which is the same engine code " +
		"over a different transport.",
}

// generatedIdentityReaderFloor is the anti-vacuity floor. A walker that
// silently stops finding readers — a moved directory, a parse failure, a
// renamed method — would otherwise report a clean roster, which is the
// shape that makes a gate defend the defect. Set below today's count (6)
// so removing one reader does not fail the build, and far enough above
// zero that a broken walk cannot pass.
const generatedIdentityReaderFloor = 4

// generatedIdentityPackageFloor requires the walk to reach both engines
// that actually carry the class. The Postgres half went unfound for a
// release BECAUSE the MySQL fix did not look at it; a roster that
// covered one engine would have the same blind spot it exists to close.
var generatedIdentityPackageFloor = []string{"mysql", "postgres"}

// TestCDCReadersRefuseAGeneratedRowIdentity is the roster gate.
func TestCDCReadersRefuseAGeneratedRowIdentity(t *testing.T) {
	readers, err := findCDCReaderPackages(".")
	if err != nil {
		t.Fatalf("walk engine sources: %v", err)
	}
	total := 0
	for _, sites := range readers {
		total += len(sites)
	}
	if total < generatedIdentityReaderFloor {
		t.Fatalf(
			"discovered only %d CDC readers (floor %d) — the walker is broken, not the tree. Found: %v",
			total, generatedIdentityReaderFloor, sortedPkgs(readers),
		)
	}
	for _, pkg := range generatedIdentityPackageFloor {
		if _, ok := readers[pkg]; !ok {
			t.Fatalf("no CDC reader found in the %q engine; the walker is not reaching it", pkg)
		}
	}

	guarded, err := packagesNamingGeneratedPKCode(".")
	if err != nil {
		t.Fatalf("scan for the refusal code: %v", err)
	}

	for _, pkg := range sortedPkgs(readers) {
		reason, isExempt := generatedIdentityExempt[pkg]
		switch {
		case guarded[pkg] && isExempt:
			t.Errorf(
				"%s is listed EXEMPT but its sources reference sluicecode.CodeCDCGeneratedPrimaryKey — "+
					"one of the two is wrong. If it now refuses the shape, drop the exemption.", pkg,
			)
		case guarded[pkg]:
			// Refuses by name. The refusal's behaviour is pinned by the
			// engine's own integration tests, not here.
		case isExempt:
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s: exemption has no reason; the reason IS the gate", pkg)
			}
		default:
			t.Errorf(
				"%s declares a CDC reader (%v) and neither refuses a GENERATED row identity nor is exempt.\n"+
					"A generated column inside the row identity is one the change stream may not carry — the\n"+
					"before-image then narrows to a NON-UNIQUE PREFIX of the key (one source DELETE removes\n"+
					"every target row sharing it) or to nothing at all (the change is silently dropped while\n"+
					"the position advances). Either refuse with sluicecode.CodeCDCGeneratedPrimaryKey, or add\n"+
					"an entry to generatedIdentityExempt saying why this engine cannot reach the shape.",
				pkg, readers[pkg],
			)
		}
	}

	for pkg := range generatedIdentityExempt {
		if _, ok := readers[pkg]; !ok {
			t.Errorf(
				"generatedIdentityExempt lists %q but that package declares no CDC reader any more — "+
					"remove the entry.", pkg,
			)
		}
	}
}

// findCDCReaderPackages walks root for non-test Go sources and returns
// "<package dir>" → ["file:line", …] for every method named
// StreamChanges — the [ir.CDCReader] core method.
//
// Keyed by the DIRECTORY name rather than the package clause so the
// `-trigger` engines (whose package names differ from their dirs) key
// readably; every engine lives in its own directory.
func findCDCReaderPackages(root string) (map[string][]string, error) {
	out := map[string][]string{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		pkgDir := filepath.Base(filepath.Dir(path))
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "StreamChanges" {
				continue
			}
			pos := fset.Position(fn.Pos())
			out[pkgDir] = append(out[pkgDir], fmt.Sprintf("%s:%d", filepath.ToSlash(path), pos.Line))
		}
		return nil
	})
	return out, err
}

// packagesNamingGeneratedPKCode returns the set of engine directories
// whose non-test sources reference sluicecode.CodeCDCGeneratedPrimaryKey.
//
// Matching the SELECTOR rather than the string literal is deliberate: it
// is what a refusal actually writes, and it cannot be satisfied by the
// code appearing in a comment or a doc table.
func packagesNamingGeneratedPKCode(root string) (map[string]bool, error) {
	out := map[string]bool{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		pkgDir := filepath.Base(filepath.Dir(path))
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "CodeCDCGeneratedPrimaryKey" {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "sluicecode" {
				out[pkgDir] = true
			}
			return true
		})
		return nil
	})
	return out, err
}

func sortedPkgs(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
