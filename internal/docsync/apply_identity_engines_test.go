// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Which sources the ADR-0190 exactly-once apply marks cover, held to the code.
//
// An applier skips a re-delivered change only when the change carries a
// stable identity ([ir.ApplyID]) and a mark proves it landed; a change with
// no identity is never skipped. So "which sources converge after a crash in
// the middle of a transaction" is decided by exactly one thing: which readers
// STAMP an identity. That is an operator-visible engine set, and CLAUDE.md's
// rule puts every such claim behind a marker:
//
//	<!-- apply-identity-engine-packages: mysql, postgres -->
//
// The marker names PACKAGES, for the reason the idempotent-copy marker does:
// the mysql package holds the binlog reader (which stamps) AND the VStream
// reader (which does not, until ADR-0190 phase 4), so a package→flavor
// mapping would claim PlanetScale and Vitess are covered. The prose beside
// the marker must say which readers; this gate checks the part it can.
//
// The second half is the STAMP roster, and it is what makes "VStream and the
// trigger-CDC sources are unchanged by ADR-0190" a checked statement rather
// than a promise: every `ApplyID:` field set in an ir.Insert / ir.Update /
// ir.Delete literal anywhere under internal/engines must sit in a file on the
// roster below. A reader that starts stamping identities (phase 4, phase 5)
// fails here until the roster, the capability pin and the doc move together.

package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// applyIDStampRoster is every file allowed to set an ir.ApplyID on a change
// it emits — the binlog reader and the pgoutput reader (ADR-0190 phase 1).
var applyIDStampRoster = map[string]bool{
	"mysql/cdc_reader.go":    true,
	"postgres/cdc_reader.go": true,
}

func TestApplyIdentityEngineListMatchesTheCode(t *testing.T) {
	fromCode := enginePackagesImplementing(t, "ApplyIdentityProvider")
	if len(fromCode) == 0 {
		t.Fatal("no engine package declares a `var _ ir.ApplyIdentityProvider` pin; either the capability was " +
			"renamed or the scan broke — an empty set would agree with any marker")
	}
	docPath := filepath.Join("..", "..", "docs", "operator", "cdc-streaming.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	m := regexp.MustCompile(`<!--\s*apply-identity-engine-packages:\s*([^>]*?)\s*-->`).FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/cdc-streaming.md carries no `<!-- apply-identity-engine-packages: … -->` marker; "+
			"add it listing: %s", strings.Join(fromCode, ", "))
	}
	if fromDoc := splitList(string(m[1])); !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's apply-identity engine-PACKAGE list disagrees with the code.\n"+
			"  code (packages pinning ir.ApplyIdentityProvider): %s\n  doc  (marker): %s",
			strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}

	// The stamp roster, derived from the AST.
	stamps := applyIDStampSites(t)
	sites := 0
	for file, n := range stamps {
		sites += n
		if !applyIDStampRoster[file] {
			t.Errorf("%s sets an ir.ApplyID on %d change literal(s) but is not on the stamp roster. A reader that "+
				"stamps an identity makes its source's re-deliveries skippable (ADR-0190): prove its identity is "+
				"stable across re-delivery (a real-server pin), declare ir.ApplyIdentityProvider, and add it here "+
				"and to the doc marker in the same change", file, n)
		}
		if pkg := strings.SplitN(file, "/", 2)[0]; !sortedContains(fromCode, pkg) {
			t.Errorf("%s stamps identities but its package %q does not declare ir.ApplyIdentityProvider", file, pkg)
		}
	}
	for file := range applyIDStampRoster {
		if stamps[file] == 0 {
			t.Errorf("%s is on the stamp roster but sets no ir.ApplyID — the roster is stale or the scan is broken", file)
		}
	}
	// Anti-vacuity: each rostered reader stamps Insert, Update and Delete.
	if sites < 6 {
		t.Fatalf("the scan found only %d ApplyID stamp sites (%v); two readers × three change kinds is six", sites, stamps)
	}
}

// applyIDStampSites counts, per file (relative to internal/engines), the
// ir.Insert / ir.Update / ir.Delete composite literals that set ApplyID.
func applyIDStampSites(t *testing.T) map[string]int {
	t.Helper()
	root := filepath.Join("..", "..", "internal", "engines")
	out := map[string]int{}
	fset := token.NewFileSet()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			sel, ok := lit.Type.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "ir" {
				return true
			}
			switch sel.Sel.Name {
			case "Insert", "Update", "Delete":
			default:
				return true
			}
			for _, el := range lit.Elts {
				if kv, ok := el.(*ast.KeyValueExpr); ok {
					if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "ApplyID" {
						out[rel]++
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func sortedContains(list []string, s string) bool {
	i := sort.SearchStrings(list, s)
	return i < len(list) && list[i] == s
}
