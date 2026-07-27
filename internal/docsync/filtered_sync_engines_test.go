// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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

// The filtered-continuous-sync engine list, kept honest against the code.
//
// # Why this exists
//
// Three consecutive releases shipped a wrong claim about WHICH ENGINES a
// filtered-sync behaviour applied to. The worst of them told SQLite and
// trigger-CDC operators their targets "have been diverging silently ever
// since" — for a mode those engines refuse at preflight and can never have
// entered. Sending someone to audit for a problem they cannot have had is its
// own kind of harm, and it is a harm no amount of care reliably prevents,
// because the mistake is always the same shape: reasoning about which engines
// LACK a capability instead of checking which ones HAVE it.
//
// There is a mechanical source of truth for that question. A filtered
// continuous sync needs the source's CDC reader to deliver full before-images,
// declared by a compile-time pin — `var _ ir.FullBeforeImageSetter = …` — in
// the implementing engine's capabilities_assert.go. This test derives the
// engine set from those pins and holds the operator doc to it.
//
// # The marker
//
// Prose should stay prose, so the doc carries a machine-checkable marker that
// this test owns:
//
//	<!-- filtered-sync-engines: mysql, postgres -->
//
// The visible sentence next to it is for humans; the marker is what fails.
// Anyone writing release notes about filtered sync now has one place to look
// whose answer is derived rather than remembered.
func TestFilteredSyncEngineListMatchesTheCode(t *testing.T) {
	const capability = "FullBeforeImageSetter"

	fromCode := enginesImplementing(t, capability)
	if len(fromCode) == 0 {
		t.Fatalf("no engine package declares a `var _ ir.%s` pin; either the capability was renamed or the "+
			"scan broke — this gate cannot be allowed to pass on an empty set", capability)
	}

	docPath := filepath.Join("..", "..", "docs", "operator", "filtered-subset-migration.md")
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	marker := regexp.MustCompile(`<!--\s*filtered-sync-engines:\s*([^>]*?)\s*-->`)
	m := marker.FindSubmatch(raw)
	if m == nil {
		t.Fatalf("docs/operator/filtered-subset-migration.md carries no `<!-- filtered-sync-engines: … -->` "+
			"marker. It is the checkable form of a claim this project has gotten wrong three releases running; "+
			"add it listing: %s", strings.Join(fromCode, ", "))
	}

	fromDoc := splitList(string(m[1]))
	if !equalStringSets(fromCode, fromDoc) {
		t.Errorf("the operator doc's filtered-sync engine list disagrees with the code.\n"+
			"  code (engines pinning ir.%s): %s\n"+
			"  doc  (marker):                %s\n\n"+
			"A filtered continuous sync requires the source CDC reader to deliver full before-images; an engine "+
			"without that pin refuses at preflight and CANNOT run one. Update the marker AND the prose beside "+
			"it — and if you are about to describe this in release notes, use this list rather than reasoning "+
			"about which engines lack the capability.",
			capability, strings.Join(fromCode, ", "), strings.Join(fromDoc, ", "))
	}
}

// enginesImplementing returns the engine package names whose sources declare a
// compile-time pin for the named ir capability.
func enginesImplementing(t *testing.T, capability string) []string {
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
