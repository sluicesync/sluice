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
	"strconv"
	"strings"
	"testing"
)

// Every operator-facing log MARKER must have an operator-doc home.
//
// # Why this exists (audit DDD-6)
//
// sluice emits grep-stable ALL-CAPS-HYPHENATED markers so an operator
// scrolling a log mid-incident has something to search for — `POSITION-MODE`,
// `UNVERIFIED-INSTANCE-IDENTITY`, `STALE-CAPTURE-FUNCTION`. The marker is only
// half the contract. If searching the docs for it returns nothing, the
// operator has a token and no explanation, which is barely better than prose.
//
// DDD-6 filed that several markers had no doc home and that the set had no
// index. Measured at the time this gate was written: 14 marker constants, 3
// with no operator-doc mention. **One of those three had been added by the
// author of this gate, an hour earlier, in the same session** — which is the
// argument for a gate rather than a resolution to remember. A convention
// nothing checks decays at exactly the rate people are busy.
//
// # How the universe is derived
//
// From the AST: every `const` whose NAME ends in `Marker` and whose VALUE is
// an ALL-CAPS-HYPHENATED string literal. That is the codebase's own
// convention, so the gate cannot drift out of step with a hand-kept list — a
// new marker is in scope the moment it is declared, without anyone
// remembering to register it.
//
// The check is presence in `docs/operator/`, deliberately not a specific file:
// where a marker is explained is an editorial decision, and pinning it would
// make the gate fight legitimate reorganisation. What it refuses is a marker
// explained NOWHERE an operator would look.
func TestOperatorMarkersHaveADocHome(t *testing.T) {
	root := repoRootFromDocsync(t)

	// Exemptions, each with a reason that is about the marker's NATURE, not
	// about it being inconvenient to document.
	exempt := map[string]string{
		"RETAINED-BUT-UNEMITTED": "not an operator log marker: an internal annotation prefixing " +
			"class-table descriptions for codes that are registered but not currently emitted, so " +
			"there is nothing for an operator to search a log for",
	}

	markerLiteral := regexp.MustCompile(`^[A-Z][A-Z0-9]*(-[A-Z0-9]+)+$`)
	found := map[string]string{} // marker -> declaring file

	err := filepath.Walk(filepath.Join(root, "internal"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			// Deliberately NOT skipped. Build tags do not prevent parsing, so a
			// parse error means genuinely malformed Go — and a marker declared
			// inside a file this walk silently skipped would be invisible to the
			// gate, which is the exact failure it exists to prevent.
			return perr
		}
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, name := range vs.Names {
				if !strings.HasSuffix(name.Name, "Marker") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				val, uerr := strconv.Unquote(lit.Value)
				if uerr != nil || !markerLiteral.MatchString(val) {
					continue
				}
				found[val] = rel
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/: %v", err)
	}

	// Anti-vacuity floor. If the AST walk stops finding markers — a renamed
	// convention, a move to a generated table — this test would otherwise
	// pass by checking nothing, which is the failure it exists to prevent.
	if len(found) < 8 {
		t.Fatalf("the walk found only %d marker constants (%v). The convention is a `const …Marker = "+
			"\"ALL-CAPS-HYPHENATED\"`; if it changed, this gate is checking nothing and needs "+
			"re-pointing rather than deleting.", len(found), found)
	}

	docs, derr := operatorDocCorpus(t, root)
	if derr != nil {
		t.Fatalf("read docs/operator: %v", derr)
	}

	for marker, file := range found {
		if reason, ok := exempt[marker]; ok {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is exempt with an empty reason", marker)
			}
			continue
		}
		if !strings.Contains(docs, marker) {
			t.Errorf("marker %q (declared in %s) appears nowhere in docs/operator/.\n"+
				"  An operator who greps their log, finds this token, and then searches the docs for it "+
				"gets nothing — the marker is a handle with no page behind it.\n"+
				"  Document it where the related markers live, or add it to this test's exempt map with "+
				"a reason about why it is not operator-facing.", marker, file)
		}
	}
}

// operatorDocCorpus concatenates docs/operator/ so a marker's home can be any
// file in it — where a marker is explained is editorial, and pinning the file
// would make this gate fight legitimate reorganisation.
func operatorDocCorpus(t *testing.T, root string) (string, error) {
	t.Helper()
	var sb strings.Builder
	err := filepath.Walk(filepath.Join(root, "docs", "operator"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		sb.Write(b)
		sb.WriteString("\n")
		return nil
	})
	return sb.String(), err
}
