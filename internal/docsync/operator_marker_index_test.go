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
// # How the universe is derived, and the half that was missing
//
// From the AST, in two passes over every non-test file under `internal/`
// and `cmd/`:
//
//  1. every `const` whose NAME ends in `Marker` and whose VALUE is an
//     ALL-CAPS-HYPHENATED string literal; and
//  2. every ALL-CAPS-HYPHENATED token IMMEDIATELY FOLLOWED BY A COLON
//     inside any string literal — the codebase's `component: MARKER:
//     message` log convention.
//
// Pass 2 is audit 2026-09-09 A0909-TCI-H-1, and the story is the one this file
// already tells about itself. The gate shipped with pass 1 only and its
// doc-comment said "a new marker is in scope the moment it is declared",
// which is true and was not the property that mattered: a marker written
// straight into the log call, never declared as a const, is in scope
// never. Two were —`CHANGE-LOG-PAGE-UNORDERED` (sqlite-trigger's poll
// refusal, itself the durable fix for a CRITICAL) and
// `CAPTURE-FUNCTION-PUBLIC-EXECUTE` — and both had zero
// `docs/operator/` hits while this test was green. Worse, the finding
// that a gate existed was recorded in the audit backlog as evidence the
// class was CLOSED. A gate whose universe is narrower than its name is
// what stops the next person from looking.
//
// Two shapes are excluded from pass 2, both because they are not log
// markers rather than because they are inconvenient:
//
//   - struct TAGS. A kong `help:"…"` is flag documentation, not a log
//     line, and one of them contains the phrase "handled IN-LANE:".
//   - `ADR-nnnn` references, which appear inside SQL comments embedded
//     in query literals.
//
// It reaches only markers a Go string literal spells out verbatim. A
// marker assembled at runtime from parts, or one that never precedes a
// colon, is outside it — say so if you write one, rather than assuming
// this catches everything.
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
	// Pass 2's shape: an ALL-CAPS-HYPHENATED token followed by a colon, at
	// a word boundary inside a message. ADR references match that shape and
	// do occur in this exact position inside SQL comments embedded in query
	// literals, so adrRef filters them explicitly.
	inlineMarker := regexp.MustCompile(`(?:^|[ (\[])([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+):(?: |$)`)
	adrRef := regexp.MustCompile(`^ADR-\d+$`)

	found := map[string]string{}       // marker -> declaring file
	inlineFound := map[string]string{} // marker -> emitting file (pass 2 only, for the floor)

	walk := func(sub string) error {
		return filepath.Walk(filepath.Join(root, sub), func(path string, info os.FileInfo, err error) error {
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

			// Struct tags are flag/serialization documentation, not log
			// lines. Collected first so pass 2 can skip them by identity.
			tags := map[*ast.BasicLit]bool{}
			ast.Inspect(f, func(n ast.Node) bool {
				if fld, ok := n.(*ast.Field); ok && fld.Tag != nil {
					tags[fld.Tag] = true
				}
				return true
			})

			ast.Inspect(f, func(n ast.Node) bool {
				// Pass 1: const …Marker = "ALL-CAPS-HYPHENATED".
				if vs, ok := n.(*ast.ValueSpec); ok {
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
				}
				// Pass 2: an inline `MARKER: ` inside any string literal.
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING || tags[lit] {
					return true
				}
				val, uerr := strconv.Unquote(lit.Value)
				if uerr != nil {
					return true
				}
				for _, m := range inlineMarker.FindAllStringSubmatch(val, -1) {
					if adrRef.MatchString(m[1]) {
						continue
					}
					if _, already := found[m[1]]; !already {
						found[m[1]] = rel
					}
					inlineFound[m[1]] = rel
				}
				return true
			})
			return nil
		})
	}
	for _, sub := range []string{"internal", "cmd"} {
		if err := walk(sub); err != nil {
			t.Fatalf("walk %s/: %v", sub, err)
		}
	}

	// Anti-vacuity floor. If the AST walk stops finding markers — a renamed
	// convention, a move to a generated table — this test would otherwise
	// pass by checking nothing, which is the failure it exists to prevent.
	if len(found) < 8 {
		t.Fatalf("the walk found only %d marker constants (%v). The convention is a `const …Marker = "+
			"\"ALL-CAPS-HYPHENATED\"`; if it changed, this gate is checking nothing and needs "+
			"re-pointing rather than deleting.", len(found), found)
	}
	// Pass 2 needs its OWN floor. Without one, a change that broke only the
	// inline scan would leave the combined count above 8 on the strength of
	// pass 1's constants alone, and the gate would go back to exactly the
	// blind spot A0909-TCI-H-1 found while still reporting green. Four inline
	// markers exist today: CAPTURE-FUNCTION-PUBLIC-EXECUTE,
	// CAPTURE-OUT-OF-SCOPE, CHANGE-LOG-PAGE-UNORDERED and
	// TABLE-FILTER-PATTERN-UNMATCHED (UNSELECTED-NAMESPACE-EXPOSURE is
	// spelled both ways and lands here too).
	if len(inlineFound) < 4 {
		t.Fatalf("the inline-literal pass found only %d marker(s) (%v); at least 4 are written straight "+
			"into log calls today. That pass is what audit 2026-09-09 A0909-TCI-H-1 added — if it is finding "+
			"nothing, re-point it rather than lowering this, because the const-only universe is the "+
			"blind spot the finding was about.", len(inlineFound), inlineFound)
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
