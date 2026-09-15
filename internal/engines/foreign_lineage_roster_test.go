// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The tree-wide half of the foreign-lineage roster (audit 2026-09-15 A0915-ARCH-MEDIUM-1).
//
// mysql's TestForeignLineageVerdictsCarryTheSentinel grades "non-test .go
// files in THIS package", and says so. That honesty is what made the gap
// findable: the pipeline's door (streamer_foreign_lineage.go) claims that
// "engines now carry ir.ErrPositionForeignLineage on every 'different
// lineage' verdict", and the one engine with the strongest lineage
// evidence in the tree — Postgres, whose checkSourceIdentity compares a
// persisted (systemid, timeline) pin against IDENTIFY_SYSTEM — was
// wrapping ir.ErrPositionInvalid, the sentinel that routes the automatic
// re-copy from whatever now answers the DSN. A package-scoped roster
// could not see it, so this one walks every engine package.
//
// Universe, stated so the name cannot be read as broader than the truth:
// every non-test .go file under internal/engines/**, and only RETURN
// statements whose own string literals carry one of the phrases in
// lineagePhrases. A verdict worded some other way is invisible here; the
// mysql roster's capability half (TestEveryCDCReaderAnswersTheLineageQuestion)
// grades the reactive door by interface rather than by wording.
//
// Exemption lives AT THE SITE, in the return statement or the comment
// block immediately above it, as `lineage-exempt: <why>` — the same
// marker and the same reason the mysql roster uses, so a site is graded
// identically by both. mysql's classifications are not changed here.

// lineagePhrases are the diagnoses that mean "the source answering the
// DSN is not the one the position came from". The first two are the
// mysql roster's; the last two are how Postgres words the same verdict.
var lineagePhrases = []string{
	"different lineage",
	"different server",
	"different instance",
	"source identity has changed",
}

// lineageExemptMarker mirrors mysql's constant of the same name; the two
// must agree for a site to be graded the same way by both rosters.
const lineageExemptMarker = "lineage-exempt:"

// lineageSiteWindow returns the text of the statement spanning
// [startLine, endLine] together with the contiguous comment block
// immediately above it — the place an exemption is allowed to live.
func lineageSiteWindow(lines []string, startLine, endLine int) string {
	first := startLine
	for first > 1 {
		above := strings.TrimSpace(lines[first-2])
		if !strings.HasPrefix(above, "//") {
			break
		}
		first--
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}
	return strings.Join(lines[first-1:endLine], "\n")
}

func mentionsLineagePhrase(s string) bool {
	for _, p := range lineagePhrases {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

// lineageVerdictSites walks every non-test .go file under root and
// returns, per package directory (relative to root), the counts of
// graded / sentinel-carrying / exempt "different lineage" return sites,
// plus every ungraded site as a failure line.
func lineageVerdictSites(t *testing.T, root string) (graded, carrying, exempted map[string]int, failures []string) {
	t.Helper()
	graded, carrying, exempted = map[string]int{}, map[string]int{}, map[string]int{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(src)
		if !mentionsLineagePhrase(text) {
			return nil
		}
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			return err
		}
		pkg, _ := filepath.Rel(root, filepath.Dir(path))
		pkg = filepath.ToSlash(pkg)
		lines := strings.Split(text, "\n")
		ast.Inspect(f, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok {
				return true
			}
			var mentions, carries bool
			ast.Inspect(ret, func(m ast.Node) bool {
				switch x := m.(type) {
				case *ast.BasicLit:
					if x.Kind == token.STRING && mentionsLineagePhrase(x.Value) {
						mentions = true
					}
				case *ast.SelectorExpr:
					if x.Sel.Name == "ErrPositionForeignLineage" {
						carries = true
					}
				}
				return true
			})
			if !mentions {
				return true
			}
			graded[pkg]++
			if carries {
				carrying[pkg]++
				return true
			}
			at := fset.Position(ret.Pos())
			window := lineageSiteWindow(lines, at.Line, fset.Position(ret.End()).Line)
			idx := strings.Index(window, lineageExemptMarker)
			if idx < 0 {
				failures = append(failures, at.String()+": returns a \"different lineage\" verdict without "+
					"ir.ErrPositionForeignLineage — it wraps the sentinel that routes the automatic re-copy, which "+
					"drops the target and refills it from the other database (A0909-MYSQL-HIGH-1; the Postgres "+
					"instance is audit 2026-09-15 A0915-ARCH-MEDIUM-1). Carry the sentinel, or write `"+lineageExemptMarker+
					" <why>` in a comment on this site.")
				return true
			}
			if reason := strings.TrimSpace(window[idx+len(lineageExemptMarker):]); len(reason) < 30 {
				failures = append(failures, at.String()+": carries "+lineageExemptMarker+" with no real reason")
				return true
			}
			exempted[pkg]++
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return graded, carrying, exempted, failures
}

// TestForeignLineageVerdictsCarryTheSentinelInEveryEngine grades every
// engine package's "different lineage" return sites, so a lane in ANY
// engine that words its verdict this way and still wraps
// ir.ErrPositionInvalid fails the build — the mysql roster's scope note
// stops at its own package boundary, and Postgres sat outside it.
//
// Mutation-proven in both directions (2026-09-15): stripping the sentinel
// from postgres.checkSourceIdentity fails on the postgres line; stripping
// it from a mysql site fails on that site AND trips mysql's own roster.
func TestForeignLineageVerdictsCarryTheSentinelInEveryEngine(t *testing.T) {
	t.Parallel()

	graded, carrying, exempted, failures := lineageVerdictSites(t, ".")
	for _, f := range failures {
		t.Error(f)
	}

	// Anti-vacuity, in three layers. The packages KNOWN to carry lineage
	// verdicts must each be graded (a rename of either engine's wording
	// would otherwise drop it out of the universe silently); every graded
	// package must have at least one site that CARRIES the sentinel (a
	// pass cannot be bought by exempting everything); and the total must
	// clear the floor the two engines' known sites set — mysql's six
	// (GTID continuity, file/pos identity, MariaDB anchor ×2 + domain,
	// VStream foreign) plus Postgres's one (checkSourceIdentity).
	for _, must := range []string{"mysql", "postgres"} {
		if graded[must] == 0 {
			t.Errorf("no \"different lineage\" return site was graded in %s — the phrase was reworded or the "+
				"scan broke; this roster expects at least one there", must)
		}
	}
	pkgs := make([]string, 0, len(graded))
	for pkg := range graded {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	total := 0
	for _, pkg := range pkgs {
		total += graded[pkg]
		if carrying[pkg] == 0 {
			t.Errorf("%s: %d graded site(s), %d exempt, and NONE carrying ir.ErrPositionForeignLineage — every "+
				"engine with a lineage verdict must carry the sentinel on at least one", pkg, graded[pkg], exempted[pkg])
		}
		t.Logf("%s: graded=%d carrying=%d exempt=%d", pkg, graded[pkg], carrying[pkg], exempted[pkg])
	}
	if total < 7 {
		t.Fatalf("graded only %d \"different lineage\" return sites across internal/engines; the roster expects at "+
			"least 7 — the phrase was reworded or the scan broke", total)
	}
}
