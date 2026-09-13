// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCopyAxesRoster_NoLaneResolvesAxesWithoutTheCeiling is the mechanical form
// of a lesson this repo keeps paying for: a fix that lands on one lane and is
// assumed to have landed on its siblings.
//
// `migcore.ResolveCopyAxes` exists to fold EVERY product ceiling — the
// operator's connection cap and the target's concurrent-COPY limit — into the
// two bulk-copy axes. It was extracted because capping one axis let the other
// multiply it back (4 became 4x4, refused live by a Neki router with 53300).
// It was then wired into `migrate` and nowhere else, so the sync cold start and
// restore kept calling the lower-level `ResolveCopyParallelismBudget` and
// silently dropped `CopyConcurrencyCeiling` — the same defect, on two of the
// three lanes that have it, found by the pre-tag perf-parity review rather than
// by anything in the tree.
//
// So this walks the AST and FAILS BY DEFAULT: any call to the lower-level
// resolver outside the allow-list below is a lane that has to justify itself.
// A roster derived from the source is the point — a hand-kept list of lanes
// would have been just as wrong as the assumption it replaced.
func TestCopyAxesRoster_NoLaneResolvesAxesWithoutTheCeiling(t *testing.T) {
	t.Parallel()

	// The ONLY legitimate callers of the lower-level resolver.
	//
	// Each entry is a REASON, not a waiver. If a lane is added here, the
	// reason has to say why the target's concurrent-COPY ceiling cannot apply
	// to it — not merely that it currently does not.
	allowed := map[string]string{
		// The fold itself. ResolveCopyAxes computes the product ceiling and
		// then delegates; this is the one call that is supposed to exist.
		"migcore/connection_budget.go": "ResolveCopyAxes IS the fold — it computes the product ceiling, then delegates here",

		// Backup reads its SOURCE with SELECT, not COPY. A concurrent-COPY
		// ceiling counts COPY statements against a target, and backup issues
		// none: the archive is the destination and it has no admission limit.
		// Inapplicable with a reason, rather than unreached by accident.
		"backup/backup_table_pool.go": "backup's budget governs SOURCE reads issued as SELECT; it opens no COPY against a target",
	}

	root := filepath.Join("..", "..", "internal")
	var offenders []string
	seen := map[string]bool{}

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			// A file this walk cannot parse is a file it cannot grade, and
			// failing the whole roster on it would turn an unrelated syntax
			// error into a confusing failure here. The anti-vacuity floor
			// below is what stops a silently-empty walk from passing.
			t.Logf("skipping unparseable %s: %v", path, perr)
			return nil //nolint:nilerr // deliberate: see above
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			// Both call shapes: `migcore.ResolveCopyParallelismBudget(...)`
			// from another package, and the bare `ResolveCopyParallelismBudget(...)`
			// from inside migcore itself. Matching only the qualified form is
			// how the first cut of this gate missed the in-package call — and
			// the anti-vacuity floor below is what caught that, which is the
			// argument for having one.
			var name string
			switch fn := call.Fun.(type) {
			case *ast.SelectorExpr:
				name = fn.Sel.Name
			case *ast.Ident:
				name = fn.Name
			}
			if name != "ResolveCopyParallelismBudget" {
				return true
			}
			rel := filepath.ToSlash(path)
			key := ""
			for a := range allowed {
				if strings.HasSuffix(rel, a) {
					key = a
					break
				}
			}
			if key != "" {
				seen[key] = true
				return true
			}
			offenders = append(offenders, rel+":"+fset.Position(call.Pos()).String())
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// Anti-vacuity: the walk must actually be finding calls. A rename of the
	// function, or a walk rooted at the wrong directory, would otherwise make
	// this test pass by seeing nothing at all.
	if len(seen) != len(allowed) {
		t.Fatalf("the roster found %d of %d allow-listed call sites (%v) — the AST walk is not reaching "+
			"the code it grades, so a genuine offender would also go unseen", len(seen), len(allowed), seen)
	}

	if len(offenders) > 0 {
		t.Fatalf("these call ResolveCopyParallelismBudget directly and therefore IGNORE the target's "+
			"CopyConcurrencyCeiling:\n  %s\n\n"+
			"Use migcore.ResolveCopyAxes, which folds in every product ceiling the run is subject to. "+
			"If the lane genuinely cannot be subject to a concurrent-COPY limit, add it to this test's "+
			"allow-list WITH the reason.", strings.Join(offenders, "\n  "))
	}
}
