// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The G-2 extension the 2026-08-11 audit demanded (CDCPOS-2): the original
// gate in checkpoint_boundary_gate_test.go honestly scopes PER-CHANGE Apply
// implementations out ("per-change Apply implementations that never touch
// BatchConfig"), and that honest scoping is exactly where the defect lived —
// applyOne persisted a position with every row, so in file/pos mode a crash
// between rows of one source transaction resumed past the un-applied tail.
//
// This gate closes the scoped-out half: it walks internal/engines/** for
// every per-change Apply implementor (the ir.ChangeApplier surface —
// `Apply(ctx, streamID, <-chan ir.Change) error` on a receiver) and requires
// each one to name, in [perChangeMidTxPositionPins], the TEST that pins its
// mid-source-transaction position story. The named test must actually exist
// in that package (a reason naming a deleted test is rot, and rot fails).
// Stale entries fail too, and an anti-vacuity floor keeps the walker honest.
//
// The story every entry pins: rows between the delivered TxBegin/TxCommit
// must not persist a position (the deferred-to-TxCommit rule), because a
// mid-transaction position is only a legal restart point if it is one for
// EVERY source engine that can feed the target — false for a MySQL binlog
// source in file/pos mode, the one-source-justification trap item 132
// recorded.
var perChangeMidTxPositionPins = map[string]string{
	"mysql":    "TestApply_PerChangePositionDeferredToTxCommit",
	"postgres": "TestApply_PerChangePositionDeferredToTxCommit",
}

// Anti-vacuity floor: the walker must keep finding the real implementors.
const minPerChangeApplyImpls = 2 // mysql + postgres

func TestPerChangeApplyMidTxPositionPins(t *testing.T) {
	impls := walkPerChangeApplyImpls(t)

	if len(impls) < minPerChangeApplyImpls {
		t.Fatalf("found %d per-change Apply implementor(s) under %s (floor %d); the walker has gone vacuous — "+
			"re-point it", len(impls), enginesRootForCheckpointGate, minPerChangeApplyImpls)
	}

	seen := map[string]bool{}
	for _, impl := range impls {
		seen[impl.pkg] = true
		pin, ok := perChangeMidTxPositionPins[impl.pkg]
		if !ok || strings.TrimSpace(pin) == "" {
			t.Errorf("per-change Apply implementor %s (%s) has no entry in perChangeMidTxPositionPins — every "+
				"per-change applier must pin its mid-source-transaction position story (defer to TxCommit, or "+
				"argue the claim for EVERY source engine); add the pin test and name it here", impl.name, impl.where)
			continue
		}
		if !packageDeclaresTest(t, impl.pkg, pin) {
			t.Errorf("perChangeMidTxPositionPins names %q for engine %q but no test file in that package "+
				"declares it — the pin rotted (renamed or deleted); re-point the entry at the real test",
				pin, impl.pkg)
		}
	}
	for pkg := range perChangeMidTxPositionPins {
		if !seen[pkg] {
			t.Errorf("perChangeMidTxPositionPins carries an entry for %q but no per-change Apply implementor "+
				"exists there — the entry is STALE; remove it (an entry describing nothing stops the next "+
				"reader from checking)", pkg)
		}
	}
}

// walkPerChangeApplyImpls parses every non-test Go file under
// internal/engines and returns each method declaration matching the
// ir.ChangeApplier per-change surface: Apply(ctx, string, <-chan ir.Change).
// Parse failures are fatal (a gate that skips what it cannot read is the
// vacuous shape the floor exists to prevent).
func walkPerChangeApplyImpls(t *testing.T) []applierDecl {
	t.Helper()
	var impls []applierDecl
	fset := token.NewFileSet()
	err := filepath.WalkDir(enginesRootForCheckpointGate, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		pkg := enginePkgOf(path)
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "Apply" {
				return true
			}
			if !isPerChangeApplySignature(fn) {
				return true
			}
			impls = append(impls, applierDecl{
				pkg:   pkg,
				name:  pkg + "." + receiverTypeName(fn) + ".Apply",
				where: posOf(fset, fn.Pos()),
			})
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", enginesRootForCheckpointGate, err)
	}
	return impls
}

// isPerChangeApplySignature matches Apply(ctx context.Context, streamID
// string, changes <-chan ir.Change) — the discriminating parameter is the
// receive-only channel of ir.Change, which no other Apply-named method in the
// tree carries.
func isPerChangeApplySignature(fn *ast.FuncDecl) bool {
	params := fn.Type.Params
	if params == nil || len(params.List) != 3 {
		return false
	}
	ch, ok := params.List[2].Type.(*ast.ChanType)
	if !ok || ch.Dir != ast.RECV {
		return false
	}
	sel, ok := ch.Value.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Change" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "ir"
}

// packageDeclaresTest reports whether any _test.go file in the engine
// package declares `func <name>(`. A plain text scan is deliberate: the pin
// may live behind a build tag the parser configuration would skip.
func packageDeclaresTest(t *testing.T, pkg, testName string) bool {
	t.Helper()
	dir := filepath.Join(enginesRootForCheckpointGate, pkg)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	re := regexp.MustCompile(`(?m)^func ` + regexp.QuoteMeta(testName) + `\(`)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if re.Match(b) {
			return true
		}
	}
	return false
}
