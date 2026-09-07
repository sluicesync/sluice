// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestBrokerColdStartResetPreflightsBeforeTheDrop is the audit
// 2026-09-06 RCN-1 wiring gate.
//
// THE DEFECT. `SyncFromBackup.coldStartReset` DROPs every table named in
// the cached tail manifest and then calls ChainRestore.Run, which owns
// seven refusals its own comments describe as pre-target. Audit
// 2026-08-11 BRK-1 hoisted exactly ONE of them (the Bug 243 malformed
// recorded schema) above the drop. Six stayed below, including all three
// integrity gates. Measured on three refusable chains: the drop ran
// first every time.
//
// The BRK-1 test's own header said so and nobody enumerated it:
// "ChainRestore.Run carries its own doors, but they fire after the
// broker has already dropped every table named in the cached manifest."
// Plural.
//
// WHAT THIS REACHES: the ORDER of two calls inside coldStartReset, by
// AST position — PreflightBeforeTarget must appear before
// dropExistingTargetTables. It does not prove the preflight covers the
// right doors; that is
// TestChainRestorePreTargetDoorRoster's job, and the behavioural half is
// pinned by the broker's own refusal tests.
func TestBrokerColdStartResetPreflightsBeforeTheDrop(t *testing.T) {
	src := mustParseFuncBody(t, "broker.go", "coldStartReset")

	preflightAt := strings.Index(src, "PreflightBeforeTarget(ctx)")
	dropAt := strings.Index(src, "dropExistingTargetTables(ctx")
	if dropAt < 0 {
		t.Fatal("anchor 'dropExistingTargetTables(ctx' not found in coldStartReset — the destructive " +
			"call was renamed and this gate can no longer check the order. Re-anchor it.")
	}
	if preflightAt < 0 {
		t.Fatal("coldStartReset no longer calls rest.PreflightBeforeTarget. The broker drops every table " +
			"in the cached tail manifest; without the full pre-target door list running FIRST, a chain " +
			"the restore then refuses (corrupt, tampered, unsigned under --require-signature) leaves the " +
			"operator with no target AND no restore. Re-wire it rather than deleting this gate.")
	}
	if preflightAt > dropAt {
		t.Error("coldStartReset runs the pre-target preflights AFTER dropExistingTargetTables, which is " +
			"the RCN-1 defect exactly: the doors that fire on a corrupt or tampered chain would report " +
			"the problem only once the target is already gone")
	}
}

// TestChainRestorePreTargetDoorRoster holds Run and the exported
// preflight to the SAME door list.
//
// WHY A ROSTER AND NOT A PROMISE. The RCN-1 defect was a caller holding
// a SUBSET of the doors — one of seven. The fix routes both callers
// through one function, and this gate is what keeps a future door from
// being added to Run's body directly, re-creating the subset.
func TestChainRestorePreTargetDoorRoster(t *testing.T) {
	body := mustParseFuncBodyIn(t, "backup/chain_restore.go", "preflightBeforeTarget")

	// The seven pre-target doors, by the call each makes. Derived from
	// the numbered steps in Run's own comments (2 through 2.8) plus the
	// two structural checks the exported wrapper runs ahead of them.
	want := []string{
		"preflightCrossEngineSupportable",
		"refuseUnrepresentableTargetShape",
		"preflightEncryption",
		"checkMixedModeChain",
		"restoreManifestIntegrityPreflights",
		"verifyChainSignatures",
	}
	for _, w := range want {
		if !strings.Contains(body, w+"(") {
			t.Errorf("the shared pre-target door list no longer calls %s. If this door moved back into "+
				"ChainRestore.Run's body, the broker's --reset-target-data cold start stops running it "+
				"before its destructive drop — which is RCN-1 returning.", w)
		}
	}

	// Run must delegate rather than inline: an inlined door is one the
	// broker does not get.
	runBody := mustParseFuncBodyIn(t, "backup/chain_restore.go", "Run")
	if !strings.Contains(runBody, "preflightBeforeTarget(ctx") {
		t.Error("ChainRestore.Run no longer delegates to preflightBeforeTarget; the two callers can now " +
			"drift, which is the shape RCN-1 was")
	}
	for _, w := range want {
		if strings.Contains(runBody, w+"(") {
			t.Errorf("ChainRestore.Run calls %s directly as well as through the shared list — a door in "+
				"two places is a door that will be changed in one", w)
		}
	}
}

// mustParseFuncBody renders the body of the named function in the given
// file of THIS package as source text.
func mustParseFuncBody(t *testing.T, file, fn string) string {
	t.Helper()
	return mustParseFuncBodyIn(t, file, fn)
}

// mustParseFuncBodyIn is the same over any path, so the roster can reach
// the sibling backup package.
func mustParseFuncBodyIn(t *testing.T, path, fn string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out string
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Name.Name != fn || fd.Body == nil {
			return true
		}
		start := fset.Position(fd.Body.Pos()).Offset
		end := fset.Position(fd.Body.End()).Offset
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("read %s: %v", path, rerr)
		}
		out = string(src[start:end])
		return false
	})
	if out == "" {
		t.Fatalf("function %q not found in %s (renamed or removed) — re-anchor this gate rather than "+
			"deleting it", fn, path)
	}
	return out
}
