// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// resignVerifyExempt names each function that re-signs a lineage WITHOUT
// a preceding signature verification, with the reason. Fail-by-default.
var resignVerifyExempt = map[string]string{
	"resignIfSigned": "the shared re-sign helper itself. It is the thing being guarded, not a guard site; " +
		"its CALLERS are what this roster grades.",
	"healStaleLineageSignatures": "the crash-recovery NO-OP door (batch C, 2026-08-23), which re-signs a " +
		"chain it did NOT restructure. It carries its own stronger provenance guards — a recorded-KeyID " +
		"wrong-key check, a preserved pre-heal .sig and a durable heal record — precisely because its own " +
		"doc observes that 're-sign is also exactly what laundering a tampered catalog looks like'. Audit " +
		"2026-09-06 S-1 is that reasoning finally reaching the RESTRUCTURE door beside it.",
}

// TestResignSitesVerifyFirst is the audit 2026-09-06 S-1 ratchet.
//
// THE DEFECT. `backup prune` and `backup compact` restructured a signed
// chain and re-signed the result, verifying nothing first. An attacker
// with store-write access — the adversary ADR-0152/0154 are written
// against — could tamper a manifest (whose edit then fails verification
// at restore) and wait for the operator's scheduled maintenance run to
// mint fresh, valid signatures over it. ADR-0154 line 47 says the
// signature is "verified at restore/verify/prune/compact time"; the
// prune/compact half was a written invariant nobody checked.
//
// WHAT IT REACHES: every function in this package that calls
// ResignLineage or resignIfSigned must also call a verification function
// somewhere in the same body, or carry an exemption saying why.
//
// WHAT IT DOES NOT REACH, so the name cannot be read wider: it does not
// prove the verify DOMINATES the re-sign on every path, nor that it is
// strict. Those are pinned behaviourally by the prune/compact refusal
// tests. It proves the verification is present, which is what was
// missing.
func TestResignSitesVerifyFirst(t *testing.T) {
	resigners := map[string]bool{
		"ResignLineage":  true,
		"resignIfSigned": true,
	}
	verifiers := map[string]bool{
		"verifyBeforeRestructure": true,
		"verifyChainSignatures":   true,
		"VerifyManifest":          true,
		"VerifyLineage":           true,
	}

	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	fset := token.NewFileSet()
	var sites []string

	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var resigns, verifies bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if resigners[fun.Name] {
						resigns = true
					}
					if verifiers[fun.Name] {
						verifies = true
					}
				case *ast.SelectorExpr:
					if resigners[fun.Sel.Name] {
						resigns = true
					}
					if verifiers[fun.Sel.Name] {
						verifies = true
					}
				}
				return true
			})
			if !resigns {
				continue
			}
			sites = append(sites, fn.Name.Name)
			if verifies {
				continue
			}
			if why, exempt := resignVerifyExempt[fn.Name.Name]; exempt {
				if strings.TrimSpace(why) == "" {
					t.Errorf("%s is exempt from the re-sign verify roster with an EMPTY reason", fn.Name.Name)
				}
				continue
			}
			t.Errorf("%s (%s) re-signs a lineage without verifying the existing signatures first.\n\n"+
				"Re-signing whatever is on the store is what laundering a tampered chain looks like: the "+
				"attacker's edit fails verification at restore, and this run replaces the failing "+
				"signature with a valid one over the same content. Call verifyBeforeRestructure, or add a "+
				"%q entry to resignVerifyExempt saying why this site cannot launder.",
				fn.Name.Name, path, fn.Name.Name)
		}
	}

	// Anti-vacuity: the walk must still find the re-sign sites at all.
	if len(sites) < 3 {
		t.Fatalf("found only %d re-sign site(s) %v; this package has prune, compact and the two helpers, "+
			"so the AST walk has drifted and this gate grades nothing", len(sites), sites)
	}
	seen := map[string]bool{}
	for _, s := range sites {
		seen[s] = true
	}
	for name := range resignVerifyExempt {
		if !seen[name] {
			t.Errorf("resignVerifyExempt names %q, which re-signs nothing in this package (renamed?). A "+
				"stale exemption is how a real site later inherits a pass it was never granted.", name)
		}
	}

	// The ADR sentence this closes must still say what it says; if the
	// ADR is reworded the gate's rationale needs re-reading, not silent
	// drift.
	adr, rerr := os.ReadFile(filepath.Join("..", "..", "..", "docs", "adr", "adr-0154-signed-backup-manifests.md"))
	if rerr != nil {
		t.Fatalf("read ADR-0154: %v", rerr)
	}
	if !strings.Contains(string(adr), "prune") {
		t.Error("ADR-0154 no longer mentions prune in its verification claim; this gate exists because " +
			"that claim was unimplemented, so a reword deserves a look rather than a silent pass")
	}
}
