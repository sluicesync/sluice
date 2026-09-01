// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// The A3 maintenance-heal artifacts exist so a re-signed chain cannot look
// untouched. They shipped wired to `backup verify` ALONE — every RESTORE
// path was blind to them (audit 2026-08-31 SEC-6), which is the one moment
// the provenance actually matters: an operator restoring a chain that was
// healed two weeks earlier saw "all manifest + lineage signatures verified"
// and nothing else.
//
// The fix reaches all three entry points. This gate is what keeps it
// reached. It DERIVES its universe from the AST rather than listing it:
// every function in this package that calls one of the signature-
// verification primitives (lineage.VerifyManifest / lineage.VerifyLineage)
// is a signature-verification entry point by definition, and each must
// either call reportMaintenanceHeals or be classified exempt WITH A REASON
// below. A new verify path fails here until someone makes that call.
//
// SCOPE, stated so the name cannot be read as broader than the truth: this
// reaches functions in package `backup` that call the two primitives
// directly. A future entry point that verifies signatures by delegating to
// one of the rostered functions inherits its report and is correctly
// invisible here; one that reimplements verification against a different
// primitive would not be found — which is why the primitive set is a
// named constant right here rather than buried in the walker.

// healReportExempt lists verification functions that deliberately do NOT
// report heal provenance. The reason is the load-bearing half.
var healReportExempt = map[string]string{
	"healStaleLineageSignatures": "the WRITER of the heal record, not a reporter — it already emits its own louder WARN naming the heal it is about to perform; reporting prior heals here would announce provenance in the same breath as creating it",
}

// healReportPrimitives are the signature-verification primitives whose
// callers constitute the entry-point universe.
var healReportPrimitives = map[string]struct{}{
	"VerifyManifest": {},
	"VerifyLineage":  {},
}

func TestMaintenanceHealReportRoster_EveryVerifyEntryPointClassified(t *testing.T) {
	entry := discoverSignatureVerifyEntryPoints(t)

	// Anti-vacuity floor: the walk must find the four known functions
	// (verifyManifestSignaturePolicy, verifyChainSignatures,
	// verifyBackupSignatures, healStaleLineageSignatures). A broken
	// matcher finds none and would otherwise pass silently.
	if len(entry) < 4 {
		names := make([]string, 0, len(entry))
		for name := range entry {
			names = append(names, name)
		}
		sort.Strings(names)
		t.Fatalf("anti-vacuity: found %d signature-verification entry point(s) %v; want >=4 — the AST matcher is likely broken", len(entry), names)
	}

	var missing []string
	reporting := 0
	for name, info := range entry {
		if info.reportsHeals {
			reporting++
			continue
		}
		if _, ok := healReportExempt[name]; ok {
			continue
		}
		missing = append(missing, name+"  ("+info.pos+")")
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("signature-verification entry point(s) that neither report maintenance-heal provenance nor are classified exempt:\n  %s\n\n"+
			"Either call reportMaintenanceHeals(ctx, <root store>, \"<verb>\") at the top of the function, or add it to "+
			"healReportExempt in this file WITH A REASON. A restore path blind to a heal is the SEC-6 defect: the chain's "+
			"signatures were regenerated and the operator is told only that they verify.",
			strings.Join(missing, "\n  "))
	}

	// Second anti-vacuity floor, and the one that matters most: exempting
	// everything would satisfy the loop above. At least three entry points
	// must actually report.
	if reporting < 3 {
		t.Fatalf("only %d entry point(s) report heal provenance; want >=3 (backup verify, chain verify, single-manifest verify)", reporting)
	}

	// Reverse guard: an exemption whose function vanished hides drift.
	for name := range healReportExempt {
		if _, ok := entry[name]; !ok {
			t.Errorf("stale exemption %q: no such signature-verification entry point — remove it or fix the name", name)
		}
	}
}

type verifyEntryPoint struct {
	pos          string
	reportsHeals bool
}

// discoverSignatureVerifyEntryPoints walks this package's non-test Go
// files and returns every function that calls one of
// [healReportPrimitives], noting whether it also calls
// reportMaintenanceHeals.
func discoverSignatureVerifyEntryPoints(t *testing.T) map[string]verifyEntryPoint {
	t.Helper()
	out := make(map[string]verifyEntryPoint)
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %q: %v", name, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var verifies, reports bool
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fn := call.Fun.(type) {
				case *ast.SelectorExpr:
					// lineage.VerifyManifest / lineage.VerifyLineage.
					if pkg, ok := fn.X.(*ast.Ident); ok && pkg.Name == "lineage" {
						if _, ok := healReportPrimitives[fn.Sel.Name]; ok {
							verifies = true
						}
					}
				case *ast.Ident:
					if fn.Name == "reportMaintenanceHeals" {
						reports = true
					}
				}
				return true
			})
			if !verifies {
				continue
			}
			out[fd.Name.Name] = verifyEntryPoint{
				pos:          fset.Position(fd.Pos()).String(),
				reportsHeals: reports,
			}
		}
	}
	return out
}
