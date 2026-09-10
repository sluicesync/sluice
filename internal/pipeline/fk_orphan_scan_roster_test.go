// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The item-109 metadata-only foreign-key add and the item-112 orphan scan are
// TWO HALVES OF ONE GUARANTEE, and this gate is what holds them together.
//
// [migcore.ArmForeignKeyConsistency] tells a MySQL-family target writer it may
// add foreign keys WITHOUT InnoDB's O(rows) re-validation — which is what
// dodges PlanetScale's ~900s statement wall on a large child table. That is
// safe only because the writer records each such FK and a later bounded
// chunked orphan scan ([migcore.ValidateUnvalidatedForeignKeys]) proves it
// clean, dropping the FK and refusing with SLUICE-E-FK-SOURCE-ORPHAN when it
// is not. Arm without scan and the run exits 0 with a foreign key on the
// target over data that may violate it.
//
// WHY IT NEEDS A GATE. The arm is applied where a WRITER IS OPENED; the scan
// runs where the CONSTRAINTS PHASE ENDS. Different functions, usually
// different files, and nothing in the type system connects them — so a new
// path that opens an armed writer inherits the fast add and silently skips the
// proof. That is exactly what v0.149.0's stopped-cold-start resume did: it was
// handed an identically-configured writer by the helper the release split out
// ([Streamer.openColdStartSchemaWriter], which arms), ran the constraints
// phase through it, and went straight to the CDC anchor write without ever
// calling the scan. Found by the pre-tag value-fidelity review, not by any
// test — the CLAUDE.md "a refusal that MOVED owes its caller list" shape, one
// release after the door moved (audit VF0910-F1).
//
// WHAT IT CHECKS. The universe is derived from the AST, not hand-maintained:
// build the receiver-typed intra-package call graph, find every function that
// arms DIRECTLY, and require each to reach the scan itself — or, when the arm
// lives in a shared opener that scans nothing (which is the shape that caused
// the defect), require every DIRECT CALLER of that opener to reach the scan
// itself or to be covered by a named ancestor. That last claim is the only
// hand-written part, and it is machine-checked in both directions: the named
// ancestor must actually reach the scan AND actually reach the caller.
//
// A "covered because some caller of mine scans" rule was tried first and is
// UNSOUND — it is what a first cut of this gate did, and the mutation run
// caught it. `Streamer.coldStart` both scans and calls the resume, so the
// resume looked covered by an ancestor that in fact returns early the moment
// the resume handles the stream. Coverage has to be established DOWN the call
// graph from the function that holds the writer, never up from it.
//
// SCOPE, stated rather than implied. The walk is package-local and follows
// calls on a function's OWN receiver plus package-level functions, the same
// discipline as [pipelineCallGraph] — so a route that stores a method value in
// a struct field and invokes it under another name is invisible to it, and a
// path leaving package pipeline is not followed. The two migcore symbols are
// recognised explicitly because they are called through the package qualifier.

const (
	fkArmNode  = "migcore.ArmForeignKeyConsistency"
	fkScanNode = "migcore.ValidateUnvalidatedForeignKeys"
)

// fkOrphanScanCoveredBy names, for a caller of a shared arming opener that
// does not itself reach the scan, the ANCESTOR that holds the armed writer and
// does. Both halves of each claim are verified against the AST: the ancestor
// must reach the scan, and must reach the caller.
//
// The multi-database cold start is deliberately absent: it does not arm at all
// (see the note at its OpenSchemaWriter in streamer_multidb.go), which is the
// other legitimate way to be safe.
var fkOrphanScanCoveredBy = map[string]string{
	// coldStartOpenTargetWriters opens the armed writer and hands it back to
	// coldStart, which passes it to coldStartRunCopy — the function that runs
	// the scan, before the CDC anchor write.
	"Streamer.coldStartOpenTargetWriters": "Streamer.coldStart",
}

func TestForeignKeyOrphanScanRoster_EveryArmedPathScans(t *testing.T) {
	callees := fkCallGraph(t)

	// Anti-vacuity: a walker that stopped matching the package would bless
	// everything below for free.
	const minFuncs = 200
	if len(callees) < minFuncs {
		t.Fatalf("call graph has %d functions (floor %d) — the walker is not seeing package pipeline, "+
			"and this gate grades nothing", len(callees), minFuncs)
	}

	// DIRECT armers — the functions that apply the arm themselves.
	var direct []string
	for node, cs := range callees {
		if cs[fkArmNode] {
			direct = append(direct, node)
		}
	}
	sort.Strings(direct)
	if len(direct) < 3 {
		t.Fatalf("anti-vacuity: expected >=3 functions calling %s directly, found %d (%s) — the matcher is "+
			"likely broken", fkArmNode, len(direct), strings.Join(direct, ", "))
	}
	if len(fkNodesReaching(callees, fkScanNode)) < 2 {
		t.Fatalf("anti-vacuity: expected >=2 functions reaching %s — the matcher is likely broken", fkScanNode)
	}

	// Positive control: the ordinary sync cold start's copy driver reaches the
	// scan. If this stops holding, the walker is broken (or the cold start
	// stopped scanning) and every verdict below is meaningless.
	if !reaches(callees, "Streamer.coldStartRunCopy", fkScanNode) {
		t.Fatalf("control: Streamer.coldStartRunCopy no longer reaches %s. Either the sync cold start "+
			"stopped scanning — a real defect — or this gate is stale; resolve that before trusting it", fkScanNode)
	}

	callers := fkReverse(callees)

	var uncovered []string
	for _, armer := range direct {
		if reaches(callees, armer, fkScanNode) {
			continue // the armer runs the scan on its own path
		}
		// A shared opener that only ARMS. Every direct caller owes a verdict:
		// the "which call PATHS reached the door" rule.
		cs := sortedTrueKeys(callers[armer])
		if len(cs) == 0 {
			uncovered = append(uncovered, armer+" (arms, never scans, and has no in-package caller)")
			continue
		}
		for _, c := range cs {
			if reaches(callees, c, fkScanNode) {
				continue
			}
			ancestor, claimed := fkOrphanScanCoveredBy[c]
			switch {
			case !claimed:
				uncovered = append(uncovered, c+" (calls "+armer+", never reaches the scan, unclassified)")
			case !reaches(callees, ancestor, fkScanNode):
				uncovered = append(uncovered, c+" (claims coverage by "+ancestor+", which does NOT reach the scan)")
			case !reaches(callees, ancestor, c):
				uncovered = append(uncovered, c+" (claims coverage by "+ancestor+", which does NOT reach it)")
			}
		}
	}
	if len(uncovered) > 0 {
		sort.Strings(uncovered)
		t.Fatalf("these paths ARM the item-109 metadata-only foreign-key add without running the item-112 "+
			"orphan scan:\n  %s\n\n"+
			"Arming without the scan lets a foreign key land on the target over data that may violate it, "+
			"at exit 0 — the scan is the LOUD HALF of the guarantee, not an optimisation.\n"+
			"Either call verifyUnvalidatedForeignKeys before the path's point of no return (for a cold start "+
			"that is the CDC anchor write; for migrate, the end of the constraints phase), or add an entry to "+
			"fkOrphanScanCoveredBy naming the ancestor that holds the armed writer and scans.",
			strings.Join(uncovered, "\n  "))
	}
}

// sortedTrueKeys returns the keys of a set, sorted.
func sortedTrueKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// fkNodesReaching returns every node that calls target directly or
// transitively, sorted.
func fkNodesReaching(callees map[string]map[string]bool, target string) []string {
	var out []string
	for node := range callees {
		if reaches(callees, node, target) {
			out = append(out, node)
		}
	}
	sort.Strings(out)
	return out
}

// fkReverse inverts a call graph into node -> callers.
func fkReverse(callees map[string]map[string]bool) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for caller, cs := range callees {
		for callee := range cs {
			if out[callee] == nil {
				out[callee] = map[string]bool{}
			}
			out[callee][caller] = true
		}
	}
	return out
}

// fkCallGraph is [pipelineCallGraph]'s shape — nodes "RecvType.Func" for
// methods and ".Func" for package-level functions — with the two migcore
// symbols this gate grades recognised as synthetic nodes, since they are
// reached through the package qualifier rather than a receiver.
func fkCallGraph(t *testing.T) map[string]map[string]bool {
	t.Helper()

	graded := map[string]string{
		"ArmForeignKeyConsistency":       fkArmNode,
		"ValidateUnvalidatedForeignKeys": fkScanNode,
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	out := map[string]map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			recvType, recvName := receiverOf(fn)
			node := recvType + "." + fn.Name.Name
			calls := out[node]
			if calls == nil {
				calls = map[string]bool{}
				out[node] = calls
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch v := call.Fun.(type) {
				case *ast.Ident:
					calls["."+v.Name] = true
				case *ast.SelectorExpr:
					if synthetic, ok := graded[v.Sel.Name]; ok {
						calls[synthetic] = true
						return true
					}
					if id, ok := v.X.(*ast.Ident); ok && recvName != "" && id.Name == recvName {
						calls[recvType+"."+v.Sel.Name] = true
					}
				}
				return true
			})
		}
	}
	return out
}
