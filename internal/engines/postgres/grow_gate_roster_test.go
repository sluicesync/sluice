// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every Postgres BULK-WRITE lane must participate in the grow gate — and this
// discovers the lanes instead of being told them (audit 2026-08-04).
//
// The gate this replaces, TestGrowGate_ReachesEveryWriteCore, constructed a
// bare &RowWriter{}, called SetGrowGate, then called awaitGrowGate and
// tripGrowGate DIRECTLY and asserted they worked. It never invoked a write
// core. The audit mutation-proved the consequence: reverting all three
// grow-gate wirings left both engine suites green. A gate named for "every
// write core" measured neither the cores nor construction — and its existence
// is what stopped anyone looking, while two of the six lanes were ungated,
// including one on the very receiver that stores the gate.
//
// # How a lane is identified, and why not by name
//
// A bulk-write lane is one that accepts ROWS IN BULK: a `<-chan ir.Row`, a
// `[]ir.Row`, or an `io.Reader` of raw COPY bytes. That is a structural
// property of the signature, so a new lane is discovered whether or not it is
// called writeViaSomething — which matters, because the two lanes that were
// missed are named ImportRawCopy and ExecBatch and would not have matched any
// naming convention.
//
// Deliberately NOT "executes SQL": that catches every reader, probe, DDL
// helper and catalog query in the package (~90 methods), which would need an
// exemption list larger than the thing it guards. Scope stated here rather
// than left implied.
//
// A lane satisfies the rule by reaching the gate itself OR by delegating to
// another lane that does — dispatchers are legitimate.
//
// LIMIT, stated: this proves the lane MENTIONS the gate, not that it calls it
// on every branch at runtime. The integration suites cover the runtime half
// for the chunked-COPY core.

package postgres

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// growGateExempt lists bulk-write lanes that legitimately never touch the
// gate, each with the reason. An entry is a claim someone can check.
var growGateExempt = map[string]string{
	// Delegates through floatrepair.RepairByPK to pgFloatBatchExecer.ExecBatch,
	// which carries the gate. The delegation is invisible to the body scan
	// because the callee is named only as a struct literal, so it is recorded
	// here rather than papered over by loosening the match.
	"UpdateFloatColumnsByPK": "dispatcher; the gate lives in pgFloatBatchExecer.ExecBatch, reached via floatrepair.RepairByPK",
}

// gateCalls are the ways a lane can reach the gate.
var gateCalls = []string{"awaitGrowGate", "tripGrowGate", "quiesceAndReportTransient", "copyChunkWithRetry"}

func TestEveryPostgresBulkWriteLaneReachesTheGrowGate(t *testing.T) {
	lanes, bodies := discoverBulkWriteLanes(t)

	// Anti-vacuity: a walk that stops finding lanes passes on nothing, which
	// is precisely how the gate this replaces failed.
	if len(lanes) < 5 {
		t.Fatalf("found only %d bulk-write lanes in package postgres (%v); the signature walk is not "+
			"reaching the tree and this roster would pass on an empty set", len(lanes), lanes)
	}

	// Satisfaction is a FIXED POINT, not one hop. A lane is satisfied if it
	// reaches the gate directly, or delegates to a lane that is itself
	// satisfied. Resolving this in a single pass was wrong and this gate
	// caught it on its first run: writeViaCopy delegates to the shared
	// copyFromOnSQLConn plumbing, which is a lane by signature and reaches no
	// gate, so a one-hop rule scored writeViaCopy as covered by delegating to
	// something equally uncovered.
	ok := map[string]bool{}
	for _, name := range lanes {
		for _, g := range gateCalls {
			if bodies[name][g] {
				ok[name] = true
				break
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for _, name := range lanes {
			if ok[name] {
				continue
			}
			for _, other := range lanes {
				if other != name && ok[other] && bodies[name][other] {
					ok[name] = true
					changed = true
					break
				}
			}
		}
	}
	satisfied := func(name string) bool { return ok[name] }
	for k := range bodies["writeViaCopy"] {
		for _, g := range gateCalls {
			if k == g {
				t.Logf("writeViaCopy calls GATE %s", k)
			}
		}
		for _, l := range lanes {
			if k == l {
				t.Logf("writeViaCopy calls LANE %s", k)
			}
		}
	}

	var missing []string
	for _, name := range lanes {
		if satisfied(name) {
			continue
		}
		if _, ok := growGateExempt[name]; ok {
			continue
		}
		missing = append(missing, name)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("bulk-write lane(s) %v never reach the grow gate.\n\n"+
			"ADR-0110's coordinated pause only works if EVERY lane participates. A lane that can neither "+
			"wait nor signal keeps writing into a grow window that every other lane is backing off from — "+
			"which makes it STRICTLY LESS resilient than the lane it replaces, not merely unoptimised.\n\n"+
			"Call awaitGrowGate before the write and quiesceAndReportTransient (or tripGrowGate) on its "+
			"error, or add the lane to growGateExempt with the reason it cannot participate.", missing)
	}

	// The other direction: an exemption that is stale lies about the debt.
	for name, why := range growGateExempt {
		if strings.TrimSpace(why) == "" {
			t.Errorf("exemption %q carries no reason; an unexplained exemption is indistinguishable from "+
				"an oversight", name)
		}
		found := false
		for _, l := range lanes {
			if l == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("growGateExempt lists %q, which is no longer a bulk-write lane — remove it", name)
		} else if satisfied(name) {
			t.Errorf("growGateExempt lists %q, but it now reaches the gate — remove the exemption so the "+
				"lane stays gated", name)
		}
	}
}

// discoverBulkWriteLanes returns every method whose signature accepts rows in
// bulk, plus its rendered body for call-site matching.
func discoverBulkWriteLanes(t *testing.T) (names []string, bodies map[string]map[string]bool) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	fset := token.NewFileSet()
	bodies = map[string]map[string]bool{}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, e.Name(), nil, 0)
		if perr != nil {
			continue
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || fn.Type.Params == nil {
				continue
			}
			if !acceptsRowsInBulk(fset, fn.Type.Params) {
				continue
			}
			calls := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ce, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch f := ce.Fun.(type) {
				case *ast.Ident:
					calls[f.Name] = true
				case *ast.SelectorExpr:
					calls[f.Sel.Name] = true
				}
				return true
			})
			names = append(names, fn.Name.Name)
			bodies[fn.Name.Name] = calls
		}
	}
	sort.Strings(names)
	return names, bodies
}

// acceptsRowsInBulk reports whether a parameter list carries rows in bulk:
// a channel of ir.Row, a slice of ir.Row, or an io.Reader (raw COPY bytes).
func acceptsRowsInBulk(fset *token.FileSet, params *ast.FieldList) bool {
	for _, p := range params.List {
		var b strings.Builder
		_ = printer.Fprint(&b, fset, p.Type)
		switch t := b.String(); {
		case strings.Contains(t, "chan ir.Row"),
			strings.Contains(t, "[]ir.Row"),
			t == "io.Reader":
			return true
		}
	}
	return false
}
