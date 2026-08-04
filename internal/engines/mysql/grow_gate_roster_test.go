// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every MySQL BULK-WRITE lane must participate in the grow gate — the SIBLING
// of the Postgres roster (perf-parity review, 2026-08-04).
//
// The commit that wired the last two ungated lanes fixed them on BOTH engines
// and then gated only ONE. Its own message says "the float-repair core, AND
// ITS MYSQL TWIN", so the cross-engine nature was understood at the time; the
// gate simply was not carried across. That is the exact shape CLAUDE.md warns
// about — a fix made for the class, guarded for the instance — reproduced in
// the very change that closed two instances of it.
//
// MySQL had no ungated lane at the time this was written; the gap was in the
// GUARD, not the code. A new MySQL bulk-write lane could have been added
// ungated and nothing would have failed. The pre-existing MySQL grow-gate
// tests are behavioural and drive WriteRows on the batched-insert path only,
// so they reach neither writeLoadData nor the float-repair core.
//
// Lane identification, exemption semantics and the stated limits are
// identical to the Postgres roster; see internal/engines/postgres/
// grow_gate_roster_test.go for the full rationale.

package mysql

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

// mysqlGrowGateExempt lists bulk-write lanes that legitimately never touch the
// gate, each with the reason. An entry is a claim someone can check.
var mysqlGrowGateExempt = map[string]string{
	// Delegates through floatrepair.RepairByPK to pgFloatBatchExecer.ExecBatch,
	// which carries the gate. The delegation is invisible to the body scan
	// because the callee is named only as a struct literal, so it is recorded
	// here rather than papered over by loosening the match.
	"UpdateFloatColumnsByPK": "dispatcher; the gate lives in pgFloatBatchExecer.ExecBatch, reached via floatrepair.RepairByPK",
}

// mysqlGateCalls are the ways a lane can reach the gate.
// MySQL reaches the gate through flushWithReparentRetry (its Await+Trip+
// replay helper, the analogue of Postgres copyChunkWithRetry) as well as
// directly. Deriving this list from the POSTGRES one without checking is how
// the first run of this gate flagged seven lanes that are in fact covered.
var mysqlGateCalls = []string{"awaitGrowGate", "tripGrowGate", "flushWithReparentRetry"}

func TestEveryMySQLBulkWriteLaneReachesTheGrowGate(t *testing.T) {
	lanes, bodies := discoverMySQLBulkWriteLanes(t)

	// Anti-vacuity: a walk that stops finding lanes passes on nothing, which
	// is precisely how the gate this replaces failed.
	if len(lanes) < 5 {
		t.Fatalf("found only %d bulk-write lanes in package mysql (%v); the signature walk is not "+
			"reaching the tree and this roster would pass on an empty set", len(lanes), lanes)
	}

	// Satisfaction is a FIXED POINT, not one hop: a lane is satisfied if it
	// reaches the gate directly, or delegates to a lane that is itself
	// satisfied. A single-pass rule would score a lane as covered for
	// delegating to something equally uncovered.
	//
	// (An earlier version of this comment justified the design by claiming
	// copyFromOnSQLConn is "a lane by signature that reaches no gate". It is
	// not a lane under the shipped rule — its signature takes a
	// pgx.CopyFromSource, not rows in bulk — so the cited evidence was wrong
	// even though the design decision is right. Corrected rather than deleted,
	// because a justification nobody can check is how the gates this file
	// replaces went bad.)
	ok := map[string]bool{}
	for _, name := range lanes {
		for _, g := range mysqlGateCalls {
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
			// Delegation counts only for a pure dispatcher — see the Postgres
			// roster for the mutation that produced this rule.
			if executesMySQLSQL(bodies[name]) {
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

	var missing []string
	for _, name := range lanes {
		if satisfied(name) {
			continue
		}
		if _, ok := mysqlGrowGateExempt[name]; ok {
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
			"error, or add the lane to mysqlGrowGateExempt with the reason it cannot participate.", missing)
	}

	// The other direction: an exemption that is stale lies about the debt.
	for name, why := range mysqlGrowGateExempt {
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
			t.Errorf("mysqlGrowGateExempt lists %q, which is no longer a bulk-write lane — remove it", name)
		} else if satisfied(name) {
			t.Errorf("mysqlGrowGateExempt lists %q, but it now reaches the gate — remove the exemption so the "+
				"lane stays gated", name)
		}
	}
}

// discoverMySQLBulkWriteLanes returns every method whose signature accepts rows in
// bulk, plus its rendered body for call-site matching.
func discoverMySQLBulkWriteLanes(t *testing.T) (names []string, bodies map[string]map[string]bool) {
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
			if !acceptsMySQLRowsInBulk(fset, fn.Type.Params) {
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

// acceptsMySQLRowsInBulk reports whether a parameter list carries rows in bulk:
// a channel of ir.Row, a slice of ir.Row, or an io.Reader (raw COPY bytes).
func acceptsMySQLRowsInBulk(fset *token.FileSet, params *ast.FieldList) bool {
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

// executesMySQLSQL reports whether a lane runs a statement itself, as opposed
// to only dispatching to another lane.
func executesMySQLSQL(calls map[string]bool) bool {
	for _, c := range []string{"ExecContext", "QueryContext", "QueryRowContext"} {
		if calls[c] {
			return true
		}
	}
	return false
}
