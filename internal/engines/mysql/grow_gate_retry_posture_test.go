// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// What each grow-gate-touching write core does when the gate DECLINES to close
// — the MySQL SIBLING of internal/engines/postgres/grow_gate_retry_posture_
// test.go (premise-naming sweep, 2026-08-05).
//
// Rationale, discovery rule and stated limits are identical to the Postgres
// roster; read that file's header for the full argument. What differs here:
//
//   - MySQL reaches the bounded replay through flushWithReparentRetry rather
//     than copyChunkWithRetry.
//   - THREE cores reach it, not two: the two batched cores and — since item
//     114 — LOAD DATA, per segment. That is a change from what the item-138
//     ceiling was reasoned against, which is the reason this roster is derived
//     from the code rather than transcribed from a review: the review that
//     surfaced this finding still listed LOAD DATA as non-retrying, correctly
//     for the tree it read and wrongly by the time the fix was written.
//   - The gate-touching function for LOAD DATA is flushLoadDataSegment, not
//     writeLoadData. Discovering by "mentions the gate" rather than by lane
//     signature is what makes that visible; the lane-coverage roster next door
//     credits writeLoadData through the segment helper instead.
//
// This reaches package `mysql` ONLY. Neither roster covers the other.

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
	"time"
)

// mysqlRetryPosture is what a gate-touching write core does when the target
// throws a classified transient and no coordinated pause is available.
type mysqlRetryPosture string

const (
	// mysqlPostureRetries: the core re-drives the write through the bounded
	// replay helper, backing off exponentially, and is loud on exhaustion.
	mysqlPostureRetries mysqlRetryPosture = "retries"
	// mysqlPostureNoRetry: the core Awaits and Trips but cannot re-drive the
	// write, so the first transient it meets is terminal. Loud, never silent.
	mysqlPostureNoRetry mysqlRetryPosture = "no-retry"
)

// mysqlGateCallNames are the ways a function touches the gate. A function
// mentioning any of these is a gate-touching core and must be classified.
var mysqlGateCallNames = []string{"awaitGrowGate", "tripGrowGate", "flushWithReparentRetry"}

// mysqlReplayHelpers are the calls that give a core a bounded REPLAY of the
// write it just lost. Reaching one is what "retries" means.
var mysqlReplayHelpers = []string{"flushWithReparentRetry"}

// mysqlGatePlumbing are the gate-touching functions that are the gate's own
// wiring rather than write cores, each with the reason.
//
// awaitGrowGate / tripGrowGate are absent deliberately: they call the ir.
// GrowGate interface directly (Await / Trip), not these names, so the walk
// never sees them as gate-touching. Naming them here would be a stale
// exemption, and the roster fails on those.
var mysqlGatePlumbing = map[string]string{
	"RowWriter.flushWithReparentRetry": "the bounded-replay helper itself; the thing the roster classifies cores BY",
	"RowWriter.acquireConnWithRetry": "item 139's connection-ACQUIRE loop, not a write core: it loses no write, " +
		"so the replay-safety question this roster classifies cores by does not arise (re-acquiring is " +
		"idempotent by construction — see row_writer_conn_acquire.go). Its answer to a DECLINING gate is " +
		"nonetheless the retrying one: Await returns, the acquire is re-tried on the same 100ms→30s ladder " +
		"TestColdCopyReparentBackoffShape binds, and it is loud on the wall-clock bound.",
}

// mysqlLaneRetryPosture is the DECLARED roster. Fail-by-default: a gate-
// touching core that is neither here nor in mysqlGatePlumbing fails the test.
//
// Keep this in sync with the table in [migcore.GrowGateMaxQuiesceShare]'s doc.
var mysqlLaneRetryPosture = map[string]struct {
	posture mysqlRetryPosture
	why     string
}{
	"RowWriter.writeBatchedConn": {
		mysqlPostureRetries,
		"plain multi-row INSERT, re-sent byte-identically. Safe because a committed-but-unacked prior attempt " +
			"surfaces as 1062, which the core tolerates ON A RETRY only. REFUSES loudly " +
			"(errKeylessAmbiguousReplay) on a table with no PK / no NOT NULL UNIQUE, where nothing collides.",
	},
	"RowWriter.writeBatchedIdempotentConn": {
		mysqlPostureRetries,
		"ON DUPLICATE KEY UPDATE upsert: a replayed batch converges. Same keyless refusal from the shared helper.",
	},
	"RowWriter.flushLoadDataSegment": {
		mysqlPostureRetries,
		"item 114. One bounded LOAD DATA per segment, the segment's encoded bytes held so the replay is " +
			"byte-identical; LOAD DATA LOCAL downgrades a duplicate key to a warning and skips the row, so the " +
			"replay is convergent on a keyed table. Same keyless refusal from the shared helper.",
	},
	"mysqlFloatBatchExecer.ExecBatch": {
		mysqlPostureNoRetry,
		"VStream float-repair UPDATE. The statement IS idempotent (keyed on the PK) so a replay would be safe; " +
			"the Await+Trip parity landed without one and adding it is a separate change. Loud on failure.",
	},
}

func TestMySQLGrowGateLaneRetryPostureRoster(t *testing.T) {
	cores := discoverMySQLGateTouchingCores(t)

	// Anti-vacuity: a walk that stops finding cores would pass on the empty
	// set, which is how the gate this pattern replaced failed.
	if len(cores) < 5 {
		names := make([]string, 0, len(cores))
		for n := range cores {
			names = append(names, n)
		}
		sort.Strings(names)
		t.Fatalf("found only %d gate-touching cores in package mysql (%v); the walk is not reaching the tree "+
			"and this roster would pass on an empty set", len(cores), names)
	}

	byPosture := map[mysqlRetryPosture][]string{}
	for name, calls := range cores {
		if _, plumbing := mysqlGatePlumbing[name]; plumbing {
			continue
		}
		declared, ok := mysqlLaneRetryPosture[name]
		if !ok {
			t.Errorf("%s touches the grow gate but declares no retry posture.\n\n"+
				"Every gate-touching write core must say what it does when the gate DECLINES to close "+
				"(migcore.GrowGateMaxQuiesceShare): a core that RETRIES is handed back to its own exponential "+
				"backoff, a core that does NOT is handed its next transient as a terminal error. The ceiling's "+
				"safety argument is stated per-core precisely because it does not hold for all of them.\n\n"+
				"Add it to mysqlLaneRetryPosture with the reason, add it to the table in "+
				"migcore.GrowGateMaxQuiesceShare's doc, or add it to mysqlGatePlumbing if it is gate wiring.", name)
			continue
		}
		derived := mysqlPostureNoRetry
		for _, h := range mysqlReplayHelpers {
			if calls[h] {
				derived = mysqlPostureRetries
				break
			}
		}
		if derived != declared.posture {
			t.Errorf("%s is declared %q but the code says %q — it %s reach a bounded-replay helper (%v).\n\n"+
				"Fix the declaration AND the roster table in migcore.GrowGateMaxQuiesceShare's doc; the ceiling's "+
				"safety argument reads off that table.",
				name, declared.posture, derived,
				map[bool]string{true: "does not", false: "does"}[derived == mysqlPostureNoRetry], mysqlReplayHelpers)
		}
		if strings.TrimSpace(declared.why) == "" {
			t.Errorf("%s declares a posture with no reason; an unexplained classification is indistinguishable "+
				"from a guess", name)
		}
		byPosture[declared.posture] = append(byPosture[declared.posture], name)
	}

	// The other direction: a roster entry for a core that no longer touches the
	// gate lies about the coverage.
	for name := range mysqlLaneRetryPosture {
		if _, ok := cores[name]; !ok {
			t.Errorf("mysqlLaneRetryPosture declares %q, which no longer touches the grow gate — remove it (and "+
				"the row in migcore.GrowGateMaxQuiesceShare's doc)", name)
		}
	}
	for name, why := range mysqlGatePlumbing {
		if strings.TrimSpace(why) == "" {
			t.Errorf("mysqlGatePlumbing exempts %q with no reason", name)
		}
		if _, ok := cores[name]; !ok {
			t.Errorf("mysqlGatePlumbing exempts %q, which no longer touches the grow gate — remove it", name)
		}
	}

	// BOTH postures must be populated. A derivation that collapsed to one
	// answer would agree with a roster edited to match it, and the whole
	// finding is that the two postures coexist.
	for _, p := range []mysqlRetryPosture{mysqlPostureRetries, mysqlPostureNoRetry} {
		if len(byPosture[p]) == 0 {
			t.Errorf("no MySQL core is classified %q; the roster exists because BOTH postures are present, so an "+
				"empty one means the derivation broke rather than that the engine changed", p)
		}
	}
}

// TestColdCopyReparentBackoffShape binds the environmental fact the item-138
// ceiling's safety argument rests on for the RETRYING cores: a released lane
// waits, and at the top of its ladder it waits ~30s between tries.
//
// migcore's grow-gate docs cite this test BY NAME — twice, in fact, and both
// citations predate the test: the run-level ceiling's doc named a check that
// was never written, and TestGrowGate_DeclinesToCloseOnceItsShareIsSpent
// pointed here for the backoff half. Writing it makes both true.
//
// This binds the SHAPE of sluice's own backoff, which is all that is bindable
// here. It says nothing about how fast the target recovers.
func TestColdCopyReparentBackoffShape(t *testing.T) {
	if got := coldCopyReparentBackoff(1); got != coldCopyReparentBackoffBaseVar {
		t.Errorf("attempt 1 backed off %v, want the base %v", got, coldCopyReparentBackoffBaseVar)
	}
	want := coldCopyReparentBackoffBaseVar
	for attempt := 1; attempt <= 20; attempt++ {
		got := coldCopyReparentBackoff(attempt)
		if want > coldCopyReparentBackoffCapVar {
			want = coldCopyReparentBackoffCapVar
		}
		if got != want {
			t.Fatalf("attempt %d backed off %v, want %v (exponential from %v, capped at %v)",
				attempt, got, want, coldCopyReparentBackoffBaseVar, coldCopyReparentBackoffCapVar)
		}
		want *= 2
	}
	// The load-bearing half: it SATURATES at the cap rather than growing
	// without bound, and the cap is the 30s the migcore argument names.
	if coldCopyReparentBackoffCapVar != 30*time.Second {
		t.Errorf("the reparent backoff cap is %v; migcore.GrowGateMaxQuiesceShare's safety argument says a "+
			"declined lane retries at roughly one try per 30s — update the argument or the cap",
			coldCopyReparentBackoffCapVar)
	}
	if got := coldCopyReparentBackoff(1000); got != coldCopyReparentBackoffCapVar {
		t.Errorf("attempt 1000 backed off %v, want the cap %v — a lane handed back by a declining gate must "+
			"keep waiting, not converge on hammering", got, coldCopyReparentBackoffCapVar)
	}
}

// discoverMySQLGateTouchingCores returns every function in the package whose
// body mentions the grow gate, keyed by "Receiver.Name" (or "Name" for a plain
// func), with its set of called names.
func discoverMySQLGateTouchingCores(t *testing.T) map[string]map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	fset := token.NewFileSet()
	out := map[string]map[string]bool{}

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
			if !ok || fn.Body == nil {
				continue
			}
			calls := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ce, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch sel := ce.Fun.(type) {
				case *ast.Ident:
					calls[sel.Name] = true
				case *ast.SelectorExpr:
					calls[sel.Sel.Name] = true
				}
				return true
			})
			touches := false
			for _, g := range mysqlGateCallNames {
				if calls[g] {
					touches = true
					break
				}
			}
			if !touches {
				continue
			}
			out[qualifiedMySQLFuncName(fset, fn)] = calls
		}
	}
	return out
}

// qualifiedMySQLFuncName renders "Receiver.Name" so the two engines'
// identically named float-repair cores stay distinguishable in a roster.
func qualifiedMySQLFuncName(fset *token.FileSet, fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	var b strings.Builder
	_ = printer.Fprint(&b, fset, fn.Recv.List[0].Type)
	return strings.TrimPrefix(b.String(), "*") + "." + fn.Name.Name
}
