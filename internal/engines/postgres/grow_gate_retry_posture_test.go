// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// What each grow-gate-touching write core does when the gate DECLINES to close
// — the roster [migcore.GrowGateMaxQuiesceShare]'s safety argument depends on
// (premise-naming sweep, 2026-08-05).
//
// # Why this gate exists
//
// The item-138 run-level quiesce ceiling justified itself with: "a gate that
// declines to close hands the target back to lanes attempting at roughly one
// try per 30s each — not a hammer", and named a test
// (TestGrowGate_DecliningToCloseLeavesTheLaneBackoffIntact) as the check
// binding that argument to the code. That test was never written, and the
// argument is FALSE for four of the nine write cores: they have no retry to be
// handed back to, so for them a decline means the next transient is terminal.
// The generalisation is what made this invisible — a roster would have shown
// it at a glance, which is why the fix is a roster and not a sentence.
//
// # What it reaches, stated so the name cannot be read as broader
//
// It reaches package `postgres` ONLY; [TestMySQLGrowGateLaneRetryPostureRoster]
// is the sibling and neither covers the other. Within the package it discovers
// every FUNCTION whose body mentions the gate — not every write lane. That is
// the right unit here: the question "does this core retry" is a property of the
// code that Awaits/Trips, and a public dispatcher that merely routes to two
// cores of different postures has no single answer. Lane COVERAGE (does every
// bulk-write lane reach the gate at all) is the separate, pre-existing
// [TestEveryPostgresBulkWriteLaneReachesTheGrowGate].
//
// LIMIT: this proves what a core's SOURCE reaches, not what it does at runtime
// on every branch. The behavioural halves live in row_writer_grow_gate_test.go
// (unit) and row_writer_grow_gate_integration_test.go (real PG).

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
	"time"
)

// pgRetryPosture is what a gate-touching write core does when the target
// throws a classified transient and no coordinated pause is available.
type pgRetryPosture string

const (
	// pgPostureRetries: the core re-drives the write through the bounded
	// replay helper, backing off exponentially, and is loud on exhaustion.
	pgPostureRetries pgRetryPosture = "retries"
	// pgPostureNoRetry: the core Awaits and Trips but cannot re-drive the
	// write, so the first transient it meets is terminal. Loud, never silent.
	pgPostureNoRetry pgRetryPosture = "no-retry"
)

// pgGateCallNames are the ways a function touches the gate. A function
// mentioning any of these is a gate-touching core and must be classified.
var pgGateCallNames = []string{"awaitGrowGate", "tripGrowGate", "quiesceAndReportTransient", "copyChunkWithRetry"}

// pgReplayHelpers are the calls that give a core a bounded REPLAY of the write
// it just lost. Reaching one is what "retries" means.
var pgReplayHelpers = []string{"copyChunkWithRetry"}

// pgGatePlumbing are the gate-touching functions that are the gate's own
// wiring rather than write cores, each with the reason. Excluded from the
// roster; listed so the exclusion is a claim someone can check rather than a
// silent filter.
//
// awaitGrowGate / tripGrowGate are absent deliberately: they call the ir.
// GrowGate interface directly (Await / Trip), not these names, so the walk
// never sees them as gate-touching. Naming them here would be a stale
// exemption, and the roster fails on those.
var pgGatePlumbing = map[string]string{
	"RowWriter.quiesceAndReportTransient": "the shared Await+Trip-without-replay helper the no-retry cores call",
	"RowWriter.copyChunkWithRetry":        "the bounded-replay helper itself; the thing the roster classifies cores BY",
}

// pgLaneRetryPosture is the DECLARED roster. Fail-by-default: a gate-touching
// core that is neither here nor in pgGatePlumbing fails the test, so a tenth
// core cannot appear unclassified. The reason strings are the roster's
// documentary half — each is a claim a reader can check against the code.
//
// Keep this in sync with the table in [migcore.GrowGateMaxQuiesceShare]'s doc:
// that comment is where the ceiling's safety argument is made, and the whole
// point of this gate is that the argument cannot silently outlive the roster.
var pgLaneRetryPosture = map[string]struct {
	posture pgRetryPosture
	why     string
}{
	"RowWriter.writeViaCopyChunked": {
		pgPostureRetries,
		"one atomic CopyFrom per buffered chunk; a rolled-back chunk wrote nothing so the buffered rows replay " +
			"clean. REFUSES loudly (errKeylessAmbiguousReplay) on a table with no PK / no NOT NULL UNIQUE, where " +
			"the committed-but-unacked branch would silently double the chunk.",
	},
	"RowWriter.writeViaBatchIdempotent": {
		pgPostureRetries,
		"ON CONFLICT upsert: a replayed batch converges rather than duplicating. The helper's keyless refusal is " +
			"unreachable here — effectiveUpsertKeyColumns already refused a keyless table up front.",
	},
	"RowWriter.writeViaBatch": {
		pgPostureNoRetry,
		"plain multi-row INSERT. A dropped connection leaves an AMBIGUOUS ack and there is no conflict target to " +
			"absorb a duplicate, so re-driving is wrong. Returns the wrapped driver error — loud, never silent.",
	},
	"RowWriter.ImportRawCopy": {
		pgPostureNoRetry,
		"raw COPY FROM STDIN over a one-shot io.Reader (an exporter pipe / a partially-consumed chunk reader): " +
			"there is nothing to replay. Returns an explicit 'no resume point, re-run this table' refusal.",
	},
	"pgFloatBatchExecer.ExecBatch": {
		pgPostureNoRetry,
		"VStream float-repair UPDATE. The statement IS idempotent (keyed on the PK) so a replay would be safe; " +
			"the Await+Trip parity landed without one and adding it is a separate change. Loud on failure.",
	},
}

func TestPostgresGrowGateLaneRetryPostureRoster(t *testing.T) {
	cores := discoverPGGateTouchingCores(t)

	// Anti-vacuity: a walk that stops finding cores would pass on the empty
	// set, which is how the gate this pattern replaced failed.
	if len(cores) < 5 {
		names := make([]string, 0, len(cores))
		for n := range cores {
			names = append(names, n)
		}
		sort.Strings(names)
		t.Fatalf("found only %d gate-touching cores in package postgres (%v); the walk is not reaching the tree "+
			"and this roster would pass on an empty set", len(cores), names)
	}

	byPosture := map[pgRetryPosture][]string{}
	for name, calls := range cores {
		if _, plumbing := pgGatePlumbing[name]; plumbing {
			continue
		}
		declared, ok := pgLaneRetryPosture[name]
		if !ok {
			t.Errorf("%s touches the grow gate but declares no retry posture.\n\n"+
				"Every gate-touching write core must say what it does when the gate DECLINES to close "+
				"(migcore.GrowGateMaxQuiesceShare): a core that RETRIES is handed back to its own exponential "+
				"backoff, a core that does NOT is handed its next transient as a terminal error. The ceiling's "+
				"safety argument is stated per-core precisely because it does not hold for all of them.\n\n"+
				"Add it to pgLaneRetryPosture with the reason, add it to the table in "+
				"migcore.GrowGateMaxQuiesceShare's doc, or add it to pgGatePlumbing if it is gate wiring.", name)
			continue
		}
		derived := pgPostureNoRetry
		for _, h := range pgReplayHelpers {
			if calls[h] {
				derived = pgPostureRetries
				break
			}
		}
		if derived != declared.posture {
			t.Errorf("%s is declared %q but the code says %q — it %s reach a bounded-replay helper (%v).\n\n"+
				"Fix the declaration AND the roster table in migcore.GrowGateMaxQuiesceShare's doc; the ceiling's "+
				"safety argument reads off that table.",
				name, declared.posture, derived,
				map[bool]string{true: "does not", false: "does"}[derived == pgPostureNoRetry], pgReplayHelpers)
		}
		if strings.TrimSpace(declared.why) == "" {
			t.Errorf("%s declares a posture with no reason; an unexplained classification is indistinguishable "+
				"from a guess", name)
		}
		byPosture[declared.posture] = append(byPosture[declared.posture], name)
	}

	// The other direction: a roster entry for a core that no longer touches the
	// gate lies about the coverage.
	for name := range pgLaneRetryPosture {
		if _, ok := cores[name]; !ok {
			t.Errorf("pgLaneRetryPosture declares %q, which no longer touches the grow gate — remove it (and the "+
				"row in migcore.GrowGateMaxQuiesceShare's doc)", name)
		}
	}
	for name, why := range pgGatePlumbing {
		if strings.TrimSpace(why) == "" {
			t.Errorf("pgGatePlumbing exempts %q with no reason", name)
		}
		if _, ok := cores[name]; !ok {
			t.Errorf("pgGatePlumbing exempts %q, which no longer touches the grow gate — remove it", name)
		}
	}

	// BOTH postures must be populated. A derivation that collapsed to one
	// answer would agree with a roster that had been edited to match it, and
	// the whole finding is that the two postures coexist.
	for _, p := range []pgRetryPosture{pgPostureRetries, pgPostureNoRetry} {
		if len(byPosture[p]) == 0 {
			t.Errorf("no Postgres core is classified %q; the roster exists because BOTH postures are present, so "+
				"an empty one means the derivation broke rather than that the engine changed", p)
		}
	}
}

// TestPGCopyReparentBackoffShape binds the environmental fact the item-138
// ceiling's safety argument rests on for the RETRYING cores: a released lane
// waits, and at the top of its ladder it waits ~30s between tries.
//
// The migcore doc cites this test by name. It was written because the doc's
// first cut cited two tests, neither of which existed — the reason the whole
// premise-naming rule exists (CLAUDE.md: a safety argument that cites an
// environmental fact owes that fact a check).
//
// This binds the SHAPE of sluice's own backoff, which is all that is bindable
// here. It says nothing about how fast the target recovers.
func TestPGCopyReparentBackoffShape(t *testing.T) {
	if got := pgCopyReparentBackoff(1); got != pgCopyReparentBackoffBaseVar {
		t.Errorf("attempt 1 backed off %v, want the base %v", got, pgCopyReparentBackoffBaseVar)
	}
	// Doubling until the cap.
	want := pgCopyReparentBackoffBaseVar
	for attempt := 1; attempt <= 20; attempt++ {
		got := pgCopyReparentBackoff(attempt)
		if want > pgCopyReparentBackoffCapVar {
			want = pgCopyReparentBackoffCapVar
		}
		if got != want {
			t.Fatalf("attempt %d backed off %v, want %v (exponential from %v, capped at %v)",
				attempt, got, want, pgCopyReparentBackoffBaseVar, pgCopyReparentBackoffCapVar)
		}
		want *= 2
	}
	// The load-bearing half: it SATURATES at the cap rather than growing
	// without bound, and the cap is the 30s the migcore argument names.
	if pgCopyReparentBackoffCapVar != 30*time.Second {
		t.Errorf("the reparent backoff cap is %v; migcore.GrowGateMaxQuiesceShare's safety argument says a "+
			"declined lane retries at roughly one try per 30s — update the argument or the cap",
			pgCopyReparentBackoffCapVar)
	}
	if got := pgCopyReparentBackoff(1000); got != pgCopyReparentBackoffCapVar {
		t.Errorf("attempt 1000 backed off %v, want the cap %v — a lane handed back by a declining gate must "+
			"keep waiting, not converge on hammering", got, pgCopyReparentBackoffCapVar)
	}
}

// discoverPGGateTouchingCores returns every function in the package whose body
// mentions the grow gate, keyed by "Receiver.Name" (or "Name" for a plain
// func), with its set of called names.
func discoverPGGateTouchingCores(t *testing.T) map[string]map[string]bool {
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
			for _, g := range pgGateCallNames {
				if calls[g] {
					touches = true
					break
				}
			}
			if !touches {
				continue
			}
			out[qualifiedFuncName(fset, fn)] = calls
		}
	}
	return out
}

// qualifiedFuncName renders "Receiver.Name" so the two engines' identically
// named float-repair cores stay distinguishable in a roster.
func qualifiedFuncName(fset *token.FileSet, fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	var b strings.Builder
	_ = printer.Fprint(&b, fset, fn.Recv.List[0].Type)
	return strings.TrimPrefix(b.String(), "*") + "." + fn.Name.Name
}
