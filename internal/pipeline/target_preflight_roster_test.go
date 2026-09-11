// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"
)

// The TARGET-side sibling of the cold-start preflight roster in
// coldstart_preflight_roster_test.go, and it exists because that file's own
// scope note asserted the thing this gate had to disprove:
//
//	"so are the target-side and emit-side preflights, which the fan-out
//	 already shares with its siblings through migcore"
//
// Sharing a migcore helper is not sharing a CALL. Both sharded-target
// refusals shipped in v0.150.0 wired only into migrate's
// [(*Migrator).phasePreflightTarget]; the post-release regression cycle
// caught it (Bug 283): `migrate` refused a shard-key/upsert-key mismatch
// with the table not created, while `sync start` against the same target
// copied all 60 rows, reached CDC, and died on the first change — an INSERT
// was enough. Same defect, same target, one command paying for it after the
// copy instead of before. A written invariant nobody checks is
// indistinguishable from one that holds, and this one was written inside a
// gate, which is the shape that stops anyone from looking.
//
// SCOPE, stated so the name cannot be read wider than the truth:
//
//   - The roster is migrate's target-preflight phase. Target-side checks
//     that live somewhere else in migrate are outside it.
//   - It matches by SYMBOL, so it cannot tell one call of a preflight from
//     another with different arguments. PreflightRLS takes an explicit side
//     and the cold start calls it on BOTH (source at
//     coldStartReadSourceSchema, target at coldStartOpenTargetWriters), so
//     the roster is right about it today — but it would stay green if the
//     target call were changed to the source side. A side-sensitive check is
//     a separate gate; this one is about call reach.
//   - It proves the call exists, not that it runs on every branch of the
//     function it lives in.
//
// Writing it also corrected three guesses made while writing it: the cold
// start was assumed not to run the stale-backend clear, the target-side RLS
// preflight, or the ownership advisory. It runs all three. The only genuine
// gaps were the two shard refusals — which is the argument for deriving a
// roster from the AST instead of reasoning about one.

// targetPreflightReferenceFuncs is migrate's target-side preflight phase —
// the roster's universe.
var targetPreflightReferenceFuncs = []string{
	"(*Migrator).phasePreflightTarget",
}

// targetPreflightColdStartFuncs are the sync cold-start functions that hold
// target-side preflights: the entry point and the gate cluster that has the
// RowWriter open.
var targetPreflightColdStartFuncs = []string{
	"(*Streamer).coldStart",
	"(*Streamer).coldStartGatePreflight",
	"(*Streamer).coldStartOpenTargetWriters",
}

// targetPreflightColdStartExempt is fail-by-default: a migrate target-side
// preflight absent from the cold start passes ONLY with an entry here,
// reusing the classes the cold-start roster defines.
// It is EMPTY, and that is the finding rather than an oversight: once the
// two shard refusals were wired, every preflight migrate runs against the
// target is also run by the sync cold-start. The map stays so the next
// divergence has to be written down with a class and a tracker rather than
// appearing as a passing build.
var targetPreflightColdStartExempt = map[string]coldStartPreflightExemption{}

func TestTargetPreflightRoster_ColdStartReachesMigrateSiblings(t *testing.T) {
	calls := discoverPreflightCallsByFunc(t)

	reference := unionPreflightCalls(t, calls, targetPreflightReferenceFuncs)
	reached := unionPreflightCalls(t, calls, targetPreflightColdStartFuncs)

	// Anti-vacuity floors, set where a single deletion reaches the check
	// being graded rather than tripping the floor first — the step-5 trap
	// the cold-start roster documents, and one this gate walked into on its
	// first mutation run. migrate's target phase runs FIVE preflights today,
	// and a floor at five meant deleting the migrate-side
	// PreflightShardPlacement call failed here, saying "the AST matcher is
	// likely broken", instead of at the named check that says the roster
	// anchor moved. Three leaves room for one deletion to reach the real
	// check while still catching a matcher that has stopped finding things.
	if len(reference) < 3 {
		t.Fatalf("anti-vacuity: expected >=3 preflight* symbols in %v, found %d: %v — the AST matcher is likely broken",
			targetPreflightReferenceFuncs, len(reference), sortedPreflightKeys(reference))
	}
	var reachedReference int
	for sym := range reference {
		if _, ok := reached[sym]; ok {
			reachedReference++
		}
	}
	if reachedReference < 1 {
		t.Fatalf("anti-vacuity: expected >=1 migrate target preflight reached from %v, found none (reached=%v) — "+
			"the AST matcher is likely broken", targetPreflightColdStartFuncs, sortedPreflightKeys(reached))
	}

	// The two sharded-target refusals are named explicitly, ABOVE the
	// generic forward check, because they are what this gate was built for
	// and a future exemption for either should have to be written against a
	// failure that says so by name rather than disappearing into a list.
	for _, sym := range []string{"PreflightShardPlacement", "PreflightShardKeyUpsert"} {
		if _, ok := reference[sym]; !ok {
			t.Errorf("%s is no longer called from migrate's target preflight phase — if it moved, move this roster's "+
				"anchor with it; if it was deleted, delete this check", sym)
			continue
		}
		if _, ok := reached[sym]; !ok {
			t.Errorf("%s refuses a sharded target on `migrate` but is NOT called from the sync cold-start %v. That is "+
				"Bug 283 exactly: the same target, the same defect, and `sync start` pays for it after copying every "+
				"row instead of before the first one. Wire it into coldStartGatePreflight.", sym, targetPreflightColdStartFuncs)
		}
	}

	// Forward: every migrate target-side preflight is reached by the cold
	// start or carries an exemption with a stated reason.
	var missing []string
	for _, sym := range sortedPreflightKeys(reference) {
		if _, ok := reached[sym]; ok {
			continue
		}
		ex, ok := targetPreflightColdStartExempt[sym]
		if !ok {
			missing = append(missing, sym)
			continue
		}
		switch ex.class {
		case exemptArchitectural:
			if strings.TrimSpace(ex.reason) == "" {
				t.Errorf("architectural exemption for %s has no reason; the mechanism is the load-bearing part", sym)
			}
		case exemptFiledGap:
			if !strings.Contains(ex.reason, "audit ") && !strings.Contains(ex.reason, "Bug ") && !strings.Contains(ex.reason, "roadmap item") {
				t.Errorf("filed-gap exemption for %s cites no tracker (want \"audit <date> <id>\", \"Bug N\", or \"roadmap item N\"): %q", sym, ex.reason)
			}
		default:
			t.Errorf("exemption for %s has an unknown class %d", sym, ex.class)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("migrate target-side preflight(s) NOT reached by the sync cold-start %v and not exempted:\n  %s\n\n"+
			"The copy-phase parity agreement (CLAUDE.md) says a refusal touching a shared pipeline phase applies to "+
			"BOTH entry points unless a written reason says otherwise. Either call it from coldStartGatePreflight, or "+
			"add a targetPreflightColdStartExempt entry with a class and a reason.",
			targetPreflightColdStartFuncs, strings.Join(missing, "\n  "))
	}

	// Reverse: an exemption must name a live reference preflight the cold
	// start does NOT reach. A phantom is a typo or a deleted preflight; a
	// stale one would silently re-cover the next regression.
	for _, sym := range sortedPreflightKeys(targetPreflightColdStartExempt) {
		if _, ok := reference[sym]; !ok {
			t.Errorf("phantom exemption %q: no such preflight is called from %v — remove it or fix the name",
				sym, targetPreflightReferenceFuncs)
			continue
		}
		if _, ok := reached[sym]; ok {
			t.Errorf("stale exemption %q: the cold start now calls it — remove the exemption so the gate holds the call", sym)
		}
	}
}
