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
// THREE cold-start-class entry points are graded, each by its own test, and
// the tests are named for the entry point they reach so none can be read as
// "the cold start" when it means one of them. The first cut of this file
// had one test named for the cold start that graded the single-database
// path only; audit 2026-09-15 A0915-ARCH-MEDIUM-2 found the other two
// reaching NONE of migrate's target-side preflights — the stopped-cold-start
// resume, and the multi-namespace fan-out, measured on plain PG dying
// mid-copy on a raw 0A000 where its siblings refused up front. The gate's
// own narrowness was the finding.
//
// The resume lane is graded against TWO references, because migrate is the
// wrong yardstick for it on its own: the resume continues a single-database
// cold start, and that entry point runs emit-side refusals (index, view,
// table-name fold) outside migrate's target phase, which a migrate-only
// roster could never ask about.
//
// SCOPE, stated so the names cannot be read wider than the truth:
//
//   - The references are migrate's target-preflight phase and, for the
//     resume lane only, the single-database cold start's own preflight
//     functions. Checks that live somewhere else in either are outside them.
//   - It matches by SYMBOL, so it cannot tell one call of a preflight from
//     another with different arguments — with one exception. A preflight
//     that takes an explicit RLSSide* argument (PreflightRLS) is also keyed
//     by that side (see discoverPreflightCallsByFunc), because every entry
//     point graded here calls it on BOTH sides and the bare symbol would
//     stay reached if the TARGET call were deleted or flipped to the source
//     side. That was not hypothetical: the multi-namespace fan-out reaches
//     a source PreflightRLS in coldStartReadOneDatabaseSchema, and before
//     the side key existed, deleting its target call left this gate green.
//   - It proves the call exists, not that it runs on every branch of the
//     function it lives in.
//
// Writing the first cut also corrected three guesses made while writing it:
// the cold start was assumed not to run the stale-backend clear, the
// target-side RLS preflight, or the ownership advisory. It runs all three.
// The only genuine gaps were the two shard refusals — which is the argument
// for deriving a roster from the AST instead of reasoning about one.

// targetPreflightReferenceFuncs is migrate's target-side preflight phase —
// the roster's universe.
// runSingleDatabase is listed alongside the preflight phase because a
// target-side preflight does not always live in phasePreflightTarget:
// PreflightDirectDDL has to run AFTER the ADR-0166 pre-create gate (a run
// that needs no DDL must not be refused for being unable to do DDL), and
// that gate is in runSingleDatabase. Anchoring the roster on one function
// made moving a call out of it silently shrink the universe — the gate went
// green the moment PreflightDirectDDL left phasePreflightTarget, which is
// the "gate narrower than its name" shape this file exists to avoid.
var targetPreflightReferenceFuncs = []string{
	"(*Migrator).phasePreflightTarget",
	"(*Migrator).runSingleDatabase",
}

// targetPreflightSingleDatabaseColdStartFuncs are the SINGLE-DATABASE sync
// cold-start functions that hold target-side preflights: the entry point
// and the gate cluster that has the RowWriter open. It is also the second
// reference the stopped-cold-start resume is graded against.
var targetPreflightSingleDatabaseColdStartFuncs = []string{
	"(*Streamer).coldStart",
	"(*Streamer).coldStartGatePreflight",
	"(*Streamer).coldStartOpenTargetWriters",
}

// targetPreflightSingleDatabaseColdStartExempt is fail-by-default: a migrate
// target-side preflight absent from the single-database cold start passes
// ONLY with an entry here, reusing the classes the cold-start roster
// defines. It is EMPTY, and that is the finding rather than an oversight:
// once the two shard refusals were wired, every preflight migrate runs
// against the target is also run by this entry point. The map stays so the
// next divergence has to be written down with a class and a tracker rather
// than appearing as a passing build.
var targetPreflightSingleDatabaseColdStartExempt = map[string]coldStartPreflightExemption{}

// targetPreflightMultiNamespaceFanOutFuncs are the multi-namespace fan-out's
// functions that hold its writers and readers. Only coldStartCopyOneDatabase
// opens a RowWriter (per database), so that is where every target-side
// preflight lives; coldStartReadOneDatabaseSchema holds the per-database
// source DSN, which the one SOURCE-side probe in migrate's pre-copy block
// (PreflightSourceBoolRanges) needs; coldStartMultiDatabase is listed for
// the once-per-run checks (the connection budget) so a preflight hoisted
// there stays in reach.
var targetPreflightMultiNamespaceFanOutFuncs = []string{
	"(*Streamer).coldStartMultiDatabase",
	"(*Streamer).coldStartReadOneDatabaseSchema",
	"(*Streamer).coldStartCopyOneDatabase",
}

// targetPreflightMultiNamespaceFanOutExempt is fail-by-default. Since
// A0915-ARCH-MEDIUM-2 the fan-out runs every migrate target-side preflight
// per database (its --schema-already-applied conditions are absent because
// the mode refuses that flag in validateMultiDatabaseStream); the one entry
// is ARCHITECTURAL.
var targetPreflightMultiNamespaceFanOutExempt = map[string]coldStartPreflightExemption{
	"PreflightPlanetScaleForeignKeys": {exemptArchitectural, "the fan-out STRIPS every foreign key before its " +
		"CreateConstraints phase (cross-database FK deferral, see coldStartCopyOneDatabase), so there is no FK add " +
		"for the control-plane FK-support check to guard; the preflight would only ever return its no-FKs INFO."},
}

// targetPreflightStoppedColdStartResumeFuncs are the stopped-cold-start
// resume's functions: the entry point and the helper that opens a RowWriter
// for the target-side checks.
var targetPreflightStoppedColdStartResumeFuncs = []string{
	"(*Streamer).resumeStoppedColdStart",
	"(*Streamer).resumeTargetPreflight",
}

// targetPreflightStoppedColdStartResumeExempt is fail-by-default against
// migrate's reference. The one entry is ARCHITECTURAL; every other migrate
// target-side preflight is run by resumeTargetPreflight (with the
// enumeration at its definition).
var targetPreflightStoppedColdStartResumeExempt = map[string]coldStartPreflightExemption{
	"PreflightSourceBoolRanges": {exemptArchitectural, resumeRunsNoCopySourceBoolRanges},
}

// targetPreflightStoppedColdStartResumeVsColdStartExempt is fail-by-default
// against the single-database cold start's own preflights — the entry point
// this lane continues. Every entry is ARCHITECTURAL and each names the
// mechanism that makes the preflight inapplicable to a run that copies
// nothing and creates no tables.
var targetPreflightStoppedColdStartResumeVsColdStartExempt = map[string]coldStartPreflightExemption{
	"PreflightSourceBoolRanges": {exemptArchitectural, resumeRunsNoCopySourceBoolRanges},
	"preflightCrossShardCollision": {exemptArchitectural, "refuses a multi-shard source merging into one target; " +
		"the lane admits only a source implementing ir.SnapshotAnchorVerifier (Postgres), which implements no " +
		"ir.ShardDiscoverer, so the preflight's own no-shards branch would return nil."},
	"preflightShardConsolidation": {exemptArchitectural, "judges whether a populated target may receive a COPY " +
		"from another shard; the resume copies nothing, and whether the rows on the target are the recorded copy's " +
		"is decided by the copy-shape gate (its `shard` aspect included) and the row floor."},
	"preflightColdStart": {exemptArchitectural, "refuses a COPY into a populated target; the resume REQUIRES a " +
		"populated target (the row floor refuses an emptied one) and copies nothing into it."},
}

// resumeRunsNoCopySourceBoolRanges is the one exemption both resume rosters
// share, spelled once so the two cannot drift.
const resumeRunsNoCopySourceBoolRanges = "a MySQL TINYINT(1) fail-fast for the COPY; this lane is " +
	"PostgreSQL-source-only (the ir.SnapshotAnchorVerifier gate at the top of resumeStoppedColdStart) and skips " +
	"the copy, so the probe would be a no-op on both counts."

// targetPreflightReference names one yardstick an entry point is graded
// against.
type targetPreflightReference struct {
	label string
	funcs []string
}

var (
	migrateTargetPreflightReference = targetPreflightReference{
		label: "migrate's target preflight phase",
		funcs: targetPreflightReferenceFuncs,
	}
	singleDatabaseColdStartPreflightReference = targetPreflightReference{
		label: "the single-database sync cold start",
		funcs: targetPreflightSingleDatabaseColdStartFuncs,
	}
)

// assertTargetPreflightRoster grades one cold-start-class entry point
// against a reference: every reference preflight is reached from
// reachedFuncs or carries an exemption with a class and a reason, the two
// sharded-target refusals are named explicitly, and every exemption names a
// live, unreached reference preflight.
func assertTargetPreflightRoster(
	t *testing.T,
	ref targetPreflightReference,
	entryPoint string,
	reachedFuncs []string,
	exempt map[string]coldStartPreflightExemption,
) {
	t.Helper()
	assertReachedFuncsAreConnected(t, entryPoint, reachedFuncs)
	calls := discoverPreflightCallsByFunc(t)

	reference := unionPreflightCalls(t, calls, ref.funcs)
	reached := unionPreflightCalls(t, calls, reachedFuncs)

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
			ref.funcs, len(reference), sortedPreflightKeys(reference))
	}
	var reachedReference int
	for sym := range reference {
		if _, ok := reached[sym]; ok {
			reachedReference++
		}
	}
	if reachedReference < 1 {
		t.Fatalf("anti-vacuity: expected >=1 preflight of %s reached from %v, found none (reached=%v) — "+
			"the AST matcher is likely broken", ref.label, reachedFuncs, sortedPreflightKeys(reached))
	}

	// The two sharded-target refusals are named explicitly, ABOVE the
	// generic forward check, because they are what this gate was built for
	// and a future exemption for either should have to be written against a
	// failure that says so by name rather than disappearing into a list.
	for _, sym := range []string{"PreflightShardPlacement", "PreflightShardKeyUpsert"} {
		if _, ok := reference[sym]; !ok {
			t.Errorf("%s is no longer called from %s %v — if it moved, move this roster's anchor with it; if it "+
				"was deleted, delete this check", sym, ref.label, ref.funcs)
			continue
		}
		if _, ok := reached[sym]; !ok {
			t.Errorf("%s refuses a sharded target in %s but is NOT called from the %s %v. That is "+
				"Bug 283 exactly: the same target, the same defect, and `sync start` pays for it after copying every "+
				"row instead of before the first one.", sym, ref.label, entryPoint, reachedFuncs)
		}
	}

	// The TARGET-side RLS refusal is named by its side-qualified key, for the
	// reason in the scope note: the bare symbol is also reached through the
	// source-side call. Asserting the key is in the reference is the
	// anti-vacuity half — if the side extraction ever stopped producing it,
	// the forward check below would pass on the bare symbol alone.
	const targetRLS = "PreflightRLS(RLSSideTarget)"
	if _, ok := reference[targetRLS]; !ok {
		t.Errorf("%s is not in %s %v — either the target-side RLS call moved (move this anchor) or the side-keyed "+
			"discovery broke, in which case this roster can no longer tell a target RLS call from a source one",
			targetRLS, ref.label, ref.funcs)
	} else if _, ok := reached[targetRLS]; !ok {
		t.Errorf("%s refuses a target whose tables force row-level security against a NOBYPASSRLS role in %s, but "+
			"the %s %v does not call it. Without it the copy dies mid-table on SQLSTATE 0A000 behind an "+
			"--exclude-table hint that fixes nothing (audit 2026-09-15 A0915-ARCH-MEDIUM-2).",
			targetRLS, ref.label, entryPoint, reachedFuncs)
	}

	// Forward: every reference preflight is reached by the entry point or
	// carries an exemption with a stated reason.
	var missing []string
	for _, sym := range sortedPreflightKeys(reference) {
		if _, ok := reached[sym]; ok {
			continue
		}
		ex, ok := exempt[sym]
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
		t.Fatalf("preflight(s) of %s NOT reached by the %s %v and not exempted:\n  %s\n\n"+
			"The copy-phase parity agreement (CLAUDE.md) says a refusal touching a shared pipeline phase applies to "+
			"BOTH entry points unless a written reason says otherwise. Either call it from one of the listed "+
			"functions, or add an exemption entry with a class and a reason.",
			ref.label, entryPoint, reachedFuncs, strings.Join(missing, "\n  "))
	}

	// Reverse: an exemption must name a live reference preflight the entry
	// point does NOT reach. A phantom is a typo or a deleted preflight; a
	// stale one would silently re-cover the next regression.
	for _, sym := range sortedPreflightKeys(exempt) {
		if _, ok := reference[sym]; !ok {
			t.Errorf("phantom exemption %q: no such preflight is called from %v — remove it or fix the name",
				sym, ref.funcs)
			continue
		}
		if _, ok := reached[sym]; ok {
			t.Errorf("stale exemption %q: the %s now calls it — remove the exemption so the gate holds the call", sym, entryPoint)
		}
	}
}

// TestTargetPreflightRoster_SingleDatabaseColdStartReachesMigrateSiblings
// grades the single-database `sync start` cold start — and only that one.
func TestTargetPreflightRoster_SingleDatabaseColdStartReachesMigrateSiblings(t *testing.T) {
	assertTargetPreflightRoster(t, migrateTargetPreflightReference, "single-database sync cold-start",
		targetPreflightSingleDatabaseColdStartFuncs, targetPreflightSingleDatabaseColdStartExempt)
}

// TestTargetPreflightRoster_MultiNamespaceFanOutReachesMigrateSiblings grades
// the `sync start --include-schema` / `--databases` fan-out, which opens its
// own writers per namespace and reached none of these until
// A0915-ARCH-MEDIUM-2. Its end-to-end pin on a real server is
// TestStreamer_MultiSchema_PG_TargetRLSRefusedBeforeTheCopy. Mutation-run
// (2026-09-15): deleting the fan-out's target-side PreflightRLS call fails
// here on the side-keyed check, and fails that integration pin too.
func TestTargetPreflightRoster_MultiNamespaceFanOutReachesMigrateSiblings(t *testing.T) {
	assertTargetPreflightRoster(t, migrateTargetPreflightReference, "multi-namespace sync cold-start fan-out",
		targetPreflightMultiNamespaceFanOutFuncs, targetPreflightMultiNamespaceFanOutExempt)
}

// TestTargetPreflightRoster_StoppedColdStartResumeReachesMigrateSiblings
// grades the A0909-STOP-1 resume lane against migrate, which skips the copy
// but still issues DDL and enters CDC — and ran zero target-side preflights
// until A0915-ARCH-MEDIUM-2.
func TestTargetPreflightRoster_StoppedColdStartResumeReachesMigrateSiblings(t *testing.T) {
	assertTargetPreflightRoster(t, migrateTargetPreflightReference, "stopped-cold-start resume",
		targetPreflightStoppedColdStartResumeFuncs, targetPreflightStoppedColdStartResumeExempt)
}

// TestTargetPreflightRoster_StoppedColdStartResumeReachesSingleDatabaseColdStartSiblings
// grades the same lane against the entry point it continues: every
// preflight the single-database cold start runs is either run again by the
// resume or written down as inapplicable to a run that copies nothing.
func TestTargetPreflightRoster_StoppedColdStartResumeReachesSingleDatabaseColdStartSiblings(t *testing.T) {
	assertTargetPreflightRoster(t, singleDatabaseColdStartPreflightReference, "stopped-cold-start resume",
		targetPreflightStoppedColdStartResumeFuncs, targetPreflightStoppedColdStartResumeVsColdStartExempt)
}

// assertReachedFuncsAreConnected closes the hole a helper-based reached set
// opens. The rosters prove a preflight is CALLED from one of the listed
// functions; listing a helper (resumeTargetPreflight, coldStartGatePreflight)
// makes every preflight inside it count as reached — including after the one
// call that runs the helper is deleted. So every listed function after the
// first (the entry point) must itself be called from another listed
// function. This still proves reach by the call graph, not by every branch.
func assertReachedFuncsAreConnected(t *testing.T, entryPoint string, reachedFuncs []string) {
	t.Helper()
	called := discoverCalledNamesByFunc(t)
	for _, helper := range reachedFuncs[1:] {
		bare := helper[strings.LastIndex(helper, ".")+1:]
		connected := false
		for _, caller := range reachedFuncs {
			if _, ok := called[caller][bare]; ok && caller != helper {
				connected = true
				break
			}
		}
		if !connected {
			t.Errorf("%s is listed as part of the %s, but no other listed function %v calls it — every preflight "+
				"inside it is counted as reached while nothing on this entry point runs it. Restore the call, or "+
				"list the function that makes it.", helper, entryPoint, reachedFuncs)
		}
	}
}
