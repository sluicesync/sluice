// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the leftover-dev-branch ADOPTION path (roadmap item 108,
// leg_adopt.go).
//
// The verdict is dispatched on a type FAMILY — PlanetScale's twenty-value
// `deployment_state` enum — so the matrix test below exercises EVERY value
// rather than a representative one, and carries an anti-vacuity floor that
// fails if the enum grows a member the classifier was never asked about
// (the Bug 74 lesson at the state-machine layer: a state sluice does not
// classify is either an infinite wait or a wrong delete).
//
// The end-to-end pins then drive the real dispatch through the fakePS
// control plane, because the crux of the item is not the verdict but what
// happens after it: an adopted deploy must ride the SAME poller with NO
// second branch, NO second deploy request, and NO DDL re-execution, and a
// refusal must leave the operator's branch untouched.

package expandcontract

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/climsggate"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ---- the state matrix ----

// deployableShapes is the SHAPE axis of the state matrix: PlanetScale's
// `deployable` flag, in each place a response can carry it. The first cut
// of the matrix below fixed this axis at one guessed value per state
// ("only `ready` is deployable"), and that guess was wrong — the live API
// reports deployable=true on a request it is actively deploying (Bug 231),
// which is precisely the row the fixture asserted could not exist. So the
// axis is now enumerated instead of assumed, and the verdict must be
// INVARIANT across it for every state [drStates] documents.
var deployableShapes = map[string]func(*api.DeployRequest){
	"absent":                   func(*api.DeployRequest) {},
	"nested deployment object": func(dr *api.DeployRequest) { dr.Deployment.Deployable = true },
	"top-level (tolerated)":    func(dr *api.DeployRequest) { dr.Deployable = true },
	"both (belt and braces)":   func(dr *api.DeployRequest) { dr.Deployable, dr.Deployment.Deployable = true, true },
}

// TestClassifyAdoption_CoversEveryDeploymentState grades every documented
// `deployment_state` against the adopt/refuse verdict, and asserts the
// table's key set is EXACTLY [drStates]' — so adding a state to the enum
// without deciding whether it is adoptable fails here instead of shipping
// as an unbounded wait.
//
// Every state is graded at every [deployableShapes] value, because the
// verdict for a DOCUMENTED state must not depend on that flag at all.
func TestClassifyAdoption_CoversEveryDeploymentState(t *testing.T) {
	want := map[string]struct {
		verdict adoptVerdict
	}{
		// Before the deploy call — never adopted: sluice cannot re-make
		// the pre-deploy freshness/blast-radius promises for a branch it
		// did not provision this run, and nothing is building yet.
		"pending": {adoptRefuseNeverDeployed},
		"ready":   {adoptRefuseNeverDeployed},
		// no_changes is pre-deploy too, but its cause is different (the
		// DDL is already on production), so it gets its own advice.
		"no_changes": {adoptRefuseAlreadyDeployed},

		// Issued and running — adopted onto the shared poller. This row
		// is the whole item: `in_progress` at 78 % was the live case a
		// re-run refused.
		"queued":                     {adoptJoin},
		"submitting":                 {adoptJoin},
		"in_progress":                {adoptJoin},
		"in_progress_vschema":        {adoptJoin},
		"in_progress_cutover":        {adoptJoin},
		"in_progress_cancel":         {adoptJoin},
		"in_progress_revert":         {adoptJoin},
		"in_progress_revert_vschema": {adoptJoin},

		// Parked on a person — adopted, because waitDeployed owns that
		// gate already (it downgrades the state on an auto_cutover
		// deployment and stops loudly on a real one).
		"pending_cutover": {adoptJoin},

		// Terminal, applied — finalize the revert window and move on.
		"complete":                {adoptFinalize},
		"complete_pending_revert": {adoptFinalize},

		// Terminal, not applied — refuse; here "delete the branch" is
		// genuinely the right advice.
		"error":                 {adoptRefuseFailed},
		"complete_error":        {adoptRefuseFailed},
		"cancelled":             {adoptRefuseFailed},
		"complete_cancel":       {adoptRefuseFailed},
		"complete_revert":       {adoptRefuseFailed},
		"complete_revert_error": {adoptRefuseFailed},
	}

	// Anti-vacuity: the graded set must be the enum, both directions.
	for state := range drStates {
		if _, ok := want[state]; !ok {
			t.Errorf("deployment_state %q is in drStates but ungraded here — decide whether a leftover deploy request in that state is adoptable", state)
		}
	}
	for state := range want {
		if _, ok := drStates[state]; !ok {
			t.Errorf("deployment_state %q is graded here but absent from drStates — the two enumerations have drifted", state)
		}
	}
	if len(want) < 20 {
		t.Fatalf("graded only %d states; PlanetScale documents 20 — this matrix has gone vacuous", len(want))
	}
	if len(deployableShapes) < 4 {
		t.Fatalf("graded only %d deployable shapes; want the flag absent, nested, top-level and both", len(deployableShapes))
	}

	for state, exp := range want {
		for shape, apply := range deployableShapes {
			dr := &api.DeployRequest{
				Number: 7, State: "open", DeploymentState: state,
				Deployment: api.Deployment{State: state},
			}
			apply(dr)
			if got := classifyAdoption(dr); got != exp.verdict {
				t.Errorf("classifyAdoption(%q) with deployable %s = %v; want %v — for a documented state the verdict must not depend on `deployable` (Bug 231)",
					state, shape, got, exp.verdict)
			}
		}
	}
}

// TestClassifyAdoption_MidDeployLeftoverIsAdopted is Bug 231 in the exact
// shape the live API served it, kept as its own named pin rather than one
// cell of the matrix above — because a matrix that grades a family can be
// green while carrying a wrong assumption about ONE axis of it, and that is
// how this shipped.
//
// Ground truth (v0.116.0 regression cycle, live PlanetScale, 2026-08-07):
// deploy request #3 read state "open", deployment_state "in_progress",
// deployable=true. sluice refused it at 20:31:07, strictly between
// PlanetScale's own started_at 20:31:03.962Z and deployed_at 20:31:11.393Z
// — so the deployment was genuinely running, and the refusal told the
// operator to delete the branch it was running on.
func TestClassifyAdoption_MidDeployLeftoverIsAdopted(t *testing.T) {
	live := &api.DeployRequest{
		Number: 3, State: "open", DeploymentState: "in_progress",
		Deployment: api.Deployment{State: "in_progress", Deployable: true},
	}
	if got := classifyAdoption(live); got != adoptJoin {
		t.Fatalf("classifyAdoption(mid-deploy, deployable=true) = %v; want adoptJoin — this is the one shape adoption exists for", got)
	}
}

// TestClassifyAdoption_ClosedWithoutDeployingRefuses pins the asymmetry
// that makes `state` unusable on its own: PlanetScale closes a deploy
// request when it DEPLOYS as well as when it is abandoned, so a closed
// request is a failure only when its deployment_state is not a success.
func TestClassifyAdoption_ClosedWithoutDeployingRefuses(t *testing.T) {
	closedMidFlight := &api.DeployRequest{
		Number: 1, State: "closed", DeploymentState: "some_future_state",
	}
	if got := classifyAdoption(closedMidFlight); got != adoptRefuseFailed {
		t.Errorf("closed request in an unrecognized state = %v; want adoptRefuseFailed", got)
	}
	closedAfterDeploy := &api.DeployRequest{
		Number: 1, State: "closed", DeploymentState: "complete",
	}
	if got := classifyAdoption(closedAfterDeploy); got != adoptFinalize {
		t.Errorf("closed-because-deployed request = %v; want adoptFinalize — `state == closed` alone means nothing", got)
	}
}

// TestClassifyAdoption_DeployableIsConsultedOnlyForAnUnrecognizedState
// pins where the `deployable` flag survives after Bug 231, and where it
// does not. For a state sluice has never seen the table has nothing to
// say, and "PlanetScale would still take a Deploy call" is the only signal
// there is — enough to refuse rather than hand a poller a request it might
// wait on forever, and NOT enough to claim nothing is running (which is
// why that refusal is its own verdict, with its own text).
func TestClassifyAdoption_DeployableIsConsultedOnlyForAnUnrecognizedState(t *testing.T) {
	unknownDeployable := &api.DeployRequest{
		Number: 1, State: "open", DeploymentState: "some_future_predeploy_state",
		Deployment: api.Deployment{Deployable: true},
	}
	if got := classifyAdoption(unknownDeployable); got != adoptRefuseUnrecognizedState {
		t.Errorf("unknown-but-deployable state = %v; want adoptRefuseUnrecognizedState", got)
	}
	unknownRunning := &api.DeployRequest{
		Number: 1, State: "open", DeploymentState: "some_future_running_state",
	}
	if got := classifyAdoption(unknownRunning); got != adoptJoin {
		t.Errorf("unknown-and-not-deployable state = %v; want adoptJoin (drStates' documented tolerance)", got)
	}
	for state := range notYetDeployedStates {
		if _, ok := drStates[state]; !ok {
			t.Errorf("notYetDeployedStates names %q, which is not a documented deployment_state", state)
		}
	}
	if notYetDeployedStates["queued"] {
		t.Error("`queued` is listed as not-yet-deployed; PlanetScale queues a deployment AFTER it is requested (ADR-0148's observed walk), so listing it here would refuse every queued build")
	}
}

// ---- end-to-end adoption ----

// TestIndexFallback_AdoptsAnInFlightDeployRequest is the item's crux, in
// the shape it was observed live: a `--resume` finds its own dev branch
// with a deploy request 78 % of the way through a VReplication index
// build. It must JOIN that build — same poller, same narration — and must
// not create a second branch, open a second deploy request, or re-execute
// the DDL.
func TestIndexFallback_AdoptsAnInFlightDeployRequest(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "in_progress", true)
	// The adopted request keeps deploying, then lands.
	ps.drs[1].states = []string{"in_progress", "complete_pending_revert"}
	ps.opPercents = []int{78}

	f, rec := newTestIndexFallback(t, ps)
	if err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub()); err != nil {
		t.Fatalf("BuildIndexDDL = %v; want the in-flight deploy request adopted and waited out", err)
	}

	if len(rec.ddls) != 0 {
		t.Errorf("DDL re-executed on adoption: %v — the branch already carries it", rec.ddls)
	}
	if ps.nextDR != 2 {
		t.Errorf("nextDR = %d; want 2 (adoption opens NO second deploy request)", ps.nextDR)
	}
	if !ps.drs[1].skipRevert {
		t.Error("adopted deploy request was not finalized through skip-revert")
	}
	if ps.diffFetches != 0 {
		t.Errorf("diff fetched %d time(s) on the adopt path; the pre-deploy blast-radius gate has nothing left to prevent once the deploy is issued", ps.diffFetches)
	}
	if ps.deployCalls != 0 {
		t.Errorf("deploy called %d time(s) on an already-deploying request", ps.deployCalls)
	}
	if len(ps.deleted) != 1 || ps.deleted[0] != leftover {
		t.Errorf("deleted = %v; want the adopted dev branch torn down exactly once", ps.deleted)
	}
}

// TestIndexFallback_AdoptionNarratesTheProgressItJoined pins the operator-
// facing half: the adoption line says which deploy request it joined and
// how far along PlanetScale says it is, so a re-run that "does nothing for
// an hour" is legible.
func TestIndexFallback_AdoptionNarratesTheProgressItJoined(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "in_progress", true)
	ps.drs[1].states = []string{"in_progress", "complete"}
	ps.opPercents = []int{78}
	ps.opETASeconds = 3099

	out := &bytes.Buffer{}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.out = out
	if _, err := r.adoptLeftoverBranch(context.Background(), leftover, cleanup); err != nil {
		t.Fatalf("adoptLeftoverBranch = %v; want the in-flight request adopted", err)
	}
	for _, want := range []string{"adopting deploy request #1", "still deploying", "in_progress", "78% complete"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("adoption narration %q missing %q", out.String(), want)
		}
	}
}

// TestIndexFallback_AdoptsAnAlreadyDeployedDeployRequest pins the other
// adoptable half: the earlier run died after the deploy landed, so there
// is nothing to wait for — finalize the revert window and tear the branch
// down, exactly as the fresh path does at the same point.
func TestIndexFallback_AdoptsAnAlreadyDeployedDeployRequest(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "closed", "complete_pending_revert", true)

	f, rec := newTestIndexFallback(t, ps)
	if err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub()); err != nil {
		t.Fatalf("BuildIndexDDL = %v; want the completed deploy request adopted and finalized", err)
	}
	if len(rec.ddls) != 0 {
		t.Errorf("DDL re-executed: %v", rec.ddls)
	}
	if !ps.drs[1].skipRevert {
		t.Error("the adopted complete_pending_revert deployment was not finalized")
	}
	if len(ps.deleted) != 1 || ps.deleted[0] != leftover {
		t.Errorf("deleted = %v; want the adopted dev branch torn down", ps.deleted)
	}
}

// TestIndexFallback_AdoptedDeployFailureIsCodedAndCleansUp pins that an
// adopted build which then FAILS reports the same coded failure a
// self-started one does, and its branch is cleaned up (nothing is running
// on it any more).
func TestIndexFallback_AdoptedDeployFailureIsCodedAndCleansUp(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "in_progress", true)
	ps.drs[1].states = []string{"in_progress", "complete_error"}

	f, _ := newTestIndexFallback(t, ps)
	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if len(ps.deleted) != 1 || ps.deleted[0] != leftover {
		t.Errorf("deleted = %v; want the adopted branch cleaned up after a terminal failure", ps.deleted)
	}
}

// TestIndexFallback_AdoptedDeployBoundedTimeoutKeepsTheBranch pins the
// keep-the-branch rule on the adopt path: a bound hit while the adopted
// deploy is healthy is the non-failure code, and the branch the running
// deployment depends on is not even asked to be deleted.
func TestIndexFallback_AdoptedDeployBoundedTimeoutKeepsTheBranch(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "in_progress", true)

	f, _ := newTestIndexFallback(t, ps)
	f.DeployTimeout = 50 * time.Millisecond
	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDeployRequestIncomplete)
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE attempted %d time(s) while the adopted deployment runs", ps.deleteAttempts)
	}
}

// ---- the refusals ----

// nothingCanBeRunning reports whether a `deployment_state` ESTABLISHES
// that no deployment is in flight — the only condition under which a
// refusal may tell an operator the dev branch is safe to delete. It is
// derived from [drStates] + [notYetDeployedStates], not from a hand-kept
// list, so a state added to the enum is graded the moment it lands.
func nothingCanBeRunning(state string) bool {
	class, documented := drStates[state]
	if !documented {
		return false
	}
	return notYetDeployedStates[state] || state == "no_changes" || class == drFailure
}

// TestAdoptRefusalNeverClaimsNothingIsDeployingAboutARunningState is the
// durable gate for the SECOND half of Bug 231 — the half that was worse
// than the refusal. The shipped refusal printed `deployment_state
// "in_progress"` and, in the same sentence, "Nothing is deploying, so the
// branch is safe to delete"; an operator who believed it would have run a
// delete against a branch with a live deployment on it (PlanetScale
// refuses that delete, which is the platform covering for sluice).
//
// The rule it enforces: a refusal may make the delete-is-safe CLAIM only
// from a state that establishes it. Everything else — a running state, a
// human gate, a state sluice does not recognize — must refuse without it.
//
// SCOPE, stated so the name cannot be read as broader than the truth: this
// grades the refusals [legRunner.adoptRefusalFor] renders on the ADOPTION
// path only. The legRunner's other messages (the review/deploy timeouts,
// which legitimately name a branch delete for LATER) are not graded here,
// and the discovery-time refusals in [legRunner.leftoverDeployRequest] —
// truncated list, wrong into_branch, two requests, no request — carry
// their own pins above.
func TestAdoptRefusalNeverClaimsNothingIsDeployingAboutARunningState(t *testing.T) {
	r := &legRunner{
		org: "o", database: "d", branch: "main",
		name: "gate-test", errPrefix: "gate-test",
		alreadyDeployedAdvice: "close the DR",
		rerunAdvice:           "run it again",
	}

	// Undocumented states are graded alongside the enum: they are exactly
	// where sluice has no evidence, and therefore where an unearned claim
	// is likeliest to slip in.
	states := make([]string, 0, len(drStates)+2)
	for state := range drStates {
		states = append(states, state)
	}
	states = append(states, "some_future_running_state", "some_future_predeploy_state")

	var graded, claimed, withheld int
	for _, state := range states {
		for shape, apply := range deployableShapes {
			for _, drState := range []string{"open", "closed"} {
				dr := &api.DeployRequest{
					Number: 9, Branch: "sluice-x", IntoBranch: "main",
					State: drState, DeploymentState: state,
					Deployment: api.Deployment{State: state},
					HTMLURL:    "https://app.planetscale.com/o/d/deploy-requests/9",
				}
				apply(dr)

				err := r.adoptRefusalFor(classifyAdoption(dr), "sluice-x", dr)
				if err == nil {
					continue // adopted — no operator-facing claim to grade
				}
				graded++

				ce, ok := sluicecode.FromError(err)
				if !ok {
					t.Fatalf("adoption refusal for %q is not coded: %v", state, err)
				}
				// The claim is rendered in exactly one place
				// ([legRunner.safeToDeleteAdvice]), so grading it is a
				// substring match on that one sentence rather than a
				// heuristic over prose — which also means a refusal cannot
				// smuggle it in by rewording.
				text := err.Error() + " || " + ce.Hint
				if !strings.Contains(text, "Nothing is deploying, so the branch is safe to delete") {
					withheld++
					continue
				}
				claimed++
				if !nothingCanBeRunning(state) {
					t.Errorf("the refusal for deployment_state %q (dr state %q, deployable %s) tells the operator the branch is safe to delete, "+
						"but that state does not establish it — a message must not assert a condition the evidence it just printed denies:\n%s\nhint: %s",
						state, drState, shape, err, ce.Hint)
				}
			}
		}
	}

	// Anti-vacuity, three floors: the loop must actually refuse things, at
	// least one refusal must MAKE the claim (or the predicate is matching
	// nothing and would pass against any text), and at least one must
	// WITHHOLD it (or the gate cannot tell the two apart).
	if graded < 40 {
		t.Errorf("graded only %d refusals across %d states × %d deployable shapes × 2 dr states — the matrix has gone vacuous",
			graded, len(states), len(deployableShapes))
	}
	if claimed == 0 {
		t.Error("no refusal made the delete-is-safe claim; the sentence this gate greps for has drifted out of legRunner.safeToDeleteAdvice, so it is grading nothing")
	}
	if withheld == 0 {
		t.Error("every refusal made the delete-is-safe claim; the gate cannot distinguish an honest refusal from an unearned one")
	}
}

// TestLegAdopt_UnrecognizedStateRefusesWithoutTheDeleteAdvice pins the
// verdict Bug 231 added, from the operator's side: sluice says it does not
// know, names no `pscale branch delete`, and sends the operator to look.
func TestLegAdopt_UnrecognizedStateRefusesWithoutTheDeleteAdvice(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.predeployDeployable = true
	ps.seedLeftover(leftover, "open", "some_future_predeploy_state", false)

	r, cleanup, _ := newGateLegRunner(t, ps)
	_, err := r.adoptLeftoverBranch(context.Background(), leftover, cleanup)
	wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
	for _, want := range []string{"does not recognize", "some_future_predeploy_state", "will not advise deleting the dev branch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("unrecognized-state refusal %q missing %q", err.Error(), want)
		}
	}
	if strings.Contains(err.Error(), "pscale branch delete") {
		t.Errorf("a refusal sluice cannot justify handed out a branch delete anyway:\n%s", err)
	}
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE attempted %d time(s) on a state sluice does not understand", ps.deleteAttempts)
	}
}

// TestLegAdopt_MidDeployDeployableLeftoverIsJoined drives Bug 231's live
// shape end to end through the fake control plane: a leftover whose deploy
// request is `in_progress` AND reports deployable=true (which is what
// PlanetScale serves mid-deploy) must be JOINED, with no second branch and
// no second deploy request.
//
// It is deliberately separate from the older adopt pins, because those
// were green throughout the bug: the fake served deployable=false for the
// whole post-deploy walk, so they exercised a shape the live API does not
// produce.
func TestLegAdopt_MidDeployDeployableLeftoverIsJoined(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "in_progress", true)
	ps.drs[1].states = []string{"in_progress", "complete_pending_revert"}
	ps.opPercents = []int{78}

	out := &bytes.Buffer{}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.out = out
	final, err := r.adoptLeftoverBranch(context.Background(), leftover, cleanup)
	if err != nil {
		t.Fatalf("adoptLeftoverBranch = %v; want the mid-deploy leftover joined", err)
	}
	if !final.Deployment.Deployable {
		t.Fatal("the fake stopped serving deployable=true mid-deploy — this test's whole point is the live shape (Bug 231); restore it")
	}
	if !strings.Contains(out.String(), "still deploying") {
		t.Errorf("adoption narration %q does not say it joined a running deploy", out.String())
	}
	if ps.nextDR != 2 {
		t.Errorf("nextDR = %d; want 2 (no second deploy request)", ps.nextDR)
	}
	if ps.deployCalls != 0 {
		t.Errorf("deploy called %d time(s) on a request PlanetScale is already deploying", ps.deployCalls)
	}
}

// TestLegAdopt_NeverDeployedRefusesAndLeavesTheBranchAlone pins the
// deliberate narrowing: a deploy request that is DEPLOYABLE but never
// deployed is refused rather than deployed, because the pre-deploy
// promises cannot be re-made for a branch this run did not provision. The
// branch is left exactly as found — the message tells the operator to
// delete it, sluice does not.
func TestLegAdopt_NeverDeployedRefusesAndLeavesTheBranchAlone(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "ready", false)

	f, _ := newTestIndexFallback(t, ps)
	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
	for _, want := range []string{
		"never deployed",
		"deployable=true",
		"schema base still matches",
		"safe to delete",
		"pscale branch delete",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("never-deployed refusal %q missing %q", err.Error(), want)
		}
	}
	if ps.deployCalls != 0 {
		t.Errorf("deploy called %d time(s); adoption must never issue a Deploy", ps.deployCalls)
	}
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE attempted %d time(s); a refusal leaves the operator's branch alone", ps.deleteAttempts)
	}
}

// TestLegAdopt_TerminalFailureNamesTheBranchDelete pins the one shape
// where "delete the branch" was always correct, and that it still says so.
func TestLegAdopt_TerminalFailureNamesTheBranchDelete(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "closed", "complete_error", true)

	f, _ := newTestIndexFallback(t, ps)
	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
	for _, want := range []string{"cannot be resumed", "complete_error", "pscale branch delete"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("terminal-failure refusal %q missing %q", err.Error(), want)
		}
	}
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE attempted %d time(s); the refusal names the delete, it does not perform it", ps.deleteAttempts)
	}
}

// TestLegAdopt_TwoDeployRequestsRefuse pins that sluice will not pick.
func TestLegAdopt_TwoDeployRequestsRefuse(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "in_progress", true)
	ps.seedLeftover(leftover, "open", "in_progress", true)

	f, _ := newTestIndexFallback(t, ps)
	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
	for _, want := range []string{"has 2 deploy requests", "#1, #2", "will not guess"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ambiguous-ownership refusal %q missing %q", err.Error(), want)
		}
	}
	if strings.Contains(err.Error(), "safe to delete") {
		t.Errorf("an ambiguous leftover must NOT be advertised as safe to delete — one of those requests may be deploying:\n%s", err)
	}
}

// TestLegAdopt_DeployRequestIntoAnotherBranchRefuses pins the second half
// of the identity argument: the request must merge into the production
// branch THIS run targets.
func TestLegAdopt_DeployRequestIntoAnotherBranchRefuses(t *testing.T) {
	ps := newFakePS(t)
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.seedLeftover(leftover, "open", "in_progress", true)
	ps.drs[1].dr.IntoBranch = "staging"

	f, _ := newTestIndexFallback(t, ps)
	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
	if !strings.Contains(err.Error(), `merges into "staging"`) {
		t.Errorf("wrong-target refusal %q does not name the branch it actually targets", err.Error())
	}
}

// TestLegAdopt_RefusesABranchNameSluiceDidNotDerive pins the ownership
// floor. Adoption's whole argument is "sluice derived this name from its
// own DDL"; a caller that ever hands the runner an operator-chosen branch
// name has no such argument, and must get a refusal rather than inherit
// one.
func TestLegAdopt_RefusesABranchNameSluiceDidNotDerive(t *testing.T) {
	ps := newFakePS(t)
	ps.seedLeftover("operators-own-branch", "open", "in_progress", true)

	r, cleanup, _ := newGateLegRunner(t, ps)
	_, err := r.adoptLeftoverBranch(context.Background(), "operators-own-branch", cleanup)
	wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
	if !strings.Contains(err.Error(), "not one sluice derived") {
		t.Errorf("refusal %q does not name the missing ownership evidence", err.Error())
	}
	if ps.drListCalls != 0 {
		t.Errorf("the deploy-request list was read %d time(s) for a branch sluice does not own", ps.drListCalls)
	}
}

// TestLegAdopt_UnenumerableListNeverBecomesTheDeleteAdvice is the
// loud-beats-wrong pin. "No deploy request" and "sluice could not see the
// deploy requests" look identical from the inside, and the first one's
// remedy is a branch delete that would destroy a running deployment — so
// the failed enumeration must NEVER collapse into it.
func TestLegAdopt_UnenumerableListNeverBecomesTheDeleteAdvice(t *testing.T) {
	t.Run("transport failure", func(t *testing.T) {
		ps := newFakePS(t)
		leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
		ps.branches[leftover] = &api.Branch{Name: leftover, Ready: true}
		ps.drListStatus = 500

		f, _ := newTestIndexFallback(t, ps)
		err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
		if err == nil {
			t.Fatal("want a failure when the deploy-request list cannot be read")
		}
		if strings.Contains(err.Error(), "safe to delete") || strings.Contains(err.Error(), "NO deploy request") {
			t.Errorf("an unreadable list was reported as an absent deploy request:\n%s", err)
		}
	})

	t.Run("more deploy requests than sluice enumerates", func(t *testing.T) {
		ps := newFakePS(t)
		leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
		ps.branches[leftover] = &api.Branch{Name: leftover, Ready: true}
		ps.drListPadTo = 100 // every page comes back full ⇒ the walk is truncated

		f, _ := newTestIndexFallback(t, ps)
		err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
		wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
		if !strings.Contains(err.Error(), "more deploy requests than sluice enumerates") {
			t.Errorf("truncated-list refusal %q does not name the truncation", err.Error())
		}
		if strings.Contains(err.Error(), "safe to delete") {
			t.Errorf("a truncated enumeration was advertised as safe to delete:\n%s", err)
		}
	})
}

// ---- per-command advice ----

// TestIndexFallbackRerunAdviceNamesOnlyRunnableFlags is the Bug 230 rule
// applied to this surface: the index-build fallback is armed on five entry
// points and only `migrate` has `--resume`, so the neutral text — the one
// every other command receives — must name no flag at all.
//
// Graded with the same tokenizer the CLI-surface gate uses
// ([climsggate.BareFlags]), so "what counts as a flag" cannot drift
// between the two.
func TestIndexFallbackRerunAdviceNamesOnlyRunnableFlags(t *testing.T) {
	defer migcore.SetRunningCommand("")

	migcore.SetRunningCommand(string(migcore.CommandMigrate))
	if got := indexFallbackRerun(); !strings.Contains(got, "--resume") {
		t.Errorf("migrate advice = %q; want it to name --resume", got)
	}

	// Every other entry point, including the zero value a library caller
	// and every test gets.
	for _, cmd := range []string{"", "sync start", "sync run", "restore", "sync from-backup"} {
		migcore.SetRunningCommand(cmd)
		got := indexFallbackRerun()
		if flags := climsggate.BareFlags(got); len(flags) != 0 {
			t.Errorf("advice for %q names flag(s) %v: %q — only `migrate` has --resume, so the shared text must name none",
				cmd, flags, got)
		}
		if !strings.Contains(got, "re-run") {
			t.Errorf("advice for %q = %q; want it to still say what to do", cmd, got)
		}
	}
}
