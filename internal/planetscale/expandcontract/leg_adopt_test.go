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

// TestClassifyAdoption_CoversEveryDeploymentState grades every documented
// `deployment_state` against the adopt/refuse verdict, and asserts the
// table's key set is EXACTLY [drStates]' — so adding a state to the enum
// without deciding whether it is adoptable fails here instead of shipping
// as an unbounded wait.
func TestClassifyAdoption_CoversEveryDeploymentState(t *testing.T) {
	// deployable mirrors what PlanetScale reports for the state; only the
	// pre-deploy `ready` is deployable.
	want := map[string]struct {
		verdict    adoptVerdict
		deployable bool
	}{
		// Before the deploy call — never adopted: sluice cannot re-make
		// the pre-deploy freshness/blast-radius promises for a branch it
		// did not provision this run, and nothing is building yet.
		"pending": {adoptRefuseNeverDeployed, false},
		"ready":   {adoptRefuseNeverDeployed, true},
		// no_changes is pre-deploy too, but its cause is different (the
		// DDL is already on production), so it gets its own advice.
		"no_changes": {adoptRefuseAlreadyDeployed, false},

		// Issued and running — adopted onto the shared poller. This row
		// is the whole item: `in_progress` at 78 % was the live case a
		// re-run refused.
		"queued":                     {adoptJoin, false},
		"submitting":                 {adoptJoin, false},
		"in_progress":                {adoptJoin, false},
		"in_progress_vschema":        {adoptJoin, false},
		"in_progress_cutover":        {adoptJoin, false},
		"in_progress_cancel":         {adoptJoin, false},
		"in_progress_revert":         {adoptJoin, false},
		"in_progress_revert_vschema": {adoptJoin, false},

		// Parked on a person — adopted, because waitDeployed owns that
		// gate already (it downgrades the state on an auto_cutover
		// deployment and stops loudly on a real one).
		"pending_cutover": {adoptJoin, false},

		// Terminal, applied — finalize the revert window and move on.
		"complete":                {adoptFinalize, false},
		"complete_pending_revert": {adoptFinalize, false},

		// Terminal, not applied — refuse; here "delete the branch" is
		// genuinely the right advice.
		"error":                 {adoptRefuseFailed, false},
		"complete_error":        {adoptRefuseFailed, false},
		"cancelled":             {adoptRefuseFailed, false},
		"complete_cancel":       {adoptRefuseFailed, false},
		"complete_revert":       {adoptRefuseFailed, false},
		"complete_revert_error": {adoptRefuseFailed, false},
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

	for state, exp := range want {
		dr := &api.DeployRequest{
			Number: 7, State: "open", DeploymentState: state,
			Deployment: api.Deployment{State: state, Deployable: exp.deployable},
		}
		if got := classifyAdoption(dr); got != exp.verdict {
			t.Errorf("classifyAdoption(%q) = %v; want %v", state, got, exp.verdict)
		}
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

// TestClassifyAdoption_DeployableIsTheIndependentSignal pins the second,
// independent refusal signal: a state name sluice has never seen that is
// nonetheless still awaiting its Deploy call is refused, not handed to a
// poller that would wait on it until the operator gives up.
func TestClassifyAdoption_DeployableIsTheIndependentSignal(t *testing.T) {
	unknownDeployable := &api.DeployRequest{
		Number: 1, State: "open", DeploymentState: "some_future_predeploy_state",
		Deployment: api.Deployment{Deployable: true},
	}
	if got := classifyAdoption(unknownDeployable); got != adoptRefuseNeverDeployed {
		t.Errorf("unknown-but-deployable state = %v; want adoptRefuseNeverDeployed", got)
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
