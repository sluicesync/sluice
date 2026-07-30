// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the enumerated deployment state machine, the unbounded deploy
// wait, the progress/throttle narration, and the "leave the dev branch
// alone" rule (ADR-0148, the 2026-07-30 field run).
//
// The family here is the `deployment_state` ENUM — twenty values, and the
// poller dispatches on it. So the pins walk the WHOLE enum rather than a
// representative state (the Bug 74 lesson applied to a state machine
// instead of a type family): every terminal-success value proceeds, every
// terminal-failure value fails FAST even with no clock to fall back on,
// every in-flight value keeps waiting, and the one human-gated value
// stops and says so. A single representative would have proved nothing —
// the whole risk of an unbounded wait lives in the states nobody thought
// to classify.

package expandcontract

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// unboundedRunCtx bounds the TEST (not the runner): every unbounded pin
// below deliberately gives the runner no deadline, so a classification
// bug would hang the suite instead of failing it. The ctx converts that
// hang into a legible failure.
func unboundedRunCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestDrStates_ClassifiesTheWholeDocumentedEnum is the anti-hang gate:
// the expected table below is PlanetScale's documented `deployment_state`
// enum in full (GET deploy-request API reference), and the test asserts
// drStates matches it exactly in BOTH directions. Adding a state to the
// code without classifying it here — or classifying one wrongly — fails.
//
// This matters more than it looks: with the deploy wait unbounded, a
// terminal state missing from drStates is not a timeout, it is a poller
// that spins forever on a deploy that already finished.
func TestDrStates_ClassifiesTheWholeDocumentedEnum(t *testing.T) {
	want := map[string]drClass{
		"pending":                    drInFlight,
		"ready":                      drInFlight,
		"no_changes":                 drInFlight,
		"queued":                     drInFlight,
		"submitting":                 drInFlight,
		"in_progress":                drInFlight,
		"pending_cutover":            drHumanGate,
		"in_progress_vschema":        drInFlight,
		"in_progress_cancel":         drInFlight,
		"in_progress_cutover":        drInFlight,
		"complete":                   drSuccess,
		"complete_cancel":            drFailure,
		"complete_error":             drFailure,
		"complete_pending_revert":    drSuccess,
		"in_progress_revert":         drInFlight,
		"in_progress_revert_vschema": drInFlight,
		"complete_revert":            drFailure,
		"complete_revert_error":      drFailure,
		"cancelled":                  drFailure,
		"error":                      drFailure,
	}
	if len(want) != 20 {
		t.Fatalf("the expected table lists %d states; PlanetScale documents 20 — fix the fixture, not the count", len(want))
	}
	for state, wantClass := range want {
		if got := classifyDeployState(state); got != wantClass {
			t.Errorf("classifyDeployState(%q) = %v; want %v", state, got, wantClass)
		}
		if _, ok := drStates[state]; !ok {
			t.Errorf("documented state %q is missing from drStates (it would silently default to in-flight)", state)
		}
	}
	for state := range drStates {
		if _, ok := want[state]; !ok {
			t.Errorf("drStates classifies %q, which is not in the documented enum fixture — add it (with its reasoning) or drop it", state)
		}
	}
}

// TestClassifyDeployState_UnknownStateKeepsWaiting pins the one surviving
// piece of the old tolerance: an intermediate state PlanetScale adds
// later must not fail a healthy deploy.
func TestClassifyDeployState_UnknownStateKeepsWaiting(t *testing.T) {
	for _, state := range []string{"", "some_future_state", "IN_PROGRESS"} {
		if got := classifyDeployState(state); got != drInFlight {
			t.Errorf("classifyDeployState(%q) = %v; want drInFlight (unknown states keep waiting)", state, got)
		}
	}
}

// TestLegRunner_UnboundedWaitWalksEveryInFlightState is the load-bearing
// regression pin for the unbounded default. deployTimeout 0 means NO
// deadline: before this change `time.Now().Add(0)` was a deadline already
// in the past, so a zero timeout timed out on the FIRST poll. Here the
// deploy walks every in-flight state in the enum and still completes.
func TestLegRunner_UnboundedWaitWalksEveryInFlightState(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{
		"queued", "submitting", "in_progress", "in_progress_vschema",
		"in_progress_cutover", "some_future_state", "complete_pending_revert",
	}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.deployTimeout = 0 // unbounded

	dr, err := r.run(unboundedRunCtx(t), "sluice-gate-unbounded", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	if err != nil {
		t.Fatalf("run = %v; want nil (an unbounded wait must poll through every in-flight state)", err)
	}
	if dr.DeploymentState != "complete_pending_revert" {
		t.Errorf("final deployment_state = %q; want complete_pending_revert", dr.DeploymentState)
	}
	if !ps.drs[1].skipRevert {
		t.Error("skip-revert was not called; the revert window stays open")
	}
}

// TestLegRunner_EveryTerminalSuccessStateProceeds walks both applied
// states, unbounded, so neither can silently become a spin.
func TestLegRunner_EveryTerminalSuccessStateProceeds(t *testing.T) {
	for _, state := range []string{"complete", "complete_pending_revert"} {
		t.Run(state, func(t *testing.T) {
			ps := newFakePS(t)
			ps.postStates = []string{"queued", state}
			r, cleanup, _ := newGateLegRunner(t, ps)
			r.deployTimeout = 0

			dr, err := r.run(unboundedRunCtx(t), "sluice-gate-"+state, "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
			if err != nil {
				t.Fatalf("run = %v; want nil for terminal-success state %q", err, state)
			}
			if dr.DeploymentState != state {
				t.Errorf("final deployment_state = %q; want %q", dr.DeploymentState, state)
			}
			// skip-revert is only for the open-revert-window state.
			if got, want := ps.drs[1].skipRevert, state == "complete_pending_revert"; got != want {
				t.Errorf("skipRevert = %v; want %v for %q", got, want, state)
			}
		})
	}
}

// TestLegRunner_EveryTerminalFailureStateFailsFastUnbounded is the other
// half of the unbounded posture, and the reason the enum had to be
// enumerated: with NO deadline, a terminal failure state that isn't
// recognized hangs forever. Each of the six is driven unbounded and must
// come back coded-FAILED promptly, with the dev branch cleaned up (a dead
// deployment does not hold its branch).
func TestLegRunner_EveryTerminalFailureStateFailsFastUnbounded(t *testing.T) {
	for _, state := range []string{
		"error", "complete_error", "cancelled",
		"complete_cancel", "complete_revert", "complete_revert_error",
	} {
		t.Run(state, func(t *testing.T) {
			ps := newFakePS(t)
			ps.postStates = []string{"queued", state}
			r, cleanup, _ := newGateLegRunner(t, ps)
			r.deployTimeout = 0 // unbounded: only the classification can stop this

			_, err := r.run(unboundedRunCtx(t), "sluice-gate-"+state, "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unbounded wait HUNG on terminal state %q — it is not classified as a failure", state)
			}
			wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
			if !strings.Contains(err.Error(), state) {
				t.Errorf("failure error %q should name the state it saw", err.Error())
			}
			cleanup.run(context.Background())
			if len(ps.deleted) != 1 {
				t.Errorf("deleted = %v; a terminally FAILED deployment releases its branch", ps.deleted)
			}
		})
	}
}

// TestLegRunner_InFlightBoundHitKeepsTheBranchUntouched is the fix for
// the field bug: sluice used to ask PlanetScale to delete the dev branch
// while the deployment was still running, got HTTP 422 ("cannot be
// deleted while a deployment is in progress"), and logged "delete it
// manually" — advising the one action that would break its own `--resume`
// recovery. The assertion is on deleteAttempts, not `deleted`: the point
// is that the request is never SENT.
func TestLegRunner_InFlightBoundHitKeepsTheBranchUntouched(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{"in_progress"} // never terminal
	ps.opPercents = []int{29}
	ps.opETASeconds = 8700
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.deployTimeout = 50 * time.Millisecond

	_, err := r.run(context.Background(), "sluice-gate-inflight", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	wantCode(t, err, sluicecode.CodePSDeployRequestIncomplete)
	for _, want := range []string{
		"still deploying",
		"nothing failed",
		"29% complete",             // the operator sees how far it got
		"PlanetScale ETA ~2h25m0s", // and what PlanetScale expects
		"--deploy-timeout was set to 50ms",
		"pass 0 to wait",
		"LEFT ALONE",
		"refuses the delete while a deployment is in progress",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("incomplete error %q missing %q", err.Error(), want)
		}
	}

	cleanup.run(context.Background())
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE was attempted %d time(s); the in-flight deployment depends on the branch and PlanetScale correctly refuses it — sluice must not ask", ps.deleteAttempts)
	}
	if !ps.branchExists("sluice-gate-inflight") {
		t.Error("dev branch is gone; the running deployment needed it")
	}
}

// TestLegRunner_PendingCutoverWithAutoCutoverKeepsWaiting: sluice's own
// deploy requests come back auto_cutover=true, so passing through
// pending_cutover is healthy and must NOT stop the wait. Without this
// downgrade the human-gate classification would fail perfectly good
// deploys.
func TestLegRunner_PendingCutoverWithAutoCutoverKeepsWaiting(t *testing.T) {
	ps := newFakePS(t)
	ps.autoCutover = true
	ps.postStates = []string{"in_progress", "pending_cutover", "in_progress_cutover", "complete"}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.deployTimeout = 0

	dr, err := r.run(unboundedRunCtx(t), "sluice-gate-autocut", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	if err != nil {
		t.Fatalf("run = %v; want nil (auto_cutover means PlanetScale clears pending_cutover itself)", err)
	}
	if dr.DeploymentState != "complete" {
		t.Errorf("final deployment_state = %q; want complete", dr.DeploymentState)
	}
}

// TestLegRunner_PendingCutoverWithoutAutoCutoverNamesTheHumanGate: on a
// gated-deployment database nobody is coming unless sluice says so — an
// unbounded wait here would look hung forever. Stop, name the gate, and
// keep the branch (the deployment still needs it).
func TestLegRunner_PendingCutoverWithoutAutoCutoverNamesTheHumanGate(t *testing.T) {
	ps := newFakePS(t)
	ps.autoCutover = false
	ps.postStates = []string{"in_progress", "pending_cutover"}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.deployTimeout = 0 // unbounded: only the gate detection can stop this

	_, err := r.run(unboundedRunCtx(t), "sluice-gate-humancut", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unbounded wait HUNG on pending_cutover — the human gate is not detected")
	}
	wantCode(t, err, sluicecode.CodePSDeployRequestIncomplete)
	for _, want := range []string{
		"waiting for a PERSON to confirm the cutover",
		"auto_cutover=false",
		"nothing failed",
		"never confirms a cutover on your behalf",
		"LEFT ALONE",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("human-gate error %q missing %q", err.Error(), want)
		}
	}
	cleanup.run(context.Background())
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE attempted %d time(s); the deployment awaiting cutover still needs the branch", ps.deleteAttempts)
	}
}

// TestLegRunner_NarratesProgressOnChangeNotEveryPoll pins the reporting
// cadence and content: a line per state/percentage change, the throttle
// caveat spelled out EXACTLY once, and far fewer lines than polls (a
// 10-second poll over a three-hour build must not emit a thousand lines).
func TestLegRunner_NarratesProgressOnChangeNotEveryPoll(t *testing.T) {
	throttled := time.Date(2026, 7, 30, 0, 21, 59, 0, time.UTC)
	ps := newFakePS(t)
	// 8 post-deploy GETs, but only 4 distinct (state, percent) pairs.
	ps.postStates = []string{"in_progress", "in_progress", "in_progress", "in_progress", "in_progress", "in_progress", "complete"}
	ps.opPercents = []int{60, 60, 60, 65, 65, 67, 67}
	ps.opETASeconds = 4299
	ps.opThrottledAt = &throttled
	r, cleanup, out := newGateLegRunner(t, ps)
	r.deployTimeout = 0

	if _, err := r.run(unboundedRunCtx(t), "sluice-gate-narrate", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run = %v; want nil", err)
	}

	var lines []string
	for _, ln := range strings.Split(out.String(), "\n") {
		if strings.Contains(ln, "deploy request #1 is ") {
			lines = append(lines, ln)
		}
	}
	// in_progress@60, @65, @67, then complete. Not one per poll.
	if len(lines) != 4 {
		t.Fatalf("narration lines = %d, want 4 (one per state/percent change, not one per poll):\n%s", len(lines), strings.Join(lines, "\n"))
	}
	for i, want := range []string{"60% complete", "65% complete", "67% complete", "is complete"} {
		if !strings.Contains(lines[i], want) {
			t.Errorf("narration line %d = %q; want it to carry %q", i, lines[i], want)
		}
	}
	if !strings.Contains(lines[0], "PlanetScale ETA ~1h11m39s") {
		t.Errorf("first narration line %q should carry PlanetScale's ETA", lines[0])
	}
	// The throttle caveat is long and load-bearing (the field's own UI
	// said "throttled" for hours while this stamp never moved) — said
	// once, then terse.
	if got := strings.Count(out.String(), "not a live gauge"); got != 1 {
		t.Errorf("throttle caveat appeared %d times; want exactly 1 (spelled out once, then terse)", got)
	}
	if !strings.Contains(out.String(), "throttled at least once, last stamp 2026-07-30T00:21:59Z") {
		t.Errorf("narration should carry the throttle stamp verbatim:\n%s", out.String())
	}
	if !strings.Contains(lines[1], "[throttled at 2026-07-30T00:21:59Z]") {
		t.Errorf("subsequent lines should report the throttle tersely: %q", lines[1])
	}
}

// TestLegRunner_NarratesProgressFromOperationSummaries pins the second
// live field location: the response carries the same progress rows in
// deploy_operations AND deploy_operation_summaries, so reading only one
// would go blind if PlanetScale ever populates just the other.
func TestLegRunner_NarratesProgressFromOperationSummaries(t *testing.T) {
	ps := newFakePS(t)
	ps.opsInSummariesOnly = true
	ps.postStates = []string{"in_progress", "complete"}
	ps.opPercents = []int{42}
	r, cleanup, out := newGateLegRunner(t, ps)
	r.deployTimeout = 0

	if _, err := r.run(unboundedRunCtx(t), "sluice-gate-summaries", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run = %v; want nil", err)
	}
	if !strings.Contains(out.String(), "42% complete") {
		t.Errorf("progress from deploy_operation_summaries was not narrated:\n%s", out.String())
	}
}

// TestLegRunner_NoOperationRowsStillNarrates: a queued deployment carries
// no operation rows at all. The narration must degrade to state-only
// rather than fabricating a percentage (or panicking on the nil slice).
func TestLegRunner_NoOperationRowsStillNarrates(t *testing.T) {
	ps := newFakePS(t)
	// The first entry is consumed by the deploy POST's own response, so
	// "queued" is listed twice to reach the poll loop.
	ps.postStates = []string{"queued", "queued", "complete"}
	r, cleanup, out := newGateLegRunner(t, ps)
	r.deployTimeout = 0

	if _, err := r.run(unboundedRunCtx(t), "sluice-gate-noops", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run = %v; want nil", err)
	}
	if !strings.Contains(out.String(), "deploy request #1 is queued") {
		t.Errorf("a deployment with no operation rows must still be narrated:\n%s", out.String())
	}
	if strings.Contains(out.String(), "% complete") {
		t.Errorf("no operation rows means no percentage — sluice must not invent one:\n%s", out.String())
	}
}

// TestLegRunner_QueuePausedIsNamedLoudly: under an unbounded wait a
// paused deploy queue is an indefinite wait on an operator-side
// condition, so it is WARNed by name (with PlanetScale's reason) instead
// of being sat out in silence.
func TestLegRunner_QueuePausedIsNamedLoudly(t *testing.T) {
	ps := newFakePS(t)
	ps.queuePaused = true
	ps.queuePauseReason = "a previous deployment errored"
	ps.postStates = []string{"queued", "queued", "complete"}
	r, cleanup, out := newGateLegRunner(t, ps)
	r.deployTimeout = 0

	logs := captureWarnLogs(t)
	if _, err := r.run(unboundedRunCtx(t), "sluice-gate-paused", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run = %v; want nil (a paused queue is reported, not abandoned)", err)
	}
	if !strings.Contains(logs.String(), "deploy queue is PAUSED") ||
		!strings.Contains(logs.String(), "a previous deployment errored") {
		t.Errorf("paused queue was not WARNed with its reason:\n%s", logs.String())
	}
	if got := strings.Count(logs.String(), "deploy queue is PAUSED"); got != 1 {
		t.Errorf("queue-pause WARN fired %d times; want exactly 1 per leg", got)
	}
	if !strings.Contains(out.String(), "deploy queue is PAUSED") {
		t.Errorf("the narration line should carry the pause too:\n%s", out.String())
	}
}

// TestLegRunner_ApprovalGateIsAdvisedNotInferred: a database that
// requires administrator approval leaves the request un-deployable until
// a human acts. sluice cannot DETECT that requirement — `approved` is
// false on plenty of requests that deploy fine (live-verified: the field
// run's mid-build request read approved=false) — so it says what to check
// once the wait has run long enough for approval to be the likely cause.
func TestLegRunner_ApprovalGateIsAdvisedNotInferred(t *testing.T) {
	ps := newFakePS(t)
	ps.approved = false
	ps.preStates = []string{"pending"} // never deployable
	r, cleanup, _ := newGateLegRunner(t, ps)
	// A clock that jumps 30 s per read: the advisory threshold (90 s) is
	// crossed in a few polls, and the deployable bound (10 min) well
	// after, so the advisory demonstrably precedes the timeout.
	base := time.Now()
	var ticks int
	r.now = func() time.Time {
		ticks++
		return base.Add(time.Duration(ticks) * 30 * time.Second)
	}
	r.deployTimeout = 10 * time.Minute

	logs := captureWarnLogs(t)
	_, err := r.run(context.Background(), "sluice-gate-approval", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	wantCode(t, err, sluicecode.CodePSDeployRequestIncomplete)
	if !strings.Contains(logs.String(), "requires administrator approval") {
		t.Errorf("the approval advisory did not fire before the deployable bound:\n%s", logs.String())
	}
	if got := strings.Count(logs.String(), "requires administrator approval"); got != 1 {
		t.Errorf("approval advisory fired %d times; want exactly 1", got)
	}
	// And the branch is kept — deleting it would close the very request
	// the operator was just told to approve.
	cleanup.run(context.Background())
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE attempted %d time(s); the still-open deploy request needs it", ps.deleteAttempts)
	}
}

// TestLegRunner_ApprovedRequestGetsNoApprovalAdvisory keeps the advisory
// from becoming noise on the common path.
func TestLegRunner_ApprovedRequestGetsNoApprovalAdvisory(t *testing.T) {
	ps := newFakePS(t)
	ps.approved = true
	ps.postStates = []string{"queued", "complete"}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.deployTimeout = 0

	logs := captureWarnLogs(t)
	if _, err := r.run(unboundedRunCtx(t), "sluice-gate-approved", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run = %v; want nil", err)
	}
	if strings.Contains(logs.String(), "requires administrator approval") {
		t.Errorf("an approved, promptly-deployable request must not draw the approval advisory:\n%s", logs.String())
	}
}

// TestReviewTimeout_CapsAnUnboundedDeployWait pins the deliberate
// asymmetry: an unbounded DEPLOY wait is machine work with visible
// progress, but the deployable wait can hang on a person, so it keeps a
// finite cap regardless.
func TestReviewTimeout_CapsAnUnboundedDeployWait(t *testing.T) {
	r := &legRunner{deployTimeout: 0}
	if got := r.reviewTimeout(); got != legHumanWaitCap {
		t.Errorf("reviewTimeout with an unbounded deploy wait = %s; want the %s human cap", got, legHumanWaitCap)
	}
	r.deployTimeout = 7 * time.Minute
	if got := r.reviewTimeout(); got != 7*time.Minute {
		t.Errorf("reviewTimeout = %s; want it to track a bounded deploy timeout", got)
	}
}

// TestPollDeadline_ZeroMeansUnbounded pins the boundary directly: before
// this change a zero timeout produced a deadline already in the past.
func TestPollDeadline_ZeroMeansUnbounded(t *testing.T) {
	now := time.Now()
	for _, d := range []time.Duration{0, -time.Second} {
		if _, bounded := pollDeadline(now, d); bounded {
			t.Errorf("pollDeadline(%s) reported a bound; zero/negative means unbounded", d)
		}
	}
	deadline, bounded := pollDeadline(now, time.Minute)
	if !bounded || !deadline.Equal(now.Add(time.Minute)) {
		t.Errorf("pollDeadline(1m) = (%s, %v); want a bound one minute out", deadline, bounded)
	}
}

// captureWarnLogs redirects the default slog logger into a buffer for the
// duration of a test so the WARN-level advisories can be asserted.
func captureWarnLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}
