// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # The PlanetScale deployment state machine, enumerated — and narrated
//
// The poller used to know six failure states and two success states by
// name and treat EVERY other value as "keep waiting until the deadline".
// That was the right posture while the deadline was the primary safety
// net; it stops being right the moment the deploy wait goes unbounded
// (ADR-0148: a 106 GB / 153 M-row index build on a real PS-160 was ~29 %
// done when the old 1 h default gave up, so the non-terminal exit was the
// DEFAULT outcome at that scale, not an edge case). With no clock to fall
// back on, a state that will never advance on its own has to be
// recognized by name or sluice hangs forever.
//
// So this file holds the FULL `deployment_state` enum — all twenty values
// PlanetScale's API reference documents — classified into four buckets,
// with the reason for each classification written down. Unknown names
// still default to in-flight (a new intermediate PlanetScale state must
// not fail a healthy deploy), which is now the ONLY place the old
// tolerance survives.
//
// The second half is the narration: a long VReplication build is
// invisible from sluice's output unless the poller reports the progress
// fields the response has always carried. An operator watching only
// sluice cannot otherwise tell a healthy-but-throttled three-hour build
// from a stuck one — which is exactly what happened on the field run this
// work came from.

package expandcontract

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
)

// drClass classifies one PlanetScale `deployment_state` for the poller.
type drClass int

const (
	// drInFlight — progressing, or waiting on PlanetScale itself. It
	// will reach a terminal state without anyone doing anything, so the
	// poller keeps polling.
	drInFlight drClass = iota

	// drSuccess — terminal, and the schema change IS applied.
	drSuccess

	// drFailure — terminal, and the schema change is NOT applied (or was
	// applied and then undone). These must fail FAST: waiting one out
	// would burn the whole timeout, and under an unbounded wait it would
	// hang forever.
	drFailure

	// drHumanGate — NOT terminal, but it will not advance until a person
	// acts in the PlanetScale UI. Sitting on one is indistinguishable
	// from hanging, so the poller stops and names the gate instead.
	drHumanGate
)

// drStates is the complete `deployment_state` enum from PlanetScale's API
// reference (GET /organizations/{org}/databases/{db}/deploy-requests/{n}),
// every value classified. `no_changes` is in the table for completeness
// but the deployable wait intercepts it earlier with its own
// already-deployed refusal, so its class is never consulted.
//
// Enumerating the whole enum is the point: with the deploy wait unbounded
// there is no clock to catch a state sluice failed to recognize, so a
// missing terminal name becomes an infinite hang rather than a
// timeout-shaped complaint.
var drStates = map[string]drClass{
	// ---- before the deploy call ----
	"pending":    drInFlight, // created; PlanetScale has not computed the diff yet
	"ready":      drInFlight, // diff computed, deployable — CanDeploy() is what actually releases the wait
	"no_changes": drInFlight, // empty diff; intercepted by waitDeployable's already-deployed refusal
	"queued":     drInFlight, // behind other deployments in the target branch's deploy queue

	// ---- the deploy, running ----
	"submitting":                 drInFlight, // handed to Vitess
	"in_progress":                drInFlight, // the VReplication build — where a large index spends hours
	"in_progress_vschema":        drInFlight, // applying the vschema half
	"in_progress_cutover":        drInFlight, // cutting the shadow table over
	"in_progress_cancel":         drInFlight, // cancelling; resolves to complete_cancel
	"in_progress_revert":         drInFlight, // reverting; resolves to complete_revert
	"in_progress_revert_vschema": drInFlight, // reverting the vschema half

	// pending_cutover is the human gate: PlanetScale's own webhook
	// changelog defines it as "ready to apply the schema and waiting on
	// the user to confirm". It should not occur for sluice's own deploy
	// requests — they come back auto_cutover=true (live-verified
	// 2026-07-30) — but a database configured for gated deployments
	// changes that, and waiting on a person forever is the failure mode
	// this classification exists to prevent. waitDeployed DOWNGRADES it
	// to in-flight when the deployment reports auto_cutover, so a
	// momentary pending_cutover on an auto-cutover deployment can never
	// fail a healthy deploy.
	"pending_cutover": drHumanGate,

	// ---- terminal, applied ----
	"complete":                drSuccess, // applied, no revert window outstanding
	"complete_pending_revert": drSuccess, // applied, revert window open — skip-revert finalizes it (ADR-0148 finding #4)

	// ---- terminal, not applied (or undone) ----
	"error":                 drFailure,
	"complete_error":        drFailure,
	"cancelled":             drFailure,
	"complete_cancel":       drFailure,
	"complete_revert":       drFailure, // applied then rolled back — the index is gone
	"complete_revert_error": drFailure,
}

// classifyDeployState classifies a `deployment_state`. An UNKNOWN name is
// in-flight: PlanetScale adding an intermediate state must not fail a
// healthy deploy. That tolerance is safe for intermediates and unsafe for
// terminals, which is why [drStates] enumerates the documented enum in
// full rather than listing only the states sluice happens to have seen.
func classifyDeployState(state string) drClass {
	if c, ok := drStates[state]; ok {
		return c
	}
	return drInFlight
}

// legProgressHeartbeat is the ceiling on silence: even with nothing
// changing, the poller narrates once per interval so an operator can see
// sluice is still watching. Progress CHANGES are reported immediately, so
// on a healthy build this heartbeat almost never fires — a 100 GB index
// build moves a percentage point every couple of minutes.
const legProgressHeartbeat = 5 * time.Minute

// legApprovalAdviceAfter is how long the deployable wait runs before
// naming the approval gate. A database that requires administrator
// approval leaves the deploy request sitting un-deployable until a human
// clicks approve; without this line sluice just looks hung. It is
// ADVISORY, not a refusal — `approved` is false on plenty of deploy
// requests that deploy fine (live-verified 2026-07-30: the field run's
// mid-build deploy request read approved=false), so sluice cannot infer
// "your org requires approval" from the flag. It can only say what to
// check.
const legApprovalAdviceAfter = 90 * time.Second

// deployProgressReporter narrates one deploy request's progress across
// the whole leg — the deployable wait and the deploy wait share an
// instance, so a state transition across the deploy call reads as one
// story. It writes to the leg's narration writer (which for the
// index-build fallback is an slog adapter, so each line becomes one INFO
// record).
//
// Cadence: a line on every state change, every progress-percentage
// change, every new throttle stamp, and otherwise once per
// [legProgressHeartbeat]. Never one line per poll — the poll interval is
// 10 s and a multi-hour build would drown the log.
type deployProgressReporter struct {
	name   string // narration prefix ("index-fallback", "expand", …)
	number int
	out    io.Writer
	now    func() time.Time

	started time.Time

	reported      bool
	lastAt        time.Time
	lastState     string
	lastPercent   int
	havePercent   bool
	lastThrottled time.Time

	// One-shot advisories: each explains itself once and then reports
	// tersely, so a long build doesn't repeat a paragraph every line.
	explainedThrottle bool
	saidApproval      bool
	saidQueuePaused   bool
}

func newDeployProgressReporter(r *legRunner, number int) *deployProgressReporter {
	now := r.nowFunc()
	return &deployProgressReporter{
		name:    r.name,
		number:  number,
		out:     r.out,
		now:     now,
		started: now(),
	}
}

// observe narrates dr when something worth saying changed, or when the
// heartbeat interval has elapsed.
func (p *deployProgressReporter) observe(dr *api.DeployRequest) {
	prog := dr.Progress()
	at := p.now()
	changed := !p.reported ||
		dr.DeploymentState != p.lastState ||
		(prog.PercentKnown && (!p.havePercent || prog.Percent != p.lastPercent)) ||
		prog.ThrottledAt.After(p.lastThrottled)
	if !changed && at.Sub(p.lastAt) < legProgressHeartbeat {
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s: deploy request #%d is %s", p.name, p.number, dr.DeploymentState)
	if prog.PercentKnown {
		fmt.Fprintf(&b, " — %d%% complete", prog.Percent)
	}
	if prog.ETAKnown {
		fmt.Fprintf(&b, ", PlanetScale ETA ~%s", prog.ETA.Round(time.Second))
	}
	fmt.Fprintf(&b, ", %s elapsed", at.Sub(p.started).Round(time.Second))
	if !prog.ThrottledAt.IsZero() {
		if p.explainedThrottle {
			fmt.Fprintf(&b, " [throttled at %s]", prog.ThrottledAt.UTC().Format(time.RFC3339))
		} else {
			// Spelled out once. PlanetScale records only the LAST throttle
			// and stamps it early (measured: once, two seconds before the
			// deployment's own started_at, unmoved 2 h 25 m later on a run
			// its own UI called throttled throughout), so this field cannot
			// answer "is it throttled right now" and must not be reported
			// as if it could.
			fmt.Fprintf(&b, " [throttled at least once, last stamp %s — PlanetScale records only the LAST throttle, so this is not a live gauge;"+
				" replication-lag throttling is normal for a large build and shows up as an ETA that stops converging, not as a failure]",
				prog.ThrottledAt.UTC().Format(time.RFC3339))
			p.explainedThrottle = true
		}
	}
	if dr.Deployment.TableLocked {
		b.WriteString(" [table locked]")
	}
	if dr.Deployment.CutoverExpiring {
		b.WriteString(" [cutover window expiring]")
	}
	if dr.Deployment.QueuePaused {
		fmt.Fprintf(&b, " [the target branch's deploy queue is PAUSED%s — nothing will deploy until it resumes]",
			reasonSuffix(dr.Deployment.QueuePauseReason))
	}
	b.WriteString("\n")
	_, _ = io.WriteString(p.out, b.String())

	p.reported = true
	p.lastAt = at
	p.lastState = dr.DeploymentState
	if prog.PercentKnown {
		p.lastPercent = prog.Percent
		p.havePercent = true
	}
	if prog.ThrottledAt.After(p.lastThrottled) {
		p.lastThrottled = prog.ThrottledAt
	}
}

// noteApprovalGate emits ONE advisory once the deployable wait has run
// long enough that an approval requirement is the likely explanation. See
// [legApprovalAdviceAfter] for why this is advice rather than detection.
func (p *deployProgressReporter) noteApprovalGate(ctx context.Context, dr *api.DeployRequest) {
	if p.saidApproval || dr.Approved || p.now().Sub(p.started) < legApprovalAdviceAfter {
		return
	}
	p.saidApproval = true
	slog.WarnContext(ctx,
		"planetscale: deploy request is not deployable yet and is not approved — if this database requires administrator approval for deploy requests, it will not proceed until a human approves it (sluice never approves on your behalf, and it cannot tell an approval requirement apart from a slow diff computation)",
		slog.Int("deploy_request", dr.Number),
		slog.String("deployment_state", dr.DeploymentState),
		slog.String("url", dr.HTMLURL))
}

// noteQueuePaused emits ONE WARN when the target branch's deploy queue is
// paused. Under an unbounded wait a paused queue means an indefinite
// wait, and the pause is an operator-side condition sluice cannot clear —
// so it is named loudly rather than waited out in silence.
func (p *deployProgressReporter) noteQueuePaused(ctx context.Context, dr *api.DeployRequest) {
	if p.saidQueuePaused || !dr.Deployment.QueuePaused {
		return
	}
	p.saidQueuePaused = true
	slog.WarnContext(ctx,
		"planetscale: the target branch's deploy queue is PAUSED"+reasonSuffix(dr.Deployment.QueuePauseReason)+
			" — this deploy request will not progress until the queue resumes; sluice keeps waiting rather than abandoning a deploy that is merely held",
		slog.Int("deploy_request", dr.Number),
		slog.String("url", dr.HTMLURL))
}

// reasonSuffix renders PlanetScale's optional human-readable reason as a
// parenthetical, or nothing when the field was null/empty.
func reasonSuffix(reason string) string {
	if reason == "" {
		return ""
	}
	return " (" + reason + ")"
}
