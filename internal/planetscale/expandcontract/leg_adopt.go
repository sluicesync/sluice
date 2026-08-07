// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Adopting the dev branch a previous run left behind (roadmap item 108)
//
// A leg's dev branch is named DETERMINISTICALLY from its DDL
// ([legBranchName]), so a re-run finds its own leftovers by name. Until
// this file existed, finding one was an unconditional refusal whose remedy
// was "delete the branch and re-run" — and the operators that refusal
// reached most often were the ones holding a branch whose deploy request
// was *healthily building*. Live 2026-07-30: a `--resume` against a deploy
// request at 78 %, `deployment_state=in_progress`, ETA ~52 min, was told to
// delete a branch whose deletion would have discarded ~3 h of completed
// index build (PlanetScale refuses that delete outright while a deployment
// runs, which is the platform being right and sluice being wrong).
//
// # What adoption is, and the line it will not cross
//
// Adoption joins a deploy that is ALREADY ISSUED. That line is the whole
// design, not a simplification:
//
//   - A deploy request that is DEPLOYING (or parked on a human cutover
//     gate) is handed to [legRunner.waitDeployed] — the same poller, the
//     same progress narration, the same terminal-failure fast-fail, the
//     same keep-the-branch rule. There is nothing left for sluice to
//     verify: the schema change is in PlanetScale's hands and refusing
//     would not un-issue it.
//   - A deploy request that has already DEPLOYED is finalized (the
//     complete_pending_revert skip-revert) and its branch torn down, which
//     is exactly what the fresh path does at the same point.
//   - A deploy request that has NOT been deployed is REFUSED. The fresh
//     path makes two promises before it calls Deploy — the dev branch's
//     schema base still matches current production ([provisionFreshBranch],
//     [recheckProductionFreshness]) and the computed diff touches nothing
//     outside the leg's intended tables ([assertDiffWithinExpected]) — and
//     neither can be re-established for a branch sluice did not provision
//     in THIS run: production may have moved arbitrarily far since, and
//     deploying from a stale base silently reverts every schema change
//     newer than the branch's backup (ADR-0162 finding 3, observed live).
//     Refusing costs the operator a re-run and nothing else, because a
//     deploy request that never deployed has no build to lose.
//
// So the pre-deploy gates are absent from the adopt path by CONSTRUCTION
// rather than by omission: adoption never reaches a Deploy call.
//
// # "Issued" is read off deployment_state, and off nothing else (Bug 231)
//
// The first cut ORed PlanetScale's `deployable` flag into that decision as
// a second, independent refusal signal — and the flag stays TRUE while a
// deployment runs, so the one shape this file exists for was the one it
// refused. Live 2026-08-07: a leftover at `deployment_state "in_progress"`,
// stamped between the deployment's own started_at and deployed_at, was
// refused as "never deployed" — with `deployment_state "in_progress"`
// printed in the same sentence as "Nothing is deploying, so the branch is
// safe to delete".
//
// Two rules came out of it, and they are separate:
//
//   - For a `deployment_state` [drStates] documents, the state table is the
//     WHOLE verdict. `deployable` answers a question about the fresh path
//     ("will PlanetScale accept a Deploy call"), not about how far along an
//     already-issued deployment is, so it is consulted only where the table
//     has nothing to say — an undocumented state.
//   - A refusal must not assert a condition the evidence it just printed
//     denies. "Nothing is deploying, delete the branch" is a CLAIM, and it
//     is made only from a state that establishes it; where sluice does not
//     know, it says it does not know and names no delete
//     ([adoptRefuseUnrecognizedState]).
//
// # How sluice knows the branch is its own (the identity argument)
//
// Three pieces of evidence, all cheap:
//
//  1. The BRANCH NAME. [legBranchName] is `sluice-<kind>-<sha256(scope,
//     ddl)[:10]>`, computed from the DDL this run intends. A branch at that
//     name was created by a sluice run intending BYTE-IDENTICAL DDL for the
//     same scope — which is also why the item's "verify the DDL matches"
//     gotcha needs no DDL comparison: an index set that changed between
//     attempts hashes to a DIFFERENT branch name, so the stale branch is
//     never found and the fresh path runs.
//  2. The deploy request was opened FROM that branch (`branch`) and merges
//     INTO this run's production branch (`into_branch`). A request aimed
//     anywhere else is refused.
//  3. There is EXACTLY ONE such deploy request. Two is ambiguous and sluice
//     will not pick.
//
// What that does NOT rule out, stated because it is the residual risk: a
// person (or another tool) who creates a branch at that exact name and
// opens a deploy request from it, and a collision in the 40-bit truncated
// hash. sluice does not read the DDL back off the branch — PlanetScale
// renders schema, not statements, and comparing rendered schema to intent
// would mean parsing DDL, which this codebase does not do.

package expandcontract

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// legBranchPrefix opens every dev-branch name [legBranchName] mints. The
// adopt path asserts it before touching anything: adoption reasons from
// "sluice derived this name", so a caller that ever passes an
// operator-chosen branch name into [legRunner.run] must refuse rather than
// inherit the ownership argument it does not have.
const legBranchPrefix = "sluice-"

// adoptVerdict is what the leftover deploy request's state says to do with
// it. The three refusal verdicts are separate values (rather than one
// "refuse") because the REMEDY differs: "delete the branch" is correct for
// two of them and wrong for none only because adoption removed the case
// where it was catastrophic.
type adoptVerdict int

const (
	// adoptJoin — the deploy is issued and running (or parked on the
	// human cutover gate): hand it to the shared poller.
	adoptJoin adoptVerdict = iota

	// adoptFinalize — the deploy already landed: close the revert window
	// and treat the leg as done.
	adoptFinalize

	// adoptRefuseNeverDeployed — the request exists but was never
	// deployed, so the pre-deploy guarantees would have to be re-made and
	// cannot be. Nothing is in flight; the branch delete is safe.
	adoptRefuseNeverDeployed

	// adoptRefuseAlreadyDeployed — `no_changes`: the diff is empty because
	// this leg's DDL is already on production.
	adoptRefuseAlreadyDeployed

	// adoptRefuseFailed — a terminal failure state, or a request closed
	// without ever deploying. The schema change is not applied and the
	// request cannot be revived; here the branch delete is genuinely the
	// right advice.
	adoptRefuseFailed

	// adoptRefuseUnrecognizedState — the `deployment_state` is not one
	// [drStates] documents AND PlanetScale reports the request
	// deployable. sluice cannot tell from that pair whether something is
	// running, so this refusal says so and names NO branch delete: it is
	// the only refusal whose honest content is "I do not know".
	adoptRefuseUnrecognizedState
)

// notYetDeployedStates are the `deployment_state` values that mean the
// Deploy call has not been made. They are the first three entries of
// [drStates]' "before the deploy call" group; `queued` is deliberately
// ABSENT, because PlanetScale queues a deployment AFTER it is requested
// (the ADR-0148 observed walk is open/pending → ready → queued → … →
// complete_pending_revert).
//
// For a state [drStates] documents, this table is the WHOLE answer —
// `deployable` is deliberately not consulted, because it does not mean what
// its name suggests. PlanetScale keeps reporting deployable=true on a
// request it is actively deploying (live-measured, Bug 231: deployment_state
// "in_progress", deployable=true, stamped strictly between the deployment's
// own started_at and deployed_at), so ORing the flag into this decision
// refused the exact mid-deploy leftover adoption exists to join — and
// [api.DeployRequest.CanDeploy] answers "will PlanetScale accept a Deploy
// call", which is a question about the fresh path, not about how far along
// an already-issued deployment is.
//
// The flag survives as a refusal signal for ONE case only: a
// `deployment_state` sluice has never seen. There the state table has
// nothing to say, and "PlanetScale would still accept a deploy call" is
// evidence the request is probably pre-deploy — enough to refuse on, never
// enough to claim nothing is running (see [adoptRefuseUnrecognizedState]).
var notYetDeployedStates = map[string]bool{
	"pending":    true,
	"ready":      true,
	"no_changes": true,
}

// classifyAdoption decides what to do with a leftover deploy request from
// its GET-by-number response alone. Pure, so the state matrix can be pinned
// without a server.
func classifyAdoption(dr *api.DeployRequest) adoptVerdict {
	class, documented := drStates[dr.DeploymentState]
	switch {
	case documented && class == drSuccess:
		return adoptFinalize
	case documented && class == drFailure:
		return adoptRefuseFailed
	case dr.DeploymentState == "no_changes":
		return adoptRefuseAlreadyDeployed
	case notYetDeployedStates[dr.DeploymentState]:
		return adoptRefuseNeverDeployed
	case dr.State == "closed":
		// Closed, and not in a terminal state sluice recognizes — it was
		// closed without ever deploying. Checked AFTER the success class on
		// purpose: PlanetScale closes a deploy request when it DOES deploy
		// too, so `state == "closed"` alone means nothing.
		return adoptRefuseFailed
	case documented:
		// drInFlight past the deploy call, and drHumanGate — waitDeployed
		// owns both (it downgrades pending_cutover on an auto_cutover
		// deployment and stops on a real human gate). The state is the
		// whole answer here; `deployable` is not consulted (Bug 231).
		return adoptJoin
	case dr.CanDeploy():
		// Undocumented state, and PlanetScale would still take a Deploy
		// call: probably pre-deploy, certainly not something to hand a
		// poller that could wait on it forever. See [notYetDeployedStates].
		return adoptRefuseUnrecognizedState
	default:
		// Undocumented and not deployable — the same tolerance
		// [classifyDeployState] extends: a new PlanetScale intermediate
		// state must not fail a healthy deploy.
		return adoptJoin
	}
}

// adoptLeftoverBranch is [legRunner.run]'s branch when the leg's dev branch
// is already there. It returns the leg's final deploy request when the
// leftover was adopted, and a coded refusal otherwise.
func (r *legRunner) adoptLeftoverBranch(ctx context.Context, branchName string, cleanup *branchCleanup) (*api.DeployRequest, error) {
	if !strings.HasPrefix(branchName, legBranchPrefix) {
		return nil, r.adoptRefusal(fmt.Sprintf(
			"dev branch %q already exists, and its name is not one sluice derived (no %q prefix) — the ownership argument adoption rests on does not hold for it, so sluice will neither adopt nor delete it",
			branchName, legBranchPrefix,
		), "inspect the branch in PlanetScale; sluice only ever adopts a dev branch whose name it derived from its own DDL")
	}

	dr, err := r.leftoverDeployRequest(ctx, branchName)
	if err != nil {
		return nil, err
	}

	verdict := classifyAdoption(dr)
	if refusal := r.adoptRefusalFor(verdict, branchName, dr); refusal != nil {
		return nil, refusal
	}

	switch verdict {
	case adoptFinalize:
		fmt.Fprintf(r.out, "%s: adopting deploy request #%d left by an earlier run on dev branch %q — it already deployed (deployment_state %q); finalizing\n",
			r.name, dr.Number, branchName, dr.DeploymentState)
		cleanup.add(branchName)
		r.finalizeRevertWindow(ctx, dr)
		fmt.Fprintf(r.out, "%s: deploy request #%d deployed\n", r.name, dr.Number)
		return dr, nil

	default: // adoptJoin — adoptRefusalFor answered every other verdict.
		fmt.Fprintf(r.out, "%s: adopting deploy request #%d left by an earlier run on dev branch %q — it is still deploying (deployment_state %q%s); joining it instead of starting over\n",
			r.name, dr.Number, branchName, dr.DeploymentState, progressSuffix(dr))
		// The branch is ours to tear down again from here on: every exit
		// path below is one the fresh path also owns the branch on, and
		// keepBranchOnRequest exempts the two where PlanetScale still needs
		// it.
		cleanup.add(branchName)
		final, err := r.waitDeployed(ctx, branchName, dr.Number, newDeployProgressReporter(r, dr.Number))
		if err != nil {
			return nil, r.keepBranchOnRequest(err, branchName, cleanup)
		}
		r.finalizeRevertWindow(ctx, final)
		fmt.Fprintf(r.out, "%s: deploy request #%d deployed\n", r.name, dr.Number)
		return final, nil
	}
}

// adoptRefusalFor renders the coded refusal for a verdict that does not
// adopt, and returns nil for the two that do. Split out of
// [legRunner.adoptLeftoverBranch] so every refusal TEXT can be graded
// against the state that produced it without driving a control plane —
// see TestAdoptRefusalNeverClaimsNothingIsDeployingAboutARunningState.
//
// The rule those texts obey, and the second half of Bug 231: a refusal must
// not assert a state the evidence it just printed denies. Telling an
// operator "nothing is deploying, delete the branch" one line after
// printing `deployment_state "in_progress"` is worse than the refusal
// itself — believed, it discards a healthy in-flight deploy. So the delete
// claim is made ONLY where the named state establishes it, and the one
// verdict that means "sluice does not know" says exactly that.
func (r *legRunner) adoptRefusalFor(verdict adoptVerdict, branchName string, dr *api.DeployRequest) error {
	switch verdict {
	case adoptFinalize, adoptJoin:
		return nil

	case adoptRefuseAlreadyDeployed:
		return r.adoptRefusal(fmt.Sprintf(
			"dev branch %q is left over from an earlier run and its deploy request #%d has NO schema changes (deployment_state %q) — this leg's DDL is already on %q; %s. %s (%s)",
			branchName, dr.Number, dr.DeploymentState, r.branch, r.alreadyDeployedAdvice,
			r.safeToDeleteAdvice(branchName), dr.HTMLURL,
		), "the leg's DDL is already deployed — delete the leftover dev branch and continue past this leg")

	case adoptRefuseNeverDeployed:
		// Reachable only for `pending` / `ready` — PlanetScale has not been
		// asked to deploy, so "nothing is deploying" is what the printed
		// deployment_state says, not a guess layered on top of it.
		return r.adoptRefusal(fmt.Sprintf(
			"dev branch %q is left over from an earlier run and its deploy request #%d was never deployed (state %q, deployment_state %q, deployable=%t) — sluice adopts only a deploy request that is already deploying, because it cannot re-establish for a branch it did not provision this run the two things it checks before every deploy: that the branch's schema base still matches %q, and that the request's diff touches nothing outside %s. Deploying from a base that has fallen behind silently reverts newer production schema. %s, then %s (%s)",
			branchName, dr.Number, dr.State, dr.DeploymentState, dr.CanDeploy(), r.branch,
			r.intendedObjects(), r.safeToDeleteAdvice(branchName), r.rerunAdvice, dr.HTMLURL,
		), "delete the leftover dev branch and re-run — nothing is deploying, so nothing is lost, and the re-run re-provisions the branch from current production")

	case adoptRefuseUnrecognizedState:
		// The "I do not know" refusal. It names no branch delete, for the
		// same reason the truncated-list refusal does not: the wrong guess
		// here is "nothing is running", and that guess destroys a build.
		return r.adoptRefusal(fmt.Sprintf(
			"dev branch %q is left over from an earlier run and its deploy request #%d reports a deployment_state sluice does not recognize (state %q, deployment_state %q, deployable=%t) — sluice will not adopt it, and will not advise deleting the dev branch, because it cannot tell from an unknown state whether a deployment is running; deleting a dev branch mid-deploy discards the build (%s)",
			branchName, dr.Number, dr.State, dr.DeploymentState, dr.CanDeploy(), dr.HTMLURL,
		), "open that deploy request in PlanetScale and see what it is doing; do NOT delete the dev branch until you have confirmed nothing is deploying from it")

	default: // adoptRefuseFailed
		if drStates[dr.DeploymentState] == drFailure {
			return r.adoptRefusal(fmt.Sprintf(
				"dev branch %q is left over from an earlier run and its deploy request #%d cannot be resumed (state %q, deployment_state %q) — that is a terminal state in which the schema change is NOT applied. %s, then %s (%s)",
				branchName, dr.Number, dr.State, dr.DeploymentState,
				r.safeToDeleteAdvice(branchName), r.rerunAdvice, dr.HTMLURL,
			), "the deploy request failed and cannot be revived — delete the leftover dev branch and re-run")
		}
		// Closed, in a state that is not a terminal failure sluice knows.
		// PlanetScale will not deploy a closed request, so re-running is
		// right — but the state does not say whether anything already ran,
		// and this message must not pretend otherwise.
		return r.adoptRefusal(fmt.Sprintf(
			"dev branch %q is left over from an earlier run and its deploy request #%d cannot be resumed: it is CLOSED, and PlanetScale does not deploy a closed request. sluice cannot tell from deployment_state %q whether the schema change already ran, so confirm on the deploy-request page that nothing is in flight, then delete the dev branch with `pscale branch delete %s %s --org %s` and %s (%s)",
			branchName, dr.Number, dr.DeploymentState,
			r.database, branchName, r.org, r.rerunAdvice, dr.HTMLURL,
		), "the deploy request is closed and cannot be revived — check it in PlanetScale, then delete the leftover dev branch and re-run")
	}
}

// leftoverDeployRequest finds the ONE deploy request opened from the
// leftover dev branch and re-fetches it BY NUMBER. The list is used purely
// for discovery (see [api.DeployRequestRef]); every field the verdict rides
// on comes from the by-number GET, whose response shape is the live-captured
// one.
func (r *legRunner) leftoverDeployRequest(ctx context.Context, branchName string) (*api.DeployRequest, error) {
	refs, err := r.api.ListDeployRequests(ctx, r.org, r.database)
	if err != nil {
		// Deliberately NOT folded into the "no deploy request" refusal: that
		// one's remedy is "delete the branch", and handing it to someone
		// whose deploy request sluice merely failed to enumerate would
		// destroy a running deployment.
		if errors.Is(err, api.ErrDeployRequestListTruncated) {
			return nil, r.adoptRefusal(fmt.Sprintf(
				"dev branch %q is left over from an earlier run, and %s/%s has more deploy requests than sluice enumerates — it cannot tell whether one of them is deploying from that branch, and will not guess: the wrong guess is \"there is none\", whose remedy is a branch delete that would destroy a running deployment",
				branchName, r.org, r.database,
			), "find the deploy request for that dev branch in PlanetScale yourself and let it finish; do NOT delete the branch until you have confirmed nothing is deploying from it")
		}
		return nil, fmt.Errorf("%s: list deploy requests to adopt dev branch %q: %w", r.errPrefix, branchName, err)
	}

	var numbers []int
	for _, ref := range refs {
		if ref.Branch != branchName {
			continue
		}
		if ref.IntoBranch != r.branch {
			return nil, r.adoptRefusal(fmt.Sprintf(
				"dev branch %q is left over from an earlier run, but its deploy request #%d merges into %q, not this run's production branch %q — sluice will not adopt a deploy request aimed somewhere else",
				branchName, ref.Number, ref.IntoBranch, r.branch,
			), "inspect that deploy request in PlanetScale; re-run against the branch it targets, or close it and delete the dev branch")
		}
		numbers = append(numbers, ref.Number)
	}
	sort.Ints(numbers)

	switch len(numbers) {
	case 0:
		return nil, r.adoptRefusal(fmt.Sprintf(
			"dev branch %q is left over from an earlier run but has NO deploy request — its DDL was never shipped anywhere, so nothing is deploying and the branch is safe to delete: `pscale branch delete %s %s --org %s`, then %s",
			branchName, r.database, branchName, r.org, r.rerunAdvice,
		), "delete the leftover dev branch and re-run — no deploy request was ever opened from it, so nothing is in flight")
	case 1:
	default:
		return nil, r.adoptRefusal(fmt.Sprintf(
			"dev branch %q is left over from an earlier run and has %d deploy requests (%s) — sluice will not guess which one is its own",
			branchName, len(numbers), joinInts(numbers),
		), "inspect those deploy requests in PlanetScale, close the ones that do not belong, then re-run")
	}

	dr, err := r.api.GetDeployRequest(ctx, r.org, r.database, numbers[0])
	if err != nil {
		return nil, fmt.Errorf("%s: read deploy request #%d to adopt dev branch %q: %w", r.errPrefix, numbers[0], branchName, err)
	}
	return dr, nil
}

// finalizeRevertWindow closes a deployment's revert window when it holds
// one. Shared with [legRunner.run] so the adopted leg finalizes exactly the
// way a leg sluice started does — including the WARN-not-fail posture: the
// schema change IS applied at this point, so a failed skip-revert is the
// operator's to finish from the DR page (ADR-0148 finding #4).
func (r *legRunner) finalizeRevertWindow(ctx context.Context, final *api.DeployRequest) {
	if final.DeploymentState != "complete_pending_revert" {
		return
	}
	if _, err := r.api.SkipRevert(ctx, r.org, r.database, final.Number); err != nil {
		slog.WarnContext(ctx, r.errPrefix+": skip-revert failed; finalize the deployment manually from the deploy-request page",
			"deploy_request", final.Number, "url", final.HTMLURL, "err", err.Error())
	}
}

// safeToDeleteAdvice renders the delete-is-safe CLAIM, in one place so it
// can be graded in one place. Every use of it asserts something about the
// world — that no deployment is running on the dev branch — so the
// literal phrase exists exactly once in the codebase and
// TestAdoptRefusalNeverClaimsNothingIsDeployingAboutARunningState greps
// for it across the whole state matrix. A refusal that cannot support the
// claim writes its own sentence and does not call this.
func (r *legRunner) safeToDeleteAdvice(branchName string) string {
	return fmt.Sprintf("Nothing is deploying, so the branch is safe to delete: `pscale branch delete %s %s --org %s`",
		r.database, branchName, r.org)
}

// intendedObjects renders the leg's intended blast radius for the
// never-deployed refusal, degrading to prose for the one leg that has none
// (deploy-ddl ships arbitrary operator DDL sluice does not parse).
func (r *legRunner) intendedObjects() string {
	if len(r.expectedDiffTables) == 0 {
		return "what this leg intends to change"
	}
	return strings.Join(r.expectedDiffTables, ", ")
}

// adoptRefusal wraps one adoption refusal in the coded shape. Every caller
// states what was FOUND and what to do with the branch; only the shapes
// where nothing is deploying mention deleting it.
func (r *legRunner) adoptRefusal(msg, remedy string) error {
	return sluicecode.Wrap(sluicecode.CodePSDevBranchNotAdoptable, remedy,
		fmt.Errorf("%s: %s", r.errPrefix, msg))
}

func joinInts(ns []int) string {
	parts := make([]string, 0, len(ns))
	for _, n := range ns {
		parts = append(parts, fmt.Sprintf("#%d", n))
	}
	return strings.Join(parts, ", ")
}
