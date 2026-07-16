// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the review-timeout cleanup exemption (audit L-D0-10): the
// deployable-wait timeout's remedy is "approve the deploy request and
// re-run", but the deferred branch cleanup used to DELETE the dev
// branch — closing the still-open DR the operator was just told to
// approve. On exactly that path the branch is now kept (auto
// --keep-branches semantics) and the timeout message names the kept
// branch; every other failure path still cleans up (pinned by the
// existing gate/DR-state tests).

package expandcontract

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestLegRunner_ReviewTimeoutKeepsBranchAndNamesIt pins the leg-level
// shape: a deploy request that never becomes deployable (an org's
// review queue) times out coded, the message names the KEPT branch and
// the post-close delete recipe, and cleanup leaves the branch alone.
func TestLegRunner_ReviewTimeoutKeepsBranchAndNamesIt(t *testing.T) {
	ps := newFakePS(t)
	ps.preStates = []string{"pending"} // last entry repeats: never deployable
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.deployTimeout = 50 * time.Millisecond

	_, err := r.run(context.Background(), "sluice-gate-review", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	for _, want := range []string{
		"did not become deployable",
		// The manual escape names BOTH steps — approval alone deploys
		// nothing because sluice never sets auto-apply (audit 2026-07-16).
		"approve it AND deploy it from the PlanetScale UI",
		"approval alone deploys nothing",
		"then approve it and re-run",                // the injected reviewTimeoutAdvice, spliced after the escape
		`"sluice-gate-review" was KEPT`,             // the exemption, named
		"pscale branch delete d sluice-gate-review", // the post-close recipe
		"would close the still-open deploy request", // the why
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("timeout error %q missing %q", err.Error(), want)
		}
	}
	if anyDeployed(ps) {
		t.Error("deploy was called despite the review timeout")
	}

	cleanup.run(context.Background())
	if len(ps.deleted) != 0 {
		t.Errorf("cleanup deleted %v; the review-timeout branch must survive", ps.deleted)
	}
	if !ps.branchExists("sluice-gate-review") {
		t.Error("dev branch is gone; deleting it closes the DR the operator was told to approve")
	}
}

// TestExpandContract_ReviewTimeoutKeepsBranch drives the orchestrator
// end to end: the expand leg's DR sits un-deployable past
// --deploy-timeout ⇒ coded failure, and the deferred cleanup keeps the
// expand dev branch (the DR stays open for the operator's approval).
func TestExpandContract_ReviewTimeoutKeepsBranch(t *testing.T) {
	ps := newFakePS(t)
	ps.preStates = []string{"pending"}
	o, _, _, _ := newTestOrchestrator(t, ps)
	o.DeployTimeout = 50 * time.Millisecond

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if !strings.Contains(err.Error(), "was KEPT") {
		t.Errorf("error %q should say the branch was kept", err.Error())
	}
	if len(ps.deleted) != 0 {
		t.Errorf("deleted = %v; want the expand branch kept while its DR awaits review", ps.deleted)
	}
	if !ps.branchExists(o.expandBranchName()) {
		t.Error("expand dev branch is gone; its still-open DR was orphaned")
	}
}
