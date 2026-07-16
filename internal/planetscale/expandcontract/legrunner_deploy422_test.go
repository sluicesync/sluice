// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the deploy still-validating 422 retry (live-caught
// 2026-07-15, first psverify CI dispatch): even after a DR reports
// deployable, POST /deploy can fail HTTP 422 (invalid_params) "We're
// currently validating that these changes are safe to deploy. Please
// try again in a few moments." — a settling state, not a failure. The
// runner retries exactly that shape (bounded, injectable clock), any
// OTHER 422 fails immediately, and the classifier is deliberately
// distinct from the branch-delete race's status-only isDeleteRace422.

package expandcontract

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// deployValidatingMsg is the live-captured message shape.
const deployValidatingMsg = "We're currently validating that these changes are safe to deploy. Please try again in a few moments."

// deployDeleteRaceMsg is the OTHER live 422 in this package (the
// branch-delete settling race) — the cross-contamination pin's probe.
const deployDeleteRaceMsg = "branch cannot be deleted while a deployment is in progress"

func deployCallCount(ps *fakePS) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.deployCalls
}

// TestLegRunner_DeployValidating422RetriesThenDeploys pins the happy
// retry: two still-validating 422s then success — the leg completes
// with no failure and exactly three deploy attempts.
func TestLegRunner_DeployValidating422RetriesThenDeploys(t *testing.T) {
	ps := newFakePS(t)
	ps.deploy422Script = []string{deployValidatingMsg, deployValidatingMsg}
	r, cleanup, _ := newGateLegRunner(t, ps)

	if _, err := r.run(context.Background(), "sluice-gate-validating", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !anyDeployed(ps) {
		t.Fatal("deploy request was not deployed after the validating window settled")
	}
	if got := deployCallCount(ps); got != 3 {
		t.Errorf("deploy attempts = %d; want 3 (two validating 422s + success)", got)
	}
}

// TestLegRunner_DeployValidating422ExhaustsCoded pins the bounded end:
// a DR that never leaves validation exhausts the retry budget and
// surfaces the coded DR failure naming the validating wall.
func TestLegRunner_DeployValidating422ExhaustsCoded(t *testing.T) {
	ps := newFakePS(t)
	for range deployValidatingRetryAttempts + 3 {
		ps.deploy422Script = append(ps.deploy422Script, deployValidatingMsg)
	}
	r, cleanup, _ := newGateLegRunner(t, ps)

	_, err := r.run(context.Background(), "sluice-gate-validating-wall", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if !strings.Contains(err.Error(), "still validating") || !strings.Contains(err.Error(), "currently validating") {
		t.Errorf("refusal %q should name the validating wall and carry the original message", err.Error())
	}
	if got := deployCallCount(ps); got != deployValidatingRetryAttempts {
		t.Errorf("deploy attempts = %d; want exactly the budget %d", got, deployValidatingRetryAttempts)
	}
	if anyDeployed(ps) {
		t.Error("a deploy succeeded despite the always-validating script")
	}
}

// TestLegRunner_DeployOther422FailsImmediately pins the class
// boundary AND the no-cross-contamination guarantee: a 422 whose
// message is NOT the validating shape — probed with the branch-delete
// race's own message, which isDeleteRace422 retries on its endpoint —
// fails the deploy immediately, uncoded, after exactly one attempt.
func TestLegRunner_DeployOther422FailsImmediately(t *testing.T) {
	ps := newFakePS(t)
	ps.deploy422Script = []string{deployDeleteRaceMsg}
	r, cleanup, _ := newGateLegRunner(t, ps)

	_, err := r.run(context.Background(), "sluice-gate-other-422", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	if err == nil {
		t.Fatal("want the immediate deploy failure; got nil")
	}
	if _, coded := sluicecode.FromError(err); coded {
		t.Errorf("non-validating 422 must keep the pre-existing uncoded failure shape; got %v", err)
	}
	if !strings.Contains(err.Error(), deployDeleteRaceMsg) {
		t.Errorf("failure %q should carry the API's message verbatim", err.Error())
	}
	if got := deployCallCount(ps); got != 1 {
		t.Errorf("deploy attempts = %d; want 1 (no retry for other 422s)", got)
	}
}

// TestDeploy422Classifiers_Distinct pins the two 422 classifiers apart
// structurally: the deploy classifier requires the validating message
// (a status-only match would retry genuine parameter errors on an
// endpoint that deploys schema), while the branch-delete classifier is
// status-only by design (a spurious retry there merely delays a
// best-effort WARN).
func TestDeploy422Classifiers_Distinct(t *testing.T) {
	validating := &api.StatusError{Status: http.StatusUnprocessableEntity, PSCode: "invalid_params", Message: deployValidatingMsg}
	deleteRace := &api.StatusError{Status: http.StatusUnprocessableEntity, PSCode: "unprocessable", Message: deployDeleteRaceMsg}
	notFound := &api.StatusError{Status: http.StatusNotFound, Message: deployValidatingMsg}

	if !isDeployValidating422(validating) {
		t.Error("isDeployValidating422(validating 422) = false")
	}
	if isDeployValidating422(deleteRace) {
		t.Error("isDeployValidating422 retried the delete-race 422 — the message gate is load-bearing")
	}
	if isDeployValidating422(notFound) {
		t.Error("isDeployValidating422 matched a non-422 status")
	}
	if isDeployValidating422(errors.New("plain error")) {
		t.Error("isDeployValidating422 matched a non-StatusError")
	}
	// The delete classifier stays status-only: both 422 shapes retry on
	// ITS endpoint, and nothing else does.
	if !isDeleteRace422(validating) || !isDeleteRace422(deleteRace) {
		t.Error("isDeleteRace422 must stay status-only (both 422 shapes match)")
	}
	if isDeleteRace422(notFound) {
		t.Error("isDeleteRace422 matched a non-422 status")
	}
}
