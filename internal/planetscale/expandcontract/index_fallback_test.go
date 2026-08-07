// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0148 deploy-request index-build fallback (control-
// plane half), against the same fakePS harness the expand-contract tests
// drive: the full branch → DDL → DR → deploy → finalize → cleanup cycle,
// the safe-migrations-off / preflight-failure unavailability shapes (the
// engine's revert-to-hint contract rides on them), the once-per-run
// preflight cache, the deterministic-branch leftover refusal, the coded
// deploy-request failure, and the stale-base freshness gate riding the
// shared legRunner (ADR-0165).

package expandcontract

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// newTestIndexFallback wires a happy-path fallback against ps; tests
// mutate what they need.
func newTestIndexFallback(t *testing.T, ps *fakePS) (*IndexFallback, *ddlRecorder) {
	t.Helper()
	if ps.drDiffs == nil {
		// The blast-radius gate refuses an EMPTY diff on legs with an
		// intended set (fail-closed tripwire); these tests route the
		// "orders" table.
		ps.drDiffs = []string{"orders"}
	}
	_, client := ps.serve()
	rec := &ddlRecorder{}
	return &IndexFallback{
		API:           client,
		Org:           "o",
		Database:      "d",
		PollInterval:  time.Millisecond,
		DeployTimeout: 5 * time.Second,
		ExecDDL:       rec.exec,
	}, rec
}

var indexFallbackDDLs = []string{
	"ALTER TABLE `orders` ADD INDEX `idx_a` (`id`), ADD INDEX `idx_b` (`val`);",
	"ALTER TABLE `orders` ADD FULLTEXT INDEX `ft_body` (`body`);",
}

// TestIndexFallback_HappyPath pins the whole cycle: both DDL statements
// land on ONE deterministic dev branch, ship in ONE deploy request,
// finalize through skip-revert, and the branch is deleted afterwards.
func TestIndexFallback_HappyPath(t *testing.T) {
	ps := newFakePS(t)
	f, rec := newTestIndexFallback(t, ps)

	if err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub()); err != nil {
		t.Fatalf("BuildIndexDDL = %v; want nil", err)
	}
	if len(rec.ddls) != 2 || rec.ddls[0] != indexFallbackDDLs[0] || rec.ddls[1] != indexFallbackDDLs[1] {
		t.Errorf("branch DDLs = %q; want both statements in order", rec.ddls)
	}
	wantBranch := indexFallbackBranchName("orders", indexFallbackDDLs)
	if !strings.HasPrefix(wantBranch, "sluice-index-") {
		t.Fatalf("branch name %q lost the sluice-index- prefix", wantBranch)
	}
	if len(ps.deleted) != 1 || ps.deleted[0] != wantBranch {
		t.Errorf("deleted branches = %v; want [%s] (cleanup always runs)", ps.deleted, wantBranch)
	}
	if len(ps.drs) != 1 {
		t.Errorf("deploy requests = %d; want 1 (one branch/DR per table)", len(ps.drs))
	}
	for _, dr := range ps.drs {
		if !dr.deployed || !dr.skipRevert {
			t.Errorf("DR deployed=%v skipRevert=%v; want both true", dr.deployed, dr.skipRevert)
		}
	}
}

// TestIndexFallback_SafeMigrationsOffIsUnavailable pins the item-67
// posture: safe migrations OFF is never toggled and never fatal here —
// the fallback reports the sentinel (the engine then surfaces the
// original direct error + hint) and touches nothing.
func TestIndexFallback_SafeMigrationsOffIsUnavailable(t *testing.T) {
	ps := newFakePS(t)
	ps.safeMigrations = false
	f, rec := newTestIndexFallback(t, ps)

	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	if !errors.Is(err, ir.ErrIndexBuildFallbackUnavailable) {
		t.Fatalf("BuildIndexDDL = %v; want the unavailability sentinel", err)
	}
	if !strings.Contains(err.Error(), "safe migrations") {
		t.Errorf("unavailability reason should name the toggle: %v", err)
	}
	if len(rec.ddls) != 0 || len(ps.branches) != 1 {
		t.Errorf("an unavailable fallback must not touch the control plane: ddls=%v branches=%d", rec.ddls, len(ps.branches))
	}
}

// TestIndexFallback_PreflightFailureIsUnavailable pins the broken-token /
// wrong-org shape: any control-plane preflight failure is unavailability
// (revert to the status quo), never a new failure mode for the migrate.
func TestIndexFallback_PreflightFailureIsUnavailable(t *testing.T) {
	ps := newFakePS(t)
	f, _ := newTestIndexFallback(t, ps)
	f.Branch = "no-such-branch"

	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	if !errors.Is(err, ir.ErrIndexBuildFallbackUnavailable) {
		t.Fatalf("BuildIndexDDL = %v; want the unavailability sentinel", err)
	}
}

// TestIndexFallback_PreflightCachedAcrossTables pins the once-per-run
// preflight: routing a second table consults the cached verdict instead
// of re-GETting the production branch.
func TestIndexFallback_PreflightCachedAcrossTables(t *testing.T) {
	ps := newFakePS(t)
	ps.safeMigrations = false
	f, _ := newTestIndexFallback(t, ps)

	for i := 0; i < 2; i++ {
		if err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub()); !errors.Is(err, ir.ErrIndexBuildFallbackUnavailable) {
			t.Fatalf("call %d = %v; want the unavailability sentinel", i, err)
		}
	}
	if got := ps.callCount(); got != 1 {
		t.Errorf("control-plane calls = %d; want 1 (the preflight is cached per run)", got)
	}
}

// TestIndexFallback_LeftoverBranchWithNoDeployRequestRefused pins the
// crash-recovery shape a leftover branch with NOTHING in flight takes: a
// coded refusal that names the branch delete (safe here — no deployment
// exists) and the re-run — never silently reused, and NOT the
// unavailability sentinel (the direct attempt already failed; hiding this
// would loop).
func TestIndexFallback_LeftoverBranchWithNoDeployRequestRefused(t *testing.T) {
	ps := newFakePS(t)
	defer migcore.SetRunningCommand("")
	migcore.SetRunningCommand(string(migcore.CommandMigrate))
	leftover := indexFallbackBranchName("orders", indexFallbackDDLs)
	ps.branches[leftover] = &api.Branch{Name: leftover, Ready: true}

	f, _ := newTestIndexFallback(t, ps)
	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDevBranchNotAdoptable)
	for _, want := range []string{"has NO deploy request", "pscale branch delete", "re-run with --resume"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
	if errors.Is(err, ir.ErrIndexBuildFallbackUnavailable) {
		t.Errorf("a leftover branch is a loud refusal, not unavailability: %v", err)
	}
}

// TestIndexFallback_DeployFailureIsCoded pins the loud DR failure: the
// reused SLUICE-E-PS-DEPLOY-REQUEST-FAILED code, and the branch still
// cleaned up.
func TestIndexFallback_DeployFailureIsCoded(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{"queued", "error"}
	f, _ := newTestIndexFallback(t, ps)

	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if errors.Is(err, ir.ErrIndexBuildFallbackUnavailable) {
		t.Errorf("a deploy failure must surface loudly, not as unavailability: %v", err)
	}
	if want := indexFallbackBranchName("orders", indexFallbackDDLs); len(ps.deleted) != 1 || ps.deleted[0] != want {
		t.Errorf("deleted branches = %v; want [%s] (cleanup runs on failure too)", ps.deleted, want)
	}
}

// TestIndexFallback_StaleBranchRebasedViaBackup pins the ADR-0162
// freshness gate riding the shared legRunner: a dev branch that comes up
// with a stale schema base is deleted, production is backed up, and the
// recreated branch proceeds — without it, the index deploy request would
// silently REVERT newer production schema.
func TestIndexFallback_StaleBranchRebasedViaBackup(t *testing.T) {
	ps := newFakePS(t)
	ps.staleNextBranches = 1
	f, rec := newTestIndexFallback(t, ps)

	if err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, nil); err != nil {
		t.Fatalf("BuildIndexDDL = %v; want nil (rebase self-heals)", err)
	}
	if ps.backups != 1 {
		t.Errorf("backups = %d; want 1 (the stale base forces one rebase backup)", ps.backups)
	}
	if len(rec.ddls) != 2 {
		t.Errorf("branch DDLs = %q; want both statements on the rebased branch", rec.ddls)
	}
}

// TestIndexFallback_ZeroDeployTimeoutWaitsForTheBuild pins the
// zero-value-safe default (the v0.99.51 trap read the right way round):
// the CLI default AND every programmatic construction leave DeployTimeout
// at zero, and zero must mean "wait for the index build", because a large
// table's build is genuinely hours. Field-measured: at the previous 1 h
// default a 106 GB / 153 M-row build was ~29 % done, so the bounded exit
// was the DEFAULT outcome at the scale this fallback exists for.
func TestIndexFallback_ZeroDeployTimeoutWaitsForTheBuild(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{
		"queued", "submitting", "in_progress", "in_progress",
		"in_progress_cutover", "complete_pending_revert",
	}
	f, _ := newTestIndexFallback(t, ps)
	f.DeployTimeout = 0 // the default

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.BuildIndexDDL(ctx, "orders", indexFallbackDDLs, err3024Stub()); err != nil {
		t.Fatalf("BuildIndexDDL = %v; want nil (a zero deploy timeout waits for the build)", err)
	}
}

// TestIndexFallback_BoundedTimeoutIsIncompleteNotFailed pins the reframed
// exit as migrate's operator sees it: the non-failure code, the
// `--planetscale-deploy-timeout` flag name (NOT expand-contract's
// `--deploy-timeout`), the --resume recovery, and the dev branch left
// completely untouched.
func TestIndexFallback_BoundedTimeoutIsIncompleteNotFailed(t *testing.T) {
	ps := newFakePS(t)
	// The `--resume` assertion below is migrate's text: this is a
	// `migrate` operator's exit, and the same line reads "re-run the same
	// command" on `sync start` / `restore`, which have no --resume.
	defer migcore.SetRunningCommand("")
	migcore.SetRunningCommand(string(migcore.CommandMigrate))
	ps.postStates = []string{"in_progress"} // never terminal
	ps.opPercents = []int{29}
	f, _ := newTestIndexFallback(t, ps)
	f.DeployTimeout = 50 * time.Millisecond

	err := f.BuildIndexDDL(context.Background(), "orders", indexFallbackDDLs, err3024Stub())
	wantCode(t, err, sluicecode.CodePSDeployRequestIncomplete)
	if errors.Is(err, ir.ErrIndexBuildFallbackUnavailable) {
		t.Errorf("a still-running deploy is not fallback unavailability: %v", err)
	}
	for _, want := range []string{
		"still deploying",
		"nothing failed",
		"29% complete",
		"--planetscale-deploy-timeout was set to 50ms",
		"re-run with --resume",
		"LEFT ALONE",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("incomplete error %q missing %q", err.Error(), want)
		}
	}
	if ps.deleteAttempts != 0 {
		t.Errorf("branch DELETE attempted %d time(s); the running index build depends on the dev branch — deleting it is exactly the advice this change removes", ps.deleteAttempts)
	}
	if want := indexFallbackBranchName("orders", indexFallbackDDLs); !ps.branchExists(want) {
		t.Errorf("dev branch %q is gone; the in-flight deployment needed it", want)
	}
}

// err3024Stub is the cause the engine would hand over — the tests only
// assert it is threaded, not interpreted.
func err3024Stub() error {
	return errors.New("Error 3024: maximum statement execution time exceeded")
}
