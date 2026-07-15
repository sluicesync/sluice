// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the deploy-ddl deployer (ADR-0165) against the same
// fakePS harness the expand-contract orchestrator uses: the happy
// single-leg path, the preflight refusal, the leftover-branch refusal,
// the stale-base freshness gate (self-heal + still-stale coded), the
// DR failure/no_changes/timeout shapes with deploy-ddl's own guidance,
// cleanup posture, and the --dry-run zero-control-plane-calls pin.

package expandcontract

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// newTestDeployer wires a full happy-path deployer; tests mutate what
// they need.
func newTestDeployer(t *testing.T, ps *fakePS) (*DDLDeployer, *ddlRecorder, *bytes.Buffer) {
	t.Helper()
	_, client := ps.serve()
	rec := &ddlRecorder{}
	out := &bytes.Buffer{}
	return &DDLDeployer{
		API:           client,
		Org:           "o",
		Database:      "d",
		DDL:           "CREATE TABLE IF NOT EXISTS `sluice_cdc_state` (stream_id VARCHAR(255) NOT NULL, PRIMARY KEY (stream_id))",
		PollInterval:  time.Millisecond,
		DeployTimeout: 5 * time.Second,
		Out:           out,
		ExecDDL:       rec.exec,
	}, rec, out
}

func TestDeployDDL_HappyPath(t *testing.T) {
	ps := newFakePS(t)
	d, rec, out := newTestDeployer(t, ps)

	result, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.DeployRequest == 0 {
		t.Error("result carries no deploy-request number")
	}
	if len(rec.ddls) != 1 || rec.ddls[0] != d.DDL {
		t.Errorf("DDLs applied = %q; want exactly the --ddl", rec.ddls)
	}
	// The scripted deploy ends in complete_pending_revert, so the run
	// must have finalized it via skip-revert.
	for n, fdr := range ps.drs {
		if !fdr.skipRevert {
			t.Errorf("DR #%d was not skip-reverted (revert window left open)", n)
		}
	}
	// Cleanup deleted the dev branch, whose name is deterministic on
	// the DDL alone.
	if len(ps.deleted) != 1 || !strings.HasPrefix(ps.deleted[0], "sluice-ddl-") {
		t.Errorf("deleted = %v; want the one sluice-ddl dev branch", ps.deleted)
	}
	if !strings.Contains(out.String(), "deploy-ddl complete") {
		t.Errorf("narration missing the completion line:\n%s", out.String())
	}
}

func TestDeployDDL_RefusesSafeMigrationsDisabled(t *testing.T) {
	ps := newFakePS(t)
	ps.safeMigrations = false
	d, rec, _ := newTestDeployer(t, ps)

	_, err := d.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSSafeMigrationsDisabled)
	if !strings.Contains(err.Error(), "safe migrations") {
		t.Errorf("refusal %q should name the toggle", err)
	}
	// Refused before anything irreversible: no branch, no DDL, no DR.
	if len(ps.branches) != 1 || len(ps.drs) != 0 || len(rec.ddls) != 0 {
		t.Errorf("safe-migrations refusal touched the control plane: branches=%v drs=%d ddls=%q",
			ps.branches, len(ps.drs), rec.ddls)
	}
}

func TestDeployDDL_ValidateRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*DDLDeployer)
		want   string
	}{
		{"missing org", func(d *DDLDeployer) { d.Org = "" }, "Org and Database are required"},
		{"missing ddl", func(d *DDLDeployer) { d.DDL = "  " }, "DDL is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ps := newFakePS(t)
			d, _, _ := newTestDeployer(t, ps)
			tc.mutate(d)
			_, err := d.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Run = %v; want error containing %q", err, tc.want)
			}
			if ps.callCount() != 0 {
				t.Errorf("validation refusal made %d control-plane calls; want 0", ps.callCount())
			}
		})
	}
}

func TestDeployDDL_RefusesLeftoverDevBranch(t *testing.T) {
	ps := newFakePS(t)
	d, _, _ := newTestDeployer(t, ps)
	leftover := d.branchName()
	ps.branches[leftover] = &api.Branch{Name: leftover, Ready: true}

	_, err := d.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "already exists") || !strings.Contains(err.Error(), "nothing left to run") {
		t.Fatalf("Run = %v; want the leftover-branch refusal with deploy-ddl guidance", err)
	}
	// The leftover is NOT ours to delete: cleanup must leave it alone.
	if len(ps.deleted) != 0 {
		t.Errorf("deleted = %v; want the leftover branch untouched", ps.deleted)
	}
}

// TestDeployDDL_StaleBranchRebasedViaBackup pins that the ADR-0162
// freshness gate rides along unchanged: deploy-ddl's whole reason to
// exist over raw pscale CRUD is that a stale dev branch would silently
// propose REVERTING newer production schema.
func TestDeployDDL_StaleBranchRebasedViaBackup(t *testing.T) {
	ps := newFakePS(t)
	ps.staleNextBranches = 1
	d, _, out := newTestDeployer(t, ps)

	result, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.DeployRequest == 0 {
		t.Error("run did not deploy after the rebase")
	}
	if ps.backups != 1 {
		t.Errorf("backups taken = %d; want exactly 1 (the rebase)", ps.backups)
	}
	if !strings.Contains(out.String(), "taking a fresh backup to rebase") {
		t.Errorf("narration missing the rebase explanation:\n%s", out.String())
	}
}

func TestDeployDDL_StillStaleAfterBackupRefusesCoded(t *testing.T) {
	ps := newFakePS(t)
	ps.staleNextBranches = 2 // the rebased branch comes back stale too
	d, _, _ := newTestDeployer(t, ps)

	_, err := d.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSBranchStaleBase)
	if len(ps.drs) != 0 {
		t.Errorf("a deploy request was opened (%d) from a stale base", len(ps.drs))
	}
}

func TestDeployDDL_DeployErrorStateIsCodedWithURL(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{"queued", "error"}
	d, _, _ := newTestDeployer(t, ps)

	_, err := d.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	msg := err.Error()
	if !strings.Contains(msg, `"error"`) || !strings.Contains(msg, "deploy-requests/1") {
		t.Errorf("error %q should carry the DR state and URL", msg)
	}
	// Cleanup ran on failure.
	if len(ps.deleted) != 1 {
		t.Errorf("deleted = %v; want the dev branch cleaned up after the failed deploy", ps.deleted)
	}
}

// TestDeployDDL_NoChangesDiffRefused pins the empty-diff refusal with
// deploy-ddl's own guidance (no resume legs — "already applied" ends
// the story): deploying nothing would silently "succeed".
func TestDeployDDL_NoChangesDiffRefused(t *testing.T) {
	ps := newFakePS(t)
	ps.preStates = []string{"no_changes"}
	d, _, _ := newTestDeployer(t, ps)

	_, err := d.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if !strings.Contains(err.Error(), "no schema changes") || !strings.Contains(err.Error(), "already applied") {
		t.Errorf("no_changes error = %q; want the already-applied guidance", err)
	}
}

func TestDeployDDL_DeployTimeoutIsCoded(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{"in_progress"} // never terminal
	d, _, _ := newTestDeployer(t, ps)
	d.DeployTimeout = 50 * time.Millisecond

	_, err := d.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if !strings.Contains(err.Error(), "still deploying") {
		t.Errorf("timeout error = %q; want the still-deploying guidance", err)
	}
}

func TestDeployDDL_KeepBranchesSkipsCleanup(t *testing.T) {
	ps := newFakePS(t)
	d, _, out := newTestDeployer(t, ps)
	d.KeepBranches = true

	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ps.deleted) != 0 {
		t.Errorf("deleted = %v; want none under --keep-branches", ps.deleted)
	}
	if !strings.Contains(out.String(), "keeping dev branches") {
		t.Error("keep-branches cleanup did not narrate what it kept")
	}
}

func TestDeployDDL_DryRunMakesZeroControlPlaneCalls(t *testing.T) {
	ps := newFakePS(t)
	d, rec, out := newTestDeployer(t, ps)
	d.DryRun = true

	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ps.callCount() != 0 {
		t.Errorf("--dry-run made %d control-plane calls; want ZERO (pinned)", ps.callCount())
	}
	if len(rec.ddls) != 0 {
		t.Errorf("--dry-run applied DDL: %q", rec.ddls)
	}
	plan := out.String()
	for _, want := range []string{"--dry-run", d.branchName(), d.DDL, "safe migrations"} {
		if !strings.Contains(plan, want) {
			t.Errorf("plan missing %q:\n%s", want, plan)
		}
	}
}

func TestDeployDDL_BranchNameDeterministicAndDDLSensitive(t *testing.T) {
	ps := newFakePS(t)
	d, _, _ := newTestDeployer(t, ps)
	a, b := d.branchName(), d.branchName()
	if a != b {
		t.Errorf("branch name not deterministic: %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, "sluice-ddl-") {
		t.Errorf("branch name %q missing prefix", a)
	}
	d.DDL += " ENGINE=InnoDB"
	if c := d.branchName(); c == a {
		t.Errorf("different DDL hashed to the same branch name %q", c)
	}
}
