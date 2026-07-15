// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # LegRunner: the generic dev-branch → DDL → deploy-request machine
//
// One branch-provision (with the ADR-0162 stale-base freshness gate) +
// branch-password mint + verbatim DDL apply + deploy-request open/poll/
// deploy/finalize cycle, with the dev branch deleted afterwards — the
// shared machinery under the expand/contract legs, the ADR-0148
// deploy-request index fallback (its first consumer), and any future
// deploy-request feature (`sluice deploy-ddl`, roadmap item 66).
//
// NEW FILE ON PURPOSE (2026-07-15): the sibling item-66 chunk is
// refactoring this package's existing files concurrently, so this runner
// is deliberately self-contained instead of rewiring [Orchestrator] onto
// it — the Orchestrator's runDeployLeg keeps its own (message-hardcoded)
// copy of this flow for now. MERGE-TIME RECONCILIATION: once both chunks
// land, converge runDeployLeg onto LegRunner (its Op/messaging
// parameterization exists precisely so the expand/contract legs can ride
// it without wording changes leaking across commands).
//
// What LegRunner deliberately does NOT do: the safe-migrations preflight
// (callers gate on it once per run — see [IndexFallback] / the
// Orchestrator's preflightControlPlane) and any data-plane work.

package expandcontract

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// LegRunner drives one dev-branch → DDL → deploy-request → deploy →
// finalize cycle against a PlanetScale production branch. Zero value is
// not usable; fill API/Org/Database and call [LegRunner.RunDDLLeg].
type LegRunner struct {
	// API is the shared control-plane client. Required.
	API *api.Client

	// Org, Database, Branch identify the PRODUCTION branch deploy
	// requests merge into. Branch defaults to "main" when empty.
	Org      string
	Database string
	Branch   string

	// Op names the consumer in every error and narration line (e.g.
	// "index-fallback"), so a deploy-request failure inside `migrate`
	// never reads as an expand-contract error.
	Op string

	// PollInterval / DeployTimeout shape the deploy-request and backup
	// state polling. Zero values default to 10s / 1h (the expand-contract
	// defaults).
	PollInterval  time.Duration
	DeployTimeout time.Duration

	// Out receives step-by-step narration. nil falls back to io.Discard.
	Out io.Writer

	// ExecDDL applies one verbatim DDL statement to the dev branch over a
	// direct MySQL connection using the just-minted branch password. nil
	// uses the real go-sql-driver implementation (ddl_exec.go); tests
	// inject a fake.
	ExecDDL func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error

	// KeepBranch skips the end-of-leg dev-branch delete (debugging aid).
	KeepBranch bool
}

// RunDDLLeg runs the full cycle for branchName carrying ddls (applied to
// the dev branch in order, shipped to production in ONE deploy request).
// leftoverGuidance is appended to the refuse-on-leftover error — the
// caller owns the recovery wording (resume flags differ per consumer).
//
// The dev branch this leg creates is deleted on EVERY exit path
// (best-effort with a WARN, on a cancel-immune context) unless KeepBranch
// is set. Deploy-request failures and timeouts return the coded runtime
// [sluicecode.CodePSDeployRequestFailed], always carrying the DR number,
// observed state, and URL.
func (r *LegRunner) RunDDLLeg(ctx context.Context, branchName string, ddls []string, leftoverGuidance string) (dr *api.DeployRequest, err error) {
	out := r.out()

	// Refuse-on-leftover (the ADR-0162 v1 resumability design): a branch
	// with the caller's deterministic name means a previous run died
	// mid-leg. Guessing whether its DDL/DR state is reusable would be the
	// silent path; name it and let the operator decide.
	if _, probeErr := r.API.GetBranch(ctx, r.Org, r.Database, branchName); probeErr == nil {
		return nil, fmt.Errorf(
			"%s: dev branch %q already exists — a previous run left it behind. Inspect its deploy request in PlanetScale, then delete the branch (`pscale branch delete %s %s --org %s`) if its DDL did not deploy. %s",
			r.Op, branchName, r.Database, branchName, r.Org, leftoverGuidance,
		)
	} else if !api.IsNotFound(probeErr) {
		return nil, fmt.Errorf("%s: probe dev branch %q: %w", r.Op, branchName, probeErr)
	}

	created := false
	defer func() {
		if created {
			r.deleteBranchBestEffort(ctx, branchName)
		}
	}()

	if err := r.provisionFreshBranch(ctx, branchName, &created); err != nil {
		return nil, err
	}

	pw, err := r.API.CreateBranchPassword(ctx, r.Org, r.Database, branchName, "sluice-"+r.Op)
	if err != nil {
		return nil, fmt.Errorf("%s: create branch password for %q: %w", r.Op, branchName, err)
	}
	execDDL := r.ExecDDL
	if execDDL == nil {
		execDDL = execBranchDDL
	}
	for _, ddl := range ddls {
		if err := execDDL(ctx, pw, r.Database, ddl); err != nil {
			return nil, fmt.Errorf("%s: apply DDL on dev branch %q: %w", r.Op, branchName, err)
		}
		fmt.Fprintf(out, "%s: applied DDL on %q: %s\n", r.Op, branchName, ddl)
	}

	dr, err = r.API.CreateDeployRequest(ctx, r.Org, r.Database, branchName, r.branch())
	if err != nil {
		return nil, fmt.Errorf("%s: create deploy request %q → %q: %w", r.Op, branchName, r.branch(), err)
	}
	fmt.Fprintf(out, "%s: opened deploy request #%d (%s)\n", r.Op, dr.Number, dr.HTMLURL)

	if err := r.waitDeployable(ctx, branchName, dr.Number); err != nil {
		return nil, err
	}
	if _, err := r.API.Deploy(ctx, r.Org, r.Database, dr.Number); err != nil {
		return nil, fmt.Errorf("%s: deploy request #%d: %w", r.Op, dr.Number, err)
	}
	final, err := r.waitDeployed(ctx, dr.Number)
	if err != nil {
		return nil, err
	}

	// Finalize the revert window: PlanetScale holds a
	// complete_pending_revert deployment "in progress" and blocks
	// lifecycle ops (branch/database deletes) until it closes (ADR-0148
	// finding #4). The schema change itself IS applied at this point, so
	// a skip-revert failure is a loud WARN, not a leg failure.
	if final.DeploymentState == "complete_pending_revert" {
		if _, err := r.API.SkipRevert(ctx, r.Org, r.Database, dr.Number); err != nil {
			slog.WarnContext(ctx, "skip-revert failed; finalize the deployment manually from the deploy-request page",
				"op", r.Op, "deploy_request", dr.Number, "url", final.HTMLURL, "err", err.Error())
		}
	}
	fmt.Fprintf(out, "%s: deploy request #%d deployed\n", r.Op, dr.Number)
	return final, nil
}

// ---- defaults ----

func (r *LegRunner) branch() string {
	if r.Branch == "" {
		return "main"
	}
	return r.Branch
}

func (r *LegRunner) pollInterval() time.Duration {
	if r.PollInterval <= 0 {
		return 10 * time.Second
	}
	return r.PollInterval
}

func (r *LegRunner) deployTimeout() time.Duration {
	if r.DeployTimeout <= 0 {
		return time.Hour
	}
	return r.DeployTimeout
}

func (r *LegRunner) out() io.Writer {
	if r.Out == nil {
		return io.Discard
	}
	return r.Out
}

// ---- branch provisioning (the ADR-0162 stale-base freshness gate) ----

// provisionFreshBranch creates the dev branch and guarantees its schema
// base matches production before any DDL is applied — the same guarantee
// [Orchestrator.provisionFreshBranch] gives the expand/contract legs: a
// new PlanetScale branch's base can LAG production (live-caught
// 2026-07-15, intermittent), and a deploy request from a stale base
// silently REVERTS every production schema change newer than its backup.
// Create → compare via the API → if stale, delete + on-demand backup of
// production + recreate + recheck → still stale is the coded
// [sluicecode.CodePSBranchStaleBase] refusal. created tracks whether a
// branch currently exists for the caller's deferred cleanup.
func (r *LegRunner) provisionFreshBranch(ctx context.Context, branchName string, created *bool) error {
	out := r.out()
	if err := r.createBranchAndWait(ctx, branchName, created); err != nil {
		return err
	}
	stale, err := r.branchBaseStale(ctx, branchName)
	if err != nil {
		return fmt.Errorf("%s: compare dev-branch schema to %q: %w", r.Op, r.branch(), err)
	}
	if !stale {
		return nil
	}

	fmt.Fprintf(out, "%s: dev branch %q came up with a schema older than %q's current one (a new PlanetScale branch's base can lag production); taking a fresh backup to rebase\n",
		r.Op, branchName, r.branch())
	if err := r.API.DeleteBranch(ctx, r.Org, r.Database, branchName); err != nil {
		return fmt.Errorf("%s: delete stale dev branch %q: %w", r.Op, branchName, err)
	}
	*created = false
	if err := r.backupProduction(ctx); err != nil {
		return err
	}
	if err := r.createBranchAndWait(ctx, branchName, created); err != nil {
		return err
	}
	if stale, err = r.branchBaseStale(ctx, branchName); err != nil {
		return fmt.Errorf("%s: recheck dev-branch schema against %q: %w", r.Op, r.branch(), err)
	}
	if stale {
		return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
			"take a fresh backup of the production branch (pscale backup create), then re-run",
			fmt.Errorf(
				"%s: dev branch %q still differs from %q after a fresh backup — deploying from it would silently revert newer production schema; inspect `pscale branch schema %s %s --org %s` vs %q and retry once the schemas converge",
				r.Op, branchName, r.branch(), r.Database, branchName, r.Org, r.branch(),
			))
	}
	fmt.Fprintf(out, "%s: rebased dev branch %q now matches %q\n", r.Op, branchName, r.branch())
	return nil
}

// createBranchAndWait creates the dev branch, flags it for the caller's
// cleanup, and waits for PlanetScale to report it ready.
func (r *LegRunner) createBranchAndWait(ctx context.Context, branchName string, created *bool) error {
	if _, err := r.API.CreateBranch(ctx, r.Org, r.Database, branchName, r.branch()); err != nil {
		return fmt.Errorf("%s: create dev branch %q off %q: %w", r.Op, branchName, r.branch(), err)
	}
	*created = true
	fmt.Fprintf(r.out(), "%s: created dev branch %q off %q\n", r.Op, branchName, r.branch())

	deadline := time.Now().Add(branchReadyTimeout)
	for {
		br, err := r.API.GetBranch(ctx, r.Org, r.Database, branchName)
		if err != nil {
			return fmt.Errorf("%s: poll dev branch %q readiness: %w", r.Op, branchName, err)
		}
		if br.Ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: dev branch %q did not become ready within %s", r.Op, branchName, branchReadyTimeout)
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// branchBaseStale reports whether the dev branch's schema differs from
// the production branch's — the from-a-stale-backup signal. Shares
// [renderSchema] with the Orchestrator so the two gates can never
// disagree on canonicalization.
func (r *LegRunner) branchBaseStale(ctx context.Context, branchName string) (bool, error) {
	dev, err := r.API.GetBranchSchema(ctx, r.Org, r.Database, branchName)
	if err != nil {
		return false, err
	}
	prod, err := r.API.GetBranchSchema(ctx, r.Org, r.Database, r.branch())
	if err != nil {
		return false, err
	}
	return renderSchema(dev) != renderSchema(prod), nil
}

// backupProduction takes an on-demand backup of the production branch and
// polls it to success, bounded by the deploy timeout.
func (r *LegRunner) backupProduction(ctx context.Context) error {
	bk, err := r.API.CreateBackup(ctx, r.Org, r.Database, r.branch())
	if err != nil {
		return fmt.Errorf("%s: create rebase backup of %q: %w", r.Op, r.branch(), err)
	}
	fmt.Fprintf(r.out(), "%s: backup of %q started (rebase base for the dev branch; duration scales with database size)\n", r.Op, r.branch())
	deadline := time.Now().Add(r.deployTimeout())
	for {
		cur, err := r.API.GetBackup(ctx, r.Org, r.Database, r.branch(), bk.ID)
		if err != nil {
			return fmt.Errorf("%s: poll rebase backup: %w", r.Op, err)
		}
		switch cur.State {
		case "success":
			return nil
		case "failed", "canceled", "cancelled":
			return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
				"take a fresh backup of the production branch (pscale backup create), then re-run",
				fmt.Errorf(
					"%s: rebase backup of %q ended %q — a fresh backup is required for the dev branch to see current production schema",
					r.Op, r.branch(), cur.State,
				))
		}
		if time.Now().After(deadline) {
			return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
				"take a fresh backup of the production branch (pscale backup create), then re-run",
				fmt.Errorf(
					"%s: rebase backup of %q still %q after %s — re-run once it completes (the backup keeps running in PlanetScale)",
					r.Op, r.branch(), cur.State, r.deployTimeout(),
				))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// ---- deploy-request polling (the ADR-0162 tolerant classification) ----

// waitDeployable polls until the deploy request is deployable. Terminal
// states are classified by name via the shared [drSuccessStates] /
// [drFailureStates] sets; everything unknown keeps waiting until the
// deadline (a new intermediate PlanetScale state must not fail a healthy
// deploy).
func (r *LegRunner) waitDeployable(ctx context.Context, branchName string, number int) error {
	deadline := time.Now().Add(r.deployTimeout())
	for {
		dr, err := r.API.GetDeployRequest(ctx, r.Org, r.Database, number)
		if err != nil {
			return fmt.Errorf("%s: poll deploy request #%d: %w", r.Op, number, err)
		}
		switch {
		case dr.CanDeploy():
			return nil
		case dr.DeploymentState == "no_changes":
			// The branch's diff against production is empty — the DDL is
			// almost certainly already deployed (a crashed earlier run
			// whose branch was cleaned up). Deploying nothing would
			// silently "succeed", so refuse with the recovery path.
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d has no schema changes — the DDL looks already deployed; close the DR, delete dev branch %q, and re-run (a re-run detects already-deployed objects and skips them)",
				number, branchName,
			))
		case drFailureStates[dr.DeploymentState] || dr.State == "closed":
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d cannot be deployed (state %q, deployment_state %q)",
				number, dr.State, dr.DeploymentState,
			))
		}
		if time.Now().After(deadline) {
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d did not become deployable within %s (deployment_state %q) — if your organization requires deploy-request review, approve it and re-run",
				number, r.deployTimeout(), dr.DeploymentState,
			))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// waitDeployed polls a deploying request to a terminal state.
func (r *LegRunner) waitDeployed(ctx context.Context, number int) (*api.DeployRequest, error) {
	deadline := time.Now().Add(r.deployTimeout())
	for {
		dr, err := r.API.GetDeployRequest(ctx, r.Org, r.Database, number)
		if err != nil {
			return nil, fmt.Errorf("%s: poll deploy request #%d: %w", r.Op, number, err)
		}
		switch {
		case drSuccessStates[dr.DeploymentState]:
			return dr, nil
		case drFailureStates[dr.DeploymentState]:
			return nil, r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d failed (deployment_state %q)", number, dr.DeploymentState,
			))
		}
		if time.Now().After(deadline) {
			return nil, r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d still deploying after %s (deployment_state %q) — the deploy keeps running in PlanetScale; watch it at the URL and re-run once it completes (a re-run detects already-deployed objects and skips them)",
				number, r.deployTimeout(), dr.DeploymentState,
			))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return nil, err
		}
	}
}

// drFailure wraps a deploy-request failure/timeout in the coded runtime
// error, always carrying the DR state and URL.
func (r *LegRunner) drFailure(dr *api.DeployRequest, msg string) error {
	return sluicecode.Wrap(
		sluicecode.CodePSDeployRequestFailed,
		"inspect the deploy request in PlanetScale: "+dr.HTMLURL,
		fmt.Errorf("%s: %s: %s", r.Op, msg, dr.HTMLURL),
	)
}

func (r *LegRunner) sleepPoll(ctx context.Context) error {
	t := time.NewTimer(r.pollInterval())
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// deleteBranchBestEffort tears the leg's dev branch down on a
// cancel-immune context (a Ctrl-C mid-deploy still cleans up), WARNing
// instead of failing — the deploy outcome is already decided by the time
// cleanup runs. Honors KeepBranch.
func (r *LegRunner) deleteBranchBestEffort(ctx context.Context, branchName string) {
	if r.KeepBranch {
		fmt.Fprintf(r.out(), "%s: keeping dev branch %q\n", r.Op, branchName)
		return
	}
	deleteCtx := context.WithoutCancel(ctx)
	if err := r.API.DeleteBranch(deleteCtx, r.Org, r.Database, branchName); err != nil && !api.IsNotFound(err) {
		slog.WarnContext(deleteCtx, "could not delete dev branch; delete it manually",
			"op", r.Op, "branch", branchName, "err", err.Error())
		return
	}
	fmt.Fprintf(r.out(), "%s: deleted dev branch %q\n", r.Op, branchName)
}
