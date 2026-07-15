// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package expandcontract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// legRunner drives ONE dev-branch → DDL → deploy-request → deploy →
// finalize cycle — the ADR-0162 deploy leg, extracted (ADR-0165) so
// expand-contract's two legs and deploy-ddl's single leg compose the
// same machine instead of forking it. Everything command-specific is
// a field: the narration/error prefixes, the optional post-DDL branch
// staging hook, and the operator-guidance strings spliced into the
// shared failure shapes.
type legRunner struct {
	api      *api.Client
	org      string
	database string
	// branch is the PRODUCTION branch deploy requests merge into.
	branch string

	pollInterval  time.Duration
	deployTimeout time.Duration
	out           io.Writer

	// execDDL applies one verbatim DDL statement to the dev branch
	// over a direct MySQL connection (tests inject a fake).
	execDDL func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error

	// name opens narration lines ("expand", "contract", "deploy-ddl");
	// errPrefix opens error messages ("expand-contract expand",
	// "deploy-ddl"); passwordName labels the ephemeral branch password
	// in the PlanetScale UI.
	name         string
	errPrefix    string
	passwordName string

	// stage, when non-nil, runs on the dev branch after the DDL (the
	// expand leg's control-table staging); stageNote is the narration
	// printed after it succeeds (pre-formatted, newline-terminated).
	stage     func(ctx context.Context, pw *api.BranchPassword) error
	stageNote string

	// Command-specific operator guidance spliced into the shared
	// failure shapes (each completes the sentence it is spliced into):
	//
	//	leftoverAdvice        — "…if the DDL already deployed, %s; otherwise…"
	//	alreadyDeployedAdvice — "…the DDL looks already deployed; %s"
	//	reviewTimeoutAdvice   — "…requires deploy-request review, %s"
	//	deployTimeoutAdvice   — "…keeps running in PlanetScale; %s"
	leftoverAdvice        string
	alreadyDeployedAdvice string
	reviewTimeoutAdvice   string
	deployTimeoutAdvice   string
}

// branch-readiness polling gets its own (generous, fixed) deadline:
// branch creation is near-instant next to a deploy, and reusing the
// operator's whole --deploy-timeout here would just delay the real
// error.
const branchReadyTimeout = 5 * time.Minute

// legBranchName derives the DETERMINISTIC dev-branch name for a leg
// from a scope string (the table for expand-contract, empty for
// deploy-ddl) + the leg's DDL, so a re-run after a crash finds (and
// refuses on) its own leftover branch by name instead of silently
// minting sluice-branch litter — the v1 resumability design
// (ADR-0162).
func legBranchName(kind, scope, ddl string) string {
	h := sha256.New()
	h.Write([]byte(scope))
	h.Write([]byte{0})
	h.Write([]byte(ddl))
	return "sluice-" + kind + "-" + hex.EncodeToString(h.Sum(nil))[:10]
}

// preflightSafeMigrations verifies the service token / org / database /
// branch in one GET, then the safe-migrations prerequisite (ADR-0148
// finding #1): deploy requests cannot be created into a branch without
// it. Enabling it is a behavior change on the operator's production
// branch (direct DDL becomes blocked), so sluice REFUSES and names the
// toggle rather than flipping it (contain-complexity tenet); the
// enable/disable propagation lag (finding #7) makes a
// toggle-around-the-run design unsafe anyway. command prefixes the
// messages ("expand-contract", "deploy-ddl").
func preflightSafeMigrations(ctx context.Context, client *api.Client, org, database, branch, command string) error {
	br, err := client.GetBranch(ctx, org, database, branch)
	if err != nil {
		if api.IsNotFound(err) {
			return fmt.Errorf("%s preflight: branch %q of %s/%s not found — check --org/--database/--branch: %w",
				command, branch, org, database, err)
		}
		return fmt.Errorf("%s preflight: read branch %q: %w", command, branch, err)
	}
	if !br.SafeMigrations {
		return sluicecode.Wrap(
			sluicecode.CodePSSafeMigrationsDisabled,
			"enable the branch's \"Safe migrations\" setting in the PlanetScale UI, or run `pscale branch safe-migrations enable "+database+" "+branch+" --org "+org+"` — note this blocks direct DDL on the branch from then on",
			fmt.Errorf("%s: branch %q of %s/%s does not have safe migrations enabled — PlanetScale refuses deploy requests into it, and sluice never enables the toggle for you (it changes how every future schema change on the branch must ship)",
				command, branch, org, database),
		)
	}
	return nil
}

// run drives the full leg: refuse-on-leftover, freshness-gated branch
// provisioning, DDL (+ optional staging), deploy request, deploy,
// skip-revert finalize.
func (r *legRunner) run(ctx context.Context, branchName, ddl string, cleanup *branchCleanup) (*api.DeployRequest, error) {
	out := r.out

	// Refuse-on-leftover: a branch with our deterministic name means a
	// previous run died mid-leg. Guessing whether its DDL/DR state is
	// reusable would be the silent path; name it and let the operator
	// decide.
	if _, err := r.api.GetBranch(ctx, r.org, r.database, branchName); err == nil {
		return nil, fmt.Errorf(
			"%s: dev branch %q already exists — a previous run left it behind. Inspect its deploy request in PlanetScale; if the DDL already deployed, %s; otherwise delete the branch (`pscale branch delete %s %s --org %s`) and re-run",
			r.errPrefix, branchName, r.leftoverAdvice, r.database, branchName, r.org,
		)
	} else if !api.IsNotFound(err) {
		return nil, fmt.Errorf("%s: probe dev branch %q: %w", r.errPrefix, branchName, err)
	}

	if err := r.provisionFreshBranch(ctx, branchName, cleanup); err != nil {
		return nil, err
	}

	pw, err := r.api.CreateBranchPassword(ctx, r.org, r.database, branchName, r.passwordName)
	if err != nil {
		return nil, fmt.Errorf("%s: create branch password for %q: %w", r.errPrefix, branchName, err)
	}
	if err := r.execDDL(ctx, pw, r.database, ddl); err != nil {
		return nil, fmt.Errorf("%s: apply DDL on dev branch %q: %w", r.errPrefix, branchName, err)
	}
	fmt.Fprintf(out, "%s: applied DDL on %q: %s\n", r.name, branchName, ddl)

	if r.stage != nil {
		if err := r.stage(ctx, pw); err != nil {
			return nil, fmt.Errorf("%s: %w", r.errPrefix, err)
		}
		fmt.Fprint(out, r.stageNote)
	}

	dr, err := r.api.CreateDeployRequest(ctx, r.org, r.database, branchName, r.branch)
	if err != nil {
		return nil, fmt.Errorf("%s: create deploy request %q → %q: %w", r.errPrefix, branchName, r.branch, err)
	}
	fmt.Fprintf(out, "%s: opened deploy request #%d (%s)\n", r.name, dr.Number, dr.HTMLURL)

	if err := r.waitDeployable(ctx, dr.Number); err != nil {
		return nil, err
	}
	if _, err := r.api.Deploy(ctx, r.org, r.database, dr.Number); err != nil {
		return nil, fmt.Errorf("%s: deploy request #%d: %w", r.errPrefix, dr.Number, err)
	}
	final, err := r.waitDeployed(ctx, dr.Number)
	if err != nil {
		return nil, err
	}

	// Finalize the revert window: PlanetScale holds a
	// complete_pending_revert deployment "in progress" and blocks
	// lifecycle ops (branch/database deletes) until it closes
	// (ADR-0148 finding #4). The schema change itself IS applied at
	// this point, so a skip-revert failure is a loud WARN, not a run
	// failure — the operator can finalize from the DR page.
	if final.DeploymentState == "complete_pending_revert" {
		if _, err := r.api.SkipRevert(ctx, r.org, r.database, dr.Number); err != nil {
			slog.WarnContext(ctx, r.errPrefix+": skip-revert failed; finalize the deployment manually from the deploy-request page",
				"deploy_request", dr.Number, "url", final.HTMLURL, "err", err.Error())
		}
	}
	fmt.Fprintf(out, "%s: deploy request #%d deployed\n", r.name, dr.Number)
	return final, nil
}

// provisionFreshBranch creates the dev branch and guarantees its
// schema base matches production before any DDL is applied.
//
// A new branch's schema base can LAG production (live-caught
// 2026-07-15, intermittent: a branch created 14 minutes after a
// deploy still lacked the deployed column, and a deploy request from
// it diffed as DROPPING that column from production; a branch created
// one minute after another deploy was current). Deploying from a
// stale base silently reverts every production schema change newer
// than the backup — for expand-contract's contract leg, that would
// drop the freshly backfilled expand column. The guarantee: create →
// compare branch schema to production via the API → if stale, delete
// the branch, take an on-demand backup of production, recreate,
// recheck → still stale is a coded runtime refusal.
func (r *legRunner) provisionFreshBranch(ctx context.Context, branchName string, cleanup *branchCleanup) error {
	out := r.out
	if err := r.createBranchAndWait(ctx, branchName, cleanup); err != nil {
		return err
	}
	stale, err := r.branchBaseStale(ctx, branchName)
	if err != nil {
		return fmt.Errorf("%s: compare dev-branch schema to %q: %w", r.errPrefix, r.branch, err)
	}
	if !stale {
		return nil
	}

	fmt.Fprintf(out, "%s: dev branch %q came up with a schema older than %q's current one (a new PlanetScale branch's base can lag production); taking a fresh backup to rebase\n",
		r.name, branchName, r.branch)
	if err := r.api.DeleteBranch(ctx, r.org, r.database, branchName); err != nil {
		return fmt.Errorf("%s: delete stale dev branch %q: %w", r.errPrefix, branchName, err)
	}
	cleanup.remove(branchName)
	if err := r.backupProduction(ctx); err != nil {
		return err
	}
	if err := r.createBranchAndWait(ctx, branchName, cleanup); err != nil {
		return err
	}
	if stale, err = r.branchBaseStale(ctx, branchName); err != nil {
		return fmt.Errorf("%s: recheck dev-branch schema against %q: %w", r.errPrefix, r.branch, err)
	}
	if stale {
		return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
			"take a fresh backup of the production branch (pscale backup create), then re-run",
			fmt.Errorf(
				"%s: dev branch %q still differs from %q after a fresh backup — deploying from it would silently revert newer production schema; inspect `pscale branch schema %s %s --org %s` vs %q and retry once the schemas converge",
				r.errPrefix, branchName, r.branch, r.database, branchName, r.org, r.branch,
			))
	}
	fmt.Fprintf(out, "%s: rebased dev branch %q now matches %q\n", r.name, branchName, r.branch)
	return nil
}

// createBranchAndWait creates the dev branch, registers it for
// cleanup, and waits for PlanetScale to report it ready.
func (r *legRunner) createBranchAndWait(ctx context.Context, branchName string, cleanup *branchCleanup) error {
	if _, err := r.api.CreateBranch(ctx, r.org, r.database, branchName, r.branch); err != nil {
		return fmt.Errorf("%s: create dev branch %q off %q: %w", r.errPrefix, branchName, r.branch, err)
	}
	cleanup.add(branchName)
	fmt.Fprintf(r.out, "%s: created dev branch %q off %q\n", r.name, branchName, r.branch)
	if err := r.waitBranchReady(ctx, branchName); err != nil {
		return fmt.Errorf("%s: %w", r.errPrefix, err)
	}
	return nil
}

// branchBaseStale reports whether the dev branch's schema differs from
// the production branch's — the from-a-stale-backup signal.
func (r *legRunner) branchBaseStale(ctx context.Context, branchName string) (bool, error) {
	dev, err := r.api.GetBranchSchema(ctx, r.org, r.database, branchName)
	if err != nil {
		return false, err
	}
	prod, err := r.api.GetBranchSchema(ctx, r.org, r.database, r.branch)
	if err != nil {
		return false, err
	}
	return renderSchema(dev) != renderSchema(prod), nil
}

// renderSchema canonicalizes a branch schema for comparison: raw DDL
// concatenated in table-name order (both sides come from the same
// PlanetScale renderer, so identical schemas render identically).
func renderSchema(tables []api.SchemaTable) string {
	sorted := append([]api.SchemaTable(nil), tables...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b strings.Builder
	for _, t := range sorted {
		b.WriteString(t.Name)
		b.WriteString("\x00")
		b.WriteString(t.Raw)
		b.WriteString("\x00")
	}
	return b.String()
}

// backupProduction takes an on-demand backup of the production branch
// and polls it to success, bounded by the deploy timeout (backup
// duration scales with database size; the narration names what's
// happening so a long wait is explained).
func (r *legRunner) backupProduction(ctx context.Context) error {
	bk, err := r.api.CreateBackup(ctx, r.org, r.database, r.branch)
	if err != nil {
		return fmt.Errorf("%s: create rebase backup of %q: %w", r.errPrefix, r.branch, err)
	}
	fmt.Fprintf(r.out, "%s: backup of %q started (rebase base for the dev branch; duration scales with database size)\n", r.name, r.branch)
	deadline := time.Now().Add(r.deployTimeout)
	for {
		cur, err := r.api.GetBackup(ctx, r.org, r.database, r.branch, bk.ID)
		if err != nil {
			return fmt.Errorf("%s: poll rebase backup: %w", r.errPrefix, err)
		}
		switch cur.State {
		case "success":
			return nil
		case "failed", "canceled", "cancelled":
			return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
				"take a fresh backup of the production branch (pscale backup create), then re-run",
				fmt.Errorf(
					"%s: rebase backup of %q ended %q — a fresh backup is required for the dev branch to see current production schema",
					r.errPrefix, r.branch, cur.State,
				))
		}
		if time.Now().After(deadline) {
			return sluicecode.Wrap(sluicecode.CodePSBranchStaleBase,
				"take a fresh backup of the production branch (pscale backup create), then re-run",
				fmt.Errorf(
					"%s: rebase backup of %q still %q after %s — re-run once it completes (the backup keeps running in PlanetScale)",
					r.errPrefix, r.branch, cur.State, r.deployTimeout,
				))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// waitBranchReady polls the dev branch until PlanetScale reports it
// ready (branch provisioning is async).
func (r *legRunner) waitBranchReady(ctx context.Context, branchName string) error {
	deadline := time.Now().Add(branchReadyTimeout)
	for {
		br, err := r.api.GetBranch(ctx, r.org, r.database, branchName)
		if err != nil {
			return fmt.Errorf("poll dev branch %q readiness: %w", branchName, err)
		}
		if br.Ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dev branch %q did not become ready within %s", branchName, branchReadyTimeout)
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// Deploy-request lifecycle classification (ADR-0148 finding #3 ground
// truth: open/pending → ready (deployable=true) → queued →
// complete_pending_revert). The poller is deliberately TOLERANT of
// state names it doesn't know: terminal-success and terminal-failure
// are matched by name, everything else keeps waiting until the
// deadline — a new intermediate PlanetScale state must not fail a
// healthy deploy, and the timeout bounds the unknown-terminal risk.
var (
	drSuccessStates = map[string]bool{
		"complete":                true,
		"complete_pending_revert": true,
	}
	drFailureStates = map[string]bool{
		"error":                 true,
		"complete_error":        true,
		"cancelled":             true,
		"complete_cancel":       true,
		"complete_revert":       true,
		"complete_revert_error": true,
	}
)

// waitDeployable polls until the deploy request is deployable (the
// diff computed and PlanetScale accepts a deploy call).
func (r *legRunner) waitDeployable(ctx context.Context, number int) error {
	deadline := time.Now().Add(r.deployTimeout)
	for {
		dr, err := r.api.GetDeployRequest(ctx, r.org, r.database, number)
		if err != nil {
			return fmt.Errorf("%s: poll deploy request #%d: %w", r.errPrefix, number, err)
		}
		switch {
		case dr.CanDeploy():
			return nil
		case dr.DeploymentState == "no_changes":
			// The branch's diff against production is empty — the DDL
			// is almost certainly already deployed (a crashed earlier
			// run, or the operator shipping something the schema
			// already has). Deploying nothing would silently
			// "succeed", so refuse with the command's guidance instead.
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d has no schema changes — the DDL looks already deployed; %s",
				number, r.alreadyDeployedAdvice,
			))
		case drFailureStates[dr.DeploymentState] || dr.State == "closed":
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d cannot be deployed (state %q, deployment_state %q)",
				number, dr.State, dr.DeploymentState,
			))
		}
		if time.Now().After(deadline) {
			return r.drFailure(dr, fmt.Sprintf(
				"deploy request #%d did not become deployable within %s (deployment_state %q) — if your organization requires deploy-request review, %s",
				number, r.deployTimeout, dr.DeploymentState, r.reviewTimeoutAdvice,
			))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// waitDeployed polls a deploying request to a terminal state.
func (r *legRunner) waitDeployed(ctx context.Context, number int) (*api.DeployRequest, error) {
	deadline := time.Now().Add(r.deployTimeout)
	for {
		dr, err := r.api.GetDeployRequest(ctx, r.org, r.database, number)
		if err != nil {
			return nil, fmt.Errorf("%s: poll deploy request #%d: %w", r.errPrefix, number, err)
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
				"deploy request #%d still deploying after %s (deployment_state %q) — the deploy keeps running in PlanetScale; %s",
				number, r.deployTimeout, dr.DeploymentState, r.deployTimeoutAdvice,
			))
		}
		if err := r.sleepPoll(ctx); err != nil {
			return nil, err
		}
	}
}

// drFailure wraps a deploy-request failure/timeout in the coded
// runtime error, always carrying the DR state and URL.
func (r *legRunner) drFailure(dr *api.DeployRequest, msg string) error {
	return sluicecode.Wrap(
		sluicecode.CodePSDeployRequestFailed,
		"inspect the deploy request in PlanetScale: "+dr.HTMLURL,
		fmt.Errorf("%s: %s: %s", r.errPrefix, msg, dr.HTMLURL),
	)
}

func (r *legRunner) sleepPoll(ctx context.Context) error {
	t := time.NewTimer(r.pollInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ---- cleanup ----

// branchCleanup deletes the dev branches a run created — always,
// including on failure (best-effort with a WARN), unless the operator
// asked to keep them for debugging. It runs on a cancel-immune context
// so a Ctrl-C mid-deploy still tears the branches down.
type branchCleanup struct {
	api      *api.Client
	org      string
	database string
	keep     bool
	out      io.Writer
	// command prefixes the could-not-delete WARN ("expand-contract",
	// "deploy-ddl").
	command  string
	branches []string
}

func (c *branchCleanup) add(name string) { c.branches = append(c.branches, name) }

// remove forgets a branch the leg runner already deleted itself (the
// stale-base rebase path), so cleanup doesn't re-delete it.
func (c *branchCleanup) remove(name string) {
	kept := c.branches[:0]
	for _, b := range c.branches {
		if b != name {
			kept = append(kept, b)
		}
	}
	c.branches = kept
}

func (c *branchCleanup) run(ctx context.Context) {
	if len(c.branches) == 0 {
		return
	}
	if c.keep {
		fmt.Fprintf(c.out, "cleanup: keeping dev branches (--keep-branches): %s\n", strings.Join(c.branches, ", "))
		return
	}
	deleteCtx := context.WithoutCancel(ctx)
	for _, name := range c.branches {
		if err := c.api.DeleteBranch(deleteCtx, c.org, c.database, name); err != nil && !api.IsNotFound(err) {
			slog.WarnContext(deleteCtx, c.command+": could not delete dev branch; delete it manually",
				"branch", name, "err", err.Error())
			continue
		}
		fmt.Fprintf(c.out, "cleanup: deleted dev branch %q\n", name)
	}
}
