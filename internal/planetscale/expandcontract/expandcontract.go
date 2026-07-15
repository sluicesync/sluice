// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package expandcontract orchestrates the full expand→migrate→contract
// pattern against a PlanetScale database (`sluice expand-contract`,
// ADR-0162, roadmap item 62 Phase 3):
//
//	expand   — dev branch off production + the operator's ADD COLUMN
//	           DDL + a deploy request, deployed and finalized
//	migrate  — the ADR-0159 backfill (pipeline.Backfiller) against the
//	           production branch, reused whole, never forked
//	verify   — the Phase-2 whole-table remaining-count gate on --where
//	contract — a second dev branch + the DROP COLUMN DDL + deploy
//	           request, HARD-GATED on a clean verify AND --yes
//
// Control-plane calls ride the shared thin client in
// internal/planetscale/api (no planetscale-go SDK — the ADR-0148
// posture). The command is strictly opt-in and PlanetScale-specific;
// nothing here is imported by the engine-neutral pipeline.
package expandcontract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Leg names the resume points of the pattern. There is deliberately no
// persisted state machine in v1 (ADR-0162): the expand and contract
// legs are refuse-on-leftover (deterministic dev-branch names, so a
// crashed run's branch is detected and named), the migrate leg is
// natively resumable through the backfill cursor store, and
// --resume-from is the operator's explicit "continue from here".
type Leg string

const (
	// LegExpand is the default: run the whole pattern from the top.
	LegExpand Leg = "expand"
	// LegMigrate skips the expand leg — the ADD COLUMN deploy request
	// already deployed (e.g. a re-run after a mid-backfill crash).
	LegMigrate Leg = "migrate"
	// LegContract skips straight to the verify gate + the contract
	// leg — the backfill already completed.
	LegContract Leg = "contract"
)

// branch-readiness polling gets its own (generous, fixed) deadline:
// branch creation is near-instant next to a deploy, and reusing the
// operator's whole --deploy-timeout here would just delay the real
// error.
const branchReadyTimeout = 5 * time.Minute

// Orchestrator drives one expand-contract run. Same shape as
// pipeline.Backfiller: hold config, call Run.
type Orchestrator struct {
	// API is the shared PlanetScale control-plane client. Required
	// (except under DryRun, which never touches the control plane).
	API *api.Client

	// Org, Database, Branch identify the PRODUCTION branch the pattern
	// targets. Branch defaults to "main" when empty.
	Org      string
	Database string
	Branch   string

	// Engine + DSN are the data plane for the migrate leg: the
	// production branch's MySQL endpoint the backfill runs inside.
	Engine ir.Engine
	DSN    string

	// Table, Sets, Where, BatchSize are the ADR-0159 backfill spec.
	// Where is REQUIRED here (unlike plain backfill): the verify gate
	// that authorizes the contract leg is meaningless without a
	// self-describing guard.
	Table     string
	Sets      []ir.BackfillSet
	Where     string
	BatchSize int

	// ExpandDDL / ContractDDL are the operator's verbatim DDL for the
	// two deploy-request legs (the --set native-SQL posture: applied
	// exactly as written, on a dev branch). ContractDDL empty ⇒ the
	// run stops after verify with resume instructions.
	ExpandDDL   string
	ContractDDL string

	// Yes authorizes the contract leg. Without it the run stops after
	// a clean verify and prints the exact resume command — a DROP
	// COLUMN is never implicit (the roadmap non-goal), and sluice
	// commands are non-interactive by contract, so there is no prompt.
	Yes bool

	// DryRun prints the full plan — branch names, deploy requests, the
	// rendered backfill statement, the gates — and returns without a
	// single control-plane call (pinned) and without writing anything.
	DryRun bool

	// KeepBranches skips the dev-branch cleanup (debugging aid).
	KeepBranches bool

	// ResumeFrom picks the first leg to run; empty means LegExpand.
	ResumeFrom Leg

	// PollInterval / DeployTimeout shape the deploy-request state
	// polling. Zero values default to 10s / 1h.
	PollInterval  time.Duration
	DeployTimeout time.Duration

	// Out receives the step-by-step narration, the dry-run plan, and
	// the stop-with-instructions reports. nil falls back to io.Discard.
	Out io.Writer

	// ExecDDL applies one verbatim DDL statement to a dev branch over
	// a direct MySQL connection using a just-minted branch password.
	// nil uses the real go-sql-driver implementation (ddl_exec.go);
	// tests inject a fake.
	ExecDDL func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error
}

// Result is the structured outcome of a run.
type Result struct {
	// ExpandDeployRequest / ContractDeployRequest are the DR numbers
	// each deployed leg drove (0 when the leg didn't run).
	ExpandDeployRequest   int
	ContractDeployRequest int

	// Backfill is the migrate leg's result (nil when the leg was
	// skipped by --resume-from contract, which runs verify only).
	Backfill *pipeline.BackfillResult

	// Verified reports the verify gate ran and found zero rows still
	// matching Where.
	Verified bool

	// ContractRun reports the contract leg actually deployed. False
	// with a nil error means the run stopped at the gate by design
	// (no --contract-ddl, or no --yes) and printed resume
	// instructions.
	ContractRun bool
}

// Run executes the pattern. Refusals carry sluicecode.CodedError; a
// designed stop at the contract gate is a nil error with
// Result.ContractRun == false.
func (o *Orchestrator) Run(ctx context.Context) (*Result, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}

	// Data-plane preflight (runs under --dry-run too — the plan should
	// refuse a doomed run just like the real thing): the table must
	// exist with a walkable PK. --set column existence is deliberately
	// NOT checked here — the expand leg is what creates those columns;
	// the migrate leg checks them post-expand.
	table, err := pipeline.ResolveBackfillTable(ctx, o.Engine, o.DSN, o.Table)
	if err != nil {
		return nil, fmt.Errorf("expand-contract preflight: %w", err)
	}

	if o.DryRun {
		return &Result{}, o.printPlan(ctx, table)
	}

	// Control-plane preflight: org/database/branch existence + the
	// safe-migrations prerequisite, refused loudly by name — sluice
	// never auto-enables the toggle (it changes the branch's behavior:
	// direct DDL becomes blocked; ADR-0148 findings #1/#7).
	if err := o.preflightControlPlane(ctx); err != nil {
		return nil, err
	}

	result := &Result{}
	cleanup := &branchCleanup{o: o}
	defer cleanup.run(ctx)

	if o.resumeFrom() == LegExpand {
		dr, err := o.runDeployLeg(ctx, "expand", o.expandBranchName(), o.ExpandDDL, cleanup)
		if err != nil {
			return nil, err
		}
		result.ExpandDeployRequest = dr.Number
	}

	if o.resumeFrom() != LegContract {
		br, err := o.runMigrateLeg(ctx)
		if err != nil {
			return nil, err
		}
		result.Backfill = br
		result.Verified = br.Verified
	} else {
		// --resume-from contract still earns its gate: the verify is
		// never skippable, only the walk is.
		if err := o.runVerifyOnly(ctx); err != nil {
			return nil, err
		}
		result.Verified = true
	}

	if stop := o.contractGateStop(); stop != "" {
		fmt.Fprint(o.out(), stop)
		return result, nil
	}

	dr, err := o.runDeployLeg(ctx, "contract", o.contractBranchName(), o.ContractDDL, cleanup)
	if err != nil {
		return nil, err
	}
	result.ContractDeployRequest = dr.Number
	result.ContractRun = true
	fmt.Fprintf(o.out(),
		"expand-contract complete: table %q expanded, backfilled (%s), verified, and contracted\n",
		o.Table, humanRows(result.Backfill))
	return result, nil
}

// ---- validation & defaults ----

func (o *Orchestrator) validate() error {
	switch {
	case o.API == nil && !o.DryRun:
		return errors.New("expand-contract: API client is required")
	case o.Engine == nil:
		return errors.New("expand-contract: Engine is required")
	case o.Org == "" || o.Database == "":
		return errors.New("expand-contract: Org and Database are required")
	case o.DSN == "":
		return errors.New("expand-contract: DSN is required")
	case o.Table == "":
		return errors.New("expand-contract: Table is required")
	case o.Where == "":
		return errors.New("expand-contract: Where is required — the verify gate that authorizes the contract step needs a self-describing guard (e.g. 'new_col IS NULL')")
	}
	switch o.resumeFrom() {
	case LegExpand:
		if o.ExpandDDL == "" {
			return errors.New("expand-contract: ExpandDDL is required (or resume past the expand leg with --resume-from migrate)")
		}
		if len(o.Sets) == 0 {
			return errors.New("expand-contract: at least one Set is required for the migrate leg")
		}
	case LegMigrate:
		if len(o.Sets) == 0 {
			return errors.New("expand-contract: at least one Set is required for the migrate leg")
		}
	case LegContract:
		if o.ContractDDL == "" {
			return errors.New("expand-contract: --resume-from contract requires ContractDDL — there is nothing else left to run")
		}
	default:
		return fmt.Errorf("expand-contract: unknown resume leg %q (want expand, migrate, or contract)", o.ResumeFrom)
	}
	return nil
}

func (o *Orchestrator) resumeFrom() Leg {
	if o.ResumeFrom == "" {
		return LegExpand
	}
	return o.ResumeFrom
}

func (o *Orchestrator) branch() string {
	if o.Branch == "" {
		return "main"
	}
	return o.Branch
}

func (o *Orchestrator) pollInterval() time.Duration {
	if o.PollInterval <= 0 {
		return 10 * time.Second
	}
	return o.PollInterval
}

func (o *Orchestrator) deployTimeout() time.Duration {
	if o.DeployTimeout <= 0 {
		return time.Hour
	}
	return o.DeployTimeout
}

func (o *Orchestrator) out() io.Writer {
	if o.Out == nil {
		return io.Discard
	}
	return o.Out
}

// expandBranchName / contractBranchName derive DETERMINISTIC dev-
// branch names from the table + the leg's DDL, so a re-run after a
// crash finds (and refuses on) its own leftover branch by name instead
// of silently minting sluice-branch litter — the v1 resumability
// design (ADR-0162).
func (o *Orchestrator) expandBranchName() string {
	return legBranchName("expand", o.Table, o.ExpandDDL)
}

func (o *Orchestrator) contractBranchName() string {
	return legBranchName("contract", o.Table, o.ContractDDL)
}

func legBranchName(kind, table, ddl string) string {
	h := sha256.New()
	h.Write([]byte(table))
	h.Write([]byte{0})
	h.Write([]byte(ddl))
	return "sluice-" + kind + "-" + hex.EncodeToString(h.Sum(nil))[:10]
}

// ---- preflight ----

// preflightControlPlane verifies the service token / org / database /
// branch in one GET, then the safe-migrations prerequisite.
func (o *Orchestrator) preflightControlPlane(ctx context.Context) error {
	br, err := o.API.GetBranch(ctx, o.Org, o.Database, o.branch())
	if err != nil {
		if api.IsNotFound(err) {
			return fmt.Errorf("expand-contract preflight: branch %q of %s/%s not found — check --org/--database/--branch: %w",
				o.branch(), o.Org, o.Database, err)
		}
		return fmt.Errorf("expand-contract preflight: read branch %q: %w", o.branch(), err)
	}
	if !br.SafeMigrations {
		// The deploy-request prerequisite (ADR-0148 finding #1).
		// Enabling it is a behavior change on the operator's production
		// branch (direct DDL becomes blocked), so sluice REFUSES and
		// names the toggle rather than flipping it (contain-complexity
		// tenet); the enable/disable propagation lag (finding #7) makes
		// a toggle-around-the-run design unsafe anyway.
		return sluicecode.Wrap(
			sluicecode.CodePSSafeMigrationsDisabled,
			"enable the branch's \"Safe migrations\" setting in the PlanetScale UI, or run `pscale branch safe-migrations enable "+o.Database+" "+o.branch()+" --org "+o.Org+"` — note this blocks direct DDL on the branch from then on",
			fmt.Errorf("expand-contract: branch %q of %s/%s does not have safe migrations enabled — PlanetScale refuses deploy requests into it, and sluice never enables the toggle for you (it changes how every future schema change on the branch must ship)",
				o.branch(), o.Org, o.Database),
		)
	}
	return nil
}

// ---- deploy legs ----

// runDeployLeg drives one branch → DDL → deploy-request → deploy →
// finalize cycle: the expand and contract legs are the same machine
// with different DDL.
func (o *Orchestrator) runDeployLeg(ctx context.Context, kind, branchName, ddl string, cleanup *branchCleanup) (*api.DeployRequest, error) {
	out := o.out()

	// Refuse-on-leftover: a branch with our deterministic name means a
	// previous run died mid-leg. Guessing whether its DDL/DR state is
	// reusable would be the silent path; name it and let the operator
	// decide.
	if _, err := o.API.GetBranch(ctx, o.Org, o.Database, branchName); err == nil {
		return nil, fmt.Errorf(
			"expand-contract %s: dev branch %q already exists — a previous run left it behind. Inspect its deploy request in PlanetScale; if the %s DDL already deployed, continue with --resume-from %s; otherwise delete the branch (`pscale branch delete %s %s --org %s`) and re-run",
			kind, branchName, kind, legAfter(kind), o.Database, branchName, o.Org,
		)
	} else if !api.IsNotFound(err) {
		return nil, fmt.Errorf("expand-contract %s: probe dev branch %q: %w", kind, branchName, err)
	}

	if _, err := o.API.CreateBranch(ctx, o.Org, o.Database, branchName, o.branch()); err != nil {
		return nil, fmt.Errorf("expand-contract %s: create dev branch %q off %q: %w", kind, branchName, o.branch(), err)
	}
	cleanup.add(branchName)
	fmt.Fprintf(out, "%s: created dev branch %q off %q\n", kind, branchName, o.branch())

	if err := o.waitBranchReady(ctx, branchName); err != nil {
		return nil, fmt.Errorf("expand-contract %s: %w", kind, err)
	}

	pw, err := o.API.CreateBranchPassword(ctx, o.Org, o.Database, branchName, "sluice-expand-contract")
	if err != nil {
		return nil, fmt.Errorf("expand-contract %s: create branch password for %q: %w", kind, branchName, err)
	}
	if err := o.execDDL(ctx, pw, ddl); err != nil {
		return nil, fmt.Errorf("expand-contract %s: apply DDL on dev branch %q: %w", kind, branchName, err)
	}
	fmt.Fprintf(out, "%s: applied DDL on %q: %s\n", kind, branchName, ddl)

	dr, err := o.API.CreateDeployRequest(ctx, o.Org, o.Database, branchName, o.branch())
	if err != nil {
		return nil, fmt.Errorf("expand-contract %s: create deploy request %q → %q: %w", kind, branchName, o.branch(), err)
	}
	fmt.Fprintf(out, "%s: opened deploy request #%d (%s)\n", kind, dr.Number, dr.HTMLURL)

	if err := o.waitDeployable(ctx, kind, dr.Number); err != nil {
		return nil, err
	}
	if _, err := o.API.Deploy(ctx, o.Org, o.Database, dr.Number); err != nil {
		return nil, fmt.Errorf("expand-contract %s: deploy request #%d: %w", kind, dr.Number, err)
	}
	final, err := o.waitDeployed(ctx, kind, dr.Number)
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
		if _, err := o.API.SkipRevert(ctx, o.Org, o.Database, dr.Number); err != nil {
			slog.WarnContext(ctx, "expand-contract: skip-revert failed; finalize the deployment manually from the deploy-request page",
				"leg", kind, "deploy_request", dr.Number, "url", final.HTMLURL, "err", err.Error())
		}
	}
	fmt.Fprintf(out, "%s: deploy request #%d deployed\n", kind, dr.Number)
	return final, nil
}

// legAfter names the --resume-from value that skips past a completed
// leg, for the leftover-branch guidance.
func legAfter(kind string) string {
	if kind == "expand" {
		return string(LegMigrate)
	}
	return string(LegContract)
}

func (o *Orchestrator) execDDL(ctx context.Context, pw *api.BranchPassword, ddl string) error {
	exec := o.ExecDDL
	if exec == nil {
		exec = execBranchDDL
	}
	return exec(ctx, pw, o.Database, ddl)
}

// waitBranchReady polls the dev branch until PlanetScale reports it
// ready (branch provisioning is async).
func (o *Orchestrator) waitBranchReady(ctx context.Context, branchName string) error {
	deadline := time.Now().Add(branchReadyTimeout)
	for {
		br, err := o.API.GetBranch(ctx, o.Org, o.Database, branchName)
		if err != nil {
			return fmt.Errorf("poll dev branch %q readiness: %w", branchName, err)
		}
		if br.Ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dev branch %q did not become ready within %s", branchName, branchReadyTimeout)
		}
		if err := o.sleepPoll(ctx); err != nil {
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
func (o *Orchestrator) waitDeployable(ctx context.Context, kind string, number int) error {
	deadline := time.Now().Add(o.deployTimeout())
	for {
		dr, err := o.API.GetDeployRequest(ctx, o.Org, o.Database, number)
		if err != nil {
			return fmt.Errorf("expand-contract %s: poll deploy request #%d: %w", kind, number, err)
		}
		switch {
		case dr.Deployable:
			return nil
		case dr.DeploymentState == "no_changes":
			// The branch's diff against production is empty — the DDL
			// is almost certainly already deployed (a crashed earlier
			// run). Deploying nothing would silently "succeed", so
			// refuse with the resume path instead.
			return o.drFailure(kind, dr, fmt.Sprintf(
				"deploy request #%d has no schema changes — the %s DDL looks already deployed; close the DR, delete the dev branch, and continue with --resume-from %s",
				number, kind, legAfter(kind),
			))
		case drFailureStates[dr.DeploymentState] || dr.State == "closed":
			return o.drFailure(kind, dr, fmt.Sprintf(
				"deploy request #%d cannot be deployed (state %q, deployment_state %q)",
				number, dr.State, dr.DeploymentState,
			))
		}
		if time.Now().After(deadline) {
			return o.drFailure(kind, dr, fmt.Sprintf(
				"deploy request #%d did not become deployable within %s (deployment_state %q) — if your organization requires deploy-request review, approve it and re-run with --resume-from %s",
				number, o.deployTimeout(), dr.DeploymentState, string(o.resumeFrom()),
			))
		}
		if err := o.sleepPoll(ctx); err != nil {
			return err
		}
	}
}

// waitDeployed polls a deploying request to a terminal state.
func (o *Orchestrator) waitDeployed(ctx context.Context, kind string, number int) (*api.DeployRequest, error) {
	deadline := time.Now().Add(o.deployTimeout())
	for {
		dr, err := o.API.GetDeployRequest(ctx, o.Org, o.Database, number)
		if err != nil {
			return nil, fmt.Errorf("expand-contract %s: poll deploy request #%d: %w", kind, number, err)
		}
		switch {
		case drSuccessStates[dr.DeploymentState]:
			return dr, nil
		case drFailureStates[dr.DeploymentState]:
			return nil, o.drFailure(kind, dr, fmt.Sprintf(
				"deploy request #%d failed (deployment_state %q)", number, dr.DeploymentState,
			))
		}
		if time.Now().After(deadline) {
			return nil, o.drFailure(kind, dr, fmt.Sprintf(
				"deploy request #%d still deploying after %s (deployment_state %q) — the deploy keeps running in PlanetScale; watch it at the URL and re-run with --resume-from %s once it completes",
				number, o.deployTimeout(), dr.DeploymentState, legAfter(kind),
			))
		}
		if err := o.sleepPoll(ctx); err != nil {
			return nil, err
		}
	}
}

// drFailure wraps a deploy-request failure/timeout in the coded
// runtime error, always carrying the DR state and URL.
func (o *Orchestrator) drFailure(kind string, dr *api.DeployRequest, msg string) error {
	return sluicecode.Wrap(
		sluicecode.CodePSDeployRequestFailed,
		"inspect the deploy request in PlanetScale: "+dr.HTMLURL,
		fmt.Errorf("expand-contract %s: %s: %s", kind, msg, dr.HTMLURL),
	)
}

func (o *Orchestrator) sleepPoll(ctx context.Context) error {
	t := time.NewTimer(o.pollInterval())
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ---- migrate + verify legs ----

// runMigrateLeg runs the ADR-0159 backfill with the verify post-pass
// in one shot: a dirty verify surfaces as the coded
// SLUICE-E-BACKFILL-INCOMPLETE error, which is exactly the contract
// gate — the caller never reaches the contract leg on it.
func (o *Orchestrator) runMigrateLeg(ctx context.Context) (*pipeline.BackfillResult, error) {
	b := &pipeline.Backfiller{
		Engine:    o.Engine,
		DSN:       o.DSN,
		Table:     o.Table,
		Sets:      o.Sets,
		Where:     o.Where,
		BatchSize: o.BatchSize,
		Verify:    true,
		Out:       o.out(),
	}
	fmt.Fprintf(o.out(), "migrate: backfilling %q (%s)\n", o.Table, pipeline.BackfillMigrationID(o.Table, o.Sets, o.Where))
	return b.Run(ctx)
}

// runVerifyOnly is the --resume-from contract gate: the walk is
// skipped, the verify never is.
func (o *Orchestrator) runVerifyOnly(ctx context.Context) error {
	b := &pipeline.Backfiller{
		Engine:     o.Engine,
		DSN:        o.DSN,
		Table:      o.Table,
		Sets:       o.Sets,
		Where:      o.Where,
		VerifyOnly: true,
		Out:        o.out(),
	}
	_, err := b.Run(ctx)
	return err
}

// contractGateStop returns the stop-with-instructions report when the
// contract leg must NOT run (no ContractDDL, or no --yes), or "" to
// proceed. A stop is a designed success, not an error: the expand +
// migrate + verify work all stands.
func (o *Orchestrator) contractGateStop() string {
	resume := fmt.Sprintf(
		"sluice expand-contract --org %s --database %s --branch %s --dsn <dsn> --table %s --where %q --resume-from contract --contract-ddl '<ALTER TABLE %s DROP COLUMN ...>' --yes",
		o.Org, o.Database, o.branch(), o.Table, o.Where, o.Table,
	)
	switch {
	case o.ContractDDL == "":
		return fmt.Sprintf(
			"expand + migrate complete and verified: 0 rows of %q still match the --where guard.\n"+
				"No --contract-ddl was given, so the contract step (dropping the old column) was not run.\n"+
				"When you are ready, run it with:\n  %s\n", o.Table, resume,
		)
	case !o.Yes:
		return fmt.Sprintf(
			"expand + migrate complete and verified: 0 rows of %q still match the --where guard.\n"+
				"The contract step is DESTRUCTIVE (it ships: %s) and needs explicit confirmation; it was not run.\n"+
				"Re-run with --yes to proceed, or later:\n  %s\n", o.Table, o.ContractDDL, resume,
		)
	}
	return ""
}

// ---- dry-run plan ----

// printPlan renders the full plan without touching the control plane
// (zero PlanetScale API calls — pinned) and without writing anything.
// It DOES open the data-plane DSN read-only, to render the exact
// backfill statement the migrate leg would run.
func (o *Orchestrator) printPlan(ctx context.Context, table *ir.Table) error {
	stmt := "(verify-only: no UPDATE — --resume-from contract runs just the verify gate + contract)"
	if o.resumeFrom() != LegContract {
		opener, ok := o.Engine.(ir.BackfillExecutorOpener)
		if !ok {
			return fmt.Errorf("expand-contract: engine %q does not support in-place backfill", o.Engine.Name())
		}
		ex, err := opener.OpenBackfillExecutor(ctx, o.DSN)
		if err != nil {
			return fmt.Errorf("expand-contract dry-run: open executor: %w", err)
		}
		defer func() { _ = ex.Close() }()
		if stmt, err = ex.BackfillStatement(table, o.Sets, o.Where); err != nil {
			return fmt.Errorf("expand-contract dry-run: render statement: %w", err)
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "-- sluice expand-contract --dry-run (no control-plane call made, nothing written)\n")
	fmt.Fprintf(&b, "database:        %s/%s, production branch %q\n", o.Org, o.Database, o.branch())
	fmt.Fprintf(&b, "resume from:     %s\n", o.resumeFrom())
	if o.resumeFrom() == LegExpand {
		fmt.Fprintf(&b, "1. preflight:    verify safe migrations are enabled on %q (refuses otherwise)\n", o.branch())
		fmt.Fprintf(&b, "2. expand:       create dev branch %q, apply:\n                   %s\n", o.expandBranchName(), o.ExpandDDL)
		fmt.Fprintf(&b, "                 open deploy request → %q, deploy, wait, finalize\n", o.branch())
	}
	if o.resumeFrom() != LegContract {
		fmt.Fprintf(&b, "3. migrate:      per-chunk (batch %s):\n                   %s\n", humanBatch(o.BatchSize), stmt)
	}
	fmt.Fprintf(&b, "4. verify:       count rows of %q still matching (%s); nonzero fails SLUICE-E-BACKFILL-INCOMPLETE\n", o.Table, o.Where)
	switch {
	case o.ContractDDL == "":
		fmt.Fprintf(&b, "5. contract:     SKIPPED (no --contract-ddl); the run stops after verify with resume instructions\n")
	case !o.Yes:
		fmt.Fprintf(&b, "5. contract:     GATED (needs --yes); the run stops after verify with resume instructions\n")
	default:
		fmt.Fprintf(&b, "5. contract:     create dev branch %q, apply:\n                   %s\n", o.contractBranchName(), o.ContractDDL)
		fmt.Fprintf(&b, "                 open deploy request → %q, deploy, wait, finalize\n", o.branch())
	}
	if o.KeepBranches {
		fmt.Fprintf(&b, "6. cleanup:      SKIPPED (--keep-branches)\n")
	} else {
		fmt.Fprintf(&b, "6. cleanup:      delete the sluice dev branches (always, best-effort, including on failure)\n")
	}
	_, err := io.WriteString(o.out(), b.String())
	return err
}

func humanBatch(n int) string {
	if n <= 0 {
		return "default"
	}
	return fmt.Sprintf("%d", n)
}

func humanRows(br *pipeline.BackfillResult) string {
	if br == nil {
		return "walk skipped"
	}
	return fmt.Sprintf("%d row(s) updated", br.RowsUpdated)
}

// ---- cleanup ----

// branchCleanup deletes the dev branches this run created — always,
// including on failure (best-effort with a WARN), unless the operator
// asked to keep them for debugging. It runs on a cancel-immune context
// so a Ctrl-C mid-deploy still tears the branches down.
type branchCleanup struct {
	o        *Orchestrator
	branches []string
}

func (c *branchCleanup) add(name string) { c.branches = append(c.branches, name) }

func (c *branchCleanup) run(ctx context.Context) {
	if len(c.branches) == 0 {
		return
	}
	if c.o.KeepBranches {
		fmt.Fprintf(c.o.out(), "cleanup: keeping dev branches (--keep-branches): %s\n", strings.Join(c.branches, ", "))
		return
	}
	deleteCtx := context.WithoutCancel(ctx)
	for _, name := range c.branches {
		if err := c.o.API.DeleteBranch(deleteCtx, c.o.Org, c.o.Database, name); err != nil && !api.IsNotFound(err) {
			slog.WarnContext(deleteCtx, "expand-contract: could not delete dev branch; delete it manually",
				"branch", name, "err", err.Error())
			continue
		}
		fmt.Fprintf(c.o.out(), "cleanup: deleted dev branch %q\n", name)
	}
}
