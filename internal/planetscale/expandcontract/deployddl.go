// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package expandcontract

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
)

// DDLDeployer drives `sluice deploy-ddl` (ADR-0165, roadmap item 66):
// ship ONE verbatim DDL statement to a PlanetScale production branch
// through the governed deploy-request channel — preflight (safe
// migrations), dev branch with the ADR-0162 stale-base freshness gate,
// apply the DDL on the branch, deploy request, deploy, skip-revert
// finalize, cleanup. It is an EXTRACTION of the expand-contract deploy
// leg, composing the same legRunner (legrunner.go) rather than forking
// it; the named consumer is the one-time control-table bootstrap on a
// safe-migrations branch (`sluice control-tables ddl` prints the
// statements to ship).
//
// Deliberately NOT raw deploy-request CRUD (the pscale CLI owns that):
// the differentiated piece is the safety wrapper, above all the
// freshness gate — a fresh PS branch can silently propose REVERTING
// recent production schema, and nothing in PlanetScale's own tooling
// guards this.
type DDLDeployer struct {
	// API is the shared PlanetScale control-plane client. Required
	// (except under DryRun, which never touches the control plane).
	API *api.Client

	// Org, Database, Branch identify the PRODUCTION branch the deploy
	// request merges into. Branch defaults to "main" when empty.
	Org      string
	Database string
	Branch   string

	// DDL is the operator's single verbatim statement (the ADR-0159
	// --set posture: applied exactly as written, on a dev branch;
	// sluice does not parse or validate it beyond the database's own
	// answer).
	DDL string

	// DryRun prints the plan and returns without a single
	// control-plane call (pinned) and without writing anything. There
	// is no data plane at all in this command, so a dry run touches
	// nothing.
	DryRun bool

	// KeepBranches skips the dev-branch cleanup (debugging aid).
	KeepBranches bool

	// PollInterval / DeployTimeout shape the deploy-request state
	// polling. Zero values default to 10s / 1h.
	PollInterval  time.Duration
	DeployTimeout time.Duration

	// Out receives the step-by-step narration and the dry-run plan.
	// nil falls back to io.Discard.
	Out io.Writer

	// ExecDDL applies the verbatim DDL statement to the dev branch
	// over a direct MySQL connection using a just-minted branch
	// password. nil uses the real go-sql-driver implementation
	// (ddl_exec.go); tests inject a fake.
	ExecDDL func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error
}

// DeployDDLResult is the structured outcome of a run.
type DeployDDLResult struct {
	// DeployRequest is the DR number the run drove to deployed.
	DeployRequest int
}

// Run executes the single deploy leg. Refusals carry
// sluicecode.CodedError (safe-migrations disabled, deploy-request
// failure states, a still-stale branch base).
func (d *DDLDeployer) Run(ctx context.Context) (*DeployDDLResult, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	if d.DryRun {
		return &DeployDDLResult{}, d.printPlan()
	}

	// Control-plane preflight: org/database/branch existence + the
	// safe-migrations prerequisite, refused loudly by name — sluice
	// never auto-enables the toggle (ADR-0148 findings #1/#7). Without
	// safe migrations the branch accepts direct DDL and this command
	// is unnecessary.
	if err := preflightSafeMigrations(ctx, d.API, d.Org, d.Database, d.branch(), "deploy-ddl"); err != nil {
		return nil, err
	}

	cleanup := &branchCleanup{
		api: d.API, org: d.Org, database: d.Database,
		keep: d.KeepBranches, out: d.out(), command: "deploy-ddl",
	}
	defer cleanup.run(ctx)

	dr, err := d.legRunner().run(ctx, d.branchName(), d.DDL, cleanup)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(d.out(), "deploy-ddl complete: deploy request #%d shipped the DDL into %q\n", dr.Number, d.branch())
	return &DeployDDLResult{DeployRequest: dr.Number}, nil
}

func (d *DDLDeployer) validate() error {
	switch {
	case d.API == nil && !d.DryRun:
		return errors.New("deploy-ddl: API client is required")
	case d.Org == "" || d.Database == "":
		return errors.New("deploy-ddl: Org and Database are required")
	case strings.TrimSpace(d.DDL) == "":
		return errors.New("deploy-ddl: DDL is required — the single verbatim statement to ship")
	}
	return nil
}

func (d *DDLDeployer) branch() string {
	if d.Branch == "" {
		return "main"
	}
	return d.Branch
}

func (d *DDLDeployer) pollInterval() time.Duration {
	if d.PollInterval <= 0 {
		return 10 * time.Second
	}
	return d.PollInterval
}

func (d *DDLDeployer) deployTimeout() time.Duration {
	if d.DeployTimeout <= 0 {
		return time.Hour
	}
	return d.DeployTimeout
}

func (d *DDLDeployer) out() io.Writer {
	if d.Out == nil {
		return io.Discard
	}
	return d.Out
}

// branchName derives the deterministic dev-branch name from the DDL
// alone (deploy-ddl has no table scope), so a crashed run's branch is
// found — and refused on — by name (the ADR-0162 resumability design).
func (d *DDLDeployer) branchName() string {
	return legBranchName("ddl", "", d.DDL)
}

func (d *DDLDeployer) legRunner() *legRunner {
	return &legRunner{
		api:           d.API,
		org:           d.Org,
		database:      d.Database,
		branch:        d.branch(),
		pollInterval:  d.pollInterval(),
		deployTimeout: d.deployTimeout(),
		out:           d.out(),
		execDDL:       d.execDDLFunc(),
		name:          "deploy-ddl",
		errPrefix:     "deploy-ddl",
		passwordName:  "sluice-deploy-ddl",

		// deploy-ddl has no resume legs: a deployed DDL means the run
		// is simply done, so the guidance ends there.
		leftoverAdvice:        "there is nothing left to run",
		alreadyDeployedAdvice: "the --ddl is already applied on the production branch and there is nothing left to ship; close the DR",
		reviewTimeoutAdvice:   "the --ddl is shipped and nothing further is needed",
		deployTimeoutAdvice:   "watch it at the URL — once it completes the DDL is deployed and nothing further is needed",

		// expectedDiffTables stays empty BY DESIGN: --ddl is an
		// arbitrary operator statement sluice deliberately does not
		// parse (no regex over DDL), so there is no intended table set
		// to assert the DR diff against. The post-wait production
		// freshness recheck still applies.
	}
}

func (d *DDLDeployer) execDDLFunc() func(ctx context.Context, pw *api.BranchPassword, database, ddl string) error {
	if d.ExecDDL != nil {
		return d.ExecDDL
	}
	return execBranchDDL
}

// printPlan renders the plan without touching the control plane (zero
// PlanetScale API calls — pinned). Unlike expand-contract's plan it
// opens nothing at all: deploy-ddl has no data plane.
func (d *DDLDeployer) printPlan() error {
	var b strings.Builder
	fmt.Fprintf(&b, "-- sluice deploy-ddl --dry-run (no control-plane call made, nothing written)\n")
	fmt.Fprintf(&b, "database:      %s/%s, production branch %q\n", d.Org, d.Database, d.branch())
	fmt.Fprintf(&b, "1. preflight:  verify safe migrations are enabled on %q (refuses otherwise)\n", d.branch())
	fmt.Fprintf(&b, "2. deploy:     create dev branch %q (schema-freshness-gated against %q), apply:\n                 %s\n", d.branchName(), d.branch(), d.DDL)
	fmt.Fprintf(&b, "               open deploy request → %q, deploy, wait, finalize\n", d.branch())
	if d.KeepBranches {
		fmt.Fprintf(&b, "3. cleanup:    SKIPPED (--keep-branches)\n")
	} else {
		fmt.Fprintf(&b, "3. cleanup:    delete the sluice dev branch (always, best-effort, including on failure)\n")
	}
	_, err := io.WriteString(d.out(), b.String())
	return err
}
