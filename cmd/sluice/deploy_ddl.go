// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"os"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/planetscale/expandcontract"
)

// DeployDDLCmd implements `sluice deploy-ddl` (ADR-0165, roadmap item
// 66): ship ONE verbatim DDL statement to a PlanetScale production
// branch safely, as one command — dev branch (with the ADR-0162
// stale-base freshness gate), apply the DDL, deploy request, deploy,
// finalize, cleanup. It replaces five hand-driven pscale commands plus
// a hazard the operator can't see (a fresh PS branch can silently
// propose reverting recent production schema). The named consumer is
// the one-time control-table bootstrap on a safe-migrations branch:
// `sluice control-tables ddl` prints the statements to ship.
//
// The command requires safe migrations ON the branch (the
// deploy-request prerequisite); without it, direct DDL works and this
// command is unnecessary. There is no data-plane DSN: the DDL runs on
// the dev branch via a just-minted branch password.
type DeployDDLCmd struct {
	Org      string `help:"PlanetScale organization slug." required:"" env:"PLANETSCALE_ORG" placeholder:"ORG"`
	Database string `help:"PlanetScale database name." required:"" placeholder:"DB"`
	Branch   string `help:"Production branch the deploy request merges into (must have safe migrations enabled)." default:"main" placeholder:"BRANCH"`

	ServiceTokenID string `name:"service-token-id" help:"PlanetScale service-token ID (branch + deploy-request scopes). Prefer the env var so it never lands in shell history." env:"PLANETSCALE_SERVICE_TOKEN_ID" placeholder:"ID"`
	ServiceToken   string `name:"service-token" help:"PlanetScale service-token secret. Set via the env var (never on the command line); never logged." env:"PLANETSCALE_SERVICE_TOKEN" placeholder:"SECRET"`

	DDL string `help:"The single verbatim DDL statement to ship (e.g. 'CREATE TABLE ...' or 'ALTER TABLE ...'), applied on a dev branch exactly as written and deployed via a deploy request." required:"" placeholder:"DDL"`

	DryRun       bool          `name:"dry-run" help:"Print the plan — branch name, the DDL, the deploy-request flow — without a single control-plane call and without writing anything."`
	KeepBranches bool          `name:"keep-branches" help:"Keep the sluice dev branch instead of deleting it at the end (debugging aid)."`
	PollInterval time.Duration `name:"poll-interval" help:"Deploy-request / branch state polling cadence." default:"10s"`

	DeployTimeout time.Duration `name:"deploy-timeout" help:"Deploy-request deadline (large tables deploy via VReplication — real wall-clock, but async and unbounded by errno 3024)." default:"1h"`
}

// Run implements `sluice deploy-ddl`.
func (c *DeployDDLCmd) Run(_ *Globals) error {
	// Token halves resolve from flags or the PLANETSCALE_SERVICE_TOKEN_ID /
	// PLANETSCALE_SERVICE_TOKEN env vars (the pscale CLI convention).
	// Checked here — not kong-required — so the refusal can name the env
	// vars; skipped under --dry-run, which never touches the control plane.
	if !c.DryRun && (c.ServiceTokenID == "" || c.ServiceToken == "") {
		return errors.New("deploy-ddl: a PlanetScale service token is required: set --service-token-id and --service-token (env PLANETSCALE_SERVICE_TOKEN_ID / PLANETSCALE_SERVICE_TOKEN); the token needs branch + deploy-request access to the database")
	}

	d := &expandcontract.DDLDeployer{
		API:           api.New(api.Config{TokenID: c.ServiceTokenID, Token: c.ServiceToken}),
		Org:           c.Org,
		Database:      c.Database,
		Branch:        c.Branch,
		DDL:           c.DDL,
		DryRun:        c.DryRun,
		KeepBranches:  c.KeepBranches,
		PollInterval:  c.PollInterval,
		DeployTimeout: c.DeployTimeout,
		Out:           os.Stdout,
	}
	// The deployer narrates each step to Out; the error is the only
	// thing left to map.
	_, err := d.Run(kongContext())
	return err
}
