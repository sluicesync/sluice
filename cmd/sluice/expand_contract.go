// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"os"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/planetscale/expandcontract"
)

// ExpandContractCmd implements `sluice expand-contract` (ADR-0162,
// roadmap item 62 Phase 3): the full expand→migrate→contract pattern
// against a PlanetScale database — dev branch + deploy-request for the
// ADD COLUMN, the ADR-0159 backfill for the data migration, the
// verify gate, and (only with --yes) a second deploy request for the
// DROP COLUMN. PlanetScale-specific by design: it needs the
// control-plane service token on top of the data-plane DSN, and the
// production branch must have safe migrations enabled (refused loudly
// otherwise — sluice never flips that toggle).
type ExpandContractCmd struct {
	Org      string `help:"PlanetScale organization slug." required:"" env:"PLANETSCALE_ORG" placeholder:"ORG"`
	Database string `help:"PlanetScale database name." required:"" placeholder:"DB"`
	Branch   string `help:"Production branch the pattern targets (deploy requests merge into it; the backfill runs against its data)." default:"main" placeholder:"BRANCH"`

	ServiceTokenID string `name:"service-token-id" help:"PlanetScale service-token ID (branch + deploy-request scopes). Prefer the env var so it never lands in shell history." env:"PLANETSCALE_SERVICE_TOKEN_ID" placeholder:"ID"`
	ServiceToken   string `name:"service-token" help:"PlanetScale service-token secret. Set via the env var (never on the command line); never logged." env:"PLANETSCALE_SERVICE_TOKEN" placeholder:"SECRET"`

	DSN   string `help:"Data-plane MySQL DSN for the production branch — the migrate (backfill) leg runs inside it." required:"" placeholder:"DSN"`
	Table string `help:"Table the pattern operates on." required:"" placeholder:"TABLE"`

	ExpandDDL   string `name:"expand-ddl" help:"Verbatim ADD COLUMN DDL for the expand leg (e.g. 'ALTER TABLE t ADD COLUMN full_name VARCHAR(255)'), applied on a dev branch and shipped via a deploy request. Required unless --resume-from skips the leg." placeholder:"DDL"`
	ContractDDL string `name:"contract-ddl" help:"Verbatim DROP COLUMN DDL for the contract leg. Optional: without it the run stops after verify with resume instructions. Runs only after a clean verify AND --yes." placeholder:"DDL"`

	Set   []string `help:"Backfill assignment 'col = <expr>' for the migrate leg (repeatable; native SQL, emitted verbatim — the ADR-0159 --set)." placeholder:"'COL = EXPR'" sep:"none"`
	Where string   `help:"Self-describing native-SQL guard (e.g. 'new_col IS NULL'). Required: it scopes the backfill AND is the verify gate that authorizes the contract step." required:"" placeholder:"PREDICATE"`

	BatchSize    int           `help:"Rows per bounded backfill UPDATE. 0 uses sluice's bulk-copy default." placeholder:"N"`
	Yes          bool          `help:"Confirm the contract leg (a destructive DROP COLUMN deploy request). Without it the run stops after verify and prints the exact resume command." short:"y" xor:"dryrunyes"`
	DryRun       bool          `help:"Print the full plan — branches, deploy requests, the rendered backfill statement, the gates — without a single control-plane call and without writing anything." xor:"dryrunyes"`
	KeepBranches bool          `name:"keep-branches" help:"Keep the sluice dev branches instead of deleting them at the end (debugging aid)."`
	ResumeFrom   string        `name:"resume-from" help:"Leg to continue from after an interrupted run: expand (default, full pattern), migrate (the ADD COLUMN already deployed), contract (the backfill already completed; still re-verifies)." enum:"expand,migrate,contract" default:"expand"`
	PollInterval time.Duration `name:"poll-interval" help:"Deploy-request / branch state polling cadence." default:"10s"`

	DeployTimeout time.Duration `name:"deploy-timeout" help:"Per-deploy-request deadline (large tables deploy via VReplication — real wall-clock, but async and unbounded by errno 3024)." default:"1h"`
}

// Run implements `sluice expand-contract`.
func (c *ExpandContractCmd) Run(g *Globals) error {
	// Token halves resolve from flags or the PLANETSCALE_SERVICE_TOKEN_ID /
	// PLANETSCALE_SERVICE_TOKEN env vars (the pscale CLI convention).
	// Checked here — not kong-required — so the refusal can name the env
	// vars; skipped under --dry-run, which never touches the control plane.
	if !c.DryRun && (c.ServiceTokenID == "" || c.ServiceToken == "") {
		return errors.New("expand-contract: a PlanetScale service token is required: set --service-token-id and --service-token (env PLANETSCALE_SERVICE_TOKEN_ID / PLANETSCALE_SERVICE_TOKEN); the token needs branch + deploy-request access to the database")
	}

	// The command is PlanetScale-specific, so the data-plane engine is
	// fixed: no --driver flag to mis-set.
	engine, err := resolveEngine("planetscale")
	if err != nil {
		return err
	}
	if engine, err = applySourceEngineOptions(engine, g); err != nil {
		return err
	}

	// --resume-from contract runs only the verify gate + contract, so
	// --set is optional there (mirroring backfill --verify-only); any
	// given sets are still parsed for their refusals.
	var sets []ir.BackfillSet
	if c.ResumeFrom != string(expandcontract.LegContract) || len(c.Set) > 0 {
		if sets, err = pipeline.ParseBackfillSets(c.Set); err != nil {
			return err
		}
	}

	o := &expandcontract.Orchestrator{
		API:           api.New(api.Config{TokenID: c.ServiceTokenID, Token: c.ServiceToken}),
		Org:           c.Org,
		Database:      c.Database,
		Branch:        c.Branch,
		Engine:        engine,
		DSN:           c.DSN,
		Table:         c.Table,
		Sets:          sets,
		Where:         c.Where,
		BatchSize:     c.BatchSize,
		ExpandDDL:     c.ExpandDDL,
		ContractDDL:   c.ContractDDL,
		Yes:           c.Yes,
		DryRun:        c.DryRun,
		KeepBranches:  c.KeepBranches,
		ResumeFrom:    expandcontract.Leg(c.ResumeFrom),
		PollInterval:  c.PollInterval,
		DeployTimeout: c.DeployTimeout,
		Out:           os.Stdout,
	}
	// The orchestrator narrates each leg (and the stop-at-the-gate
	// instructions) to Out; the error is the only thing left to map.
	_, err = o.Run(kongContext())
	return err
}
