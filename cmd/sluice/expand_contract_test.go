// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// CLI-layer pins for `sluice expand-contract` (ADR-0162), driven
// through the REAL kong parser with argv (the Bug-180 house rule).

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
)

// ecArgs builds the minimal valid argv; flag values carrying spaces
// (--where, the DDLs) stay single arguments.
func ecArgs(extra ...string) []string {
	args := make([]string, 0, 6+len(extra))
	args = append(
		args,
		"expand-contract", "--org=acme", "--database=shop",
		"--dsn=user:pw@tcp(h:3306)/shop", "--table=items",
		"--where=full_name IS NULL",
	)
	return append(args, extra...)
}

func TestExpandContractCmd_ParsesAllFlags(t *testing.T) {
	cli := parseInto(t, ecArgs(
		"--branch=prod",
		"--service-token-id=tid",
		"--service-token=tsecret",
		"--expand-ddl=ALTER TABLE items ADD COLUMN full_name VARCHAR(255)",
		"--contract-ddl=ALTER TABLE items DROP COLUMN first_name",
		"--set=full_name = CONCAT(first_name, ' ', last_name)",
		"--batch-size=500",
		"--yes",
		"--keep-branches",
		"--resume-from=migrate",
		"--poll-interval=2s",
		"--deploy-timeout=30m",
	)...)
	c := cli.ExpandContract
	if c.Org != "acme" || c.Database != "shop" || c.Branch != "prod" {
		t.Errorf("org/db/branch = %q/%q/%q", c.Org, c.Database, c.Branch)
	}
	if c.ServiceTokenID != "tid" || c.ServiceToken != "tsecret" {
		t.Errorf("token halves = %q/%q", c.ServiceTokenID, c.ServiceToken)
	}
	if c.ExpandDDL != "ALTER TABLE items ADD COLUMN full_name VARCHAR(255)" {
		t.Errorf("ExpandDDL = %q", c.ExpandDDL)
	}
	if c.ContractDDL != "ALTER TABLE items DROP COLUMN first_name" {
		t.Errorf("ContractDDL = %q", c.ContractDDL)
	}
	// sep:"none": the comma inside CONCAT must NOT split the --set value.
	if len(c.Set) != 1 || c.Set[0] != "full_name = CONCAT(first_name, ' ', last_name)" {
		t.Errorf("Set = %q; want one unsplit clause", c.Set)
	}
	if c.Where != "full_name IS NULL" {
		t.Errorf("Where = %q", c.Where)
	}
	if c.BatchSize != 500 || !c.Yes || !c.KeepBranches {
		t.Errorf("BatchSize=%d Yes=%v KeepBranches=%v", c.BatchSize, c.Yes, c.KeepBranches)
	}
	if c.ResumeFrom != "migrate" {
		t.Errorf("ResumeFrom = %q; want migrate", c.ResumeFrom)
	}
	if c.PollInterval != 2*time.Second || c.DeployTimeout != 30*time.Minute {
		t.Errorf("PollInterval=%s DeployTimeout=%s", c.PollInterval, c.DeployTimeout)
	}
}

// TestExpandContractCmd_Defaults pins the zero-config shape: branch
// main, resume-from expand, 10s poll, 1h deploy timeout, contract
// gated off (--yes false).
func TestExpandContractCmd_Defaults(t *testing.T) {
	c := parseInto(t, ecArgs()...).ExpandContract
	if c.Branch != "main" {
		t.Errorf("default Branch = %q; want main", c.Branch)
	}
	if c.ResumeFrom != "expand" {
		t.Errorf("default ResumeFrom = %q; want expand", c.ResumeFrom)
	}
	if c.PollInterval != 10*time.Second || c.DeployTimeout != time.Hour {
		t.Errorf("defaults PollInterval=%s DeployTimeout=%s; want 10s/1h", c.PollInterval, c.DeployTimeout)
	}
	if c.Yes || c.DryRun || c.KeepBranches {
		t.Errorf("Yes=%v DryRun=%v KeepBranches=%v; want all false", c.Yes, c.DryRun, c.KeepBranches)
	}
}

// TestExpandContractCmd_ServiceTokenFromEnv pins the pscale-CLI env
// convention: PLANETSCALE_SERVICE_TOKEN_ID / PLANETSCALE_SERVICE_TOKEN
// fill the flags when unset on the command line.
func TestExpandContractCmd_ServiceTokenFromEnv(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")
	c := parseInto(t, ecArgs()...).ExpandContract
	if c.ServiceTokenID != "env-id" || c.ServiceToken != "env-secret" {
		t.Errorf("token halves from env = %q/%q; want env-id/env-secret", c.ServiceTokenID, c.ServiceToken)
	}
}

func TestExpandContractCmd_RequiredFlags(t *testing.T) {
	required := []string{"--org", "--database", "--dsn", "--table", "--where"}
	for _, missing := range required {
		t.Run("missing "+missing, func(t *testing.T) {
			var args []string
			for _, a := range ecArgs() {
				if !strings.HasPrefix(a, missing+"=") {
					args = append(args, a)
				}
			}
			cli := &CLI{}
			parser, err := kong.New(cli, kong.Exit(func(int) {}))
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			if _, err := parser.Parse(args); err == nil {
				t.Errorf("parse without %s succeeded; want a missing-required error", missing)
			}
		})
	}
}

func TestExpandContractCmd_ResumeFromEnumRejectsUnknownLeg(t *testing.T) {
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse(ecArgs("--resume-from=verify")); err == nil {
		t.Error("parse --resume-from=verify succeeded; want an enum error (verify is never a resume point — it always runs before contract)")
	}
}

// TestExpandContractCmd_DryRunXorYes pins that --dry-run and --yes are
// contradictory at the parser: a dry run plans, it never confirms a
// destructive contract.
func TestExpandContractCmd_DryRunXorYes(t *testing.T) {
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse(ecArgs("--dry-run", "--yes")); err == nil {
		t.Error("parse --dry-run --yes succeeded; want an xor usage error")
	}
}

// TestExpandContractCmd_MissingTokenIsRunTimeErrorNamingEnvVars pins
// the token refusal shape: not kong-required (so the message can name
// the env vars), refused in Run before anything is touched.
func TestExpandContractCmd_MissingTokenIsRunTimeErrorNamingEnvVars(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "")
	cli := parseInto(t, ecArgs("--set=a = b", "--expand-ddl=ALTER TABLE items ADD COLUMN a INT")...)
	err := cli.ExpandContract.Run(&Globals{})
	if err == nil || !strings.Contains(err.Error(), "PLANETSCALE_SERVICE_TOKEN_ID") {
		t.Fatalf("Run without a token = %v; want a refusal naming the env vars", err)
	}
}
