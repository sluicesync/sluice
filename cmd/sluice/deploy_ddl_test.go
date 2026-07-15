// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// CLI-layer pins for `sluice deploy-ddl` and `sluice control-tables
// ddl` (ADR-0165), driven through the REAL kong parser with argv (the
// Bug-180 house rule).

package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
)

func ddArgs(extra ...string) []string {
	args := make([]string, 0, 4+len(extra))
	args = append(
		args,
		"deploy-ddl", "--org=acme", "--database=shop",
		"--ddl=CREATE TABLE IF NOT EXISTS `sluice_cdc_state` (stream_id VARCHAR(255) NOT NULL)",
	)
	return append(args, extra...)
}

func TestDeployDDLCmd_ParsesAllFlags(t *testing.T) {
	cli := parseInto(t, ddArgs(
		"--branch=prod",
		"--service-token-id=tid",
		"--service-token=tsecret",
		"--dry-run",
		"--keep-branches",
		"--poll-interval=2s",
		"--deploy-timeout=30m",
	)...)
	c := cli.DeployDDL
	if c.Org != "acme" || c.Database != "shop" || c.Branch != "prod" {
		t.Errorf("org/db/branch = %q/%q/%q", c.Org, c.Database, c.Branch)
	}
	if c.ServiceTokenID != "tid" || c.ServiceToken != "tsecret" {
		t.Errorf("token halves = %q/%q", c.ServiceTokenID, c.ServiceToken)
	}
	// The DDL value carries spaces and backticks verbatim.
	if !strings.HasPrefix(c.DDL, "CREATE TABLE IF NOT EXISTS `sluice_cdc_state`") {
		t.Errorf("DDL = %q", c.DDL)
	}
	if !c.DryRun || !c.KeepBranches {
		t.Errorf("bools: dry-run=%v keep-branches=%v", c.DryRun, c.KeepBranches)
	}
	if c.PollInterval != 2*time.Second || c.DeployTimeout != 30*time.Minute {
		t.Errorf("intervals = %v/%v", c.PollInterval, c.DeployTimeout)
	}
}

// TestDeployDDLCmd_Defaults pins the zero-config shape: branch main,
// 10s poll, 1h deploy timeout — the expand-contract defaults.
func TestDeployDDLCmd_Defaults(t *testing.T) {
	c := parseInto(t, ddArgs()...).DeployDDL
	if c.Branch != "main" {
		t.Errorf("Branch default = %q; want main", c.Branch)
	}
	if c.PollInterval != 10*time.Second || c.DeployTimeout != time.Hour {
		t.Errorf("interval defaults = %v/%v; want 10s/1h", c.PollInterval, c.DeployTimeout)
	}
}

func TestDeployDDLCmd_RequiredFlags(t *testing.T) {
	for _, missing := range []string{"--org", "--database", "--ddl"} {
		t.Run("missing "+missing, func(t *testing.T) {
			var args []string
			for _, a := range ddArgs() {
				if !strings.HasPrefix(a, missing+"=") {
					args = append(args, a)
				}
			}
			parser, err := kong.New(&CLI{}, kong.Exit(func(int) {}))
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			if _, err := parser.Parse(args); err == nil {
				t.Errorf("parse without %s succeeded; want a missing-required error", missing)
			}
		})
	}
}

// TestDeployDDLCmd_MissingTokenIsRunTimeErrorNamingEnvVars pins that
// the token refusal happens in Run (not kong-required) so it can name
// the env vars, and that --dry-run skips it (no control plane).
func TestDeployDDLCmd_MissingTokenIsRunTimeErrorNamingEnvVars(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "")
	cli := parseInto(t, ddArgs()...)
	err := cli.DeployDDL.Run(&Globals{})
	if err == nil || !strings.Contains(err.Error(), "PLANETSCALE_SERVICE_TOKEN_ID") {
		t.Fatalf("Run without a token = %v; want a refusal naming the env vars", err)
	}
}

// ---- control-tables ddl ----

func TestControlTablesDDLCmd_DefaultEngineIsPlanetScale(t *testing.T) {
	c := parseInto(t, "control-tables", "ddl").ControlTables.DDL
	if c.Engine != "planetscale" {
		t.Errorf("Engine default = %q; want planetscale", c.Engine)
	}
}

// TestControlTablesDDLCmd_PrintsBootstrapSet runs the real printer and
// pins the five-table bootstrap set plus the deploy-ddl recipe line —
// stdout is pure SQL + `--` comments, pasteable per statement.
func TestControlTablesDDLCmd_PrintsBootstrapSet(t *testing.T) {
	c := parseInto(t, "control-tables", "ddl").ControlTables.DDL

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	runErr := c.Run()
	os.Stdout = orig
	_ = w.Close()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	out := string(raw)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	for _, want := range []string{
		"sluice deploy-ddl",
		"-- sluice_migrate_state\n",
		"-- sluice_migrate_table_progress\n",
		"-- sluice_cdc_state\n",
		"-- sluice_cdc_schema_history\n",
		"-- sluice_shard_consolidation_lease\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if got := strings.Count(out, "CREATE TABLE IF NOT EXISTS"); got != 5 {
		t.Errorf("CREATE statements = %d; want 5", got)
	}
	// Pasteable: every non-empty line is SQL or a -- comment.
	for _, line := range strings.Split(out, "\n") {
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		if strings.Contains(line, "sluice ") && !strings.HasPrefix(line, "--") {
			t.Errorf("non-comment prose line in output: %q", line)
		}
	}
}

// TestControlTablesDDLCmd_RefusesEngineWithoutTheSurface pins the
// capability refusal: postgres does not (yet) publish control-table
// DDL, and the refusal names the supported family instead of printing
// nothing.
func TestControlTablesDDLCmd_RefusesEngineWithoutTheSurface(t *testing.T) {
	c := parseInto(t, "control-tables", "ddl", "--engine=postgres").ControlTables.DDL
	err := c.Run()
	if err == nil || !strings.Contains(err.Error(), "does not publish") {
		t.Fatalf("Run with postgres = %v; want the capability refusal", err)
	}
}
