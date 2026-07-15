// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// CLI-layer pins for `sluice backfill` (ADR-0159), driven through the
// REAL kong parser with argv (the Bug-180 house rule: a fix or default
// that only fires for a CLI value must be pinned through the actual
// parser, not a struct literal — a kong default/enum could make the
// intended zero value unreachable and a direct-call test would never
// notice).

package main

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

const backfillBaseArgs = "backfill --driver=mysql --dsn=user:pw@tcp(h:3306)/db --table=items"

func TestBackfillCmd_ParsesAllFlags(t *testing.T) {
	args := append(
		strings.Fields(backfillBaseArgs),
		"--set=new_col = old_col * 2",
		"--set=flag = CASE WHEN status = 'x' THEN 1 ELSE 0 END",
		"--where=new_col IS NULL",
		"--batch-size=250",
		"--dry-run",
		"--restart",
	)
	cli := parseInto(t, args...)
	c := cli.Backfill
	if c.Driver != "mysql" {
		t.Errorf("Driver = %q; want mysql", c.Driver)
	}
	if c.DSN != "user:pw@tcp(h:3306)/db" {
		t.Errorf("DSN = %q", c.DSN)
	}
	if c.Table != "items" {
		t.Errorf("Table = %q; want items", c.Table)
	}
	if len(c.Set) != 2 ||
		c.Set[0] != "new_col = old_col * 2" ||
		c.Set[1] != "flag = CASE WHEN status = 'x' THEN 1 ELSE 0 END" {
		t.Errorf("Set = %q; want the two raw clauses", c.Set)
	}
	if c.Where != "new_col IS NULL" {
		t.Errorf("Where = %q", c.Where)
	}
	// sep:"none": a comma inside an expression must NOT split the value
	// (kong's slice default would split on ',').
	commaArgs := append(strings.Fields(backfillBaseArgs), "--set=n = COALESCE(a, b, 0)")
	if got := parseInto(t, commaArgs...).Backfill.Set; len(got) != 1 || got[0] != "n = COALESCE(a, b, 0)" {
		t.Errorf("comma-carrying --set = %q; want one unsplit clause", got)
	}
	if c.BatchSize != 250 {
		t.Errorf("BatchSize = %d; want 250", c.BatchSize)
	}
	if !c.DryRun || !c.Restart {
		t.Errorf("DryRun=%v Restart=%v; want true, true", c.DryRun, c.Restart)
	}
}

// TestBackfillCmd_OmittedOptionalFlagDefaults pins the zero-value-safe
// defaults: an omitted --where is the empty predicate (whole table),
// an omitted --batch-size parses to 0 — which the pipeline resolves to
// migcore.DefaultBulkBatchSize (pinned in
// pipeline.TestBackfill_ZeroBatchSizeUsesDefault) — and --dry-run /
// --restart default off.
func TestBackfillCmd_OmittedOptionalFlagDefaults(t *testing.T) {
	args := append(strings.Fields(backfillBaseArgs), "--set=a = b")
	cli := parseInto(t, args...)
	c := cli.Backfill
	if c.Where != "" {
		t.Errorf("omitted --where = %q; want empty", c.Where)
	}
	if c.BatchSize != 0 {
		t.Errorf("omitted --batch-size = %d; want 0 (pipeline resolves the default)", c.BatchSize)
	}
	if c.DryRun || c.Restart {
		t.Errorf("DryRun=%v Restart=%v; want false, false", c.DryRun, c.Restart)
	}
}

// TestBackfillCmd_RequiredFlags pins that driver/dsn/table/set are all
// kong-required — omitting any is a parse error, not a Run-time one.
func TestBackfillCmd_RequiredFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"missing --set", strings.Fields(backfillBaseArgs)},
		{"missing --table", []string{"backfill", "--driver=mysql", "--dsn=d", "--set=a = b"}},
		{"missing --dsn", []string{"backfill", "--driver=mysql", "--table=t", "--set=a = b"}},
		{"missing --driver", []string{"backfill", "--dsn=d", "--table=t", "--set=a = b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli := &CLI{}
			parser, err := kong.New(cli, kong.Exit(func(int) {}))
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			if _, err := parser.Parse(tc.args); err == nil {
				t.Errorf("parse %v succeeded; want a missing-required error", tc.args)
			}
		})
	}
}
