// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/alecthomas/kong"
)

// TestExprAndTypeOverrideFlag_KongCommaPreservation pins Bug #16-sub
// (and its latent --type-override sibling) — the same regression class
// as Bug 59's --redact split. kong's default sep:"," for a []string
// flag splits a comma-bearing value, so
// `--expr-override 'tbl.col=ST_SetSRID(ST_MakePoint(lon,lat),0)'`
// arrived as the fragments [`tbl.col=ST_SetSRID(ST_MakePoint(lon`,
// `lat)`, `0)`], and the parser then rejected `lat)` with
// `missing '=' in "lat)"`. --type-override has the identical defect
// for a parameterized target type (`products.amt=numeric(20,0)`).
// The fix is `sep:"none"` on every ExprOverride / TypeOverride field,
// exactly as Bug 59 did for Redact. Each subtest builds a kong parser,
// parses a comma-containing value, and confirms the slice has exactly
// one element with the comma intact.
func TestExprAndTypeOverrideFlag_KongCommaPreservation(t *testing.T) {
	const exprVal = "geo.p=ST_SetSRID(ST_MakePoint(lon,lat),0)"
	const typeVal = "products.amt=numeric(20,0)"
	cases := []struct {
		name string
		args []string
		want string
		get  func(*CLI) []string
	}{
		{
			name: "migrate expr-override",
			args: []string{
				"migrate",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=postgres", "--target=postgres://u:p@h/db",
				"--expr-override=" + exprVal,
			},
			want: exprVal,
			get:  func(c *CLI) []string { return c.Migrate.ExprOverride },
		},
		{
			name: "migrate type-override",
			args: []string{
				"migrate",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=postgres", "--target=postgres://u:p@h/db",
				"--type-override=" + typeVal,
			},
			want: typeVal,
			get:  func(c *CLI) []string { return c.Migrate.TypeOverride },
		},
		{
			name: "sync-start expr-override",
			args: []string{
				"sync", "start",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=postgres", "--target=postgres://u:p@h/db",
				"--expr-override=" + exprVal,
			},
			want: exprVal,
			get:  func(c *CLI) []string { return c.Sync.Start.ExprOverride },
		},
		{
			name: "schema-preview expr-override",
			args: []string{
				"schema", "preview",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=postgres", "--target=postgres://u:p@h/db",
				"--expr-override=" + exprVal,
			},
			want: exprVal,
			get:  func(c *CLI) []string { return c.Schema.Preview.ExprOverride },
		},
		{
			name: "schema-preview type-override",
			args: []string{
				"schema", "preview",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=postgres", "--target=postgres://u:p@h/db",
				"--type-override=" + typeVal,
			},
			want: typeVal,
			get:  func(c *CLI) []string { return c.Schema.Preview.TypeOverride },
		},
		{
			name: "schema-diff expr-override",
			args: []string{
				"schema", "diff",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=postgres", "--target=postgres://u:p@h/db",
				"--expr-override=" + exprVal,
			},
			want: exprVal,
			get:  func(c *CLI) []string { return c.Schema.Diff.ExprOverride },
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cli := &CLI{}
			parser, err := kong.New(cli,
				kong.Vars{"version": "test"},
				kong.Exit(func(int) {}),
			)
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			if _, err := parser.Parse(c.args); err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got := c.get(cli)
			if len(got) != 1 {
				t.Fatalf("len = %d; want 1 (kong split the value on a comma — Bug #16-sub regression). values: %q", len(got), got)
			}
			if got[0] != c.want {
				t.Errorf("[0] = %q; want %q", got[0], c.want)
			}
		})
	}
}
