// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for composing the PlanetScale foreign-key preflight probe through the
// CLI layer (the Bug-180 "pin the fix through the CLI plumbing" lesson): the
// checker is composed whenever the PlanetScale credentials resolve, and nil is a
// valid, expected result (a missing token is a WARN in the pipeline, not a
// refusal here). It is deliberately NOT armed by a flag.

package main

import (
	"testing"

	"sluicesync.dev/sluice/internal/planetscale/fkcheck"
)

func armedFKCheckerCmd() *MigrateCmd {
	return &MigrateCmd{
		TargetDriver:              "planetscale",
		Target:                    "user:pw@tcp(host.psdb.cloud:3306)/shopdb?tls=true",
		PlanetScaleOrg:            "acme",
		PlanetScaleBranch:         "main",
		PlanetScaleServiceTokenID: "tokid",
		PlanetScaleServiceToken:   "toksecret",
	}
}

func TestForeignKeyChecker_ComposeMatrix(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(*MigrateCmd)
		wantChkr   bool
		wantDB     string
		wantBranch string
	}{
		{"fully credentialled derives database from target DSN", func(*MigrateCmd) {}, true, "shopdb", "main"},
		{"explicit --planetscale-database wins", func(m *MigrateCmd) { m.PlanetScaleDatabase = "other" }, true, "other", "main"},
		{"empty branch is left empty (checker defaults to main)", func(m *MigrateCmd) { m.PlanetScaleBranch = "" }, true, "shopdb", ""},
		{"non-planetscale target composes nothing", func(m *MigrateCmd) { m.TargetDriver = "mysql" }, false, "", ""},
		{"postgres target composes nothing", func(m *MigrateCmd) { m.TargetDriver = "postgres" }, false, "", ""},
		{"no org composes nothing", func(m *MigrateCmd) { m.PlanetScaleOrg = "" }, false, "", ""},
		{"missing token secret composes nothing (WARN path, not refusal)", func(m *MigrateCmd) { m.PlanetScaleServiceToken = "" }, false, "", ""},
		{"missing token id composes nothing", func(m *MigrateCmd) { m.PlanetScaleServiceTokenID = "" }, false, "", ""},
		{"unparsable DSN + no explicit db composes nothing", func(m *MigrateCmd) { m.Target = "://nope" }, false, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := armedFKCheckerCmd()
			tc.mutate(m)
			chkr := m.planetScaleForeignKeyChecker()
			if !tc.wantChkr {
				if chkr != nil {
					t.Fatalf("checker = %#v; want nil", chkr)
				}
				return
			}
			c, ok := chkr.(*fkcheck.Checker)
			if !ok {
				t.Fatalf("checker = %T; want *fkcheck.Checker", chkr)
			}
			if c.Org != "acme" || c.Database != tc.wantDB || c.Branch != tc.wantBranch {
				t.Errorf("checker org/db/branch = %s/%s/%s; want acme/%s/%s", c.Org, c.Database, c.Branch, tc.wantDB, tc.wantBranch)
			}
		})
	}
}
