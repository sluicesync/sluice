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

// TestSyncForeignKeyChecker_ComposeMatrix is the sync cold-start half of the
// copy-phase parity: the SAME composition, pinned through BOTH sync
// construction paths — `sync start` (SyncStartCmd.planetScaleForeignKeyChecker)
// and the fleet SyncSpec (SyncSpec.resolveForeignKeyChecker). A gap here is the
// v0.129.0 field report: `sync start` into a fresh FK-disabled PlanetScale DB
// burns the whole cold-start copy then walls at constraints. Same Bug-180
// through-the-CLI-plumbing discipline as the migrate matrix above.
func TestSyncForeignKeyChecker_ComposeMatrix(t *testing.T) {
	cases := []struct {
		name     string
		driver   string
		org      string
		tokenID  string
		token    string
		db       string
		wantChkr bool
		wantDB   string
	}{
		{"fully credentialled derives database from target DSN", "planetscale", "acme", "tokid", "toksecret", "", true, "shopdb"},
		{"explicit database wins over the DSN", "planetscale", "acme", "tokid", "toksecret", "other", true, "other"},
		{"non-planetscale target composes nothing", "mysql", "acme", "tokid", "toksecret", "", false, ""},
		{"no org composes nothing", "planetscale", "", "tokid", "toksecret", "", false, ""},
		{"missing token secret composes nothing (WARN path)", "planetscale", "acme", "tokid", "", "", false, ""},
	}
	const targetDSN = "user:pw@tcp(host.psdb.cloud:3306)/shopdb?tls=true"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := &SyncStartCmd{
				TargetDriver: tc.driver, Target: targetDSN, PlanetScaleOrg: tc.org,
				PlanetScaleBranch: "main", PlanetScaleDatabase: tc.db,
				PlanetScaleServiceTokenID: tc.tokenID, PlanetScaleServiceToken: tc.token,
			}
			spec := &SyncSpec{
				TargetDriver: tc.driver, Target: targetDSN, PlanetScaleOrg: tc.org,
				PlanetScaleBranch: "main", PlanetScaleDatabase: tc.db,
				PlanetScaleServiceTokenID: tc.tokenID, PlanetScaleServiceToken: tc.token,
			}
			for who, chkr := range map[string]any{
				"sync start": start.planetScaleForeignKeyChecker(),
				"fleet spec": spec.resolveForeignKeyChecker(),
			} {
				c, ok := chkr.(*fkcheck.Checker)
				switch {
				case !tc.wantChkr && ok:
					t.Errorf("%s: checker = %#v; want nil", who, c)
				case tc.wantChkr && !ok:
					t.Errorf("%s: checker = %T; want *fkcheck.Checker", who, chkr)
				case tc.wantChkr && ok && (c.Org != "acme" || c.Database != tc.wantDB):
					t.Errorf("%s: checker org/db = %s/%s; want acme/%s", who, c.Org, c.Database, tc.wantDB)
				}
			}
		})
	}
}
