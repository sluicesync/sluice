// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the ADR-0182 query-timeout controller arming (roadmap item 110):
// the controller is composed whenever the PlanetScale credentials resolve
// (so it is also available for the crash-recovery auto-revert), and the
// --planetscale-raise-query-timeout flag is a LOUD refusal — not a silent
// no-op — when set on a target/credential combination that cannot honor it.

package main

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/planetscale/querytimeout"
)

// armedTimeoutCmd is a fully-credentialled baseline; tests knock pieces out.
func armedTimeoutCmd() *MigrateCmd {
	return &MigrateCmd{
		TargetDriver:              "planetscale",
		Target:                    "user:pw@tcp(host.psdb.cloud:3306)/shopdb?tls=true",
		PlanetScaleOrg:            "acme",
		PlanetScaleBranch:         "main",
		PlanetScaleServiceTokenID: "tokid",
		PlanetScaleServiceToken:   "toksecret",
	}
}

func TestQueryTimeoutController_ComposeMatrix(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*MigrateCmd)
		wantCtl bool
		wantDB  string
	}{
		{"fully credentialled derives database from target DSN", func(*MigrateCmd) {}, true, "shopdb"},
		{"explicit --planetscale-database wins", func(m *MigrateCmd) { m.PlanetScaleDatabase = "other" }, true, "other"},
		{"non-planetscale target composes nothing", func(m *MigrateCmd) { m.TargetDriver = "mysql" }, false, ""},
		{"postgres target composes nothing", func(m *MigrateCmd) { m.TargetDriver = "postgres" }, false, ""},
		{"no org composes nothing", func(m *MigrateCmd) { m.PlanetScaleOrg = "" }, false, ""},
		{"missing token secret composes nothing", func(m *MigrateCmd) { m.PlanetScaleServiceToken = "" }, false, ""},
		{"missing token id composes nothing", func(m *MigrateCmd) { m.PlanetScaleServiceTokenID = "" }, false, ""},
		{"unparsable DSN + no explicit db composes nothing", func(m *MigrateCmd) { m.Target = "://nope" }, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := armedTimeoutCmd()
			tc.mutate(m)
			ctl, err := m.planetScaleQueryTimeoutController()
			if err != nil {
				t.Fatalf("compose (flag off) must never error; got %v", err)
			}
			if !tc.wantCtl {
				if ctl != nil {
					t.Fatalf("controller = %#v; want nil", ctl)
				}
				return
			}
			r, ok := ctl.(*querytimeout.Raiser)
			if !ok {
				t.Fatalf("controller = %T; want *querytimeout.Raiser", ctl)
			}
			if r.Org != "acme" || r.Database != tc.wantDB || r.Branch != "main" {
				t.Errorf("raiser org/db/branch = %s/%s/%s; want acme/%s/main", r.Org, r.Database, r.Branch, tc.wantDB)
			}
		})
	}
}

// TestQueryTimeoutController_ArmedRefusesWithoutCredentials pins the loud
// refusal: with the flag set, an unarmable configuration is an ERROR, unlike
// the index fallback's opportunistic WARN-and-off.
func TestQueryTimeoutController_ArmedRefusesWithoutCredentials(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*MigrateCmd)
	}{
		{"non-planetscale target", func(m *MigrateCmd) { m.TargetDriver = "mysql" }},
		{"no org", func(m *MigrateCmd) { m.PlanetScaleOrg = "" }},
		{"missing service token", func(m *MigrateCmd) { m.PlanetScaleServiceToken = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := armedTimeoutCmd()
			m.PlanetScaleRaiseQueryTimeout = true
			tc.mutate(m)
			ctl, err := m.planetScaleQueryTimeoutController()
			if err == nil {
				t.Fatalf("armed flag with %s must refuse; got controller %#v", tc.name, ctl)
			}
			if !strings.Contains(err.Error(), "planetscale-raise-query-timeout") {
				t.Errorf("refusal should name the flag; got %v", err)
			}
		})
	}
}

// TestQueryTimeoutController_ArmedWithCredentialsComposes confirms the armed
// happy path returns the controller and no error.
func TestQueryTimeoutController_ArmedWithCredentialsComposes(t *testing.T) {
	m := armedTimeoutCmd()
	m.PlanetScaleRaiseQueryTimeout = true
	ctl, err := m.planetScaleQueryTimeoutController()
	if err != nil {
		t.Fatalf("armed + credentialled must compose cleanly; got %v", err)
	}
	if ctl == nil {
		t.Fatal("armed + credentialled must return a controller")
	}
}

// TestQueryTimeoutRaiseFlagParses proves the kong flag reaches the field (the
// Bug-180 pin-through-the-real-parser lesson).
func TestQueryTimeoutRaiseFlagParses(t *testing.T) {
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse([]string{
		"migrate",
		"--source-driver=mysql", "--source=src-dsn",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
		"--planetscale-raise-query-timeout",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !cli.Migrate.PlanetScaleRaiseQueryTimeout {
		t.Error("--planetscale-raise-query-timeout did not set the field")
	}
}
