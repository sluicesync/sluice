// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the ADR-0148 index-fallback arming (roadmap item 67): the
// opportunistic never-refuse arming matrix, the target-DSN database
// derivation, and — per the Bug-180 pin-through-the-real-parser lesson —
// a kong parse proving the new migrate flags (and their env fallbacks)
// actually reach the fields the arming reads.

package main

import (
	"testing"
	"time"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/planetscale/expandcontract"
)

// armedMigrateCmd is a fully-armed baseline; tests knock pieces out.
func armedMigrateCmd() *MigrateCmd {
	return &MigrateCmd{
		TargetDriver:              "planetscale",
		Target:                    "user:pw@tcp(host.psdb.cloud:3306)/shopdb?tls=true",
		PlanetScaleOrg:            "acme",
		PlanetScaleBranch:         "main",
		PlanetScaleServiceTokenID: "tokid",
		PlanetScaleServiceToken:   "toksecret",
		PlanetScaleDeployTimeout:  time.Hour,
	}
}

// TestPlanetScaleIndexFallback_ArmingMatrix pins the opportunistic
// arming contract: every incomplete configuration yields nil (the
// byte-identical pre-ADR-0148 migrate — WARN at most, never a refusal,
// because the org/token routinely arrive from ambient env vars), and the
// complete planetscale-target configuration arms.
func TestPlanetScaleIndexFallback_ArmingMatrix(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*MigrateCmd)
		wantArm bool
		wantDB  string
	}{
		{"fully armed derives database from target DSN", func(*MigrateCmd) {}, true, "shopdb"},
		{"explicit --planetscale-database wins over the DSN", func(m *MigrateCmd) { m.PlanetScaleDatabase = "other" }, true, "other"},
		{"non-planetscale target stays unarmed", func(m *MigrateCmd) { m.TargetDriver = "mysql" }, false, ""},
		{"postgres target stays unarmed", func(m *MigrateCmd) { m.TargetDriver = "postgres" }, false, ""},
		{"no org stays unarmed", func(m *MigrateCmd) { m.PlanetScaleOrg = "" }, false, ""},
		{"missing token secret stays unarmed", func(m *MigrateCmd) { m.PlanetScaleServiceToken = "" }, false, ""},
		{"missing token id stays unarmed", func(m *MigrateCmd) { m.PlanetScaleServiceTokenID = "" }, false, ""},
		{"unparsable DSN with no explicit database stays unarmed", func(m *MigrateCmd) { m.Target = "://not-a-dsn" }, false, ""},
		{"unparsable DSN with explicit database arms", func(m *MigrateCmd) {
			m.Target = "://not-a-dsn"
			m.PlanetScaleDatabase = "shopdb"
		}, true, "shopdb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := armedMigrateCmd()
			tc.mutate(m)
			got := m.planetScaleIndexFallback()
			if !tc.wantArm {
				if got != nil {
					t.Fatalf("fallback armed = %#v; want nil", got)
				}
				return
			}
			fb, ok := got.(*expandcontract.IndexFallback)
			if !ok {
				t.Fatalf("fallback = %T; want *expandcontract.IndexFallback", got)
			}
			if fb.Org != "acme" || fb.Database != tc.wantDB || fb.Branch != "main" {
				t.Errorf("fallback org/db/branch = %s/%s/%s; want acme/%s/main", fb.Org, fb.Database, fb.Branch, tc.wantDB)
			}
			if fb.DeployTimeout != time.Hour {
				t.Errorf("DeployTimeout = %v; want 1h", fb.DeployTimeout)
			}
			if fb.API == nil {
				t.Error("armed fallback carries no API client")
			}
		})
	}
}

// TestMigrateIndexFallbackFlags_ThroughKong pins the real-parser leg
// (the Bug-180 lesson): the new flags and their env fallbacks populate
// the exact fields planetScaleIndexFallback reads.
func TestMigrateIndexFallbackFlags_ThroughKong(t *testing.T) {
	t.Setenv("PLANETSCALE_ORG", "env-org")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")

	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse([]string{
		"migrate",
		"--source-driver=mysql", "--source=src-dsn",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
		"--planetscale-database=explicit-db",
		"--planetscale-branch=prod",
		"--planetscale-deploy-timeout=30m",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	m := &cli.Migrate
	if m.PlanetScaleOrg != "env-org" {
		t.Errorf("PlanetScaleOrg = %q; want the PLANETSCALE_ORG env fallback", m.PlanetScaleOrg)
	}
	if m.PlanetScaleServiceTokenID != "env-id" || m.PlanetScaleServiceToken != "env-secret" {
		t.Errorf("token = %q/%q; want the env fallbacks", m.PlanetScaleServiceTokenID, m.PlanetScaleServiceToken)
	}
	if m.PlanetScaleDatabase != "explicit-db" || m.PlanetScaleBranch != "prod" {
		t.Errorf("database/branch = %q/%q; want explicit-db/prod", m.PlanetScaleDatabase, m.PlanetScaleBranch)
	}
	if m.PlanetScaleDeployTimeout != 30*time.Minute {
		t.Errorf("PlanetScaleDeployTimeout = %v; want 30m", m.PlanetScaleDeployTimeout)
	}
	// The env-armed parse must produce a live fallback end-to-end.
	if fb := m.planetScaleIndexFallback(); fb == nil {
		t.Error("kong-parsed, env-armed migrate did not arm the fallback")
	}
}

// TestMigrateIndexFallbackFlags_DefaultsUnarmed pins the zero-config
// default through the real parser: with no PS env vars and no flags the
// fallback stays nil — the pre-ADR-0148 migrate, byte-identical.
func TestMigrateIndexFallbackFlags_DefaultsUnarmed(t *testing.T) {
	t.Setenv("PLANETSCALE_ORG", "")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "")

	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse([]string{
		"migrate",
		"--source-driver=mysql", "--source=src-dsn",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if fb := cli.Migrate.planetScaleIndexFallback(); fb != nil {
		t.Errorf("fallback = %#v; want nil with no org/token configured", fb)
	}
}

// TestMysqlDSNDatabase pins the derivation helper's edges.
func TestMysqlDSNDatabase(t *testing.T) {
	cases := []struct {
		dsn  string
		want string
	}{
		{"user:pw@tcp(host:3306)/shopdb", "shopdb"},
		{"user:pw@tcp(host:3306)/shopdb?tls=true&interpolateParams=true", "shopdb"},
		{"user:pw@tcp(host:3306)/", ""},
		{"://not-a-dsn", ""},
	}
	for _, tc := range cases {
		if got := mysqlDSNDatabase(tc.dsn); got != tc.want {
			t.Errorf("mysqlDSNDatabase(%q) = %q; want %q", tc.dsn, got, tc.want)
		}
	}
}
