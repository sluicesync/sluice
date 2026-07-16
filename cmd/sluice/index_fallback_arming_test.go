// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the audit-MED-A1 widening of the ADR-0148 index-fallback
// arming beyond migrate: `restore` and `sync start` carry the same
// arming flag set (shared composer), their pre-existing
// `--planetscale-org` TELEMETRY meaning is reconciled via
// telemetryParamsSharedOrg (a fallback-only arming turns telemetry off
// with a WARN instead of tripping the all-or-nothing refusal), and — per
// the Bug-180 pin-through-the-real-parser lesson — kong parses prove the
// new flags (and the service-token env fallbacks) reach the exact fields
// the arming reads.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/planetscale/expandcontract"
)

// armedRestoreCmd is a fully-armed baseline; tests knock pieces out.
func armedRestoreCmd() *RestoreCmd {
	return &RestoreCmd{
		TargetDriver:              "planetscale",
		Target:                    "user:pw@tcp(host.psdb.cloud:3306)/shopdb?tls=true",
		PlanetScaleOrg:            "acme",
		PlanetScaleBranch:         "main",
		PlanetScaleServiceTokenID: "tokid",
		PlanetScaleServiceToken:   "toksecret",
		PlanetScaleDeployTimeout:  time.Hour,
	}
}

func (r *RestoreCmd) testIndexFallbackParams() indexFallbackParams {
	return indexFallbackParams{
		targetDriver:  r.TargetDriver,
		targetDSN:     r.Target,
		org:           r.PlanetScaleOrg,
		database:      r.PlanetScaleDatabase,
		branch:        r.PlanetScaleBranch,
		tokenID:       r.PlanetScaleServiceTokenID,
		token:         r.PlanetScaleServiceToken,
		deployTimeout: r.PlanetScaleDeployTimeout,
	}
}

// TestRestoreIndexFallback_ArmingMatrix pins the restore-side arming
// contract — the same opportunistic never-refuse matrix migrate pins,
// through the shared composer.
func TestRestoreIndexFallback_ArmingMatrix(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*RestoreCmd)
		wantArm bool
		wantDB  string
	}{
		{"fully armed derives database from target DSN", func(*RestoreCmd) {}, true, "shopdb"},
		{"explicit --planetscale-database wins over the DSN", func(r *RestoreCmd) { r.PlanetScaleDatabase = "other" }, true, "other"},
		{"non-planetscale target stays unarmed", func(r *RestoreCmd) { r.TargetDriver = "mysql" }, false, ""},
		{"no org stays unarmed", func(r *RestoreCmd) { r.PlanetScaleOrg = "" }, false, ""},
		{"missing token secret stays unarmed", func(r *RestoreCmd) { r.PlanetScaleServiceToken = "" }, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := armedRestoreCmd()
			tc.mutate(r)
			got := composePlanetScaleIndexFallback(r.testIndexFallbackParams())
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
		})
	}
}

// TestRestoreIndexFallbackFlags_ThroughKong pins the real-parser leg for
// `restore` (the Bug-180 lesson): the new flags and the service-token env
// fallbacks populate the fields the arming reads, alongside the
// pre-existing telemetry flags on the same command.
func TestRestoreIndexFallbackFlags_ThroughKong(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")

	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse([]string{
		"restore",
		"--from-dir=/backups/x",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
		"--planetscale-org=acme",
		"--planetscale-database=explicit-db",
		"--planetscale-branch=prod",
		"--planetscale-deploy-timeout=30m",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	r := &cli.Restore
	if r.PlanetScaleServiceTokenID != "env-id" || r.PlanetScaleServiceToken != "env-secret" {
		t.Errorf("token = %q/%q; want the env fallbacks", r.PlanetScaleServiceTokenID, r.PlanetScaleServiceToken)
	}
	if r.PlanetScaleDatabase != "explicit-db" || r.PlanetScaleBranch != "prod" {
		t.Errorf("database/branch = %q/%q; want explicit-db/prod", r.PlanetScaleDatabase, r.PlanetScaleBranch)
	}
	if r.PlanetScaleDeployTimeout != 30*time.Minute {
		t.Errorf("PlanetScaleDeployTimeout = %v; want 30m", r.PlanetScaleDeployTimeout)
	}
	if fb := composePlanetScaleIndexFallback(r.testIndexFallbackParams()); fb == nil {
		t.Error("kong-parsed, env-armed restore did not arm the fallback")
	}
	// The org flag on restore is DELIBERATELY not env-bound (it is shared
	// with the telemetry opt-in whose all-or-nothing refusal would fire on
	// an ambient PLANETSCALE_ORG); pin that so a future env binding is a
	// conscious decision, not drift.
	if _, err := parser.Parse([]string{
		"restore", "--from-dir=/backups/x",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
	}); err != nil {
		t.Fatalf("parse (no org): %v", err)
	}
	t.Setenv("PLANETSCALE_ORG", "ambient-org")
	cli2 := &CLI{}
	parser2, err := kong.New(cli2, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser2.Parse([]string{
		"restore", "--from-dir=/backups/x",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
	}); err != nil {
		t.Fatalf("parse (ambient org): %v", err)
	}
	if cli2.Restore.PlanetScaleOrg != "" {
		t.Errorf("restore --planetscale-org picked up the ambient env var (%q); it must stay explicit-only", cli2.Restore.PlanetScaleOrg)
	}
}

// TestSyncStartIndexFallbackFlags_ThroughKong pins the real-parser leg
// for `sync start` and the end-to-end arming from the parsed command.
func TestSyncStartIndexFallbackFlags_ThroughKong(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")

	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse([]string{
		"sync", "start",
		"--source-driver=mysql", "--source=src-dsn",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
		"--planetscale-org=acme",
		"--planetscale-branch=prod",
		"--planetscale-deploy-timeout=45m",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := &cli.Sync.Start
	if s.PlanetScaleServiceTokenID != "env-id" || s.PlanetScaleServiceToken != "env-secret" {
		t.Errorf("token = %q/%q; want the env fallbacks", s.PlanetScaleServiceTokenID, s.PlanetScaleServiceToken)
	}
	if s.PlanetScaleDeployTimeout != 45*time.Minute {
		t.Errorf("PlanetScaleDeployTimeout = %v; want 45m", s.PlanetScaleDeployTimeout)
	}
	fb, ok := s.planetScaleIndexFallback().(*expandcontract.IndexFallback)
	if !ok {
		t.Fatalf("sync start did not arm the fallback (got %T)", s.planetScaleIndexFallback())
	}
	if fb.Org != "acme" || fb.Database != "db" || fb.Branch != "prod" {
		t.Errorf("fallback org/db/branch = %s/%s/%s; want acme/db/prod", fb.Org, fb.Database, fb.Branch)
	}

	// Zero-config default through the real parser: unarmed, byte-identical.
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "")
	cli2 := &CLI{}
	parser2, err := kong.New(cli2, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser2.Parse([]string{
		"sync", "start",
		"--source-driver=mysql", "--source=src-dsn",
		"--target-driver=planetscale", "--target=user:pw@tcp(h:3306)/db",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if fb := cli2.Sync.Start.planetScaleIndexFallback(); fb != nil {
		t.Errorf("fallback = %#v; want nil with no org/token configured", fb)
	}
}

// TestTelemetryParamsSharedOrg pins the one-flag-two-consumers
// reconciliation: a fallback-only arming (org + service tokens, no
// metrics token piece) blanks the org for the telemetry builder —
// telemetry off, no refusal — while every telemetry-shaped input is
// byte-identical to before (complete pair runs, PARTIAL pair keeps the
// loud all-or-nothing refusal, unarmed keeps the refusal too).
func TestTelemetryParamsSharedOrg(t *testing.T) {
	ctx := context.Background()
	base := telemetryParams{org: "acme", targetDSN: "dsn", engine: "planetscale"}

	t.Run("fallback-only arming blanks the org (telemetry off, no refusal)", func(t *testing.T) {
		got := telemetryParamsSharedOrg(ctx, base, true)
		if got.org != "" {
			t.Errorf("org = %q; want blanked", got.org)
		}
		if _, err := buildTargetTelemetryProvider(ctx, got); err != nil {
			t.Errorf("telemetry builder refused a fallback-only arming: %v", err)
		}
	})
	t.Run("unarmed keeps the all-or-nothing refusal", func(t *testing.T) {
		got := telemetryParamsSharedOrg(ctx, base, false)
		if got.org != "acme" {
			t.Fatalf("org = %q; want untouched", got.org)
		}
		if _, err := buildTargetTelemetryProvider(ctx, got); err == nil {
			t.Error("org without any token pair must keep the loud telemetry refusal")
		}
	})
	t.Run("partial metrics pair keeps the refusal even when armed", func(t *testing.T) {
		p := base
		p.tokenID = "mtok-id" // token missing — evident telemetry intent, typo'd
		got := telemetryParamsSharedOrg(ctx, p, true)
		if got.org != "acme" {
			t.Fatalf("org = %q; want untouched (partial pair = telemetry intent)", got.org)
		}
		if _, err := buildTargetTelemetryProvider(ctx, got); err == nil {
			t.Error("a partial metrics pair must keep the all-or-nothing refusal")
		}
	})
	t.Run("complete metrics pair is untouched", func(t *testing.T) {
		p := base
		p.tokenID, p.token = "mtok-id", "mtok-secret"
		got := telemetryParamsSharedOrg(ctx, p, true)
		if got.org != "acme" || got.tokenID != "mtok-id" || got.token != "mtok-secret" {
			t.Errorf("params changed for a complete pair: %+v", got)
		}
	})
	t.Run("empty org is untouched", func(t *testing.T) {
		got := telemetryParamsSharedOrg(ctx, telemetryParams{}, true)
		if got.org != "" {
			t.Errorf("org = %q; want empty", got.org)
		}
	})
}
