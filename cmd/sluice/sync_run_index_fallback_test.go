// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the audit-MED-A1 gap-#12 fleet threading of the ADR-0148
// deploy-request index-build fallback: a fleet-managed sync composes the
// fallback from its per-sync spec (org + service-token pair, env-first) and
// buildStreamerFromSpec hands it to the built Streamer, so a fleet cold
// start onto a walled PlanetScale target auto-recovers a deferred index
// build the same way `sync start` does. The shared `planetscale-org` key is
// reconciled with the ADR-0126 telemetry opt-in: a fallback-only arming
// (org + service pair, no metrics piece) does NOT trip the telemetry
// all-or-nothing refusal at config-load, and turns telemetry off at build.

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/expandcontract"
)

// fallbackSpec is a planetscale-target sync armed for the ADR-0148 index-build
// fallback. The explicit control-keyspace short-circuits the sharded-target
// auto-detect so buildStreamerFromSpec needs no live connection.
func fallbackSpec(id string) SyncSpec {
	return SyncSpec{
		StreamID:        id,
		SourceDriver:    "mysql",
		Source:          "mysql://u:p@src:3306/app",
		TargetDriver:    "planetscale",
		Target:          "user:pw@tcp(host.psdb.cloud:3306)/shopdb?tls=true",
		ControlKeyspace: "sluice_ctrl",
		PlanetScaleOrg:  "acme",
	}
}

// TestSyncSpec_ResolveIndexFallback_ArmingMatrix pins the fleet-side arming
// contract through the shared composer — the same opportunistic never-refuse
// matrix migrate/restore/broker pin, with the service token resolved env-first.
func TestSyncSpec_ResolveIndexFallback_ArmingMatrix(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")

	cases := []struct {
		name    string
		mutate  func(*SyncSpec)
		wantArm bool
		wantDB  string
	}{
		{"fully armed derives database from target DSN", func(*SyncSpec) {}, true, "shopdb"},
		{"explicit planetscale-database wins over the DSN", func(s *SyncSpec) { s.PlanetScaleDatabase = "other" }, true, "other"},
		{"non-planetscale target stays unarmed", func(s *SyncSpec) { s.TargetDriver = "mysql" }, false, ""},
		{"no org stays unarmed", func(s *SyncSpec) { s.PlanetScaleOrg = "" }, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := fallbackSpec("orders")
			tc.mutate(&s)
			got := s.resolveIndexFallback()
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
			if fb.Org != "acme" || fb.Database != tc.wantDB {
				t.Errorf("fallback org/db = %s/%s; want acme/%s", fb.Org, fb.Database, tc.wantDB)
			}
		})
	}

	// Service token absent (env cleared) stays unarmed — byte-identical.
	t.Run("missing service token stays unarmed", func(t *testing.T) {
		t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "")
		t.Setenv("PLANETSCALE_SERVICE_TOKEN", "")
		s := fallbackSpec("orders")
		if got := s.resolveIndexFallback(); got != nil {
			t.Fatalf("fallback armed = %#v; want nil with no service token", got)
		}
	})
}

// TestBuildStreamerFromSpec_ThreadsIndexBuildFallback pins the seam the
// coordinator asked for: buildStreamerFromSpec populates
// Streamer.IndexBuildFallback for an armed fleet sync, and leaves it nil for
// an unarmed one (the byte-identical default every fleet sync had before).
func TestBuildStreamerFromSpec_ThreadsIndexBuildFallback(t *testing.T) {
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")
	ctx := context.Background()

	t.Run("armed sync threads a non-nil fallback", func(t *testing.T) {
		spec := fallbackSpec("orders")
		streamer, err := buildStreamerFromSpec(ctx, &spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec: %v", err)
		}
		if streamer.IndexBuildFallback == nil {
			t.Error("armed fleet sync: Streamer.IndexBuildFallback is nil; want the composed fallback")
		}
	})

	t.Run("unarmed sync leaves the fallback nil", func(t *testing.T) {
		spec := mysqlSpec("plain")
		streamer, err := buildStreamerFromSpec(ctx, &spec, testFleetGlobals())
		if err != nil {
			t.Fatalf("buildStreamerFromSpec: %v", err)
		}
		if streamer.IndexBuildFallback != nil {
			t.Errorf("unarmed fleet sync: Streamer.IndexBuildFallback = %#v; want nil", streamer.IndexBuildFallback)
		}
	})
}

// TestFleetValidate_FallbackOnlyArmingNotRefused pins the config-load
// reconciliation: a fallback-only arming (org + service pair via env, no
// metrics token piece) must PASS validate() — the ADR-0126 telemetry
// all-or-nothing refusal is for a metrics-token intent, not a fallback one.
// The typo-catch is preserved: org with NO token pair of either kind still
// refuses.
func TestFleetValidate_FallbackOnlyArmingNotRefused(t *testing.T) {
	t.Setenv("PLANETSCALE_METRICS_TOKEN_ID", "")
	t.Setenv("PLANETSCALE_METRICS_TOKEN", "")

	t.Run("org + service pair (no metrics) → ok, telemetry off", func(t *testing.T) {
		t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
		t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")
		if err := fleetFromSpecs(fallbackSpec("orders")).validate(); err != nil {
			t.Fatalf("fallback-only arming must not be refused: %v", err)
		}
	})

	t.Run("org with no token pair of either kind → still refused (typo-catch)", func(t *testing.T) {
		t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "")
		t.Setenv("PLANETSCALE_SERVICE_TOKEN", "")
		err := fleetFromSpecs(fallbackSpec("orders")).validate()
		if err == nil || !strings.Contains(err.Error(), "planetscale-metrics-token") {
			t.Fatalf("org with no token pair must keep the loud telemetry refusal; got %v", err)
		}
	})
}

// TestBuildSupervisedFleet_FallbackOnlyArming_TelemetryOff pins the build-time
// half: a fallback-only arming attaches the index fallback to the Streamer but
// leaves TargetTelemetry a TRUE nil interface (telemetry blanked with a WARN,
// not run) — the fleet mirror of telemetryParamsSharedOrg.
func TestBuildSupervisedFleet_FallbackOnlyArming_TelemetryOff(t *testing.T) {
	t.Setenv("PLANETSCALE_METRICS_TOKEN_ID", "")
	t.Setenv("PLANETSCALE_METRICS_TOKEN", "")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN_ID", "env-id")
	t.Setenv("PLANETSCALE_SERVICE_TOKEN", "env-secret")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	supervised, closeTelemetry, err := buildSupervisedFleet(ctx, fleetFromSpecs(fallbackSpec("orders")), testFleetGlobals())
	if err != nil {
		t.Fatalf("buildSupervisedFleet: %v", err)
	}
	defer func() { _ = closeTelemetry() }()

	if len(supervised) != 1 {
		t.Fatalf("got %d supervised; want 1", len(supervised))
	}
	streamer := mustStreamer(t, supervised[0])
	if streamer.IndexBuildFallback == nil {
		t.Error("fallback-only arming: Streamer.IndexBuildFallback is nil; want the composed fallback")
	}
	// Telemetry must be a TRUE nil interface (blanked), not a typed-nil provider.
	if streamer.TargetTelemetry != nil {
		t.Error("fallback-only arming: TargetTelemetry must stay off (a true nil interface)")
	}
}

// TestLoadFleetConfig_ParsesIndexFallbackKeys pins that the koanf loader
// decodes the new ADR-0148 fallback keys into the typed SyncSpec (the
// ErrorUnused loader would otherwise reject them as unknown keys).
func TestLoadFleetConfig_ParsesIndexFallbackKeys(t *testing.T) {
	path := writeFleetYAML(t, `
syncs:
  - stream-id: orders
    source-driver: mysql
    source: mysql://u:p@src:3306/app
    target-driver: planetscale
    target: user:pw@tcp(h:3306)/shopdb
    planetscale-org: acme
    planetscale-database: shopdb
    planetscale-branch: prod
    planetscale-deploy-timeout: 45m
`)
	fleet, err := loadFleetConfig(path)
	if err != nil {
		t.Fatalf("loadFleetConfig: %v", err)
	}
	s := fleet.Syncs[0]
	if s.PlanetScaleDatabase != "shopdb" || s.PlanetScaleBranch != "prod" {
		t.Errorf("database/branch = %q/%q; want shopdb/prod", s.PlanetScaleDatabase, s.PlanetScaleBranch)
	}
	if s.PlanetScaleDeployTimeout != 45*time.Minute {
		t.Errorf("PlanetScaleDeployTimeout = %v; want 45m", s.PlanetScaleDeployTimeout)
	}
}
