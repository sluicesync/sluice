// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline"
)

// TestLoadFleetConfig_ParsesTelemetryKeys pins that the koanf loader decodes
// the ADR-0126 per-sync PlanetScale telemetry keys (planetscale-* +
// suppress-target-metrics-history + the PS-gated notify-* thresholds).
func TestLoadFleetConfig_ParsesTelemetryKeys(t *testing.T) {
	path := writeFleetYAML(t, `
syncs:
  - stream-id: orders
    source-driver: mysql
    source: mysql://u:p@src:3306/app
    target-driver: postgres
    target: postgres://u:p@dst:5432/app
    planetscale-org: acme
    planetscale-metrics-token-id: tid
    planetscale-metrics-token: tok
    planetscale-metrics-branch: prod
    planetscale-metrics-db: orders_db
    suppress-target-metrics-history: true
    notify-storage-util: 0.85
    notify-cpu-util: 0.9
    notify-mem-util: 0.8
    notify-lag-seconds: 30
    notify-storage-growth-per-min: 0.02
    notify-dead-tuple-ratio: 0.3
    notify-xid-age: 1000000000
    publication-name: wave1
`)
	fleet, err := loadFleetConfig(path)
	if err != nil {
		t.Fatalf("loadFleetConfig: %v", err)
	}
	s := fleet.Syncs[0]
	if s.PlanetScaleOrg != "acme" || s.PlanetScaleMetricsTokenID != "tid" || s.PlanetScaleMetricsToken != "tok" {
		t.Errorf("PS org/token mismatch: %+v", s)
	}
	if s.PlanetScaleMetricsBranch != "prod" || s.PlanetScaleMetricsDB != "orders_db" {
		t.Errorf("PS branch/db mismatch: %+v", s)
	}
	if !s.SuppressTargetMetricsHistory {
		t.Error("SuppressTargetMetricsHistory = false; want true")
	}
	if s.NotifyStorageUtil != 0.85 || s.NotifyCPUUtil != 0.9 || s.NotifyMemUtil != 0.8 {
		t.Errorf("util thresholds mismatch: %+v", s)
	}
	if s.NotifyLagSeconds != 30 || s.NotifyStorageGrowthPerMin != 0.02 {
		t.Errorf("lag/growth thresholds mismatch: %+v", s)
	}
	if s.NotifyDeadTupleRatio != 0.3 || s.NotifyXIDAge != 1_000_000_000 {
		t.Errorf("vacuum thresholds mismatch: %+v", s)
	}
	// Audit 2026-07-23 DEVEX-1: the ADR-0175 refusal's primary escape must
	// be expressible on the fleet surface.
	if s.PublicationName != "wave1" {
		t.Errorf("PublicationName = %q; want wave1", s.PublicationName)
	}
}

// telemetrySpec is a minimal telemetry-on spec (MySQL→PG so no slot guard).
func telemetrySpec(id, org string) SyncSpec {
	s := mysqlSpec(id)
	s.PlanetScaleOrg = org
	s.PlanetScaleMetricsDB = "db" // explicit so build never needs a DSN derive
	return s
}

// TestFleetValidate_Telemetry pins the ADR-0126 §3 all-or-nothing contract:
// planetscale-org without a resolvable token is refused (spec path AND env-
// fallback path), the refusal names the stream-id, and the env-supplied token
// or telemetry-off pass. The env vars are set/unset within the test so the
// fallback branch is exercised deterministically.
func TestFleetValidate_Telemetry(t *testing.T) {
	// Ensure a clean env baseline; restore on exit.
	t.Setenv("PLANETSCALE_METRICS_TOKEN_ID", "")
	t.Setenv("PLANETSCALE_METRICS_TOKEN", "")

	t.Run("org without token (spec or env) → refused, names stream-id + field", func(t *testing.T) {
		err := fleetFromSpecs(telemetrySpec("orders", "acme")).validate()
		if err == nil {
			t.Fatal("org without a token must be refused")
		}
		for _, sub := range []string{"orders", "planetscale-metrics-token"} {
			if !strings.Contains(err.Error(), sub) {
				t.Errorf("error %q missing substring %q", err.Error(), sub)
			}
		}
	})

	t.Run("org with spec token → ok", func(t *testing.T) {
		s := telemetrySpec("orders", "acme")
		s.PlanetScaleMetricsTokenID = "tid"
		s.PlanetScaleMetricsToken = "tok"
		if err := fleetFromSpecs(s).validate(); err != nil {
			t.Fatalf("complete spec token should pass: %v", err)
		}
	})

	t.Run("org with token from env → ok (env-fallback path)", func(t *testing.T) {
		t.Setenv("PLANETSCALE_METRICS_TOKEN_ID", "envid")
		t.Setenv("PLANETSCALE_METRICS_TOKEN", "envtok")
		if err := fleetFromSpecs(telemetrySpec("orders", "acme")).validate(); err != nil {
			t.Fatalf("env-resolved token should pass: %v", err)
		}
	})

	t.Run("org with only env token-id (token still missing) → refused", func(t *testing.T) {
		t.Setenv("PLANETSCALE_METRICS_TOKEN_ID", "envid")
		t.Setenv("PLANETSCALE_METRICS_TOKEN", "")
		err := fleetFromSpecs(telemetrySpec("orders", "acme")).validate()
		if err == nil || !strings.Contains(err.Error(), "planetscale-metrics-token") {
			t.Fatalf("half-resolved token must be refused naming the field; got %v", err)
		}
	})

	t.Run("telemetry off → ok", func(t *testing.T) {
		if err := fleetFromSpecs(mysqlSpec("orders")).validate(); err != nil {
			t.Fatalf("telemetry-off sync should pass: %v", err)
		}
	})
}

// TestBuildSupervisedFleet_TelemetryAttach pins that buildSupervisedFleet
// attaches a non-nil TargetTelemetry for a telemetry-on sync (token via env)
// and leaves it a TRUE nil interface for a telemetry-off sync, and that the
// returned closer closes cleanly. The provider constructs offline (its poll
// loop fails against the bogus org but never errors construction), so no
// network is needed.
func TestBuildSupervisedFleet_TelemetryAttach(t *testing.T) {
	t.Setenv("PLANETSCALE_METRICS_TOKEN_ID", "envid")
	t.Setenv("PLANETSCALE_METRICS_TOKEN", "envtok")

	fleet := fleetFromSpecs(
		telemetrySpec("on", "acme"), // telemetry on, token from env
		mysqlSpec("off"),            // telemetry off
	)
	// Distinct slots are not a concern here (MySQL sources, no slot).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	supervised, closeTelemetry, err := buildSupervisedFleet(ctx, fleet, testFleetGlobals())
	if err != nil {
		t.Fatalf("buildSupervisedFleet: %v", err)
	}
	defer func() {
		if err := closeTelemetry(); err != nil {
			t.Errorf("closeTelemetry returned error: %v", err)
		}
	}()
	if len(supervised) != 2 {
		t.Fatalf("got %d supervised; want 2", len(supervised))
	}

	onStreamer := mustStreamer(t, supervised[0])
	if onStreamer.TargetTelemetry == nil {
		t.Error("telemetry-on sync: TargetTelemetry is nil; want a wired provider")
	}
	offStreamer := mustStreamer(t, supervised[1])
	// The exact typed-nil guard: a telemetry-off sync must be a TRUE nil
	// interface, not a (*Provider)(nil) typed-nil that the streamer's
	// `TargetTelemetry != nil` checks would wrongly fire on.
	if offStreamer.TargetTelemetry != nil {
		t.Error("telemetry-off sync: TargetTelemetry must be a true nil interface")
	}
}

// mustStreamer extracts the *pipeline.Streamer from a SupervisedSync's Runner.
func mustStreamer(t *testing.T, ss pipeline.SupervisedSync) *pipeline.Streamer {
	t.Helper()
	s, ok := ss.Runner.(*pipeline.Streamer)
	if !ok {
		t.Fatalf("Runner for %q is %T; want *pipeline.Streamer", ss.ID, ss.Runner)
	}
	return s
}

// TestPrintFleetPlan_TelemetryLabel pins the ADR-0126 §5 dry-run plan line:
// telemetry=org/db@branch for an enabled sync (never the token) and
// telemetry=off for a disabled one.
func TestPrintFleetPlan_TelemetryLabel(t *testing.T) {
	on := telemetrySpec("on", "acme")
	on.PlanetScaleMetricsDB = "orders_db"
	on.PlanetScaleMetricsBranch = "prod"
	on.PlanetScaleMetricsTokenID = "supersecret-id"
	on.PlanetScaleMetricsToken = "supersecret-token"

	fleet := fleetFromSpecs(on, mysqlSpec("off"))

	f, err := os.CreateTemp(t.TempDir(), "plan-*.txt")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := printFleetPlan(f, fleet); err != nil {
		t.Fatalf("printFleetPlan: %v", err)
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	out := string(data)
	if !strings.Contains(out, "telemetry=acme/orders_db@prod") {
		t.Errorf("plan missing enabled telemetry label:\n%s", out)
	}
	if !strings.Contains(out, "telemetry=off") {
		t.Errorf("plan missing telemetry=off for the disabled sync:\n%s", out)
	}
	// The token must NEVER appear in the plan.
	if bytes.Contains(data, []byte("supersecret")) {
		t.Errorf("plan leaked the telemetry token:\n%s", out)
	}
}

// TestTelemetryPlanLabel pins the label helper directly across the branches.
func TestTelemetryPlanLabel(t *testing.T) {
	off := mysqlSpec("x")
	if got := telemetryPlanLabel(&off); got != "off" {
		t.Errorf("telemetryPlanLabel(off) = %q; want off", got)
	}

	explicit := telemetrySpec("x", "acme")
	explicit.PlanetScaleMetricsDB = "db1"
	explicit.PlanetScaleMetricsBranch = "b1"
	if got := telemetryPlanLabel(&explicit); got != "acme/db1@b1" {
		t.Errorf("telemetryPlanLabel(explicit) = %q; want acme/db1@b1", got)
	}

	// DB derived from the target DSN (mysqlSpec targets a postgres DSN ending
	// in /app), branch defaulting to main.
	derived := mysqlSpec("x")
	derived.PlanetScaleOrg = "acme"
	if got := telemetryPlanLabel(&derived); got != "acme/app@main" {
		t.Errorf("telemetryPlanLabel(derived) = %q; want acme/app@main", got)
	}
}
