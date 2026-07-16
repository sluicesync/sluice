// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// TestBuildTargetTelemetry_QuietSuppressesEnableLog pins the ADR-0156 panel-leak
// fix: the "telemetry enabled" INFO line prints on the default (quiet=false)
// path but is SUPPRESSED when quiet=true (the live-panel commands set it so the
// line does not leak above the panel, which installs its slog gate afterward).
// The provider itself is constructed either way — quiet gates only the log line.
func TestBuildTargetTelemetry_QuietSuppressesEnableLog(t *testing.T) {
	base := telemetryParams{
		org:       "o",
		tokenID:   "i",
		token:     "s",
		metricsDB: "explicit-db",
		engine:    "planetscale",
	}
	for _, tc := range []struct {
		name      string
		quiet     bool
		wantEmbed bool
	}{
		{"loud emits enable INFO", false, true},
		{"quiet suppresses enable INFO", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
			defer slog.SetDefault(prev)

			p := base
			p.quiet = tc.quiet
			provider, err := buildTargetTelemetryProvider(context.Background(), p)
			if err != nil {
				t.Fatalf("complete opt-in should construct regardless of quiet: %v", err)
			}
			if provider == nil {
				t.Fatal("complete opt-in should yield a provider regardless of quiet")
			}
			defer func() { _ = provider.Close() }()

			got := strings.Contains(buf.String(), "target-health telemetry enabled")
			if got != tc.wantEmbed {
				t.Errorf("enable-INFO present=%v, want %v (quiet=%v); log=%q", got, tc.wantEmbed, tc.quiet, buf.String())
			}
		})
	}
}

// TestBuildTargetTelemetry_OffWhenNoOrg pins the default-off contract: no
// --planetscale-org ⇒ (nil, nil), the byte-identical pre-ADR-0107 path.
func TestBuildTargetTelemetry_OffWhenNoOrg(t *testing.T) {
	p, err := buildTargetTelemetry(context.Background(), &SyncStartCmd{}, false, false)
	if err != nil {
		t.Fatalf("no-org should be a no-op, got err: %v", err)
	}
	if p != nil {
		t.Fatal("no-org should yield a nil provider")
		return
	}
	// And the interface-conversion stays a TRUE nil (no typed-nil trap).
	if iface := telemetryProviderOrNil(p); iface != nil {
		t.Error("telemetryProviderOrNil(nil) must be a true nil interface")
	}
}

// TestBuildTargetTelemetry_RefusesIncompleteCreds pins the all-or-nothing
// loud refusal: --planetscale-org set but token incomplete ⇒ error.
func TestBuildTargetTelemetry_RefusesIncompleteCreds(t *testing.T) {
	cases := []struct {
		name string
		cmd  SyncStartCmd
	}{
		{"org no token at all", SyncStartCmd{PlanetScaleOrg: "o", Target: "u@tcp(h:3306)/db"}},
		{"org token id only", SyncStartCmd{PlanetScaleOrg: "o", PlanetScaleMetricsTokenID: "i", Target: "u@tcp(h:3306)/db"}},
		{"org token only", SyncStartCmd{PlanetScaleOrg: "o", PlanetScaleMetricsToken: "s", Target: "u@tcp(h:3306)/db"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := buildTargetTelemetry(context.Background(), &tc.cmd, false, false)
			if err == nil {
				t.Error("incomplete token should refuse loudly; got nil error")
			}
			if p != nil {
				_ = p.Close()
				t.Error("incomplete token should not construct a provider")
			}
		})
	}
}

// TestBuildTargetTelemetry_RefusesUndeterminableDB pins that a complete
// opt-in with no derivable database (and no explicit --planetscale-metrics-
// database) refuses loudly rather than silently watching nothing.
func TestBuildTargetTelemetry_RefusesUndeterminableDB(t *testing.T) {
	cmd := &SyncStartCmd{
		PlanetScaleOrg:            "o",
		PlanetScaleMetricsTokenID: "i",
		PlanetScaleMetricsToken:   "s",
		Target:                    "postgres://host", // no path segment
	}
	p, err := buildTargetTelemetry(context.Background(), cmd, false, false)
	if err == nil {
		t.Error("undeterminable database should refuse loudly; got nil error")
	}
	if p != nil {
		_ = p.Close()
		t.Error("undeterminable database should not construct a provider")
	}
}

// TestBuildTargetTelemetry_ConstructsWithCompleteOptIn pins the happy path:
// a complete opt-in constructs a provider (its poll loop will fail against
// the bogus org, which is fine — Sample degrades to ok=false, never errors).
func TestBuildTargetTelemetry_ConstructsWithCompleteOptIn(t *testing.T) {
	cmd := &SyncStartCmd{
		PlanetScaleOrg:            "o",
		PlanetScaleMetricsTokenID: "i",
		PlanetScaleMetricsToken:   "s",
		PlanetScaleMetricsDB:      "explicit-db",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := buildTargetTelemetry(ctx, cmd, false, false)
	if err != nil {
		t.Fatalf("complete opt-in should construct: %v", err)
	}
	if p == nil {
		t.Fatal("complete opt-in should yield a provider")
	}
	defer func() { _ = p.Close() }()
	// Interface conversion is non-nil for a real provider.
	if iface := telemetryProviderOrNil(p); iface == nil {
		t.Error("telemetryProviderOrNil(provider) must be non-nil")
	}
}

func TestDatabaseFromDSN(t *testing.T) {
	cases := map[string]string{
		"user:pass@tcp(host:3306)/mydb":                       "mydb",
		"user:pass@tcp(host:3306)/mydb?tls=true&parseTime=1":  "mydb",
		"postgres://user:pass@host:5432/pgdb":                 "pgdb",
		"postgres://user:pass@host:5432/pgdb?sslmode=require": "pgdb",
		"mysql://user@host/branchdb":                          "branchdb",
		// No database segment → "".
		"postgres://host":           "",
		"user:pass@tcp(host:3306)/": "",
		"":                          "",
	}
	for dsn, want := range cases {
		if got := databaseFromDSN(dsn); got != want {
			t.Errorf("databaseFromDSN(%q) = %q, want %q", dsn, got, want)
		}
	}
}
