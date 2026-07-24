// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

// parseMetricsWatch runs the real kong parser over `metrics-watch` args, so
// these pins go THROUGH the CLI layer rather than around it (the Bug-180
// lesson: a kong default/required tag can make a branch unreachable while a
// direct-call test greens it).
func parseMetricsWatch(t *testing.T, args ...string) *MetricsWatchCmd {
	t.Helper()
	var cli struct {
		MetricsWatch MetricsWatchCmd `cmd:""`
	}
	parser, err := kong.New(&cli)
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse(append([]string{"metrics-watch"}, args...)); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return &cli.MetricsWatch
}

// TestMetricsWatch_MetricsDBIsOptional pins the item-75b flag relaxation:
// --planetscale-metrics-db is no longer REQUIRED, and omitting it is what
// selects org-wide mode. If the required tag ever came back, the org-wide
// path would be unreachable from the CLI no matter how correct the loop is.
func TestMetricsWatch_MetricsDBIsOptional(t *testing.T) {
	m := parseMetricsWatch(t, "--engine", "planetscale", "--planetscale-org", "acme")
	if m.PlanetScaleMetricsDB != "" {
		t.Fatalf("expected an empty metrics-db, got %q", m.PlanetScaleMetricsDB)
	}

	// And supplying it still works — the single-database mode is unchanged.
	single := parseMetricsWatch(t, "--engine", "planetscale", "--planetscale-org", "acme", "--planetscale-metrics-db", "app")
	if single.PlanetScaleMetricsDB != "app" {
		t.Fatalf("metrics-db = %q, want app", single.PlanetScaleMetricsDB)
	}
}

// TestMetricsWatch_FleetFlagsParse pins the org-wide scoping flags, including
// their repeatability.
func TestMetricsWatch_FleetFlagsParse(t *testing.T) {
	m := parseMetricsWatch(
		t,
		"--engine", "planetscale", "--planetscale-org", "acme",
		"--include-database", "shop-*", "--include-database", "blog",
		"--exclude-database", "shop-test",
		"--fleet-concurrency", "8",
	)
	if len(m.IncludeDatabase) != 2 || m.IncludeDatabase[0] != "shop-*" || m.IncludeDatabase[1] != "blog" {
		t.Errorf("include-database = %v", m.IncludeDatabase)
	}
	if len(m.ExcludeDatabase) != 1 || m.ExcludeDatabase[0] != "shop-test" {
		t.Errorf("exclude-database = %v", m.ExcludeDatabase)
	}
	if m.FleetConcurrency != 8 {
		t.Errorf("fleet-concurrency = %d, want 8", m.FleetConcurrency)
	}
}

// TestMetricsWatch_SinkFlagsDefaultToOff pins the zero-value posture: with no
// --sink-* flag the sink is a TRUE nil interface, so the pipeline's
// `Sink != nil` guard is exact and the watch behaves byte-identically to
// before item 75c.
func TestMetricsWatch_SinkFlagsDefaultToOff(t *testing.T) {
	m := parseMetricsWatch(t, "--engine", "planetscale", "--planetscale-org", "acme")
	sink, err := m.buildSampleSink()
	if err != nil {
		t.Fatalf("buildSampleSink: %v", err)
	}
	if sink != nil {
		t.Fatalf("no --sink-* flag must yield a TRUE nil sink, got %#v", sink)
	}
	// Rotation knobs default to 0 ⇒ the package defaults; nothing here may
	// require an explicit value to behave sanely.
	if m.SinkFileMaxBytes != 0 || m.SinkFileMaxFiles != 0 {
		t.Errorf("rotation knobs should default to 0 (package defaults), got %d/%d", m.SinkFileMaxBytes, m.SinkFileMaxFiles)
	}
}

// TestMetricsWatch_SinkFileFlagBuildsTheSink pins the file-sink construction
// through the CLI layer, including that a bad path refuses LOUDLY at startup.
func TestMetricsWatch_SinkFileFlagBuildsTheSink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	m := parseMetricsWatch(
		t,
		"--engine", "planetscale", "--planetscale-org", "acme",
		"--sink-file", path, "--sink-file-max-bytes", "4096", "--sink-file-max-files", "2",
	)
	if m.SinkFileMaxBytes != 4096 || m.SinkFileMaxFiles != 2 {
		t.Fatalf("rotation flags = %d/%d", m.SinkFileMaxBytes, m.SinkFileMaxFiles)
	}
	sink, err := m.buildSampleSink()
	if err != nil {
		t.Fatalf("buildSampleSink: %v", err)
	}
	if sink == nil {
		t.Fatal("--sink-file must produce a sink")
	}
	defer func() { _ = sink.Close() }()

	bad := parseMetricsWatch(t, "--engine", "planetscale", "--planetscale-org", "acme",
		"--sink-file", filepath.Join(path, "under-a-file.jsonl"))
	if _, err := bad.buildSampleSink(); err == nil {
		t.Error("an unusable --sink-file path must refuse at startup, not silently record nothing")
	}
}

// TestMetricsWatch_SinkHTTPFlagBuildsTheSink pins the push sink's flag +
// env binding (the URL is a credential, so the env var is the documented
// way in).
func TestMetricsWatch_SinkHTTPFlagBuildsTheSink(t *testing.T) {
	m := parseMetricsWatch(t, "--engine", "planetscale", "--planetscale-org", "acme",
		"--sink-http", "https://example.test/ingest")
	sink, err := m.buildSampleSink()
	if err != nil {
		t.Fatalf("buildSampleSink: %v", err)
	}
	if sink == nil {
		t.Fatal("--sink-http must produce a sink")
	}
	if got := sink.Name(); got != "http" && got != "multi" {
		t.Errorf("unexpected sink name %q", got)
	}

	t.Setenv("SLUICE_METRICS_SINK_HTTP", "https://example.test/from-env")
	fromEnv := parseMetricsWatch(t, "--engine", "planetscale", "--planetscale-org", "acme")
	if fromEnv.SinkHTTP != "https://example.test/from-env" {
		t.Errorf("--sink-http must bind SLUICE_METRICS_SINK_HTTP, got %q", fromEnv.SinkHTTP)
	}
}

// TestBuildFleetTelemetryProvider_AllOrNothing pins the opt-in refusal on the
// org-wide path, matching the single-database builder's contract.
func TestBuildFleetTelemetryProvider_AllOrNothing(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name   string
		params fleetTelemetryParams
	}{
		{"no org", fleetTelemetryParams{tokenID: "a", token: "b"}},
		{"org without token id", fleetTelemetryParams{org: "acme", token: "b"}},
		{"org without token", fleetTelemetryParams{org: "acme", tokenID: "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := buildFleetTelemetryProvider(ctx, tc.params); err == nil {
				t.Error("an incomplete telemetry opt-in must be a loud refusal")
			}
		})
	}

	// A malformed filter glob is refused too — before any polling starts.
	_, err := buildFleetTelemetryProvider(ctx, fleetTelemetryParams{
		org: "acme", tokenID: "a", token: "b", include: []string{"["},
	})
	if err == nil {
		t.Error("a malformed --include-database pattern must be refused")
	} else if !strings.Contains(err.Error(), "include-database") {
		t.Errorf("refusal should name the flag: %v", err)
	}
}

// TestFleetProviderOrNil_TrueNilInterface pins the typed-nil trap guard: a
// nil *Fleet must convert to a TRUE nil interface so the pipeline's
// `Fleet != nil` mode switch cannot misfire into a nil-deref.
func TestFleetProviderOrNil_TrueNilInterface(t *testing.T) {
	if got := fleetProviderOrNil(nil); got != nil {
		t.Fatalf("nil *Fleet must yield a true nil interface, got %#v", got)
	}
}
