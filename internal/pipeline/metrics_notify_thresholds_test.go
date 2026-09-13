// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The PROG-NOTIFY-1 pins (audit 2026-08-11): the utilisation thresholds
// and the storage-growth rate are FRACTIONS (0–1), and an out-of-range
// value used to arm a rule that could never fire — `--notify-storage-util
// 85` (an operator meaning 85%) was a silently inert alert, the exact
// quiet failure an alert threshold exists to prevent. The validator
// refuses loudly at every entry (sync start, metrics-watch, the fleet
// watcher — the fleet dispatch reuses RunMetricsWatch's door).
func TestValidateMetricsNotifyThresholds(t *testing.T) {
	ok := func(name string, th metricsNotifyThresholds) {
		t.Helper()
		if err := validateMetricsNotifyThresholds(th); err != nil {
			t.Errorf("%s: refused a valid config: %v", name, err)
		}
	}
	ok("all disabled (the default)", metricsNotifyThresholds{})
	ok("in-range fractions + seconds", metricsNotifyThresholds{
		StorageUtil: 0.85, CPUUtil: 0.9, MemUtil: 0.95, LagSeconds: 300,
		StorageGrowthPerMin: 0.02, RouterCPUUtil: 0.8,
	})
	ok("boundary 1.0 is a legal fraction", metricsNotifyThresholds{
		StorageUtil: 1, CPUUtil: 1, MemUtil: 1, StorageGrowthPerMin: 1, RouterCPUUtil: 1,
	})

	bad := []struct {
		name string
		th   metricsNotifyThresholds
		want string
	}{
		{"storage-util as a percentage", metricsNotifyThresholds{StorageUtil: 85}, "0.85"},
		{"cpu-util above 1", metricsNotifyThresholds{CPUUtil: 1.5}, "--notify-cpu-util"},
		{"mem-util above 1", metricsNotifyThresholds{MemUtil: 2}, "--notify-mem-util"},
		{"growth rate above 1", metricsNotifyThresholds{StorageGrowthPerMin: 2}, "--notify-storage-growth-per-min"},
		// The routing-layer threshold is a fraction like the rest, and it is
		// listed here because a NEW threshold that skipped the validator is
		// exactly the silently-inert alert this door exists to stop.
		{"router-cpu-util as a percentage", metricsNotifyThresholds{RouterCPUUtil: 90}, "--notify-router-cpu-util"},
		{"router-cpu-util negative", metricsNotifyThresholds{RouterCPUUtil: -1}, "--notify-router-cpu-util"},
		{"router-cpu-util NaN", metricsNotifyThresholds{RouterCPUUtil: math.NaN()}, "--notify-router-cpu-util"},
		{"negative fraction", metricsNotifyThresholds{StorageUtil: -0.5}, "--notify-storage-util"},
		{"NaN fraction", metricsNotifyThresholds{StorageUtil: math.NaN()}, "--notify-storage-util"},
		{"negative lag seconds", metricsNotifyThresholds{LagSeconds: -1}, "--notify-lag-seconds"},
		{"NaN lag seconds", metricsNotifyThresholds{LagSeconds: math.NaN()}, "--notify-lag-seconds"},
	}
	for _, tc := range bad {
		err := validateMetricsNotifyThresholds(tc.th)
		if err == nil {
			t.Errorf("%s: accepted — the rule would be armed and silently inert", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: refusal %q does not carry %q", tc.name, err, tc.want)
		}
	}

	// The percentage hint is the operator-facing half: the refusal must
	// name the corrected value, not just reject.
	err := validateMetricsNotifyThresholds(metricsNotifyThresholds{StorageUtil: 85})
	if err == nil || !strings.Contains(err.Error(), "if you meant 85%, pass 0.85") {
		t.Errorf("the 85→0.85 hint is missing: %v", err)
	}
}

// TestRunMetricsWatch_RefusesOutOfRangeThreshold pins the second door:
// the standalone watcher (and, via its fleet dispatch, the fleet
// watcher) refuses before touching the provider — a nil provider after
// a threshold error proves the order.
func TestRunMetricsWatch_RefusesOutOfRangeThreshold(t *testing.T) {
	err := RunMetricsWatch(t.Context(), nil, MetricsWatchConfig{StorageUtil: 85})
	if err == nil || !strings.Contains(err.Error(), "0.85") {
		t.Fatalf("RunMetricsWatch accepted --notify-storage-util 85 (or lost the hint): %v", err)
	}
}

// cdcCapableStubEngine gets a Streamer past validate()'s CDC gate so
// the threshold arm below it is reachable.
type cdcCapableStubEngine struct{ stubEngine }

func (cdcCapableStubEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{CDC: ir.CDCBinlog}
}

// TestStreamerValidate_RefusesOutOfRangeNotifyThreshold is the wiring
// pin: the refusal fires from `sync start`'s own validate, before any
// connection — not silently at every alert tick.
func TestStreamerValidate_RefusesOutOfRangeNotifyThreshold(t *testing.T) {
	s := &Streamer{
		Source:            cdcCapableStubEngine{},
		Target:            stubEngine{},
		SourceDSN:         "src-dsn",
		TargetDSN:         "dst-dsn",
		NotifyStorageUtil: 85,
	}
	err := s.validate()
	if err == nil {
		t.Fatal("Streamer.validate() accepted --notify-storage-util 85 — the alert would be silently inert")
	}
	if !strings.Contains(err.Error(), "0.85") {
		t.Fatalf("refusal does not carry the corrected-value hint: %v", err)
	}
}
