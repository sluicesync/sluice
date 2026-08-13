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
	ok := func(name string, su, cu, mu, lag, growth float64) {
		t.Helper()
		if err := validateMetricsNotifyThresholds(su, cu, mu, lag, growth); err != nil {
			t.Errorf("%s: refused a valid config: %v", name, err)
		}
	}
	ok("all disabled (the default)", 0, 0, 0, 0, 0)
	ok("in-range fractions + seconds", 0.85, 0.9, 0.95, 300, 0.02)
	ok("boundary 1.0 is a legal fraction", 1, 1, 1, 0, 1)

	bad := []struct {
		name                    string
		su, cu, mu, lag, growth float64
		want                    string
	}{
		{"storage-util as a percentage", 85, 0, 0, 0, 0, "0.85"},
		{"cpu-util above 1", 0, 1.5, 0, 0, 0, "--notify-cpu-util"},
		{"mem-util above 1", 0, 0, 2, 0, 0, "--notify-mem-util"},
		{"growth rate above 1", 0, 0, 0, 0, 2, "--notify-storage-growth-per-min"},
		{"negative fraction", -0.5, 0, 0, 0, 0, "--notify-storage-util"},
		{"NaN fraction", math.NaN(), 0, 0, 0, 0, "--notify-storage-util"},
		{"negative lag seconds", 0, 0, 0, -1, 0, "--notify-lag-seconds"},
		{"NaN lag seconds", 0, 0, 0, math.NaN(), 0, "--notify-lag-seconds"},
	}
	for _, tc := range bad {
		err := validateMetricsNotifyThresholds(tc.su, tc.cu, tc.mu, tc.lag, tc.growth)
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
	err := validateMetricsNotifyThresholds(85, 0, 0, 0, 0)
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
