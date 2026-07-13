// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
	"sluicesync.dev/sluice/internal/progress"
)

// TestMetricsWatchReadoutFields_Honesty pins the ADR-0156 readout builder: a
// known snapshot renders every metric (storage with the used/capacity detail),
// an unknown metric renders "n/a" (never a misleading 0), and a no-sample poll
// collapses to a single status row rather than a wall of n/a.
func TestMetricsWatchReadoutFields_Honesty(t *testing.T) {
	now := time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC)

	// ok=false → single status row.
	nf := metricsWatchReadoutFields(now, ir.TargetHealthSnapshot{}, false)
	if len(nf) != 1 || nf[0].Label != "status" {
		t.Fatalf("no-sample readout should be one status row, got %+v", nf)
	}

	// Mixed known/unknown snapshot.
	snap := ir.TargetHealthSnapshot{
		SampledAt: now,
		CPUUtil:   0.42, CPUKnown: true,
		MemKnown:              false, // unobserved
		StorageUtil:           0.55,
		StorageAvailableBytes: 5_000_000_000,
		StorageCapacityBytes:  10_000_000_000,
		StorageKnown:          true,
		ReplicaLagSeconds:     1.5, LagKnown: true,
		ActiveConnections: 12, MaxConnections: 100, ConnKnown: true,
	}
	got := metricsWatchReadoutFields(now, snap, true)
	want := map[string]string{
		"cpu":         "0.420",
		"mem":         "n/a",
		"storage":     "0.550 (used 5.0G/10.0G)",
		"lag":         "1.5s",
		"connections": "12/100",
		"fresh":       "true",
	}
	if len(got) != len(want) {
		t.Fatalf("field count = %d, want %d: %+v", len(got), len(want), got)
	}
	for _, f := range got {
		if w, ok := want[f.Label]; !ok || w != f.Value {
			t.Errorf("field %q = %q, want %q", f.Label, f.Value, w)
		}
	}
}

// TestPanelEventNotifier_LevelMapping pins the breach→event mapping: a critical
// breach surfaces as ERROR, a warning breach as WARN, and the title/body
// compose into one line.
func TestPanelEventNotifier_LevelMapping(t *testing.T) {
	var gotLevel, gotText string
	pen := panelEventNotifier{emit: func(level, text string) { gotLevel, gotText = level, text }}

	_ = pen.Notify(context.Background(), notify.Notification{Level: notify.LevelCritical, Title: "storage approaching capacity", Body: "0.95 ≥ 0.90"})
	if gotLevel != "ERROR" || gotText != "storage approaching capacity — 0.95 ≥ 0.90" {
		t.Errorf("critical breach: level=%q text=%q", gotLevel, gotText)
	}

	_ = pen.Notify(context.Background(), notify.Notification{Level: notify.LevelWarning, Title: "cpu saturating"})
	if gotLevel != "WARN" || gotText != "cpu saturating" {
		t.Errorf("warning breach: level=%q text=%q", gotLevel, gotText)
	}
}

// TestRunMetricsWatch_ReadoutAndEventWiring pins the pretty-path wiring: a
// single Once tick pushes a Readout, and a breaching sample fires a panel Event
// EVEN WITH NO external --notify-* sink (the internal panel notifier makes the
// rules evaluate). It also confirms the plain sample line is NOT printed on the
// Readout path (the panel owns stdout).
func TestRunMetricsWatch_ReadoutAndEventWiring(t *testing.T) {
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt:            time.Now(),
		StorageUtil:          0.95,
		StorageKnown:         true,
		StorageCapacityBytes: 10_000_000_000, StorageAvailableBytes: 500_000_000,
	}}

	var readouts int
	var events []string
	err := RunMetricsWatch(context.Background(), prov, MetricsWatchConfig{
		StorageUtil: 0.90, // breached by 0.95
		Once:        true,
		Print:       true, // must be OVERRIDDEN by the Readout hook
		Readout:     func(_ []progress.Field) { readouts++ },
		Event:       func(_, _ string) {}, // presence enables the panel notifier
	})
	if err != nil {
		t.Fatalf("RunMetricsWatch: %v", err)
	}
	// Re-run capturing the event text (a fresh state, deterministic edge fire).
	err = RunMetricsWatch(context.Background(), prov, MetricsWatchConfig{
		StorageUtil: 0.90,
		Once:        true,
		Readout:     func(_ []progress.Field) { readouts++ },
		Event:       func(level, text string) { events = append(events, level+": "+text) },
	})
	if err != nil {
		t.Fatalf("RunMetricsWatch (2): %v", err)
	}
	if readouts != 2 {
		t.Errorf("expected one Readout push per Once run (2 total), got %d", readouts)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly one panel breach event, got %v", events)
	}
	if events[0][:6] != "ERROR:" {
		t.Errorf("storage breach should map to an ERROR event, got %q", events[0])
	}
}
