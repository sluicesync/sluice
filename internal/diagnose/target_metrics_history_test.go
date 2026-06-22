// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// metricsHistoryApplier is a fakeApplier that ALSO implements
// ir.TargetMetricsHistoryStore — the type-assert branch the bundle's
// target_metrics_history section takes. Only ListTargetMetricsHistory is
// exercised by diagnose; the write/prune methods satisfy the interface.
type metricsHistoryApplier struct {
	*fakeApplier
	rows []ir.TargetMetricsHistoryRow
}

func (a *metricsHistoryApplier) EnsureTargetMetricsHistory(context.Context) error { return nil }
func (a *metricsHistoryApplier) RecordTargetMetricsSample(context.Context, ir.TargetMetricsSample) error {
	return nil
}

func (a *metricsHistoryApplier) PruneTargetMetricsHistory(context.Context, time.Duration) error {
	return nil
}

func (a *metricsHistoryApplier) ListTargetMetricsHistory(_ context.Context, _ string, _ int) ([]ir.TargetMetricsHistoryRow, error) {
	return a.rows, nil
}

// metricsHistoryEngine returns a configurable applier from
// OpenChangeApplier so the section sees the store surface (or, when
// withStore is false, the bare fakeApplier without it).
type metricsHistoryEngine struct {
	*fakeEngine
	applierWithStore ir.ChangeApplier
}

func (e *metricsHistoryEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return e.applierWithStore, nil
}

func writeMetricsHistoryBundle(t *testing.T, target ir.Engine) map[string][]byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Write(context.Background(), &buf, Request{
		StreamID:     "stream-1",
		PrivacyLevel: PrivacyStandard,
		TargetEngine: target,
		TargetDSN:    "postgres://u:p@h:5432/db",
		Now:          time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return readBundle(t, &buf)
}

// TestBundle_TargetMetricsHistory_HonestStates pins the three honest
// states of the ADR-0107 item 35 section: engine without the store impl →
// reason file; store with no rows → empty arrays/aggregates; populated →
// rendered rows.
func TestBundle_TargetMetricsHistory_HonestStates(t *testing.T) {
	base, _ := newFakeTarget("stream-1")

	t.Run("no store impl → reason", func(t *testing.T) {
		// A plain fakeEngine returns the base fakeApplier, which does NOT
		// implement ir.TargetMetricsHistoryStore.
		files := writeMetricsHistoryBundle(t, base)
		if _, ok := files["health/target_metrics_history.json"]; ok {
			t.Error("expected reason file, got a JSON section with no store impl")
		}
		if _, ok := files["health/target_metrics_history.json/__skipped.txt"]; !ok {
			t.Errorf("missing reason file; have %v", fileNames(files))
		}
	})

	t.Run("store with no rows → empty", func(t *testing.T) {
		eng := &metricsHistoryEngine{
			fakeEngine:       base,
			applierWithStore: &metricsHistoryApplier{fakeApplier: base.applier, rows: nil},
		}
		files := writeMetricsHistoryBundle(t, eng)
		b, ok := files["health/target_metrics_history.json"]
		if !ok {
			t.Fatalf("missing section; have %v", fileNames(files))
		}
		var out struct {
			Recent     []map[string]any `json:"recent"`
			Aggregates map[string]any   `json:"aggregates"`
		}
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(out.Recent) != 0 {
			t.Errorf("recent should be empty, got %v", out.Recent)
		}
	})

	t.Run("populated → rendered rows", func(t *testing.T) {
		eng := &metricsHistoryEngine{
			fakeEngine: base,
			applierWithStore: &metricsHistoryApplier{
				fakeApplier: base.applier,
				rows: []ir.TargetMetricsHistoryRow{
					{
						SampledAt: time.Date(2026, 6, 22, 11, 59, 0, 0, time.UTC),
						CPUUtil:   0.6, CPUKnown: true,
						StorageUtil: 0.8, StorageKnown: true,
						StorageAvailableBytes: 5 << 30, StorageCapacityBytes: 10 << 30,
						// Mem / lag / conns UNKNOWN — must be omitted.
					},
				},
			},
		}
		files := writeMetricsHistoryBundle(t, eng)
		b := files["health/target_metrics_history.json"]
		var out struct {
			Recent []map[string]any `json:"recent"`
		}
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(out.Recent) != 1 {
			t.Fatalf("expected 1 recent row, got %d", len(out.Recent))
		}
		row := out.Recent[0]
		if row["cpu_util"] != 0.6 {
			t.Errorf("cpu_util = %v, want 0.6", row["cpu_util"])
		}
		// Unobserved metrics must be ABSENT, not present-as-zero.
		for _, k := range []string{"mem_util", "replica_lag_seconds", "active_connections"} {
			if _, present := row[k]; present {
				t.Errorf("unobserved metric %q present; honesty contract requires omission", k)
			}
		}
	})
}

// TestComputeTargetMetricsAggregates pins the window math directly: feed
// known rows at known instants and assert avg/max per 1m/5m/10m window
// (relative to the newest row) + current.
func TestComputeTargetMetricsAggregates(t *testing.T) {
	newest := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	// Rows sampled_at DESC (newest first), CPU climbing as time goes back.
	rows := []ir.TargetMetricsHistoryRow{
		{SampledAt: newest, CPUUtil: 0.90, CPUKnown: true},                        // current, in 1m/5m/10m
		{SampledAt: newest.Add(-30 * time.Second), CPUUtil: 0.50, CPUKnown: true}, // in 1m/5m/10m
		{SampledAt: newest.Add(-3 * time.Minute), CPUUtil: 0.30, CPUKnown: true},  // in 5m/10m
		{SampledAt: newest.Add(-8 * time.Minute), CPUUtil: 0.10, CPUKnown: true},  // in 10m only
		{SampledAt: newest.Add(-20 * time.Minute), CPUUtil: 0.05, CPUKnown: true}, // outside all
	}
	agg := computeTargetMetricsAggregates(rows)
	cpu, ok := agg["cpu"].(map[string]any)
	if !ok {
		t.Fatalf("cpu aggregate missing: %v", agg)
	}

	if cpu["current"] != 0.90 {
		t.Errorf("current = %v, want 0.90", cpu["current"])
	}
	// 1m window: rows at 0s and -30s → {0.90, 0.50}.
	assertFloat(t, cpu, "avg_1m", (0.90+0.50)/2)
	assertFloat(t, cpu, "max_1m", 0.90)
	// 5m window: rows at 0s, -30s, -3m → {0.90, 0.50, 0.30}.
	assertFloat(t, cpu, "avg_5m", (0.90+0.50+0.30)/3)
	assertFloat(t, cpu, "max_5m", 0.90)
	// 10m window: + the -8m row → {0.90, 0.50, 0.30, 0.10}; the -20m row excluded.
	assertFloat(t, cpu, "avg_10m", (0.90+0.50+0.30+0.10)/4)
	assertFloat(t, cpu, "max_10m", 0.90)
}

// TestComputeTargetMetricsAggregates_UnknownExcluded pins that an
// unobserved value does NOT contribute to any window (no fabricated 0).
func TestComputeTargetMetricsAggregates_UnknownExcluded(t *testing.T) {
	newest := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	rows := []ir.TargetMetricsHistoryRow{
		{SampledAt: newest, MemUtil: 0, MemKnown: false},                          // unobserved
		{SampledAt: newest.Add(-10 * time.Second), MemUtil: 0.40, MemKnown: true}, // the only known
	}
	agg := computeTargetMetricsAggregates(rows)
	mem := agg["mem"].(map[string]any)
	if mem["current"] != 0.40 {
		t.Errorf("current = %v, want 0.40 (the newest KNOWN reading)", mem["current"])
	}
	// avg/max over the single known value.
	assertFloat(t, mem, "avg_1m", 0.40)
	assertFloat(t, mem, "max_1m", 0.40)
}

func assertFloat(t *testing.T, m map[string]any, key string, want float64) {
	t.Helper()
	got, ok := m[key]
	if !ok {
		t.Errorf("%s missing", key)
		return
	}
	gf, ok := got.(float64)
	if !ok {
		t.Errorf("%s is %T, want float64", key, got)
		return
	}
	if gf != want {
		t.Errorf("%s = %v, want %v", key, gf, want)
	}
}
