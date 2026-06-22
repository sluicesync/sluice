// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

// fakeTelemetry is a scriptable [ir.TargetTelemetry] for the ADR-0107
// Phase-1 tests. It returns a fixed snapshot + ok flag; tests mutate the
// fields between calls to model staleness / saturation transitions. No
// PlanetScale, no HTTP — the whole point of the engine-neutral seam.
type fakeTelemetry struct {
	snap ir.TargetHealthSnapshot
	ok   bool
}

func (f *fakeTelemetry) Sample(context.Context) (ir.TargetHealthSnapshot, bool) {
	return f.snap, f.ok
}

func freshNow() time.Time { return time.Now() }

// --- emitTargetTelemetryMetrics exposition shape (use (c)) ---

func TestEmitTargetTelemetryMetrics_AllKnown(t *testing.T) {
	snap := ir.TargetHealthSnapshot{
		SampledAt:             freshNow(),
		CPUUtil:               0.42,
		CPUKnown:              true,
		MemUtil:               0.70,
		MemKnown:              true,
		StorageUtil:           0.55,
		StorageAvailableBytes: 5 << 30,
		StorageCapacityBytes:  10 << 30,
		StorageKnown:          true,
		ReplicaLagSeconds:     1.5,
		LagKnown:              true,
		ActiveConnections:     12,
		MaxConnections:        100,
		ConnKnown:             true,
	}
	var buf bytes.Buffer
	emitTargetTelemetryMetrics(&buf, "s1", snap)
	out := buf.String()
	for _, want := range []string{
		`sluice_target_cpu_util{stream_id="s1"} 0.4200`,
		`sluice_target_mem_util{stream_id="s1"} 0.7000`,
		`sluice_target_storage_util{stream_id="s1"} 0.5500`,
		`sluice_target_storage_available_bytes{stream_id="s1"} 5368709120`,
		`sluice_target_storage_capacity_bytes{stream_id="s1"} 10737418240`,
		`sluice_target_replica_lag_seconds{stream_id="s1"} 1.5000`,
		`sluice_target_active_connections{stream_id="s1"} 12`,
		`sluice_target_max_connections{stream_id="s1"} 100`,
		"# TYPE sluice_target_cpu_util gauge",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output; got:\n%s", want, out)
		}
	}
}

// TestEmitTargetTelemetryMetrics_UnknownOmitted pins that an unobserved
// metric (*Known=false) OMITS its line entirely — never a misleading 0.
func TestEmitTargetTelemetryMetrics_UnknownOmitted(t *testing.T) {
	snap := ir.TargetHealthSnapshot{
		SampledAt: freshNow(),
		CPUUtil:   0.9,
		CPUKnown:  true,
		// Mem / Storage / Lag / Conn all UNKNOWN.
	}
	var buf bytes.Buffer
	emitTargetTelemetryMetrics(&buf, "s1", snap)
	out := buf.String()
	if !strings.Contains(out, `sluice_target_cpu_util{stream_id="s1"} 0.9000`) {
		t.Fatalf("known CPU line missing:\n%s", out)
	}
	for _, absent := range []string{
		"sluice_target_mem_util",
		"sluice_target_storage_util",
		"sluice_target_storage_available_bytes",
		"sluice_target_replica_lag_seconds",
		"sluice_target_active_connections",
	} {
		if strings.Contains(out, absent) {
			t.Errorf("unobserved metric %q should be omitted, not emitted as 0; got:\n%s", absent, out)
		}
	}
}

// TestHandleMetrics_TelemetryNoSignalEmitsComment pins that a wired
// provider returning ok=false emits the exposition comment (not a 500,
// not silence) so the scraper sees "no signal" — and that NO provider
// emits neither comment nor sluice_target_* lines (byte-identical to
// pre-ADR-0107).
func TestHandleMetrics_TelemetryNoSignalEmitsComment(t *testing.T) {
	srv := newTestMetricsServer(t)
	// ok=false provider: the scrape emits the no-signal comment, not a
	// 500 and not silence, and no sluice_target_* lines.
	srv.AttachTargetTelemetry(&fakeTelemetry{ok: false})
	out := scrapeMetrics(t, srv)
	if !strings.Contains(out, "# target-telemetry: no signal") {
		t.Fatalf("expected no-signal comment; got:\n%s", out)
	}
	if strings.Contains(out, "sluice_target_cpu_util") {
		t.Fatalf("ok=false must not emit sluice_target_* lines")
	}

	// A fresh, known snapshot emits the gauges and no comment.
	srv.AttachTargetTelemetry(&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: freshNow(), CPUUtil: 0.5, CPUKnown: true,
	}})
	out = scrapeMetrics(t, srv)
	if !strings.Contains(out, "sluice_target_cpu_util") {
		t.Fatalf("fresh known snapshot should emit gauges; got:\n%s", out)
	}
	if strings.Contains(out, "no signal") {
		t.Fatalf("fresh known snapshot should not emit the no-signal comment")
	}

	// Detached: neither comment nor lines (byte-identical to pre-ADR-0107).
	srv.AttachTargetTelemetry(nil)
	out = scrapeMetrics(t, srv)
	if strings.Contains(out, "target-telemetry") || strings.Contains(out, "sluice_target_") {
		t.Fatalf("nil provider must add nothing; got:\n%s", out)
	}
}

// --- storage-headroom WARN edge-trigger (use (b)) ---

func TestStorageHeadroomTick_EdgeFiresOncePerCrossing(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt:             freshNow(),
		StorageUtil:           0.90, // above the 0.85 high-water
		StorageAvailableBytes: 1 << 30,
		StorageCapacityBytes:  10 << 30,
		StorageKnown:          true,
	}}
	ctx := context.Background()

	warnCount := func() int { return strings.Count(buf.String(), "approaching capacity") }

	// First tick over the mark: WARN fires once; latch -> true.
	warned := evalStorageHeadroomTick(ctx, logger, prov, "s1", false)
	if !warned || warnCount() != 1 {
		t.Fatalf("first crossing should warn once; warned=%v count=%d", warned, warnCount())
	}
	// Sustained over the mark: NO re-warn.
	warned = evalStorageHeadroomTick(ctx, logger, prov, "s1", warned)
	warned = evalStorageHeadroomTick(ctx, logger, prov, "s1", warned)
	if warnCount() != 1 {
		t.Fatalf("sustained-low-headroom must not re-warn; count=%d", warnCount())
	}
	// Headroom recovers: latch re-arms (no warn on recovery).
	prov.snap.StorageUtil = 0.40
	prov.snap.SampledAt = freshNow()
	warned = evalStorageHeadroomTick(ctx, logger, prov, "s1", warned)
	if warned {
		t.Fatalf("recovery should re-arm the latch (warned=false)")
	}
	if warnCount() != 1 {
		t.Fatalf("recovery must not emit a WARN; count=%d", warnCount())
	}
	// Second crossing: WARN fires again.
	prov.snap.StorageUtil = 0.95
	prov.snap.SampledAt = freshNow()
	warned = evalStorageHeadroomTick(ctx, logger, prov, "s1", warned)
	if !warned || warnCount() != 2 {
		t.Fatalf("second crossing should warn again; warned=%v count=%d", warned, warnCount())
	}
}

// TestStorageHeadroomTick_NoSignalNeverWarns pins that ok=false /
// unknown-storage / stale snapshots never warn and never disturb the
// latch.
func TestStorageHeadroomTick_NoSignalNeverWarns(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx := context.Background()

	cases := []struct {
		name string
		prov *fakeTelemetry
	}{
		{"ok=false", &fakeTelemetry{ok: false, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), StorageUtil: 0.99, StorageKnown: true}}},
		{"storage unknown", &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), StorageUtil: 0.99, StorageKnown: false}}},
		{"stale snapshot", &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: time.Now().Add(-10 * time.Minute), StorageUtil: 0.99, StorageKnown: true}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Starting latch=false; a no-signal tick must leave it false
			// and emit nothing.
			if got := evalStorageHeadroomTick(ctx, logger, tc.prov, "s1", false); got {
				t.Fatalf("no-signal tick should not set the latch")
			}
			// Starting latch=true; a no-signal tick must NOT re-arm it
			// (so a transient stale poll doesn't cause a spurious re-warn).
			if got := evalStorageHeadroomTick(ctx, logger, tc.prov, "s1", true); !got {
				t.Fatalf("no-signal tick should leave a latched warn intact")
			}
			if strings.Contains(buf.String(), "approaching capacity") {
				t.Fatalf("no-signal must not WARN; got:\n%s", buf.String())
			}
		})
	}
}

func TestStartStorageHeadroomWatch_NilProviderNoOp(_ *testing.T) {
	// A Streamer with no provider must not spawn a goroutine or panic.
	s := &Streamer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startStorageHeadroomWatch(ctx, "s1", nil) // must be a total no-op.
}

// --- telemetryHint adapter degrade behaviour (use (a)) ---

func TestTelemetryHint_NilProviderDegradesToNil(t *testing.T) {
	if h := newTelemetryHint(context.Background(), nil, 0.85); h != nil {
		t.Fatalf("nil provider should yield a nil adapter")
	}
	// And the interface conversion stays a true nil (no typed-nil trap).
	if got := telemetryHintOrNil(nil); got != nil {
		t.Fatalf("telemetryHintOrNil(nil) must be a true nil interface")
	}
}

func TestTelemetryHint_SaturationVerdicts(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name          string
		snap          ir.TargetHealthSnapshot
		ok            bool
		wantSaturated bool
		wantOK        bool
	}{
		{
			name:          "fresh CPU over mark -> saturated",
			snap:          ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.90, CPUKnown: true},
			ok:            true,
			wantSaturated: true,
			wantOK:        true,
		},
		{
			name:          "fresh mem over mark -> saturated",
			snap:          ir.TargetHealthSnapshot{SampledAt: freshNow(), MemUtil: 0.86, MemKnown: true},
			ok:            true,
			wantSaturated: true,
			wantOK:        true,
		},
		{
			name:          "fresh and healthy -> not saturated but ok",
			snap:          ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.10, CPUKnown: true, MemUtil: 0.20, MemKnown: true},
			ok:            true,
			wantSaturated: false,
			wantOK:        true,
		},
		{
			name:          "provider ok=false -> no signal",
			snap:          ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.99, CPUKnown: true},
			ok:            false,
			wantSaturated: false,
			wantOK:        false,
		},
		{
			name:          "stale snapshot -> no signal",
			snap:          ir.TargetHealthSnapshot{SampledAt: time.Now().Add(-10 * time.Minute), CPUUtil: 0.99, CPUKnown: true},
			ok:            true,
			wantSaturated: false,
			wantOK:        false,
		},
		{
			name:          "CPU and mem both unknown -> no signal",
			snap:          ir.TargetHealthSnapshot{SampledAt: freshNow(), StorageUtil: 0.99, StorageKnown: true},
			ok:            true,
			wantSaturated: false,
			wantOK:        false,
		},
		{
			name:          "exactly at high-water -> saturated (inclusive)",
			snap:          ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: appliercontrol.DefaultTelemetryHighWater, CPUKnown: true},
			ok:            true,
			wantSaturated: true,
			wantOK:        true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTelemetryHint(ctx, &fakeTelemetry{snap: tc.snap, ok: tc.ok}, appliercontrol.DefaultTelemetryHighWater)
			sat, ok := h.Saturated()
			if sat != tc.wantSaturated || ok != tc.wantOK {
				t.Fatalf("Saturated() = (%v, %v); want (%v, %v)", sat, ok, tc.wantSaturated, tc.wantOK)
			}
		})
	}
}
