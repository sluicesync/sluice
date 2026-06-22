// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0110 telemetry-side pins: the storage-recovery probe the proactive
// grow-gate consults. The WARN edge-trigger semantics are pinned in
// streamer_telemetry_test.go and are untouched by ADR-0110; here we pin only
// the recovery probe's degrade-safe verdicts.

func TestStorageRecoveredProbe_NilProviderIsNil(t *testing.T) {
	if probe := storageRecoveredProbe(context.Background(), nil); probe != nil {
		t.Fatal("nil provider must yield a nil recovery probe (gate then signal-driven only)")
	}
}

func TestStorageRecoveredProbe_Verdicts(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		prov *fakeTelemetry
		want bool
	}{
		{
			name: "recovered: fresh known storage below high-water",
			prov: &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
				SampledAt: freshNow(), StorageUtil: 0.40, StorageKnown: true,
			}},
			want: true,
		},
		{
			name: "not recovered: still above high-water",
			prov: &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
				SampledAt: freshNow(), StorageUtil: 0.95, StorageKnown: true,
			}},
			want: false,
		},
		{
			name: "degrade: ok=false (no signal) ⇒ not recovered (fall back to max-hold)",
			prov: &fakeTelemetry{ok: false, snap: ir.TargetHealthSnapshot{
				SampledAt: freshNow(), StorageUtil: 0.10, StorageKnown: true,
			}},
			want: false,
		},
		{
			name: "degrade: storage unknown ⇒ not recovered",
			prov: &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
				SampledAt: freshNow(), StorageUtil: 0.10, StorageKnown: false,
			}},
			want: false,
		},
		{
			name: "degrade: stale snapshot ⇒ not recovered",
			prov: &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
				SampledAt: time.Now().Add(-10 * time.Minute), StorageUtil: 0.10, StorageKnown: true,
			}},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := storageRecoveredProbe(ctx, tc.prov)
			if probe == nil {
				t.Fatal("non-nil provider must yield a probe")
			}
			if got := probe(); got != tc.want {
				t.Errorf("probe() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestStorageHeadroomTickWithGate_TripsOnRisingEdge pins the real
// production tick helper: it trips the supplied gate exactly once per
// rising edge of crossing the storage high-water mark (the PROACTIVE
// coordinated pause), and NOT on a sustained-high or a recovery tick — and
// the underlying WARN edge-trigger latch behaves exactly as without a gate.
func TestStorageHeadroomTickWithGate_TripsOnRisingEdge(t *testing.T) {
	captureSlog(t)
	logger := slog.Default()
	gate := &recordingGate{}
	ctx := context.Background()

	above := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: freshNow(), StorageUtil: 0.95, StorageKnown: true,
	}}
	below := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: freshNow(), StorageUtil: 0.40, StorageKnown: true,
	}}

	warned := false
	// below → above (rising edge): trip + latch true.
	warned = evalStorageHeadroomTickWithGate(ctx, logger, above, "s1", warned, gate)
	if !warned {
		t.Fatal("crossing the mark must latch warned=true")
	}
	// above → above (sustained): no further trip.
	warned = evalStorageHeadroomTickWithGate(ctx, logger, above, "s1", warned, gate)
	// above → below (recovered): re-arm, no trip.
	warned = evalStorageHeadroomTickWithGate(ctx, logger, below, "s1", warned, gate)
	if warned {
		t.Fatal("recovery must re-arm the latch (warned=false)")
	}
	// below → above again (second crossing): another trip.
	evalStorageHeadroomTickWithGate(ctx, logger, above, "s1", warned, gate)

	if got := gate.trips.Load(); got != 2 {
		t.Fatalf("gate trips = %d, want 2 (one per rising-edge crossing, none on sustain/recover)", got)
	}
}

// TestStorageHeadroomTickWithGate_NilGateNeverTrips pins the apply-phase /
// no-cold-copy degrade: a nil gate makes the tick WARN-only — never a trip,
// never a panic.
func TestStorageHeadroomTickWithGate_NilGateNeverTrips(t *testing.T) {
	captureSlog(t)
	logger := slog.Default()
	above := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: freshNow(), StorageUtil: 0.95, StorageKnown: true,
	}}
	// A crossing with a nil gate must not panic and must still latch.
	if warned := evalStorageHeadroomTickWithGate(context.Background(), logger, above, "s1", false, nil); !warned {
		t.Fatal("crossing must still latch with a nil gate (WARN-only path)")
	}
}
