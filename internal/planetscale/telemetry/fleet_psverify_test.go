//go:build psverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Credentialed smoke test for the ORG-WIDE fleet provider (roadmap item
// 75b). Same posture as TestPSVerify_TelemetryProvider: gated behind the
// psverify build tag, credentials from the environment or the machine-local
// env file, token never printed.
//
//	go test -tags=psverify -v -count=1 -timeout=3m \
//	  -run 'TestPSVerify_FleetTelemetry' ./internal/planetscale/telemetry/...
//
// What it proves that no fake can: that ONE service-discovery call really
// does enumerate the whole org (the discovery-source decision), and that the
// per-target engine detection reads a REAL mixed org correctly — the
// `sluicesync` org carries both Vitess/MySQL and Postgres databases, which
// is exactly the case a single declared --engine would get wrong.

package telemetry

import (
	"context"
	"testing"
	"time"
)

func TestPSVerify_FleetTelemetry(t *testing.T) {
	tokenID := psverifyEnv(t, "PLANETSCALE_METRICS_SERVICE_TOKEN_ID")
	token := psverifyEnv(t, "PLANETSCALE_METRICS_SERVICE_TOKEN")
	org := psverifyEnv(t, "PLANETSCALE_METRICS_ORG")
	if tokenID == "" || token == "" || org == "" {
		t.Skip("PLANETSCALE_METRICS_* credentials absent — skipping credentialed smoke test")
	}
	t.Logf("psverify fleet: org=%s token_id=%s token=<%d-char redacted>", org, tokenID, len(token))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	f, err := NewFleet(ctx, FleetConfig{
		Org:          org,
		TokenID:      tokenID,
		Token:        token,
		PollInterval: 15 * time.Second,
		Concurrency:  4,
	})
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	defer func() { _ = f.Close() }()

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		samples := f.SampleFleet(ctx)
		observed := 0
		for _, s := range samples {
			if s.OK {
				observed++
			}
		}
		if len(samples) == 0 || observed == 0 {
			time.Sleep(2 * time.Second)
			continue
		}
		for _, s := range samples {
			snap := s.Snapshot
			t.Logf("live %s: ok=%v cpu=%.3f(known=%v) mem=%.3f(known=%v) storage=%.3f(known=%v avail=%d cap=%d) lag=%.1fs(known=%v) conns=%d/%d(known=%v)",
				s.Target, s.OK,
				snap.CPUUtil, snap.CPUKnown, snap.MemUtil, snap.MemKnown,
				snap.StorageUtil, snap.StorageKnown, snap.StorageAvailableBytes, snap.StorageCapacityBytes,
				snap.ReplicaLagSeconds, snap.LagKnown, snap.ActiveConnections, snap.MaxConnections, (snap.ActiveConnKnown || snap.MaxConnKnown))
		}
		t.Logf("fleet: %d targets discovered, %d observed", len(samples), observed)

		// Every OBSERVED target must have read at least one metric family —
		// the same bar TestPSVerify_TelemetryProvider sets. A target that is
		// fresh but reads NOTHING means the per-target engine detection
		// picked the wrong metric-name table (or the marker constants have
		// drifted from the live exposition).
		for _, s := range samples {
			if !s.OK {
				continue
			}
			snap := s.Snapshot
			if !snap.CPUKnown && !snap.MemKnown && !snap.StorageKnown && !snap.LagKnown && (!snap.ActiveConnKnown || !snap.MaxConnKnown) {
				t.Errorf("%s: observed but NO metric family read — metricNamesForExposition likely picked the wrong table for this target", s.Target)
			}
		}

		// KNOWN GAP, deliberately not asserted (pre-dates this fan-out;
		// ground-truthed 2026-07-24). On a real Postgres branch the
		// `planetscale_volume_*` series are per-pod and labelled
		// `planetscale_role="primary"|"replica"` — NOT `planetscale_tablet_type`
		// and with no `planetscale_container` — so selectPrimaryValue's
		// cascade finds three indistinguishable series and honestly refuses
		// to guess, leaving StorageKnown=false. That is the *Known contract
		// working (no wrong number), but it means PG storage reads n/a on the
		// live endpoint in BOTH single-database and org-wide mode. Closing it
		// means teaching selectPrimaryValue the `planetscale_role` label,
		// which changes the single-database PG path and needs its own
		// reduction decision (primary's volume vs fullest volume) — filed as
		// a follow-up rather than folded in here.
		return
	}
	t.Fatal("fleet never reported an observed target within 90s (check the org and the service-token permission)")
}
