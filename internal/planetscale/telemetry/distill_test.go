// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fullExposition is a representative MySQL/Vitess scrape with multiple pods
// per metric, so the primary-vttablet selection is exercised against decoy
// vtgate/replica series.
const fullExposition = `
planetscale_pods_cpu_util_percentages{planetscale_component="vtgate",planetscale_tablet_type=""} 9
planetscale_pods_cpu_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="replica"} 12
planetscale_pods_cpu_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="primary"} 87.5
planetscale_pods_mem_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="primary"} 40
planetscale_pods_mem_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="replica"} 30
planetscale_vttablet_volume_available_bytes{planetscale_component="vttablet",planetscale_tablet_type="primary"} 250000000000
planetscale_vttablet_volume_capacity_bytes{planetscale_component="vttablet",planetscale_tablet_type="primary"} 1000000000000
planetscale_mysql_replica_lag_seconds{planetscale_component="vttablet",planetscale_tablet_type="primary"} 3
planetscale_edge_active_connections{planetscale_component="vtgate"} 128
planetscale_mysql_max_connections{planetscale_component="vttablet",planetscale_tablet_type="primary"} 1000
`

func TestDistill_FullExposition_AllFamilies(t *testing.T) {
	snap := distillText(fullExposition)

	// CPU: PERCENTAGE 87.5 → fraction 0.875, primary-vttablet selected over
	// the vtgate (9) and replica (12) decoys.
	if !snap.CPUKnown {
		t.Fatal("CPUKnown false; want true")
	}
	if snap.CPUUtil != 0.875 {
		t.Errorf("CPUUtil = %v, want 0.875 (87.5%% normalized, primary pod)", snap.CPUUtil)
	}

	// Mem: 40 → 0.40, primary over replica.
	if !snap.MemKnown || snap.MemUtil != 0.40 {
		t.Errorf("MemUtil = %v known=%v, want 0.40 true", snap.MemUtil, snap.MemKnown)
	}

	// Storage: 1 - 250e9/1000e9 = 0.75; raw bytes carried.
	if !snap.StorageKnown {
		t.Fatal("StorageKnown false; want true")
	}
	if snap.StorageUtil != 0.75 {
		t.Errorf("StorageUtil = %v, want 0.75", snap.StorageUtil)
	}
	if snap.StorageAvailableBytes != 250000000000 || snap.StorageCapacityBytes != 1000000000000 {
		t.Errorf("raw storage bytes = %d/%d", snap.StorageAvailableBytes, snap.StorageCapacityBytes)
	}

	// Lag (secondary).
	if !snap.LagKnown || snap.ReplicaLagSeconds != 3 {
		t.Errorf("ReplicaLagSeconds = %v known=%v, want 3 true", snap.ReplicaLagSeconds, snap.LagKnown)
	}

	// Connections (secondary): active from edge (128), max from primary
	// vttablet (1000).
	if !snap.ConnKnown {
		t.Fatal("ConnKnown false; want true")
	}
	if snap.ActiveConnections != 128 || snap.MaxConnections != 1000 {
		t.Errorf("conns = %d/%d, want 128/1000", snap.ActiveConnections, snap.MaxConnections)
	}
}

// TestDistill_MissingMetric_KnownFalse pins the honesty contract: a metric
// absent from the scrape leaves its value 0 AND *Known false — never a 0
// that a consumer reads as "idle".
func TestDistill_MissingMetric_KnownFalse(t *testing.T) {
	// Only CPU present; everything else absent.
	const text = `planetscale_pods_cpu_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="primary"} 50`
	snap := distillText(text)

	if !snap.CPUKnown || snap.CPUUtil != 0.5 {
		t.Errorf("CPU = %v known=%v, want 0.5 true", snap.CPUUtil, snap.CPUKnown)
	}
	if snap.MemKnown {
		t.Error("MemKnown true; want false (metric absent)")
	}
	if snap.StorageKnown {
		t.Error("StorageKnown true; want false (volume metrics absent)")
	}
	if snap.LagKnown {
		t.Error("LagKnown true; want false (lag absent)")
	}
	if snap.ConnKnown {
		t.Error("ConnKnown true; want false (conn metrics absent)")
	}
}

// TestDistill_StorageMissingHalf pins that storage needs BOTH available and
// capacity to be Known (a partial volume read can't yield a util fraction).
func TestDistill_StorageMissingHalf(t *testing.T) {
	const text = `planetscale_vttablet_volume_available_bytes{planetscale_tablet_type="primary"} 5`
	snap := distillText(text)
	if snap.StorageKnown {
		t.Error("StorageKnown true with only available (no capacity); want false")
	}
}

// TestSelectPrimaryValue_SelectionCascade pins the graceful primary-pod
// selection cascade.
func TestSelectPrimaryValue_SelectionCascade(t *testing.T) {
	t.Run("exact vttablet primary wins over decoys", func(t *testing.T) {
		samples := parsePromText(strings.NewReader(`
m{planetscale_component="vtgate"} 1
m{planetscale_component="vttablet",planetscale_tablet_type="replica"} 2
m{planetscale_component="vttablet",planetscale_tablet_type="primary"} 3
`))
		v, ok := selectPrimaryValue(samples, "m")
		if !ok || v != 3 {
			t.Errorf("got %v ok=%v, want 3 true", v, ok)
		}
	})

	t.Run("primary label without vttablet component (fallback 2)", func(t *testing.T) {
		samples := parsePromText(strings.NewReader(`
m{planetscale_component="vtgate"} 1
m{planetscale_tablet_type="primary"} 7
`))
		v, ok := selectPrimaryValue(samples, "m")
		if !ok || v != 7 {
			t.Errorf("got %v ok=%v, want 7 true", v, ok)
		}
	})

	t.Run("single unlabelled series (fallback 3)", func(t *testing.T) {
		samples := parsePromText(strings.NewReader(`m 11`))
		v, ok := selectPrimaryValue(samples, "m")
		if !ok || v != 11 {
			t.Errorf("got %v ok=%v, want 11 true", v, ok)
		}
	})

	t.Run("multiple ambiguous series → refuse to guess", func(t *testing.T) {
		samples := parsePromText(strings.NewReader(`
m{planetscale_component="vtgate"} 1
m{planetscale_component="vtorc"} 2
`))
		_, ok := selectPrimaryValue(samples, "m")
		if ok {
			t.Error("ok true for ambiguous non-primary series; want false (refuse to guess the wrong pod)")
		}
	})

	t.Run("absent metric", func(t *testing.T) {
		_, ok := selectPrimaryValue(nil, "m")
		if ok {
			t.Error("ok true for absent metric; want false")
		}
	})
}

func TestClampFraction(t *testing.T) {
	cases := map[float64]float64{-0.1: 0, 0: 0, 0.5: 0.5, 1: 1, 1.2: 1}
	for in, want := range cases {
		if got := clampFraction(in); got != want {
			t.Errorf("clampFraction(%v) = %v, want %v", in, got, want)
		}
	}
}

// distillText is the test helper: parse + distill at a fixed time.
func distillText(text string) ir.TargetHealthSnapshot {
	return distill(parsePromText(strings.NewReader(text)), time.Unix(1000, 0))
}
