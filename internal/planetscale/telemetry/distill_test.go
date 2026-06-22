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

// pgExposition is a representative Postgres scrape: shared pod CPU/mem
// percentages, the PG-flavor `planetscale_volume_*` storage names (no
// `vttablet_` prefix), and `planetscale_postgres_replica_lag_seconds`. A
// single pod with no tablet labels exercises the (3) single-series selection
// fallback. The MySQL/Vitess volume names are present too, as DECOYS, to prove
// the PG table reads the right series.
const pgExposition = `
planetscale_pods_cpu_util_percentages 62
planetscale_pods_mem_util_percentages 55
planetscale_volume_available_bytes 40000000000
planetscale_volume_capacity_bytes 160000000000
planetscale_vttablet_volume_available_bytes 999
planetscale_vttablet_volume_capacity_bytes 999
planetscale_postgres_replica_lag_seconds 2
planetscale_mysql_replica_lag_seconds 99
`

// TestDistill_PostgresEngine_UsesPGNames pins Phase 3(c): a PG target reads
// the `planetscale_volume_*` / `planetscale_postgres_*` series, NOT the Vitess
// decoys, and leaves connections unobserved (the honest PG-conn gap).
func TestDistill_PostgresEngine_UsesPGNames(t *testing.T) {
	snap := distillTextWith(pgExposition, "postgres")

	if !snap.CPUKnown || snap.CPUUtil != 0.62 {
		t.Errorf("CPU = %v known=%v, want 0.62 true", snap.CPUUtil, snap.CPUKnown)
	}
	if !snap.MemKnown || snap.MemUtil != 0.55 {
		t.Errorf("Mem = %v known=%v, want 0.55 true", snap.MemUtil, snap.MemKnown)
	}
	// Storage from the PG names: 1 - 40e9/160e9 = 0.75; the vttablet decoy
	// (999/999 → 0) must NOT win.
	if !snap.StorageKnown || snap.StorageUtil != 0.75 {
		t.Errorf("Storage = %v known=%v, want 0.75 true (PG volume names)", snap.StorageUtil, snap.StorageKnown)
	}
	if snap.StorageAvailableBytes != 40000000000 || snap.StorageCapacityBytes != 160000000000 {
		t.Errorf("raw storage = %d/%d, want 40e9/160e9", snap.StorageAvailableBytes, snap.StorageCapacityBytes)
	}
	// Lag from the postgres series (2), not the mysql decoy (99).
	if !snap.LagKnown || snap.ReplicaLagSeconds != 2 {
		t.Errorf("Lag = %v known=%v, want 2 true (postgres lag name)", snap.ReplicaLagSeconds, snap.LagKnown)
	}
	// Connections: PG table leaves these unset → honest unobserved.
	if snap.ConnKnown {
		t.Error("ConnKnown true; want false (PG conn metrics intentionally unmapped)")
	}
}

// TestDistill_MySQLDefault_IgnoresPGVolumeNames is the mirror: the default
// (MySQL/Vitess) table must NOT read the PG `planetscale_volume_*` names, so a
// scrape that has ONLY the PG volume series leaves storage unobserved.
func TestDistill_MySQLDefault_IgnoresPGVolumeNames(t *testing.T) {
	const text = `
planetscale_volume_available_bytes 40000000000
planetscale_volume_capacity_bytes 160000000000
`
	snap := distillTextWith(text, "mysql")
	if snap.StorageKnown {
		t.Error("StorageKnown true under MySQL table reading PG volume names; want false")
	}
	// metricNamesFor: empty and unknown engines fall back to the MySQL table.
	if metricNamesFor("") != mysqlMetricNames {
		t.Error(`metricNamesFor("") did not fall back to mysqlMetricNames`)
	}
	if metricNamesFor("planetscale") != mysqlMetricNames {
		t.Error(`metricNamesFor("planetscale") did not map to mysqlMetricNames`)
	}
	if metricNamesFor("postgres") != postgresMetricNames {
		t.Error(`metricNamesFor("postgres") did not map to postgresMetricNames`)
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

// distillText is the test helper: parse + distill at a fixed time using the
// MySQL/Vitess metric names (the default, confirmed surface).
func distillText(text string) ir.TargetHealthSnapshot {
	return distill(parsePromText(strings.NewReader(text)), mysqlMetricNames, time.Unix(1000, 0))
}

// distillTextWith is the test helper variant that selects the metric-name
// table by engine (Phase 3 — exercises the Postgres name set).
func distillTextWith(text, engine string) ir.TargetHealthSnapshot {
	return distill(parsePromText(strings.NewReader(text)), metricNamesFor(engine), time.Unix(1000, 0))
}
