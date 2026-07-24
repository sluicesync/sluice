// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"testing"
	"time"
)

// pgVolumeSamples reproduces the REAL PlanetScale-Postgres volume exposition
// observed live 2026-07-24 on branch czksg8wxwz9g: three `hzinstance` pods,
// each emitting both volume metrics, discriminated ONLY by
// `planetscale_role` — no `planetscale_tablet_type`, no
// `planetscale_container`. The replicas are fuller than the primary, which is
// the shape that motivates the worst-pod signal.
func pgVolumeSamples() []promSample {
	pod := func(cell, role string, avail float64) []promSample {
		l := map[string]string{
			"cluster":                        "us-east-2",
			"planetscale_cell":               cell,
			"planetscale_component":          "hzinstance",
			"planetscale_database_branch_id": "czksg8wxwz9g",
			"planetscale_pod":                "hzi-czksg8wxwz9g-" + cell,
			"planetscale_role":               role,
		}
		cp := func() map[string]string {
			m := make(map[string]string, len(l))
			for k, v := range l {
				m[k] = v
			}
			return m
		}
		return []promSample{
			{name: "planetscale_volume_available_bytes", labels: cp(), value: avail},
			{name: "planetscale_volume_capacity_bytes", labels: cp(), value: 10737418240},
		}
	}
	out := make([]promSample, 0, 6)
	out = append(out, pod("aws_useast1c_3", "primary", 10133684224)...) // 5.62% used
	out = append(out, pod("aws_useast1a_3", "replica", 10100211712)...) // 5.93% used
	out = append(out, pod("aws_useast1b_3", "replica", 10100211712)...)
	return out
}

// TestDistill_PGVolume_PrimarySelectedByRoleLabel is the regression pin for
// the shipped gap: before `planetscale_role` was a recognised selector, a
// 3-pod PG branch fell through every tier of the cascade to "refuse to
// guess", so StorageKnown stayed false and --notify-storage-util could never
// fire on a Postgres target.
func TestDistill_PGVolume_PrimarySelectedByRoleLabel(t *testing.T) {
	snap := distill(pgVolumeSamples(), postgresMetricNames, time.Now())

	if !snap.StorageKnown {
		t.Fatal("StorageKnown = false; want true — the PG primary pod is identified by planetscale_role=\"primary\"")
	}
	if got, want := snap.StorageAvailableBytes, int64(10133684224); got != want {
		t.Errorf("StorageAvailableBytes = %d; want %d (the PRIMARY pod's, not a replica's)", got, want)
	}
	// 1 - 10133684224/10737418240 = 0.05623...
	if got := snap.StorageUtil; got < 0.056 || got > 0.0563 {
		t.Errorf("StorageUtil = %v; want ~0.0562 (the primary's)", got)
	}
}

// TestDistill_PGVolume_WorstIsTheFullestPod pins that the worst signal picks
// the fullest pod — here a REPLICA — and that it stays a separate value from
// the primary's rather than overwriting it.
func TestDistill_PGVolume_WorstIsTheFullestPod(t *testing.T) {
	snap := distill(pgVolumeSamples(), postgresMetricNames, time.Now())

	if !snap.StorageWorstKnown {
		t.Fatal("StorageWorstKnown = false; want true")
	}
	if got, want := snap.StorageAvailableWorstBytes, int64(10100211712); got != want {
		t.Errorf("StorageAvailableWorstBytes = %d; want %d (a REPLICA — the fullest pod)", got, want)
	}
	if snap.StorageUtilWorst <= snap.StorageUtil {
		t.Errorf("StorageUtilWorst (%v) <= StorageUtil (%v); want the worst STRICTLY fuller here — "+
			"if these collapse to one value the signals have been merged, which is the thing the split exists to prevent",
			snap.StorageUtilWorst, snap.StorageUtil)
	}
}

// TestSelectWorstVolume_JoinsPerPodNotAcrossPods is the correctness crux.
// Taking min(available) and max(capacity) INDEPENDENTLY across series would
// pair a small pod's free bytes with a big pod's capacity and manufacture a
// utilisation that no pod actually has. Two pods with different volume SIZES
// make that mistake observable.
func TestSelectWorstVolume_JoinsPerPodNotAcrossPods(t *testing.T) {
	mk := func(podID string, avail, capac float64) []promSample {
		l1 := map[string]string{"planetscale_pod": podID}
		l2 := map[string]string{"planetscale_pod": podID}
		return []promSample{
			{name: "avail", labels: l1, value: avail},
			{name: "capac", labels: l2, value: capac},
		}
	}
	samples := make([]promSample, 0, 4)
	// small pod: 1 GiB volume, 90% used (0.1 GiB free) — the FULLEST.
	samples = append(samples, mk("small", 100, 1000)...)
	// big pod: 100 GiB volume, 50% used (50 GiB free).
	samples = append(samples, mk("big", 50000, 100000)...)

	avail, capac, ok := selectWorstVolume(samples, "avail", "capac")
	if !ok {
		t.Fatal("ok = false; want true")
	}
	if avail != 100 || capac != 1000 {
		t.Errorf("selectWorstVolume = (%v, %v); want (100, 1000) — the SMALL pod's own pair. "+
			"Getting (100, 100000) would mean available and capacity were minimised/maximised "+
			"independently and crossed pods.", avail, capac)
	}
}

// TestSelectWorstVolume_HonestlyUnobserved pins the refuse-rather-than-guess
// contract across the ways the reduction can legitimately fail.
func TestSelectWorstVolume_HonestlyUnobserved(t *testing.T) {
	l := func() map[string]string { return map[string]string{"planetscale_pod": "p1"} }
	cases := []struct {
		name    string
		samples []promSample
		aName   string
		cName   string
	}{
		{"metric name unset (engine exposes none)", pgVolumeSamples(), "", "planetscale_volume_capacity_bytes"},
		{"capacity absent entirely", []promSample{{name: "avail", labels: l(), value: 1}}, "avail", "capac"},
		{"available absent entirely", []promSample{{name: "capac", labels: l(), value: 1}}, "avail", "capac"},
		{"no pod exposes BOTH", []promSample{
			{name: "avail", labels: map[string]string{"planetscale_pod": "a"}, value: 1},
			{name: "capac", labels: map[string]string{"planetscale_pod": "b"}, value: 2},
		}, "avail", "capac"},
		{"capacity non-positive", []promSample{
			{name: "avail", labels: l(), value: 1},
			{name: "capac", labels: l(), value: 0},
		}, "avail", "capac"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, ok := selectWorstVolume(tc.samples, tc.aName, tc.cName); ok {
				t.Error("ok = true; want false — an unreducible volume set must stay honestly unobserved, never guess")
			}
		})
	}
}

// TestSelectPrimaryValue_VitessUnaffectedByRoleTier pins that adding the PG
// role tier did not disturb the Vitess selection: an exact
// component=vttablet + tablet_type=primary match must still win, including
// when other pods carry a role label.
func TestSelectPrimaryValue_VitessUnaffectedByRoleTier(t *testing.T) {
	samples := []promSample{
		{name: "m", labels: map[string]string{"planetscale_component": "vttablet", "planetscale_tablet_type": "replica"}, value: 1},
		{name: "m", labels: map[string]string{"planetscale_role": "primary"}, value: 2},
		{name: "m", labels: map[string]string{"planetscale_component": "vttablet", "planetscale_tablet_type": "primary"}, value: 3},
	}
	got, ok := selectPrimaryValue(samples, "m", "")
	if !ok || got != 3 {
		t.Errorf("selectPrimaryValue = (%v, %v); want (3, true) — the exact vttablet primary must outrank a role-tagged series", got, ok)
	}
}

// TestSelectWorstVolume_MySQLShapeAlsoReduces pins engine parity: the Vitess
// vttablet volume metrics fan out per tablet too, so the worst-pod reduction
// must work there as well rather than being a Postgres-only feature.
func TestSelectWorstVolume_MySQLShapeAlsoReduces(t *testing.T) {
	mk := func(tt string, avail float64) []promSample {
		l := map[string]string{"planetscale_component": "vttablet", "planetscale_tablet_type": tt, "planetscale_shard": "-"}
		l2 := map[string]string{"planetscale_component": "vttablet", "planetscale_tablet_type": tt, "planetscale_shard": "-"}
		return []promSample{
			{name: mysqlMetricNames.volAvailableByte, labels: l, value: avail},
			{name: mysqlMetricNames.volCapacityByte, labels: l2, value: 1000},
		}
	}
	samples := make([]promSample, 0, 4)
	samples = append(samples, mk("primary", 800)...) // 20% used
	samples = append(samples, mk("replica", 300)...) // 70% used — fullest

	snap := distill(samples, mysqlMetricNames, time.Now())
	if !snap.StorageWorstKnown {
		t.Fatal("StorageWorstKnown = false on the MySQL/Vitess shape; want true (engine parity)")
	}
	if got := snap.StorageAvailableWorstBytes; got != 300 {
		t.Errorf("StorageAvailableWorstBytes = %d; want 300 (the fullest tablet)", got)
	}
	if !snap.StorageKnown || snap.StorageAvailableBytes != 800 {
		t.Errorf("primary reading disturbed: StorageKnown=%v available=%d; want true/800",
			snap.StorageKnown, snap.StorageAvailableBytes)
	}
}

// TestDistill_ReplicaLag_WorstAcrossReplicas_BothEngines pins the lag repair.
// Neither engine emits a PRIMARY lag series — PG tags every series
// role="replica", Vitess tags every one tablet_type="replica" — so the old
// primary-selection cascade found no primary, fell past the single-series
// tier on any branch with >=2 replicas, and refused. LagKnown was therefore
// silently false on multi-replica branches of BOTH engines.
func TestDistill_ReplicaLag_WorstAcrossReplicas_BothEngines(t *testing.T) {
	cases := []struct {
		name   string
		names  metricNames
		metric string
		labels []map[string]string
	}{
		{
			name:   "postgres (role=replica)",
			names:  postgresMetricNames,
			metric: "planetscale_postgres_replica_lag_seconds",
			labels: []map[string]string{
				{"planetscale_pod": "a", "planetscale_role": "replica"},
				{"planetscale_pod": "b", "planetscale_role": "replica"},
			},
		},
		{
			name:   "vitess (tablet_type=replica)",
			names:  mysqlMetricNames,
			metric: "planetscale_mysql_replica_lag_seconds",
			labels: []map[string]string{
				{"planetscale_pod": "a", "planetscale_tablet_type": "replica"},
				{"planetscale_pod": "b", "planetscale_tablet_type": "replica"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			samples := []promSample{
				{name: tc.metric, labels: tc.labels[0], value: 3},
				{name: tc.metric, labels: tc.labels[1], value: 41.5}, // the furthest behind
			}
			snap := distill(samples, tc.names, time.Now())
			if !snap.LagKnown {
				t.Fatal("LagKnown = false; want true — two replica series must reduce, not refuse")
			}
			if snap.ReplicaLagSeconds != 41.5 {
				t.Errorf("ReplicaLagSeconds = %v; want 41.5 (the WORST replica, not the first or an average)",
					snap.ReplicaLagSeconds)
			}
		})
	}
}

// TestSelectWorstOf_SingleSeriesIsUnchanged pins that the max reduction
// degenerates to the old value on a single-replica branch, so this is a
// strict repair rather than a behaviour change where lag already resolved.
func TestSelectWorstOf_SingleSeriesIsUnchanged(t *testing.T) {
	samples := []promSample{{name: "m", labels: map[string]string{"p": "only"}, value: 7.25}}
	got, ok := selectWorstOf(samples, "m")
	if !ok || got != 7.25 {
		t.Errorf("selectWorstOf = (%v, %v); want (7.25, true)", got, ok)
	}
	if _, ok := selectWorstOf(samples, ""); ok {
		t.Error("unset metric name returned ok=true; want false (honest unobserved)")
	}
	if _, ok := selectWorstOf(samples, "absent"); ok {
		t.Error("absent metric returned ok=true; want false")
	}
}

// TestDistill_PGConnections_ResolveOnTheRealShapes pins the two PG connection
// series at the shapes the live endpoint emits: active connections as a
// single unlabelled series (single-series tier), and max_connections fanned
// per pod with planetscale_role (the role tier added for storage).
func TestDistill_PGConnections_ResolveOnTheRealShapes(t *testing.T) {
	samples := []promSample{
		{name: "planetscale_edge_postgres_active_connections", labels: map[string]string{}, value: 4},
		{name: "planetscale_postgres_settings_max_connections", labels: map[string]string{"planetscale_pod": "a", "planetscale_role": "replica"}, value: 25},
		{name: "planetscale_postgres_settings_max_connections", labels: map[string]string{"planetscale_pod": "b", "planetscale_role": "replica"}, value: 25},
		{name: "planetscale_postgres_settings_max_connections", labels: map[string]string{"planetscale_pod": "c", "planetscale_role": "primary"}, value: 25},
	}
	snap := distill(samples, postgresMetricNames, time.Now())
	if !snap.ConnKnown {
		t.Fatal("ConnKnown = false; want true — both PG connection shapes are resolvable")
	}
	if snap.ActiveConnections != 4 || snap.MaxConnections != 25 {
		t.Errorf("connections = %d/%d; want 4/25 (max_connections from the PRIMARY pod)",
			snap.ActiveConnections, snap.MaxConnections)
	}
}
