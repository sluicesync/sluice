// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestDistillNekiExposition_PicksThePrimaryNotAnAverage settles a question that
// was OPEN and unverified — which is worse than either answer, because a CPU
// alert nobody trusts is a CPU alert nobody acts on.
//
// `metrics-watch` works against a PlanetScale Neki branch unchanged (verified
// live: storage, capacity, lag and connections all read correctly). What could
// not be checked live was WHICH pod's CPU it reports, because the only test ran
// against an IDLE branch where every pod agrees. Under load they do not agree
// at all: measured on a real branch, the primary sat at 100% while a replica
// sat at 0.02% and the three routers at 16–19%.
//
// THIS TEST FOUND A REAL DEFECT, and the first version of it passed anyway —
// which is why the fixture is shaped the way it is. The selector matched on
// `planetscale_container="postgres"` alone and returned whichever pod the
// exposition listed first. On the real capture that was a REPLICA. It went
// unnoticed because the branch was thrashing and every pod sat near 100%, so
// the wrong answer equalled the right one.
//
// The fixture therefore keeps the REAL label structure — captured from a live
// Neki branch, which is what proves primary and replica genuinely share
// `container="postgres"` while routers carry no role label at all — with the
// replica CPU VALUES edited to 3.5 so the roles are distinguishable. That edit
// is the point: a fixture whose pods all report the same number cannot tell a
// correct selection from a lucky one.
//
//	primary  100    ← the only correct answer
//	replica  3.5    ← picking a replica (the defect)
//	routers  16-19  ← picking a router, or averaging
//
// SCOPE, enumerated rather than implied. [selectPrimaryValue] has six callers
// — cpu, mem, volume-available, volume-capacity, active-conns, max-conns — and
// all six share the container tier, but the tier can only fire on a series that
// CARRIES planetscale_container, which on a Neki exposition is cpu and mem
// alone. The volume and replica-lag families carry no container label at all,
// so they fell through to the role tier and were already selecting the primary.
// That is why the live check of storage, capacity and lag agreed with the
// console while CPU quietly did not. This fixture is a trimmed subset of a real
// exposition, so the assertion below covers CPU and claims nothing about mem.
func TestDistillNekiExposition_PicksThePrimaryNotAnAverage(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("testdata/neki_exposition.txt")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	samples := parsePromText(strings.NewReader(string(raw)))
	if len(samples) == 0 {
		t.Fatal("fixture parsed to zero samples — the assertions below would be vacuous")
	}

	// Sanity: the fixture really is discriminating. If the primary and the
	// routers ever converged, this test would pass for the wrong reason.
	var sawPrimary100, sawRouterMid, sawReplicaLow bool
	for _, s := range samples {
		if s.name != "planetscale_pods_cpu_util_percentages" {
			continue
		}
		switch {
		case s.label("planetscale_role") == "primary" && s.value > 99:
			sawPrimary100 = true
		case s.label("planetscale_router") != "" && s.value > 10 && s.value < 20:
			sawRouterMid = true
		case s.label("planetscale_role") == "replica" && s.value < 10:
			sawReplicaLow = true
		}
	}
	if !sawPrimary100 || !sawRouterMid || !sawReplicaLow {
		t.Fatalf("fixture is not discriminating (primary100=%v routerMid=%v replicaLow=%v) — "+
			"a selection bug could pass unnoticed", sawPrimary100, sawRouterMid, sawReplicaLow)
	}

	snap := distill(samples, pgMetricNames(), time.Now())

	// The primary is pegged; a replica reports 3.5; the routers 16-19.
	if snap.CPUUtil < 0.9 {
		t.Fatalf("distilled CPU = %v, want ~1.0 (the primary at 100%%). A replica in this exposition "+
			"reports 3.5%% and the routers 16-19%%, so any value below 0.9 means the selector took "+
			"a replica, a router, or an average — and a CPU alert on a Neki branch would then "+
			"under-report the pod that is actually saturated", snap.CPUUtil)
	}
}

// pgMetricNames returns the Postgres-flavoured metric-name set the Neki branch
// exposes. Neki is Postgres by wire protocol and engine, and its exposition
// carries the planetscale_postgres_* / planetscale_pods_* families rather than
// the Vitess ones — which is precisely why metrics-watch needed no changes to
// work against it.
func pgMetricNames() metricNames {
	return postgresMetricNames
}
