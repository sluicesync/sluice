// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// TestDistillNekiExposition_ReportsTheRouterSeparately pins the routing-layer
// signal, which exists because an operator watched their routing layer sit
// pegged at 100% on a live PlanetScale Neki branch while `metrics-watch` had
// no field to report it in. Throughput collapsed; every number sluice could
// show was about the database, and the database was not the problem.
//
// Three properties, and each has a way of being wrong that looks fine:
//
//   - It must read the ROUTER pods, not the database ones. A selector that
//     forgot the label filter returns the primary's 100% — a plausible-looking
//     number that is about the wrong machine entirely.
//   - It must take the BUSIEST router, not an average. Connections spread
//     across router pods, so one pegged pod stalls its share of traffic while
//     a mean over three dilutes it.
//   - It must not disturb the database CPU/mem it sits beside. The two are
//     separate signals on purpose; folding them answers "something is
//     saturated" and never "which".
//
// The fixture keeps the live label structure — which is what proves the
// routers carry `planetscale_router` and the tablets do not — with one router
// CPU raised to 45 so max (45) and mean (~26.4) are far apart, and both well
// clear of the primary's 100. A fixture whose routers all report the same
// number cannot tell a correct reduction from a lucky one.
func TestDistillNekiExposition_ReportsTheRouterSeparately(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("testdata/neki_exposition.txt")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	samples := parsePromText(strings.NewReader(string(raw)))
	if len(samples) == 0 {
		t.Fatal("fixture parsed to zero samples — the assertions below would be vacuous")
	}

	// Fixture sanity: the three CPU populations must be separable, or every
	// assertion below could pass for the wrong reason.
	var routerCPU []float64
	var sawTabletCPU bool
	for _, s := range samples {
		if s.name != "planetscale_pods_cpu_util_percentages" {
			continue
		}
		if s.label(labelRouter) != "" {
			routerCPU = append(routerCPU, s.value)
			continue
		}
		if s.value > 99 {
			sawTabletCPU = true
		}
	}
	if len(routerCPU) < 3 || !sawTabletCPU {
		t.Fatalf("fixture is not discriminating: %d router CPU series (want >=3), tablet-at-100 seen=%v",
			len(routerCPU), sawTabletCPU)
	}
	var sum, busiest float64
	for _, v := range routerCPU {
		sum += v
		if v > busiest {
			busiest = v
		}
	}
	mean := sum / float64(len(routerCPU))
	if math.Abs(busiest-mean) < 10 {
		t.Fatalf("router CPU max (%v) and mean (%v) are within 10 points — this test could not tell a "+
			"max reduction from an average one", busiest, mean)
	}

	snap := distill(samples, pgMetricNames(), time.Now())

	if !snap.RouterCPUKnown {
		t.Fatal("RouterCPUKnown is false on an exposition that carries three router CPU series — the " +
			"routing layer reads as unobserved, and a saturated router stays invisible exactly as it was")
	}
	if got, want := snap.RouterCPUUtil, busiest/100.0; math.Abs(got-want) > 1e-9 {
		switch {
		case math.Abs(got-mean/100.0) < 1e-9:
			t.Fatalf("RouterCPUUtil = %v, the MEAN of the router pods; want %v, the busiest. An average over "+
				"three pods dilutes one pegged pod to a third of its real reading", got, want)
		case got > 0.9:
			t.Fatalf("RouterCPUUtil = %v — that is the database PRIMARY's reading, not a router's. The "+
				"selector is not filtering on %s, so the routing-layer series reports the wrong machine",
				got, labelRouter)
		default:
			t.Fatalf("RouterCPUUtil = %v, want %v (the busiest router pod)", got, want)
		}
	}

	if !snap.RouterMemKnown {
		t.Fatal("RouterMemKnown is false although the fixture carries router memory series")
	}
	// The fixture's only >50% memory series is a database pod's, so a missing
	// filter shows up here as a high reading rather than a subtly wrong one.
	if snap.RouterMemUtil > 0.5 {
		t.Fatalf("RouterMemUtil = %v — the fixture's routers report ~0.26 and its database pod ~0.89, so a "+
			"value this high means the memory selector took a database pod", snap.RouterMemUtil)
	}

	// A Neki branch runs no PgBouncer at all, so the whole pooler group must
	// read as unobserved. This is the mirror of the assertion the PgBouncer
	// test makes about the router fields, and it matters for the same reason:
	// the two signals carry different operational meaning, so neither may
	// stand in for the other. A fabricated zero wait would be the worst of
	// them — it asserts nobody is ever queueing at a component that is not
	// there.
	if snap.PgBouncerCPUKnown || snap.PgBouncerCPUUtil != 0 ||
		snap.PgBouncerMemKnown || snap.PgBouncerMemUtil != 0 ||
		snap.PgBouncerWaitKnown || snap.PgBouncerClientWaitSeconds != 0 {
		t.Errorf("pooler fields populated on a Neki exposition: cpu=%v (known=%v) mem=%v (known=%v) "+
			"wait=%v (known=%v) — Neki runs no PgBouncer, and the router's readings must not be reported "+
			"as a pooler's",
			snap.PgBouncerCPUUtil, snap.PgBouncerCPUKnown,
			snap.PgBouncerMemUtil, snap.PgBouncerMemKnown,
			snap.PgBouncerClientWaitSeconds, snap.PgBouncerWaitKnown)
	}

	// And the database's own signals are untouched by any of it.
	if !snap.CPUKnown || snap.CPUUtil < 0.9 {
		t.Fatalf("CPUUtil = %v (known=%v) — adding the router signal must not disturb the primary's own "+
			"reading, which this exposition puts at 100%%", snap.CPUUtil, snap.CPUKnown)
	}
}

// TestDistillWithoutRouters_LeavesTheRouterUnobserved is the other half of
// the honesty contract, and the reason the fields carry their own *Known flags.
//
// A Vitess/MySQL branch exposes no router series at all (verified live: the
// `planetscale_router` label does not appear on that surface). The right answer
// there is "unobserved" — flags false, values 0 — never a fabricated 0.0 that a
// dashboard would render as a perfectly idle routing layer that does not exist.
func TestDistillWithoutRouters_LeavesTheRouterUnobserved(t *testing.T) {
	t.Parallel()

	const exposition = `planetscale_pods_cpu_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="primary"} 42
planetscale_pods_mem_util_percentages{planetscale_component="vttablet",planetscale_tablet_type="primary"} 55
`
	samples := parsePromText(strings.NewReader(exposition))
	if len(samples) != 2 {
		t.Fatalf("fixture parsed to %d samples, want 2", len(samples))
	}

	snap := distill(samples, mysqlMetricNames, time.Now())

	if snap.RouterCPUKnown || snap.RouterCPUUtil != 0 {
		t.Errorf("RouterCPUUtil = %v (known=%v) on an exposition with no router series at all — a platform "+
			"without a routing layer must read as unobserved, not as an idle one",
			snap.RouterCPUUtil, snap.RouterCPUKnown)
	}
	if snap.RouterMemKnown || snap.RouterMemUtil != 0 {
		t.Errorf("RouterMemUtil = %v (known=%v) on an exposition with no router series at all",
			snap.RouterMemUtil, snap.RouterMemKnown)
	}
	// The tablet readings still resolve — the new selector must not have
	// eaten the series it declines to claim.
	if !snap.CPUKnown || math.Abs(snap.CPUUtil-0.42) > 1e-9 {
		t.Errorf("CPUUtil = %v (known=%v), want 0.42", snap.CPUUtil, snap.CPUKnown)
	}
}
