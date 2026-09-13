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

// TestDistillPGExposition_ReadsThePgBouncerFrontDoor is the PlanetScale
// Postgres half of the front-door signal.
//
// The two platforms fill the same role with very different machinery — Neki
// with a fleet of router pods on their own tier, Postgres with a PgBouncer
// sidecar riding on each database pod — and the operator's question is the same
// for both: is the hop in front of my database the thing that is saturated?
// So one pair of fields answers it, and this test is what keeps the Postgres
// arm honest.
//
// Three ways to be wrong here, each of which produces a plausible number:
//
//   - CPU read from the pod metric's pgbouncer slice instead of PgBouncer's
//     OWN per-peer metric. PgBouncer is single-threaded per peer process, so a
//     peer at 100% is saturated while the pod it rides on is nearly idle; the
//     pod-level slice understates exactly the condition worth alerting on.
//     Measured on this fixture, which carries BOTH series for the same pods:
//     swapping to the pod slice reports 0.0086 where the peer is at 0.725, an
//     84x understatement that reads as a perfectly healthy front door.
//   - MEMORY read without the container filter, which picks up the database
//     container sharing the pod — a much larger number about a different
//     process.
//   - The queue wait dropped, which is the one signal here that is not an
//     inference.
//
// The fixture is a trimmed live capture with three values raised so the
// populations separate: the primary's PgBouncer peer to 72.5% CPU (its
// replicas stay near zero, so max ≠ mean), the postgres containers' memory to
// 88% (so a missing container filter is visible against PgBouncer's ~38%), and
// one pod's maxwait to 3.5s.
func TestDistillPGExposition_ReadsThePgBouncerFrontDoor(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("testdata/pg_pgbouncer_exposition.txt")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	samples := parsePromText(strings.NewReader(string(raw)))
	if len(samples) == 0 {
		t.Fatal("fixture parsed to zero samples — the assertions below would be vacuous")
	}

	// Fixture sanity. Each assertion below distinguishes two populations, so
	// each of those populations has to actually be present and separated.
	var sawBusyPeer, sawIdlePeer, sawPgBouncerMem, sawDatabaseMem, sawWait bool
	for _, s := range samples {
		switch s.name {
		case "planetscale_pgbouncer_cpu_util_per_peer_percentages":
			if s.value > 50 {
				sawBusyPeer = true
			} else {
				sawIdlePeer = true
			}
		case "planetscale_pods_mem_util_percentages":
			switch s.label(labelContainer) {
			case "pgbouncer":
				sawPgBouncerMem = true
			case "postgres":
				sawDatabaseMem = true
			}
		case "planetscale_pgbouncer_pools_client_maxwait_seconds":
			if s.value > 1 {
				sawWait = true
			}
		}
	}
	if !sawBusyPeer || !sawIdlePeer || !sawPgBouncerMem || !sawDatabaseMem || !sawWait {
		t.Fatalf("fixture is not discriminating (busyPeer=%v idlePeer=%v pgbouncerMem=%v databaseMem=%v wait=%v)",
			sawBusyPeer, sawIdlePeer, sawPgBouncerMem, sawDatabaseMem, sawWait)
	}

	snap := distill(samples, postgresMetricNames, time.Now())

	if !snap.FrontDoorCPUKnown {
		t.Fatal("FrontDoorCPUKnown is false on a PlanetScale Postgres exposition carrying three PgBouncer " +
			"CPU series — the front door reads as unobserved and a saturated pooler stays invisible")
	}
	if got, want := snap.FrontDoorCPUUtil, 0.725; math.Abs(got-want) > 1e-9 {
		t.Fatalf("FrontDoorCPUUtil = %v, want %v (the busiest PgBouncer peer). A mean over the three peers "+
			"would read ~0.242, and the pod metric's pgbouncer slice is a different measurement entirely", got, want)
	}

	if !snap.FrontDoorMemKnown {
		t.Fatal("FrontDoorMemKnown is false although the fixture carries pgbouncer-container memory series")
	}
	if got := snap.FrontDoorMemUtil; math.Abs(got-0.384033203125) > 1e-9 {
		if got > 0.8 {
			t.Fatalf("FrontDoorMemUtil = %v — that is the DATABASE container's memory (0.88), not PgBouncer's "+
				"(~0.384). The memory selector is not filtering on planetscale_container", got)
		}
		t.Fatalf("FrontDoorMemUtil = %v, want ~0.384 (the busiest pgbouncer container)", got)
	}

	if !snap.FrontDoorWaitKnown {
		t.Fatal("FrontDoorWaitKnown is false although the fixture has a pod whose oldest client has waited " +
			"3.5s — this is the least ambiguous saturation signal the platform publishes and it must not be dropped")
	}
	if got := snap.FrontDoorWaitSeconds; math.Abs(got-3.5) > 1e-9 {
		t.Fatalf("FrontDoorWaitSeconds = %v, want 3.5 (the longest-waiting client across pods)", got)
	}

	// A duration, deliberately NOT run through clampFraction — a wait longer
	// than one second is the entire point of the signal.
	if snap.FrontDoorWaitSeconds <= 1 {
		t.Fatalf("FrontDoorWaitSeconds = %v: a wait above 1 has been clamped as though it were a fraction, "+
			"which would cap every real queue stall at one second", snap.FrontDoorWaitSeconds)
	}
}

// TestFrontDoorSurfacesAreDisjoint is the premise check the metricNames doc
// promises, and the reason one metric-name table can safely describe both
// front-door shapes.
//
// Neki is Postgres by engine, so a Neki branch and a PlanetScale Postgres
// branch both read [postgresMetricNames]. The cascade in [selectFrontDoorCPU]
// tries the router-pod arm and then the pooler arm, which is only sound
// because at most one can answer. That is a fact about two live expositions,
// not about this code, so it is asserted rather than described: if PlanetScale
// ever ships a branch carrying both, the cascade silently starts preferring
// one and this fails first.
func TestFrontDoorSurfacesAreDisjoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		file        string
		wantRouter  bool
		wantPooler  bool
		description string
	}{
		{
			name:        "neki branch has routers and no pooler",
			file:        "testdata/neki_exposition.txt",
			wantRouter:  true,
			wantPooler:  false,
			description: "a Neki branch fronts its shards with router pods; there is no PgBouncer in the path",
		},
		{
			name:        "planetscale postgres branch has a pooler and no routers",
			file:        "testdata/pg_pgbouncer_exposition.txt",
			wantRouter:  false,
			wantPooler:  true,
			description: "a PlanetScale Postgres branch fronts its primary with a PgBouncer sidecar; there are no router pods",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			samples := parsePromText(strings.NewReader(string(raw)))
			if len(samples) == 0 {
				t.Fatalf("fixture %s parsed to zero samples", tc.file)
			}

			var haveRouter, havePooler bool
			for _, s := range samples {
				if s.label(labelRouter) != "" {
					haveRouter = true
				}
				if strings.Contains(s.name, "pgbouncer") || s.label(labelContainer) == "pgbouncer" {
					havePooler = true
				}
			}
			if haveRouter != tc.wantRouter || havePooler != tc.wantPooler {
				t.Fatalf("%s: router series present=%v (want %v), pooler series present=%v (want %v) — %s",
					tc.file, haveRouter, tc.wantRouter, havePooler, tc.wantPooler, tc.description)
			}
			if haveRouter && havePooler {
				t.Fatal("BOTH front-door shapes are present in one exposition. The cascade in " +
					"selectFrontDoorCPU assumes at most one answers; with both it silently prefers the " +
					"router arm and the pooler's reading is never published")
			}
		})
	}
}
