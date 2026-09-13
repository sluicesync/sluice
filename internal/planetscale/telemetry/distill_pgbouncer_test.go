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

// TestDistillPGExposition_ReadsThePgBouncerPooler pins the connection-pooler
// signal on PlanetScale Postgres.
//
// It is deliberately a SEPARATE signal from the routing layer the Neki test
// covers, not a second instance of one "front door" reading, because the two
// differ in the way that matters operationally: a Neki router terminates every
// connection sluice makes, while a PgBouncer terminates none of them — sluice
// connects to PlanetScale Postgres directly, and logical replication cannot
// traverse a transaction pooler at all. So these numbers describe the
// OPERATOR.S OWN application traffic against a database sluice is loading,
// which is worth watching during a migration and worth never confusing with
// sluice.s own throughput.
//
// Three ways to be wrong here, each of which produces a plausible number:
//
//   - CPU read from the pod metric's pgbouncer slice instead of PgBouncer's
//     OWN per-peer metric. PgBouncer is single-threaded per peer process, so a
//     peer at 100% is saturated while the pod it rides on is nearly idle; the
//     pod-level slice understates exactly the condition worth alerting on.
//     Measured on this fixture, which carries BOTH series for the same pods:
//     swapping to the pod slice reports 0.0086 where the peer is at 0.725, an
//     84x understatement that reads as a perfectly healthy pooler.
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
func TestDistillPGExposition_ReadsThePgBouncerPooler(t *testing.T) {
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

	if !snap.PgBouncerCPUKnown {
		t.Fatal("PgBouncerCPUKnown is false on a PlanetScale Postgres exposition carrying three PgBouncer " +
			"CPU series — the pooler reads as unobserved and a saturated one stays invisible")
	}
	if got, want := snap.PgBouncerCPUUtil, 0.725; math.Abs(got-want) > 1e-9 {
		t.Fatalf("PgBouncerCPUUtil = %v, want %v (the busiest PgBouncer peer). A mean over the three peers "+
			"would read ~0.242, and the pod metric's pgbouncer slice is a different measurement entirely", got, want)
	}

	if !snap.PgBouncerMemKnown {
		t.Fatal("PgBouncerMemKnown is false although the fixture carries pgbouncer-container memory series")
	}
	if got := snap.PgBouncerMemUtil; math.Abs(got-0.384033203125) > 1e-9 {
		if got > 0.8 {
			t.Fatalf("PgBouncerMemUtil = %v — that is the DATABASE container's memory (0.88), not PgBouncer's "+
				"(~0.384). The memory selector is not filtering on planetscale_container", got)
		}
		t.Fatalf("PgBouncerMemUtil = %v, want ~0.384 (the busiest pgbouncer container)", got)
	}

	if !snap.PgBouncerWaitKnown {
		t.Fatal("PgBouncerWaitKnown is false although the fixture has a pod whose oldest client has waited " +
			"3.5s — this is the least ambiguous saturation signal the platform publishes and it must not be dropped")
	}
	if got := snap.PgBouncerClientWaitSeconds; math.Abs(got-3.5) > 1e-9 {
		t.Fatalf("PgBouncerClientWaitSeconds = %v, want 3.5 (the longest-waiting client across pods)", got)
	}

	// A duration, deliberately NOT run through clampFraction — a wait longer
	// than one second is the entire point of the signal.
	if snap.PgBouncerClientWaitSeconds <= 1 {
		t.Fatalf("PgBouncerClientWaitSeconds = %v: a wait above 1 has been clamped as though it were a fraction, "+
			"which would cap every real queue stall at one second", snap.PgBouncerClientWaitSeconds)
	}

	// And NOTHING may land in the router fields. An unsharded PlanetScale
	// Postgres branch has no routing layer, and the two signals mean
	// different things to an operator: a router is in sluice's connection
	// path, a pooler is not. Publishing the pooler's number as a router
	// reading would tell them to resize a component that does not exist and
	// would imply sluice's own statements are queueing when they are not.
	if snap.RouterCPUKnown || snap.RouterCPUUtil != 0 || snap.RouterMemKnown || snap.RouterMemUtil != 0 {
		t.Errorf("router fields populated on a PlanetScale Postgres exposition: cpu=%v (known=%v) "+
			"mem=%v (known=%v) — there is no routing layer here, and the pooler's reading must not be "+
			"reported as one", snap.RouterCPUUtil, snap.RouterCPUKnown, snap.RouterMemUtil, snap.RouterMemKnown)
	}
}

// TestRouterAndPoolerSurfacesAreDisjoint records a platform fact the
// metricNames doc cites: a branch has a routing layer or a connection pooler,
// never both.
//
// Neki is Postgres by engine, so a Neki branch and a PlanetScale Postgres
// branch read the same [postgresMetricNames] table — which carries the names
// for both shapes. Nothing DEPENDS on the disjointness any more (the two
// signals select independently into separate snapshot fields, so a branch
// running both would simply report both), and an earlier cut of this code that
// cascaded between them did depend on it. The test stays because the fact is
// what makes the two-signal split the right model at all: if PlanetScale ever
// ships a branch with both, that is a design question worth being told about
// rather than discovering in a dashboard.
func TestRouterAndPoolerSurfacesAreDisjoint(t *testing.T) {
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
				t.Fatal("a routing layer AND a connection pooler are both present in one exposition. " +
					"Nothing breaks — the two signals select independently — but the model here assumes a " +
					"branch has one or the other, and the operator guidance attached to each (resize the " +
					"router tier; this is your application's traffic, not sluice's) was written on that " +
					"assumption. Re-read both before trusting either")
			}
		})
	}
}
