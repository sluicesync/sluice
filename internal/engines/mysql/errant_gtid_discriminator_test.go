// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// The three-way lineage verdict, and the case that used to be misdiagnosed.
//
// # What was wrong
//
// The VStream lineage pre-flight had two outcomes: every resume UUID present
// (replica lag — proceed) or anything else (a different lineage — refuse). An
// ERRANT GTID falls in "anything else" and is not a different lineage.
//
// An errant GTID is a transaction executed DIRECTLY on a replica, so that
// tablet's `gtid_executed` carries a UUID the primary never executed. sluice
// records Vitess's VGTID verbatim and the CDC tail streams from a REPLICA by
// default on PlanetScale, so the errant UUID lands in the persisted resume
// position. The next resume probes through vtgate, which may pick a different
// tablet with no trace of that UUID — and the old test called that a replaced
// keyspace.
//
// # Measured on a real Vitess cluster, 2026-09-08
//
// The errant transaction was in a database OUTSIDE the keyspace entirely, so
// nothing sluice syncs was touched and the target did not diverge by one byte.
// It still poisoned the position, and the refusal fired verbatim. The control
// is what settles it: the SAME position probed at the tablet HOLDING the
// errant UUID resumed cleanly. Same database, same instant, opposite verdict —
// decided purely by which tablet vtgate happened to pick. Left alone, the
// permanent form is worse: once the errant-carrying replica is replaced, no
// tablet has that UUID and every future resume refuses forever.
//
// # What the fix does and deliberately does not do
//
// It corrects the DIAGNOSIS and the remedy. It does NOT proceed on the errant
// shape, because handing vttablet a resume set that is not a subset of its
// gtid_executed makes it answer "GTIDSet Mismatch" — a refusal that does not
// reliably reach sluice, since vtgate marks the tablet ignorable and blocks.
// That is audit SLM-2's silent-loss path. Trading a wasteful re-copy for a
// silent gap would be the wrong direction, so the action is unchanged and only
// the operator-facing story changes.
func TestGTIDLineageDiscriminator_SeparatesErrantFromForeign(t *testing.T) {
	// The UUIDs the cluster probe actually produced, abbreviated. tablet101 is
	// the replica that executed the errant transaction; shard is the lineage
	// every tablet shares.
	const (
		shard     = "5b176afb-0000-0000-0000-000000000001"
		tablet101 = "5b15e7d4-0000-0000-0000-000000000002"
		foreign   = "9999aaaa-0000-0000-0000-000000000003"
	)

	for _, tc := range []struct {
		name             string
		resume, executed string
		wantSubset       bool // every resume UUID present -> replica lag
		wantIntersect    bool // shares at least one -> errant, not foreign
		wantVerdict      lineageVerdict
		verdict          string
	}{
		{
			name:     "healthy resume — same lineage, fully contained",
			resume:   shard + ":1-38",
			executed: shard + ":1-39",
			// Note this cell never reaches the discriminator in production:
			// GTID_SUBSET(resume, executed) = 1 short-circuits first. It is
			// here so the helpers stay honest on the ordinary shape.
			wantSubset: true, wantIntersect: true,
			wantVerdict: lineageReplicaLag,
			verdict:     "resumes",
		},
		{
			name:       "replica lag — every UUID present, lower sequence numbers",
			resume:     shard + ":1-38",
			executed:   shard + ":1-20",
			wantSubset: true, wantIntersect: true,
			wantVerdict: lineageReplicaLag,
			verdict:     "INFO, proceed — vtgate picks another tablet",
		},
		{
			name: "ERRANT GTID — shares the shard lineage, adds one no other tablet has",
			// Exactly the shape measured on the cluster.
			resume:     tablet101 + ":1-3," + shard + ":1-38",
			executed:   shard + ":1-39",
			wantSubset: false, wantIntersect: true,
			wantVerdict: lineageErrantGTID,
			verdict:     "refuse, but diagnosed as errant with the reconcile remedy",
		},
		{
			name:       "genuinely foreign lineage — shares nothing",
			resume:     shard + ":1-38",
			executed:   foreign + ":1-100",
			wantSubset: false, wantIntersect: false,
			wantVerdict: lineageForeign,
			verdict:     "refuse as a replaced keyspace",
		},
		{
			name:       "fresh instance — empty executed set shares nothing",
			resume:     shard + ":1-38",
			executed:   "",
			wantSubset: false, wantIntersect: false,
			wantVerdict: lineageForeign,
			verdict:     "refuse as a replaced keyspace",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gtidSetUUIDsSubset(tc.resume, tc.executed); got != tc.wantSubset {
				t.Errorf("gtidSetUUIDsSubset = %v, want %v (%s)", got, tc.wantSubset, tc.verdict)
			}
			if got := gtidSetUUIDsIntersect(tc.resume, tc.executed); got != tc.wantIntersect {
				t.Errorf("gtidSetUUIDsIntersect = %v, want %v (%s)", got, tc.wantIntersect, tc.verdict)
			}
			// AND THE VERDICT ITSELF, which is the assertion that matters.
			// Grading only the two helpers above leaves the DISPATCH unpinned:
			// a mutation collapsing the three-way decision back to two outcomes
			// keeps both helpers correct and every cell above green, while an
			// errant GTID is again reported as a replaced keyspace. That is the
			// helper-versus-wiring seam that let the pgtrigger capture-shape
			// door ship inert, which is why classifyLineage is a value.
			if got := classifyLineage(tc.resume, tc.executed); got != tc.wantVerdict {
				t.Errorf("classifyLineage = %v, want %v (%s)", got, tc.wantVerdict, tc.verdict)
			}
		})
	}

	// THE FLOOR. The errant case must be distinguishable from the foreign one
	// — if the two helpers ever agree on it, the three-way verdict collapses
	// back to the two-way one that misdiagnosed it, and every cell above still
	// passes individually.
	errantResume := tablet101 + ":1-3," + shard + ":1-38"
	shardExecuted := shard + ":1-39"
	foreignExecuted := foreign + ":1-100"
	if gtidSetUUIDsIntersect(errantResume, shardExecuted) == gtidSetUUIDsIntersect(errantResume, foreignExecuted) {
		t.Fatal("the errant and foreign shapes are no longer distinguishable, so the pre-flight has " +
			"collapsed back to a two-way verdict and an errant GTID is again reported as a replaced keyspace")
	}
}
