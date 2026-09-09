// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

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
// shape, and a 3-tablet cluster measurement settled why: once the
// errant-carrying tablet is replaced — routine on PlanetScale — no tablet can
// serve the position, every one answers "GTIDSet Mismatch", and vtgate spends
// ~90s cycling through them before giving up. Refusing at the door costs one
// re-copy; proceeding would cost that discovery on every future resume.
//
// While that tablet is still in the pool, vtgate DOES route the stream to it
// and resuming works — so the refusal is knowingly conservative in that half,
// and that cost is accepted rather than unexamined.
//
// (An earlier version of this comment said vtgate "marks the tablet ignorable
// and blocks" and called it audit SLM-2's silent-loss path. It does not block
// indefinitely, and the silent path does not reach here — the give-up is
// classified, stored via setErr, and surfaced loudly, and
// cleanExitOnCallerCancel checks ctx.Err() first. Corrected here after the
// same claim was found uncorrected in three places, one commit apart.)
//
// # A LIMIT OF THE DISCRIMINATOR, stated so the next reader does not assume otherwise
//
// This separates an errant GTID riding a UUID the shard has NEVER executed.
// An errant transaction on a DEMOTED PRIMARY rides a UUID already present in
// every tablet's gtid_executed, so the UUID-set test sees every UUID present,
// returns lineageReplicaLag, and PROCEEDS — landing on the same ~90s give-up.
// That shape is pre-existing, unchanged by this fix, and is pinned below as a
// known gap rather than left to be rediscovered.
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
		{
			// THE REAL WIRE FORMAT. @@global.gtid_executed on a multi-UUID
			// server comes back comma-AND-NEWLINE separated, which every
			// cell above silently avoids by using single-UUID sets. If the
			// splitting ever stopped tolerating the newline, the helpers
			// would disagree with production on the ordinary shape.
			name:       "multi-UUID executed set in MySQL's real ,\\n format",
			resume:     shard + ":1-38",
			executed:   shard + ":1-20,\n" + tablet101 + ":1-4",
			wantSubset: true, wantIntersect: true,
			wantVerdict: lineageReplicaLag,
			verdict:     "INFO, proceed",
		},
		{
			name:       "uppercase resume UUID — both helpers must fold case identically",
			resume:     strings.ToUpper(shard) + ":1-38",
			executed:   shard + ":1-20",
			wantSubset: true, wantIntersect: true,
			wantVerdict: lineageReplicaLag,
			verdict:     "INFO, proceed",
		},
		{
			// A KNOWN GAP, pinned as such rather than left to be
			// rediscovered (v0.147.1 pre-tag review). An errant transaction
			// on a DEMOTED PRIMARY rides a UUID already present in every
			// tablet's gtid_executed, so the UUID-set test sees every UUID
			// present and returns lag — which PROCEEDS, landing on the ~90s
			// give-up. This discriminator separates the fresh-replica-UUID
			// errant shape only. Closing it needs sequence-range reasoning,
			// not UUID-set reasoning, and is not attempted here.
			name:       "GAP: errant GTID on a demoted primary is classified as lag",
			resume:     shard + ":1-38",
			executed:   shard + ":1-20," + tablet101 + ":1-4",
			wantSubset: true, wantIntersect: true,
			wantVerdict: lineageReplicaLag,
			verdict:     "proceeds — the known limit of a UUID-set discriminator",
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
			// Coherence: subset must IMPLY intersect. If they ever disagree
			// that way, classifyLineage is incoherent — the lag arm would be
			// reachable for a position sharing no UUIDs at all.
			if tc.wantSubset && !tc.wantIntersect {
				t.Fatal("subset without intersect: the two helpers disagree in the direction that makes the verdict incoherent")
			}
		})
	}

	// THE ACTION, not just the verdict. This is the seam the v0.147.1 pre-tag
	// review found ungated: folding the errant arm into the lag arm — so an
	// errant GTID silently PROCEEDS instead of refusing — passed the ENTIRE
	// unit suite, because every test graded classifyLineage and nothing
	// graded what the pre-flight DOES with it.
	//
	// The ir.ErrPositionInvalid assertion is load-bearing separately from the
	// nil/non-nil one: the streamer's ADR-0022 fall-through keys on it, so a
	// refusal that stopped wrapping it would silently change the failure mode
	// from re-copy to hard stop with nothing failing.
	t.Run("the verdict reaches the right ACTION", func(t *testing.T) {
		const sh, tgt = "0", "test:0@replica"
		if err := lineageRefusal(lineageReplicaLag, sh, tgt, "a:1-2", "a:1-1"); err != nil {
			t.Errorf("replica lag must PROCEED (nil), got %v", err)
		}
		for _, tc := range []struct {
			name    string
			verdict lineageVerdict
			wantIn  string
			// autoRecopy says whether the refusal may route into the
			// streamer's automatic cold-start re-snapshot. The errant arm
			// keeps that route (its remedy is executable on the source; a
			// terminal refusal there on PlanetScale is a filed policy
			// call). The FOREIGN arm must NOT: the re-snapshot would drop
			// the target and re-copy from a different keyspace (audit
			// 2026-09-09 A0909-MYSQL-HIGH-1).
			autoRecopy bool
		}{
			{"errant refuses and says so", lineageErrantGTID, "ERRANT GTID", true},
			{"foreign refuses and says so", lineageForeign, "shares NONE", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := lineageRefusal(tc.verdict, sh, tgt, "a:1-2", "b:1-1")
				if err == nil {
					t.Fatalf("%v must REFUSE; a nil here resumes a position the shard cannot serve", tc.verdict)
				}
				if got := errors.Is(err, ir.ErrPositionInvalid); got != tc.autoRecopy {
					t.Errorf("refusal wraps ir.ErrPositionInvalid = %v; want %v (that sentinel is what routes the "+
						"destructive automatic re-copy): %v", got, tc.autoRecopy, err)
				}
				if got := errors.Is(err, ir.ErrPositionForeignLineage); got != !tc.autoRecopy {
					t.Errorf("refusal wraps ir.ErrPositionForeignLineage = %v; want %v: %v", got, !tc.autoRecopy, err)
				}
				if !strings.Contains(err.Error(), tc.wantIn) {
					t.Errorf("refusal does not name %q, so an operator cannot tell the two apart: %v", tc.wantIn, err)
				}
			})
		}
	})

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
