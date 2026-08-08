// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item 143 gates, coordinator side: the gate's own CLAIM about why it closed.
//
// # The two properties, and why they are stated together
//
// The item's finding is that a mechanism asserted a cause it had no evidence
// for. Correcting that has two halves and each is worthless alone:
//
//   - the log must STATE the observed verdict rather than a guess
//     ([TestGrowGate_ClosedLogStatesTheObservedEvidenceAndNotACause]), and
//   - the verdict must remain DESCRIPTIVE — the gate's timing must be identical
//     for every value ([TestGrowGate_EvidenceIsDescriptiveOnlyAndNeverChanges-
//     TheHold]).
//
// Without the second, someone reading "the gate now knows the difference"
// would reasonably wire it into the escalation, and Phase A of item 143
// established that we cannot: a real storage-grow reparent's most common face
// on an in-flight write is the connection dying, which is byte-identical to a
// plain transport drop. Quiescing less on [ir.GrowEvidenceNone] would trade a
// bounded false-positive cost for the chance of missing the event the gate
// exists for. So the second gate is not a formality — it is the thing that
// fails if that decision is quietly reversed without the reasoning being
// revisited.
//
// # What these reach
//
// The concrete [GrowGate], the only production implementor of [ir.GrowGate].
// They say nothing about whether a LANE passes a derived verdict; that is
// checked in each engine package (mysql/postgres grow_evidence_test.go),
// because a coordinator test cannot see call-site tagging — the item-136 M4
// lesson.

package migcore

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestGrowGate_EvidenceGovernsTheDeepEscalationAndNotTheEarlyHolds is the
// item-154 replacement for TestGrowGate_EvidenceIsDescriptiveOnlyAndNever-
// ChangesTheHold, which pinned the OPPOSITE property and was removed
// deliberately rather than widened.
//
// # What changed and why the old pin had to go
//
// Item 143 derived [ir.GrowEvidence] and left it descriptive, on the argument
// that a real grow reparent is not reliably distinguishable from a transport
// drop AT THE TRIP POINT. Item 154 keeps exactly that argument for the trip
// point and rejects it for the tenth consecutive re-trip: by then the episode
// has produced no evidence at all across many armed-held-reopened-refuted
// windows, and "this is a bounded serving transition" is no longer an
// unfalsified hypothesis. See [GrowGateEvidenceFreeHoldCap] for the full
// argument, including why prompt re-trip could NOT have been the
// discriminator.
//
// # The two halves, because either alone is a wrong pin
//
// EARLY rungs must stay evidence-INDEPENDENT. That is the half item 143 was
// right about and the half ADR-0110's shield actually rides on: the first hold
// an episode takes is identical whatever the lane observed, so a real grow
// that announces itself only in a way this codebase does not recognise is
// still quiesced exactly as before.
//
// DEEP rungs must diverge. That is the fix: an episode that has never produced
// evidence stops climbing at the cap, while an evidenced episode keeps the
// full ladder.
//
// The INDEPENDENT expected value for both halves is the OTHER gate's observed
// holds, measured in this run — not a constant written down here and not
// anything a gate reports about its own intent.
func TestGrowGate_EvidenceGovernsTheDeepEscalationAndNotTheEarlyHolds(t *testing.T) {
	captureSlog(t)
	const (
		base = 20 * time.Millisecond
		// The cap sits exactly on rung 3 (20 → 40 → 80), so rungs 1-3 are
		// below-or-at it and must match across evidence values, while rungs
		// 4+ are where an evidenced episode pulls away.
		evidenceFreeCap = 80 * time.Millisecond
		windows         = 6
		sharedRungs     = 3
	)
	withScaledGrowGate(t, base, 1600*time.Millisecond, evidenceFreeCap, time.Hour, time.Hour, 1.0)

	holds := map[ir.GrowEvidence][]time.Duration{}
	for _, ev := range []ir.GrowEvidence{ir.GrowEvidenceNone, ir.GrowEvidenceTargetFace, ir.GrowEvidenceTelemetry} {
		g := NewGrowGate(context.Background(), nil)
		done := make(chan time.Duration, windows+2)
		g.onWindowClosed = func(d time.Duration) {
			select {
			case done <- d:
			default:
			}
		}
		for range windows {
			g.Trip("lane transient", ev)
			select {
			case d := <-done:
				holds[ev] = append(holds[ev], d)
			case <-time.After(30 * time.Second):
				t.Fatalf("evidence=%s: the gate never reopened", ev)
			}
		}
	}

	free := holds[ir.GrowEvidenceNone]
	// Anti-vacuity for the shared half: the early ladder must actually climb,
	// or "identical" below would be trivially true of a gate that did nothing.
	if free[sharedRungs-1] <= free[0] {
		t.Fatalf("the ladder did not climb over its first %d rungs (%v); the comparison below would be vacuous",
			sharedRungs, free)
	}

	for _, ev := range []ir.GrowEvidence{ir.GrowEvidenceTargetFace, ir.GrowEvidenceTelemetry} {
		got := holds[ev]

		// HALF ONE: the early rungs are evidence-independent.
		for i := range sharedRungs {
			lo, hi := free[i]/2, free[i]*2
			if got[i] < lo || got[i] > hi {
				t.Errorf(
					"window %d: evidence=%s held %v but evidence=%s held %v — the EARLY rungs must not depend on "+
						"the evidence. Item 154 caps only the deep escalation precisely so that a real grow which "+
						"presents without a recognised face is still quiesced normally at the trip point; making "+
						"the first hold evidence-dependent gives up item 143's actual safety argument",
					i+1, ev, got[i], ir.GrowEvidenceNone, free[i],
				)
			}
		}

		// HALF TWO: the deep rungs diverge. This is the livelock fix itself.
		lastFree, lastEv := free[windows-1], got[windows-1]
		if lastEv <= 3*lastFree {
			t.Errorf(
				"window %d: evidence=%s held %v and evidence=%s held %v — an episode that has produced NO grow "+
					"evidence across %d refuted windows is escalating just as deep as one that has, which is the "+
					"2026-08-05 livelock (246 windows, 2,687 drops, zero reparent evidence, ladder pinned at its "+
					"cap). See [GrowGateEvidenceFreeHoldCap] (item 157)",
				windows, ev, lastEv, ir.GrowEvidenceNone, lastFree, windows,
			)
		}
		if lastFree > evidenceFreeCap*2 {
			t.Errorf(
				"window %d: an evidence-free episode held %v, above its declared cap of %v — the cap is not "+
					"binding, so nothing bounds an evidence-free storm's hold",
				windows, lastFree, evidenceFreeCap,
			)
		}
	}
}

// TestGrowGate_EvidenceAccumulatesPerEpisodeAndResetsWithTheLadder pins the
// two state transitions [GrowGate.episodeSawGrowEvidence] has, which the
// behavioural gate above only observes in aggregate.
//
// Both directions matter and they fail differently. If evidence did NOT stick
// for the rest of an episode, a real grow whose drops mostly present as bare
// transport errors would be demoted to the capped ladder by its own noise —
// the false negative item 143 warned about, reintroduced. If it did NOT reset
// with the ladder, one evidenced blip early in a multi-hour run would unlock
// the deep ladder permanently and the livelock would return for every later
// evidence-free storm.
func TestGrowGate_EvidenceAccumulatesPerEpisodeAndResetsWithTheLadder(t *testing.T) {
	captureSlog(t)
	const (
		base = 20 * time.Millisecond
		idle = 250 * time.Millisecond
	)
	withScaledGrowGate(t, base, 1600*time.Millisecond, base, idle, time.Hour, 1.0)

	g := NewGrowGate(context.Background(), nil)
	holds := make(chan time.Duration, 16)
	g.onWindowClosed = func(d time.Duration) { holds <- d }
	next := func() time.Duration {
		select {
		case d := <-holds:
			return d
		case <-time.After(10 * time.Second):
			t.Fatal("no window observed")
			return 0
		}
	}
	// window drives one trip → reopen cycle and returns the hold taken.
	window := func(ev ir.GrowEvidence) time.Duration {
		g.Trip("lane transient", ev)
		if err := g.Await(context.Background()); err != nil {
			t.Fatalf("Await: %v", err)
		}
		return next()
	}

	// STICKINESS. One evidenced trip, then evidence-free trips in the same
	// episode: the ladder must keep climbing past the cap.
	window(ir.GrowEvidenceTargetFace)
	var deepest time.Duration
	for range 4 {
		if d := window(ir.GrowEvidenceNone); d > deepest {
			deepest = d
		}
	}
	if deepest <= 2*base {
		t.Errorf(
			"after one EVIDENCED trip, four evidence-free trips in the same episode escalated only to %v "+
				"(cap %v) — the evidence did not stick, so a real grow whose other drops present as bare "+
				"transport errors is demoted to the capped ladder by its own noise",
			deepest, base,
		)
	}

	// RESET. A healthy stretch longer than the episode idle ends the episode;
	// the next evidence-free STORM must be back under the cap.
	//
	// SEVERAL windows, not one, and a mutation run is why. The episode ladder
	// resets to rung 1 independently of this flag, so the FIRST window after a
	// healthy stretch holds exactly `base` whether or not the evidence flag
	// reset with it — the cap is not binding at rung 1 and cannot be observed
	// there. A single-window version of this check passed the mutant that
	// deleted the reset outright. The divergence only appears once the ladder
	// has had room to climb.
	time.Sleep(idle + 150*time.Millisecond)
	var deepestAfterReset time.Duration
	for range 4 {
		if d := window(ir.GrowEvidenceNone); d > deepestAfterReset {
			deepestAfterReset = d
		}
	}
	if deepestAfterReset > 2*base {
		t.Errorf(
			"after a healthy stretch, an evidence-free storm escalated to %v (cap %v) — the evidence flag did "+
				"not reset with the episode ladder, so one evidenced blip early in a run unlocks the deep ladder "+
				"for every later evidence-free storm and the livelock returns",
			deepestAfterReset, base,
		)
	}
}

// TestGrowGate_ClosedLogStatesTheObservedEvidenceAndNotACause is the direct
// item-143 regression pin on the claim.
//
// The shipped CLOSED line read "quiescing all cold-copy lanes for a
// coordinated target storage-grow / reparent window" for EVERY close. In the
// 2026-08-05 field log 244 of the 246 windows were opened by
// `vtgate connection error: no endpoints` and none by a reparent-evidenced
// face, so that sentence sent every reader hunting for an event that had not
// occurred — and, because it was prose, nothing failed when it stopped being
// true.
//
// So this asserts both halves: the derived verdict is PRESENT and correct, and
// the unconditional cause-claim is ABSENT.
func TestGrowGate_ClosedLogStatesTheObservedEvidenceAndNotACause(t *testing.T) {
	withScaledGrowGate(t, 5*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond, time.Hour, time.Hour, 1.0)

	for _, tc := range []struct {
		ev    ir.GrowEvidence
		token string
	}{
		{ir.GrowEvidenceNone, "no-grow-evidence"},
		{ir.GrowEvidenceTargetFace, "target-grow-face"},
		{ir.GrowEvidenceTelemetry, "telemetry-headroom"},
	} {
		t.Run(tc.token, func(t *testing.T) {
			buf := captureSlog(t)
			g := NewGrowGate(context.Background(), nil)
			done := make(chan struct{}, 2)
			g.onWindowClosed = func(time.Duration) {
				select {
				case done <- struct{}{}:
				default:
				}
			}
			g.Trip("lane transient", tc.ev)
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("the gate never reopened")
			}

			out := buf.String()
			if !strings.Contains(out, "grow-gate CLOSED") {
				t.Fatalf("no CLOSED line was emitted; this gate would be vacuous. got:\n%s", out)
			}
			if !strings.Contains(out, "evidence="+tc.token) {
				t.Errorf("the CLOSED/reopened lines do not carry evidence=%s. got:\n%s", tc.token, out)
			}
			// The unconditional cause-claim must be gone. Checked on the
			// no-evidence case too — especially there, since that is the
			// population the field log was made of.
			for _, banned := range []string{"storage-grow / reparent window", "likely a primary reparent"} {
				if strings.Contains(out, banned) {
					t.Errorf(
						"the grow-gate log still asserts %q as the cause of a close. Item 143: this is a "+
							"HYPOTHESIS the gate cannot check, and two independent datasets say it is almost "+
							"always wrong. State the derived evidence instead. got:\n%s", banned, out,
					)
				}
			}
		})
	}
}

// TestGrowGate_EvidenceZeroValueUnderClaims pins the zero-value-safe direction
// (the v0.99.51 trap applied to a claim rather than to behaviour): a caller
// that forgets to classify must produce the UNDER-claiming verdict, never a
// positive finding. This is what makes a future trip site's omission a
// silent-but-honest default rather than a silent lie.
func TestGrowGate_EvidenceZeroValueUnderClaims(t *testing.T) {
	var zero ir.GrowEvidence
	if zero != ir.GrowEvidenceNone {
		t.Fatalf("the zero value of ir.GrowEvidence is %s; it must be GrowEvidenceNone so an unclassified trip under-claims", zero)
	}
}
