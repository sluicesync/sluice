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

// TestGrowGate_EvidenceIsDescriptiveOnlyAndNeverChangesTheHold drives three
// otherwise-identical gates through the same trip sequence, differing only in
// the evidence value, and requires the window holds to match exactly.
//
// The INDEPENDENT expected value is the OTHER gate's observed holds — not
// anything the gate reports about its own intent — so a change that made the
// timing evidence-dependent fails here regardless of how it is documented.
func TestGrowGate_EvidenceIsDescriptiveOnlyAndNeverChangesTheHold(t *testing.T) {
	captureSlog(t)
	withScaledGrowGate(t, 20*time.Millisecond, 400*time.Millisecond, time.Hour, time.Hour, 1.0)

	const windows = 4
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

	base := holds[ir.GrowEvidenceNone]
	// Anti-vacuity: the ladder must actually have climbed, or "identical" would
	// be trivially true of a gate that did nothing.
	if len(base) != windows || base[len(base)-1] <= base[0] {
		t.Fatalf("the escalation ladder did not climb across %d windows (%v); the comparison below would be vacuous", windows, base)
	}
	for _, ev := range []ir.GrowEvidence{ir.GrowEvidenceTargetFace, ir.GrowEvidenceTelemetry} {
		got := holds[ev]
		for i := range base {
			// Generous slack: the property is "the evidence is not an input",
			// and any evidence-driven ladder would diverge by a factor, not by
			// timer jitter.
			lo, hi := base[i]/2, base[i]*2
			if got[i] < lo || got[i] > hi {
				t.Errorf(
					"window %d: evidence=%s held %v but evidence=%s held %v — the gate's TIMING is reading the "+
						"evidence. Item 143 deliberately kept the verdict descriptive: a real grow reparent is not "+
						"reliably distinguishable from a transport drop at the trip point, so quiescing less on "+
						"'no evidence' risks missing the event the gate exists for. If that decision is being "+
						"reversed, revisit ADR-0110 and this test's doc — do not just widen the bound",
					i+1, ev, got[i], ir.GrowEvidenceNone, base[i],
				)
			}
		}
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
	withScaledGrowGate(t, 5*time.Millisecond, 20*time.Millisecond, time.Hour, time.Hour, 1.0)

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
