// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Unit tests for the F13 pre-emptive slot-health threshold evaluator
// + rate-limit / de-dup logic. The threshold evaluator is a pure
// function from (snapshot, state, thresholds, now) → decision; these
// tests drive the boundary cases deterministically without sleeping,
// per the CLAUDE.md "pin the class, not the representative" rule:
//
//   - 70% boundary crossed (warn)
//   - 85% boundary crossed (critical) — including skipping past 70 in
//     one step
//   - max_slot_wal_keep_size = -1 (unlimited; no warn)
//   - max_slot_wal_keep_size = 0 (extreme; percentage path skipped)
//   - slot inactive >= threshold (warn)
//   - slot active again (warn clears → INFO)
//   - repeated warn within rate-limit window (suppressed)
//   - repeated warn outside rate-limit window (re-emitted)
//   - retention-warn supersedes inactivity-warn when both hold
//   - state transitions emit immediately (70 → 85 within window)

import (
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// fixedNow returns a wall-clock baseline the tests can offset from.
// Using a fixed point eliminates timer-flake across runs.
var fixedNow = time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

func defaultThresholds() SlotHealthThresholds {
	return DefaultSlotHealthThresholds()
}

// TestEvaluateSlotHealth_NoBoundNoWarning pins case (d) — unlimited
// retention disables the percentage warning regardless of how far the
// slot has fallen behind. -1 is the documented sentinel.
func TestEvaluateSlotHealth_NoBoundNoWarning(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	snap := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           true,
		LagBytes:         1 << 40, // 1 TiB lag
		MaxKeepSizeBytes: -1,      // unlimited
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnNone {
		t.Errorf("expected slotWarnNone with unlimited keep_size; got %v (percent=%.1f)", dec.Kind, dec.PercentUsed)
	}
	if dec.Emit {
		t.Errorf("expected Emit=false; got true")
	}
}

// TestEvaluateSlotHealth_ZeroBoundSkipsPercentage pins the extreme
// max_slot_wal_keep_size=0 case: the percentage path is skipped (no
// divide-by-zero, no spurious infinite-percent warn). Inactivity can
// still fire.
func TestEvaluateSlotHealth_ZeroBoundSkipsPercentage(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	snap := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           true,
		LagBytes:         500 << 20, // 500 MiB
		MaxKeepSizeBytes: 0,
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnNone {
		t.Errorf("expected slotWarnNone with keep_size=0; got %v (percent=%.1f)", dec.Kind, dec.PercentUsed)
	}
}

// TestEvaluateSlotHealth_WarnAt70 pins case (a) — crossing exactly 70%
// emits the WARN.
func TestEvaluateSlotHealth_WarnAt70(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	snap := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           true,
		LagBytes:         700 << 20, // 700 MiB
		MaxKeepSizeBytes: 1000 << 20,
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnRetention70 {
		t.Errorf("expected slotWarnRetention70; got %v", dec.Kind)
	}
	if !dec.Emit {
		t.Errorf("expected Emit=true at first cross of 70%%")
	}
	if dec.PercentUsed < 69.9 || dec.PercentUsed > 70.1 {
		t.Errorf("expected percent ~70; got %.2f", dec.PercentUsed)
	}
}

// TestEvaluateSlotHealth_CriticalAt85 pins case (b) — crossing 85%
// emits the louder CRITICAL.
func TestEvaluateSlotHealth_CriticalAt85(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	snap := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           true,
		LagBytes:         85,
		MaxKeepSizeBytes: 100,
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnRetention85 {
		t.Errorf("expected slotWarnRetention85; got %v", dec.Kind)
	}
	if !dec.Emit {
		t.Errorf("expected Emit=true at first cross of 85%%")
	}
}

// TestEvaluateSlotHealth_SkipPast70_StraightToCritical pins case (b)
// variant: a probe finds pressure already at 90% on first observation
// (the 70 threshold was crossed between probes); the critical fires
// directly without first emitting a 70 WARN.
func TestEvaluateSlotHealth_SkipPast70_StraightToCritical(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	snap := ir.SlotHealth{
		Active:           true,
		LagBytes:         90,
		MaxKeepSizeBytes: 100,
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnRetention85 {
		t.Errorf("expected slotWarnRetention85; got %v", dec.Kind)
	}
	if !dec.Emit {
		t.Errorf("expected Emit=true")
	}
}

// TestEvaluateSlotHealth_InactiveBelowThreshold pins the inactive-but-
// fresh case: a probe sees active=false but inactivity hasn't reached
// 30m yet. No warning.
func TestEvaluateSlotHealth_InactiveBelowThreshold(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	// Pretend the slot was last seen active 15 minutes ago — half the
	// 30m threshold.
	st.lastActiveSeenAt = fixedNow.Add(-15 * time.Minute)

	snap := ir.SlotHealth{
		Active:           false,
		LagBytes:         0,
		MaxKeepSizeBytes: -1,
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnNone {
		t.Errorf("expected slotWarnNone (15m < 30m threshold); got %v", dec.Kind)
	}
}

// TestEvaluateSlotHealth_InactiveCrossesThreshold pins case (c) —
// inactivity duration exceeds the threshold.
func TestEvaluateSlotHealth_InactiveCrossesThreshold(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	st.lastActiveSeenAt = fixedNow.Add(-45 * time.Minute) // 45m > 30m

	snap := ir.SlotHealth{
		Active:           false,
		LagBytes:         0,
		MaxKeepSizeBytes: -1,
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnInactive {
		t.Errorf("expected slotWarnInactive; got %v", dec.Kind)
	}
	if !dec.Emit {
		t.Errorf("expected Emit=true")
	}
	if dec.InactiveFor < 44*time.Minute {
		t.Errorf("expected InactiveFor ~45m; got %v", dec.InactiveFor)
	}
}

// TestEvaluateSlotHealth_ActiveProbeResetsInactivity pins the
// observation-based active tracker: a probe seeing active=true resets
// lastActiveSeenAt to now(), so a later inactivity probe measures
// from the most recent active observation.
func TestEvaluateSlotHealth_ActiveProbeResetsInactivity(t *testing.T) {
	st := newSlotHealthState(fixedNow.Add(-1 * time.Hour))

	// Tick 1: probe finds active=true. Should refresh lastActiveSeenAt.
	dec1 := evaluateSlotHealth(ir.SlotHealth{Active: true, MaxKeepSizeBytes: -1}, st, defaultThresholds(), fixedNow)
	if dec1.Kind != slotWarnNone {
		t.Fatalf("tick1: expected no warn; got %v", dec1.Kind)
	}
	if !st.lastActiveSeenAt.Equal(fixedNow) {
		t.Errorf("active probe should set lastActiveSeenAt = now; got %v", st.lastActiveSeenAt)
	}

	// Tick 2 (5 minutes later): probe finds active=false. Inactivity =
	// 5 minutes < 30m threshold; no warn.
	t2 := fixedNow.Add(5 * time.Minute)
	dec2 := evaluateSlotHealth(ir.SlotHealth{Active: false, MaxKeepSizeBytes: -1}, st, defaultThresholds(), t2)
	if dec2.Kind != slotWarnNone {
		t.Errorf("tick2: 5m inactivity should be below threshold; got %v", dec2.Kind)
	}
}

// TestEvaluateSlotHealth_RateLimitSuppression pins case (f) — same
// condition firing twice within the 5m window emits once. Mutation of
// state happens via recordSlotHealthEmission so the test mirrors the
// production probe loop exactly.
func TestEvaluateSlotHealth_RateLimitSuppression(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	thr := defaultThresholds()
	snap := ir.SlotHealth{
		Active:           true,
		LagBytes:         72,
		MaxKeepSizeBytes: 100,
	}

	// Tick 1: first 70% cross — emit.
	dec1 := evaluateSlotHealth(snap, st, thr, fixedNow)
	if dec1.Kind != slotWarnRetention70 || !dec1.Emit {
		t.Fatalf("tick1: want (warn70, emit=true); got (%v, emit=%v)", dec1.Kind, dec1.Emit)
	}
	recordSlotHealthEmission(st, dec1, fixedNow)

	// Tick 2 (1 minute later, same condition): suppressed by rate-limit.
	t2 := fixedNow.Add(1 * time.Minute)
	dec2 := evaluateSlotHealth(snap, st, thr, t2)
	if dec2.Kind != slotWarnRetention70 {
		t.Errorf("tick2: kind should still be warn70; got %v", dec2.Kind)
	}
	if dec2.Emit {
		t.Errorf("tick2: expected suppression within rate-limit window; got Emit=true")
	}
}

// TestEvaluateSlotHealth_RateLimitExpires pins case (g) — same
// condition firing after the rate-limit window re-emits.
func TestEvaluateSlotHealth_RateLimitExpires(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	thr := defaultThresholds()
	snap := ir.SlotHealth{
		Active:           true,
		LagBytes:         72,
		MaxKeepSizeBytes: 100,
	}

	// Tick 1: emit, record.
	dec1 := evaluateSlotHealth(snap, st, thr, fixedNow)
	recordSlotHealthEmission(st, dec1, fixedNow)

	// Tick 2 (6 minutes later — past the 5m window): re-emit.
	t2 := fixedNow.Add(6 * time.Minute)
	dec2 := evaluateSlotHealth(snap, st, thr, t2)
	if dec2.Kind != slotWarnRetention70 {
		t.Errorf("tick2: kind should be warn70; got %v", dec2.Kind)
	}
	if !dec2.Emit {
		t.Errorf("tick2: expected re-emission past rate-limit window; got Emit=false")
	}
}

// TestEvaluateSlotHealth_TransitionEmitsImmediately pins the state-
// transition rule: 70 → 85 within the rate-limit window still emits
// (operator needs to see "now critical" promptly).
func TestEvaluateSlotHealth_TransitionEmitsImmediately(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	thr := defaultThresholds()

	// Tick 1: cross 70%.
	snap1 := ir.SlotHealth{Active: true, LagBytes: 72, MaxKeepSizeBytes: 100}
	dec1 := evaluateSlotHealth(snap1, st, thr, fixedNow)
	if dec1.Kind != slotWarnRetention70 || !dec1.Emit {
		t.Fatalf("tick1: want warn70 emit; got (%v, emit=%v)", dec1.Kind, dec1.Emit)
	}
	recordSlotHealthEmission(st, dec1, fixedNow)

	// Tick 2 (1 minute later, escalated to 87%): different kind —
	// emit immediately despite being inside the 70 rate-limit window.
	t2 := fixedNow.Add(1 * time.Minute)
	snap2 := ir.SlotHealth{Active: true, LagBytes: 87, MaxKeepSizeBytes: 100}
	dec2 := evaluateSlotHealth(snap2, st, thr, t2)
	if dec2.Kind != slotWarnRetention85 {
		t.Errorf("tick2: kind should be warn85; got %v", dec2.Kind)
	}
	if !dec2.Emit {
		t.Errorf("tick2: state transition (warn70 → warn85) should emit even inside rate-limit window")
	}
}

// TestEvaluateSlotHealth_ClearsToINFO pins case (e) — warning clears
// when condition resolves. The decision carries Cleared=true so the
// caller emits an INFO ("alarm resolved") rather than silence.
func TestEvaluateSlotHealth_ClearsToINFO(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	thr := defaultThresholds()

	// Tick 1: warn70 fires.
	snap1 := ir.SlotHealth{Active: true, LagBytes: 72, MaxKeepSizeBytes: 100}
	dec1 := evaluateSlotHealth(snap1, st, thr, fixedNow)
	recordSlotHealthEmission(st, dec1, fixedNow)
	if st.lastFiredKind != slotWarnRetention70 {
		t.Fatalf("setup: lastFiredKind should be warn70; got %v", st.lastFiredKind)
	}

	// Tick 2: pressure dropped back to 40%. Cleared.
	t2 := fixedNow.Add(2 * time.Minute)
	snap2 := ir.SlotHealth{Active: true, LagBytes: 40, MaxKeepSizeBytes: 100}
	dec2 := evaluateSlotHealth(snap2, st, thr, t2)
	if dec2.Kind != slotWarnNone {
		t.Errorf("tick2: kind should be none (cleared); got %v", dec2.Kind)
	}
	if !dec2.Cleared {
		t.Errorf("tick2: expected Cleared=true")
	}
	if dec2.Emit {
		t.Errorf("tick2: Cleared and Emit are mutually exclusive on the same tick; got Emit=true")
	}
	recordSlotHealthEmission(st, dec2, t2)
	if st.lastFiredKind != slotWarnNone {
		t.Errorf("after cleared+record: lastFiredKind should reset to none; got %v", st.lastFiredKind)
	}
}

// TestEvaluateSlotHealth_StillCleanIsSilent pins the "no event"
// negative: a probe finding nothing wrong on a stream that was already
// clean must not emit anything (no Cleared, no Emit).
func TestEvaluateSlotHealth_StillCleanIsSilent(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	snap := ir.SlotHealth{Active: true, LagBytes: 10, MaxKeepSizeBytes: 100}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnNone || dec.Emit || dec.Cleared {
		t.Errorf("expected clean-and-silent; got %+v", dec)
	}
}

// TestEvaluateSlotHealth_RetentionSupersedesInactive pins the "both
// hold" precedence: when the slot is inactive AND lagging past 70%,
// retention wins (it's the operator-actionable signal PG will evict
// on; inactivity is informational).
func TestEvaluateSlotHealth_RetentionSupersedesInactive(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	st.lastActiveSeenAt = fixedNow.Add(-1 * time.Hour) // would trigger inactive

	snap := ir.SlotHealth{
		Active:           false,
		LagBytes:         72,
		MaxKeepSizeBytes: 100,
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnRetention70 {
		t.Errorf("expected warn70 to supersede inactive; got %v", dec.Kind)
	}
}
