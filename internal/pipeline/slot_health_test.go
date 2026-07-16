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
//   - escalations emit immediately (70 → 85 within window)
//   - wal_status='lost' ⇒ CRITICAL once, then latched — never cleared
//     (MED-D0-9), superseding every other condition
//   - wal_status='unreserved' ⇒ CRITICAL, clearable (not terminal)
//   - slot vanished after a condition fired ⇒ terminal CRITICAL once,
//     then latched (LOW-D0-17); vanished-while-clean stays silent
//   - downgrades (85→70, anything→clean) hold for one extra probe tick
//     so boundary flapping can't page every 30s (LOW-D0-18)

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
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
// caller emits an INFO ("alarm resolved") rather than silence. Since
// the LOW-D0-18 hysteresis, a clear is a DOWNGRADE and must persist
// for one extra probe tick before committing — tick 2 is held, tick 3
// clears.
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

	// Tick 2: pressure dropped back to 40%. Held by hysteresis (a
	// clear must persist one extra tick before it announces).
	t2 := fixedNow.Add(2 * time.Minute)
	snap2 := ir.SlotHealth{Active: true, LagBytes: 40, MaxKeepSizeBytes: 100}
	dec2 := evaluateSlotHealth(snap2, st, thr, t2)
	if dec2.Cleared || dec2.Emit {
		t.Errorf("tick2: first clean tick after a fire must be held (hysteresis); got %+v", dec2)
	}
	recordSlotHealthEmission(st, dec2, t2)

	// Tick 3: still clean — the clear commits.
	t3 := fixedNow.Add(3 * time.Minute)
	dec3 := evaluateSlotHealth(snap2, st, thr, t3)
	if dec3.Kind != slotWarnNone {
		t.Errorf("tick3: kind should be none (cleared); got %v", dec3.Kind)
	}
	if !dec3.Cleared {
		t.Errorf("tick3: expected Cleared=true")
	}
	if dec3.Emit {
		t.Errorf("tick3: Cleared and Emit are mutually exclusive on the same tick; got Emit=true")
	}
	recordSlotHealthEmission(st, dec3, t3)
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

// TestEvaluateSlotHealth_LostIsTerminal is the MED-D0-9 pin. A lost
// slot reports NULL lag, which the reporter maps to 0 — so pre-fix the
// evaluator saw 0% pressure, decided "clean", and emitted a false
// "condition cleared" INFO at the exact moment the slot became
// unrecoverable (observed live on PG16). The fix dispatches on
// wal_status BEFORE the percentage math: 'lost' pages CRITICAL exactly
// once, latches, and is NEVER cleared — not even by a snapshot that
// would otherwise evaluate clean.
func TestEvaluateSlotHealth_LostIsTerminal(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	thr := defaultThresholds()

	// The observed PG16 shape: invalidated slot → active=false,
	// wal_status='lost', NULL lag (→0), NULL restart_lsn. GUC still set.
	lost := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           false,
		LagBytes:         0,
		MaxKeepSizeBytes: 100 << 20,
		WALStatus:        "lost",
	}

	// Tick 1: terminal CRITICAL fires.
	dec1 := evaluateSlotHealth(lost, st, thr, fixedNow)
	if dec1.Kind != slotWarnLost || !dec1.Emit {
		t.Fatalf("tick1: want (lost, emit=true); got (%v, emit=%v)", dec1.Kind, dec1.Emit)
	}
	if dec1.Cleared {
		t.Fatalf("tick1: a lost slot must never read as cleared")
	}
	recordSlotHealthEmission(st, dec1, fixedNow)
	if !st.terminalLatched {
		t.Fatal("recording a lost emission must set the terminal latch")
	}

	// Tick 2 (still lost, past the rate-limit window): latched — no
	// re-fire (an unrecoverable event re-paged every 5m is noise).
	t2 := fixedNow.Add(6 * time.Minute)
	dec2 := evaluateSlotHealth(lost, st, thr, t2)
	if dec2.Emit || dec2.Cleared {
		t.Errorf("tick2: latched terminal must be silent; got %+v", dec2)
	}
	recordSlotHealthEmission(st, dec2, t2)

	// Tick 3: a snapshot that would otherwise evaluate CLEAN (this is
	// exactly the pre-fix false-clear shape). Still silent: no
	// "condition cleared", no emission, forever.
	clean := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           true,
		LagBytes:         0,
		MaxKeepSizeBytes: 100 << 20,
		WALStatus:        "reserved",
	}
	t3 := fixedNow.Add(10 * time.Minute)
	dec3 := evaluateSlotHealth(clean, st, thr, t3)
	if dec3.Emit || dec3.Cleared {
		t.Errorf("tick3: terminal latch must survive a clean-looking probe (the MED-D0-9 shape); got %+v", dec3)
	}
}

// TestEvaluateSlotHealth_LostSupersedesRetention pins the precedence:
// when a slot is simultaneously lost AND showing retention pressure
// (possible transiently), the terminal condition wins — the percentage
// story is over.
func TestEvaluateSlotHealth_LostSupersedesRetention(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	snap := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           false,
		LagBytes:         95,
		MaxKeepSizeBytes: 100,
		WALStatus:        "lost",
	}
	dec := evaluateSlotHealth(snap, st, defaultThresholds(), fixedNow)
	if dec.Kind != slotWarnLost {
		t.Errorf("expected lost to supersede retention85; got %v", dec.Kind)
	}
}

// TestEvaluateSlotHealth_UnreservedIsCriticalButClearable pins the
// non-terminal treatment of wal_status='unreserved': it pages
// immediately (invalidation lands at the next checkpoint — the last
// window where intervention helps) but PG documents that the state can
// return to reserved/extended if the consumer catches up, so it is NOT
// latched — a recovery clears (after the hysteresis tick) and the
// paging net stays alive.
func TestEvaluateSlotHealth_UnreservedIsCriticalButClearable(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	thr := defaultThresholds()

	unres := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           false,
		LagBytes:         120,
		MaxKeepSizeBytes: 100,
		WALStatus:        "unreserved",
	}
	dec1 := evaluateSlotHealth(unres, st, thr, fixedNow)
	if dec1.Kind != slotWarnUnreserved || !dec1.Emit {
		t.Fatalf("tick1: want (unreserved, emit=true); got (%v, emit=%v)", dec1.Kind, dec1.Emit)
	}
	recordSlotHealthEmission(st, dec1, fixedNow)
	if st.terminalLatched {
		t.Fatal("unreserved must NOT set the terminal latch (PG documents it can recover)")
	}

	// The consumer catches up before the checkpoint: clean probes.
	clean := ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           true,
		LagBytes:         10,
		MaxKeepSizeBytes: 100,
		WALStatus:        "reserved",
	}
	t2 := fixedNow.Add(1 * time.Minute)
	dec2 := evaluateSlotHealth(clean, st, thr, t2)
	recordSlotHealthEmission(st, dec2, t2)
	t3 := fixedNow.Add(2 * time.Minute)
	dec3 := evaluateSlotHealth(clean, st, thr, t3)
	if !dec3.Cleared {
		t.Errorf("unreserved must clear once the recovery persists a tick; got %+v", dec3)
	}
}

// TestEvaluateSlotVanished is the LOW-D0-17 pin: a probe returning
// ok=false (no slot row) is silent while nothing has fired (cold-start
// race, or an operator dropping a healthy slot), but a slot that
// vanishes while a condition is OUTSTANDING pages terminal CRITICAL
// once and latches.
func TestEvaluateSlotVanished(t *testing.T) {
	t.Run("clean history → silent skip", func(t *testing.T) {
		st := newSlotHealthState(fixedNow)
		dec := evaluateSlotVanished(st)
		if dec.Emit || dec.Cleared || dec.Kind != slotWarnNone {
			t.Errorf("vanished-while-clean must stay silent; got %+v", dec)
		}
	})

	t.Run("condition outstanding → terminal once, then latched", func(t *testing.T) {
		st := newSlotHealthState(fixedNow)
		thr := defaultThresholds()

		// A critical retention condition fires first.
		snap := ir.SlotHealth{SlotName: "sluice_slot", Active: true, LagBytes: 90, MaxKeepSizeBytes: 100}
		dec := evaluateSlotHealth(snap, st, thr, fixedNow)
		if dec.Kind != slotWarnRetention85 || !dec.Emit {
			t.Fatalf("setup: want critical85 emit; got %+v", dec)
		}
		recordSlotHealthEmission(st, dec, fixedNow)

		// The slot row vanishes: one terminal page.
		t2 := fixedNow.Add(1 * time.Minute)
		gone := evaluateSlotVanished(st)
		if gone.Kind != slotWarnDropped || !gone.Emit {
			t.Fatalf("vanished-with-condition: want (dropped, emit=true); got (%v, emit=%v)", gone.Kind, gone.Emit)
		}
		recordSlotHealthEmission(st, gone, t2)
		if !st.terminalLatched {
			t.Fatal("recording a dropped emission must set the terminal latch")
		}

		// Still gone on later ticks: silent.
		again := evaluateSlotVanished(st)
		if again.Emit || again.Cleared {
			t.Errorf("latched dropped must be silent; got %+v", again)
		}

		// Even if a slot row REAPPEARS looking clean (recreated by
		// someone else), the latch holds for this attachment's lifetime.
		clean := ir.SlotHealth{SlotName: "sluice_slot", Active: true, LagBytes: 0, MaxKeepSizeBytes: 100, WALStatus: "reserved"}
		dec3 := evaluateSlotHealth(clean, st, thr, fixedNow.Add(10*time.Minute))
		if dec3.Emit || dec3.Cleared {
			t.Errorf("terminal latch must survive a reappeared slot; got %+v", dec3)
		}
	})

	t.Run("condition fired then cleared, then vanished → silent", func(t *testing.T) {
		st := newSlotHealthState(fixedNow)
		thr := defaultThresholds()

		snap := ir.SlotHealth{SlotName: "sluice_slot", Active: true, LagBytes: 72, MaxKeepSizeBytes: 100}
		dec := evaluateSlotHealth(snap, st, thr, fixedNow)
		recordSlotHealthEmission(st, dec, fixedNow)

		// Genuine clear (two clean ticks per the hysteresis).
		clean := ir.SlotHealth{SlotName: "sluice_slot", Active: true, LagBytes: 10, MaxKeepSizeBytes: 100}
		d2 := evaluateSlotHealth(clean, st, thr, fixedNow.Add(1*time.Minute))
		recordSlotHealthEmission(st, d2, fixedNow.Add(1*time.Minute))
		d3 := evaluateSlotHealth(clean, st, thr, fixedNow.Add(2*time.Minute))
		if !d3.Cleared {
			t.Fatalf("setup: expected cleared; got %+v", d3)
		}
		recordSlotHealthEmission(st, d3, fixedNow.Add(2*time.Minute))

		// Now the slot vanishes with NO condition outstanding — a
		// deliberate operator drop of a healthy slot; stays silent.
		gone := evaluateSlotVanished(st)
		if gone.Emit {
			t.Errorf("vanished-after-clear must stay silent; got %+v", gone)
		}
	})
}

// TestEvaluateSlotHealth_FlappingDamped is the LOW-D0-18 pin: a reading
// hovering on a threshold boundary used to page on EVERY 30s probe
// because each kind bounce was a "transition" that bypassed the 5m
// rate limit. With the one-tick hysteresis, an oscillation emits once
// per rate-limit window.
func TestEvaluateSlotHealth_FlappingDamped(t *testing.T) {
	t.Run("70↔85 boundary flap", func(t *testing.T) {
		st := newSlotHealthState(fixedNow)
		thr := defaultThresholds()
		hi := ir.SlotHealth{Active: true, LagBytes: 86, MaxKeepSizeBytes: 100}
		lo := ir.SlotHealth{Active: true, LagBytes: 84, MaxKeepSizeBytes: 100}

		emits := 0
		tick := func(snap ir.SlotHealth, at time.Time) {
			dec := evaluateSlotHealth(snap, st, thr, at)
			if dec.Emit {
				emits++
			}
			recordSlotHealthEmission(st, dec, at)
		}

		// 10 alternating probes 30s apart, all inside the 5m window.
		for i := 0; i < 10; i++ {
			at := fixedNow.Add(time.Duration(i) * 30 * time.Second)
			if i%2 == 0 {
				tick(hi, at)
			} else {
				tick(lo, at)
			}
		}
		if emits != 1 {
			t.Errorf("oscillation inside the rate-limit window emitted %d times; want 1", emits)
		}
	})

	t.Run("clean↔70 boundary flap", func(t *testing.T) {
		st := newSlotHealthState(fixedNow)
		thr := defaultThresholds()
		hot := ir.SlotHealth{Active: true, LagBytes: 71, MaxKeepSizeBytes: 100}
		cool := ir.SlotHealth{Active: true, LagBytes: 69, MaxKeepSizeBytes: 100}

		emits, clears := 0, 0
		tick := func(snap ir.SlotHealth, at time.Time) {
			dec := evaluateSlotHealth(snap, st, thr, at)
			if dec.Emit {
				emits++
			}
			if dec.Cleared {
				clears++
			}
			recordSlotHealthEmission(st, dec, at)
		}

		for i := 0; i < 10; i++ {
			at := fixedNow.Add(time.Duration(i) * 30 * time.Second)
			if i%2 == 0 {
				tick(hot, at)
			} else {
				tick(cool, at)
			}
		}
		if emits != 1 {
			t.Errorf("clean↔70 flap inside the rate-limit window emitted %d times; want 1", emits)
		}
		if clears != 0 {
			t.Errorf("clean↔70 flap announced %d clears while still flapping; want 0", clears)
		}
	})
}

// TestEvaluateSlotHealth_GenuineDowngradeEmitsAfterPersist pins the
// other side of the hysteresis: a REAL downgrade (85 → sustained 70)
// still reaches the operator — one probe tick later.
func TestEvaluateSlotHealth_GenuineDowngradeEmitsAfterPersist(t *testing.T) {
	st := newSlotHealthState(fixedNow)
	thr := defaultThresholds()

	dec1 := evaluateSlotHealth(ir.SlotHealth{Active: true, LagBytes: 90, MaxKeepSizeBytes: 100}, st, thr, fixedNow)
	if dec1.Kind != slotWarnRetention85 || !dec1.Emit {
		t.Fatalf("tick1: want critical85 emit; got %+v", dec1)
	}
	recordSlotHealthEmission(st, dec1, fixedNow)

	lo := ir.SlotHealth{Active: true, LagBytes: 75, MaxKeepSizeBytes: 100}
	t2 := fixedNow.Add(30 * time.Second)
	dec2 := evaluateSlotHealth(lo, st, thr, t2)
	if dec2.Emit {
		t.Fatalf("tick2: first downgraded tick must be held; got %+v", dec2)
	}
	recordSlotHealthEmission(st, dec2, t2)

	t3 := fixedNow.Add(1 * time.Minute)
	dec3 := evaluateSlotHealth(lo, st, thr, t3)
	if dec3.Kind != slotWarnRetention70 || !dec3.Emit {
		t.Errorf("tick3: persisted downgrade must emit; got %+v", dec3)
	}
}
