// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item 138 gates: the coordinated grow-pause must not livelock a cold copy.
//
// # What these gates reach, and what they do not
//
// They exercise the CONCRETE coordinator [GrowGate] — the only production
// implementor of [ir.GrowGate] — driving it exactly as a cold-copy lane does
// (Await before an attempt, Trip on a classified transient). They do NOT
// exercise the lane wiring: that a MySQL/Postgres bulk-write lane actually
// reaches the gate is a separate, already-existing roster gate
// (mysql.TestEveryMySQLBulkWriteLaneReachesTheGrowGate), and these tests
// would stay green if every lane stopped calling the gate tomorrow.
//
// # Why the pair, in both directions
//
// The defect these close is a LIVELOCK: correct in the small, stalled in the
// large, every log line healthy. A gate that only proved "the gate closes"
// would have passed on the shipped v0.111.1 behaviour that quiesced 81.1% of
// a 91-minute field run. So the property is stated as a PAIR, and neither
// half is sufficient:
//
//   - a sustained drop storm must leave the copy most of the wall clock
//     ([TestGrowGate_SustainedDropStormLeavesTheCopyMostOfTheWallClock]), and
//   - a sustained genuine grow must STILL quiesce meaningfully
//     ([TestGrowGate_SustainedTroubleStillEscalatesToAMeaningfulQuiesce]) —
//     the anti-vacuity half, which fails if anyone "fixes" the livelock by
//     pinning the hold at its base or gutting the gate.
//
// [TestGrowGate_HoldIsInvariantToTheNumberOfLanesReportingOneEvent] is the
// direct regression pin on the root cause.

package migcore

import (
	"context"
	"sync"
	"testing"
	"time"
)

// withScaledGrowGate shrinks the whole timing envelope — ladder, per-window
// ceiling, episode idle, and the run-level quiesce share window — by a
// constant factor so these gates run in seconds while preserving every RATIO
// the production envelope has. Ratios are what the properties are about.
func withScaledGrowGate(t *testing.T, base, capHold, episodeIdle, quiesceWindow time.Duration, share float64) {
	t.Helper()
	oBase, oCap, oHold := GrowGateBackoffBase, GrowGateBackoffCap, GrowGateMaxHold
	oIdle, oWin, oShare := GrowGateEpisodeIdle, GrowGateQuiesceWindow, GrowGateMaxQuiesceShare
	GrowGateBackoffBase = base
	GrowGateBackoffCap = capHold
	GrowGateMaxHold = capHold * 100
	GrowGateEpisodeIdle = episodeIdle
	GrowGateQuiesceWindow = quiesceWindow
	GrowGateMaxQuiesceShare = share
	t.Cleanup(func() {
		GrowGateBackoffBase, GrowGateBackoffCap, GrowGateMaxHold = oBase, oCap, oHold
		GrowGateEpisodeIdle, GrowGateQuiesceWindow, GrowGateMaxQuiesceShare = oIdle, oWin, oShare
	})
}

// tripBurst reports one target event as n lanes would: n Trips staggered by a
// few hundred microseconds, the spacing the 2026-08-05 field log shows for a
// W×D fan-out all failing on the same event (a 13–16 trip burst spread over
// ~4ms). The stagger is LOAD-BEARING for the pins below: a burst fired with
// zero spacing is absorbed by any single-slot coalescing buffer and would let
// the pre-fix code pass.
func tripBurst(g *GrowGate, n int) {
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() { defer wg.Done(); g.Trip("lane transient") }()
		time.Sleep(tripBurstStagger)
	}
	wg.Wait()
}

// tripBurstStagger keeps a whole burst far shorter than any window these
// tests use, so a burst can never straddle a close→reopen boundary and
// misalign the window sequence being measured. It must stay well ABOVE the
// gate owner's loop latency (microseconds): a burst fired with no spacing at
// all is absorbed by a single-slot coalescing buffer, which would let the
// pre-fix per-trip ladder pass these gates. Verified in both directions by
// the item-138 mutation runs.
const tripBurstStagger = 150 * time.Microsecond

// TestGrowGate_HoldIsInvariantToTheNumberOfLanesReportingOneEvent is the
// direct item-138 regression pin.
//
// ONE target event observed by N cold-copy lanes must produce the same
// quiesce as one lane observing it. N is fan-out width — 16 on the sync
// cold-start native-concurrent path (W=4 readers × D=4 write fan-out), up to
// 32 on migrate — and it says nothing about the target.
//
// Before the fix the hold was backoff(N): measured across 246 windows of the
// field log it matched to the millisecond (0.100s at 1 trip, 0.81s at 4,
// 12.87s at 8, 30.0s at ≥14 — the 30s CAP, reached in ~4 milliseconds).
//
// TWO consecutive windows are measured, not one, and that is deliberate.
// A mutation run found the single-window version blind to half the defect:
// a Trip that advances the episode ladder while the gate is CLOSED leaves
// the LIVE window's hold untouched (the owner takes its hold by value) and
// shows up only in the NEXT window. Measuring window 1 alone passed that
// mutant. Both windows must be fan-out-invariant.
func TestGrowGate_HoldIsInvariantToTheNumberOfLanesReportingOneEvent(t *testing.T) {
	captureSlog(t)
	// The base is two orders of magnitude above the widest burst's own span
	// (32 lanes × 150µs ≈ 5ms) so a burst cannot straddle a window boundary
	// and misalign the sequence; the cap is far above the base so a per-trip
	// ladder has room to escalate visibly rather than being clipped.
	withScaledGrowGate(t, 200*time.Millisecond, 8*time.Second, time.Hour, time.Hour, 1.0)

	// held[lanes] = the first two window holds produced by bursts of that width.
	held := make(map[int][]time.Duration)
	for _, lanes := range []int{1, 4, 16, 32} {
		g := NewGrowGate(context.Background(), nil)
		done := make(chan time.Duration, 8)
		g.onWindowClosed = func(d time.Duration) {
			select {
			case done <- d:
			default:
			}
		}
		for range 2 {
			tripBurst(g, lanes)
			select {
			case d := <-done:
				held[lanes] = append(held[lanes], d)
			case <-time.After(60 * time.Second):
				t.Fatalf("lanes=%d: the gate never reopened", lanes)
			}
		}
		// Straddle guard: two bursts must have produced exactly two windows.
		// A burst that outran its own window would leave an extra one queued
		// and silently misalign the per-index comparison below — a wrong
		// answer dressed as a right one. Fail loudly instead.
		if extra := len(done); extra != 0 {
			t.Fatalf("lanes=%d: %d unexpected extra window(s) — a burst straddled a window boundary, "+
				"so the measurement is misaligned; widen the base relative to the burst span", lanes, extra)
		}
	}

	// Generous slack: the property is "does not scale with N", and a per-trip
	// ladder makes 32 lanes ~2^31 × one lane (clipped to the cap, here 100×
	// the base). Anything within 3× of the single-lane hold for the SAME
	// window index is unambiguously not escalating with fan-out.
	for window := range 2 {
		one := held[1][window]
		for _, lanes := range []int{4, 16, 32} {
			got := held[lanes][window]
			if got > 3*one {
				t.Errorf(
					"window %d: one target event reported by %d lanes quiesced the copy for %v, but by 1 lane "+
						"for %v — the hold is escalating with FAN-OUT WIDTH, not with how long the target has "+
						"been bad (item 138)",
					window+1, lanes, got, one,
				)
			}
		}
	}
}

// TestGrowGate_SustainedDropStormLeavesTheCopyMostOfTheWallClock is the
// livelock gate, stated as the field shape: a target that rejects every
// attempt, with a full-width lane fan-out re-tripping the instant the gate
// reopens, must not hold the copy closed for more than the gate's declared
// share of the wall clock.
//
// The INDEPENDENT expected value this compares against is the wall clock
// measured by the test, not anything the gate reports about itself: the
// gate's own ledger is the thing under test, so the pass/fail number is
// summed from the observed close→reopen durations against elapsed real time.
//
// The shipped v0.111.1 gate scored 81.1% closed on this shape in the field.
func TestGrowGate_SustainedDropStormLeavesTheCopyMostOfTheWallClock(t *testing.T) {
	captureSlog(t)
	const (
		share  = 0.5
		window = 300 * time.Millisecond
		run    = 3 * time.Second
	)
	// episodeIdle far above any gap the storm produces, so the ladder is NEVER
	// reset by luck — the storm must be bounded by the share ceiling alone.
	withScaledGrowGate(t, 5*time.Millisecond, 60*time.Millisecond, time.Hour, window, share)

	g := NewGrowGate(context.Background(), nil)
	var mu sync.Mutex
	var closedTotal time.Duration
	g.onWindowClosed = func(d time.Duration) {
		mu.Lock()
		closedTotal += d
		mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), run)
	defer cancel()

	// 16 lanes: Await, "attempt" (which always fails against this target),
	// Trip. Exactly the cold-copy lane loop.
	var lanes sync.WaitGroup
	for range 16 {
		lanes.Add(1)
		go func() {
			defer lanes.Done()
			for ctx.Err() == nil {
				if err := g.Await(ctx); err != nil {
					return
				}
				time.Sleep(time.Millisecond) // the failing attempt
				g.Trip("target rejecting writes")
			}
		}()
	}
	start := time.Now()
	lanes.Wait()
	elapsed := time.Since(start)

	mu.Lock()
	closed := closedTotal
	mu.Unlock()

	got := float64(closed) / float64(elapsed)
	// Slack for the window that is still open when the run ends plus timer
	// granularity; the failure this guards against scored 0.81.
	if got > share+0.15 {
		t.Errorf(
			"a target dropping every attempt held the copy quiesced %.1f%% of the wall clock (%v of %v); "+
				"the gate declares a ceiling of %.0f%% — a sustained storm must not livelock the copy (item 138)",
			100*got, closed, elapsed, 100*share,
		)
	}
}

// TestGrowGate_SustainedTroubleStillEscalatesToAMeaningfulQuiesce is the
// ANTI-VACUITY half, and the reason the item-138 fix is not "shorten the
// quiesce". A real storage-grow reparent is a genuine hazard: continuing to
// write into the serving transition is actively harmful, and ADR-0110 exists
// to quiesce every lane for it. A gate that reopened promptly forever would
// pass the storm test above and would have deleted the feature.
//
// So: trouble that PERSISTS must climb the ladder to a hold materially longer
// than the base probe interval, within a small number of windows.
func TestGrowGate_SustainedTroubleStillEscalatesToAMeaningfulQuiesce(t *testing.T) {
	captureSlog(t)
	const base = 5 * time.Millisecond
	// Share ceiling wide open: this test is about escalation, and the storm
	// test above is what bounds it. Keeping them separate means neither can
	// mask the other.
	withScaledGrowGate(t, base, 320*time.Millisecond, time.Hour, time.Hour, 1.0)

	g := NewGrowGate(context.Background(), nil)
	holds := make(chan time.Duration, 32)
	g.onWindowClosed = func(d time.Duration) {
		select {
		case holds <- d:
		default:
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const windows = 8
	var longest time.Duration
	for range windows {
		tripBurst(g, 4)
		if err := g.Await(ctx); err != nil {
			t.Fatalf("Await: %v", err)
		}
		select {
		case d := <-holds:
			if d > longest {
				longest = d
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no window observed")
		}
	}

	// 8 consecutive re-tripped windows reach rung 8 (base × 2^7), clipped by
	// the scaled cap of 64 × base. Requiring 32 × base leaves headroom for
	// timer granularity while still being 32× the base probe interval — a
	// hold that is unambiguously a coordinated quiesce rather than a probe.
	if want := 32 * base; longest < want {
		t.Errorf(
			"after %d consecutive windows of persistent trouble the longest quiesce was %v, want >= %v — "+
				"the ladder is not climbing across windows, so a genuine sustained storage-grow reparent "+
				"would get only the base probe interval (ADR-0110's whole purpose)",
			windows, longest, want,
		)
	}
}

// TestGrowGate_EpisodeLadderClimbsPerWindowAndResetsAfterAHealthyStretch pins
// the ladder's two transitions directly, which the two behavioural gates above
// only observe in aggregate: one rung per window while trouble persists, and a
// reset to the base probe interval once the gate has been open and un-tripped
// for GrowGateEpisodeIdle.
//
// The reset half matters on its own: without it the first bad minute of a
// multi-hour run would leave the gate permanently at its 30s cap, so a single
// early blip would cost every later blip a full cap-length stall.
func TestGrowGate_EpisodeLadderClimbsPerWindowAndResetsAfterAHealthyStretch(t *testing.T) {
	captureSlog(t)
	const (
		base = 20 * time.Millisecond
		idle = 200 * time.Millisecond
	)
	withScaledGrowGate(t, base, 10*time.Second, idle, time.Hour, 1.0)

	g := NewGrowGate(context.Background(), nil)
	holds := make(chan time.Duration, 8)
	g.onWindowClosed = func(d time.Duration) { holds <- d }

	next := func() time.Duration {
		select {
		case d := <-holds:
			return d
		case <-time.After(5 * time.Second):
			t.Fatal("no window observed")
			return 0
		}
	}

	// Three consecutive windows, each re-tripped promptly: rungs 1, 2, 3.
	climbed := make([]time.Duration, 0, 3)
	for range 3 {
		tripBurst(g, 3)
		if err := g.Await(context.Background()); err != nil {
			t.Fatalf("Await: %v", err)
		}
		climbed = append(climbed, next())
	}
	for i := 1; i < len(climbed); i++ {
		if climbed[i] <= climbed[i-1] {
			t.Errorf("window %d held %v, not longer than window %d's %v — the ladder is not climbing across windows",
				i+1, climbed[i], i, climbed[i-1])
		}
	}

	// Now a healthy stretch longer than the episode idle, then trouble returns.
	time.Sleep(idle + 100*time.Millisecond)
	tripBurst(g, 3)
	if err := g.Await(context.Background()); err != nil {
		t.Fatalf("Await: %v", err)
	}
	afterReset := next()
	if afterReset > 3*base {
		t.Errorf(
			"trouble returning after a healthy stretch quiesced for %v, want ~%v — the episode ladder did not "+
				"reset, so one early bad patch permanently taxes every later one",
			afterReset, base,
		)
	}
}

// TestGrowGate_DeclinesToCloseOnceItsShareIsSpent pins the run-level ceiling's
// mechanism, and specifically that spending it hands the run BACK to the lanes
// rather than blocking them: Await must be instant while the share is spent.
//
// What this does and does not bind. It binds the gate half of the safety
// argument in [GrowGateMaxQuiesceShare]'s doc — declining to close is a
// released lane, not a stalled one. It does NOT bind the other half, that the
// released lane then waits on its OWN exponential reparent backoff rather than
// hammering; that lives in the engine packages (mysql's
// coldCopyReparentBackoff / postgres's pgCopyReparentBackoff, pinned by
// mysql.TestColdCopyReparentBackoffShape and
// postgres.TestPGCopyReparentBackoffShape) and is not reachable from here.
// Stated rather than implied, because a reader who assumed this test covered
// both would be wrong.
//
// And it binds NOTHING about the four lanes that have no backoff to be handed
// back to — see the roster in [GrowGateMaxQuiesceShare]'s doc and the two
// engine-side posture rosters that derive it. When this test was written its
// sibling comment generalised over "every lane"; four of nine do not fit.
func TestGrowGate_DeclinesToCloseOnceItsShareIsSpent(t *testing.T) {
	captureSlog(t)
	const window = 400 * time.Millisecond
	withScaledGrowGate(t, 40*time.Millisecond, 40*time.Millisecond, time.Hour, window, 0.5)

	g := NewGrowGate(context.Background(), nil)

	// Spend the share: at a 40ms hold, 0.5 × 400ms = 200ms of budget is gone
	// after five windows.
	deadline := time.Now().Add(5 * time.Second)
	closes := 0
	for closes < 12 && time.Now().Before(deadline) {
		g.Trip("target rejecting writes")
		g.mu.Lock()
		closed := g.closed
		g.mu.Unlock()
		if !closed {
			break // the gate declined — the share is spent
		}
		closes++
		if err := g.Await(context.Background()); err != nil {
			t.Fatalf("Await: %v", err)
		}
	}
	if closes >= 12 {
		t.Fatalf("the gate closed %d times without ever declining; the run-level quiesce share is not bounding it", closes)
	}

	// While the share is spent, a Trip must leave the gate OPEN and Await must
	// return immediately — the lane proceeds on its own retry budget.
	g.Trip("target still rejecting writes")
	g.mu.Lock()
	stillClosed := g.closed
	g.mu.Unlock()
	if stillClosed {
		t.Fatal("the gate closed after declaring its share spent")
	}
	done := make(chan error, 1)
	go func() { done <- g.Await(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Await returned %v, want an instant nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Await blocked while the gate's share was spent — declining to close must RELEASE lanes, not park them")
	}
}

// TestGrowGate_DeclinedTripDoesNotAdvanceTheEpisodeLadder is the item-138
// regression pin for the DECLINING state, the half the first cut of the
// run-level ceiling left open.
//
// The ladder's whole meaning is "how many WINDOWS has this episode already
// spent without the trouble clearing" — [GrowGate.cycle]'s doc, the type doc,
// and Trip's doc all say per-window. A trip the share ceiling declines arms no
// window, so it is not evidence of temporal persistence; it is evidence that
// some lane failed while the gate happened to be out of budget. Advancing on
// it lets ONE target event observed by a W×D fan-out climb the ladder by the
// fan-out's WIDTH in the few milliseconds the burst spans — which is exactly
// the defect item 138 was written to remove, reproduced inside the code that
// removed it. Measured cost when it fires: the moment budget frees up the gate
// arms at or near its 30s cap instead of the fast probe interval, so a run
// whose ladder should have been at rung 1 pays cap-length holds for the rest
// of the episode.
//
// The pin is white-box on g.cycle deliberately. Observing it through hold
// durations is what the two duty gates already do, and the allowance clamp
// (hold = min(backoff(cycle), allowance)) hides the rung precisely in the
// state under test — measuring the observable would have been blind to it.
//
// BOTH directions, per the mutation-run rule: the climb before the decline is
// asserted too, so freezing the ladder outright fails this test rather than
// passing it.
func TestGrowGate_DeclinedTripDoesNotAdvanceTheEpisodeLadder(t *testing.T) {
	captureSlog(t)
	const window = 400 * time.Millisecond
	// episodeIdle an hour: the ladder must never reset by luck mid-test, so a
	// rung that fails to advance can only be the arming rule and not a reset.
	withScaledGrowGate(t, 40*time.Millisecond, 40*time.Millisecond, time.Hour, window, 0.5)

	g := NewGrowGate(context.Background(), nil)
	rung := func() int {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.cycle
	}
	isClosed := func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.closed
	}

	// Drive windows until the share is spent, asserting the ladder climbs by
	// exactly one per ARMED window on the way (the anti-vacuity half).
	windows := 0
	deadline := time.Now().Add(5 * time.Second)
	for windows < 12 && time.Now().Before(deadline) {
		before := rung()
		g.Trip("target rejecting writes")
		if !isClosed() {
			break // declined — the share is spent
		}
		if got := rung(); got != before+1 {
			t.Fatalf("armed window %d advanced the ladder from %d to %d, want exactly one rung", windows+1, before, got)
		}
		windows++
		if err := g.Await(context.Background()); err != nil {
			t.Fatalf("Await: %v", err)
		}
	}
	if windows == 0 {
		t.Fatal("the gate never closed; this test cannot reach the declining state it exists to pin")
	}
	if windows >= 12 {
		t.Fatalf("the gate closed %d times without ever declining; the run-level share is not bounding it", windows)
	}

	// Now the state under test: the share is spent and the gate is declining.
	// One target event, reported by 16 lanes — the sync cold-start W×D fan-out
	// from the field log — must move the ladder by NOTHING.
	spent := rung()
	tripBurst(g, 16)
	if isClosed() {
		t.Fatal("the gate closed after declaring its share spent; this test is no longer measuring the declining state")
	}
	if got := rung(); got != spent {
		t.Errorf(
			"16 lanes reporting ONE event while the share was spent climbed the episode ladder from rung %d to %d. "+
				"A declined trip arms no window, so it is not evidence the trouble persisted — and a ladder driven by "+
				"fan-out WIDTH is the item-138 defect. When budget frees up the gate will now arm at rung %d "+
				"(near its cap) instead of resuming the fast probe.",
			spent, got, got,
		)
	}
}
