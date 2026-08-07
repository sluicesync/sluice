// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Coordinated cold-copy grow-window pause (ADR-0110)
//
// The PROACTIVE deepening of the v0.99.92–v0.99.99 reactive storage-grow
// arc (ADR-0108 cold-copy reparent-retry, ADR-0109 source-read-resume).
//
// A non-Metal PlanetScale MySQL volume auto-grows in steps; each step
// opens a serving-transition / reparent window during which the target
// rejects writes. The reactive arc made every face retriable so the
// cold-copy RIDES THROUGH a grow — but inefficiently: during the
// multi-minute window all ~16 cold-copy lanes (W tables × D fan-out)
// independently hammer-retry the struggling target, prolonging the grow
// and breeding secondary 1205 lock-wait-timeouts. The target completes
// the transition faster if left alone.
//
// # WHAT THE GATE ACTUALLY TRIPS ON, versus what that paragraph describes
//
// The paragraph above is the argument this gate was designed around, and it is
// an argument about a SERVING TRANSITION. It is not the situation the gate
// spends most of its time in, and roadmap item 143 exists because nothing said
// so. The gate trips on the engine's whole retriable-transient class, and by
// two independent datasets that class is dominated by plain transport drops:
// 244 of the 246 windows in the 2026-08-05 field log were opened by
// `vtgate connection error: no endpoints`, with ZERO opened by a
// reparent-evidenced face; and across ~870 absorbed drops over six A/B runs on
// real PlanetScale, none carried reparent evidence either.
//
// The trip set is unchanged, deliberately. What a real grow looks like on the
// wire is not reliably separable from a plain drop at the trip point — a
// grow's most common face on an in-flight write is the connection dying — and
// the corroborating observations are not available where the decision is made:
// the tier probe is a one-shot preflight capacity read, and the telemetry
// sidecar is opt-in (`--planetscale-org`), absent entirely on `migrate` and
// `restore`, lagging by up to a poll interval plus its freshness window, and
// documented right here (see [GrowGate.runOwner]) as transiently DISAPPEARING
// across the very reparent it would corroborate. So the gate cannot be made
// evidence-GATED; what it can do is stop pretending, which is
// [ir.GrowEvidence].
//
// # So why quiesce on a plain transport drop at all
//
// Two reasons, and neither is the paragraph above — writing them down is half
// of what item 143 delivered:
//
//   - ALIGNMENT for the lane that tripped: its hold overlaps the per-lane
//     backoff it would have taken anyway, so the target gets contiguous quiet
//     rather than a smear of retries. Note the scope, because the earlier
//     wording of this claim was broader than the truth: it holds for the lane
//     that hit the transient. For a SIBLING lane that was mid-successful-copy
//     the hold is not alignment, it is added latency — [GrowGate.Await] is
//     consulted before EVERY attempt, including the first, so a healthy lane
//     parks too.
//   - A probabilistic SHIELD for the four bulk-write lanes that have no replay
//     (see [GrowGateMaxQuiesceShare]'s roster): while the gate is closed they
//     do not issue a write into a window where it would fail loudly. That is a
//     real benefit and it is unrelated to whether the target is growing.
//
// And the cost, quantified rather than assumed, because item 138 changed it:
// the run-level ceiling means a sustained storm converges on
// [GrowGateMaxQuiesceShare] of the wall clock — half, not the 81% that started
// this — and item 138's real-PlanetScale validation measured that ceiling
// actually engaging (150.1 s of quiesce per 300 s window, in both runs). Half
// of a copy is not a rounding error, so this is a live tension and not a
// settled question; what it is NOT is a silent-loss risk, since the gate never
// swallows an error and each lane's own bounded retry remains the
// loud-on-exhaustion floor.
//
// GrowGate is ONE engine-neutral coordinated-pause primitive shared
// across every cold-copy write lane in a run, tripped from two sources
// driving the SAME mechanism:
//
//   - Signal-driven (the universal floor): the FIRST classified
//     grow-transient on any write lane or source-read attempt trips the
//     gate; all sibling lanes quiesce together for a coordinated window,
//     then resume. Works for ANY storage-auto-grow / transient-reparent
//     target — non-PlanetScale included — because the trigger is the
//     classified transient itself, not a PS-specific metric.
//   - Telemetry-driven (precision enhancement): the Item-32 storage-
//     headroom sidecar trips the SAME gate PROACTIVELY, before lanes
//     start hitting transients, when storage heads toward the grow
//     boundary. Advisory: a no-metrics run still rides through via the
//     signal path, just less efficiently.
//
// It implements [ir.GrowGate]. It is constructed UNCONDITIONALLY for a
// cold-copy run (so there is no EnableX-defaulting-true config bool — the
// v0.99.51 zero-value trap — and the default for a non-PlanetScale /
// no-config run is "signal-driven gate active"): with no trip source
// firing it is inert (Await fast-paths the open read; the owner goroutine
// is only spawned by the first Trip). A nil *GrowGate is threaded as a
// nil [ir.GrowGate] via [GrowGateOrNil] so the typed-nil trap can't make a
// nil concrete value look like a non-nil interface.
//
// What it does NOT do (the correctness contract, unchanged from
// ADR-0108/0109): it NEVER swallows a terminal error, advances a
// position, or marks a table complete. It only changes HOW a wait is
// spent — coordinated-and-calm vs independent-and-hammering. The per-lane
// flushWithReparentRetry / source-read retry budgets remain the
// authoritative loud-on-exhaustion floor; the gate has its own max-hold
// so a genuinely-dead target still surfaces rather than parking forever.

package migcore

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Cold-copy coordinated-pause bounds. The backoff SHAPE mirrors
// ADR-0108/0109's per-lane retry envelope (100ms → … → 30s cap) so the
// coordinated pause window matches the retry window the lanes already
// tolerate. GrowGateMaxHold bounds the total coordinated pause: after
// this much continuous quiesce the gate force-reopens so each lane's own
// retry budget (the authoritative floor) can take over and surface a
// genuinely-dead target loudly. Each close→reopen cycle holds for one
// growGateBackoff interval before reopening to let the lanes probe.
//
// These are package vars (not consts) ONLY so the unit tests can shrink
// the time envelope to keep the suite fast and deterministic — production
// NEVER mutates them, so the zero-value-safe-default reasoning is
// unaffected (there is no config field, no zero-value path; the values
// are baked at package init). This is the same vars-not-consts discipline
// the cold-copy reparent-retry / source-read-retry helpers use.
//
// CRITICAL (the -race lesson, v0.99.100): these globals are read ONLY at
// gate CONSTRUCTION — NewGrowGate snapshots them into per-instance fields
// (backoffBase / backoffCap / maxHold). The owner goroutine reads the
// gate's OWN fields, never these globals, so a test's shrink-then-restore
// of a global can never race a still-running owner from an earlier gate.
// The first cut read GrowGateBackoffCap directly in runOwner and the
// -race gate caught it racing a test's t.Cleanup restore; snapshotting at
// construction removes the shared-mutable-global read from the hot path.
var (
	GrowGateBackoffBase = 100 * time.Millisecond
	GrowGateBackoffCap  = 30 * time.Second
	// GrowGateMaxHold ≈ the reparent-retry envelope (~15-20 min): long
	// enough to ride a prolonged multi-step storage-grow, short enough
	// that a genuinely-wedged target surfaces via the lanes' own budgets
	// rather than hiding behind a forever-closed gate.
	//
	// SCOPE, stated because its name reads broader than it is: this bounds
	// ONE window. It is NOT a bound on how long the gate may stall a run —
	// consecutive windows each get a fresh max-hold. The run-level bound is
	// [GrowGateMaxQuiesceShare]; see [GrowGate.quiesceAllowanceLocked].
	GrowGateMaxHold = 20 * time.Minute

	// GrowGateEpisodeIdle is the gate-OPEN, un-tripped stretch that ends a
	// trouble EPISODE and resets the escalation ladder, so the next window the
	// gate arms is rung 1 again (the reset is applied on the trip; the rung is
	// taken when a window is actually armed — see [GrowGate.Trip]). Trouble
	// that returns sooner than this is treated as the same episode
	// continuing, so the ladder climbs; trouble that returns after a
	// genuinely healthy stretch starts over at the fast probe interval.
	GrowGateEpisodeIdle = 60 * time.Second

	// GrowGateQuiesceWindow / GrowGateMaxQuiesceShare are the RUN-level
	// ceiling on coordinated quiesce: over any trailing GrowGateQuiesceWindow
	// the gate may hold the cold copy closed for at most GrowGateMaxQuiesceShare
	// of that window. Past that it declines to close at all and each lane
	// carries itself.
	//
	// # Why capping the share does not reintroduce the thrashing (the lanes
	// that retry)
	//
	// ADR-0110's cost case is that ~W×D lanes "independently hammer-retry the
	// struggling target". The retrying lanes do not hammer: each runs its own
	// exponential reparent backoff (100ms → 30s cap), and the 2026-08-05 field
	// log shows 1061 of 2195 lane retries already sitting at the 30s cap. So
	// for those lanes a gate that declines to close hands the target back at
	// roughly one try per 30s each — the gate's marginal value there is
	// ALIGNMENT (contiguous quiet rather than a smear), not rate limiting.
	//
	// The BACKOFF half of that argument is pinned where the backoff lives:
	// [mysql.TestColdCopyReparentBackoffShape] and
	// [postgres.TestPGCopyReparentBackoffShape] assert both ladders reach and
	// hold the 30s cap. The GATE half — that declining RELEASES a lane rather
	// than parking it — is [TestGrowGate_DeclinesToCloseOnceItsShareIsSpent].
	//
	// # WHICH LANES, because the argument above is FALSE for four of them
	//
	// It was written as a generalisation over "every lane" and cited a test
	// that was never written. Four of the nine bulk-write lanes have no retry
	// to fall back on, so for them a decline is not "back to one try per 30s",
	// it is "the next transient this lane meets is terminal":
	//
	//	RETRY (bounded replay, then loud)   REFUSE-KEYLESS   NO RETRY
	//	pg  writeViaCopyChunked             yes              —
	//	pg  writeViaBatchIdempotent         n/a (refused up front)
	//	my  writeBatchedConn                yes              —
	//	my  writeBatchedIdempotentConn      yes              —
	//	my  flushLoadDataSegment (item 114) yes              —
	//	pg  writeViaBatch                   —                YES (ambiguous ack, no conflict target)
	//	pg  ImportRawCopy                   —                YES (one-shot io.Reader)
	//	pg  pgFloatBatchExecer.ExecBatch    —                YES (Await+Trip parity only)
	//	my  mysqlFloatBatchExecer.ExecBatch —                YES (Await+Trip parity only)
	//
	// The roster is DERIVED, not asserted here: mysql.TestMySQLGrowGateLane-
	// RetryPostureRoster and postgres.TestPostgresGrowGateLaneRetryPosture-
	// Roster walk each engine package, discover its bulk-write lanes by
	// signature, derive each lane's posture from whether it reaches the
	// bounded-replay helper, and fail on a lane whose declared posture is
	// wrong or missing. A tenth lane cannot appear unclassified.
	//
	// The exposure those four carry, stated plainly rather than left implied:
	// while the share is spent, a lane that would have PARKED instead issues
	// its write into the bad window and fails. That failure is LOUD on all
	// four (a wrapped driver error out of writeViaBatch / the float-repair
	// cores, and ImportRawCopy's explicit "no resume point, re-run this table"
	// refusal) — never a silent under-copy — which is why the ceiling stays.
	// Note also what the gate never was for them: it is a probabilistic SHIELD,
	// not a floor. A non-retrying lane that is the FIRST to meet a transient
	// dies whatever the gate does; it is only protected when a sibling trips
	// first and it has not yet issued its write. Capping the share narrows
	// that shield, it does not remove a guarantee that existed.
	//
	// (Two of the four have an outer net on SOME callers — ImportRawCopy's
	// migrate whole-table leg rides the pipeline's truncate-restart source-read
	// retry — but not on the chunked leg or sync cold-start. That asymmetry is
	// filed in docs/dev/perf-parity-matrix.md row 17, not re-argued here.)
	GrowGateQuiesceWindow   = 5 * time.Minute
	GrowGateMaxQuiesceShare = 0.5
)

// growGateMaxCycle bounds the escalation ladder's integer so a long-running
// episode cannot grow it without limit (backoff() saturates at its cap long
// before this; the bound exists so backoff()'s loop stays O(1)-ish and the
// counter cannot overflow on a multi-day run).
const growGateMaxCycle = 32

// GrowGate is the concrete [ir.GrowGate] coordinator. State is open/closed
// guarded by mu plus a closed-channel-broadcast reopenCh (re-created on
// each close→open) so Await can park without holding mu across the block.
//
// # The escalation model (item 138)
//
// A pause WINDOW is one close→reopen of exactly backoff(cycle). Three
// separate things govern how much of a run the gate may quiesce, and they
// answer three different questions:
//
//   - cycle — the EPISODE ladder. How long should THIS window be? It
//     advances one rung per window while trouble persists and resets after
//     GrowGateEpisodeIdle of healthy open time. It is deliberately sized to
//     track the per-lane reparent-retry ladder (same 100ms → 30s shape), so
//     the gate's hold OVERLAPS the backoff the TRIPPING lane would have taken
//     anyway. Scope correction (item 143): that is alignment for the lane that
//     hit the transient, and it is NOT true of a sibling that was copying
//     successfully — [GrowGate.Await] runs before EVERY attempt, so a healthy
//     lane's hold is added latency, not overlap. The package doc carries the
//     full accounting of what the quiesce buys and costs.
//   - maxHold — the per-WINDOW ceiling. Bounds one window only.
//   - maxQuiesceShare over quiesceWindow — the RUN-level ceiling, and the
//     only one of the three that can see a run being quiesced to death by a
//     long sequence of individually-reasonable windows. That is exactly what
//     the 2026-08-05 field report was.
//
// What does NOT govern it, and this is the item-138 correction: the NUMBER
// of lanes reporting a fault. See [GrowGate.Trip].
//
// It satisfies [ir.GrowGate] and, for the item-146 copy watchdog,
// [ir.GrowGateQuiesceObserver] — both asserted on the REAL type just below,
// not on a test stub that would satisfy them by construction.
type GrowGate struct {
	mu sync.Mutex

	// closed is the gate state. When false (the common case) Await returns
	// immediately. When true a pause is in effect and Await parks on
	// reopenCh / ctx.Done().
	closed bool

	// reopenCh is closed (broadcast) when the gate reopens, waking every
	// parked Await. It is re-created on each close so a fresh pause window
	// has a fresh channel. nil until the first close.
	reopenCh chan struct{}

	// cycle is the escalation ladder's rung, EPISODE-scoped: it advances by
	// exactly ONE per pause WINDOW while a trouble episode persists, and
	// resets when the gate has been open and un-tripped for episodeIdle. Read
	// the type doc for why it may not advance per Trip. "Per WINDOW" is the
	// whole invariant and it has two ways to break: a trip arriving DURING a
	// window (pinned by
	// [TestGrowGate_HoldIsInvariantToTheNumberOfLanesReportingOneEvent]) and a
	// trip the share ceiling DECLINES, which also arms no window (pinned by
	// [TestGrowGate_DeclinedTripDoesNotAdvanceTheEpisodeLadder]). The first
	// cut of the ceiling closed only the first of those.
	cycle int

	// lastReopen is when the previous window ended. A Trip arriving within
	// episodeIdle of it continues the episode (the ladder climbs); a Trip
	// arriving later starts a fresh episode at rung 1.
	lastReopen time.Time

	// quiesced is the trailing ledger of closed spans backing the
	// run-level quiesce-share ceiling. Pruned to quiesceWindow on every
	// consultation, so it holds O(quiesceWindow / backoffCap) entries.
	quiesced []quiesceSpan

	// shareExhaustedLogged suppresses the "declining to close" WARN to once
	// per episode rather than once per Trip (the field shape is thousands of
	// trips per episode). Cleared when a new episode starts.
	shareExhaustedLogged bool

	// recovered, when non-nil, is consulted by a PROACTIVE (telemetry)
	// pause: the owner reopens on the earlier of {max-hold | recovered()
	// reporting the storage headroom has recovered}. nil ⇒ the owner only
	// uses its probe-cycle + max-hold timing (the signal-driven case).
	recovered func() bool

	// reason is the most recent Trip reason, for the structured log.
	reason string

	// evidence is the most recent Trip's verdict — what the tripping lane
	// actually OBSERVED, not what it suspects (roadmap item 143). Carried
	// alongside reason and logged with it; it never influences any timing
	// decision, which is the property [TestGrowGate_EvidenceIsDescriptive-
	// OnlyAndNeverChangesTheHold] pins.
	evidence ir.GrowEvidence

	// owner scopes the owner goroutine to the gate's lifetime ctx so it
	// exits on run-ctx cancel and on the gate's own Close. Captured at
	// construction.
	ownerCtx context.Context

	// backoffBase / backoffCap / maxHold are the gate's OWN copy of the
	// timing envelope, snapshotted from the package defaults at
	// construction (NewGrowGate). The owner goroutine reads these — never
	// the package globals — so a test that shrinks a global and restores it
	// on cleanup can never race a still-running owner (the v0.99.100 -race
	// lesson). Production always gets the package defaults; tests shrink a
	// global before constructing the gate they then drive.
	backoffBase     time.Duration
	backoffCap      time.Duration
	maxHold         time.Duration
	episodeIdle     time.Duration
	quiesceWindow   time.Duration
	maxQuiesceShare float64

	// clock-injection seams for deterministic tests; nil ⇒ real time.
	afterFn func(time.Duration) <-chan time.Time
	nowFn   func() time.Time

	// onOwnerStart is a test-only observability seam: called once at the
	// top of each owner goroutine so a test can count how many windows /
	// owners a Trip burst spawned (the coalescing pin). nil in production.
	onOwnerStart func()

	// onWindowClosed is a test-only observability seam: called from
	// finishWindow with the window's ACTUAL closed duration, so the item-138
	// duty-cycle gates can measure the quiesce the gate imposed exactly
	// rather than by sampling g.closed. nil in production.
	onWindowClosed func(held time.Duration)

	// windowStart is when the live window closed the gate; it backs
	// onWindowClosed and is meaningless while the gate is open.
	windowStart time.Time
}

var (
	_ ir.GrowGate                = (*GrowGate)(nil)
	_ ir.GrowGateQuiesceObserver = (*GrowGate)(nil)
)

// NewGrowGate constructs the run's shared coordinator. ctx is the
// cold-copy run context: when it is cancelled (e.g. the errgroup unwinds
// on a lane's terminal error) the owner goroutine exits and any pause is
// abandoned — every parked Await then unwinds via its own ctx.Done(),
// the load-bearing no-deadlock contract. recovered is the optional
// telemetry-recovery probe (nil for the signal-only path).
func NewGrowGate(ctx context.Context, recovered func() bool) *GrowGate {
	return &GrowGate{
		ownerCtx:  ctx,
		recovered: recovered,
		// Snapshot the timing envelope at construction so the owner
		// goroutine reads instance fields, never the mutable package
		// globals (the -race safety property; see the var block above).
		backoffBase:     GrowGateBackoffBase,
		backoffCap:      GrowGateBackoffCap,
		maxHold:         GrowGateMaxHold,
		episodeIdle:     GrowGateEpisodeIdle,
		quiesceWindow:   GrowGateQuiesceWindow,
		maxQuiesceShare: GrowGateMaxQuiesceShare,
	}
}

// quiesceSpan is one closed window in the trailing quiesce ledger. end is
// the window's PLANNED reopen at close time and is corrected to the actual
// reopen by finishWindow, so an early release (recovery / ctx-cancel) does
// not over-charge the run-level share.
type quiesceSpan struct {
	start time.Time
	end   time.Time
}

// quiesceAllowanceLocked returns how much MORE closed time the gate may
// impose right now without exceeding maxQuiesceShare of the trailing
// quiesceWindow, pruning the ledger as it goes. A non-positive result means
// the gate has already spent its share and must decline to close.
//
// This is the RUN-level bound that [GrowGateMaxHold] is not: max-hold bounds
// one window, and consecutive windows each get a fresh one. The 2026-08-05
// field report is what proved the difference matters — 246 windows, none
// remotely near the 20-minute max-hold, totalling 73 minutes of quiesce in a
// 91-minute run. A bound scoped to the window could not see that.
//
// Caller holds g.mu.
func (g *GrowGate) quiesceAllowanceLocked(now time.Time) time.Duration {
	cutoff := now.Add(-g.quiesceWindow)
	kept := g.quiesced[:0]
	var spent time.Duration
	for _, s := range g.quiesced {
		if !s.end.After(cutoff) {
			continue // wholly outside the trailing window — drop it
		}
		kept = append(kept, s)
		start, end := s.start, s.end
		if start.Before(cutoff) {
			start = cutoff
		}
		if end.After(now) {
			end = now
		}
		if end.After(start) {
			spent += end.Sub(start)
		}
	}
	g.quiesced = kept
	budget := time.Duration(float64(g.quiesceWindow) * g.maxQuiesceShare)
	return budget - spent
}

// backoff returns the per-cycle hold duration for the gate's owner
// goroutine: exponential doubling from the gate's OWN backoffBase, capped
// at its backoffCap. cycle is 1-based (cycle 1 is the first hold). Reading
// the per-instance fields (snapshotted at construction) — never the package
// globals — is what keeps the owner goroutine race-free against a test's
// global shrink/restore. Mirrors coldCopyReparentBackoff's shape.
func (g *GrowGate) backoff(cycle int) time.Duration {
	b := g.backoffBase
	for i := 1; i < cycle; i++ {
		b *= 2
		if b > g.backoffCap {
			return g.backoffCap
		}
	}
	return b
}

// Await implements [ir.GrowGate]. Fast-paths the open read under mu (a
// couple of atomic-equivalent operations); when closed it captures the
// current reopenCh and parks on {reopenCh, ctx.Done()} WITHOUT holding mu,
// re-checking the state on each wake so a coalesced extend (which leaves
// the gate closed but does not re-create reopenCh) keeps it parked.
func (g *GrowGate) Await(ctx context.Context) error {
	for {
		g.mu.Lock()
		if !g.closed {
			g.mu.Unlock()
			return nil
		}
		ch := g.reopenCh
		g.mu.Unlock()

		select {
		case <-ch:
			// Reopened (or a new close→open cycle started). Loop to
			// re-read the state: if a fresh close re-armed the gate the
			// next iteration re-parks on the new reopenCh.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Trip implements [ir.GrowGate]. If a pause window is already live the trip
// COALESCES into it and returns, changing nothing. Otherwise it advances the
// episode ladder, consults the run-level quiesce share, and (if there is
// share left) closes the gate and spawns the single owner goroutine for one
// window of exactly [GrowGate.backoff](cycle).
//
// # Why an in-window trip must not lengthen the window (item 138)
//
// This is the load-bearing correction. The ladder expresses TEMPORAL
// persistence — "the target is still bad after we waited" — so it may only
// advance on evidence of that. A trip arriving while the gate is CLOSED is
// not that evidence: every sibling lane is already parked in [GrowGate.Await],
// so the only trips that can arrive during a window come from the lanes that
// were mid-flush when it closed, all reporting the SAME target event. They
// carry no information the first trip did not.
//
// The shipped v0.111.1 gate advanced the ladder once per trip, via an
// `extend` signal the owner consumed as fast as it arrived. Measured on the
// 2026-08-05 field log: with the default fan-out (W×D = 16 lanes on sync
// cold-start, up to 32 on migrate) ONE target event produced 13–16 trips
// within ~4ms, burning the ladder to its 30s cap in those 4ms. Across 246
// windows the hold matched backoff(tripCount) to the millisecond — 0.100s at
// 1 trip, 12.87s at 8, 30.0s at ≥14 — and the copy spent 81.1% of a 91-minute
// run quiesced, four chunks in the last hour, on a target that was merely
// dropping connections. The gate's escalation was being driven by FAN-OUT
// WIDTH, not by time.
//
// So: in-window trips coalesce and are otherwise inert, and persistence is
// observed where it actually lives — ACROSS windows, one rung per window.
// # What the gate may CLAIM about why it closed (item 143)
//
// evidence is recorded and logged, and it changes NOTHING else — every timing
// decision below is identical for every value. That is deliberate. Phase A of
// item 143 established that a real storage-grow reparent is not reliably
// distinguishable from a plain transport drop at the trip point (a genuine
// grow's most common face on an in-flight write is the connection dying), so
// gating the quiesce on the evidence would trade a bounded false-positive cost
// for the chance of missing the event the gate exists for. What changed is the
// CLAIM: this log line used to announce "a coordinated target storage-grow /
// reparent window" for every close, and by two independent datasets that was
// almost never what had happened.
func (g *GrowGate) Trip(reason string, evidence ir.GrowEvidence) {
	g.mu.Lock()
	g.reason = reason
	g.evidence = evidence
	if g.closed {
		// A window is live. Sibling lanes reporting the same event are
		// already quiesced; nothing to do.
		g.mu.Unlock()
		return
	}

	now := g.now()
	// EPISODE BOUNDARY. Whether trouble is CONTINUING or has returned after a
	// genuinely healthy stretch is a question about TIME, so it is answered on
	// every trip — including one the share ceiling is about to decline.
	if g.lastReopen.IsZero() || now.Sub(g.lastReopen) >= g.episodeIdle {
		g.cycle = 0
		g.shareExhaustedLogged = false
	}

	allowance := g.quiesceAllowanceLocked(now)
	if allowance <= 0 {
		// The gate has spent its share of the trailing window. Decline to
		// close and hand each lane back to itself — for five of the nine
		// bulk-write lanes that means their own exponential retry budget; for
		// the other four it means their next transient is terminal-but-loud.
		// The roster and the exposure are in [GrowGateMaxQuiesceShare]'s doc.
		//
		// Note what does NOT happen here: the episode ladder does not advance.
		// A declined trip arms no window, and the ladder measures windows (see
		// below). Advancing it here would let one fan-out burst of declined
		// trips climb the ladder by its WIDTH — the exact item-138 defect, in
		// the state item 138 introduced.
		first := !g.shareExhaustedLogged
		g.shareExhaustedLogged = true
		g.mu.Unlock()
		if first {
			slog.WarnContext(
				g.ownerCtx, "pipeline: cold-copy grow-gate DECLINING to close — it has already quiesced "+
					"its permitted share of the recent wall clock and further quiescing would stall the copy "+
					"rather than help the target. Each lane now carries itself: the batched/chunked/LOAD DATA "+
					"cores fall back to their own bounded reparent-retry, but the plain-INSERT, raw-COPY and "+
					"float-repair cores have no replay, so a transient reaching one of those will fail this "+
					"table loudly rather than being ridden out (ADR-0110, item 138)",
				slog.String("reason", reason),
				slog.Duration("quiesce_window", g.quiesceWindow),
				slog.Float64("max_quiesce_share", g.maxQuiesceShare),
			)
		}
		return
	}

	// EPISODE LADDER, advanced ONLY here — on the path that actually ARMS a
	// window. A prompt re-trip (the previous quiesce did not suffice) climbs
	// one rung; a trip after a genuinely healthy stretch has already been
	// reset to the fast probe interval above. This is what makes the
	// "the exponential backoff grows across windows" claim true — before item
	// 138 every window restarted at rung 1 and all the growth happened inside
	// a single window instead.
	if g.cycle < growGateMaxCycle {
		g.cycle++
	}
	cycle := g.cycle

	hold := g.backoff(cycle)
	if hold > g.maxHold {
		hold = g.maxHold
	}
	if hold > allowance {
		hold = allowance
	}

	g.closed = true
	g.reopenCh = make(chan struct{})
	g.windowStart = now
	g.quiesced = append(g.quiesced, quiesceSpan{start: now, end: now.Add(hold)})
	reopenCh := g.reopenCh
	proactive := g.recovered != nil
	g.mu.Unlock()

	slog.WarnContext(
		g.ownerCtx, "pipeline: cold-copy grow-gate CLOSED — quiescing all cold-copy lanes so they back off together "+
			"instead of independently retrying a target that just refused a write (ADR-0110)",
		slog.String("reason", reason),
		// evidence says what the tripping lane OBSERVED — see [ir.GrowEvidence].
		// This line used to name a "target storage-grow / reparent window" for
		// every close; 244 of the 246 windows in the 2026-08-05 field log were
		// opened by a transport drop and none by a reparent-evidenced face, so
		// the sentence sent readers hunting for an event that had not occurred.
		// The verdict is derived per trip and is the field the next report
		// should be counted on.
		slog.String("evidence", evidence.String()),
		slog.Bool("proactive", proactive),
		// hold + cycle are here because their ABSENCE is what made item 138
		// invisible: the shipped log recorded only the close and the reopen,
		// so the hold had to be reverse-engineered from timestamps across 246
		// windows before anyone could see it was a function of fan-out width.
		slog.Duration("hold", hold),
		slog.Int("escalation_cycle", cycle),
	)
	go g.runOwner(reopenCh, hold)
}

// growGateRecoveryProbeInterval is how often a PROACTIVE (telemetry) window
// re-consults recovered() while holding, so a long hold can still be released
// early by a trustworthy recovery signal. Signal-driven windows (recovered ==
// nil) sleep the whole hold in one wait and never poll.
const growGateRecoveryProbeInterval = time.Second

// runOwner is the single owner goroutine for ONE pause window. It holds the
// gate closed for hold — the interval Trip already computed from the episode
// ladder and the run-level quiesce share — and reopens exactly once, via
// finishWindow's single teardown, so a parked Await always unwinds. The
// window ends on the FIRST of:
//
//   - HOLD ELAPSED (the primary reopen): the interval is up. Reopening lets
//     the lanes resume and PROBE the target. If the target is still bad the
//     first lane to re-hit the transient opens a FRESH window one rung higher
//     — "reopen to let lanes probe, re-trip if still bad", expressed as
//     window-ends-then-new-window rather than a mid-window reopen (which would
//     need a second owner / a probe-vs-Trip race; this is simpler and
//     race-free).
//   - RECOVERED (proactive windows only): recovered() reports the target's
//     storage headroom restored — reopen immediately. An ACCELERATOR, never
//     the sole reopen path: the volume_* gauges swing wildly and transiently
//     DISAPPEAR across a reparent (v0.99.104 v14 live: 62GB@85.9% →
//     1.66TB@~0% mid-reparent, series absent), so recovered() often cannot
//     confirm "grow finished" exactly when it matters.
//   - CTX-CANCEL: the run ctx was cancelled (e.g. the errgroup unwound on a
//     lane's terminal error). Reopen and exit so no goroutine leaks.
//
// The owner has NO extend channel and no re-trip accounting: a trip arriving
// while this window is closed is inert by construction (see [GrowGate.Trip]).
// Removing that channel is deliberate — it is what made the item-138 defect
// (a ladder advanced by trip count) inexpressible rather than merely fixed.
func (g *GrowGate) runOwner(reopenCh chan struct{}, hold time.Duration) {
	if g.onOwnerStart != nil {
		g.onOwnerStart()
	}
	deadline := g.now().Add(hold)
	for {
		remaining := deadline.Sub(g.now())
		if remaining <= 0 {
			g.finishWindow(reopenCh, "hold elapsed — lanes resume and probe")
			return
		}
		step := remaining
		if g.recovered != nil && step > growGateRecoveryProbeInterval {
			step = growGateRecoveryProbeInterval
		}
		select {
		case <-g.after(step):
			if g.recovered != nil && g.recovered() {
				g.finishWindow(reopenCh, "storage headroom recovered")
				return
			}
		case <-g.ownerCtx.Done():
			g.finishWindow(reopenCh, "run ctx cancelled")
			return
		}
	}
}

// finishWindow is the SINGLE teardown for a pause window: it flips the gate
// open and broadcasts to every parked Await by closing reopenCh, then clears
// the window state so the next Trip arms a fresh window rather than
// coalescing onto this dead owner. It also stamps lastReopen (the episode
// ladder's clock) and trues up the trailing quiesce ledger's last span to
// the ACTUAL reopen, so an early release does not over-charge the run-level
// share. Guarded so a defensive double-finish is a no-op rather than a
// close-of-closed panic. Every owner-exit path goes through here, so a parked
// Await never hangs.
func (g *GrowGate) finishWindow(reopenCh chan struct{}, why string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed || g.reopenCh != reopenCh {
		return // already reopened / a newer window owns the gate
	}
	now := g.now()
	g.closed = false
	g.reopenCh = nil
	g.lastReopen = now
	if n := len(g.quiesced); n > 0 && g.quiesced[n-1].end.After(now) {
		g.quiesced[n-1].end = now
	}
	close(reopenCh)
	if g.onWindowClosed != nil {
		g.onWindowClosed(now.Sub(g.windowStart))
	}
	slog.InfoContext(
		g.ownerCtx, "pipeline: cold-copy grow-gate reopened — lanes resuming (ADR-0110)",
		slog.String("reason", g.reason),
		slog.String("evidence", g.evidence.String()),
		slog.String("why", why),
	)
}

// QuiescedSince implements [ir.GrowGateQuiesceObserver] (roadmap item 146):
// it reports whether any part of the interval [t, now] was spent with the
// gate CLOSED, so the idle-progress copy watchdog can tell a deliberate
// coordinated pause from a stall and stay silent for the former.
//
// It answers from [GrowGate.lastReopen] rather than from the trailing
// [GrowGate.quiesced] ledger on purpose: the ledger is a run-level SHARE
// accounting structure whose spans age out of the trailing window, so a
// window that closed and reopened long enough ago to be trimmed would read as
// "never quiesced". lastReopen is stamped by [GrowGate.finishWindow] on every
// window-exit path and never rolls back, which is precisely the "was there a
// pause in this interval" question.
func (g *GrowGate) QuiescedSince(t time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return true // a window is live right now
	}
	return !g.lastReopen.IsZero() && !g.lastReopen.Before(t)
}

func (g *GrowGate) now() time.Time {
	if g.nowFn != nil {
		return g.nowFn()
	}
	return time.Now()
}

func (g *GrowGate) after(d time.Duration) <-chan time.Time {
	if g.afterFn != nil {
		return g.afterFn(d)
	}
	return time.After(d)
}

// GrowGateOrNil converts a possibly-nil *GrowGate into the [ir.GrowGate]
// interface WITHOUT the typed-nil trap: assigning a nil *GrowGate straight
// to an interface yields a NON-nil interface (concrete type, nil value),
// which would make a `gate != nil` guard wrongly fire and then nil-deref.
// Returning a true nil interface keeps "no gate ⇒ pre-ADR-0110 behaviour"
// exact. Mirrors telemetryHintOrNil.
func GrowGateOrNil(g *GrowGate) ir.GrowGate {
	if g == nil {
		return nil
	}
	return g
}

// AwaitGrowGate blocks while the gate is closed and returns ctx.Err()
// promptly on cancel. A nil gate ⇒ instant nil (pre-ADR-0110 behaviour).
// The pipeline-side nil-guard counterpart to the MySQL writer's
// AwaitGrowGate method, used by the source-read retry helper.
func AwaitGrowGate(ctx context.Context, gate ir.GrowGate) error {
	if gate == nil {
		return nil
	}
	return gate.Await(ctx)
}

// TripGrowGate trips the gate so sibling lanes quiesce. A nil gate ⇒ no-op.
// evidence is what the caller OBSERVED — see [ir.GrowEvidence]; a caller with
// no target-side verdict to offer passes [ir.GrowEvidenceNone], which is the
// zero value precisely so under-claiming is the default.
func TripGrowGate(gate ir.GrowGate, reason string, evidence ir.GrowEvidence) {
	if gate == nil {
		return
	}
	gate.Trip(reason, evidence)
}

// ApplyGrowGate wires the cold-copy run's shared coordinated-pause gate
// (ADR-0110) onto a freshly-opened writer that opts into it via
// [ir.GrowGateSetter]. Two of the three target engines implement the setter
// today (MySQL and Postgres); SQLite does NOT, and that is believed
// correct — a local file has no grow window, and there is no D1 target on
// this path to consider at all (`d1`'s OpenRowWriter returns
// ErrD1NotImplemented; D1 is a migrate SOURCE only) — but it is stated rather
// than left to "both engines", which is the phrasing that stops the next
// reader checking there is a third. So on a cold-copy run — where the gate is constructed
// unconditionally (see [NewGrowGate]) — the writer receives a non-nil gate
// and takes its grow-aware path. Any future engine that does NOT implement
// the setter retains its per-lane behaviour unchanged. A nil gate
// (non-cold-copy callers / direct unit tests) is a no-op: the writer keeps
// its zero-value nil gate (pre-ADR-0110 behaviour). Called immediately after
// each chunk/table writer opens, alongside ApplyMaxBufferBytes, before any flush.
func ApplyGrowGate(target any, gate ir.GrowGate) {
	if gate == nil {
		return
	}
	if setter, ok := target.(ir.GrowGateSetter); ok {
		setter.SetGrowGate(gate)
	}
}
