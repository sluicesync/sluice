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
// growGate is ONE engine-neutral coordinated-pause primitive shared
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
// is only spawned by the first Trip). A nil *growGate is threaded as a
// nil [ir.GrowGate] via [growGateOrNil] so the typed-nil trap can't make a
// nil concrete value look like a non-nil interface.
//
// What it does NOT do (the correctness contract, unchanged from
// ADR-0108/0109): it NEVER swallows a terminal error, advances a
// position, or marks a table complete. It only changes HOW a wait is
// spent — coordinated-and-calm vs independent-and-hammering. The per-lane
// flushWithReparentRetry / source-read retry budgets remain the
// authoritative loud-on-exhaustion floor; the gate has its own max-hold
// so a genuinely-dead target still surfaces rather than parking forever.

package pipeline

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
// tolerate. growGateMaxHold bounds the total coordinated pause: after
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
// gate CONSTRUCTION — newGrowGate snapshots them into per-instance fields
// (backoffBase / backoffCap / maxHold). The owner goroutine reads the
// gate's OWN fields, never these globals, so a test's shrink-then-restore
// of a global can never race a still-running owner from an earlier gate.
// The first cut read growGateBackoffCap directly in runOwner and the
// -race gate caught it racing a test's t.Cleanup restore; snapshotting at
// construction removes the shared-mutable-global read from the hot path.
var (
	growGateBackoffBase = 100 * time.Millisecond
	growGateBackoffCap  = 30 * time.Second
	// growGateMaxHold ≈ the reparent-retry envelope (~15-20 min): long
	// enough to ride a prolonged multi-step storage-grow, short enough
	// that a genuinely-wedged target surfaces via the lanes' own budgets
	// rather than hiding behind a forever-closed gate.
	growGateMaxHold = 20 * time.Minute
)

// growGate is the concrete [ir.GrowGate] coordinator. State is open/closed
// guarded by mu plus a closed-channel-broadcast reopenCh (re-created on
// each close→open) so Await can park without holding mu across the block.
type growGate struct {
	mu sync.Mutex

	// closed is the gate state. When false (the common case) Await returns
	// immediately. When true a pause is in effect and Await parks on
	// reopenCh / ctx.Done().
	closed bool

	// reopenCh is closed (broadcast) when the gate reopens, waking every
	// parked Await. It is re-created on each close so a fresh pause window
	// has a fresh channel. nil until the first close.
	reopenCh chan struct{}

	// extend is non-nil while the owner goroutine is running; a Trip on an
	// already-closed gate signals it (non-blocking) so the owner extends
	// the pause / re-arms its probe cycle (coalescing). Re-created with
	// reopenCh on each close.
	extend chan struct{}

	// recovered, when non-nil, is consulted by a PROACTIVE (telemetry)
	// pause: the owner reopens on the earlier of {max-hold | recovered()
	// reporting the storage headroom has recovered}. nil ⇒ the owner only
	// uses its probe-cycle + max-hold timing (the signal-driven case).
	recovered func() bool

	// reason is the most recent Trip reason, for the structured log.
	reason string

	// owner scopes the owner goroutine to the gate's lifetime ctx so it
	// exits on run-ctx cancel and on the gate's own Close. Captured at
	// construction.
	ownerCtx context.Context

	// backoffBase / backoffCap / maxHold are the gate's OWN copy of the
	// timing envelope, snapshotted from the package defaults at
	// construction (newGrowGate). The owner goroutine reads these — never
	// the package globals — so a test that shrinks a global and restores it
	// on cleanup can never race a still-running owner (the v0.99.100 -race
	// lesson). Production always gets the package defaults; tests shrink a
	// global before constructing the gate they then drive.
	backoffBase time.Duration
	backoffCap  time.Duration
	maxHold     time.Duration

	// clock-injection seams for deterministic tests; nil ⇒ real time.
	afterFn func(time.Duration) <-chan time.Time
	nowFn   func() time.Time

	// onOwnerStart is a test-only observability seam: called once at the
	// top of each owner goroutine so a test can count how many windows /
	// owners a Trip burst spawned (the coalescing pin). nil in production.
	onOwnerStart func()
}

// newGrowGate constructs the run's shared coordinator. ctx is the
// cold-copy run context: when it is cancelled (e.g. the errgroup unwinds
// on a lane's terminal error) the owner goroutine exits and any pause is
// abandoned — every parked Await then unwinds via its own ctx.Done(),
// the load-bearing no-deadlock contract. recovered is the optional
// telemetry-recovery probe (nil for the signal-only path).
func newGrowGate(ctx context.Context, recovered func() bool) *growGate {
	return &growGate{
		ownerCtx:  ctx,
		recovered: recovered,
		// Snapshot the timing envelope at construction so the owner
		// goroutine reads instance fields, never the mutable package
		// globals (the -race safety property; see the var block above).
		backoffBase: growGateBackoffBase,
		backoffCap:  growGateBackoffCap,
		maxHold:     growGateMaxHold,
	}
}

// backoff returns the per-cycle hold duration for the gate's owner
// goroutine: exponential doubling from the gate's OWN backoffBase, capped
// at its backoffCap. cycle is 1-based (cycle 1 is the first hold). Reading
// the per-instance fields (snapshotted at construction) — never the package
// globals — is what keeps the owner goroutine race-free against a test's
// global shrink/restore. Mirrors coldCopyReparentBackoff's shape.
func (g *growGate) backoff(cycle int) time.Duration {
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
func (g *growGate) Await(ctx context.Context) error {
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

// Trip implements [ir.GrowGate]. If NO pause window is live it closes the
// gate and spawns the single owner goroutine. If a window is already live
// (g.extend != nil — true whether the owner is in its closed HOLD or its
// reopened PROBE/SETTLE phase) it coalesces onto that owner by signalling
// extend, so a re-trip during the probe window does NOT spawn a second
// owner. Idempotent and concurrency-safe; concurrent trips from many lanes
// + the telemetry sidecar collapse into ONE window.
func (g *growGate) Trip(reason string) {
	g.mu.Lock()
	g.reason = reason
	if g.extend != nil {
		// A window is live (closed-hold or reopened-probe). Coalesce: nudge
		// the owner to extend / re-close. Non-blocking — a pending extend
		// already means "at least one more cycle is coming".
		extend := g.extend
		g.mu.Unlock()
		select {
		case extend <- struct{}{}:
		default:
		}
		return
	}
	// No live window — arm a fresh one.
	g.closed = true
	g.reopenCh = make(chan struct{})
	g.extend = make(chan struct{}, 1)
	reopenCh := g.reopenCh
	extend := g.extend
	proactive := g.recovered != nil
	g.mu.Unlock()

	slog.WarnContext(
		g.ownerCtx, "pipeline: cold-copy grow-gate CLOSED — quiescing all cold-copy lanes for a coordinated target storage-grow / reparent window (ADR-0110)",
		slog.String("reason", reason),
		slog.Bool("proactive", proactive),
	)
	go g.runOwner(reopenCh, extend)
}

// runOwner is the single owner goroutine for one pause window. It holds
// the gate closed across a sequence of exponential-backoff cycles, ALL
// owned by this one goroutine (so there is exactly ONE owner per window —
// no owner-spawn race), then reopens exactly once when the window ends.
// The window ends — and the gate reopens (via finishWindow's single
// teardown, so a parked Await always unwinds) — on the FIRST of:
//
//   - QUIET: a full backoff cycle elapsed with NO re-trip (no pending
//     extend). The transient burst is over; reopening lets the lanes
//     resume and PROBE the target. If the target is still bad the first
//     lane to re-hit the transient opens a FRESH window — the "reopen to
//     let lanes probe, re-trip if still bad" loop, expressed as
//     window-ends-then-new-window rather than a mid-window reopen (which
//     would need a second owner / a probe-vs-trip race; this is simpler
//     and race-free).
//   - RECOVERED (proactive pauses only): recovered() reports the target's
//     storage headroom restored — reopen immediately (the earlier of
//     {max-hold | recovery}).
//   - MAX-HOLD: the cumulative pause hit growGateMaxHold. Reopen so each
//     lane's own bounded retry budget (the AUTHORITATIVE floor) takes over
//     and surfaces a genuinely-dead target loudly, rather than the gate
//     hiding it behind a forever-closed door.
//   - CTX-CANCEL: the run ctx was cancelled (e.g. the errgroup unwound on
//     a lane's terminal error). Reopen and exit so no goroutine leaks.
//
// A re-trip while closed (a coalesced Trip) sends on extend; the owner
// observes it at the end of the cycle and holds another (longer) cycle —
// this is how concurrent trips from ~16 lanes collapse into one extending
// window.
func (g *growGate) runOwner(reopenCh, extend chan struct{}) {
	if g.onOwnerStart != nil {
		g.onOwnerStart()
	}
	deadline := g.now().Add(g.maxHold)
	for cycle := 1; ; cycle++ {
		hold := g.backoff(cycle)
		if remaining := deadline.Sub(g.now()); remaining < hold {
			hold = remaining
		}
		if hold < 0 {
			hold = 0
		}

		retripped := false
		select {
		case <-extend:
			// A lane re-tripped during this hold — the grow isn't over yet.
			retripped = true
		case <-g.after(hold):
			// Hold elapsed with no re-trip observed in the blocking select;
			// a trip that raced the timer is still caught by the drain below.
		case <-g.ownerCtx.Done():
			g.finishWindow(reopenCh, "run ctx cancelled")
			return
		}
		// Catch a re-trip that raced the timer (extend is buffered, depth 1).
		select {
		case <-extend:
			retripped = true
		default:
		}

		// EARLY REOPEN (telemetry, when present): if a fresh sample shows the
		// storage headroom genuinely recovered, reopen before the quiet cycle
		// would. This is an ACCELERATOR, not the sole reopen path — it must not
		// be relied on, because the volume_* gauges swing wildly and transiently
		// DISAPPEAR across a reparent (v0.99.104 v14 live: capacity 62GB@85.9%
		// → 1.66TB@~0% mid-reparent, series absent), so recovered() often can't
		// confirm "grow finished" exactly when it matters.
		if g.recovered != nil && g.recovered() {
			g.finishWindow(reopenCh, "storage headroom recovered")
			return
		}

		// MAX-HOLD: surface a stalled target via the lanes' own budgets.
		if !g.now().Before(deadline) {
			g.finishWindow(reopenCh, "max-hold reached — per-lane retry budgets take over")
			return
		}

		// QUIET CYCLE (the primary reopen, ALWAYS applied — telemetry or not):
		// a backoff cycle elapsed with no re-trip, so the transient burst (or an
		// anticipated grow that hasn't actually faulted yet) is over for now.
		// Reopen so the lanes resume and PROBE; if the target is still bad the
		// next transient re-trips a FRESH window (the exponential backoff grows
		// across windows). This applies to BOTH signal-driven and proactive
		// (telemetry) trips: a proactive trip is a BRIEF anticipatory pause that
		// hands off to this reactive cycling — NOT a hold for the whole grow.
		//
		// v0.99.104 v14 live proved the old "proactive holds until
		// recovered()/max-hold" rode a flat ~20-min max-hold on every reparent
		// (zero progress) because recovered() is defeated by the mid-reparent
		// metric instability above — strictly worse than the reactive cycling
		// (which makes incremental progress every ~30s window). Cycling-always
		// matches the proven reactive behaviour; recovered() above only reopens
		// EARLIER when the signal is trustworthy; max-hold stays the backstop.
		if !retripped {
			g.finishWindow(reopenCh, "quiet cycle — lanes resume and probe")
			return
		}
		// Otherwise (re-tripped this cycle): keep holding for the next (longer)
		// cycle, bounded by max-hold.
	}
}

// finishWindow is the SINGLE teardown for a pause window: it flips the gate
// open and broadcasts to every parked Await by closing reopenCh, then
// clears the window state (extend, reopenCh) so the next Trip arms a fresh
// window rather than coalescing onto this dead owner. Guarded so a
// defensive double-finish is a no-op rather than a close-of-closed panic.
// Every owner-exit path goes through here, so a parked Await never hangs
// and g.extend is never left dangling.
func (g *growGate) finishWindow(reopenCh chan struct{}, why string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed || g.reopenCh != reopenCh {
		return // already reopened / a newer window owns the gate
	}
	g.closed = false
	g.extend = nil
	g.reopenCh = nil
	close(reopenCh)
	slog.InfoContext(
		g.ownerCtx, "pipeline: cold-copy grow-gate reopened — lanes resuming (ADR-0110)",
		slog.String("reason", g.reason),
		slog.String("why", why),
	)
}

func (g *growGate) now() time.Time {
	if g.nowFn != nil {
		return g.nowFn()
	}
	return time.Now()
}

func (g *growGate) after(d time.Duration) <-chan time.Time {
	if g.afterFn != nil {
		return g.afterFn(d)
	}
	return time.After(d)
}

// growGateOrNil converts a possibly-nil *growGate into the [ir.GrowGate]
// interface WITHOUT the typed-nil trap: assigning a nil *growGate straight
// to an interface yields a NON-nil interface (concrete type, nil value),
// which would make a `gate != nil` guard wrongly fire and then nil-deref.
// Returning a true nil interface keeps "no gate ⇒ pre-ADR-0110 behaviour"
// exact. Mirrors telemetryHintOrNil.
func growGateOrNil(g *growGate) ir.GrowGate {
	if g == nil {
		return nil
	}
	return g
}

// awaitGrowGate blocks while the gate is closed and returns ctx.Err()
// promptly on cancel. A nil gate ⇒ instant nil (pre-ADR-0110 behaviour).
// The pipeline-side nil-guard counterpart to the MySQL writer's
// awaitGrowGate method, used by the source-read retry helper.
func awaitGrowGate(ctx context.Context, gate ir.GrowGate) error {
	if gate == nil {
		return nil
	}
	return gate.Await(ctx)
}

// tripGrowGate trips the gate so sibling lanes quiesce. A nil gate ⇒ no-op.
func tripGrowGate(gate ir.GrowGate, reason string) {
	if gate == nil {
		return
	}
	gate.Trip(reason)
}
