// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pre-emptive Postgres replication-slot health WARNings — severity-A
// finding F13 of the 2026-05-22 Reddit-research run. See ADR-0059.
//
// The streamer attaches a background ticker (default 30s) that probes
// the source-side [ir.SlotHealthReporter] surface and emits structured
// slog WARNs when:
//
//   - Retention pressure crosses 70% of `max_slot_wal_keep_size`
//     (WARN — the consumer is starting to fall behind);
//   - Retention pressure crosses 85% (CRITICAL — slot eviction is
//     imminent without operator intervention);
//   - The slot has been observed inactive for >= 30 minutes (WARN —
//     the consumer may have crashed silently).
//
// Each warning is de-duplicated within a 5-minute window (the rate-limit
// window): the same condition firing twice in quick succession emits
// once, not twice. Transitions emit immediately — going from clean →
// 70% emits the WARN; going from 70% → 85% emits the CRITICAL even if
// the 70% emission was within the suppression window; going clean →
// inactive → clean emits a "cleared" INFO so operators see "the alarm
// resolved itself" rather than silence.
//
// Roadmap item 64a promotes the same crossings to the notification
// sinks (webhook/Slack/SMTP) — see slot_health_notify.go and the
// ADR-0059 implementation note. The notification fires exactly when the
// slog WARN fires; a clear stays log-only.
//
// **Why this is a separate file from streamer.go.** The threshold logic
// is a pure function (no engine imports, no DB calls) and the rate-limit
// state is a small map; both belong in their own unit-testable surface.
// The streamer wires the goroutine; the threshold evaluator lives here.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// SlotHealthThresholds carries the configurable bounds the threshold
// evaluator uses. Documented in ADR-0059 §"Thresholds and rationale".
// Exposed as a struct (rather than hard-coded) so tests can drive the
// boundary cases deterministically without sleeping; operators get the
// production defaults via [DefaultSlotHealthThresholds].
type SlotHealthThresholds struct {
	// WarnPercent is the retention-pressure threshold (as a 0-100
	// percentage of `max_slot_wal_keep_size`) above which a WARN is
	// emitted. Default 70.
	WarnPercent float64

	// CriticalPercent is the retention-pressure threshold above which
	// the WARN escalates (distinct slog message keyed "critical=true"
	// so operators can grep / alert on the louder signal). Must be >=
	// WarnPercent. Default 85.
	CriticalPercent float64

	// InactivityThreshold is the duration the slot must be observed as
	// `active=false` before the inactivity WARN fires. Default 30m.
	InactivityThreshold time.Duration

	// RateLimitWindow is the per-condition de-dup window: the same
	// condition firing twice within this window emits once. Default 5m.
	RateLimitWindow time.Duration
}

// DefaultSlotHealthThresholds returns the production threshold set.
// Values come from ADR-0059 §"Thresholds and rationale": 70/85% leaves
// roughly 30%/15% headroom before PG marks the slot 'lost'; 30m
// inactivity is long enough to avoid spam on legitimate stream
// reconnects yet short enough that a silently-dead consumer is caught
// inside an operator's typical pager window.
func DefaultSlotHealthThresholds() SlotHealthThresholds {
	return SlotHealthThresholds{
		WarnPercent:         70,
		CriticalPercent:     85,
		InactivityThreshold: 30 * time.Minute,
		RateLimitWindow:     5 * time.Minute,
	}
}

// slotWarningKind names the three conditions the evaluator can flag.
// Used as the map key for rate-limit / dedup state so each condition
// rate-limits independently — a WARN clearing doesn't reset the
// CRITICAL's cooldown.
type slotWarningKind int

const (
	slotWarnNone        slotWarningKind = iota
	slotWarnRetention70                 // 70% <= pressure < 85%
	slotWarnRetention85                 // pressure >= 85%
	slotWarnInactive                    // active=false for >= InactivityThreshold
)

// slotHealthState carries the per-stream evaluator state across probe
// ticks. Owned by one goroutine (the slot-health probe loop); no
// concurrent access, so no mutex.
type slotHealthState struct {
	// lastEmittedAt tracks the most recent slog.Warn emission per
	// condition, used to gate the rate-limit window. A zero time
	// means "never emitted in this stream's lifetime."
	lastEmittedAt map[slotWarningKind]time.Time

	// lastFiredKind is the condition that most recently emitted (or
	// slotWarnNone if the state is clean). State transitions
	// (clean→warn, warn→critical, critical→warn, anything→clean) emit
	// regardless of the rate-limit window; same-state repeats are
	// suppressed.
	lastFiredKind slotWarningKind

	// lastActiveSeenAt is the wall-clock timestamp of the most recent
	// probe that reported active=true. Used to compute the inactivity
	// duration when active=false on a subsequent probe. Zero means
	// "never seen active in this stream's lifetime"; the evaluator
	// treats that as "use the streamer's start time" via the
	// inactivityStartFallback caller-supplied value.
	lastActiveSeenAt time.Time
}

// newSlotHealthState constructs the per-stream evaluator state. The
// caller-supplied now() is the streamer's start time; it's used as the
// inactivity baseline so a slot that's never been seen active gets a
// sensible "how long has it been inactive?" reading without us having
// to record "first probe at" separately.
func newSlotHealthState(streamerStart time.Time) *slotHealthState {
	return &slotHealthState{
		lastEmittedAt:    make(map[slotWarningKind]time.Time),
		lastFiredKind:    slotWarnNone,
		lastActiveSeenAt: streamerStart,
	}
}

// slotWarningDecision is the threshold evaluator's per-tick output.
// Pure data (no logger, no context) so the decision logic is trivially
// unit-testable.
type slotWarningDecision struct {
	// Kind is the condition the evaluator chose for this tick (or
	// slotWarnNone when nothing is wrong).
	Kind slotWarningKind

	// Emit is true when the WARN should actually be logged this tick.
	// False on rate-limit-suppressed ticks where Kind has fired
	// recently and the state hasn't transitioned.
	Emit bool

	// Cleared is true when the condition that most recently fired has
	// resolved (e.g. retention pressure dropped back below 70%, or
	// active=true again). When true, the caller emits a one-line INFO
	// so operators see the alarm resolve.
	Cleared bool

	// PercentUsed is the computed retention-pressure percentage for
	// the retention warnings (0 for the inactive warning).
	PercentUsed float64

	// InactiveFor is the computed inactivity duration for the inactive
	// warning (0 for retention warnings).
	InactiveFor time.Duration
}

// evaluateSlotHealth is the threshold-decision pure function. Given a
// snapshot, the current eval state, the thresholds, and now(), returns
// the decision (kind + emit/clear flags). Side effects (mutating the
// state's lastEmittedAt / lastFiredKind) happen in the caller after the
// decision so the function stays pure and deterministically testable.
//
// The mutation step is exposed separately as [recordSlotHealthEmission]
// so tests can build a decision, inspect it, then choose whether to
// "commit" it to the state.
func evaluateSlotHealth(
	snap ir.SlotHealth,
	st *slotHealthState,
	thr SlotHealthThresholds,
	now time.Time,
) slotWarningDecision {
	// Step 1: refresh the "last seen active" timestamp when the probe
	// reports active=true. The inactivity duration is now() - this
	// timestamp; any probe seeing active resets the clock.
	if snap.Active {
		st.lastActiveSeenAt = now
	}

	// Step 2: pick the condition for this tick. Retention pressure is
	// evaluated first because a slot can be both falling behind AND
	// inactive — the retention warning is the more actionable signal
	// (it's the one PG will evict on). Inactivity wins only when
	// retention is clean.
	kind := slotWarnNone
	var percent float64
	var inactiveFor time.Duration

	if snap.MaxKeepSizeBytes > 0 {
		percent = float64(snap.LagBytes) / float64(snap.MaxKeepSizeBytes) * 100
		switch {
		case percent >= thr.CriticalPercent:
			kind = slotWarnRetention85
		case percent >= thr.WarnPercent:
			kind = slotWarnRetention70
		}
	}
	// MaxKeepSizeBytes == -1 → unlimited; no retention warning possible.
	// MaxKeepSizeBytes == 0 → "no retention at all" extreme: any
	// non-zero lag is over-bound. PG's docs say this value disables the
	// slot's retention entirely, which is itself an operator-set
	// recipe for slot loss — but the loud-failure surface for that is
	// the eviction itself, not a percentage warning (we'd be dividing
	// by zero). Skip the percentage path; if the slot becomes inactive
	// the inactivity path still fires.

	if kind == slotWarnNone && !snap.Active {
		inactiveFor = now.Sub(st.lastActiveSeenAt)
		if inactiveFor >= thr.InactivityThreshold {
			kind = slotWarnInactive
		}
	}

	// Step 3: decide emit / clear vs. suppress.
	dec := slotWarningDecision{
		Kind:        kind,
		PercentUsed: percent,
		InactiveFor: inactiveFor,
	}

	// Clear-event: previously firing, now clean (or downgraded to clean
	// across all conditions). Emit a single INFO so the operator sees
	// the alarm resolve. The clear path takes precedence over emit:
	// we don't double-log "fired AND cleared" on the same tick.
	if kind == slotWarnNone && st.lastFiredKind != slotWarnNone {
		dec.Cleared = true
		return dec
	}

	if kind == slotWarnNone {
		return dec // still clean, nothing to say
	}

	// State-transition: lastFiredKind differs from this tick's kind →
	// emit unconditionally (the operator needs to see "now critical"
	// even if "70% warn" fired 30s ago).
	if st.lastFiredKind != kind {
		dec.Emit = true
		return dec
	}

	// Same condition as last emit: gate on the rate-limit window.
	last, ever := st.lastEmittedAt[kind]
	if !ever || now.Sub(last) >= thr.RateLimitWindow {
		dec.Emit = true
		return dec
	}

	// Suppressed.
	return dec
}

// recordSlotHealthEmission applies a decision to the state. Called by
// the probe loop *after* the slog emission so the state reflects "this
// is what we've already told the operator about." Tests call it
// explicitly to advance the state across simulated ticks.
func recordSlotHealthEmission(st *slotHealthState, dec slotWarningDecision, now time.Time) {
	if dec.Cleared {
		st.lastFiredKind = slotWarnNone
		return
	}
	if dec.Emit {
		st.lastEmittedAt[dec.Kind] = now
		st.lastFiredKind = dec.Kind
	}
}

// emitSlotHealthWarning logs the structured slog WARN for a non-clear
// decision, or the matching INFO when the condition clears. Centralised
// so the message shape is consistent and the ADR-0059 hint text doesn't
// drift between conditions.
//
// **Loud-failure discipline.** Every warning embeds the slot name, the
// concrete signal (percent or duration), the GUC bound for context,
// and an operator-actionable next-step ("check the consumer", "bump
// max_slot_wal_keep_size", "re-snapshot if you've already lost the
// slot"). The ADR cite is in the log so operators landing on the line
// in their log aggregator can find the rationale without grepping
// source.
func emitSlotHealthWarning(ctx context.Context, snap ir.SlotHealth, streamID string, dec slotWarningDecision) {
	if dec.Cleared {
		slog.InfoContext(
			ctx, "postgres: slot-health condition cleared",
			slog.String("stream_id", streamID),
			slog.String("slot", snap.SlotName),
			slog.Bool("active", snap.Active),
			slog.String("wal_status", snap.WALStatus),
			slog.Int64("lag_bytes", snap.LagBytes),
			slog.String("see", "ADR-0059"),
		)
		return
	}
	if !dec.Emit {
		return
	}
	switch dec.Kind {
	case slotWarnRetention70:
		slog.WarnContext(
			ctx, "postgres: slot-health: WAL retention pressure",
			slog.String("stream_id", streamID),
			slog.String("slot", snap.SlotName),
			slog.Float64("percent_used", dec.PercentUsed),
			slog.Int64("lag_bytes", snap.LagBytes),
			slog.Int64("max_slot_wal_keep_size_bytes", snap.MaxKeepSizeBytes),
			slog.String("wal_status", snap.WALStatus),
			slog.Bool("critical", false),
			slog.String("hint", slotHealthHint(snap, dec)),
			slog.String("see", "ADR-0059"),
		)
	case slotWarnRetention85:
		slog.WarnContext(
			ctx, "postgres: slot-health: WAL retention pressure CRITICAL",
			slog.String("stream_id", streamID),
			slog.String("slot", snap.SlotName),
			slog.Float64("percent_used", dec.PercentUsed),
			slog.Int64("lag_bytes", snap.LagBytes),
			slog.Int64("max_slot_wal_keep_size_bytes", snap.MaxKeepSizeBytes),
			slog.String("wal_status", snap.WALStatus),
			slog.Bool("critical", true),
			slog.String("hint", slotHealthHint(snap, dec)),
			slog.String("see", "ADR-0059"),
		)
	case slotWarnInactive:
		slog.WarnContext(
			ctx, "postgres: slot-health: slot inactive",
			slog.String("stream_id", streamID),
			slog.String("slot", snap.SlotName),
			slog.Bool("active", false),
			slog.Duration("inactive_for", dec.InactiveFor),
			slog.String("wal_status", snap.WALStatus),
			slog.Int64("lag_bytes", snap.LagBytes),
			slog.String("hint", slotHealthHint(snap, dec)),
			slog.String("see", "ADR-0059"),
		)
	}
}

// slotHealthHint is the single definition of the ADR-0059 operator-
// actionable remediation text per condition. Shared by the slog WARN
// (the "hint" field above) and the roadmap-64a notification body
// ([makeSlotHealthNotification]) so the guidance an operator sees in the
// log and in the page can never drift apart.
func slotHealthHint(snap ir.SlotHealth, dec slotWarningDecision) string {
	switch dec.Kind {
	case slotWarnRetention70:
		return fmt.Sprintf(
			"slot %q is holding back %d bytes of WAL (%.1f%% of max_slot_wal_keep_size); the consumer may be falling behind — check that sluice (or whichever consumer owns this slot) is keeping up with source writes, or raise max_slot_wal_keep_size if the workload's burst is legitimate.",
			snap.SlotName, snap.LagBytes, dec.PercentUsed,
		)
	case slotWarnRetention85:
		return fmt.Sprintf(
			"slot %q is holding back %d bytes of WAL (%.1f%% of max_slot_wal_keep_size); Postgres will invalidate this slot (wal_status -> 'lost') if the lag exceeds 100%% — intervene now: confirm the consumer is alive, drain its backlog, or raise max_slot_wal_keep_size temporarily. If the slot is already lost a fresh re-snapshot is the only recovery; if the slot is abandoned (no consumer will ever resume), drop it so it stops retaining WAL.",
			snap.SlotName, snap.LagBytes, dec.PercentUsed,
		)
	case slotWarnInactive:
		return fmt.Sprintf(
			"slot %q has been inactive for %s; the consumer (sluice or otherwise) is no longer attached — check whether the streamer is still running, the network path to the source is healthy, and the replication connection hasn't been killed by the source-side wal_sender_timeout.",
			snap.SlotName, dec.InactiveFor.Round(time.Second),
		)
	}
	return ""
}

// slotHealthProbeLoop is the per-stream background goroutine that
// runs the F13 probe on a fixed cadence. Owned by the streamer; exits
// cleanly on ctx cancellation. Errors from the underlying reporter
// log at DEBUG (a transient PG hiccup shouldn't itself trigger an
// operator-visible WARN about the warner) and the tick advances.
//
// The probe is a single cheap query (one row from pg_replication_slots
// joined to pg_current_wal_lsn + a pg_settings lookup); calling it
// every 30s adds negligible source-side load.
//
// notifier is the roadmap-64a sink fan-out for threshold crossings
// (nil ⇒ slog WARNs only, the pre-64a behavior). A notification fires
// exactly when the slog WARN fires (dec.Emit): crossings and
// escalations page immediately, in-condition repeats are held to the
// thresholds' rate-limit window, and a cleared-then-re-entered
// condition re-fires — one mechanism for both surfaces, per the
// ADR-0059 implementation note.
func slotHealthProbeLoop(
	ctx context.Context,
	reporter ir.SlotHealthReporter,
	slotName, streamID string,
	thresholds SlotHealthThresholds,
	tickInterval time.Duration,
	notifier notify.Notifier,
) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	state := newSlotHealthState(time.Now())

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			snap, ok, err := reporter.SlotHealth(ctx, slotName)
			if err != nil {
				slog.DebugContext(
					ctx, "postgres: slot-health probe failed (will retry next tick)",
					slog.String("stream_id", streamID),
					slog.String("slot", slotName),
					slog.String("err", err.Error()),
				)
				continue
			}
			if !ok {
				// Slot doesn't exist yet (cold-start race) — silent
				// skip, the next tick will find it.
				continue
			}
			dec := evaluateSlotHealth(snap, state, thresholds, now)
			emitSlotHealthWarning(ctx, snap, streamID, dec)
			notifySlotHealthCrossing(ctx, notifier, streamID, snap, dec)
			recordSlotHealthEmission(state, dec, now)
		}
	}
}

// slotHealthProbeAttachment is the bundle the streamer holds onto so
// it can release resources (close the dedicated SchemaReader, cancel
// the goroutine ctx) when the stream tears down. Mirrors the cleanup-
// closure shape of [Streamer.attachSpillReporter].
type slotHealthProbeAttachment struct {
	cancel context.CancelFunc
	once   sync.Once
	close  func()
}

// Close releases the probe goroutine and its dedicated source-DB
// connection. Idempotent.
func (a *slotHealthProbeAttachment) Close() {
	a.once.Do(func() {
		if a.cancel != nil {
			a.cancel()
		}
		if a.close != nil {
			a.close()
		}
	})
}

// slotHealthProbeTickInterval is the production probe cadence. Exposed
// as a package-level var so the integration test can drive it down to
// sub-second ticks without sleeping for 30s per probe.
var slotHealthProbeTickInterval = 30 * time.Second
