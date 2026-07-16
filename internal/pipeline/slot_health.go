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
//     the consumer may have crashed silently);
//   - `wal_status` reaches 'unreserved' (CRITICAL — the retention cap
//     is exceeded; PG invalidates the slot at the next checkpoint);
//   - `wal_status` reaches 'lost', or the slot row vanishes while a
//     condition is outstanding (TERMINAL CRITICAL — see below).
//
// Each warning is de-duplicated within a 5-minute window (the rate-limit
// window): the same condition firing twice in quick succession emits
// once, not twice. ESCALATIONS emit immediately — going from clean →
// 70% emits the WARN; going from 70% → 85% emits the CRITICAL even if
// the 70% emission was within the suppression window. DOWNGRADES
// (85% → 70%, anything → clean) are damped by one probe tick (audit
// finding LOW-D0-18): the lower state must persist for one extra probe
// before it emits, so a reading hovering on a threshold boundary can't
// page on every 30s probe by bouncing between kinds (transitions used to
// bypass the 5m rate limit entirely). A genuine clear still emits a
// "cleared" INFO — one tick later — so operators see "the alarm resolved
// itself" rather than silence.
//
// **Terminal conditions latch (audit findings MED-D0-9 / LOW-D0-17).**
// `wal_status='lost'` (Postgres invalidated the slot; a re-snapshot is
// the only recovery) and "slot row gone while a condition was
// outstanding" (someone dropped it mid-stream) are UNRECOVERABLE within
// this slot's lifetime. They page CRITICAL exactly once and then LATCH:
// no repeat every rate-limit window (re-firing an unrecoverable event
// adds noise without information), and — load-bearing — NEVER a
// "condition cleared" INFO. Pre-fix, a lost slot reported NULL lag,
// which the reporter mapped to 0%, so the evaluator saw "clean" and
// emitted a false "condition cleared" at the exact moment paging
// mattered most (observed live on PG16). The latch lives for the probe
// attachment's lifetime, i.e. one runOnce attempt: a restart (including
// an auto-re-snapshot restart) re-attaches the probe with fresh state.
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

// slotWarningKind names the conditions the evaluator can flag.
// Used as the map key for rate-limit / dedup state so each condition
// rate-limits independently — a WARN clearing doesn't reset the
// CRITICAL's cooldown.
type slotWarningKind int

const (
	slotWarnNone        slotWarningKind = iota
	slotWarnInactive                    // active=false for >= InactivityThreshold
	slotWarnRetention70                 // 70% <= pressure < 85%
	slotWarnRetention85                 // pressure >= 85%
	slotWarnUnreserved                  // wal_status='unreserved' — invalidation at next checkpoint
	slotWarnLost                        // wal_status='lost' — slot invalidated; TERMINAL
	slotWarnDropped                     // slot row vanished with a condition outstanding; TERMINAL
)

// The declaration order above doubles as the severity ranking the
// hysteresis logic uses (see slotWarnRank): an upward move is an
// escalation (emits immediately), a downward move is a downgrade
// (damped by one probe tick).

// slotWarnRank is the severity ordering for transition direction.
// Separate function (rather than comparing the iota values directly at
// call sites) so the "constants ARE the ranking" coupling is named and
// a future reorder has exactly one place to break loudly.
func slotWarnRank(k slotWarningKind) int { return int(k) }

// isTerminalSlotWarning reports whether the condition is unrecoverable
// within the slot's lifetime — the class that pages once and latches
// (MED-D0-9 / LOW-D0-17). 'unreserved' is deliberately NOT terminal:
// PG documents that wal_status can return from 'unreserved' to
// 'reserved'/'extended' if the consumer catches up before the next
// checkpoint, and latching on a recoverable state would silence the
// paging net for the rest of the stream — the very inversion this fix
// removes. It pages CRITICAL immediately but keeps clear semantics.
func isTerminalSlotWarning(k slotWarningKind) bool {
	return k == slotWarnLost || k == slotWarnDropped
}

// Verbatim pg_replication_slots.wal_status values the evaluator
// dispatches on (the other two, "reserved" and "extended", are healthy).
const (
	walStatusUnreserved = "unreserved"
	walStatusLost       = "lost"
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

	// terminalLatched is the MED-D0-9 / LOW-D0-17 terminal latch: set
	// when a terminal condition (lost / dropped) has emitted. Once set,
	// every subsequent probe decision is silent — no repeats (an
	// unrecoverable event re-paged every 5m is noise, not information)
	// and, load-bearing, no "condition cleared" INFO (a lost slot's
	// NULL lag reads as 0% pressure, which pre-fix produced a false
	// clear at the exact terminal moment). Lifetime = the probe
	// attachment (one runOnce attempt); a restart re-attaches with
	// fresh state.
	terminalLatched bool

	// pendingDowngradeKind / pendingDowngradeArmed implement the
	// LOW-D0-18 hysteresis: a DOWNGRADE transition (85%→70%,
	// anything→clean) is held until the lower kind persists for one
	// extra probe tick. armed=false means no downgrade is pending; the
	// separate bool exists because slotWarnNone (the clear) is itself a
	// valid pending kind and the kind field's zero value couldn't
	// distinguish "pending clear" from "nothing pending."
	pendingDowngradeKind  slotWarningKind
	pendingDowngradeArmed bool
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

	// PercentUsed is the computed retention-pressure percentage
	// whenever a finite bound exists (0 when the GUC is unlimited/0, or
	// for the vanished-slot decision). Load-bearing for the retention
	// warnings; context for the others.
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
	// Terminal latch (MED-D0-9 / LOW-D0-17): after a lost/dropped page,
	// every decision is silent — no repeats, and NEVER a "cleared" INFO
	// (a lost slot's NULL lag reads as 0% pressure, so without the
	// latch this tick would look clean and falsely announce recovery).
	if st.terminalLatched {
		return slotWarningDecision{Kind: st.lastFiredKind}
	}

	// Step 1: refresh the "last seen active" timestamp when the probe
	// reports active=true. The inactivity duration is now() - this
	// timestamp; any probe seeing active resets the clock.
	if snap.Active {
		st.lastActiveSeenAt = now
	}

	// Step 2: pick the condition for this tick, most severe first.
	// wal_status='lost' means Postgres has already invalidated the slot
	// — it supersedes everything (the percentage math is meaningless at
	// that point: lag reads NULL→0). 'unreserved' means the cap is
	// exceeded and invalidation lands at the next checkpoint — the last
	// window where intervention can still save the slot. Below those,
	// retention pressure is evaluated before inactivity because a slot
	// can be both falling behind AND inactive — the retention warning
	// is the more actionable signal (it's the one PG will evict on).
	kind := slotWarnNone
	var percent float64
	var inactiveFor time.Duration

	if snap.MaxKeepSizeBytes > 0 {
		percent = float64(snap.LagBytes) / float64(snap.MaxKeepSizeBytes) * 100
	}
	// MaxKeepSizeBytes == -1 → unlimited; no retention warning possible.
	// MaxKeepSizeBytes == 0 → "no retention at all" extreme: any
	// non-zero lag is over-bound. PG's docs say this value disables the
	// slot's retention entirely, which is itself an operator-set
	// recipe for slot loss — but the loud-failure surface for that is
	// the eviction itself, not a percentage warning (we'd be dividing
	// by zero). Skip the percentage path; the wal_status path below
	// still catches the resulting unreserved/lost transition, and if
	// the slot becomes inactive the inactivity path still fires.

	switch {
	case snap.WALStatus == walStatusLost:
		kind = slotWarnLost
	case snap.WALStatus == walStatusUnreserved:
		kind = slotWarnUnreserved
	case snap.MaxKeepSizeBytes > 0 && percent >= thr.CriticalPercent:
		kind = slotWarnRetention85
	case snap.MaxKeepSizeBytes > 0 && percent >= thr.WarnPercent:
		kind = slotWarnRetention70
	case !snap.Active:
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

	// Same condition as the last emission: gate on the rate-limit
	// window. Reaching the same kind also disarms any pending downgrade
	// — the reading bounced back before the lower state persisted,
	// which is exactly the flap the hysteresis exists to absorb.
	if kind == st.lastFiredKind {
		st.pendingDowngradeArmed = false
		if kind == slotWarnNone {
			return dec // still clean, nothing to say
		}
		last, ever := st.lastEmittedAt[kind]
		if !ever || now.Sub(last) >= thr.RateLimitWindow {
			dec.Emit = true
		}
		return dec
	}

	// Escalation (rank up, including the first fire from clean): emit
	// unconditionally and immediately — a warn70 → critical85 move
	// inside the rate-limit window is exactly when the operator most
	// needs promptness (ADR-0059 transition rule).
	if slotWarnRank(kind) > slotWarnRank(st.lastFiredKind) {
		st.pendingDowngradeArmed = false
		dec.Emit = true
		return dec
	}

	// Downgrade (rank down, including → clean): hysteresis (LOW-D0-18).
	// The lower state must persist for one extra probe tick before it
	// emits; without this, a reading hovering on the 70/85 boundary
	// (or flapping across 70/clean) pages on every 30s probe because
	// each bounce is a "transition" that bypasses the 5m rate limit.
	if !st.pendingDowngradeArmed || st.pendingDowngradeKind != kind {
		st.pendingDowngradeArmed = true
		st.pendingDowngradeKind = kind
		return dec // held this tick; commits next tick if it persists
	}
	st.pendingDowngradeArmed = false

	// Clear-event: previously firing, now clean (persisted one tick).
	// Emit a single INFO so the operator sees the alarm resolve.
	if kind == slotWarnNone {
		dec.Cleared = true
		return dec
	}

	// Persisted downgrade to a lower non-clean condition.
	dec.Emit = true
	return dec
}

// evaluateSlotVanished is the LOW-D0-17 decision for a probe that
// returns ok=false (no row in pg_replication_slots). Two benign shapes
// stay silent: the cold-start race (the slot row hasn't materialised
// yet — nothing has ever fired) and a slot dropped while HEALTHY (an
// operator deliberately dropping an idle slot is their own action; the
// streamer's read path fails loudly if it was still needed). But a slot
// that vanishes while a condition is OUTSTANDING is the terminal end of
// the story the operator was just paged about — pre-fix it skipped
// silently forever, leaving the last page ("critical") as the operator's
// final, now-wrong, picture. That shape pages CRITICAL once and latches
// (same terminal class as 'lost').
func evaluateSlotVanished(st *slotHealthState) slotWarningDecision {
	if st.terminalLatched || st.lastFiredKind == slotWarnNone {
		return slotWarningDecision{Kind: st.lastFiredKind}
	}
	return slotWarningDecision{Kind: slotWarnDropped, Emit: true}
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
		if isTerminalSlotWarning(dec.Kind) {
			// Terminal conditions page once and latch (MED-D0-9 /
			// LOW-D0-17) — see the slotHealthState field comment.
			st.terminalLatched = true
		}
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
	case slotWarnUnreserved:
		slog.WarnContext(
			ctx, "postgres: slot-health: retention cap exceeded — invalidation at next checkpoint",
			slog.String("stream_id", streamID),
			slog.String("slot", snap.SlotName),
			slog.String("wal_status", snap.WALStatus),
			slog.Int64("lag_bytes", snap.LagBytes),
			slog.Int64("max_slot_wal_keep_size_bytes", snap.MaxKeepSizeBytes),
			slog.Bool("critical", true),
			slog.String("hint", slotHealthHint(snap, dec)),
			slog.String("see", "ADR-0059"),
		)
	// The two terminal conditions log at ERROR: they are the loss event
	// itself, not a pre-warning, and they are the ONLY emission the
	// operator will get (the latch suppresses repeats and clears).
	case slotWarnLost:
		slog.ErrorContext(
			ctx, "postgres: slot-health: slot INVALIDATED (wal_status=lost) — terminal, re-snapshot required",
			slog.String("stream_id", streamID),
			slog.String("slot", snap.SlotName),
			slog.String("wal_status", snap.WALStatus),
			slog.Bool("critical", true),
			slog.Bool("terminal", true),
			slog.String("hint", slotHealthHint(snap, dec)),
			slog.String("see", "ADR-0059"),
		)
	case slotWarnDropped:
		slog.ErrorContext(
			ctx, "postgres: slot-health: slot dropped mid-stream — terminal",
			slog.String("stream_id", streamID),
			slog.String("slot", snap.SlotName),
			slog.Bool("critical", true),
			slog.Bool("terminal", true),
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
	case slotWarnUnreserved:
		return fmt.Sprintf(
			"slot %q has exceeded max_slot_wal_keep_size (wal_status='unreserved'); Postgres will invalidate it at the NEXT CHECKPOINT unless the consumer catches up immediately — this is the last window where intervention can still save the slot: unstick/drain the consumer now, or raise max_slot_wal_keep_size before the checkpoint lands.",
			snap.SlotName,
		)
	case slotWarnLost:
		return fmt.Sprintf(
			"slot %q has been INVALIDATED by Postgres (wal_status='lost'): the WAL it required has been removed and no consumer can ever resume from it. Recovery requires a fresh re-snapshot (or a restore from backup); drop the slot if it is abandoned. This condition is terminal — this alert fires once and will not repeat or clear.",
			snap.SlotName,
		)
	case slotWarnDropped:
		return fmt.Sprintf(
			"slot %q no longer exists on the source: it was dropped while a health condition was outstanding. If an operator dropped it deliberately this is expected; otherwise the CDC stream cannot resume from it and a fresh re-snapshot is required. This condition is terminal — this alert fires once and will not repeat or clear.",
			snap.SlotName,
		)
	}
	return ""
}

// slotHealthProbeFailureEscalateAfter is the number of CONSECUTIVE
// probe failures after which the loop escalates from per-tick DEBUG to
// an operator-visible WARN + page (MED-D0-11): while the probe is
// failing, the ENTIRE slot-health paging net is blind — a revoked role
// or killed connection would otherwise silently disable the watcher the
// net exists to be. Five failures at the 30s production cadence is
// ~2.5 minutes of blindness: long enough to skip a transient PG hiccup
// or restart, short enough that the operator hears about a persistent
// outage well inside the retention runway the thresholds assume. The
// escalation fires once per outage streak (a blind net is a fact, not
// a repeating event); a subsequent successful probe logs a recovery
// INFO and re-arms it.
const slotHealthProbeFailureEscalateAfter = 5

// slotHealthProbeLoop is the per-stream background goroutine that
// runs the F13 probe on a fixed cadence. Owned by the streamer; exits
// cleanly on ctx cancellation. Errors from the underlying reporter
// log at DEBUG (a transient PG hiccup shouldn't itself trigger an
// operator-visible WARN about the warner) and the tick advances —
// until [slotHealthProbeFailureEscalateAfter] consecutive failures,
// at which point the sustained outage WARNs and pages (MED-D0-11).
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
	probeFailures := 0 // consecutive; reset by any successful probe

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			snap, ok, err := reporter.SlotHealth(ctx, slotName)
			if err != nil {
				probeFailures++
				if probeFailures == slotHealthProbeFailureEscalateAfter {
					// MED-D0-11: a sustained probe outage means the
					// whole paging net is blind — say so loudly, once
					// per outage streak, through the same sink the net
					// pages.
					slog.WarnContext(
						ctx, "postgres: slot-health probe failing — retention/invalidation alerts are blind until it recovers",
						slog.String("stream_id", streamID),
						slog.String("slot", slotName),
						slog.Int("consecutive_failures", probeFailures),
						slog.String("err", err.Error()),
						slog.String("hint", "check that the probe connection's role still has access to pg_replication_slots and that the network path to the source is healthy"),
						slog.String("see", "ADR-0059"),
					)
					notifySlotProbeFailure(ctx, notifier, streamID, slotName, probeFailures, err)
					continue
				}
				slog.DebugContext(
					ctx, "postgres: slot-health probe failed (will retry next tick)",
					slog.String("stream_id", streamID),
					slog.String("slot", slotName),
					slog.String("err", err.Error()),
				)
				continue
			}
			if probeFailures >= slotHealthProbeFailureEscalateAfter {
				// The outage that WARNed above has resolved — close the
				// loop for the operator (log-only; recovery is not a
				// page, mirroring the cleared-INFO posture).
				slog.InfoContext(
					ctx, "postgres: slot-health probe recovered",
					slog.String("stream_id", streamID),
					slog.String("slot", slotName),
					slog.Int("failed_probes", probeFailures),
				)
			}
			probeFailures = 0
			if !ok {
				// No slot row. Cold-start race (never fired) → silent
				// skip, the next tick will find it. Dropped while a
				// condition was outstanding → terminal page + latch
				// (LOW-D0-17); see evaluateSlotVanished.
				dec := evaluateSlotVanished(state)
				if dec.Emit {
					gone := ir.SlotHealth{SlotName: slotName}
					emitSlotHealthWarning(ctx, gone, streamID, dec)
					notifySlotHealthCrossing(ctx, notifier, streamID, gone, dec)
					recordSlotHealthEmission(state, dec, now)
				}
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
