// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// ADR-0107 item 36 — sync-scoped target-metrics threshold ALERTER sidecar.
//
// A slow-tick goroutine that mirrors [Streamer.startStorageHeadroomWatch] /
// [Streamer.startTargetMetricsHistoryRecorder]: each tick it reads the
// telemetry provider's CACHED snapshot (off the apply hot path) and
// evaluates a set of operator-configured threshold rules. When a rule
// transitions not-breached → breached (EDGE-TRIGGER) it fires one
// [notify.Notification] to the configured sinks; while still breached it
// re-fires at most once per cooldown (a sustained-breach reminder, not a
// per-tick flood); it re-arms only after the metric recovers below the
// threshold (with a small hysteresis margin so a value parked right at the
// line doesn't flap).
//
// OBSERVABILITY ONLY — never on the value path. A notifier error is logged
// at WARN and SWALLOWED: a dead Slack/webhook sink must never stall or
// crash the sync. nil provider / nil notifier / no active rule ⇒ no
// goroutine (the default, pre-item-36 byte-for-byte).

const (
	// defaultNotifyCooldown is the minimum interval between re-fires of a
	// STILL-breached rule. 15m means a sustained breach reminds 4x/hour
	// rather than every telemetryPollInterval (60s) — enough to stay on the
	// operator's radar without spamming.
	defaultNotifyCooldown = 15 * time.Minute

	// notifyRearmHysteresis is the fraction of the threshold a metric must
	// drop BELOW to re-arm a fired rule (re-arm at threshold*0.97). A small
	// margin avoids flapping when a value oscillates right at the line: once
	// fired, the rule stays latched until the metric genuinely recovers, not
	// merely dips a hair under the threshold for one poll.
	notifyRearmHysteresis = 0.97
)

// notifyMetric identifies a threshold rule. Strings are stable (they appear
// in the emitted Notification.Metric and in logs).
type notifyMetric string

const (
	notifyStorageUtil   notifyMetric = "storage_util"
	notifyCPUUtil       notifyMetric = "cpu_util"
	notifyMemUtil       notifyMetric = "mem_util"
	notifyLagSeconds    notifyMetric = "replica_lag_seconds"
	notifyStorageGrowth notifyMetric = "storage_growth_per_min"
)

// metricsNotifyRule is one configured threshold. read extracts the metric's
// current value from a snapshot and whether the provider observed it
// (the *Known honesty contract); a rule whose metric is unobserved this
// tick is skipped (neither fires nor re-arms). level is the Notification
// severity. The rate-of-change rule (storage growth) is special-cased: its
// value is a derived per-minute fraction, computed from the delta between
// the current and the previous snapshot (see evalMetricsNotifyTick).
type metricsNotifyRule struct {
	metric    notifyMetric
	threshold float64
	level     notify.Level
	title     string
	// read returns (value, observed). For the rate rule read is nil and the
	// value is derived in evalMetricsNotifyTick from the snapshot history.
	read func(snap ir.TargetHealthSnapshot) (float64, bool)
}

// metricsNotifyRuleState is the per-rule latch carried across ticks. fired
// is the edge latch (true == currently in a breach we've already alerted
// on); lastFired is when we last sent (for cooldown re-fire).
type metricsNotifyRuleState struct {
	fired     bool
	lastFired time.Time
}

// metricsNotifyState is the whole alerter's carried state: the per-rule
// latches keyed by metric, plus the previous storage sample for the
// rate-of-change rule. It is mutated in place by evalMetricsNotifyTick.
type metricsNotifyState struct {
	rules map[notifyMetric]*metricsNotifyRuleState

	// prevStorageUtil / prevStorageAt hold the last snapshot whose storage
	// was observed, so the growth rule can compute (Δutil / Δminutes). Zero
	// time ⇒ no prior sample yet (the rule can't fire on the first
	// observation; it needs two points to measure a rate).
	prevStorageUtil float64
	prevStorageAt   time.Time
}

func newMetricsNotifyState() *metricsNotifyState {
	return &metricsNotifyState{rules: map[notifyMetric]*metricsNotifyRuleState{}}
}

// buildMetricsNotifyRules assembles the active rule set from the streamer
// config. A rule whose threshold is 0 (unset) is INERT and omitted, so an
// operator opts each metric in individually. Returns nil when no rule is
// active (the alerter then no-ops).
func (s *Streamer) buildMetricsNotifyRules() []metricsNotifyRule {
	var rules []metricsNotifyRule
	add := func(metric notifyMetric, threshold float64, level notify.Level, title string, read func(ir.TargetHealthSnapshot) (float64, bool)) {
		if threshold <= 0 {
			return
		}
		rules = append(rules, metricsNotifyRule{
			metric:    metric,
			threshold: threshold,
			level:     level,
			title:     title,
			read:      read,
		})
	}
	add(notifyStorageUtil, s.NotifyStorageUtil, notify.LevelCritical, "target storage approaching capacity", func(snap ir.TargetHealthSnapshot) (float64, bool) {
		return snap.StorageUtil, snap.StorageKnown
	})
	add(notifyCPUUtil, s.NotifyCPUUtil, notify.LevelWarning, "target CPU saturating", func(snap ir.TargetHealthSnapshot) (float64, bool) {
		return snap.CPUUtil, snap.CPUKnown
	})
	add(notifyMemUtil, s.NotifyMemUtil, notify.LevelWarning, "target memory saturating", func(snap ir.TargetHealthSnapshot) (float64, bool) {
		return snap.MemUtil, snap.MemKnown
	})
	add(notifyLagSeconds, s.NotifyLagSeconds, notify.LevelCritical, "target replica lag high", func(snap ir.TargetHealthSnapshot) (float64, bool) {
		return snap.ReplicaLagSeconds, snap.LagKnown
	})
	// The storage rate-of-change rule has a nil reader; its value is derived
	// in evalMetricsNotifyTick from the storage delta between ticks.
	add(notifyStorageGrowth, s.NotifyStorageGrowthPerMin, notify.LevelCritical, "target storage climbing fast (auto-grow may be imminent)", nil)
	return rules
}

// buildMetricsNotifier assembles the [notify.Notifier] from the configured
// sink URLs: a generic webhook and/or a Slack incoming webhook. Returns a
// TRUE nil interface when no sink URL is set (NOT a typed-nil
// MultiNotifier), so startTargetMetricsNotifier's `notifier == nil` guard
// stays exact — assigning a nil MultiNotifier straight into the interface
// would yield a non-nil interface and a wrong "notifier configured" verdict.
func (s *Streamer) buildMetricsNotifier() notify.Notifier {
	var sinks []notify.Notifier
	if s.NotifyWebhookURL != "" {
		sinks = append(sinks, &notify.WebhookNotifier{URL: s.NotifyWebhookURL})
	}
	if s.NotifySlackWebhookURL != "" {
		sinks = append(sinks, &notify.SlackNotifier{WebhookURL: s.NotifySlackWebhookURL})
	}
	m := notify.NewMultiNotifier(sinks...)
	if m == nil {
		return nil
	}
	return m
}

// notifyCooldown returns the configured re-fire cooldown, defaulting to
// defaultNotifyCooldown when unset.
func (s *Streamer) notifyCooldown() time.Duration {
	if s.NotifyCooldown > 0 {
		return s.NotifyCooldown
	}
	return defaultNotifyCooldown
}

// startTargetMetricsNotifier spawns the item-36 threshold alerter for the
// stream. No-op (returns without spawning) when provider is nil, notifier
// is nil, or no rule is active — so the zero-config case costs nothing. One
// goroutine ticks at telemetryPollInterval; each tick reads the provider's
// cached sample and evaluates the rules, firing notifications with
// edge-trigger + cooldown semantics. The caller does not track the
// goroutine; it exits on ctx.Done.
func (s *Streamer) startTargetMetricsNotifier(ctx context.Context, streamID string, _ ir.ChangeApplier, provider ir.TargetTelemetry, notifier notify.Notifier) {
	if provider == nil || notifier == nil {
		return
	}
	rules := s.buildMetricsNotifyRules()
	if len(rules) == 0 {
		return
	}
	cooldown := s.notifyCooldown()
	logger := slog.Default()
	state := newMetricsNotifyState()
	go func() {
		ticker := time.NewTicker(telemetryPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runMetricsNotifyTick(ctx, logger, provider, notifier, streamID, rules, state, cooldown, time.Now)
			}
		}
	}()
}

// runMetricsNotifyTick is one alerter tick: read the cached sample, evaluate
// the rules into notifications, and deliver each (failure-isolated). Pulled
// out from the goroutine so the read + deliver path is also exercisable, but
// the pure decision logic lives in evalMetricsNotifyTick.
func runMetricsNotifyTick(
	ctx context.Context,
	logger *slog.Logger,
	provider ir.TargetTelemetry,
	notifier notify.Notifier,
	streamID string,
	rules []metricsNotifyRule,
	state *metricsNotifyState,
	cooldown time.Duration,
	now func() time.Time,
) {
	snap, ok := provider.Sample(ctx)
	if !ok || !snap.Fresh(now(), telemetryFreshnessWindow) {
		// No usable / stale signal — don't evaluate (a stale poll must not
		// fire a spurious alert or re-arm a latched one).
		return
	}
	notifs := evalMetricsNotifyTick(snap, rules, state, cooldown, now())
	for _, n := range notifs {
		n.StreamID = streamID
		if err := notifier.Notify(ctx, n); err != nil {
			// Failure isolation (load-bearing): a dead sink is logged once
			// per failed fire and SWALLOWED — never propagated, never fatal.
			logger.WarnContext(
				ctx, "target-metrics alert: notify failed (advisory only, sync unaffected)",
				slog.String("stream_id", streamID),
				slog.String("metric", n.Metric),
				slog.String("error", err.Error()),
			)
		}
	}
}

// evalMetricsNotifyTick is the PURE per-tick decision: given a fresh
// snapshot, the active rules, the carried latch state, the cooldown, and an
// injected clock, it returns the notifications to send and MUTATES state in
// place (latch transitions, cooldown timestamps, the rate-rule's previous
// sample). No I/O, no goroutine — so edge-trigger / cooldown / re-arm /
// hysteresis / rate-of-change are all unit-testable deterministically.
//
// Per rule, with v = the metric's current value (or derived rate):
//   - breached := v >= threshold
//   - FIRE when (breached && !fired)                         — rising edge
//     OR  (breached && fired && now-lastFired >= cooldown)   — re-fire
//   - RE-ARM (fired=false) when v < threshold*hysteresis     — recovered
//   - a metric UNOBSERVED this tick is skipped (no fire, no re-arm) — the
//     *Known honesty contract: absence of signal is not "recovered".
func evalMetricsNotifyTick(
	snap ir.TargetHealthSnapshot,
	rules []metricsNotifyRule,
	state *metricsNotifyState,
	cooldown time.Duration,
	now time.Time,
) []notify.Notification {
	var out []notify.Notification
	for _, rule := range rules {
		value, observed := ruleValue(rule, snap, state)
		if !observed {
			continue
		}
		st := state.rules[rule.metric]
		if st == nil {
			st = &metricsNotifyRuleState{}
			state.rules[rule.metric] = st
		}
		breached := value >= rule.threshold
		switch {
		case breached && !st.fired:
			// Rising edge — fire and latch.
			st.fired = true
			st.lastFired = now
			out = append(out, makeNotification(rule, value, now))
		case breached && st.fired && now.Sub(st.lastFired) >= cooldown:
			// Sustained breach past the cooldown — re-fire (reminder).
			st.lastFired = now
			out = append(out, makeNotification(rule, value, now))
		case !breached && value < rule.threshold*notifyRearmHysteresis:
			// Recovered below the hysteresis floor — re-arm.
			st.fired = false
		}
		// breached && fired && within cooldown: hold (no fire).
		// !breached but still within [threshold*hysteresis, threshold):
		// hold the latch (hysteresis dead-band) so we don't flap.
	}
	return out
}

// ruleValue extracts a rule's current value + observed flag. For the
// storage rate-of-change rule (nil reader) it derives the per-minute growth
// fraction from the snapshot delta against the previous observed storage
// sample, updating state's prev cursor as a side effect. It reports
// observed=false until two storage points exist (a rate needs two).
func ruleValue(rule metricsNotifyRule, snap ir.TargetHealthSnapshot, state *metricsNotifyState) (float64, bool) {
	if rule.read != nil {
		return rule.read(snap)
	}
	// Rate-of-change (storage_growth_per_min). Computed as the change in
	// StorageUtil (a fraction of capacity) per minute since the previous
	// observed storage sample — chosen over GB/min so the threshold is
	// capacity-relative and engine/size-independent (documented).
	if !snap.StorageKnown || snap.SampledAt.IsZero() {
		return 0, false
	}
	prevAt := state.prevStorageAt
	prevUtil := state.prevStorageUtil
	// Advance the cursor for the next tick regardless of whether we can emit
	// a rate this tick.
	state.prevStorageAt = snap.SampledAt
	state.prevStorageUtil = snap.StorageUtil
	if prevAt.IsZero() || !snap.SampledAt.After(prevAt) {
		// No prior point, or the same/older poll (the source updates ~1/min;
		// a re-read of the same sample yields no new rate).
		return 0, false
	}
	minutes := snap.SampledAt.Sub(prevAt).Minutes()
	if minutes <= 0 {
		return 0, false
	}
	rate := (snap.StorageUtil - prevUtil) / minutes
	if rate < 0 {
		rate = 0 // storage shrank (e.g. post-grow) — not a growth signal.
	}
	return rate, true
}

// makeNotification builds the Notification for a fired rule. StreamID is set
// by the caller (runMetricsNotifyTick) so the pure evaluator stays
// stream-agnostic.
func makeNotification(rule metricsNotifyRule, value float64, at time.Time) notify.Notification {
	return notify.Notification{
		Level:     rule.level,
		Metric:    string(rule.metric),
		Title:     rule.title,
		Body:      fmt.Sprintf("%s %.4g ≥ %.4g (%s)", rule.metric, value, rule.threshold, rule.title),
		Value:     value,
		Threshold: rule.threshold,
		At:        at,
	}
}
