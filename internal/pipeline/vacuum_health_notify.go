// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Target-side autovacuum / dead-tuple / wraparound advisory rules — the
// ADR-0107 item-36 alerter's vacuum rule family (roadmap 2026-07-22, from
// the Hatchet "Postgres survival guide" review).
//
// A sluice bulk copy into Postgres is a sustained high-write workload, and
// so is a long CDC catch-up against a high-churn source — exactly the shape
// where dead tuples outrun autovacuum, with transaction-ID wraparound as
// the worst case. These two rules watch the TARGET's own catalog for that:
//
//   - dead_tuple_ratio — the worst user table's n_dead_tup/(n_dead_tup+
//     n_live_tup) at or above the threshold (autovacuum losing to the
//     write load; bloat accumulating);
//   - xid_age — the database's age(datfrozenxid) at or above the
//     threshold (wraparound headroom being consumed; Postgres force-stops
//     around ~2.1B).
//
// Unlike the ADR-0107 snapshot rules this needs NO PlanetScale telemetry
// provider — it follows the item-45 sync-lag alerter's shape instead: an
// optional target-side probe ([ir.TargetVacuumHealthReporter], implemented
// by the Postgres applier over the pool it already holds), the SAME
// edge-trigger + cooldown + hysteresis decision ([evalThresholdAlert]), and
// the SAME sinks ([Streamer.buildMetricsNotifier]). ADVISORY ONLY: sluice's
// writes stay correct regardless; probe and sink errors are logged at WARN
// and swallowed, never able to stall or crash the sync.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// Rule identities (stable — they appear in Notification.Metric and logs).
const (
	notifyDeadTupleRatio notifyMetric = "dead_tuple_ratio"
	notifyXIDAge         notifyMetric = "xid_age"
)

// startVacuumHealthNotifier spawns the vacuum rule family's alerter for the
// stream. No-op (no goroutine) when neither threshold is set or no sink is
// configured — the zero-config case costs nothing and the zero value is the
// safe off default. A configured threshold against a target whose applier
// does not expose the probe (a non-Postgres target) WARNs once and returns:
// per the loud-failure tenet an operator who opted in must learn the rule is
// inert, but the alerter is advisory so it never fails the stream.
//
// One goroutine ticks at [Streamer.vacuumHealthTick] (telemetryPollInterval
// unless a test injects a faster cadence); unlike the snapshot alerter
// each tick is a LIVE probe (two cheap catalog reads against the applier's
// pool — pg_stat_user_tables and pg_database — well off the apply hot
// path). It exits on ctx.Done; the caller does not track it.
func (s *Streamer) startVacuumHealthNotifier(ctx context.Context, streamID string, applier ir.ChangeApplier) {
	if s.NotifyDeadTupleRatio <= 0 && s.NotifyXIDAge <= 0 {
		return
	}
	notifier := s.buildMetricsNotifier()
	if notifier == nil {
		return
	}
	logger := slog.Default()
	reporter, ok := applier.(ir.TargetVacuumHealthReporter)
	if !ok {
		logger.WarnContext(
			ctx, "vacuum-health alert: the target engine does not expose autovacuum/dead-tuple health; --notify-dead-tuple-ratio/--notify-xid-age are inert on this target (Postgres targets only)",
			slog.String("stream_id", streamID),
		)
		return
	}
	cooldown := s.notifyCooldown()
	state := map[notifyMetric]*metricsNotifyRuleState{
		notifyDeadTupleRatio: {},
		notifyXIDAge:         {},
	}
	deadTupleThreshold, xidAgeThreshold := s.NotifyDeadTupleRatio, s.NotifyXIDAge
	go func() {
		ticker := time.NewTicker(s.vacuumHealthTick())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runVacuumHealthNotifyTick(ctx, logger, reporter, notifier, streamID, deadTupleThreshold, xidAgeThreshold, state, cooldown, time.Now)
			}
		}
	}()
}

// vacuumHealthTick resolves the alerter's tick cadence: the injected
// test override when set, the canonical telemetryPollInterval otherwise
// (the zero value is the production default — the v0.99.51 lesson).
func (s *Streamer) vacuumHealthTick() time.Duration {
	if s.vacuumHealthTickInterval > 0 {
		return s.vacuumHealthTickInterval
	}
	return telemetryPollInterval
}

// runVacuumHealthNotifyTick is one alerter tick: probe the target, evaluate
// the two rules with the shared edge-trigger + cooldown + hysteresis
// decision, and deliver fires failure-isolated. A probe failure or ok=false
// reading skips the tick — no fire, no re-arm — per the *Known honesty
// contract (absence of signal is not "recovered"). A rule whose threshold
// is unset (<= 0) is inert. Pulled out of the goroutine with an injected
// clock so the decision path is unit-testable deterministically.
func runVacuumHealthNotifyTick(
	ctx context.Context,
	logger *slog.Logger,
	reporter ir.TargetVacuumHealthReporter,
	notifier notify.Notifier,
	streamID string,
	deadTupleThreshold, xidAgeThreshold float64,
	state map[notifyMetric]*metricsNotifyRuleState,
	cooldown time.Duration,
	now func() time.Time,
) {
	health, ok, err := reporter.TargetVacuumHealth(ctx)
	if err != nil {
		// Failure isolation: an unreachable/unreadable stats view must never
		// stall or crash the sync. WARN and wait for the next tick.
		logger.WarnContext(
			ctx, "vacuum-health alert: target probe failed (advisory only, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
		return
	}
	if !ok {
		return
	}
	var notifs []notify.Notification
	if deadTupleThreshold > 0 &&
		evalThresholdAlert(state[notifyDeadTupleRatio], health.DeadTupleRatio, deadTupleThreshold, cooldown, now()) {
		notifs = append(notifs, deadTupleNotification(health, deadTupleThreshold, now()))
	}
	if xidAgeThreshold > 0 &&
		evalThresholdAlert(state[notifyXIDAge], float64(health.XIDAge), xidAgeThreshold, cooldown, now()) {
		notifs = append(notifs, xidAgeNotification(health, xidAgeThreshold, now()))
	}
	for _, n := range notifs {
		n.StreamID = streamID
		if err := notifier.Notify(ctx, n); err != nil {
			logger.WarnContext(
				ctx, "vacuum-health alert: notify failed (advisory only, sync unaffected)",
				slog.String("stream_id", streamID),
				slog.String("metric", n.Metric),
				slog.String("error", err.Error()),
			)
		}
	}
}

// deadTupleNotification renders the dead-tuple-ratio fire. The body carries
// the diagnosis detail the probe already paid for — which table, how much
// bloat, and whether autovacuum has been reaching it at all — so the
// operator's first question ("is autovacuum running but losing, or not
// running?") is answered in the page itself.
func deadTupleNotification(h ir.VacuumHealth, threshold float64, at time.Time) notify.Notification {
	const title = "target dead tuples accumulating (autovacuum falling behind)"
	body := formatThresholdBody(notifyDeadTupleRatio, h.DeadTupleRatio, threshold, title)
	body += fmt.Sprintf("; worst table %s: %d dead / %d live, last autovacuum %s (%d completed runs). Sustained bulk writes can outrun autovacuum — consider lowering autovacuum_vacuum_cost_delay / raising autovacuum_max_workers on the target for the duration.",
		h.WorstTable, h.DeadTuples, h.LiveTuples, humanizeSince(h.LastAutovacuum, at), h.AutovacuumCount)
	return notify.Notification{
		Level:     notify.LevelWarning,
		Metric:    string(notifyDeadTupleRatio),
		Title:     title,
		Body:      body,
		Value:     h.DeadTupleRatio,
		Threshold: threshold,
		At:        at,
	}
}

// xidAgeNotification renders the wraparound-headroom fire. CRITICAL: unlike
// bloat, a database that reaches the wraparound horizon (~2.1B) is force-
// stopped by Postgres itself.
func xidAgeNotification(h ir.VacuumHealth, threshold float64, at time.Time) notify.Notification {
	const title = "target transaction-ID age high (wraparound headroom shrinking)"
	body := formatThresholdBody(notifyXIDAge, float64(h.XIDAge), threshold, title)
	body += fmt.Sprintf("; database %q at age(datfrozenxid) %d of the ~2.1B horizon. Autovacuum's freeze cycles normally hold this near autovacuum_freeze_max_age (default 200M); if it keeps climbing under load, run VACUUM FREEZE on the busiest tables or give autovacuum more headroom.",
		h.Datname, h.XIDAge)
	return notify.Notification{
		Level:     notify.LevelCritical,
		Metric:    string(notifyXIDAge),
		Title:     title,
		Body:      body,
		Value:     float64(h.XIDAge),
		Threshold: threshold,
		At:        at,
	}
}

// humanizeSince renders "how long ago" for the alert body: "never" for the
// zero time (autovacuum has not completed on the table since stats reset),
// a whole-unit duration otherwise. Coarse on purpose — an alert body, not a
// metric.
func humanizeSince(t, now time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := now.Sub(t)
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return "under a minute ago"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}
