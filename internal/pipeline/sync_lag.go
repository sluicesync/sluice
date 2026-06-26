// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Engine-neutral "seconds behind source" sync-lag signal (roadmap item 45).
//
// This is sluice's OWN apply lag — how far the target trails the source's
// latest applied commit — derived purely from the IR change's source commit
// timestamp ([ir.Change.SourceCommitTime]). It is deliberately DISTINCT from
// the PlanetScale control-plane `sluice_target_replica_lag_seconds`
// (ADR-0107): that one is the target's internal replica lag and is PS-gated;
// this one works on MySQL and Postgres alike with no telemetry provider.
//
// It is also distinct from `sluice_seconds_since_last_apply` (now − the
// control row's UpdatedAt), which AGES on a quiet stream and exists to catch
// a STUCK/not-applying stream. Sync lag, by contrast, is honest when idle:
// once the applier has drained the available source changes it reports 0
// (caught up), never a number that grows with wall-clock. The two are
// complementary — alert on sync lag for "falling behind while flowing" and
// on seconds-since-last-apply for "not applying at all".

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// syncLagCaughtUpAfter is how long the applier may go without applying a
// source-timestamped change before the lag reading is reported as caught-up
// (0). It exists ONLY to retire a STALE non-zero reading after the source
// goes quiet: on a flowing-but-behind stream the reader keeps the apply
// channel fed (the backlog drains continuously), so the gap between applied
// changes stays small and a genuine lag is held through transient apply
// hiccups; only a SUSTAINED absence of applied work flips the reading to
// "caught up". It is sized well above the batch idle-flush (100ms) and the
// telemetry poll cadence so a brief target stall does not falsely zero a
// real lag, while a genuinely idle/caught-up source still settles to 0.
//
// Caveat (documented, not a bug): a stream that is behind AND wedged (target
// down, nothing flowing) flips to 0 here after the window — that "not
// applying at all" failure mode is the job of `sluice_seconds_since_last_apply`
// (which ages); sync lag answers only "falling behind while flowing".
const syncLagCaughtUpAfter = 30 * time.Second

// syncLagSource is the read side of the sync-lag signal consumed by the
// metrics endpoint and the threshold alerter. Kept an interface so the
// [MetricsServer] and the notifier stay decoupled from the concrete tracker
// (and so tests can inject a fixed reading).
type syncLagSource interface {
	// SyncLagSeconds reports (seconds-behind-source, known) as of now.
	// known is false until at least one change carrying a source commit
	// timestamp has been applied — the *Known honesty contract: absence
	// of signal is reported as "unknown" (metric omitted), never as 0.
	SyncLagSeconds(now time.Time) (seconds float64, known bool)
}

// syncLagTracker is the lock-free write+read side of the sync-lag signal. The
// interceptor goroutine ([observeSyncLagChanges]) calls observe on every
// change flowing to the applier; the metrics scrape and the alerter tick call
// SyncLagSeconds. Both fields are atomics so the read never blocks the apply
// path (mirrors the AIMD snapshot posture — no mutex on the hot path); a
// scrape that reads the two atomics a hair apart is a benign sub-tick skew on
// an advisory gauge.
type syncLagTracker struct {
	// observedAtUnixNano is the wall-clock (UnixNano) of the most recent
	// observe with a non-zero source commit time. 0 ⇒ nothing observed yet.
	observedAtUnixNano atomic.Int64
	// frozenLagNanos is (observedAt − commitTime − applyDelay), clamped ≥0,
	// FROZEN at observe time so it does not age between changes — the key to
	// idle honesty: a caught-up stream that applied its last change promptly
	// holds a ~0 reading rather than a value that climbs with wall-clock.
	frozenLagNanos atomic.Int64
	// applyDelay is the configured [Streamer.ApplyDelay] (roadmap item 46,
	// ADR-0121 §5). It is SUBTRACTED from the observed lag so a deliberately
	// delayed replica reads ~0 ("not falling behind") rather than `delay`
	// seconds: the change is observed at now ≈ commitTime + applyDelay, and we
	// back the intentional delay out. Zero on every non-delayed stream, so the
	// subtraction is a no-op there (byte-identical to the pre-item-46 metric).
	// Immutable after construction; read without synchronisation.
	applyDelay time.Duration
}

func newSyncLagTracker(applyDelay time.Duration) *syncLagTracker {
	return &syncLagTracker{applyDelay: applyDelay}
}

// observe records one applied-bound change. A zero commitTime is ignored (the
// source/path supplied no timestamp — see [ir.Change.SourceCommitTime]); the
// lag is floored at 0 so source/target clock skew, a future-dated commit, or
// the configured apply-delay subtraction can never report a negative "behind".
func (t *syncLagTracker) observe(commitTime, now time.Time) {
	if commitTime.IsZero() {
		return
	}
	lag := now.Sub(commitTime) - t.applyDelay
	if lag < 0 {
		lag = 0
	}
	t.frozenLagNanos.Store(lag.Nanoseconds())
	t.observedAtUnixNano.Store(now.UnixNano())
}

// SyncLagSeconds implements [syncLagSource]. See [syncLagCaughtUpAfter] for
// the idle/caught-up rule.
func (t *syncLagTracker) SyncLagSeconds(now time.Time) (float64, bool) {
	obs := t.observedAtUnixNano.Load()
	if obs == 0 {
		return 0, false // no source-timestamped change applied yet — unknown
	}
	if now.UnixNano()-obs > syncLagCaughtUpAfter.Nanoseconds() {
		return 0, true // sustained idle ⇒ caught up
	}
	return float64(t.frozenLagNanos.Load()) / float64(time.Second), true
}

// observeSyncLagChanges is the pass-through interceptor that feeds the
// tracker. It observes every change on its way to the applier — covering the
// batched, per-change, and concurrent-lane apply paths uniformly, and (on a
// merged multi-shard VStream) naturally surfacing the slowest shard's lag as
// its older-timestamped events flow. It is wired only when the operator opted
// into the metrics endpoint or a sync-lag alert, so the default apply path is
// byte-identical (no extra goroutine / channel hop).
func observeSyncLagChanges(ctx context.Context, in <-chan ir.Change, tracker *syncLagTracker) <-chan ir.Change {
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				tracker.observe(c.SourceCommitTime(), time.Now())
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// notifySyncLag is the rule identity for the engine-neutral sync-lag alert.
// Distinct string from notifyLagSeconds ("replica_lag_seconds") so a dashboard
// never conflates apply lag with the PS target-internal replica lag.
const notifySyncLag notifyMetric = "sync_lag_seconds"

// runSyncLagNotifyTick is one tick of the ungated sync-lag threshold alerter:
// read the current lag from the source, and (when observed) apply the SAME
// edge-trigger + cooldown + hysteresis decision the target-metrics rules use
// ([evalThresholdAlert]), delivering a failure-isolated notification on a
// fire. An UNOBSERVED reading is skipped — no fire, no re-arm — exactly as the
// snapshot rules skip an unobserved metric (the *Known honesty contract).
func runSyncLagNotifyTick(
	ctx context.Context,
	logger *slog.Logger,
	source syncLagSource,
	notifier notify.Notifier,
	streamID string,
	threshold float64,
	st *metricsNotifyRuleState,
	cooldown time.Duration,
	now func() time.Time,
) {
	value, known := source.SyncLagSeconds(now())
	if !known {
		return
	}
	if !evalThresholdAlert(st, value, threshold, cooldown, now()) {
		return
	}
	n := notify.Notification{
		Level:     notify.LevelCritical,
		StreamID:  streamID,
		Metric:    string(notifySyncLag),
		Title:     "sync lag high (target falling behind source)",
		Body:      formatThresholdBody(notifySyncLag, value, threshold, "sync lag high (target falling behind source)"),
		Value:     value,
		Threshold: threshold,
		At:        now(),
	}
	if err := notifier.Notify(ctx, n); err != nil {
		// Failure isolation (load-bearing): a dead sink is logged once per
		// failed fire and SWALLOWED — never propagated, never fatal.
		logger.WarnContext(
			ctx, "sync-lag alert: notify failed (advisory only, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("metric", n.Metric),
			slog.String("error", err.Error()),
		)
	}
}

// syncLagObservationWanted reports whether the operator has opted into
// observing the sync-lag signal — either by exposing the /metrics endpoint
// (the gauge) or by configuring a sync-lag alert WITH a sink. When false the
// streamer wires no interceptor and spawns no goroutine, keeping the default
// apply path byte-identical.
func (s *Streamer) syncLagObservationWanted() bool {
	return s.MetricsListen != "" ||
		(s.NotifySyncLagSeconds > 0 && s.buildMetricsNotifier() != nil)
}

// startSyncLagNotifier spawns the ungated sync-lag threshold alerter for the
// stream. No-op (no goroutine) when the tracker is absent, the threshold is
// unset, or no sink is configured — so the zero-config case costs nothing and
// the zero value is the safe off default. One goroutine ticks at
// telemetryPollInterval; each tick reads the tracker and fires edge-triggered,
// cooldown'd, failure-isolated notifications. Unlike the ADR-0107 alerter this
// needs NO PlanetScale telemetry provider — it works on any engine. The caller
// does not track the goroutine; it exits on ctx.Done.
func (s *Streamer) startSyncLagNotifier(ctx context.Context, streamID string) {
	if s.syncLag == nil || s.NotifySyncLagSeconds <= 0 {
		return
	}
	notifier := s.buildMetricsNotifier()
	if notifier == nil {
		return
	}
	cooldown := s.notifyCooldown()
	logger := slog.Default()
	st := &metricsNotifyRuleState{}
	threshold := s.NotifySyncLagSeconds
	source := s.syncLag
	go func() {
		ticker := time.NewTicker(telemetryPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runSyncLagNotifyTick(ctx, logger, source, notifier, streamID, threshold, st, cooldown, time.Now)
			}
		}
	}()
}
