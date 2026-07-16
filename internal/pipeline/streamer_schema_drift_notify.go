// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/notify"
)

// ADR-0157 — schema-drift notification.
//
// When the ADR-0054/ADR-0058 schema-forward intercept REFUSES a source DDL
// it cannot auto-forward (RENAME COLUMN on MySQL, a volatile-DEFAULT ADD
// COLUMN, a multi-shape combo, a target apply failure), it stores the
// refusal into [Streamer.schemaSnapshotErr] and the stream STALLS at the
// boundary — no data is lost (the loud-failure tenet), but for an
// unattended operator the stall is invisible until someone reads the logs.
// This fires a critical [notify.Notification] to the SAME sinks the metrics
// alerter uses (webhook/Slack/SMTP), carrying the drift detail + the
// recovery steps, the moment the streamer SURFACES the refusal in
// [Streamer.phaseSettleDispatch].
//
// Distinct from the metrics alerter in three ways: it is EVENT-DRIVEN (fires
// at the refusal, not on a poll tick), needs NO telemetry provider (works on
// every engine pair), and has NO numeric threshold (a discrete stall event —
// hence [notify.CategorySchemaDrift]). It reuses the metrics alerter's sink
// assembly ([buildMetricsNotifierFrom]) and failure-isolation contract
// verbatim: a notify error is logged at WARN and SWALLOWED — the sync is
// already stalled on the drift, the notification is purely advisory.

// makeSchemaDriftNotification maps a schema-forward REFUSAL error into the
// operator-facing schema-drift [notify.Notification]. Pure (no stream, no
// I/O) so the Level/Category/Title/Body mapping is unit-testable — mirrors
// how [makeNotification] factors the threshold path. The Body is the
// refusal's own message: it already carries the shape + the offending table
// (from the ADR-0060 drift diff) AND the forwardRecoveryHint recovery steps,
// so the alert tells the operator exactly what drifted and how to recover.
func makeSchemaDriftNotification(streamID string, refusal error, at time.Time) notify.Notification {
	return notify.Notification{
		Level:    notify.LevelCritical,
		Category: notify.CategorySchemaDrift,
		StreamID: streamID,
		Title:    fmt.Sprintf("Schema change stalled sync %q — manual recovery needed", streamID),
		Body:     refusal.Error(),
		At:       at,
	}
}

// schemaDriftLatchDecision is the PURE edge-once latch decision. Given the
// message identity of the currently pending refusal (empty ⇒ no pending
// refusal, i.e. the stall cleared) and the identity last fired on, it
// returns whether to fire now and what the latch should become:
//
//   - a NEW non-empty identity ⇒ FIRE, latch := pending (rising edge)
//   - the SAME identity re-observed ⇒ HOLD (already fired), latch unchanged
//   - an empty pending ⇒ RE-ARM, latch := "" (so the next distinct refusal,
//     or the same one after a genuine resume, fires again)
//
// Keeping this a free function means the "same error twice ⇒ one fire;
// re-arm after clear ⇒ fires again" semantics are unit-testable without a
// live stream (the counterpart to [evalThresholdAlert] for the event path).
func schemaDriftLatchDecision(pending, lastFired string) (fire bool, newLatch string) {
	if pending == "" {
		return false, "" // stall cleared → re-arm
	}
	if pending == lastFired {
		return false, lastFired // same refusal, already alerted
	}
	return true, pending // new refusal → fire
}

// observeSchemaDriftForNotify fires the ADR-0157 schema-drift alert once per
// distinct schema-forward refusal, at the moment the streamer surfaces it in
// [phaseSettleDispatch]. It reads the pending refusal from
// [Streamer.schemaSnapshotErr], applies the edge-once latch
// ([schemaDriftLatchDecision]) so a retry re-observing the same refusal does
// NOT re-fire, and re-arms when no refusal is pending. "Once" means once
// DELIVERED (audit MED-D0-10): the latch advances only after a successful
// Notify, so a transiently dead sink at the stall moment doesn't
// permanently swallow the stall's only page — delivery is re-attempted on
// each subsequent settle tick until it lands.
//
// Gated (zero-value-safe): fires only when NOT SuppressSchemaDriftNotify AND
// a sink is configured. Telemetry-independent — it never consults the
// telemetry provider. Failure-isolated exactly like [runMetricsNotifyTick]:
// a notify error is logged at WARN and SWALLOWED, never propagated (the sync
// is already stalled on the drift; the alert is advisory). Owned by the
// settle path, which is single-goroutine per attempt with sequential
// retries, so the latch field needs no synchronization.
func (s *Streamer) observeSchemaDriftForNotify(ctx context.Context, streamID string) {
	if s.SuppressSchemaDriftNotify {
		return
	}
	var pending error
	if p := s.schemaSnapshotErr.Load(); p != nil {
		pending = *p
	}
	pendingID := ""
	if pending != nil {
		pendingID = pending.Error()
	}
	fire, newLatch := schemaDriftLatchDecision(pendingID, s.lastSchemaDriftNotified)
	if !fire {
		// HOLD (already alerted) or RE-ARM (stall cleared) — both commit
		// the latch unconditionally; neither involves a delivery.
		s.lastSchemaDriftNotified = newLatch
		return
	}
	notifier := s.schemaDriftNotifier()
	if notifier == nil {
		// No sink configured — inert (the "opt in by configuring a sink"
		// model, same as the metrics alerts). Advance the latch anyway:
		// with no sink we never deliver regardless, and re-checking a
		// pending refusal forever buys nothing.
		s.lastSchemaDriftNotified = newLatch
		return
	}
	n := makeSchemaDriftNotification(streamID, pending, time.Now())
	if err := notifier.Notify(ctx, n); err != nil {
		// Failure isolation (load-bearing): a dead sink is logged and
		// SWALLOWED — never propagated, never fatal. The sync is already
		// stalled on the drift; the notification is advisory only.
		//
		// Audit finding MED-D0-10: the latch does NOT advance on a failed
		// delivery. This alert is the ONLY page a persistent stall gets
		// (every subsequent settle retry re-observes the same refusal and
		// HOLDs), so advancing the latch before delivery let one transient
		// sink error at the stall moment permanently swallow it. Leaving
		// the latch un-advanced makes each settle-retry tick re-attempt
		// delivery (each failure logged here) until one succeeds — chosen
		// over a separate re-fire timer because it reuses the existing
		// edge-latch shape and adds no new state or cadence to reason
		// about; the retry cadence is the settle path's own.
		slog.WarnContext(
			ctx, "schema-drift alert: notify failed (will retry on next settle tick)",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
		return
	}
	s.lastSchemaDriftNotified = newLatch
}

// schemaDriftNotifier resolves the sink set for the schema-drift alert. It
// reuses the metrics alerter's sink assembly ([buildMetricsNotifierFrom]) —
// the SINGLE definition of the sink set, so schema-drift and metrics alerts
// can never target different sinks (ADR-0157 §4). The test seam is honoured
// first so a test can capture the fired notification without a real sink.
func (s *Streamer) schemaDriftNotifier() notify.Notifier {
	if s.schemaDriftNotifierForTest != nil {
		return s.schemaDriftNotifierForTest
	}
	return buildMetricsNotifierFrom(s.NotifyWebhookURL, s.NotifySlackWebhookURL, s.NotifySMTP)
}
