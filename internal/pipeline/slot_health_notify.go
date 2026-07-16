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

// Roadmap item 64a — slot-health notifications (ADR-0059 implementation
// note, following the ADR-0157 pattern).
//
// The ADR-0059 slot-health probe emits structured slog WARNs when the
// source Postgres replication slot approaches eviction (70%/85% of
// max_slot_wal_keep_size) or sits inactive for 30m — but slog is invisible
// to an unattended operator, and once the slot invalidates
// (wal_status='lost') the only recovery is a full re-snapshot. This
// promotes the SAME threshold crossings to the notification sinks the
// metrics alerter and schema-drift alerts use (webhook/Slack/SMTP): the
// 85% crossing pages at [notify.LevelCritical]; the 70% and inactivity
// crossings at [notify.LevelWarning].
//
// Deliberately ONE firing mechanism, not two: a notification fires exactly
// when the slog WARN fires ([slotWarningDecision.Emit]) — state transitions
// (clean→warn, warn→critical, cleared-then-re-entered) page immediately,
// and an unchanged in-condition repeat is held to the existing
// [SlotHealthThresholds.RateLimitWindow] (the sustained-condition reminder,
// the counterpart of the metrics alerter's cooldown re-fire). Reusing the
// evaluator's decision means the paged and logged surfaces can never
// disagree about when a crossing happened.
//
// Failure-isolated exactly like the ADR-0157 path: a notify error is
// logged at WARN and SWALLOWED — the probe (and the sync it watches) is
// never affected by a dead sink.

// makeSlotHealthNotification maps an emitting slot-health decision into the
// operator-facing [notify.Notification]. Pure (no stream, no I/O) so the
// Level/Category/Title/Body mapping is unit-testable — mirrors
// [makeSchemaDriftNotification]. The Body reuses the slog hint verbatim
// ([slotHealthHint] — slot name, the concrete reading, the remediation) and
// appends the raw slot facts (the GUC bound, wal_status, lag bytes) so the
// page carries everything the log line does.
func makeSlotHealthNotification(streamID string, snap ir.SlotHealth, dec slotWarningDecision, at time.Time) notify.Notification {
	level := notify.LevelWarning
	var title string
	switch dec.Kind {
	case slotWarnRetention85:
		level = notify.LevelCritical
		title = fmt.Sprintf("Replication slot nearing eviction on sync %q — WAL retention at %.1f%%", streamID, dec.PercentUsed)
	case slotWarnRetention70:
		title = fmt.Sprintf("Replication slot under WAL retention pressure on sync %q — %.1f%% of max_slot_wal_keep_size", streamID, dec.PercentUsed)
	case slotWarnInactive:
		title = fmt.Sprintf("Replication slot inactive for %s on sync %q — is the consumer dead?", dec.InactiveFor.Round(time.Second), streamID)
	case slotWarnUnreserved:
		level = notify.LevelCritical
		title = fmt.Sprintf("Replication slot past retention cap on sync %q — invalidation at next checkpoint", streamID)
	case slotWarnLost:
		// Terminal (MED-D0-9): this page fires once — once DELIVERED
		// (M1.2: a dead sink retries per tick) — and latches; it is
		// the only one the operator will get for this slot's loss.
		level = notify.LevelCritical
		title = fmt.Sprintf("Replication slot LOST on sync %q — re-snapshot required", streamID)
	case slotWarnDropped:
		// Terminal (LOW-D0-17): same once-and-latch contract as lost.
		level = notify.LevelCritical
		title = fmt.Sprintf("Replication slot dropped mid-stream on sync %q — CDC cannot resume", streamID)
	}
	return notify.Notification{
		Level:    level,
		Category: notify.CategorySlotHealth,
		StreamID: streamID,
		Title:    title,
		Body: fmt.Sprintf(
			"%s [max_slot_wal_keep_size=%d bytes, wal_status=%q, lag=%d bytes]",
			slotHealthHint(snap, dec), snap.MaxKeepSizeBytes, snap.WALStatus, snap.LagBytes,
		),
		At: at,
	}
}

// notifySlotHealthCrossing delivers the roadmap-64a alert for one probe
// tick. No-op when no sink is configured (nil notifier) or the tick isn't
// an emitting one (rate-limit-suppressed repeat, clean, or cleared — a
// clear stays a slog INFO, not a page). Failure isolation (load-bearing):
// a notify error is logged at WARN and SWALLOWED, never propagated — the
// notification is advisory, and a dead sink must not disturb the probe
// loop or the sync it watches.
//
// Returns the built notification when a sink WAS configured but delivery
// failed (nil otherwise — delivered, no sink, or nothing to send). The
// probe loop parks a TERMINAL undelivered page for per-tick delivery
// retries (audit 2026-07-16 M1.2); non-terminal pages re-fire through
// the rate-limit window on their own, so callers ignore those.
func notifySlotHealthCrossing(ctx context.Context, notifier notify.Notifier, streamID string, snap ir.SlotHealth, dec slotWarningDecision) (undelivered *notify.Notification) {
	if notifier == nil || !dec.Emit {
		return nil
	}
	n := makeSlotHealthNotification(streamID, snap, dec, time.Now())
	if !deliverSlotHealthNotification(ctx, notifier, streamID, snap.SlotName, n) {
		return &n
	}
	return nil
}

// deliverSlotHealthNotification attempts one sink delivery, reporting
// whether it landed. A nil notifier reports true — with no sink there is
// nothing left to retry. The error handling is the shared slot-health
// failure-isolation contract: logged at WARN, swallowed, never
// propagated.
func deliverSlotHealthNotification(ctx context.Context, notifier notify.Notifier, streamID, slotName string, n notify.Notification) bool {
	if notifier == nil {
		return true
	}
	if err := notifier.Notify(ctx, n); err != nil {
		slog.WarnContext(
			ctx, "slot-health alert: notify failed (advisory only, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("slot", slotName),
			slog.String("error", err.Error()),
		)
		return false
	}
	return true
}

// makeSlotProbeFailureNotification maps a sustained probe outage
// (MED-D0-11 — [slotHealthProbeFailureEscalateAfter] consecutive
// failures) into the operator-facing warning page. Pure, mirroring
// [makeSlotHealthNotification]. Warning (not critical): the slot may be
// perfectly healthy — what's broken is sluice's ability to watch it.
func makeSlotProbeFailureNotification(streamID, slotName string, failures int, probeErr error, at time.Time) notify.Notification {
	return notify.Notification{
		Level:    notify.LevelWarning,
		Category: notify.CategorySlotHealth,
		StreamID: streamID,
		Title:    fmt.Sprintf("Slot-health probe failing on sync %q — slot alerts are blind", streamID),
		Body: fmt.Sprintf(
			"the slot-health probe for slot %q has failed %d consecutive times (last error: %s). While the probe is failing, retention-pressure and slot-invalidation alerts CANNOT fire — check that the probe connection's role still has access to pg_replication_slots and that the network path to the source is healthy.",
			slotName, failures, probeErr,
		),
		At: at,
	}
}

// notifySlotProbeFailure delivers the MED-D0-11 probe-outage page. Same
// contract as [notifySlotHealthCrossing]: no-op on a nil notifier, and
// failure-isolated — a notify error is logged at WARN and SWALLOWED
// (the probe loop rides through; the page is advisory).
func notifySlotProbeFailure(ctx context.Context, notifier notify.Notifier, streamID, slotName string, failures int, probeErr error) {
	if notifier == nil {
		return
	}
	n := makeSlotProbeFailureNotification(streamID, slotName, failures, probeErr, time.Now())
	if err := notifier.Notify(ctx, n); err != nil {
		slog.WarnContext(
			ctx, "slot-health alert: notify failed (advisory only, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("slot", slotName),
			slog.String("error", err.Error()),
		)
	}
}

// slotHealthNotifier resolves the sink set the probe loop pages, applying
// the opt-out gate: nil when SuppressSlotHealthNotify is set (the slog
// WARNs still fire — only the paging is disabled) or when no sink is
// configured. Reuses the metrics alerter's sink assembly
// ([buildMetricsNotifierFrom]) — the SINGLE definition of the sink set, so
// slot-health, schema-drift, and metrics alerts can never target different
// sinks. The test seam is honoured first so a test can capture the fired
// notification without a real sink.
func (s *Streamer) slotHealthNotifier() notify.Notifier {
	if s.SuppressSlotHealthNotify {
		return nil
	}
	if s.slotHealthNotifierForTest != nil {
		return s.slotHealthNotifierForTest
	}
	return buildMetricsNotifierFrom(s.NotifyWebhookURL, s.NotifySlackWebhookURL, s.NotifySMTP)
}
