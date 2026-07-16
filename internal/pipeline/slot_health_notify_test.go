// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Tests for the roadmap-64a slot-health notifications (ADR-0059
// implementation note): the decision→Notification mapping for EVERY
// condition kind (pin the class, not the representative), the
// fire/suppress/re-arm semantics against the real evaluator, the gating
// (suppress flag, no sink), and failure isolation (a dead sink is
// swallowed). The loop-level test drives [slotHealthProbeLoop] with the
// REAL evaluator + a fake reporter + a capturing sink — the stated
// substitute for forcing a genuine >=85% retention crossing on a
// testcontainer.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// pressureSnap builds a SlotHealth snapshot at the given lag/bound.
func pressureSnap(lag, keep int64) ir.SlotHealth {
	return ir.SlotHealth{
		SlotName:         "sluice_slot",
		Active:           true,
		LagBytes:         lag,
		MaxKeepSizeBytes: keep,
		WALStatus:        "extended",
	}
}

// TestMakeSlotHealthNotification pins the decision → Notification mapping
// for EVERY condition kind — 85% (critical), 70% (warning), inactivity
// (warning) — including the facts the page must carry: slot name, the
// concrete reading, lag bytes, the GUC bound, wal_status, and the
// remediation text (shared verbatim with the slog hint).
func TestMakeSlotHealthNotification(t *testing.T) {
	at := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		dec       slotWarningDecision
		wantLevel notify.Level
		wantTitle []string
		wantBody  []string
	}{
		{
			name:      "retention 85 → critical",
			dec:       slotWarningDecision{Kind: slotWarnRetention85, Emit: true, PercentUsed: 92.5},
			wantLevel: notify.LevelCritical,
			wantTitle: []string{"app-prod", "nearing eviction", "92.5%"},
			wantBody: []string{
				`slot "sluice_slot"`, "92.5%", "925 bytes", "intervene now",
				"raise max_slot_wal_keep_size", "re-snapshot is the only recovery",
				"drop it", "max_slot_wal_keep_size=1000 bytes", `wal_status="extended"`, "lag=925 bytes",
			},
		},
		{
			name:      "retention 70 → warning",
			dec:       slotWarningDecision{Kind: slotWarnRetention70, Emit: true, PercentUsed: 74.0},
			wantLevel: notify.LevelWarning,
			wantTitle: []string{"app-prod", "WAL retention pressure", "74.0%"},
			wantBody: []string{
				`slot "sluice_slot"`, "74.0%", "falling behind",
				"raise max_slot_wal_keep_size", `wal_status="extended"`,
			},
		},
		{
			name:      "inactive → warning with the is-the-consumer-dead framing",
			dec:       slotWarningDecision{Kind: slotWarnInactive, Emit: true, InactiveFor: 45 * time.Minute},
			wantLevel: notify.LevelWarning,
			wantTitle: []string{"app-prod", "inactive for 45m0s", "is the consumer dead?"},
			wantBody: []string{
				`slot "sluice_slot"`, "inactive for 45m0s", "no longer attached",
				"wal_sender_timeout", `wal_status="extended"`,
			},
		},
		{
			name:      "unreserved → critical with the next-checkpoint deadline",
			dec:       slotWarningDecision{Kind: slotWarnUnreserved, Emit: true},
			wantLevel: notify.LevelCritical,
			wantTitle: []string{"app-prod", "past retention cap", "next checkpoint"},
			wantBody: []string{
				`slot "sluice_slot"`, "unreserved", "NEXT CHECKPOINT",
				"raise max_slot_wal_keep_size",
			},
		},
		{
			name:      "lost → terminal critical naming the re-snapshot recovery",
			dec:       slotWarningDecision{Kind: slotWarnLost, Emit: true},
			wantLevel: notify.LevelCritical,
			wantTitle: []string{"app-prod", "LOST", "re-snapshot required"},
			wantBody: []string{
				`slot "sluice_slot"`, "INVALIDATED", "re-snapshot",
				"terminal", "will not repeat or clear",
			},
		},
		{
			name:      "dropped → terminal critical naming the mid-stream drop",
			dec:       slotWarningDecision{Kind: slotWarnDropped, Emit: true},
			wantLevel: notify.LevelCritical,
			wantTitle: []string{"app-prod", "dropped mid-stream", "CDC cannot resume"},
			wantBody: []string{
				`slot "sluice_slot"`, "no longer exists",
				"terminal", "will not repeat or clear",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := pressureSnap(925, 1000)
			n := makeSlotHealthNotification("app-prod", snap, tc.dec, at)
			if n.Level != tc.wantLevel {
				t.Errorf("Level = %q; want %q", n.Level, tc.wantLevel)
			}
			if n.Category != notify.CategorySlotHealth {
				t.Errorf("Category = %q; want slot-health", n.Category)
			}
			if n.StreamID != "app-prod" {
				t.Errorf("StreamID = %q; want app-prod", n.StreamID)
			}
			if !n.At.Equal(at) {
				t.Errorf("At = %v; want %v", n.At, at)
			}
			for _, want := range tc.wantTitle {
				if !strings.Contains(n.Title, want) {
					t.Errorf("Title %q missing %q", n.Title, want)
				}
			}
			for _, want := range tc.wantBody {
				if !strings.Contains(n.Body, want) {
					t.Errorf("Body %q missing %q", n.Body, want)
				}
			}
		})
	}
}

// notifyTick runs one simulated probe tick against the REAL evaluator +
// the notify hook, exactly in the loop's order (evaluate → notify →
// record), so the edge tests exercise the shipped mechanism rather than a
// re-implementation.
func notifyTick(t *testing.T, sink notify.Notifier, st *slotHealthState, snap ir.SlotHealth, now time.Time) {
	t.Helper()
	dec := evaluateSlotHealth(snap, st, DefaultSlotHealthThresholds(), now)
	notifySlotHealthCrossing(context.Background(), sink, "s1", snap, dec)
	recordSlotHealthEmission(st, dec, now)
}

// TestSlotHealthNotify_EdgeSemantics pins the roadmap-64a firing contract
// against the real evaluator: a crossing fires when entered, an unchanged
// in-condition repeat inside the rate-limit window is suppressed, a
// GENUINE clear (persisting past the LOW-D0-18 hysteresis tick) does not
// page, and a cleared-then-re-entered condition re-fires — even inside
// the original rate-limit window (the state-transition rule). A clear
// that does NOT persist (a boundary flap) no longer re-fires on
// re-entry; that's pinned by TestEvaluateSlotHealth_FlappingDamped.
func TestSlotHealthNotify_EdgeSemantics(t *testing.T) {
	captured := &capturingNotifier{}
	t0 := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	st := newSlotHealthState(t0)

	// Enter critical (90%) → fires.
	notifyTick(t, captured, st, pressureSnap(90, 100), t0)
	if captured.count() != 1 {
		t.Fatalf("crossing must fire: fires = %d; want 1", captured.count())
	}
	if got := captured.got[0]; got.Level != notify.LevelCritical || got.Category != notify.CategorySlotHealth {
		t.Fatalf("delivered notification = %+v; want critical slot-health", got)
	}

	// Same condition 1m later (inside the 5m window) → suppressed.
	notifyTick(t, captured, st, pressureSnap(90, 100), t0.Add(1*time.Minute))
	if captured.count() != 1 {
		t.Fatalf("in-window repeat must be suppressed: fires = %d; want 1", captured.count())
	}

	// Condition clears (10%) and PERSISTS one extra tick (the
	// hysteresis) → no page either way (the clear stays a slog INFO).
	notifyTick(t, captured, st, pressureSnap(10, 100), t0.Add(2*time.Minute))
	notifyTick(t, captured, st, pressureSnap(10, 100), t0.Add(150*time.Second))
	if captured.count() != 1 {
		t.Fatalf("clear must not page: fires = %d; want 1", captured.count())
	}

	// Re-enters at 3m — still inside the original 5m window — → re-fires
	// (cleared-then-re-entered is a state transition, not a repeat).
	notifyTick(t, captured, st, pressureSnap(90, 100), t0.Add(3*time.Minute))
	if captured.count() != 2 {
		t.Fatalf("cleared-then-re-entered must re-fire: fires = %d; want 2", captured.count())
	}
}

// TestSlotHealthNotify_EscalationFiresInsideWindow pins the 70→85
// escalation: the warning fires on entry and the critical fires on the
// escalation even inside the 5m rate-limit window (transitions always
// emit — the moment the operator most needs promptness).
func TestSlotHealthNotify_EscalationFiresInsideWindow(t *testing.T) {
	captured := &capturingNotifier{}
	t0 := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	st := newSlotHealthState(t0)

	notifyTick(t, captured, st, pressureSnap(75, 100), t0)
	notifyTick(t, captured, st, pressureSnap(90, 100), t0.Add(30*time.Second))
	if captured.count() != 2 {
		t.Fatalf("warn + escalation = %d fires; want 2", captured.count())
	}
	if captured.got[0].Level != notify.LevelWarning {
		t.Errorf("70%% crossing level = %q; want warning", captured.got[0].Level)
	}
	if captured.got[1].Level != notify.LevelCritical {
		t.Errorf("85%% escalation level = %q; want critical", captured.got[1].Level)
	}
}

// TestNotifySlotHealthCrossing_Gating pins the inert paths: a nil notifier
// (no sink / suppressed) makes no attempt and never panics; a
// non-emitting decision (suppressed repeat, clean, cleared) makes no
// attempt even with a live sink.
func TestNotifySlotHealthCrossing_Gating(t *testing.T) {
	t.Run("nil notifier → no attempt, no panic", func(_ *testing.T) {
		dec := slotWarningDecision{Kind: slotWarnRetention85, Emit: true, PercentUsed: 90}
		notifySlotHealthCrossing(context.Background(), nil, "s1", pressureSnap(90, 100), dec)
	})

	t.Run("non-emitting decision → no attempt", func(t *testing.T) {
		captured := &capturingNotifier{}
		for _, dec := range []slotWarningDecision{
			{Kind: slotWarnNone},
			{Kind: slotWarnRetention85, Emit: false}, // rate-limit-suppressed repeat
			{Cleared: true},
		} {
			notifySlotHealthCrossing(context.Background(), captured, "s1", pressureSnap(90, 100), dec)
		}
		if captured.count() != 0 {
			t.Fatalf("non-emitting decisions must not page: attempts = %d", captured.count())
		}
	})
}

// TestNotifySlotHealthCrossing_DeadSinkSwallowed pins failure isolation: a
// sink error is logged and swallowed — the hook has no error return and
// must not panic, so the probe loop (and the sync) rides through.
func TestNotifySlotHealthCrossing_DeadSinkSwallowed(t *testing.T) {
	captured := &capturingNotifier{failWith: errors.New("sink down")}
	dec := slotWarningDecision{Kind: slotWarnRetention85, Emit: true, PercentUsed: 90}
	notifySlotHealthCrossing(context.Background(), captured, "s1", pressureSnap(90, 100), dec)
	if captured.count() != 1 {
		t.Fatalf("dead sink must still be attempted once: attempts = %d", captured.count())
	}
}

// TestSlotHealthNotifier_Gating pins the streamer-side resolution: the
// suppress flag wins over everything (slog WARNs only), the test seam is
// honoured, and a zero-value streamer (no sinks) resolves to nil so the
// loop never attempts delivery.
func TestSlotHealthNotifier_Gating(t *testing.T) {
	t.Run("suppressed → nil even with a sink", func(t *testing.T) {
		s := &Streamer{SuppressSlotHealthNotify: true, slotHealthNotifierForTest: &capturingNotifier{}}
		if got := s.slotHealthNotifier(); got != nil {
			t.Fatalf("suppressed streamer resolved a notifier: %v", got)
		}
	})

	t.Run("test seam honoured", func(t *testing.T) {
		captured := &capturingNotifier{}
		s := &Streamer{slotHealthNotifierForTest: captured}
		if got := s.slotHealthNotifier(); got != notify.Notifier(captured) {
			t.Fatalf("test seam not honoured: %v", got)
		}
	})

	t.Run("no sink configured → nil (inert)", func(t *testing.T) {
		s := &Streamer{}
		if got := s.slotHealthNotifier(); got != nil {
			t.Fatalf("zero-value streamer must resolve nil notifier, got %v", got)
		}
	})
}

// TestSlotHealthProbeLoop_DeliversNotification drives the REAL loop + REAL
// evaluator with a fake reporter parked at 90% retention pressure and a
// capturing sink: the crossing pages exactly once (the 5m rate-limit
// window suppresses the subsequent ticks), critical + slot-health-shaped.
// This is the unit-layer substitute for forcing a genuine >=85% crossing
// on a live testcontainer.
func TestSlotHealthProbeLoop_DeliversNotification(t *testing.T) {
	r := &stubSlotHealthReporter{}
	r.setSnap(pressureSnap(90, 100), true)
	captured := &capturingNotifier{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		slotHealthProbeLoop(ctx, r, "sluice_slot", "stream-n", DefaultSlotHealthThresholds(), 20*time.Millisecond, captured)
		close(done)
	}()

	// Wait for the first crossing to page (bounded).
	deadline := time.After(2 * time.Second)
	for captured.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("no notification delivered within 2s of a >=85% crossing")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Let several more ticks elapse: the rate-limit window (5m) must hold
	// the sustained condition to the single page.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if got := captured.count(); got != 1 {
		t.Fatalf("sustained condition paged %d times inside the rate-limit window; want 1", got)
	}
	n := captured.got[0]
	if n.Level != notify.LevelCritical || n.Category != notify.CategorySlotHealth || n.StreamID != "stream-n" {
		t.Errorf("delivered notification = %+v; want critical slot-health for stream-n", n)
	}
	if !strings.Contains(n.Body, `slot "sluice_slot"`) {
		t.Errorf("Body %q should name the slot", n.Body)
	}
}

// TestMakeSlotProbeFailureNotification pins the MED-D0-11 mapping: a
// sustained probe outage pages a WARNING that says the net is blind,
// names the slot, the failure count, and the underlying error.
func TestMakeSlotProbeFailureNotification(t *testing.T) {
	at := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	n := makeSlotProbeFailureNotification("app-prod", "sluice_slot", 5, errors.New("permission denied for view pg_replication_slots"), at)
	if n.Level != notify.LevelWarning {
		t.Errorf("Level = %q; want warning (the slot may be healthy — the WATCHER is broken)", n.Level)
	}
	if n.Category != notify.CategorySlotHealth {
		t.Errorf("Category = %q; want slot-health", n.Category)
	}
	for _, want := range []string{"app-prod", "probe failing", "blind"} {
		if !strings.Contains(n.Title, want) {
			t.Errorf("Title %q missing %q", n.Title, want)
		}
	}
	for _, want := range []string{
		`slot "sluice_slot"`, "5 consecutive times",
		"permission denied for view pg_replication_slots",
		"CANNOT fire", "pg_replication_slots",
	} {
		if !strings.Contains(n.Body, want) {
			t.Errorf("Body %q missing %q", n.Body, want)
		}
	}
	if !n.At.Equal(at) {
		t.Errorf("At = %v; want %v", n.At, at)
	}
}

// TestSlotHealthProbeLoop_SustainedProbeFailureEscalates is the
// MED-D0-11 loop pin: probe failures are DEBUG-only until
// [slotHealthProbeFailureEscalateAfter] consecutive failures, then WARN
// + page exactly once per outage streak; a successful probe re-arms the
// escalation so a second outage pages again.
func TestSlotHealthProbeLoop_SustainedProbeFailureEscalates(t *testing.T) {
	r := &stubSlotHealthReporter{}
	r.setErr(errors.New("role revoked"))
	captured := &capturingNotifier{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		slotHealthProbeLoop(ctx, r, "sluice_slot", "stream-f", DefaultSlotHealthThresholds(), 5*time.Millisecond, captured)
		close(done)
	}()

	waitFor := func(want int, what string) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for captured.count() < want {
			select {
			case <-deadline:
				t.Fatalf("%s: pages = %d; want %d within 5s", what, captured.count(), want)
			case <-time.After(5 * time.Millisecond):
			}
		}
	}

	// First outage: exactly one page after N consecutive failures.
	waitFor(1, "first outage")
	n := captured.got[0]
	if n.Level != notify.LevelWarning || n.Category != notify.CategorySlotHealth {
		t.Errorf("outage page = %+v; want warning slot-health", n)
	}
	// Many more failing ticks: still one page (once per streak).
	time.Sleep(100 * time.Millisecond)
	if got := captured.count(); got != 1 {
		t.Fatalf("sustained outage paged %d times; want 1 per streak", got)
	}

	// Recovery, then a second outage: the escalation re-arms and pages
	// again.
	r.setSnap(pressureSnap(10, 100), true)
	time.Sleep(100 * time.Millisecond) // several successful probes reset the counter
	r.setErr(errors.New("role revoked again"))
	waitFor(2, "second outage")

	cancel()
	<-done
}

// TestSlotHealthProbeLoop_DroppedSlotPagesTerminalOnce is the LOW-D0-17
// loop pin: a slot that vanishes (ok=false) while a condition is
// outstanding pages terminal CRITICAL exactly once, and stays silent
// afterwards — even if the reporter later shows a healthy slot again.
func TestSlotHealthProbeLoop_DroppedSlotPagesTerminalOnce(t *testing.T) {
	r := &stubSlotHealthReporter{}
	r.setSnap(pressureSnap(90, 100), true) // critical retention condition
	captured := &capturingNotifier{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		slotHealthProbeLoop(ctx, r, "sluice_slot", "stream-d", DefaultSlotHealthThresholds(), 5*time.Millisecond, captured)
		close(done)
	}()

	waitFor := func(want int, what string) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for captured.count() < want {
			select {
			case <-deadline:
				t.Fatalf("%s: pages = %d; want %d within 5s", what, captured.count(), want)
			case <-time.After(5 * time.Millisecond):
			}
		}
	}

	// Page 1: the 85% crossing.
	waitFor(1, "retention crossing")

	// The slot row vanishes mid-stream → page 2, terminal.
	r.setSnap(ir.SlotHealth{}, false)
	waitFor(2, "dropped-slot terminal")
	n := captured.got[1]
	if n.Level != notify.LevelCritical {
		t.Errorf("dropped page level = %q; want critical", n.Level)
	}
	if !strings.Contains(n.Title, "dropped mid-stream") {
		t.Errorf("dropped page title = %q; want the dropped-mid-stream framing", n.Title)
	}

	// Latched: further ok=false ticks — and even a reappearing healthy
	// slot — page nothing.
	time.Sleep(60 * time.Millisecond)
	r.setSnap(pressureSnap(0, 100), true)
	time.Sleep(60 * time.Millisecond)
	if got := captured.count(); got != 2 {
		t.Fatalf("terminal latch violated: pages = %d; want exactly 2", got)
	}

	cancel()
	<-done
}
