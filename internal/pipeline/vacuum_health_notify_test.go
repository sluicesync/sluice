// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// fakeVacuumReporter returns a scripted probe result, to drive the tick
// deterministically.
type fakeVacuumReporter struct {
	health ir.VacuumHealth
	ok     bool
	err    error
}

func (f *fakeVacuumReporter) TargetVacuumHealth(context.Context) (ir.VacuumHealth, bool, error) {
	return f.health, f.ok, f.err
}

// stubApplier is a ChangeApplier that implements nothing beyond the
// embedded (nil) interface — the gating tests only type-assert on it, so
// any method call would rightly panic (the stubEngine discipline).
type stubApplier struct{ ir.ChangeApplier }

func vacuumState() map[notifyMetric]*metricsNotifyRuleState {
	return map[notifyMetric]*metricsNotifyRuleState{
		notifyDeadTupleRatio: {},
		notifyXIDAge:         {},
	}
}

func bloatedHealth(ratio float64, xidAge int64) ir.VacuumHealth {
	return ir.VacuumHealth{
		WorstTable:      "public.orders",
		DeadTuples:      50_000,
		LiveTuples:      50_000,
		DeadTupleRatio:  ratio,
		AutovacuumCount: 3,
		XIDAge:          xidAge,
		Datname:         "appdb",
	}
}

func runVacuumTick(rep *fakeVacuumReporter, nf *fakeNotifier, deadThresh, xidThresh float64, state map[notifyMetric]*metricsNotifyRuleState, at time.Time) {
	runVacuumHealthNotifyTick(
		context.Background(), slog.Default(), rep, nf, "s1",
		deadThresh, xidThresh, state, 15*time.Minute, func() time.Time { return at },
	)
}

func TestVacuumHealthNotifyTick_FiresBothRulesOnBreach(t *testing.T) {
	rep := &fakeVacuumReporter{health: bloatedHealth(0.5, 1_200_000_000), ok: true}
	nf := &fakeNotifier{}
	state := vacuumState()
	now := time.Now()

	runVacuumTick(rep, nf, 0.3, 1_000_000_000, state, now)

	got := nf.calls()
	if len(got) != 2 {
		t.Fatalf("notifications = %d, want 2 (both rules breached)", len(got))
	}
	dead, xid := got[0], got[1]
	if dead.Metric != string(notifyDeadTupleRatio) || xid.Metric != string(notifyXIDAge) {
		t.Fatalf("metrics = %q, %q; want dead_tuple_ratio, xid_age", dead.Metric, xid.Metric)
	}
	if dead.Level != notify.LevelWarning {
		t.Errorf("dead-tuple level = %v, want warning", dead.Level)
	}
	if xid.Level != notify.LevelCritical {
		t.Errorf("xid-age level = %v, want critical", xid.Level)
	}
	if dead.StreamID != "s1" || xid.StreamID != "s1" {
		t.Errorf("StreamID not set: %q / %q", dead.StreamID, xid.StreamID)
	}
	// The body must answer the operator's first question in the page itself.
	for _, want := range []string{"public.orders", "50000 dead", "never", "3 completed runs"} {
		if !strings.Contains(dead.Body, want) {
			t.Errorf("dead-tuple body missing %q: %s", want, dead.Body)
		}
	}
	if !strings.Contains(xid.Body, `"appdb"`) {
		t.Errorf("xid-age body missing database name: %s", xid.Body)
	}
}

func TestVacuumHealthNotifyTick_EdgeTriggerAndRearm(t *testing.T) {
	rep := &fakeVacuumReporter{health: bloatedHealth(0.5, 0), ok: true}
	nf := &fakeNotifier{}
	state := vacuumState()
	now := time.Now()

	runVacuumTick(rep, nf, 0.3, 0, state, now)
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(time.Minute)) // still breached, inside cooldown
	if n := len(nf.calls()); n != 1 {
		t.Fatalf("notifications after sustained breach inside cooldown = %d, want 1 (edge-trigger)", n)
	}

	// Cooldown elapsed while still breached ⇒ one reminder.
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(16*time.Minute))
	if n := len(nf.calls()); n != 2 {
		t.Fatalf("notifications after cooldown = %d, want 2 (re-fire reminder)", n)
	}

	// Recovery well below the hysteresis line re-arms; the next breach fires.
	rep.health = bloatedHealth(0.1, 0)
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(17*time.Minute))
	rep.health = bloatedHealth(0.5, 0)
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(18*time.Minute))
	if n := len(nf.calls()); n != 3 {
		t.Fatalf("notifications after recover+re-breach = %d, want 3 (re-armed)", n)
	}
}

func TestVacuumHealthNotifyTick_HealthyZeroReadingRearms(t *testing.T) {
	// A target with NO table above the noise floor reports ratio 0 with
	// ok=true — a real observation that must re-arm a fired rule (this is
	// the "healthy is not unobserved" half of the honesty contract).
	rep := &fakeVacuumReporter{health: bloatedHealth(0.5, 0), ok: true}
	nf := &fakeNotifier{}
	state := vacuumState()
	now := time.Now()

	runVacuumTick(rep, nf, 0.3, 0, state, now)
	rep.health = ir.VacuumHealth{XIDAge: 100, Datname: "appdb"} // healthy: nothing above the floor
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(time.Minute))
	rep.health = bloatedHealth(0.5, 0)
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(2*time.Minute))
	if n := len(nf.calls()); n != 2 {
		t.Fatalf("notifications = %d, want 2 (healthy reading re-armed the latch)", n)
	}
}

func TestVacuumHealthNotifyTick_ProbeFailureSkipsWithoutRearm(t *testing.T) {
	rep := &fakeVacuumReporter{health: bloatedHealth(0.5, 0), ok: true}
	nf := &fakeNotifier{}
	state := vacuumState()
	now := time.Now()

	runVacuumTick(rep, nf, 0.3, 0, state, now)

	// A probe error, then an ok=false reading: neither fires NOR re-arms —
	// absence of signal is not "recovered".
	rep.err = errors.New("permission denied for view pg_stat_user_tables")
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(time.Minute))
	rep.err = nil
	rep.ok = false
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(2*time.Minute))

	// Back to breached inside the cooldown: still latched, so no new fire.
	rep.ok = true
	runVacuumTick(rep, nf, 0.3, 0, state, now.Add(3*time.Minute))
	if n := len(nf.calls()); n != 1 {
		t.Fatalf("notifications = %d, want 1 (skipped ticks neither fired nor re-armed)", n)
	}
}

func TestVacuumHealthNotifyTick_UnsetThresholdIsInert(t *testing.T) {
	rep := &fakeVacuumReporter{health: bloatedHealth(0.99, 2_000_000_000), ok: true}
	nf := &fakeNotifier{}
	state := vacuumState()

	// Only the xid rule is configured; the (massively breached) dead-tuple
	// rule must stay silent.
	runVacuumTick(rep, nf, 0, 1_000_000_000, state, time.Now())
	got := nf.calls()
	if len(got) != 1 || got[0].Metric != string(notifyXIDAge) {
		t.Fatalf("got %d notifications (first metric %v), want exactly the xid_age fire", len(got), func() string {
			if len(got) == 0 {
				return "<none>"
			}
			return got[0].Metric
		}())
	}
}

func TestVacuumHealthNotifyTick_SinkErrorIsSwallowed(t *testing.T) {
	rep := &fakeVacuumReporter{health: bloatedHealth(0.5, 0), ok: true}
	nf := &fakeNotifier{err: errors.New("webhook 500")}
	state := vacuumState()

	// Must not panic or propagate; the latch still advances (the fire was
	// decided; delivery failed).
	runVacuumTick(rep, nf, 0.3, 0, state, time.Now())
	if n := len(nf.calls()); n != 1 {
		t.Fatalf("delivery attempts = %d, want 1", n)
	}
}

func TestStartVacuumHealthNotifier_NonReporterApplierIsInert(t *testing.T) {
	// A non-Postgres applier (no TargetVacuumHealthReporter) with the rule
	// configured: WARN-once path, no goroutine, no panic.
	s := &Streamer{
		NotifyDeadTupleRatio: 0.3,
		NotifyWebhookURL:     "http://localhost:1", // sink configured so gating reaches the type-assert
	}
	s.startVacuumHealthNotifier(t.Context(), "s1", stubApplier{})
}

func TestStartVacuumHealthNotifier_NoThresholdNoSinkIsNoop(t *testing.T) {
	// No thresholds ⇒ no-op regardless of sink.
	(&Streamer{NotifyWebhookURL: "http://localhost:1"}).startVacuumHealthNotifier(t.Context(), "s1", stubApplier{})
	// Threshold but no sink ⇒ no-op.
	(&Streamer{NotifyXIDAge: 1}).startVacuumHealthNotifier(t.Context(), "s1", stubApplier{})
}

func TestHumanizeSince(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		at   time.Time
		want string
	}{
		{time.Time{}, "never"},
		{now.Add(-30 * time.Second), "under a minute ago"},
		{now.Add(-45 * time.Minute), "45m ago"},
		{now.Add(-30 * time.Hour), "30h ago"},
		{now.Add(-72 * time.Hour), "3d ago"},
		{now.Add(time.Hour), "under a minute ago"}, // clock skew clamps to 0
	}
	for _, c := range cases {
		if got := humanizeSince(c.at, now); got != c.want {
			t.Errorf("humanizeSince(%v) = %q, want %q", c.at, got, c.want)
		}
	}
}
