// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// fakeNotifier records every Notification and can be set to error, to pin
// the failure-isolation path.
type fakeNotifier struct {
	mu  sync.Mutex
	err error
	got []notify.Notification
}

func (f *fakeNotifier) Notify(_ context.Context, n notify.Notification) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, n)
	return f.err
}
func (f *fakeNotifier) Name() string { return "fake" }

func (f *fakeNotifier) calls() []notify.Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]notify.Notification(nil), f.got...)
}

// oneRule builds a single-metric rule set for the deterministic evaluator
// tests (storage_util, critical).
func storageRule(threshold float64) []metricsNotifyRule {
	return []metricsNotifyRule{{
		metric:    notifyStorageUtil,
		threshold: threshold,
		level:     notify.LevelCritical,
		title:     "target storage approaching capacity",
		read: func(snap ir.TargetHealthSnapshot) (float64, bool) {
			return snap.StorageUtil, snap.StorageKnown
		},
	}}
}

func storageSnap(util float64) ir.TargetHealthSnapshot {
	return ir.TargetHealthSnapshot{StorageUtil: util, StorageKnown: true}
}

func TestEvalMetricsNotifyTick_EdgeTriggerFiresOnceWhileBreached(t *testing.T) {
	rules := storageRule(0.90)
	st := newMetricsNotifyState()
	cooldown := 15 * time.Minute
	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)

	// Below threshold — no fire.
	if n := evalMetricsNotifyTick(storageSnap(0.50), rules, st, cooldown, now); len(n) != 0 {
		t.Fatalf("below threshold should not fire, got %d", len(n))
	}
	// Cross the threshold — fires once (rising edge).
	n := evalMetricsNotifyTick(storageSnap(0.91), rules, st, cooldown, now.Add(time.Minute))
	if len(n) != 1 {
		t.Fatalf("rising edge should fire once, got %d", len(n))
	}
	if n[0].Metric != "storage_util" || n[0].Value != 0.91 || n[0].Threshold != 0.90 || n[0].Level != notify.LevelCritical {
		t.Errorf("notification fields wrong: %+v", n[0])
	}
	// Still breached, within cooldown — does NOT fire again.
	if n := evalMetricsNotifyTick(storageSnap(0.93), rules, st, cooldown, now.Add(2*time.Minute)); len(n) != 0 {
		t.Fatalf("breached-within-cooldown must not re-fire, got %d", len(n))
	}
}

func TestEvalMetricsNotifyTick_CooldownRefire(t *testing.T) {
	rules := storageRule(0.90)
	st := newMetricsNotifyState()
	cooldown := 15 * time.Minute
	base := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)

	// Fire on the edge.
	if n := evalMetricsNotifyTick(storageSnap(0.91), rules, st, cooldown, base); len(n) != 1 {
		t.Fatalf("edge fire expected, got %d", len(n))
	}
	// Just before the cooldown elapses — no re-fire.
	if n := evalMetricsNotifyTick(storageSnap(0.92), rules, st, cooldown, base.Add(cooldown-time.Second)); len(n) != 0 {
		t.Fatalf("pre-cooldown must not re-fire, got %d", len(n))
	}
	// At/after the cooldown — re-fires.
	if n := evalMetricsNotifyTick(storageSnap(0.92), rules, st, cooldown, base.Add(cooldown)); len(n) != 1 {
		t.Fatalf("post-cooldown should re-fire once, got %d", len(n))
	}
}

func TestEvalMetricsNotifyTick_ReArmAfterRecoveryWithHysteresis(t *testing.T) {
	rules := storageRule(0.90)
	st := newMetricsNotifyState()
	cooldown := 15 * time.Minute
	base := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)

	// Fire.
	evalMetricsNotifyTick(storageSnap(0.95), rules, st, cooldown, base)
	// Drop just below the threshold but still inside the hysteresis dead-band
	// (>= 0.90*0.97 = 0.873): must NOT re-arm (so it won't re-fire on a
	// re-cross).
	if n := evalMetricsNotifyTick(storageSnap(0.88), rules, st, cooldown, base.Add(time.Minute)); len(n) != 0 {
		t.Fatalf("dead-band dip should not fire, got %d", len(n))
	}
	if !st.rules[notifyStorageUtil].fired {
		t.Fatal("dead-band dip must NOT re-arm (latch should stay fired)")
	}
	// Re-cross while still latched — no fire (proves it didn't re-arm).
	if n := evalMetricsNotifyTick(storageSnap(0.92), rules, st, cooldown, base.Add(2*time.Minute)); len(n) != 0 {
		t.Fatalf("re-cross without re-arm must not fire, got %d", len(n))
	}
	// Genuine recovery below the hysteresis floor — re-arms.
	if n := evalMetricsNotifyTick(storageSnap(0.50), rules, st, cooldown, base.Add(3*time.Minute)); len(n) != 0 {
		t.Fatalf("recovery tick itself should not fire, got %d", len(n))
	}
	if st.rules[notifyStorageUtil].fired {
		t.Fatal("recovery below hysteresis floor must re-arm")
	}
	// Now a fresh crossing fires again.
	if n := evalMetricsNotifyTick(storageSnap(0.95), rules, st, cooldown, base.Add(4*time.Minute)); len(n) != 1 {
		t.Fatalf("post-recovery crossing should fire, got %d", len(n))
	}
}

func TestEvalMetricsNotifyTick_UnobservedMetricSkipped(t *testing.T) {
	rules := storageRule(0.90)
	st := newMetricsNotifyState()
	cooldown := 15 * time.Minute
	now := time.Now()

	// Storage unknown — neither fires nor re-arms even though value >= thresh.
	snap := ir.TargetHealthSnapshot{StorageUtil: 0.99, StorageKnown: false}
	if n := evalMetricsNotifyTick(snap, rules, st, cooldown, now); len(n) != 0 {
		t.Fatalf("unobserved metric must be skipped, got %d", len(n))
	}
	if _, ok := st.rules[notifyStorageUtil]; ok {
		t.Fatal("unobserved metric should not create latch state")
	}
}

func TestEvalMetricsNotifyTick_RateOfChangeRule(t *testing.T) {
	// Growth rule: storage util climbing >= 0.02/min.
	rules := []metricsNotifyRule{{
		metric:    notifyStorageGrowth,
		threshold: 0.02,
		level:     notify.LevelCritical,
		title:     "target storage climbing fast",
		read:      nil, // derived
	}}
	st := newMetricsNotifyState()
	cooldown := 15 * time.Minute
	base := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)

	// First observation: no prior point — cannot compute a rate, no fire.
	s0 := ir.TargetHealthSnapshot{StorageUtil: 0.50, StorageKnown: true, SampledAt: base}
	if n := evalMetricsNotifyTick(s0, rules, st, cooldown, base); len(n) != 0 {
		t.Fatalf("first point cannot fire a rate rule, got %d", len(n))
	}
	// Second observation 1 min later, +0.05 util ⇒ 0.05/min >= 0.02 ⇒ fire.
	s1 := ir.TargetHealthSnapshot{StorageUtil: 0.55, StorageKnown: true, SampledAt: base.Add(time.Minute)}
	n := evalMetricsNotifyTick(s1, rules, st, cooldown, base.Add(time.Minute))
	if len(n) != 1 {
		t.Fatalf("0.05/min growth should fire, got %d", len(n))
	}
	if n[0].Value < 0.049 || n[0].Value > 0.051 {
		t.Errorf("derived rate wrong: %v", n[0].Value)
	}
	// Slow growth (+0.005 over 1 min = 0.005/min < 0.02) AND recovery below
	// hysteresis re-arms; here it stays below ⇒ re-arm, no fire.
	s2 := ir.TargetHealthSnapshot{StorageUtil: 0.555, StorageKnown: true, SampledAt: base.Add(2 * time.Minute)}
	if n := evalMetricsNotifyTick(s2, rules, st, cooldown, base.Add(2*time.Minute)); len(n) != 0 {
		t.Fatalf("slow growth should not fire, got %d", len(n))
	}
}

func TestEvalMetricsNotifyTick_MultipleRulesIndependent(t *testing.T) {
	rules := []metricsNotifyRule{
		{metric: notifyStorageUtil, threshold: 0.90, level: notify.LevelCritical, title: "storage", read: func(s ir.TargetHealthSnapshot) (float64, bool) { return s.StorageUtil, s.StorageKnown }},
		{metric: notifyCPUUtil, threshold: 0.80, level: notify.LevelWarning, title: "cpu", read: func(s ir.TargetHealthSnapshot) (float64, bool) { return s.CPUUtil, s.CPUKnown }},
	}
	st := newMetricsNotifyState()
	cooldown := 15 * time.Minute
	now := time.Now()

	// Only CPU breached.
	snap := ir.TargetHealthSnapshot{StorageUtil: 0.20, StorageKnown: true, CPUUtil: 0.85, CPUKnown: true}
	n := evalMetricsNotifyTick(snap, rules, st, cooldown, now)
	if len(n) != 1 || n[0].Metric != "cpu_util" {
		t.Fatalf("only CPU should fire, got %+v", n)
	}
	// Now storage breaches too — independent rising edge fires for storage
	// only (CPU still latched, within cooldown).
	snap2 := ir.TargetHealthSnapshot{StorageUtil: 0.95, StorageKnown: true, CPUUtil: 0.86, CPUKnown: true}
	n2 := evalMetricsNotifyTick(snap2, rules, st, cooldown, now.Add(time.Minute))
	if len(n2) != 1 || n2[0].Metric != "storage_util" {
		t.Fatalf("only storage should newly fire, got %+v", n2)
	}
}

func TestRunMetricsNotifyTick_StaleSnapshotNoFire(t *testing.T) {
	rules := storageRule(0.90)
	st := newMetricsNotifyState()
	nf := &fakeNotifier{}
	// Snapshot is breaching but STALE (SampledAt far in the past).
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		StorageUtil: 0.99, StorageKnown: true, SampledAt: time.Now().Add(-1 * time.Hour),
	}}
	runMetricsNotifyTick(context.Background(), newDiscardLogger(), prov, nf, "s1", rules, st, time.Minute, time.Now)
	if len(nf.calls()) != 0 {
		t.Fatalf("stale snapshot must not fire, got %d", len(nf.calls()))
	}
}

func TestRunMetricsNotifyTick_FailureIsolation(t *testing.T) {
	rules := storageRule(0.90)
	st := newMetricsNotifyState()
	nf := &fakeNotifier{err: errors.New("slack 500")}
	now := time.Now()
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		StorageUtil: 0.99, StorageKnown: true, SampledAt: now,
	}}
	// A notifier error must be swallowed (no panic, no propagation): the call
	// returns normally and the StreamID was stamped on the attempted send.
	runMetricsNotifyTick(context.Background(), newDiscardLogger(), prov, nf, "s1", rules, st, time.Minute, func() time.Time { return now })
	calls := nf.calls()
	if len(calls) != 1 {
		t.Fatalf("the fire was attempted once despite the error, got %d", len(calls))
	}
	if calls[0].StreamID != "s1" {
		t.Errorf("StreamID not stamped: %q", calls[0].StreamID)
	}
}

func TestStartTargetMetricsNotifier_NoOpPaths(t *testing.T) {
	freshSnap := ir.TargetHealthSnapshot{StorageUtil: 0.99, StorageKnown: true, SampledAt: time.Now()}
	prov := &fakeTelemetry{ok: true, snap: freshSnap}
	nf := &fakeNotifier{}

	t.Run("nil provider", func(t *testing.T) {
		s := &Streamer{NotifyStorageUtil: 0.90}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.startTargetMetricsNotifier(ctx, "s1", nil, nil, nf)
		if len(nf.calls()) != 0 {
			t.Errorf("nil provider must be a no-op")
		}
	})

	t.Run("nil notifier", func(_ *testing.T) {
		s := &Streamer{NotifyStorageUtil: 0.90}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Must not panic; a nil notifier ⇒ no goroutine.
		s.startTargetMetricsNotifier(ctx, "s1", nil, prov, nil)
	})

	t.Run("no active rule", func(t *testing.T) {
		s := &Streamer{} // all thresholds zero
		local := &fakeNotifier{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.startTargetMetricsNotifier(ctx, "s1", nil, prov, local)
		if len(local.calls()) != 0 {
			t.Errorf("no active rule must be a no-op")
		}
	})
}

func TestBuildMetricsNotifier_NilWhenNoSink(t *testing.T) {
	if got := (&Streamer{}).buildMetricsNotifier(); got != nil {
		t.Errorf("no sink URL should yield a true-nil notifier, got %T", got)
	}
	s := &Streamer{NotifyWebhookURL: "http://example/hook"}
	if got := s.buildMetricsNotifier(); got == nil {
		t.Error("a configured webhook should yield a notifier")
	}
}

func TestBuildMetricsNotifyRules_InertWhenZero(t *testing.T) {
	if got := (&Streamer{}).buildMetricsNotifyRules(); got != nil {
		t.Errorf("all-zero thresholds should yield no rules, got %d", len(got))
	}
	s := &Streamer{NotifyStorageUtil: 0.9, NotifyCPUUtil: 0.8}
	if got := s.buildMetricsNotifyRules(); len(got) != 2 {
		t.Errorf("two set thresholds should yield two rules, got %d", len(got))
	}
}
