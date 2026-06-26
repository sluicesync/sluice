// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/notify"
)

// TestSyncLagTracker_UnknownUntilObserved pins the *Known honesty contract:
// before any source-timestamped change is applied the reading is "unknown"
// (omitted, never 0).
func TestSyncLagTracker_UnknownUntilObserved(t *testing.T) {
	tr := newSyncLagTracker()
	if _, known := tr.SyncLagSeconds(time.Now()); known {
		t.Fatal("fresh tracker reported known; want unknown until first observation")
	}
}

// TestSyncLagTracker_FrozenLagDoesNotAge is the core idle-honesty pin: a lag
// computed at observe time stays put between changes (it does NOT climb with
// wall-clock), so a caught-up stream that applied its last change promptly
// keeps reading ~0 rather than a growing number.
func TestSyncLagTracker_FrozenLagDoesNotAge(t *testing.T) {
	tr := newSyncLagTracker()
	base := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	commit := base.Add(-3 * time.Second) // applied 3s behind source
	tr.observe(commit, base)

	got, known := tr.SyncLagSeconds(base)
	if !known {
		t.Fatal("known=false after observe; want known")
	}
	if got < 2.9 || got > 3.1 {
		t.Fatalf("lag = %v; want ~3s", got)
	}
	// 10s later, still inside the caught-up window, with NO new change: the
	// frozen lag must not have aged to ~13s.
	got2, _ := tr.SyncLagSeconds(base.Add(10 * time.Second))
	if got2 < 2.9 || got2 > 3.1 {
		t.Fatalf("lag aged to %v after 10s idle; frozen lag must stay ~3s", got2)
	}
}

// TestSyncLagTracker_CaughtUpAfterIdle pins that a SUSTAINED absence of
// applied work retires a stale non-zero reading to 0 (caught up) — the #1
// false-alarm class the metric must avoid.
func TestSyncLagTracker_CaughtUpAfterIdle(t *testing.T) {
	tr := newSyncLagTracker()
	base := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	tr.observe(base.Add(-30*time.Second), base) // 30s behind when last applied

	// Past the caught-up window with nothing flowing ⇒ 0, still known.
	got, known := tr.SyncLagSeconds(base.Add(syncLagCaughtUpAfter + time.Second))
	if !known {
		t.Fatal("known=false past idle window; want known (caught up, not unknown)")
	}
	if got != 0 {
		t.Fatalf("lag = %v past idle window; want 0 (caught up)", got)
	}
}

// TestSyncLagTracker_ZeroCommitIgnored pins that a change carrying no source
// commit time contributes no signal (the engine/path didn't supply one).
func TestSyncLagTracker_ZeroCommitIgnored(t *testing.T) {
	tr := newSyncLagTracker()
	tr.observe(time.Time{}, time.Now())
	if _, known := tr.SyncLagSeconds(time.Now()); known {
		t.Fatal("zero-commit observe registered a reading; want still unknown")
	}
}

// TestSyncLagTracker_ClockSkewFloor pins that a future-dated source commit
// (clock skew) floors at 0 rather than reporting a negative "behind".
func TestSyncLagTracker_ClockSkewFloor(t *testing.T) {
	tr := newSyncLagTracker()
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	tr.observe(now.Add(5*time.Second), now) // commit "in the future"
	got, known := tr.SyncLagSeconds(now)
	if !known || got != 0 {
		t.Fatalf("lag = %v known=%v; want 0/known (clock-skew floor)", got, known)
	}
}

// TestSyncLagTracker_MultiShardSlowestReflected pins the multi-shard gotcha:
// a merged VStream interleaves shards, and the metric must surface the SLOWEST
// shard's lag as its older-commit-time events flow. Observing a fast shard's
// recent change then a slow shard's old change reflects the slow shard.
func TestSyncLagTracker_MultiShardSlowestReflected(t *testing.T) {
	tr := newSyncLagTracker()
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	// Fast shard: event committed ~0s ago.
	tr.observe(now, now)
	if got, _ := tr.SyncLagSeconds(now); got > 0.1 {
		t.Fatalf("after fast-shard event lag = %v; want ~0", got)
	}
	// Slow shard's (older) event flows next on the merged channel.
	tr.observe(now.Add(-45*time.Second), now)
	if got, _ := tr.SyncLagSeconds(now); got < 44 || got > 46 {
		t.Fatalf("after slow-shard event lag = %v; want ~45s (slowest shard reflected)", got)
	}
}

// TestObserveSyncLagChanges_PassThroughAndFeeds pins the interceptor: every
// change is forwarded unchanged AND its commit time feeds the tracker.
func TestObserveSyncLagChanges_PassThroughAndFeeds(t *testing.T) {
	tr := newSyncLagTracker()
	in := make(chan ir.Change, 2)
	commit := time.Now().Add(-7 * time.Second)
	in <- ir.Insert{Schema: "s", Table: "t", CommitTime: commit}
	in <- ir.TxCommit{} // zero commit — must not clobber the reading
	close(in)

	out := observeSyncLagChanges(context.Background(), in, tr)
	var n int
	for range out {
		n++
	}
	if n != 2 {
		t.Fatalf("forwarded %d changes; want 2 (pass-through)", n)
	}
	got, known := tr.SyncLagSeconds(time.Now())
	if !known || got < 6.5 || got > 7.5 {
		t.Fatalf("tracker lag = %v known=%v; want ~7s/known", got, known)
	}
}

// TestEvalThresholdAlert_EdgeCooldownHysteresis pins the shared firing
// decision used by both the target-metrics rules and the sync-lag alerter.
func TestEvalThresholdAlert_EdgeCooldownHysteresis(t *testing.T) {
	st := &metricsNotifyRuleState{}
	const threshold = 10.0
	cooldown := 15 * time.Minute
	t0 := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)

	// Rising edge fires once.
	if !evalThresholdAlert(st, 12, threshold, cooldown, t0) {
		t.Fatal("rising edge did not fire")
	}
	// Still breached within cooldown: hold.
	if evalThresholdAlert(st, 12, threshold, cooldown, t0.Add(time.Minute)) {
		t.Fatal("re-fired within cooldown; want hold")
	}
	// Still breached past cooldown: re-fire.
	if !evalThresholdAlert(st, 12, threshold, cooldown, t0.Add(16*time.Minute)) {
		t.Fatal("did not re-fire past cooldown")
	}
	// Dip into the hysteresis dead-band (below threshold but above
	// threshold*0.97 = 9.7): stays latched, no re-arm.
	if evalThresholdAlert(st, 9.8, threshold, cooldown, t0.Add(17*time.Minute)) {
		t.Fatal("fired inside hysteresis dead-band")
	}
	// Recover below the hysteresis floor: re-arm (no fire).
	if evalThresholdAlert(st, 9.0, threshold, cooldown, t0.Add(18*time.Minute)) {
		t.Fatal("fired on recovery")
	}
	// New rising edge fires again now that we re-armed.
	if !evalThresholdAlert(st, 12, threshold, cooldown, t0.Add(19*time.Minute)) {
		t.Fatal("did not fire on second rising edge after re-arm")
	}
}

// fixedSyncLag is a syncLagSource returning a constant (value, known).
type fixedSyncLag struct {
	value float64
	known bool
}

func (f fixedSyncLag) SyncLagSeconds(time.Time) (float64, bool) { return f.value, f.known }

// TestRunSyncLagNotifyTick_FiresAtThreshold pins that a breached reading
// fires exactly one critical notification carrying the sync-lag metric name.
func TestRunSyncLagNotifyTick_FiresAtThreshold(t *testing.T) {
	fn := &fakeNotifier{}
	st := &metricsNotifyRuleState{}
	now := func() time.Time { return time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC) }
	runSyncLagNotifyTick(context.Background(), newDiscardLogger(), fixedSyncLag{value: 90, known: true}, fn, "s1", 60, st, 15*time.Minute, now)

	got := fn.calls()
	if len(got) != 1 {
		t.Fatalf("fired %d notifications; want 1", len(got))
	}
	n := got[0]
	if n.Metric != string(notifySyncLag) {
		t.Errorf("Metric = %q; want %q", n.Metric, notifySyncLag)
	}
	if n.Level != notify.LevelCritical {
		t.Errorf("Level = %q; want critical", n.Level)
	}
	if n.StreamID != "s1" {
		t.Errorf("StreamID = %q; want s1", n.StreamID)
	}
	if n.Value != 90 || n.Threshold != 60 {
		t.Errorf("Value/Threshold = %v/%v; want 90/60", n.Value, n.Threshold)
	}
}

// TestRunSyncLagNotifyTick_UnknownSkipped pins the *Known honesty contract on
// the alerter side: an unknown reading neither fires nor re-arms.
func TestRunSyncLagNotifyTick_UnknownSkipped(t *testing.T) {
	fn := &fakeNotifier{}
	st := &metricsNotifyRuleState{fired: true} // latched from a prior breach
	runSyncLagNotifyTick(context.Background(), newDiscardLogger(), fixedSyncLag{known: false}, fn, "s1", 60, st, time.Minute, time.Now)
	if len(fn.calls()) != 0 {
		t.Fatal("unknown reading fired a notification; want skip")
	}
	if !st.fired {
		t.Fatal("unknown reading re-armed the latch; want hold (absence of signal != recovered)")
	}
}

// TestRunSyncLagNotifyTick_FailureIsolation pins that a dead sink is swallowed
// (advisory only) — the tick returns normally, never panics or propagates.
func TestRunSyncLagNotifyTick_FailureIsolation(t *testing.T) {
	fn := &fakeNotifier{err: context.DeadlineExceeded}
	st := &metricsNotifyRuleState{}
	// Must not panic / must return; the error is logged-and-swallowed.
	runSyncLagNotifyTick(context.Background(), newDiscardLogger(), fixedSyncLag{value: 90, known: true}, fn, "s1", 60, st, time.Minute, time.Now)
	// The sink WAS attempted (the notification was delivered to it) — the
	// failure is swallowed, not skipped before delivery.
	if len(fn.calls()) != 1 {
		t.Fatalf("sink attempted %d times; want 1 (delivered, error swallowed)", len(fn.calls()))
	}
}

// TestEmitSyncLagMetrics pins the exposition line shape.
func TestEmitSyncLagMetrics(t *testing.T) {
	var sb strings.Builder
	emitSyncLagMetrics(&sb, "stream-A", 12.5)
	out := sb.String()
	for _, want := range []string{
		"# TYPE sluice_sync_lag_seconds gauge",
		`sluice_sync_lag_seconds{stream_id="stream-A"} 12.5000`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("emitSyncLagMetrics output missing %q\n--- got ---\n%s", want, out)
		}
	}
}

// TestSyncLagObservationWanted_ZeroValueOff pins the zero-value-safe default:
// a streamer with no metrics endpoint and no sync-lag threshold observes
// nothing (no interceptor, no goroutine).
func TestSyncLagObservationWanted_ZeroValueOff(t *testing.T) {
	if (&Streamer{}).syncLagObservationWanted() {
		t.Fatal("zero-value Streamer wants sync-lag observation; want off by default")
	}
	// A threshold WITHOUT a sink is still off (gated on a sink, like the
	// other notify rules).
	if (&Streamer{NotifySyncLagSeconds: 60}).syncLagObservationWanted() {
		t.Fatal("threshold without a sink enabled observation; want off")
	}
	// Metrics endpoint alone turns the gauge on.
	if !(&Streamer{MetricsListen: ":9090"}).syncLagObservationWanted() {
		t.Fatal("metrics endpoint did not enable observation")
	}
	// Threshold + a sink turns alerting on.
	s := &Streamer{NotifySyncLagSeconds: 60, NotifyWebhookURL: "https://example.test/hook"}
	if !s.syncLagObservationWanted() {
		t.Fatal("threshold + sink did not enable observation")
	}
}
