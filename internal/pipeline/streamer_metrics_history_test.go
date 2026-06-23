// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeMetricsStore is a scriptable [ir.TargetMetricsHistoryStore] for the
// recorder unit tests. It records every sample, counts the calls, and can
// be set to error on Ensure / Record / Prune to exercise failure-isolation.
type fakeMetricsStore struct {
	mu sync.Mutex

	ensureErr error
	recordErr error
	pruneErr  error

	ensureCalls  int
	recorded     []ir.TargetMetricsSample
	pruneCalls   int
	listResponse []ir.TargetMetricsHistoryRow
}

func (f *fakeMetricsStore) EnsureTargetMetricsHistory(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	return f.ensureErr
}

func (f *fakeMetricsStore) RecordTargetMetricsSample(_ context.Context, s ir.TargetMetricsSample) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recordErr != nil {
		return f.recordErr
	}
	f.recorded = append(f.recorded, s)
	return nil
}

func (f *fakeMetricsStore) PruneTargetMetricsHistory(context.Context, time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pruneCalls++
	return f.pruneErr
}

func (f *fakeMetricsStore) ListTargetMetricsHistory(context.Context, string, int) ([]ir.TargetMetricsHistoryRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listResponse, nil
}

func (f *fakeMetricsStore) recordedSamples() []ir.TargetMetricsSample {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ir.TargetMetricsSample(nil), f.recorded...)
}

// metricsStoreApplier is an ir.ChangeApplier that ALSO implements
// ir.TargetMetricsHistoryStore — the type-assert path the recorder takes.
// It embeds a panicking stub so any unexpected applier method call fails
// loudly (the recorder must only touch the history-store surface).
type metricsStoreApplier struct {
	ir.ChangeApplier // nil — never called; the recorder only uses the store surface
	*fakeMetricsStore
}

// plainApplier implements ir.ChangeApplier but NOT the metrics store, to
// pin the no-op-when-unsupported path.
type plainApplier struct {
	ir.ChangeApplier
}

// TestStartTelemetrySidecars_StartsRecorderEarly pins roadmap item 39: the
// unified entry point (called from runOnce BEFORE cold-copy, so the telemetry
// consumers cover the cold-copy phase) starts the recorder when a provider +
// store-applier are wired — proven by a synchronous EnsureTargetMetricsHistory
// call before the ticker goroutine spawns.
func TestStartTelemetrySidecars_StartsRecorderEarly(t *testing.T) {
	store := &fakeMetricsStore{}
	applier := &metricsStoreApplier{fakeMetricsStore: store}
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: time.Now(), CPUUtil: 0.4, CPUKnown: true,
	}}
	s := &Streamer{TargetTelemetry: prov}

	ctx, cancel := context.WithCancel(context.Background())
	s.startTelemetrySidecars(ctx, applier, "s1")
	cancel() // stop the recorder's ticker goroutine

	store.mu.Lock()
	n := store.ensureCalls
	store.mu.Unlock()
	if n != 1 {
		t.Fatalf("recorder not started by startTelemetrySidecars: ensureCalls=%d, want 1", n)
	}
}

// TestStartTelemetrySidecars_NoProviderNoOp pins the degrade: no telemetry
// provider ⇒ neither sidecar starts (the common no-PlanetScale-telemetry sync).
func TestStartTelemetrySidecars_NoProviderNoOp(t *testing.T) {
	store := &fakeMetricsStore{}
	applier := &metricsStoreApplier{fakeMetricsStore: store}
	s := &Streamer{} // nil TargetTelemetry

	s.startTelemetrySidecars(context.Background(), applier, "s1")

	store.mu.Lock()
	n := store.ensureCalls
	store.mu.Unlock()
	if n != 0 {
		t.Fatalf("recorder started with no provider: ensureCalls=%d, want 0", n)
	}
}

func TestRecordTargetMetricsTick_DedupesOnSampledAt(t *testing.T) {
	store := &fakeMetricsStore{}
	t0 := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{
		SampledAt: t0, CPUUtil: 0.4, CPUKnown: true,
	}}
	var last time.Time
	logger := newDiscardLogger()

	// First tick records.
	recordTargetMetricsTick(context.Background(), logger, store, prov, "s1", &last)
	// Same SampledAt (source hasn't updated) — must NOT re-record.
	recordTargetMetricsTick(context.Background(), logger, store, prov, "s1", &last)
	// Advance the poll timestamp — records again.
	prov.snap.SampledAt = t0.Add(60 * time.Second)
	recordTargetMetricsTick(context.Background(), logger, store, prov, "s1", &last)

	got := store.recordedSamples()
	if len(got) != 2 {
		t.Fatalf("expected 2 records (dedupe on identical SampledAt), got %d", len(got))
	}
	if !got[0].SampledAt.Equal(t0) || !got[1].SampledAt.Equal(t0.Add(60*time.Second)) {
		t.Errorf("recorded SampledAt mismatch: %v", got)
	}
	if !got[0].CPUKnown || got[0].CPUUtil != 0.4 {
		t.Errorf("CPU field not carried: %+v", got[0])
	}
}

func TestRecordTargetMetricsTick_NoSignalNoRecord(t *testing.T) {
	store := &fakeMetricsStore{}
	prov := &fakeTelemetry{ok: false} // no usable signal
	var last time.Time
	recordTargetMetricsTick(context.Background(), newDiscardLogger(), store, prov, "s1", &last)

	// Also a fresh-ok but zero-SampledAt snapshot must be a no-op.
	prov2 := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{}}
	recordTargetMetricsTick(context.Background(), newDiscardLogger(), store, prov2, "s1", &last)

	if n := len(store.recordedSamples()); n != 0 {
		t.Fatalf("expected 0 records on no-signal/zero-time, got %d", n)
	}
}

func TestRecordTargetMetricsTick_RecordErrorIsolatedAndRetried(t *testing.T) {
	store := &fakeMetricsStore{recordErr: errors.New("target unreachable")}
	t0 := time.Now().UTC()
	prov := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: t0, CPUUtil: 0.5, CPUKnown: true}}
	var last time.Time

	// A store error must be swallowed (no panic) AND must NOT advance the
	// dedupe cursor, so the next tick retries the same sample once the store
	// recovers.
	recordTargetMetricsTick(context.Background(), newDiscardLogger(), store, prov, "s1", &last)
	if !last.IsZero() {
		t.Errorf("dedupe cursor advanced despite a failed write; want it to retry")
	}
	store.mu.Lock()
	store.recordErr = nil
	store.mu.Unlock()
	recordTargetMetricsTick(context.Background(), newDiscardLogger(), store, prov, "s1", &last)
	if n := len(store.recordedSamples()); n != 1 {
		t.Fatalf("expected the retried sample to land once, got %d", n)
	}
}

func TestStartTargetMetricsHistoryRecorder_NoOpPaths(t *testing.T) {
	t0 := time.Now().UTC()
	freshSnap := ir.TargetHealthSnapshot{SampledAt: t0, CPUUtil: 0.4, CPUKnown: true}

	t.Run("nil provider", func(t *testing.T) {
		s := &Streamer{}
		store := &fakeMetricsStore{}
		applier := metricsStoreApplier{fakeMetricsStore: store}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.startTargetMetricsHistoryRecorder(ctx, "s1", applier, nil)
		if store.ensureCalls != 0 {
			t.Errorf("nil provider must be a no-op; ensure called %d times", store.ensureCalls)
		}
	})

	t.Run("applier lacks store impl", func(_ *testing.T) {
		s := &Streamer{}
		prov := &fakeTelemetry{ok: true, snap: freshSnap}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// plainApplier does NOT implement the store — must be a clean no-op.
		s.startTargetMetricsHistoryRecorder(ctx, "s1", plainApplier{}, prov)
	})

	t.Run("suppressed by flag", func(t *testing.T) {
		s := &Streamer{SuppressTargetMetricsHistory: true}
		store := &fakeMetricsStore{}
		applier := metricsStoreApplier{fakeMetricsStore: store}
		prov := &fakeTelemetry{ok: true, snap: freshSnap}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.startTargetMetricsHistoryRecorder(ctx, "s1", applier, prov)
		if store.ensureCalls != 0 {
			t.Errorf("suppressed recorder must be a no-op; ensure called %d times", store.ensureCalls)
		}
	})

	t.Run("ensure error disables recorder (no goroutine)", func(t *testing.T) {
		s := &Streamer{}
		store := &fakeMetricsStore{ensureErr: errors.New("create table denied")}
		applier := metricsStoreApplier{fakeMetricsStore: store}
		prov := &fakeTelemetry{ok: true, snap: freshSnap}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Must not panic; ensure is attempted once, then it returns.
		s.startTargetMetricsHistoryRecorder(ctx, "s1", applier, prov)
		if store.ensureCalls != 1 {
			t.Errorf("ensure should be attempted exactly once, got %d", store.ensureCalls)
		}
		if n := len(store.recordedSamples()); n != 0 {
			t.Errorf("a failed ensure must not record, got %d", n)
		}
	})
}
