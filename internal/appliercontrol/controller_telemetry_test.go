// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliercontrol

import (
	"context"
	"testing"
	"time"
)

// fakeHint is a scriptable [TelemetryHint] for the ADR-0107 advisory
// proactive-damp tests. saturated/ok are read on every Saturated() call
// so a test can flip them between ObserveBatch calls to model a rising /
// falling saturation edge or a provider going stale (ok=false).
type fakeHint struct {
	saturated bool
	ok        bool
}

func (f *fakeHint) Saturated() (saturated, ok bool) {
	return f.saturated, f.ok
}

// observeHealthy drives n successful, fast batches (p95 well under the
// target) so the only thing that could change the size is the telemetry
// hint — isolating its effect from the reactive latency path.
func observeHealthy(c *Controller, n int) {
	ctx := context.Background()
	for i := 0; i < n; i++ {
		c.ObserveBatch(ctx, 1*time.Millisecond, 10, nil)
	}
}

// TestController_TelemetrySaturationSuppressesAI pins assertion (i): a
// FRESH saturated signal suppresses additive-increase even when p95 is
// healthy (the controller would otherwise be climbing toward the
// ceiling). After the one edge-MD, the size holds — it does not grow.
func TestController_TelemetrySaturationSuppressesAI(t *testing.T) {
	hint := &fakeHint{saturated: true, ok: true}
	c := mustController(t, Config{
		StreamID:      "sat",
		Floor:         1,
		Ceiling:       1000,
		InitialSize:   100,
		TargetLatency: 5 * time.Second,
		WindowSize:    10,
		AdditiveStep:  5,
		TelemetryHint: hint,
	})
	observeHealthy(c, 30)
	// The first saturated edge MD halves 100 -> 50; from then on the
	// controller holds (sustained saturation suppresses AI). It must NEVER
	// climb back above the post-MD size while saturation persists.
	if got := c.NextBatchSize(); got != 50 {
		t.Fatalf("under sustained fresh saturation, size = %d; want held at 50 (one edge MD, then hold)", got)
	}
	if !c.Snapshot().TelemetryDamped {
		t.Fatalf("expected TelemetryDamped=true while actively damping")
	}
}

// TestController_TelemetrySaturationEdgeFiresOnceThenHolds pins
// assertion (ii): exactly ONE multiplicative-decrease fires on a fresh
// saturation edge; sustained saturation does not keep shrinking.
func TestController_TelemetrySaturationEdgeFiresOnceThenHolds(t *testing.T) {
	hint := &fakeHint{saturated: false, ok: true}
	c := mustController(t, Config{
		StreamID:             "edge",
		Floor:                1,
		Ceiling:              1000,
		InitialSize:          200,
		TargetLatency:        5 * time.Second,
		WindowSize:           10,
		AdditiveStep:         5,
		MultiplicativeFactor: 0.5,
		TelemetryHint:        hint,
	})
	// Healthy + not-saturated: AI climbs (warm up then +5/batch).
	observeHealthy(c, 5)
	beforeEdge := c.NextBatchSize()
	if beforeEdge <= 200 {
		t.Fatalf("expected AI to climb above 200 before the edge; got %d", beforeEdge)
	}
	if c.Snapshot().DecreasesTotal != 0 {
		t.Fatalf("no MD should have fired yet; got %d", c.Snapshot().DecreasesTotal)
	}

	// Rising edge: saturation crosses true.
	hint.saturated = true
	c.ObserveBatch(context.Background(), 1*time.Millisecond, 10, nil)
	afterEdge := c.NextBatchSize()
	if afterEdge != beforeEdge/2 {
		t.Fatalf("edge MD: size = %d; want %d (half of %d)", afterEdge, beforeEdge/2, beforeEdge)
	}
	if dt := c.Snapshot().DecreasesTotal; dt != 1 {
		t.Fatalf("edge MD count = %d; want exactly 1", dt)
	}

	// Sustained saturation: many more batches, NO further shrink.
	observeHealthy(c, 50)
	if got := c.NextBatchSize(); got != afterEdge {
		t.Fatalf("sustained saturation must hold; size = %d, want held at %d", got, afterEdge)
	}
	if dt := c.Snapshot().DecreasesTotal; dt != 1 {
		t.Fatalf("sustained saturation fired extra MDs; count = %d, want 1", dt)
	}
}

// TestController_TelemetryReSaturationFiresAgain pins that the edge latch
// RESETS when saturation clears, so a NEW crossing fires a fresh MD —
// the damp tracks edges, not a one-time fuse.
func TestController_TelemetryReSaturationFiresAgain(t *testing.T) {
	hint := &fakeHint{saturated: true, ok: true}
	c := mustController(t, Config{
		StreamID:             "resat",
		Floor:                1,
		Ceiling:              1000,
		InitialSize:          400,
		TargetLatency:        5 * time.Second,
		MultiplicativeFactor: 0.5,
		TelemetryHint:        hint,
	})
	observeHealthy(c, 3) // first edge: 400 -> 200
	if got := c.NextBatchSize(); got != 200 {
		t.Fatalf("first edge MD: size = %d; want 200", got)
	}
	// Saturation clears: latch resets (and AI may begin again).
	hint.saturated = false
	observeHealthy(c, 1)
	if c.Snapshot().TelemetryDamped {
		t.Fatalf("TelemetryDamped should be false once saturation clears")
	}
	// Second crossing fires another MD.
	hint.saturated = true
	c.ObserveBatch(context.Background(), 1*time.Millisecond, 10, nil)
	if dt := c.Snapshot().DecreasesTotal; dt != 2 {
		t.Fatalf("re-saturation MD count = %d; want 2", dt)
	}
}

// TestController_TelemetryNoSignalIsByteIdenticalNoOp pins assertion
// (iii): ok=false (provider not warmed up / stale) is a byte-for-byte
// no-op versus a controller with NO hint at all. Two controllers driven
// by the identical batch sequence must land on the identical size and
// decrease count.
func TestController_TelemetryNoSignalIsByteIdenticalNoOp(t *testing.T) {
	mk := func(hint TelemetryHint) *Controller {
		return mustController(t, Config{
			StreamID:      "noop",
			Floor:         1,
			Ceiling:       1000,
			InitialSize:   50,
			TargetLatency: 5 * time.Second,
			WindowSize:    10,
			AdditiveStep:  5,
			TelemetryHint: hint,
		})
	}
	// Stale/no-signal hint: saturated could be anything; ok=false means
	// "ignore me". We deliberately set saturated=true to prove ok gates it.
	noSignal := &fakeHint{saturated: true, ok: false}
	withHint := mk(noSignal)
	noHint := mk(nil)

	for i := 0; i < 40; i++ {
		withHint.ObserveBatch(context.Background(), 1*time.Millisecond, 10, nil)
		noHint.ObserveBatch(context.Background(), 1*time.Millisecond, 10, nil)
	}
	if a, b := withHint.NextBatchSize(), noHint.NextBatchSize(); a != b {
		t.Fatalf("no-signal hint diverged from no-hint: %d vs %d", a, b)
	}
	if a, b := withHint.Snapshot().DecreasesTotal, noHint.Snapshot().DecreasesTotal; a != b {
		t.Fatalf("no-signal hint changed decrease count: %d vs %d", a, b)
	}
	if withHint.Snapshot().TelemetryDamped {
		t.Fatalf("no-signal hint must not report TelemetryDamped")
	}
}

// TestController_TelemetryNeverExceedsCeilingOrFloor pins assertion (iv):
// the hint can only HOLD or SHRINK within [Floor, Ceiling] — it can never
// push the size above Ceiling or below Floor regardless of how the
// signal flaps. We drive an adversarial saturate/clear flap and assert
// the invariant holds on every observation.
func TestController_TelemetryNeverExceedsCeilingOrFloor(t *testing.T) {
	hint := &fakeHint{ok: true}
	const floor, ceiling = 4, 64
	c := mustController(t, Config{
		StreamID:             "bounds",
		Floor:                floor,
		Ceiling:              ceiling,
		InitialSize:          ceiling,
		TargetLatency:        5 * time.Second,
		MultiplicativeFactor: 0.5,
		TelemetryHint:        hint,
	})
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		// Flap saturation every other batch to maximise edge churn.
		hint.saturated = i%2 == 0
		c.ObserveBatch(ctx, 1*time.Millisecond, 10, nil)
		if sz := c.NextBatchSize(); sz < floor || sz > ceiling {
			t.Fatalf("batch %d: size %d escaped [%d, %d]", i, sz, floor, ceiling)
		}
	}
}
