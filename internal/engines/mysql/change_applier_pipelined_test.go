// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_ApplyPipelineDepth_ZeroValueSafe pins the v0.99.51
// trap for ADR-0104: a bare-struct applier (every non-CLI construction:
// tests, broker, chain, future callers) defaults to SERIAL apply. The Go
// zero value (0) and 1 BOTH resolve to serial — no pipeline engaged, no
// dedicated pool. Pipelining engages ONLY for an explicit W > 1.
func TestChangeApplier_ApplyPipelineDepth_ZeroValueSafe(t *testing.T) {
	cases := []struct {
		depth       int
		wantEnabled bool
	}{
		{depth: 0, wantEnabled: false},  // zero value — the common default
		{depth: 1, wantEnabled: false},  // explicit serial
		{depth: 2, wantEnabled: true},   // engaged
		{depth: 8, wantEnabled: true},   // engaged
		{depth: -3, wantEnabled: false}, // clamped to 0 by the setter
	}
	for _, c := range cases {
		a := &ChangeApplier{}
		a.SetApplyPipelineDepth(c.depth)
		if got := a.pipelineEnabled(); got != c.wantEnabled {
			t.Errorf("SetApplyPipelineDepth(%d) → pipelineEnabled() = %v; want %v",
				c.depth, got, c.wantEnabled)
		}
	}
}

// TestChangeApplier_ApplyPipelineDepth_NegativeClampedToSerial pins that a
// mis-parsed / negative flag can NEVER engage the pipeline — it clamps to
// 0 (serial). Loud-failure discipline applied to a config edge: the safe
// outcome (serial) is the floor.
func TestChangeApplier_ApplyPipelineDepth_NegativeClampedToSerial(t *testing.T) {
	a := &ChangeApplier{}
	a.SetApplyPipelineDepth(-10)
	if a.applyPipelineDepth != 0 {
		t.Errorf("applyPipelineDepth after SetApplyPipelineDepth(-10) = %d; want 0 (clamped)", a.applyPipelineDepth)
	}
	if a.pipelineEnabled() {
		t.Error("pipelineEnabled() = true after a negative depth; want false (serial)")
	}
}

// TestChangeApplier_Pipeline_SerialDepthIsUnavailable pins that pipeline()
// reports errPipelineUnavailable for the serial depths, so the BeginTx
// closure takes the serial *sql.Tx path with NO WARN (no degradation —
// the operator never opted in).
func TestChangeApplier_Pipeline_SerialDepthIsUnavailable(t *testing.T) {
	for _, depth := range []int{0, 1} {
		a := &ChangeApplier{}
		a.SetApplyPipelineDepth(depth)
		_, err := a.pipeline(context.Background())
		if !errors.Is(err, errPipelineUnavailable) {
			t.Errorf("depth=%d: pipeline() err = %v; want errPipelineUnavailable", depth, err)
		}
	}
}

// TestChangeApplier_Pipeline_NoCfgFallsBackUnavailable pins the
// direct-API / unit-construction fallback: an applier with depth > 1 but
// no pipelineCfg (never went through OpenChangeApplier) cannot open a
// dedicated pool, so pipeline() reports errPipelineUnavailable and the
// batch path falls back to serial with a one-time WARN — loud, never
// silent. (OpenChangeApplier is the only constructor that sets
// pipelineCfg.)
func TestChangeApplier_Pipeline_NoCfgFallsBackUnavailable(t *testing.T) {
	a := &ChangeApplier{}
	a.SetApplyPipelineDepth(4)
	if a.pipelineCfg != nil {
		t.Fatal("bare struct unexpectedly has pipelineCfg set")
	}
	_, err := a.pipeline(context.Background())
	if !errors.Is(err, errPipelineUnavailable) {
		t.Errorf("pipeline() with nil cfg err = %v; want errPipelineUnavailable", err)
	}
}

// TestChangeApplier_DrainPipeline_NilIsNoop pins that draining when no
// pipeline was ever started (the serial path) is a clean no-op returning
// nil — ApplyBatch calls it unconditionally after the loop.
func TestChangeApplier_DrainPipeline_NilIsNoop(t *testing.T) {
	a := &ChangeApplier{}
	if err := a.drainPipeline(); err != nil {
		t.Errorf("drainPipeline() with no pipeline = %v; want nil", err)
	}
}

// TestChangeApplier_ImplementsApplyPipelineDepthSetter is a compile-time
// guarantee that the MySQL applier exposes the ADR-0104 optional-surface
// setter the streamer probes for. A refactor that drops it breaks the
// build here — the loud-failure shape we want.
func TestChangeApplier_ImplementsApplyPipelineDepthSetter(_ *testing.T) {
	var _ ir.ApplyPipelineDepthSetter = (*ChangeApplier)(nil)
}
