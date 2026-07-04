// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// recordingGateWriter is a minimal ir.RowWriter that ALSO implements
// ir.GrowGateSetter, recording the gate it is handed. It pins the ADR-0110
// (v0.99.102) wiring: runBulkCopyPhases must attach the run's coordinated
// grow-pause gate to the TOP-LEVEL cold-copy writer (the one the
// native-concurrent / fan-out path reuses across all D workers), not only
// the migrate keyset-chunked per-chunk writers. v0.99.100 wired only the
// chunked path, so the gate was inert in the sync cold-start path — the
// PS-320-v11 live run tripped the gate zero times under 74 real grow
// retries.
type recordingGateWriter struct {
	gotGate    ir.GrowGate
	setCalled  bool
	setCallNum int
}

func (w *recordingGateWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error {
	return nil
}

func (w *recordingGateWriter) SetGrowGate(g ir.GrowGate) {
	w.setCalled = true
	w.setCallNum++
	w.gotGate = g
}

// TestRunBulkCopyPhases_WiresGrowGateOntoTopLevelWriter pins that the
// central cold-copy entry attaches deps.growGate to the writer it is given,
// so the native-concurrent / fan-out path (which reuses that writer across
// every worker) actually engages the ADR-0110 coordination. Uses an empty
// schema so the copy/index/constraint phases are no-ops — only the wiring
// at the top of runBulkCopyPhases is exercised.
func TestRunBulkCopyPhases_WiresGrowGateOntoTopLevelWriter(t *testing.T) {
	ctx := context.Background()
	gate := growGateOrNil(newGrowGate(ctx, nil))
	if gate == nil {
		t.Fatal("test setup: gate should be non-nil")
	}
	rw := &recordingGateWriter{}
	deps := &parallelBulkCopyDeps{growGate: gate}

	err := runBulkCopyPhases(
		ctx,
		resumeContext{}, // disabled: state ops no-op
		&ir.MigrationState{},
		&ir.Schema{}, // no tables → copy/index/constraint phases are no-ops
		noopRowReader{},
		noopSchemaWriter{},
		rw,
		false, // resuming
		1000,  // bulkBatchSize
		deps,
		1, // tableParallelism
		nil,
		ShardColumnSpec{},
		false, // upfrontIndexes
		false, // analyzeAfter
	)
	if err != nil {
		t.Fatalf("runBulkCopyPhases(empty schema): %v", err)
	}
	if !rw.setCalled {
		t.Fatal("runBulkCopyPhases did not call SetGrowGate on the top-level writer — the ADR-0110 gate is inert in this path (the v0.99.100 sync-path bug)")
	}
	if rw.gotGate != gate {
		t.Errorf("writer received gate %v, want the run's deps.growGate %v", rw.gotGate, gate)
	}
}

// TestRunBulkCopyPhases_NilGrowGateIsNoOp pins the pre-ADR-0110 degrade: a
// nil run gate must NOT call SetGrowGate (the writer keeps its zero-value
// nil gate), so an untroubled / non-PlanetScale copy is byte-for-byte
// unchanged.
func TestRunBulkCopyPhases_NilGrowGateIsNoOp(t *testing.T) {
	ctx := context.Background()
	rw := &recordingGateWriter{}
	deps := &parallelBulkCopyDeps{growGate: nil}

	err := runBulkCopyPhases(
		ctx, resumeContext{}, &ir.MigrationState{}, &ir.Schema{},
		noopRowReader{}, noopSchemaWriter{}, rw, false, 1000, deps, 1, nil, ShardColumnSpec{}, false, false,
	)
	if err != nil {
		t.Fatalf("runBulkCopyPhases(nil gate): %v", err)
	}
	if rw.setCalled {
		t.Error("SetGrowGate was called with a nil run gate — applyGrowGate must no-op on a nil gate (pre-ADR-0110 behaviour)")
	}
}
