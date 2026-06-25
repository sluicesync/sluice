// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClampRestoreParallelismByHeadroom_ProductClamp pins the ADR-0115 restore
// headroom clamp: with BOTH axes auto, the resolved table×chunk PRODUCT is
// reduced ~divisor-fold by the target's live CPU/mem headroom, sharing the same
// thresholds as the apply-path clamp (healthy → unchanged, approaching →
// halved, saturated → quartered). The cross-table axis absorbs the reduction
// first (the within-table chunk fan-out is preserved).
func TestClampRestoreParallelismByHeadroom_ProductClamp(t *testing.T) {
	const (
		tableP = 4
		chunkP = 4 // product 16
	)
	cases := []struct {
		name         string
		prov         ir.TargetTelemetry
		wantT, wantC int
	}{
		{
			"no telemetry (nil) → unchanged",
			nil, tableP, chunkP,
		},
		{
			"healthy (0.10) → unchanged",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.10, CPUKnown: true}},
			tableP, chunkP,
		},
		{
			"approaching (0.72) → product halved (table absorbs)",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.72, CPUKnown: true}},
			2, 4, // 8 == 16/2
		},
		{
			"saturated (0.90) → product quartered (table absorbs)",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.90, CPUKnown: true}},
			1, 4, // 4 == 16/4
		},
		{
			"mem drives when cpu unknown (saturated)",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), MemUtil: 0.95, MemKnown: true}},
			1, 4,
		},
		{
			"no fresh signal (ok=false) → unchanged",
			&fakeTelemetry{ok: false, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.99, CPUKnown: true}},
			tableP, chunkP,
		},
		{
			"neither cpu nor mem observed → unchanged",
			&fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), StorageUtil: 0.99, StorageKnown: true}},
			tableP, chunkP,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &Restore{TargetTelemetry: c.prov} // both axes auto (0)
			gotT, gotC := r.clampRestoreParallelismByHeadroom(context.Background(), tableP, chunkP)
			if gotT != c.wantT || gotC != c.wantC {
				t.Errorf("clamp(table=%d,chunk=%d) = (%d,%d); want (%d,%d)", tableP, chunkP, gotT, gotC, c.wantT, c.wantC)
			}
			// The clamp must NEVER raise the product.
			if gotT*gotC > tableP*chunkP {
				t.Errorf("clamp raised the product: %d > %d", gotT*gotC, tableP*chunkP)
			}
		})
	}
}

// TestClampRestoreParallelismByHeadroom_RespectsPinnedAxes pins that an
// explicitly-pinned axis (operator passed --table-parallelism / --bulk-
// parallelism, so the field is non-zero) is NOT clamped; only an auto axis is
// reduced, and when both are pinned the clamp is a no-op.
func TestClampRestoreParallelismByHeadroom_RespectsPinnedAxes(t *testing.T) {
	sat := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.95, CPUKnown: true}}

	// Both pinned → no clamp at all.
	r := &Restore{TargetTelemetry: sat, TableParallelism: 4, ChunkParallelism: 4}
	if gotT, gotC := r.clampRestoreParallelismByHeadroom(context.Background(), 4, 4); gotT != 4 || gotC != 4 {
		t.Errorf("both pinned: clamp = (%d,%d); want (4,4) — operator intent must be respected", gotT, gotC)
	}

	// Table pinned, chunk auto → only the chunk (within-table) axis shrinks.
	r = &Restore{TargetTelemetry: sat, TableParallelism: 4 /* pinned */, ChunkParallelism: 0 /* auto */}
	gotT, gotC := r.clampRestoreParallelismByHeadroom(context.Background(), 4, 4)
	if gotT != 4 {
		t.Errorf("table pinned but reduced: table=%d; want 4", gotT)
	}
	if gotC != 1 { // target 16/4=4; chunk = 4/table(4) = 1
		t.Errorf("chunk (auto) not clamped to fit: chunk=%d; want 1", gotC)
	}

	// Chunk pinned, table auto → only the cross-table axis shrinks.
	r = &Restore{TargetTelemetry: sat, TableParallelism: 0 /* auto */, ChunkParallelism: 8 /* pinned */}
	gotT, gotC = r.clampRestoreParallelismByHeadroom(context.Background(), 4, 8) // product 32, /4 → 8
	if gotC != 8 {
		t.Errorf("chunk pinned but reduced: chunk=%d; want 8", gotC)
	}
	if gotT != 1 { // target 32/4=8; table = 8/chunk(8) = 1
		t.Errorf("table (auto) not clamped to fit: table=%d; want 1", gotT)
	}
}

// TestClampRestoreParallelismByHeadroom_NeverBelowOne pins the floor: a minimal
// 1×1 product is untouched, and a saturated clamp never produces a 0 axis.
func TestClampRestoreParallelismByHeadroom_NeverBelowOne(t *testing.T) {
	sat := &fakeTelemetry{ok: true, snap: ir.TargetHealthSnapshot{SampledAt: freshNow(), CPUUtil: 0.99, CPUKnown: true}}
	r := &Restore{TargetTelemetry: sat} // both auto
	if gotT, gotC := r.clampRestoreParallelismByHeadroom(context.Background(), 1, 1); gotT != 1 || gotC != 1 {
		t.Errorf("1x1 saturated = (%d,%d); want (1,1)", gotT, gotC)
	}
	// 2x1 product 2, /4 → target 1: table floored to 1, chunk 1.
	if gotT, gotC := r.clampRestoreParallelismByHeadroom(context.Background(), 2, 1); gotT < 1 || gotC < 1 {
		t.Errorf("2x1 saturated = (%d,%d); axes must stay >= 1", gotT, gotC)
	}
}
