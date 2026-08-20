// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestOrchestrator_LookAheadCapBoundsDispatch pins the audit-2026-08-19
// coordinator-side look-ahead cap: with the frontier stalled (no lane
// goroutines running, so nothing commits) the router may dispatch at most
// LookAheadCap changes ahead of the frontier, then BLOCKS — bounding
// Frontier.pending instead of letting it grow unbounded while a lane stalls.
// The channel buffer is made large (MaxBatchSize 1000) so the ONLY thing that
// can block the router is the cap, not a full lane channel.
//
// Mutation-verified: removing the WaitForFrontier cap from routeRow lets
// routeRow(cap+1) return immediately, failing the "must block" assertion.
func TestOrchestrator_LookAheadCapBoundsDispatch(t *testing.T) {
	const capN = 4
	o := NewOrchestrator(
		Config{Lanes: 1, MaxBatchSize: 1000, LookAheadCap: capN},
		&routingSeam{commit: func(context.Context, []ir.Change) error { return nil }},
	)
	ctx := context.Background()
	mk := func(i int) ir.Change { return ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(i)}} }

	// The frontier never advances on its own here (no lanes draining). Dispatch
	// up to capN changes ahead of frontier 0 — all must proceed.
	for seq := uint64(1); seq <= capN; seq++ {
		if err := o.routeRow(ctx, seq, mk(int(seq))); err != nil {
			t.Fatalf("routeRow(%d) within the cap should not block: %v", seq, err)
		}
	}

	// seq capN+1 needs frontier ≥ 1, which never happens → routeRow blocks.
	blocked := make(chan error, 1)
	go func() { blocked <- o.routeRow(ctx, capN+1, mk(capN+1)) }()
	select {
	case err := <-blocked:
		t.Fatalf("routeRow(%d) returned (%v); it must block on the look-ahead cap while the frontier lags at 0 — pending is unbounded without it", capN+1, err)
	case <-time.After(300 * time.Millisecond):
		// correctly blocked
	}

	// Advancing the frontier by one releases exactly the blocked dispatch.
	o.frontier.MarkCommitted(1)
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("routeRow unblocked with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("routeRow did not unblock after the frontier advanced past the cap floor")
	}
}

// TestOrchestrator_LookAheadCapDefault confirms an unset LookAheadCap resolves
// to the generous default (a safety bound, not a per-run tuning value) so a
// zero-value Config never accidentally throttles a healthy stream.
func TestOrchestrator_LookAheadCapDefault(t *testing.T) {
	o := NewOrchestrator(Config{Lanes: 1, MaxBatchSize: 1}, &routingSeam{
		commit: func(context.Context, []ir.Change) error { return nil },
	})
	if o.lookAheadCap != defaultFrontierLookAheadCap {
		t.Fatalf("unset LookAheadCap = %d; want the default %d", o.lookAheadCap, uint64(defaultFrontierLookAheadCap))
	}
}
