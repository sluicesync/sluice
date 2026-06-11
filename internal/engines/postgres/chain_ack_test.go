// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

func mustPos(t *testing.T, lsn pglogrepl.LSN) ir.Position {
	t.Helper()
	pos, err := encodePGPos(pgPos{Slot: "sluice_slot", LSN: lsn.String()})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	return pos
}

// TestCDCReader_AckLSN_ChainConsumerHold pins the chain-consumer ack
// clamp (task #40 half (c)): with HoldSlotAckAtCommitted set and no
// applier tracker, the keepalive must never advertise past
// max(startLSN, released ceiling) — the no-tracker fallback (ack the
// streamed LSN) is exactly the silent-loss shape for backup chains
// (events parsed by the pump but never committed to chunks would
// release WAL the chain doesn't carry).
func TestCDCReader_AckLSN_ChainConsumerHold(t *testing.T) {
	r := &CDCReader{}
	start := pglogrepl.LSN(100)
	streamed := pglogrepl.LSN(500)

	// Baseline (no hold): legacy fallback acks the streamed LSN.
	if got := r.ackLSN(streamed, start); got != streamed {
		t.Fatalf("no-hold ackLSN = %v; want streamed %v", got, streamed)
	}

	r.HoldSlotAckAtCommitted()

	// Held, nothing released: clamp to startLSN even though the pump
	// has streamed far past it.
	if got := r.ackLSN(streamed, start); got != start {
		t.Errorf("held ackLSN = %v; want start %v (streamed must not leak)", got, start)
	}

	// Release to 300: ack follows the ceiling, still not streamed.
	if err := r.ReleaseSlotAckTo(mustPos(t, 300)); err != nil {
		t.Fatalf("ReleaseSlotAckTo: %v", err)
	}
	if got := r.ackLSN(streamed, start); got != pglogrepl.LSN(300) {
		t.Errorf("after release(300), ackLSN = %v; want 300", got)
	}

	// Ratchet is monotonic: a lower release is ignored.
	if err := r.ReleaseSlotAckTo(mustPos(t, 200)); err != nil {
		t.Fatalf("ReleaseSlotAckTo(lower): %v", err)
	}
	if got := r.ackLSN(streamed, start); got != pglogrepl.LSN(300) {
		t.Errorf("after lower release(200), ackLSN = %v; want 300 (monotonic)", got)
	}

	// Streamed below the ceiling: ack the streamed value (the clamp
	// only caps, it never inflates).
	if got := r.ackLSN(pglogrepl.LSN(250), start); got != pglogrepl.LSN(250) {
		t.Errorf("streamed(250) under ceiling(300): ackLSN = %v; want 250", got)
	}

	// The zero position (the "from now" sentinel) is ignored without
	// error; a FOREIGN engine's position is a loud error (a chain
	// whose parent position belongs to another engine is corrupt —
	// decodePGPos's cross-engine refusal propagates).
	if err := r.ReleaseSlotAckTo(ir.Position{}); err != nil {
		t.Errorf("ReleaseSlotAckTo(zero position) = %v; want nil (ignored)", err)
	}
	if err := r.ReleaseSlotAckTo(ir.Position{Engine: "mysql", Token: "x"}); err == nil {
		t.Error("ReleaseSlotAckTo(foreign position) = nil; want loud cross-engine error")
	}
}

// TestPreflightChainResume_PositionShapes pins the preflight's
// non-server-touching paths: the zero "from now" sentinel is a nil
// skip (nothing to verify), and a foreign engine's position is a loud
// refusal (corrupt chain), both decided before any connection is
// dialed.
func TestPreflightChainResume_PositionShapes(t *testing.T) {
	if err := (Engine{}).PreflightChainResume(context.Background(), "postgres://unused", ir.Position{}); err != nil {
		t.Errorf("PreflightChainResume(zero position) = %v; want nil skip", err)
	}
	if err := (Engine{}).PreflightChainResume(context.Background(), "postgres://unused", ir.Position{Engine: "mysql", Token: `{"gtid":"x"}`}); err == nil {
		t.Error("PreflightChainResume(foreign position) = nil; want loud cross-engine error")
	}
}
