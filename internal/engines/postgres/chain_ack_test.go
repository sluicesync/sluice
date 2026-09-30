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

// TestAckLSN_NeverPastTheReleasedCeiling pins GC-41's reader half: the
// keepalive never advertises past max(startLSN, the highest position a
// consumer has released), and it does so for the ZERO-VALUE reader — no
// opt-in call. Before GC-41 the clamp was a mode only the backup chain
// engaged, and a reader nobody engaged acked the STREAMED LSN: a Postgres
// source synced to MySQL / SQLite / D1 released WAL for changes still in
// an apply batch, and a stop mid-batch resumed past them (silent loss).
func TestAckLSN_NeverPastTheReleasedCeiling(t *testing.T) {
	r := &CDCReader{slotName: "sluice_slot"}
	start := pglogrepl.LSN(100)
	streamed := pglogrepl.LSN(500)

	// Nothing released: the ack holds at startLSN even though the pump
	// has streamed far past it. This is the arm the defect lived in.
	if got := r.ackLSN(streamed, start); got != start {
		t.Fatalf("unreleased ackLSN = %v; want start %v (the streamed LSN must not leak)", got, start)
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
	// only caps, it never inflates past what the pump has seen).
	if got := r.ackLSN(pglogrepl.LSN(250), start); got != pglogrepl.LSN(250) {
		t.Errorf("streamed(250) under ceiling(300): ackLSN = %v; want 250", got)
	}

	// A ceiling below startLSN is floored at startLSN (the start
	// position is durable by definition).
	if got := r.ackLSN(streamed, pglogrepl.LSN(400)); got != pglogrepl.LSN(400) {
		t.Errorf("ceiling(300) under start(400): ackLSN = %v; want start 400", got)
	}
}

// TestReleaseSlotAckTo_PositionShapes pins what a release accepts. The
// zero position (the "from now" sentinel) is ignored without error; a
// FOREIGN engine's position is a loud error (decodePGPos's cross-engine
// refusal — a caller handing one over has a corrupt position, and saying
// so beats a silently frozen slot); and a position naming a DIFFERENT
// slot is ignored, because it is no evidence about this slot's stream and
// ignoring it can only hold the ack back.
func TestReleaseSlotAckTo_PositionShapes(t *testing.T) {
	r := &CDCReader{slotName: "sluice_slot"}
	start := pglogrepl.LSN(100)
	streamed := pglogrepl.LSN(500)

	if err := r.ReleaseSlotAckTo(ir.Position{}); err != nil {
		t.Errorf("ReleaseSlotAckTo(zero position) = %v; want nil (ignored)", err)
	}
	if err := r.ReleaseSlotAckTo(ir.Position{Engine: "mysql", Token: "x"}); err == nil {
		t.Error("ReleaseSlotAckTo(foreign position) = nil; want loud cross-engine error")
	}
	other, err := encodePGPos(pgPos{Slot: "sluice_other", LSN: pglogrepl.LSN(400).String()})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	if err := r.ReleaseSlotAckTo(other); err != nil {
		t.Fatalf("ReleaseSlotAckTo(other slot) = %v; want nil (ignored)", err)
	}
	if got := r.ackLSN(streamed, start); got != start {
		t.Errorf("after a release naming another slot, ackLSN = %v; want start %v (ignored)", got, start)
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
