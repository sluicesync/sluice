// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0110 grow-gate wiring pins (pipeline source-read retry side). The
// coordinator FSM is pinned in grow_gate_test.go; here we pin only that
// copyTableWithSourceReadRetry reaches the gate — Awaits before each
// (re)attempt and Trips on a classified source-read transient.

// recordingGate is a pass-through ir.GrowGate that counts Await/Trip.
type recordingGate struct {
	awaits       atomic.Int64
	trips        atomic.Int64
	lastEvidence atomic.Int32
}

func (g *recordingGate) Await(ctx context.Context) error {
	g.awaits.Add(1)
	return ctx.Err()
}

func (g *recordingGate) Trip(_ string, ev ir.GrowEvidence) {
	g.trips.Add(1)
	g.lastEvidence.Store(int32(ev))
}

// LastEvidence reports the verdict of the most recent Trip.
func (g *recordingGate) LastEvidence() ir.GrowEvidence {
	return ir.GrowEvidence(g.lastEvidence.Load())
}

// TestSourceReadGrowGate_AwaitsAndTrips pins that a classified source-read
// drop trips the shared gate so siblings quiesce, and that every (re)attempt
// Awaits the gate. It also pins the trip's CLAIM: a source-side drop reports
// [ir.GrowEvidenceNone], because nothing on this path observed the target
// (item 143).
func TestSourceReadGrowGate_AwaitsAndTrips(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	gate := &recordingGate{}
	var attempts int
	attempt := func(_ context.Context, _ ir.RowReader, _ bool) error {
		attempts++
		if attempts <= 2 {
			return fakeRetriableErr{msg: "mysql: rows iteration: invalid connection"}
		}
		return nil
	}
	freshReader := func(_ context.Context) (ir.RowReader, func(), error) {
		return noopRowReader{}, func() {}, nil
	}
	truncate := func(_ context.Context) error { return nil }

	err := copyTableWithSourceReadRetry(context.Background(), "documents",
		resumeFromChunkCursor, noopRowReader{}, false, attempt, freshReader, truncate, gate)
	if err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
	// 3 attempts (2 transient + 1 success) ⇒ 3 Awaits (one before each).
	if got := gate.awaits.Load(); got != 3 {
		t.Errorf("gate.Await calls = %d; want 3 (one before each of the 3 attempts)", got)
	}
	// 2 classified transients ⇒ 2 Trips.
	if got := gate.trips.Load(); got != 2 {
		t.Errorf("gate.Trip calls = %d; want 2 (one per classified transient)", got)
	}
	// Item 143: this path classifies a SOURCE-side error and observes the
	// target not at all, so it has nothing to claim. The comment it replaced
	// asserted the opposite — "the READ-side face of a target storage-grow
	// stall" — as a fact, via a two-step inference nothing here can check.
	if got := gate.LastEvidence(); got != ir.GrowEvidenceNone {
		t.Errorf(
			"a source-read drop tripped the gate claiming %s; it must claim %s — nothing on this path "+
				"observed the target", got, ir.GrowEvidenceNone,
		)
	}
}

// TestSourceReadGrowGate_NoTripOnTerminal pins that a NON-retriable
// source-read error never trips the gate (it returns terminal immediately),
// and that the first attempt still Awaited the gate.
func TestSourceReadGrowGate_NoTripOnTerminal(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	gate := &recordingGate{}
	decodeErr := errPlain("mysql: column \"j\": decode json: invalid")
	attempt := func(_ context.Context, _ ir.RowReader, _ bool) error { return decodeErr }
	freshReader := func(_ context.Context) (ir.RowReader, func(), error) {
		return noopRowReader{}, func() {}, nil
	}

	err := copyTableWithSourceReadRetry(context.Background(), "t",
		resumeTruncateRestart, noopRowReader{}, false, attempt, freshReader,
		func(context.Context) error { return nil }, gate)
	if err == nil {
		t.Fatal("expected the terminal decode error")
	}
	if got := gate.trips.Load(); got != 0 {
		t.Errorf("gate.Trip calls = %d; want 0 (terminal error must not trip the pause)", got)
	}
	if got := gate.awaits.Load(); got != 1 {
		t.Errorf("gate.Await calls = %d; want 1 (first attempt only — terminal, no retry)", got)
	}
}

// errPlain is a non-retriable error (does NOT implement ir.RetriableError).
type errPlain string

func (e errPlain) Error() string { return string(e) }
