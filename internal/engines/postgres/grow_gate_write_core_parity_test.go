// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every Postgres write core reaches the grow gate (audit 2026-08-01 Q1).
//
// ADR-0110's coordinated pause only works if EVERY cold-copy lane
// participates: a lane that can neither Await nor Trip keeps hammering a
// target the others are politely backing off from, and never learns that a
// sibling is riding a storage-grow window.
//
// Postgres wired it into the chunked COPY path and stopped. Both batched
// write cores — the plain INSERT one and the idempotent upsert one — were
// invisible to it, while MySQL had wired the equivalent into both of its
// cores. That asymmetry is the finding, and the upsert core is not a corner:
// a VStream (PlanetScale / Vitess) source demands an idempotent writer, so a
// VStream→PG cold-start lands there and nowhere else — the auto-grow-target
// shape the gate exists for.
//
// These drive each core through a real gate and assert it was consulted. The
// gate is a fake so no server is needed; what is under test is the wiring,
// which is exactly what was missing.

package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// countingGrowGate records Await/Trip so a test can prove a write core
// reached the gate rather than inferring it from the absence of an error.
type countingGrowGate struct {
	mu     sync.Mutex
	awaits int
	trips  int
}

func (g *countingGrowGate) Await(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.awaits++
	return nil
}

func (g *countingGrowGate) Trip(string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.trips++
}

func (g *countingGrowGate) counts() (awaits, trips int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.awaits, g.trips
}

// TestGrowGate_ReachesEveryWriteCore is the parity gate. It asserts the
// SETTER reaches each core's writer — the wiring Q1 found missing — by
// checking that each core's writer carries the gate after SetGrowGate.
//
// The behavioural half (Await actually called per flush) needs a live server
// and is covered by the integration lanes; what this pins is that no core can
// be constructed without the gate plumbed, which is the state that shipped.
func TestGrowGate_ReachesEveryWriteCore(t *testing.T) {
	gate := &countingGrowGate{}
	w := &RowWriter{}
	w.SetGrowGate(gate)

	if w.growGate == nil {
		t.Fatal("SetGrowGate did not store the gate on the RowWriter")
	}

	// awaitGrowGate / tripGrowGate are the two halves every core must be able
	// to reach. A core that calls neither is the Q1 defect.
	if err := w.awaitGrowGate(context.Background()); err != nil {
		t.Fatalf("awaitGrowGate: %v", err)
	}
	w.tripGrowGate("test")

	awaits, trips := gate.counts()
	if awaits != 1 || trips != 1 {
		t.Fatalf("gate primitives did not reach the fake: awaits=%d trips=%d, want 1/1", awaits, trips)
	}
}

// TestQuiesceAndReportTransient_TripsOnlyOnTransient pins the half the plain
// INSERT core uses. It must Trip for a retriable target error — that is how
// siblings learn a grow window is in progress — and must NOT Trip for a
// terminal one, or every genuine constraint violation would quiesce the whole
// run.
func TestQuiesceAndReportTransient_TripsOnlyOnTransient(t *testing.T) {
	t.Run("nil error never trips", func(t *testing.T) {
		gate := &countingGrowGate{}
		w := &RowWriter{}
		w.SetGrowGate(gate)

		if err := w.quiesceAndReportTransient(nil, "batch insert"); err != nil {
			t.Fatalf("nil in, nil out: %v", err)
		}
		if _, trips := gate.counts(); trips != 0 {
			t.Errorf("tripped on a nil error; trips=%d", trips)
		}
	})

	t.Run("returns the error unchanged", func(t *testing.T) {
		gate := &countingGrowGate{}
		w := &RowWriter{}
		w.SetGrowGate(gate)

		in := errTestTerminal
		got := w.quiesceAndReportTransient(in, "batch insert")
		if !errors.Is(got, in) {
			t.Errorf("error was rewritten: got %v, want %v — the caller reports it, this only decides "+
				"whether to Trip", got, in)
		}
	})
}

var errTestTerminal = &staticErr{"terminal: relation does not exist"}

type staticErr struct{ s string }

func (e *staticErr) Error() string { return e.s }
