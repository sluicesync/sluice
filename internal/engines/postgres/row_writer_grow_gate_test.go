// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ADR-0110 / roadmap item 38 grow-gate wiring pins (PG RowWriter side). These
// pin that the chunked-COPY retry loop (copyChunkWithRetry) Awaits the shared
// coordinated-pause gate before each attempt and Trips it on a CLASSIFIED
// grow-transient — using a recording fake ir.GrowGate so no testcontainers /
// no real coordinator timing is involved. The byte-identical value-fidelity
// of the chunked vs monolithic path is pinned by the integration test
// (row_writer_grow_gate_integration_test.go), which needs a real PG. The
// retry/replay convergence under an injected transient is pinned here against
// the loop directly + end-to-end against real PG in the integration test.

// recordingGrowGate is a fake ir.GrowGate that counts Await/Trip calls. Await
// is a pass-through (always open) so it never blocks — the FSM blocking
// behaviour is tested in the pipeline package.
type recordingGrowGate struct {
	awaits atomic.Int64
	trips  atomic.Int64
}

func (g *recordingGrowGate) Await(ctx context.Context) error {
	g.awaits.Add(1)
	return ctx.Err()
}

func (g *recordingGrowGate) Trip(string) { g.trips.Add(1) }

// withFastPGCopyBackoff shrinks the chunked-COPY reparent-retry envelope so
// the unit tests run fast, restoring the production values after the test.
func withFastPGCopyBackoff(t *testing.T) {
	t.Helper()
	base, capV, wall, attempts := pgCopyReparentBackoffBaseVar, pgCopyReparentBackoffCapVar, pgCopyReparentMaxWallVar, pgCopyReparentRetryAttemptsVar
	pgCopyReparentBackoffBaseVar = time.Millisecond
	pgCopyReparentBackoffCapVar = 2 * time.Millisecond
	pgCopyReparentMaxWallVar = 5 * time.Second
	pgCopyReparentRetryAttemptsVar = 50
	t.Cleanup(func() {
		pgCopyReparentBackoffBaseVar = base
		pgCopyReparentBackoffCapVar = capV
		pgCopyReparentMaxWallVar = wall
		pgCopyReparentRetryAttemptsVar = attempts
	})
}

func diskFull53100() error {
	return &pgconn.PgError{Code: "53100", Message: `could not extend file "base/16384/24576": No space left on device`}
}

// TestPGCopyChunkRetry_AwaitsBeforeEachAttempt pins that copyChunkWithRetry
// consults the gate before the first attempt AND before each retry attempt:
// one transient then success ⇒ at least 2 Awaits.
func TestPGCopyChunkRetry_AwaitsBeforeEachAttempt(t *testing.T) {
	withFastPGCopyBackoff(t)
	gate := &recordingGrowGate{}
	w := &RowWriter{growGate: gate}

	var calls int
	err := w.copyChunkWithRetry(context.Background(), "t", 3, func(context.Context) error {
		calls++
		if calls == 1 {
			return diskFull53100() // transient on the first attempt
		}
		return nil // replay succeeds
	})
	if err != nil {
		t.Fatalf("copyChunkWithRetry: %v", err)
	}
	if calls != 2 {
		t.Errorf("attempt calls = %d; want 2 (transient + replay)", calls)
	}
	if got := gate.awaits.Load(); got < 2 {
		t.Errorf("gate.Await calls = %d; want >= 2 (first attempt + before the retry)", got)
	}
}

// TestPGCopyChunkRetry_TripsOnClassifiedTransient pins that a classified
// grow-transient (53100) trips the gate so sibling lanes quiesce. One
// transient then success ⇒ exactly one Trip.
func TestPGCopyChunkRetry_TripsOnClassifiedTransient(t *testing.T) {
	withFastPGCopyBackoff(t)
	gate := &recordingGrowGate{}
	w := &RowWriter{growGate: gate}

	var calls int
	err := w.copyChunkWithRetry(context.Background(), "t", 1, func(context.Context) error {
		calls++
		if calls == 1 {
			return diskFull53100()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("copyChunkWithRetry: %v", err)
	}
	if got := gate.trips.Load(); got != 1 {
		t.Errorf("gate.Trip calls = %d; want 1 (one classified transient)", got)
	}
}

// TestPGCopyChunkRetry_NoTripNoRetryOnTerminal pins that a NON-retriable
// (terminal) error never trips the gate and is returned verbatim after the
// first attempt: a real unique violation (23505) is not a grow-transient.
func TestPGCopyChunkRetry_NoTripNoRetryOnTerminal(t *testing.T) {
	withFastPGCopyBackoff(t)
	gate := &recordingGrowGate{}
	w := &RowWriter{growGate: gate}

	terminal := &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"}
	var calls int
	err := w.copyChunkWithRetry(context.Background(), "t", 1, func(context.Context) error {
		calls++
		return terminal
	})
	if err == nil {
		t.Fatal("expected the terminal error to propagate")
	}
	if !errors.Is(err, terminal) {
		t.Errorf("terminal error should propagate verbatim; got %v", err)
	}
	if calls != 1 {
		t.Errorf("attempt calls = %d; want 1 (terminal, no retry)", calls)
	}
	if got := gate.trips.Load(); got != 0 {
		t.Errorf("gate.Trip calls = %d; want 0 (terminal must not trip the pause)", got)
	}
	if got := gate.awaits.Load(); got != 1 {
		t.Errorf("gate.Await calls = %d; want 1 (first attempt only)", got)
	}
}

// TestPGCopyChunkRetry_LoudOnExhaustion pins that a transient that NEVER
// clears surfaces a LOUD terminal error (naming the table + the window) once
// the wall-clock budget is exhausted — never silent, never infinite.
func TestPGCopyChunkRetry_LoudOnExhaustion(t *testing.T) {
	withFastPGCopyBackoff(t)
	pgCopyReparentMaxWallVar = 20 * time.Millisecond // exhaust quickly
	gate := &recordingGrowGate{}
	w := &RowWriter{growGate: gate}

	err := w.copyChunkWithRetry(context.Background(), "big_table", 42, func(context.Context) error {
		return diskFull53100() // never clears
	})
	if err == nil {
		t.Fatal("expected a loud terminal error on budget exhaustion")
	}
	if !errors.Is(err, diskFull53100()) && !errors.As(err, new(*pgconn.PgError)) {
		// errors.Is against a fresh value won't match (different pointer); the
		// As check confirms the underlying *pgconn.PgError is reachable.
		t.Errorf("exhaustion error should wrap the underlying transient; got %v", err)
	}
	for _, want := range []string{"big_table", "still failing", "42 rows"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("exhaustion error %q missing %q", err.Error(), want)
		}
	}
}

// TestPGCopyChunkRetry_AwaitCtxCancelHalts pins the no-deadlock contract: if
// the gate's Await returns ctx.Err() (the run unwound), the loop returns it
// promptly rather than proceeding to an attempt.
func TestPGCopyChunkRetry_AwaitCtxCancelHalts(t *testing.T) {
	withFastPGCopyBackoff(t)
	gate := &recordingGrowGate{}
	w := &RowWriter{growGate: gate}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Await returns ctx.Err() immediately

	var calls int
	err := w.copyChunkWithRetry(ctx, "t", 1, func(context.Context) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v; want context.Canceled (Await must halt before the attempt)", err)
	}
	if calls != 0 {
		t.Errorf("attempt calls = %d; want 0 (cancel halts before the first attempt)", calls)
	}
}

// TestPGCopyChunkRetry_NilGateInert pins that with no gate attached the loop
// still runs the attempt (and rides the per-lane retry budget) — the gate
// helpers are nil-safe. (writeViaCopy's nil-gate path stays monolithic; this
// pins the retry helper itself degrades cleanly.)
func TestPGCopyChunkRetry_NilGateInert(t *testing.T) {
	withFastPGCopyBackoff(t)
	w := &RowWriter{} // nil growGate

	var calls int
	err := w.copyChunkWithRetry(context.Background(), "t", 1, func(context.Context) error {
		calls++
		if calls == 1 {
			return diskFull53100()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("copyChunkWithRetry with nil gate: %v", err)
	}
	if calls != 2 {
		t.Errorf("attempt calls = %d; want 2 (transient + replay, gate inert)", calls)
	}
}

// TestSetGrowGate_RoutesChunkedVsMonolithic pins the engage-only-where-needed
// contract: SetGrowGate(non-nil) makes growGate non-nil (chunked path);
// SetGrowGate(nil) leaves it nil (monolithic path).
func TestSetGrowGate_RoutesChunkedVsMonolithic(t *testing.T) {
	w := &RowWriter{}
	if w.growGate != nil {
		t.Fatal("default writer must have a nil grow-gate (monolithic path)")
	}
	gate := &recordingGrowGate{}
	w.SetGrowGate(gate)
	if w.growGate == nil {
		t.Fatal("SetGrowGate(non-nil) must attach the gate (chunked path)")
	}
	w.SetGrowGate(nil)
	if w.growGate != nil {
		t.Fatal("SetGrowGate(nil) must detach the gate (back to monolithic path)")
	}
}
