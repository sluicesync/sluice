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

	"sluicesync.dev/sluice/internal/ir"
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

// TestWriteViaCopyChunked_TerminalErrorSurfacesLoudly pins the loud-fail-safe
// contract of chunk-everywhere (the design decision behind item 38): a
// NON-retriable (terminal) error on a chunk makes the whole table copy return a
// non-nil error — the run fails, the table is NOT reported complete (no silent
// partial-as-done). The operator recovers via the existing --reset/resume
// re-copy: a rolled-back chunk wrote nothing and earlier chunks are
// append-only into the fresh cold-copy table, so the re-copy is clean.
//
// It drives writeViaCopyChunked directly. copyChunkFaultHook is consulted
// BEFORE any conn is acquired, so a terminal fault never touches the (nil) db —
// no testcontainers needed. The "terminal on a LATER chunk (chunk 2 of N)"
// case requires earlier chunks to succeed against a real db (a nil-db unit
// harness can't let a chunk succeed without panicking on Conn()), so it is
// pinned in row_writer_grow_gate_integration_test.go
// (TestPGGrowGate_TerminalErrorMidChunkSurfacesLoudly_NotComplete). This unit
// pin proves the path-level loudness without a container.
func TestWriteViaCopyChunked_TerminalErrorSurfacesLoudly(t *testing.T) {
	withFastPGCopyBackoff(t)
	withSmallChunkUnit(t, 2) // 5 rows / 2 ⇒ several chunks; the first faults terminally

	gate := &recordingGrowGate{}
	w := &RowWriter{growGate: gate} // nil db: the hook short-circuits before Conn()

	// A terminal SQLSTATE: 42501 insufficient_privilege. classifyApplierError
	// returns it unchanged (not in the retriable set), so the retry loop must
	// surface it on the first attempt without retrying.
	terminal := &pgconn.PgError{Code: "42501", Message: "permission denied for table grow_chunk"}
	var attempts int
	w.copyChunkFaultHook = func(int) error {
		attempts++
		return terminal
	}

	rows := make(chan ir.Row, 5)
	for i := 0; i < 5; i++ {
		rows <- ir.Row{"id": int64(i)}
	}
	close(rows)

	tbl := &ir.Table{Name: "grow_chunk", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	err := w.writeViaCopyChunked(context.Background(), tbl, rows)
	if err == nil {
		t.Fatal("writeViaCopyChunked must return the terminal error LOUDLY (got nil — silent partial-as-complete)")
	}
	if !errors.Is(err, terminal) {
		t.Errorf("the terminal error must propagate verbatim; got %v", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d; want 1 (terminal must NOT be retried)", attempts)
	}
	if got := gate.trips.Load(); got != 0 {
		t.Errorf("gate.Trip = %d; want 0 (a terminal error must not trip the grow pause)", got)
	}
}

// withSmallChunkUnit shrinks the chunked-COPY row cap for a unit test so a
// small fixture spans several chunks, restoring it afterwards.
func withSmallChunkUnit(t *testing.T, rowsPerChunk int) {
	t.Helper()
	prev := pgCopyChunkRowsVar
	pgCopyChunkRowsVar = rowsPerChunk
	t.Cleanup(func() { pgCopyChunkRowsVar = prev })
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
