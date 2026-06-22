// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0110 grow-gate wiring pins (MySQL RowWriter side). These assert the
// flush hot path Awaits the shared coordinated-pause gate before each flush
// attempt and Trips it on a CLASSIFIED grow-transient — using a recording
// fake ir.GrowGate so no testcontainers / no real coordinator timing is
// involved. The coordinator FSM itself is pinned in the pipeline package's
// grow_gate_test.go; here we pin only that the writer reaches it.

// recordingGrowGate is a fake ir.GrowGate that counts Await/Trip calls.
// Await is a pass-through (always open) so it never blocks the flush loop —
// the FSM blocking behaviour is tested in the pipeline package.
type recordingGrowGate struct {
	awaits atomic.Int64
	trips  atomic.Int64
}

func (g *recordingGrowGate) Await(ctx context.Context) error {
	g.awaits.Add(1)
	return ctx.Err()
}

func (g *recordingGrowGate) Trip(string) { g.trips.Add(1) }

// TestColdCopyGrowGate_AwaitsBeforeEachFlushAttempt pins that the flush hot
// path consults the gate before the first attempt AND before each retry
// attempt: one transient then success ⇒ at least 2 Awaits (first attempt +
// the retry attempt).
func TestColdCopyGrowGate_AwaitsBeforeEachFlushAttempt(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{
		vttabletUnavailable(),
		nil, // retry succeeds
	}}
	db := newScriptDB(t, script)
	gate := &recordingGrowGate{}
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert, growGate: gate}

	if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	// First-attempt Await + the retry Await = at least 2.
	if got := gate.awaits.Load(); got < 2 {
		t.Errorf("gate.Await calls = %d; want >= 2 (top of first attempt + before the retry attempt)", got)
	}
}

// TestColdCopyGrowGate_TripsOnClassifiedTransient pins that a classified
// grow-transient trips the gate so sibling lanes quiesce. One transient
// then success ⇒ exactly one Trip.
func TestColdCopyGrowGate_TripsOnClassifiedTransient(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{
		vttabletUnavailable(),
		nil,
	}}
	db := newScriptDB(t, script)
	gate := &recordingGrowGate{}
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert, growGate: gate}

	if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if got := gate.trips.Load(); got != 1 {
		t.Errorf("gate.Trip calls = %d; want 1 (one classified transient)", got)
	}
}

// TestColdCopyGrowGate_NoTripOnTerminalError pins that a NON-retriable
// (terminal) error never trips the gate: the gate coordinates transients
// only; a real dup-key / value-fidelity failure must fail loudly without a
// coordinated pause. A first-attempt 1062 is terminal.
func TestColdCopyGrowGate_NoTripOnTerminalError(t *testing.T) {
	withFastReparentBackoff(t, 12)
	dup := &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '0' for key 't_pin.PRIMARY'"}
	script := &flushScript{execErrs: []error{dup}}
	db := newScriptDB(t, script)
	gate := &recordingGrowGate{}
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert, growGate: gate}

	err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(1))
	if err == nil {
		t.Fatal("expected a terminal error on first-attempt 1062")
	}
	if got := gate.trips.Load(); got != 0 {
		t.Errorf("gate.Trip calls = %d; want 0 (terminal error must not trip the coordinated pause)", got)
	}
	// The first attempt still Awaited (the gate gates every attempt), but no
	// retry attempt followed (terminal), so Await fired exactly once.
	if got := gate.awaits.Load(); got != 1 {
		t.Errorf("gate.Await calls = %d; want 1 (first attempt only — terminal, no retry)", got)
	}
}

// TestColdCopyGrowGate_NilGateIsInert pins the byte-for-byte pre-ADR-0110
// behaviour: a writer with no gate (the default) flushes exactly as before —
// no panic, the transient still rides the per-lane reparent-retry budget.
func TestColdCopyGrowGate_NilGateIsInert(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{
		vttabletUnavailable(),
		nil,
	}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert} // nil growGate

	if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRows with nil gate: %v", err)
	}
	if got := script.execCalls.Load(); got != 2 {
		t.Errorf("INSERT exec calls = %d; want 2 (transient + success, gate inert)", got)
	}
}

// TestColdCopyGrowGate_AwaitCtxCancelHalts pins the no-deadlock contract at
// the writer seam: if the gate's Await returns ctx.Err() (the run unwound),
// the flush returns it promptly rather than proceeding to exec. A gate that
// returns a cancelled-ctx error from Await models a closed gate whose run
// ctx was cancelled.
func TestColdCopyGrowGate_AwaitCtxCancelHalts(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{} // exec would succeed if reached
	db := newScriptDB(t, script)
	gate := &recordingGrowGate{}
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert, growGate: gate}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Await will return ctx.Err()

	err := w.WriteRows(ctx, pinReparentTable(), feedReparentRows(2))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteRows error = %v; want context.Canceled (Await must halt the flush on cancel)", err)
	}
}
