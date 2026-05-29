// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// closedDB opens a mysql-backed *sql.DB on a throwaway DSN and closes
// it immediately. database/sql is lazy — no connection is dialled by
// Open — so the returned handle never touches the network; a subsequent
// BeginTx fails fast with "sql: database is closed". That lets a unit
// test drive applyOneBatch up to (and only up to) the BeginTx call
// without a real MySQL, which is exactly the surface the Fix-A
// latency-timing assertion needs: the begin-tx attempt is the first
// thing that happens after the apply clock starts.
func closedDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", "root@tcp(unit-test-never-dialled:3306)/db")
	if err != nil {
		t.Fatalf("sql.Open(mysql): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	return db
}

// TestChangeApplier_ApplyBatch_LatencyExcludesWaitForWork pins roadmap
// item 18 Fix A: the latency fed to the AIMD controller via
// ObserveBatch must reflect APPLY WORK only, not the time the applier
// spends blocked in the pre-tx wait loop waiting for the first change.
//
// Pre-fix shape: batchStart was set at the top of applyOneBatch, before
// the wait loop, so a sparse stream's first batch reported a latency
// dominated by the blocked wait. The controller read that as "apply is
// catastrophically slow" and collapsed batch size 1000→1.
//
// Test mechanism: feed the first change onto the channel only after an
// injected delay. The applier blocks in the wait loop for that whole
// delay, THEN starts the apply clock, THEN calls BeginTx — which fails
// instantly against a closed DB. ObserveBatch fires (n==0, err!=nil),
// and the observed latency must be well under the injected wait. With
// the bug, it would be >= the injected wait.
func TestChangeApplier_ApplyBatch_LatencyExcludesWaitForWork(t *testing.T) {
	const injectedWait = 600 * time.Millisecond

	o := &fakeObserver{}
	a := &ChangeApplier{db: closedDB(t), batchObserver: o}

	ch := make(chan ir.Change)
	go func() {
		time.Sleep(injectedWait)
		ch <- ir.Insert{
			Position: ir.Position{Token: "p1"},
			Schema:   "target_db",
			Table:    "users",
			Row:      ir.Row{"id": int64(1)},
		}
	}()

	// applyOneBatch blocks ~injectedWait in the pre-tx wait loop, then
	// BeginTx fails instantly against the closed DB. Returns n==0 with a
	// non-nil err; the defer feeds ObserveBatch the apply-only latency.
	n, _, _, err := a.applyOneBatch(context.Background(), "unit-stream", ch, 10)
	if err == nil {
		t.Fatal("applyOneBatch: expected begin-tx error against closed DB; got nil")
	}
	if n != 0 {
		t.Fatalf("applyOneBatch n = %d; want 0 (begin-tx failed before any dispatch)", n)
	}
	if o.calls != 1 {
		t.Fatalf("ObserveBatch calls = %d; want 1 (failure path must observe)", o.calls)
	}

	// The load-bearing assertion: observed latency must exclude the
	// blocked wait. A comfortable margin (half the injected wait) keeps
	// the assertion robust against CI scheduling jitter on the begin-tx
	// path while still failing loudly if the wait leaks into the timing.
	if o.lastLat >= injectedWait/2 {
		t.Errorf("observed latency = %v; want < %v (apply-only timing must exclude the %v wait-for-first-change)",
			o.lastLat, injectedWait/2, injectedWait)
	}
}

// TestChangeApplier_ApplyBatch_PreTxCancelDoesNotObserve pins the
// IsZero guard added alongside Fix A: a ctx cancellation that fires
// while the applier is still blocked in the pre-tx wait loop (before
// the apply clock starts) must NOT feed ObserveBatch a bogus latency.
//
// Without the guard, the ctx.Done path in the wait loop returns n==0
// with a non-nil err and a zero-value batchStart, so time.Since would
// compute a multi-decade "latency" and poison the controller's window.
func TestChangeApplier_ApplyBatch_PreTxCancelDoesNotObserve(t *testing.T) {
	o := &fakeObserver{}
	a := &ChangeApplier{db: closedDB(t), batchObserver: o}

	ctx, cancel := context.WithCancel(context.Background())
	// Never-fed, never-closed channel: the applier sits in the pre-tx
	// wait loop until ctx is cancelled.
	ch := make(chan ir.Change)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	n, _, _, err := a.applyOneBatch(ctx, "unit-stream", ch, 10)
	if err == nil {
		t.Fatal("applyOneBatch: expected ctx error; got nil")
	}
	if n != 0 {
		t.Fatalf("applyOneBatch n = %d; want 0", n)
	}
	if o.calls != 0 {
		t.Fatalf("ObserveBatch calls = %d; want 0 (pre-tx-wait cancellation must not observe — IsZero guard)", o.calls)
	}
}

// TestDefaultIdleFlushPeriod_IsShortGrace pins roadmap item 18 Fix B at
// the unit level: the idle-flush grace is the short ~100ms value, not
// the pre-fix 5s. The behavioural proof (a partial batch actually
// commits within the grace) is an integration test
// (TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial), since
// committing needs a real target; this guards the constant itself so a
// regression that re-bumps it to seconds fails here cheaply.
func TestDefaultIdleFlushPeriod_IsShortGrace(t *testing.T) {
	if defaultIdleFlushPeriod > 500*time.Millisecond {
		t.Errorf("defaultIdleFlushPeriod = %v; want a short grace (<= 500ms) so a drained/sparse stream flushes promptly (item 18 Fix B)",
			defaultIdleFlushPeriod)
	}
	if defaultIdleFlushPeriod <= 0 {
		t.Errorf("defaultIdleFlushPeriod = %v; want > 0 (the idle flush must still fire)", defaultIdleFlushPeriod)
	}
}

// fakeProvider is a minimal [ir.BatchSizeProvider] for testing the
// applier's optional-surface wiring (ADR-0052) without standing up an
// AIMD controller.
type fakeProvider struct {
	next     int
	hits     int
	hintRows int
	hintByte int64
	hintCap  int64
	hintHits int
}

func (f *fakeProvider) NextBatchSize() int {
	f.hits++
	return f.next
}

func (f *fakeProvider) NoteByteCapDominant(_ context.Context, rows int, bytes, byteCap int64) {
	f.hintHits++
	f.hintRows = rows
	f.hintByte = bytes
	f.hintCap = byteCap
}

// fakeObserver is a minimal [ir.BatchObserver] for testing the
// applier's optional-surface wiring.
type fakeObserver struct {
	calls    int
	lastErr  error
	lastRows int
	lastLat  time.Duration
}

func (f *fakeObserver) ObserveBatch(_ context.Context, latency time.Duration, rows int, err error) {
	f.calls++
	f.lastLat = latency
	f.lastRows = rows
	f.lastErr = err
}

func TestChangeApplier_SetBatchSizeProvider(t *testing.T) {
	// Unit-level: just confirm the setter stores the value and the
	// applier exposes the same ir.BatchSizeProviderSetter shape the
	// streamer probes for.
	a := &ChangeApplier{}
	var setter ir.BatchSizeProviderSetter = a
	p := &fakeProvider{next: 42}
	setter.SetBatchSizeProvider(p)
	if a.batchSizeProvider == nil {
		t.Fatalf("SetBatchSizeProvider: stored value is nil")
	}
	if got := a.batchSizeProvider.NextBatchSize(); got != 42 {
		t.Fatalf("provider NextBatchSize via applier field = %d; want 42", got)
	}
	// Nil clears the wiring.
	setter.SetBatchSizeProvider(nil)
	if a.batchSizeProvider != nil {
		t.Fatalf("SetBatchSizeProvider(nil): expected to clear; got %v", a.batchSizeProvider)
	}
}

func TestChangeApplier_SetBatchObserver(t *testing.T) {
	a := &ChangeApplier{}
	var setter ir.BatchObserverSetter = a
	o := &fakeObserver{}
	setter.SetBatchObserver(o)
	if a.batchObserver == nil {
		t.Fatalf("SetBatchObserver: stored value is nil")
	}
	// Invoke through the applier field to confirm the same observer
	// is reachable.
	a.batchObserver.ObserveBatch(context.Background(), 5*time.Millisecond, 7, nil)
	if o.calls != 1 || o.lastRows != 7 || o.lastLat != 5*time.Millisecond {
		t.Fatalf("observer call = %+v; want calls=1 rows=7 lat=5ms", o)
	}
	setter.SetBatchObserver(nil)
	if a.batchObserver != nil {
		t.Fatalf("SetBatchObserver(nil): expected to clear")
	}
}

// TestChangeApplier_ImplementsAIMDInterfaces is a compile-time
// guarantee that the MySQL applier exposes both optional-surface
// setters the streamer probes for. A future refactor that drops
// either setter would break this assertion at build time, which is
// the loud-failure shape we want.
func TestChangeApplier_ImplementsAIMDInterfaces(_ *testing.T) {
	var _ ir.BatchSizeProviderSetter = (*ChangeApplier)(nil)
	var _ ir.BatchObserverSetter = (*ChangeApplier)(nil)
}
