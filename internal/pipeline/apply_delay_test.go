// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// recordingSleep is a deterministic [sleepFunc] for the delayed-replica gate
// tests: it records every requested wait and returns "completed" (true) without
// real time passing, so the held change is forwarded immediately in test time
// while the assertion still sees the requested hold duration.
type recordingSleep struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (r *recordingSleep) fn(_ context.Context, d time.Duration) bool {
	r.mu.Lock()
	r.waits = append(r.waits, d)
	r.mu.Unlock()
	return true
}

func (r *recordingSleep) calls() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]time.Duration, len(r.waits))
	copy(out, r.waits)
	return out
}

// delayClock returns a now() pinned to t.
func delayClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// TestDelayChanges_FutureCommitHeld pins that a change whose commitTs+delay is
// in the future is HELD (the gate sleeps for the remaining wait) before being
// forwarded.
func TestDelayChanges_FutureCommitHeld(t *testing.T) {
	const delay = 10 * time.Minute
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	// Committed 2 minutes ago ⇒ remaining hold = delay − 2m = 8m.
	commit := now.Add(-2 * time.Minute)

	in := make(chan ir.Change, 1)
	in <- ir.Insert{Schema: "s", Table: "t", CommitTime: commit}
	close(in)

	sl := &recordingSleep{}
	out := delayChanges(context.Background(), in, delay, delayClock(now), sl.fn)

	var n int
	for range out {
		n++
	}
	if n != 1 {
		t.Fatalf("forwarded %d changes; want 1", n)
	}
	waits := sl.calls()
	if len(waits) != 1 {
		t.Fatalf("sleep called %d times; want 1 (the held change)", len(waits))
	}
	if want := 8 * time.Minute; waits[0] != want {
		t.Fatalf("held for %v; want %v (delay − age)", waits[0], want)
	}
}

// TestDelayChanges_PastCommitImmediate pins that a change whose commitTs+delay
// is already past is forwarded immediately (no sleep).
func TestDelayChanges_PastCommitImmediate(t *testing.T) {
	const delay = time.Minute
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	commit := now.Add(-5 * time.Minute) // long past commitTs+delay

	in := make(chan ir.Change, 1)
	in <- ir.Insert{Schema: "s", Table: "t", CommitTime: commit}
	close(in)

	sl := &recordingSleep{}
	out := delayChanges(context.Background(), in, delay, delayClock(now), sl.fn)
	var n int
	for range out {
		n++
	}
	if n != 1 {
		t.Fatalf("forwarded %d changes; want 1", n)
	}
	if len(sl.calls()) != 0 {
		t.Fatalf("sleep called %d times; want 0 (release already past)", len(sl.calls()))
	}
}

// TestDelayChanges_ZeroCommitImmediate pins that a change carrying no source
// commit time (e.g. a SchemaSnapshot, or a source/path that supplies none) is
// forwarded immediately — there is no basis to delay it.
func TestDelayChanges_ZeroCommitImmediate(t *testing.T) {
	const delay = time.Hour
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)

	in := make(chan ir.Change, 2)
	in <- ir.SchemaSnapshot{Schema: "s", Table: "t"} // SourceCommitTime() == zero
	in <- ir.TxCommit{}                              // zero commit time too
	close(in)

	sl := &recordingSleep{}
	out := delayChanges(context.Background(), in, delay, delayClock(now), sl.fn)
	var n int
	for range out {
		n++
	}
	if n != 2 {
		t.Fatalf("forwarded %d changes; want 2", n)
	}
	if len(sl.calls()) != 0 {
		t.Fatalf("sleep called %d times; want 0 (zero commit time)", len(sl.calls()))
	}
}

// TestDelayChanges_TransactionReleasesTogether pins ADR-0121 §4: every row
// event in a source transaction carries that transaction's commit timestamp,
// so the whole transaction (TxBegin → rows → TxCommit) is gated to ONE release
// instant and forwarded IN ORDER — never split across the delay.
func TestDelayChanges_TransactionReleasesTogether(t *testing.T) {
	const delay = 5 * time.Minute
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	commit := now.Add(-1 * time.Minute) // remaining hold = 4m for every event

	tx := []ir.Change{
		ir.TxBegin{CommitTime: commit},
		ir.Insert{Schema: "s", Table: "t", Row: ir.Row{"id": 1}, CommitTime: commit},
		ir.Insert{Schema: "s", Table: "t", Row: ir.Row{"id": 2}, CommitTime: commit},
		ir.TxCommit{CommitTime: commit},
	}
	in := make(chan ir.Change, len(tx))
	for _, c := range tx {
		in <- c
	}
	close(in)

	sl := &recordingSleep{}
	out := delayChanges(context.Background(), in, delay, delayClock(now), sl.fn)
	var got []ir.Change
	for c := range out {
		got = append(got, c)
	}
	if len(got) != len(tx) {
		t.Fatalf("forwarded %d changes; want %d (whole tx)", len(got), len(tx))
	}
	// Order preserved: Begin, two inserts, Commit.
	if _, ok := got[0].(ir.TxBegin); !ok {
		t.Errorf("event 0 = %T; want TxBegin", got[0])
	}
	if _, ok := got[3].(ir.TxCommit); !ok {
		t.Errorf("event 3 = %T; want TxCommit", got[3])
	}
	// Every event with a non-zero commit time is gated to the SAME 4m release
	// (the tx is never split): all four share the commit time, so all four
	// request the same hold.
	for i, w := range sl.calls() {
		if want := 4 * time.Minute; w != want {
			t.Errorf("event %d held for %v; want %v (single release instant)", i, w, want)
		}
	}
}

// TestDelayChanges_CtxCancelMidHoldDropsChange is the RESUME-SAFETY pin
// (ADR-0121 §2): a change held when the context cancels is NOT forwarded — it
// stays un-applied, so the downstream applier never advances the position past
// it and resume re-reads it. A held change that leaked downstream on cancel
// would be the silent-loss-on-crash class this design exists to prevent.
func TestDelayChanges_CtxCancelMidHoldDropsChange(t *testing.T) {
	const delay = time.Hour
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	commit := now // commitTs+delay is an hour out ⇒ the change is held

	in := make(chan ir.Change, 1)
	in <- ir.Insert{Schema: "s", Table: "t", CommitTime: commit}
	// Do NOT close in: the gate is parked in the hold for the first change.

	ctx, cancel := context.WithCancel(context.Background())

	// A sleep that blocks until ctx is cancelled, then reports "not completed"
	// (false) — exactly what realSleep does on cancel mid-hold.
	blockingSleep := func(c context.Context, _ time.Duration) bool {
		<-c.Done()
		return false
	}
	out := delayChanges(ctx, in, delay, delayClock(now), blockingSleep)

	// Cancel while the gate holds the first change.
	cancel()

	// The output channel must close WITHOUT ever forwarding the held change.
	for c := range out {
		t.Fatalf("a held change leaked downstream on ctx-cancel: %T — resume-safety violated", c)
	}
}

// TestDelayChanges_DrainsThenClosesOnSourceClose pins that when the input
// channel closes (source EOF), the gate drains the in-flight (already-eligible)
// changes and then closes its output — no goroutine leak, no lost change.
func TestDelayChanges_DrainsThenClosesOnSourceClose(t *testing.T) {
	const delay = time.Minute
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	commit := now.Add(-10 * time.Minute) // all eligible immediately

	in := make(chan ir.Change, 3)
	in <- ir.Insert{Schema: "s", Table: "t", Row: ir.Row{"id": 1}, CommitTime: commit}
	in <- ir.Insert{Schema: "s", Table: "t", Row: ir.Row{"id": 2}, CommitTime: commit}
	in <- ir.Insert{Schema: "s", Table: "t", Row: ir.Row{"id": 3}, CommitTime: commit}
	close(in)

	out := delayChanges(context.Background(), in, delay, delayClock(now), realSleep)
	var n int
	for range out {
		n++
	}
	if n != 3 {
		t.Fatalf("forwarded %d changes; want 3", n)
	}
}
