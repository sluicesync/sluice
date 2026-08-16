// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSkipWarnTracker_OncePerTable pins the WARN de-duplication
// contract (audit C-11): the first sighting of a qualified table
// reports true (WARN fires), every later sighting false — per table,
// independently.
func TestSkipWarnTracker_OncePerTable(t *testing.T) {
	var tr SkipWarnTracker
	if !tr.FirstSighting("app.orders") {
		t.Fatal("first sighting of app.orders = false; want true")
	}
	if tr.FirstSighting("app.orders") {
		t.Fatal("second sighting of app.orders = true; want false")
	}
	if !tr.FirstSighting("app.invoices") {
		t.Fatal("first sighting of app.invoices = false; want true (tables track independently)")
	}
	// Case matters: qualified names are byte-exact keys, matching the
	// utf8mb4_bin / byte-exact identifier discipline of the durable
	// ledger itself.
	if !tr.FirstSighting("app.Orders") {
		t.Fatal("first sighting of app.Orders = false; want true (byte-exact keying)")
	}
}

// TestSkipWarnTracker_ConcurrentFirstSightingIsExactlyOne drives W
// goroutines at the same table — the ADR-0104/0105 concurrent-lane
// shape — and asserts exactly one wins the WARN.
func TestSkipWarnTracker_ConcurrentFirstSightingIsExactlyOne(t *testing.T) {
	var tr SkipWarnTracker
	const workers = 16
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		firsts int
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tr.FirstSighting("app.orders") {
				mu.Lock()
				firsts++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firsts != 1 {
		t.Fatalf("concurrent FirstSighting reported %d firsts; want exactly 1", firsts)
	}
}

// TestSkipLedgerAccumulator_CoalescesToOneUpsert is the H-4 coalescing pin
// (the "counting hook" the design asks for, deterministic and timing-free):
// N recorded skips of ONE table become exactly ONE upsert call carrying the
// summed count and the FIRST / LAST position tokens verbatim — fewer ledger
// writes than events, the whole point of the fix.
func TestSkipLedgerAccumulator_CoalescesToOneUpsert(t *testing.T) {
	var acc SkipLedgerAccumulator
	const n = 100
	for i := range n {
		tok := "pos-mid"
		switch i {
		case 0:
			tok = "pos-first"
		case n - 1:
			tok = "pos-last"
		}
		acc.Record("stream-A", "app.orders", tok)
	}

	var (
		calls int
		got   SkipLedgerEntry
		key   SkipLedgerKey
	)
	err := acc.Flush(context.Background(), func(_ context.Context, k SkipLedgerKey, e SkipLedgerEntry) error {
		calls++
		key = k
		got = e
		return nil
	})
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if calls != 1 {
		t.Fatalf("upsert called %d times for %d skips of one table; want exactly 1 (coalesced)", calls, n)
	}
	if key != (SkipLedgerKey{StreamID: "stream-A", Table: "app.orders"}) {
		t.Fatalf("key = %+v; want {stream-A app.orders}", key)
	}
	if got.Count != n {
		t.Errorf("Count = %d; want %d", got.Count, n)
	}
	if got.FirstPos != "pos-first" || got.LastPos != "pos-last" {
		t.Errorf("first/last = %q/%q; want pos-first/pos-last (verbatim first & last)", got.FirstPos, got.LastPos)
	}

	// The accumulator is drained: a second flush with no new skips is a no-op
	// (zero upserts), so a boundary that saw no skips never touches the DB.
	calls = 0
	if err := acc.Flush(context.Background(), func(_ context.Context, _ SkipLedgerKey, _ SkipLedgerEntry) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if calls != 0 {
		t.Fatalf("drained accumulator issued %d upserts; want 0", calls)
	}
}

// TestSkipLedgerAccumulator_PerTableRowAndEmpty pins that distinct tables get
// distinct upserts (one row each) and that an empty accumulator flushes
// without any DB call.
func TestSkipLedgerAccumulator_PerTableRowAndEmpty(t *testing.T) {
	var acc SkipLedgerAccumulator

	// Empty flush: no upserts.
	empties := 0
	if err := acc.Flush(context.Background(), func(_ context.Context, _ SkipLedgerKey, _ SkipLedgerEntry) error {
		empties++
		return nil
	}); err != nil {
		t.Fatalf("empty Flush: %v", err)
	}
	if empties != 0 {
		t.Fatalf("empty accumulator issued %d upserts; want 0", empties)
	}

	acc.Record("s", "a.one", "p1")
	acc.Record("s", "a.two", "p2")
	acc.Record("s", "a.one", "p3")

	counts := map[SkipLedgerKey]int64{}
	if err := acc.Flush(context.Background(), func(_ context.Context, k SkipLedgerKey, e SkipLedgerEntry) error {
		counts[k] = e.Count
		return nil
	}); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(counts) != 2 {
		t.Fatalf("distinct tables = %d; want 2", len(counts))
	}
	if counts[SkipLedgerKey{"s", "a.one"}] != 2 || counts[SkipLedgerKey{"s", "a.two"}] != 1 {
		t.Fatalf("per-table counts = %+v; want a.one=2, a.two=1", counts)
	}
}

// TestSkipLedgerAccumulator_FlushErrorDropsRatherThanReMerges pins the
// at-least-once drop-on-error contract: a failed flush does NOT re-merge the
// drained entries (which would inflate the over-count), because the caller
// flushes before the covering position is durable and the interrupted work
// re-delivers on resume. After a failing flush, the accumulator is empty; the
// dropped count comes back only via re-delivery (simulated here by a fresh
// Record), never a lingering ghost.
func TestSkipLedgerAccumulator_FlushErrorDropsRatherThanReMerges(t *testing.T) {
	var acc SkipLedgerAccumulator
	acc.Record("s", "a.t", "p1")
	acc.Record("s", "a.t", "p2")

	boom := errors.New("control-write failed")
	if err := acc.Flush(context.Background(), func(_ context.Context, _ SkipLedgerKey, _ SkipLedgerEntry) error {
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Flush error = %v; want the upsert error propagated", err)
	}

	// The failed flush dropped the drained entries — a subsequent flush sees
	// nothing (no re-merge). The count is recovered by re-delivery, not by a
	// retained accumulator.
	var seen int64 = -1
	if err := acc.Flush(context.Background(), func(_ context.Context, _ SkipLedgerKey, e SkipLedgerEntry) error {
		seen = e.Count
		return nil
	}); err != nil {
		t.Fatalf("post-error Flush: %v", err)
	}
	if seen != -1 {
		t.Fatalf("post-error Flush saw a re-merged entry (count %d); want the failed batch dropped", seen)
	}
}

// TestSkipLedgerAccumulator_ConcurrentRecordAndFlush drives W recorders
// against the accumulator while a flusher periodically drains it — the
// ADR-0104/0105 shape (lanes Record, coordinator Flushes) — and asserts every
// recorded skip is accounted for exactly once across the flushes. Under
// CI's -race it also proves the swap-under-lock is data-race-free.
func TestSkipLedgerAccumulator_ConcurrentRecordAndFlush(t *testing.T) {
	var acc SkipLedgerAccumulator
	const (
		workers        = 8
		perWorker      = 500
		wantTotalSkips = workers * perWorker
	)

	var flushed atomic.Int64
	drain := func() {
		_ = acc.Flush(context.Background(), func(_ context.Context, _ SkipLedgerKey, e SkipLedgerEntry) error {
			flushed.Add(e.Count)
			return nil
		})
	}

	// Interleaved flusher (the coordinator) racing the recorders, until the
	// recorders are all done.
	stop := make(chan struct{})
	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		for {
			select {
			case <-stop:
				return
			default:
				drain()
			}
		}
	}()

	var recorders sync.WaitGroup
	for range workers {
		recorders.Add(1)
		go func() {
			defer recorders.Done()
			for range perWorker {
				acc.Record("s", "a.t", "p")
			}
		}()
	}
	recorders.Wait() // all skips recorded
	close(stop)      // stop the interleaved flusher
	<-flusherDone
	drain() // final authoritative drain of anything recorded after its last pass

	if got := flushed.Load(); got != wantTotalSkips {
		t.Fatalf("flushed total = %d; want %d (every recorded skip accounted for exactly once)", got, wantTotalSkips)
	}
}
