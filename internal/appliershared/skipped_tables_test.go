// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"sync"
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
