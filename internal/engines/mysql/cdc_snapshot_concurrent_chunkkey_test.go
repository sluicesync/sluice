// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestWorkItemCursorKey pins the per-work-item cursor-key disambiguation
// (ADR-0119 Decision 4): a whole-table item keys on the bare table name; a chunk
// keys on "table#chunkIndex"; concurrent chunks of one table get DISTINCT keys,
// so they never alias on the shared in-memory cursor map under W readers.
func TestWorkItemCursorKey(t *testing.T) {
	if got := workItemCursorKey("orders", -1); got != "orders" {
		t.Errorf("whole-table key = %q; want %q", got, "orders")
	}
	if got := workItemCursorKey("orders", 0); got != "orders#0" {
		t.Errorf("chunk-0 key = %q; want %q", got, "orders#0")
	}

	// Distinctness across chunks of the SAME table (the collision the shared
	// cursor map must avoid).
	seen := map[string]int{}
	const m = 8
	for i := 0; i < m; i++ {
		seen[workItemCursorKey("orders", i)]++
	}
	if len(seen) != m {
		t.Fatalf("%d chunks produced %d distinct keys; want %d (a collision aliases two chunks' cursors)", m, len(seen), m)
	}

	// A chunk key of one table never equals the WHOLE-table key of a
	// differently-named table.
	if workItemCursorKey("a", 1) == workItemCursorKey("b", -1) {
		t.Error("chunk key of \"a\" aliased the whole-table key of \"b\"")
	}
}
