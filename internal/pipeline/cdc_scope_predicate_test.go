// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// recordingScopeReader records the predicate the streamer wires (Bug 246).
type recordingScopeReader struct {
	ir.CDCReader // nil embed — only the setter is exercised
	allowed      func(schema, table string) bool
}

func (r *recordingScopeReader) SetCDCScopePredicate(allowed func(schema, table string) bool) {
	r.allowed = allowed
}

// TestWireCDCScopePredicate pins the Bug 246 wiring closure: the predicate
// the streamer hands the reader must agree with the dispatch filter —
// operator filter plus live-adds — so the reader's XA policy check can
// never refuse a table the sync excludes (the v0.123.0 cycle's broken
// working configuration) nor exempt one the sync replicates. The behaviour
// half (an exempted XA row streams past the refusal; an included one still
// refuses) is pinned engine-side in cdc_reader_xa_test.go; this closes the
// closure-semantics half. The four call sites (cold start, warm resume,
// both multidb paths) are enumerated in the fix commit alongside the
// poll-interval setter they mirror.
func TestWireCDCScopePredicate(t *testing.T) {
	s := &Streamer{}
	filter, err := migcore.NewTableFilter(nil, []string{"orders"}) // --exclude-table=orders
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	s.Filter = filter

	r := &recordingScopeReader{}
	s.wireCDCScopePredicate(r)
	if r.allowed == nil {
		t.Fatal("wireCDCScopePredicate did not call the setter on a reader that implements it")
	}
	if r.allowed("app", "orders") {
		t.Fatal("the predicate allows a table the operator excluded — the XA refusal would fire for a " +
			"filtered-out table, the exact Bug 246 shape")
	}
	if !r.allowed("app", "users") {
		t.Fatal("the predicate excludes a table the sync replicates — the XA refusal would silently stop " +
			"guarding it")
	}

	// Live-adds join the scope once the sidecars publish the filter (the
	// dispatch filter and the reader-side predicate must agree here too —
	// a live-added table under XA must refuse, not stream).
	live := &liveAddedFilter{}
	live.Set([]string{"orders"})
	s.liveFilterRef.Store(live)
	if !r.allowed("app", "orders") {
		t.Fatal("a live-ADDED table must be in scope for the reader-side policy check, exactly as it is " +
			"for the dispatch filter")
	}
}
