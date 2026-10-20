// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
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
	live.Set([]string{"orders", "audit"})
	s.liveFilterRef.Store(live)
	if r.allowed("app", "orders") {
		t.Fatal("a live-added table the operator's --exclude-table names is in scope; an explicit exclusion wins " +
			"(Bug 321 R6 A)")
	}
	if !r.allowed("app", "audit") {
		t.Fatal("a live-ADDED table must be in scope for the reader-side policy check, exactly as it is " +
			"for the dispatch filter")
	}
	// Include mode: a live-added table outside the --include-table list is in
	// scope — the staged-wave add-table case.
	inc, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Filter = inc
	if !r.allowed("app", "orders") {
		t.Fatal("a live-added table outside an --include-table list is out of the reader's scope")
	}
}

// TestWireCDCScopePredicate_PersistedLiveAddInScopeBeforeTheReaderOpens pins
// Bug 322 at the seam every reader-open site shares: once
// [Streamer.phaseReadLiveAddedScope] has run — the first thing
// phaseOpenChangeStream does, before any branch opens a reader — the reader's
// scope predicate already admits a table a previous run live-added. The apply
// sidecars, which used to create the filter, have NOT run here. Before the
// fix the predicate rejected the table in that gap and the MySQL binlog
// reader dropped its stopped-window changes at every restart, at exit 0.
// Engine-neutral by construction: it is the predicate every
// [ir.CDCScopePredicateSetter] reader is handed.
func TestWireCDCScopePredicate_PersistedLiveAddInScopeBeforeTheReaderOpens(t *testing.T) {
	inc, err := migcore.NewTableFilter([]string{"kd"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &Streamer{Filter: inc}
	app := &scopeDriftApplier{liveAdded: []string{"users"}}
	if err := s.phaseReadLiveAddedScope(context.Background(), app, "st"); err != nil {
		t.Fatalf("phaseReadLiveAddedScope: %v", err)
	}
	r := &recordingScopeReader{}
	s.wireCDCScopePredicate(r)
	if !r.allowed("app", "users") {
		t.Fatal("the reader's scope predicate rejects a persisted live-added table before the apply sidecars run — " +
			"the reader drops its stopped-window changes (Bug 322)")
	}
	if r.allowed("app", "other") {
		t.Fatal("the predicate admits a table neither the filter nor the live-added set names")
	}

	// A later attempt starts from the set it reads, not the previous
	// attempt's: an --abandon'ed add is out of scope at the next start.
	app.liveAdded = nil
	if err := s.phaseReadLiveAddedScope(context.Background(), app, "st"); err != nil {
		t.Fatalf("phaseReadLiveAddedScope (second attempt): %v", err)
	}
	if r.allowed("app", "users") {
		t.Fatal("a live-added table the stream no longer carries stays in the reader's scope from a previous attempt")
	}
}
