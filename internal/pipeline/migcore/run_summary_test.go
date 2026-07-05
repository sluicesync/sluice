// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import "testing"

// TestRunSummaryNilSafe pins the zero-plumbing contract: every method
// on a nil *RunSummary is a no-op, so orchestrator call sites record
// unconditionally and text mode pays nothing.
func TestRunSummaryNilSafe(t *testing.T) {
	var s *RunSummary
	s.RecordTable("app", "users")
	s.RecordTableRows("app", "users", 5)
	if got := s.Tables(); got != nil {
		t.Fatalf("nil summary Tables() = %v, want nil", got)
	}
}

// TestRunSummaryAccumulatesAndOrders pins the accumulation semantics
// (repeat RecordTableRows sums — chain restores re-apply a table per
// segment), the unknown-vs-zero distinction (nil Rows vs *0), and the
// first-recorded ordering.
func TestRunSummaryAccumulatesAndOrders(t *testing.T) {
	s := &RunSummary{}
	s.RecordTable("", "no_rows_known")
	s.RecordTableRows("app", "orders", 2)
	s.RecordTableRows("app", "orders", 3)
	s.RecordTableRows("", "empty", 0)
	s.RecordTable("", "no_rows_known") // repeat is a no-op

	got := s.Tables()
	if len(got) != 3 {
		t.Fatalf("Tables() len = %d, want 3 (%v)", len(got), got)
	}
	if got[0].Name != "no_rows_known" || got[0].Rows != nil {
		t.Errorf("got[0] = %+v; want no_rows_known with nil Rows", got[0])
	}
	if got[1].Schema != "app" || got[1].Name != "orders" || got[1].Rows == nil || *got[1].Rows != 5 {
		t.Errorf("got[1] = %+v; want app.orders rows=5", got[1])
	}
	if got[2].Name != "empty" || got[2].Rows == nil || *got[2].Rows != 0 {
		t.Errorf("got[2] = %+v; want empty with a REAL 0 (not unknown)", got[2])
	}

	// The returned Rows pointers are copies — mutating them must not
	// write through into the collector.
	*got[1].Rows = 99
	if again := s.Tables(); *again[1].Rows != 5 {
		t.Errorf("Tables() returned a live pointer; internal total mutated to %d", *again[1].Rows)
	}
}
