// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Audit B-9 pins: ADR-0108's cold-copy retry re-sends a byte-identical
// batch, and its safety argument is entirely about the 1062 collision that
// a committed-but-unacked prior attempt would produce. A table with nothing
// to collide on gets no 1062 and the re-send silently doubles the batch.
//
// The oracle these tests use is deliberately NOT the writer's own return
// value or bookkeeping: it is script.execCalls, the number of INSERTs the
// DRIVER actually saw. "The second INSERT never reached the wire" is the
// property that matters, and the writer cannot fake it.
//
// Which paths these reach: the plain batched-INSERT core
// (writeBatchedConn, via WriteRows and WriteRowsParallel) and the
// idempotent upsert core (writeBatchedIdempotentConn) — every caller of
// flushWithReparentRetry, which is where the gate lives. The LOAD DATA
// core is out of scope because it never retries at all (it returns a loud
// "CANNOT be resumed" terminal error), and the PG twin is pinned in the
// postgres package.

// keylessPinTable is pinReparentTable's shape with the primary key
// removed and no unique index — a truly keyless table, the shape the
// 1062 argument is inert for.
func keylessPinTable() *ir.Table {
	return &ir.Table{
		Name: "t_keyless",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
	}
}

// notNullUniquePinTable has no PRIMARY KEY but carries a UNIQUE index
// over a NOT NULL column — keyed enough for a re-send to collide, so the
// retry must still run.
func notNullUniquePinTable() *ir.Table {
	return &ir.Table{
		Name: "t_uniq",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}
}

// nullableUniquePinTable's only unique index is over a NULLABLE column.
// MySQL permits many rows with NULL there, so the index does NOT reliably
// collide — the same hazard as no key at all, and the reason
// TableReplayIdempotent excludes it.
func nullableUniquePinTable() *ir.Table {
	return &ir.Table{
		Name: "t_nullable_uniq",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}, Nullable: true},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}
}

// TestColdCopyReparentRetry_KeylessRefusesRatherThanResend is the B-9
// crux. A classified transient on the first attempt would, before this
// fix, be followed by a byte-identical re-send that a keyless table could
// not notice. The pin is that the second INSERT NEVER REACHES THE DRIVER.
func TestColdCopyReparentRetry_KeylessRefusesRatherThanResend(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{vttabletUnavailable()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	err := w.WriteRows(context.Background(), keylessPinTable(), feedReparentRows(3))
	if err == nil {
		t.Fatal("WriteRows on a keyless table after a transient: want a refusal, got nil " +
			"(the batch was silently re-sent — this is the B-9 silent-duplication path)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
		t.Errorf("refusal code = %v (coded=%v); want %s", ce, ok, sluicecode.CodeCopyRetryAmbiguousKeyless)
	}
	// The independent oracle: the driver saw the batch exactly ONCE.
	if got := script.execCalls.Load(); got != 1 {
		t.Errorf("INSERT exec calls = %d; want 1 — the whole point is that the ambiguous batch is not re-sent", got)
	}
	if !containsAll(err.Error(), "t_keyless", "PRIMARY KEY") {
		t.Errorf("refusal must name the table and the remedy; got: %v", err)
	}
}

// TestColdCopyReparentRetry_KeylessCleanCopyIsUnaffected is the
// false-refusal floor: the gate fires on the RETRY decision, not on the
// table. A keyless copy that hits no transient must be byte-for-byte the
// behaviour it always had.
func TestColdCopyReparentRetry_KeylessCleanCopyIsUnaffected(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	if err := w.WriteRows(context.Background(), keylessPinTable(), feedReparentRows(3)); err != nil {
		t.Fatalf("a clean keyless copy must still succeed; got: %v", err)
	}
	if got := script.execCalls.Load(); got != 1 {
		t.Errorf("INSERT exec calls = %d; want 1", got)
	}
}

// TestColdCopyReparentRetry_NotNullUniqueStillRetries proves the gate is
// keyed off the REPLAY predicate and not off "PrimaryKey == nil": a
// PK-less table with a NOT NULL UNIQUE index can still collide, so it
// keeps riding the reparent exactly as before.
func TestColdCopyReparentRetry_NotNullUniqueStillRetries(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{vttabletUnavailable(), nil}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	if err := w.WriteRows(context.Background(), notNullUniquePinTable(), feedReparentRows(2)); err != nil {
		t.Fatalf("a NOT NULL UNIQUE table must still ride the transient; got: %v", err)
	}
	if got := script.execCalls.Load(); got != 2 {
		t.Errorf("INSERT exec calls = %d; want 2 (transient then success)", got)
	}
}

// TestColdCopyReparentRetry_NullableUniqueCountsAsKeyless is the shape
// most likely to be got wrong by a hand-rolled "does it have a unique
// index?" check. MySQL allows repeated NULLs in a UNIQUE column, so such
// an index cannot be relied on to collide, and the retry must be refused.
func TestColdCopyReparentRetry_NullableUniqueCountsAsKeyless(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{vttabletUnavailable()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	err := w.WriteRows(context.Background(), nullableUniquePinTable(), feedReparentRows(2))
	if err == nil {
		t.Fatal("a UNIQUE index over a NULLABLE column does not reliably collide; want a refusal")
	}
	if got := script.execCalls.Load(); got != 1 {
		t.Errorf("INSERT exec calls = %d; want 1 (no re-send)", got)
	}
}

// TestReplayKeyPredicatesAgree is the premise check behind letting ONE
// gate serve both write cores.
//
// The plain core is now gated on irbackup.TableReplayIdempotent while the
// idempotent core refuses keyless tables upfront via
// effectiveUpsertKeyColumns. The claim that routing the idempotent core
// through the same gate is a no-op rests on those two predicates
// classifying every table the same way — a fact about two functions in
// two packages, which is exactly the kind of thing that quietly stops
// being true. If they ever diverge, this fails instead of a production
// upsert copy refusing for a table it used to accept.
func TestReplayKeyPredicatesAgree(t *testing.T) {
	cases := []struct {
		name  string
		table *ir.Table
	}{
		{"nil", nil},
		{"keyless", keylessPinTable()},
		{"single-column PK", pinReparentTable()},
		{"not-null unique, no PK", notNullUniquePinTable()},
		{"nullable unique, no PK", nullableUniquePinTable()},
		{"empty PK index", &ir.Table{
			Name:       "t_empty_pk",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}}},
			PrimaryKey: &ir.Index{Columns: nil, Unique: true},
		}},
		{"expression unique index only", &ir.Table{
			Name:    "t_expr",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false}},
			Indexes: []*ir.Index{{
				Name: "uq_expr", Unique: true,
				Columns: []ir.IndexColumn{{Column: "id", Expression: "(id * 2)"}},
			}},
		}},
		{"composite unique, one column nullable", &ir.Table{
			Name: "t_mixed",
			Columns: []*ir.Column{
				{Name: "a", Type: ir.Integer{Width: 8}, Nullable: false},
				{Name: "b", Type: ir.Integer{Width: 8}, Nullable: true},
			},
			Indexes: []*ir.Index{{
				Name: "uq_ab", Unique: true,
				Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}},
			}},
		}},
		{"composite unique, all not null", &ir.Table{
			Name: "t_composite",
			Columns: []*ir.Column{
				{Name: "a", Type: ir.Integer{Width: 8}, Nullable: false},
				{Name: "b", Type: ir.Integer{Width: 8}, Nullable: false},
			},
			Indexes: []*ir.Index{{
				Name: "uq_ab", Unique: true,
				Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}},
			}},
		}},
		{"non-unique index only", &ir.Table{
			Name:    "t_plain_index",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false}},
			Indexes: []*ir.Index{{Name: "ix_id", Columns: []ir.IndexColumn{{Column: "id"}}}},
		}},
	}
	// Anti-vacuity floor: a matrix that only ever produced one answer
	// would pass this test while proving nothing about agreement.
	sawTrue, sawFalse := false, false
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			replay := irbackup.TableReplayIdempotent(tc.table)
			_, upsertOK := effectiveUpsertKeyColumns(tc.table)
			if replay != upsertOK {
				t.Errorf("predicates disagree: irbackup.TableReplayIdempotent=%v, effectiveUpsertKeyColumns ok=%v. "+
					"The plain core's B-9 gate and the idempotent core's keyless refusal are no longer the same "+
					"question, so routing both through flushWithReparentRetry's gate can refuse a table the "+
					"upsert path accepts", replay, upsertOK)
			}
			if replay {
				sawTrue = true
			} else {
				sawFalse = true
			}
		})
	}
	if !sawTrue || !sawFalse {
		t.Errorf("matrix produced only one classification (sawTrue=%v sawFalse=%v); "+
			"a single-valued matrix cannot detect divergence", sawTrue, sawFalse)
	}
}

// TestKeylessAmbiguousReplayRefusalShape pins the operator-facing surface:
// the code, the wrapped cause, and that the message names the table.
func TestKeylessAmbiguousReplayRefusalShape(t *testing.T) {
	cause := errors.New("vttablet: code = Unavailable desc = tablet not serving")
	err := errKeylessAmbiguousReplay("orders_audit", 500, cause)
	if !errors.Is(err, cause) {
		t.Error("the refusal must wrap the underlying transient so it stays diagnosable")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCopyRetryAmbiguousKeyless {
		t.Errorf("code = %v (coded=%v); want %s", ce, ok, sluicecode.CodeCopyRetryAmbiguousKeyless)
	}
	if !containsAll(err.Error(), "orders_audit", "500") {
		t.Errorf("message must name the table and the batch size; got: %v", err)
	}
}

// containsAll is a small readability helper for the message assertions.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
