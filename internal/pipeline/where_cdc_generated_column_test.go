// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for audit 2026-07-26 SL-1 — a released-code CRITICAL.
//
// The defect: a `--where` predicate referencing a GENERATED column compiled
// cleanly and the cold-start copy was CORRECT (the predicate is pushed into
// the source SELECT, which can read a generated column perfectly well), and
// then the CDC leg silently dropped every matching INSERT forever. A change
// stream does not deliver a generated column to the filter: MySQL's binlog row
// image DOES carry it, but sluice's decoder drops it on purpose so the target's
// own GENERATED clause recomputes the value rather than freezing the source's
// (engines/mysql/cdc_reader.go, `if col.IsGenerated() { continue }`), and
// pgoutput omits it from RelationMessage before PG 18 — so either way the
// decoded row had no such key, the comparison scored UNKNOWN, and the INSERT arm treated
// that as "not in scope" and returned nil. Exit 0, green status, no warning.
//
// What made it survive review: the INSERT arm was the ONE row-bearing arm of
// four with no image-completeness check, and the UPDATE/DELETE arms' loud
// refusal names a remedy (binlog_row_image=FULL / REPLICA IDENTITY FULL) that
// CANNOT fix this cause, because sluice's own decoder is what strips the
// column. An operator following the error would have chased a source setting
// that was already correct.
//
// Two independent gates, because there are two independent failures:
//  1. the predicate must be REFUSED at compile time (the operator hears about
//     it before any data moves, rather than after a cold start that looked
//     perfect), and
//  2. the INSERT arm must REFUSE an incomplete after-image rather than drop
//     it, so any OTHER producer of a partial image is loud too.
package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/rowpredicate"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// generatedColumnTable mirrors the audit's reproduction shape: an append-only
// orders table whose filter column is VIRTUAL-generated from a JSON payload.
// This is the canonical --where target, and the shape that never hits the loud
// UPDATE/DELETE sibling.
func generatedColumnTable() *ir.Table {
	return &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "payload", Type: ir.Text{}},
			{Name: "region", Type: ir.Varchar{Length: 8}, GeneratedExpr: "payload->>'$.r'"},
			{Name: "note", Type: ir.Varchar{Length: 64}},
		},
	}
}

// TestCompile_RefusesGeneratedColumnPredicate is gate 1: compile-time refusal.
func TestCompile_RefusesGeneratedColumnPredicate(t *testing.T) {
	tbl := generatedColumnTable()
	infos := rowpredicate.ColumnInfosFromIR(ir.ByteExactCollationResolver{}, tbl.Columns, false)

	if !infos["region"].Generated {
		t.Fatal("ColumnInfosFromIR did not mark the generated column — the refusal below cannot fire, " +
			"so this gate would pass vacuously")
	}

	_, err := rowpredicate.Compile("orders", "region = 'EU'", infos)
	if err == nil {
		t.Fatal("a --where predicate on a GENERATED column compiled cleanly. The cold-start copy would be " +
			"CORRECT and the CDC leg would then silently drop every matching INSERT (audit SL-1). It must be " +
			"refused before any data moves.")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeWhereCDCUnsupportedPredicate {
		t.Errorf("refusal carries code %v (matched=%v); want %v so operators can match on it", ce, ok, sluicecode.CodeWhereCDCUnsupportedPredicate)
	}
	// The message must name the column AND say why, or the operator cannot act.
	for _, want := range []string{"region", "GENERATED"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q; got: %v", want, err)
		}
	}

	// A predicate on the NON-generated columns of the same table must still
	// compile — the refusal is scoped to the column, not the table. Without
	// this leg the fix could be an over-refusal nobody notices.
	if _, err := rowpredicate.Compile("orders", "note = 'x'", infos); err != nil {
		t.Errorf("a predicate on a non-generated column of the same table was refused: %v", err)
	}
}

// TestRoute_InsertRefusesIncompleteAfterImage is gate 2: the runtime belt.
// It builds the filter by hand so the compile-time refusal (gate 1) cannot
// mask the arm being tested — the two failures are independent and must stay
// independently pinned.
func TestRoute_InsertRefusesIncompleteAfterImage(t *testing.T) {
	infos := rowpredicate.ColumnInfosFromIR(ir.ByteExactCollationResolver{}, []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "region", Type: ir.Varchar{Length: 8}},
	}, false)
	p, err := rowpredicate.Compile("orders", "region = 'EU'", infos)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	f := &whereCDCFilter{
		preds:   map[string]*rowpredicate.Predicate{"orders": p},
		pkCols:  map[string]map[string]bool{"orders": {"id": true}},
		refCols: map[string]map[string]bool{"orders": {"region": true}},
	}

	// The after-image omits the predicate column — exactly what the binlog
	// decoder produced for a generated column.
	out, err := f.route(ir.Insert{
		Schema: "app",
		Table:  "orders",
		Row:    ir.Row{"id": int64(1), "note": "x"},
	})
	if err == nil {
		t.Fatalf("an INSERT whose after-image omits the predicate column was silently DROPPED "+
			"(returned %v rows, no error). That is exit-0 data loss: the row exists at the source and "+
			"never arrives at the target (audit SL-1).", len(out))
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeWhereCDCAfterImage {
		t.Errorf("refusal carries code %v (matched=%v); want %v", ce, ok, sluicecode.CodeWhereCDCAfterImage)
	}
	// The consequence stated must be the INSERT one. Telling an operator a
	// dropped INSERT "could emit a spurious DELETE" sends them after the wrong
	// bug — the reason this helper takes the op.
	if !strings.Contains(err.Error(), "INSERT") {
		t.Errorf("refusal does not name the INSERT arm; got: %v", err)
	}

	// A COMPLETE after-image must still route normally, in and out of scope.
	if got, err := f.route(ir.Insert{Schema: "app", Table: "orders", Row: ir.Row{"id": int64(2), "region": "EU"}}); err != nil || len(got) != 1 {
		t.Errorf("an in-scope INSERT with a complete image should forward: got %d changes, err=%v", len(got), err)
	}
	if got, err := f.route(ir.Insert{Schema: "app", Table: "orders", Row: ir.Row{"id": int64(3), "region": "US"}}); err != nil || len(got) != 0 {
		t.Errorf("an out-of-scope INSERT with a complete image should drop: got %d changes, err=%v", len(got), err)
	}
}

// TestRoute_EveryRowBearingArmChecksImageCompleteness is the CLASS gate.
//
// The bug was not "the INSERT arm has a missing check" — it was that the check
// is a per-IMAGE obligation implemented per-ARM, so adding a fifth arm silently
// opts out of it. This enumerates every row-bearing arm and asserts each
// refuses an image missing a predicate column, so a new arm cannot ship without
// either the check or a deliberate change to this table.
func TestRoute_EveryRowBearingArmChecksImageCompleteness(t *testing.T) {
	infos := rowpredicate.ColumnInfosFromIR(ir.ByteExactCollationResolver{}, []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "region", Type: ir.Varchar{Length: 8}},
	}, false)
	p, err := rowpredicate.Compile("orders", "region = 'EU'", infos)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	newFilter := func() *whereCDCFilter {
		return &whereCDCFilter{
			preds:   map[string]*rowpredicate.Predicate{"orders": p},
			pkCols:  map[string]map[string]bool{"orders": {"id": true}},
			refCols: map[string]map[string]bool{"orders": {"region": true}},
		}
	}
	incomplete := ir.Row{"id": int64(1)}
	complete := ir.Row{"id": int64(1), "region": "EU"}

	cases := []struct {
		arm    string
		change ir.Change
	}{
		{"INSERT (after-image)", ir.Insert{Schema: "app", Table: "orders", Row: incomplete}},
		{"DELETE (before-image)", ir.Delete{Schema: "app", Table: "orders", Before: incomplete}},
		{"UPDATE (before-image)", ir.Update{Schema: "app", Table: "orders", Before: incomplete, After: complete}},
		{"UPDATE (after-image)", ir.Update{Schema: "app", Table: "orders", Before: complete, After: incomplete}},
	}
	for _, tc := range cases {
		t.Run(tc.arm, func(t *testing.T) {
			if _, err := newFilter().route(tc.change); err == nil {
				t.Errorf("%s: an image omitting the predicate column was accepted instead of refused. "+
					"Every row-bearing arm must check completeness — the check is an obligation of the "+
					"IMAGE, not of the arm (audit SL-1).", tc.arm)
			}
		})
	}
}
