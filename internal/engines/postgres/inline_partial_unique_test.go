// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A partial UNIQUE index must not become the inline copy key
// (audit 2026-08-04, the Postgres sibling of S8's second half).
//
// pickNonNullUniqueIndex filtered on uniqueness and column nullability and
// never on Predicate, so a partial unique index could be selected as a PK-less
// table's inline copy key. Two things then went wrong at once:
//
//  1. The winner is rendered as an inline `CONSTRAINT … UNIQUE (cols)`, and
//     Postgres cannot qualify a table constraint with a WHERE — so the emitted
//     constraint is UNCONDITIONAL and the target rejects rows the source holds
//     legally.
//  2. The winner is added to inlineSkipIndexNames so the index phase does not
//     re-create it as a duplicate. So the partial index was widened into a
//     constraint AND then never built as the partial index the source had.
//
// Declining to select it fixes both, and costs nothing that was ever real: a
// partial index is not a valid `ON CONFLICT (cols)` target unless the statement
// repeats its predicate, so an unconditional upsert could never have inferred
// it anyway.
//
// MySQL refuses the same shape instead of skipping it, and that asymmetry is
// correct rather than an inconsistency: MySQL cannot express a partial unique
// index at all, while Postgres can — just not inline.

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// pklessTable is the shape that reaches the inline copy-key path: no primary
// key, one non-null unique index.
func pklessTable(predicate string) *ir.Table {
	return &ir.Table{
		Schema: "public", Name: "events",
		Columns: []*ir.Column{
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: false},
			{Name: "deleted_at", Type: ir.Timestamp{}, Nullable: true},
		},
		Indexes: []*ir.Index{{
			Name:      "uq_email",
			Unique:    true,
			Columns:   []ir.IndexColumn{{Column: "email"}},
			Predicate: predicate,
		}},
	}
}

// TestPartialUniqueIndexNotChosenAsInlineCopyKey is the fix.
func TestPartialUniqueIndexNotChosenAsInlineCopyKey(t *testing.T) {
	got := pickNonNullUniqueIndex(pklessTable("deleted_at IS NULL"))
	if got != nil {
		t.Fatalf("pickNonNullUniqueIndex selected the PARTIAL index %q.\n\n"+
			"It would be rendered as an unconditional inline CONSTRAINT (Postgres cannot put a WHERE on a "+
			"table constraint), making the target stricter than the source — and it would then be added to "+
			"inlineSkipIndexNames, so the real partial index is never built either.", got.Name)
	}
}

// TestNonPartialUniqueIndexStillChosen is the control that keeps the fix from
// being a silent capability regression: Bug 125's inline copy key exists so a
// PK-less table's idempotent COPY has a conflict target, and an ORDINARY
// unique index must still provide one.
func TestNonPartialUniqueIndexStillChosen(t *testing.T) {
	got := pickNonNullUniqueIndex(pklessTable(""))
	if got == nil {
		t.Fatal("pickNonNullUniqueIndex rejected an ordinary non-null UNIQUE index; the Bug-125 inline copy " +
			"key is now never emitted and PK-less tables lose their conflict target")
	}
	if got.Name != "uq_email" {
		t.Errorf("selected %q, want uq_email", got.Name)
	}
}

// TestPartialIndexNotAddedToSkipSet is the second half of the defect, pinned
// separately because it has a different consequence: even if the constraint
// widening were somehow harmless, the index itself must still get built.
func TestPartialIndexNotAddedToSkipSet(t *testing.T) {
	skip := inlineSkipIndexNames(pklessTable("deleted_at IS NULL"))
	if _, skipped := skip["uq_email"]; skipped {
		t.Error("the partial index is in the inline-skip set, so the index-build phase will not create it — " +
			"the source's partial unique index would exist on the target in NO form")
	}

	// And the control: an ordinary inline key SHOULD be skipped, or the build
	// phase creates it a second time.
	skip = inlineSkipIndexNames(pklessTable(""))
	if _, skipped := skip["uq_email"]; !skipped {
		t.Error("an ordinary inline copy key is no longer in the skip set; the index-build phase will try to " +
			"create a duplicate of the inline constraint")
	}
}
