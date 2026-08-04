// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The index-emit preflight must give the SAME answer as the emitter, earlier
// (roadmap item 118).
//
// SQLite is the sharpest member of the class the item was filed for:
// [checkIndexPrefixLength]'s only call site is [emitCreateIndex], and this
// engine renders no secondary index inline at CREATE TABLE, so before
// [Engine.PreflightIndexes] existed there was NO early path at all — every
// prefix refusal landed after the whole table had been copied. v0.110.1's
// notes said "before any data moves"; they were corrected post-publish.
//
// The pin is a verdict-agreement matrix against the real emitter, driven
// through the real [Engine] value the registry holds. Both directions of
// disagreement are defects: an over-refusal breaks migrations that work today,
// an under-refusal leaves item 118 open for that shape.

package sqlite

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func sqlitePreflightTable(idx *ir.Index) *ir.Table {
	return &ir.Table{
		Name:    "users",
		Columns: []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 255}}},
		Indexes: []*ir.Index{idx},
	}
}

var sqliteIndexShapes = []struct {
	name        string
	idx         *ir.Index
	wantRefused bool
}{
	{
		name: "UNIQUE index with a prefix",
		idx: &ir.Index{
			Name: "uq_email", Unique: true,
			Columns: []ir.IndexColumn{{Column: "email", Length: 20}},
		},
		wantRefused: true,
	},
	{
		name: "UNIQUE index without a prefix",
		idx: &ir.Index{
			Name: "uq_email", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
		},
	},
	{
		// The control: on a non-unique index the prefix is a size choice, so
		// dropping it changes cost and not which rows are legal. MySQL schemas
		// carry these routinely; refusing would be the over-fire.
		name: "non-unique index with a prefix",
		idx: &ir.Index{
			Name: "idx_email", Columns: []ir.IndexColumn{{Column: "email", Length: 20}},
		},
	},
	{
		// SQLite HAS partial indexes and emits the predicate verbatim, so a
		// partial UNIQUE — the shape MySQL refuses — must pass here. This row
		// is what stops the three engines' preflights from being copy-pasted
		// into each other.
		name: "PARTIAL UNIQUE index (native on SQLite)",
		idx: &ir.Index{
			Name: "uq_email_live", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
			Predicate: "deleted_at IS NULL", PredicateDialect: "postgres",
		},
	},
	{
		// A Length riding an EXPRESSION entry indexes no column prefix at all.
		name: "UNIQUE expression index carrying a stray Length",
		idx: &ir.Index{
			Name: "uq_expr", Unique: true,
			Columns: []ir.IndexColumn{{Expression: "lower(email)", Length: 20}},
		},
	},
}

func TestPreflightIndexesAgreesWithTheEmitter(t *testing.T) {
	for _, tc := range sqliteIndexShapes {
		t.Run(tc.name, func(t *testing.T) {
			_, emitErr := emitCreateIndex("users", tc.idx)
			preErr := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{sqlitePreflightTable(tc.idx)}})

			if (emitErr != nil) != tc.wantRefused {
				t.Fatalf("the EMITTER's verdict changed: refused=%v, want %v (%v).\n\n"+
					"This matrix records what SQLite can represent; if the emitter's answer moved, the "+
					"policy moved and this row needs re-deriving, not the preflight.",
					emitErr != nil, tc.wantRefused, emitErr)
			}
			if (preErr != nil) != (emitErr != nil) {
				if preErr != nil {
					t.Fatalf("PreflightIndexes REFUSED a shape the emitter accepts: %v\n\n"+
						"An over-refusing preflight is a worse defect than the late refusal it replaces — "+
						"it fails migrations that work today.", preErr)
				}
				t.Fatalf("PreflightIndexes accepted a shape the emitter REFUSES (%v).\n\n"+
					"That is roadmap item 118's original bug, still open for this shape: on SQLite there was "+
					"no early path at all, so the operator paid for the whole copy first.", emitErr)
			}
		})
	}
}

func TestPreflightIndexesRefusalIsActionable(t *testing.T) {
	idx := &ir.Index{
		Name: "uq_email", Unique: true, Columns: []ir.IndexColumn{{Column: "email", Length: 20}},
	}
	err := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{sqlitePreflightTable(idx)}})
	if err == nil {
		t.Fatal("a prefixed UNIQUE index passed the preflight")
	}
	for _, want := range []string{"users", "uq_email", "email", "substr("} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — it must name the table, the index, the column and a "+
				"way forward, exactly as the emit-time refusal does.\ngot: %v", want, err)
		}
	}
}

func TestPreflightIndexesWalksEveryTable(t *testing.T) {
	clean := sqlitePreflightTable(&ir.Index{
		Name: "idx_email", Columns: []ir.IndexColumn{{Column: "email"}},
	})
	clean.Name = "clean"
	offending := sqlitePreflightTable(&ir.Index{
		Name: "uq_email", Unique: true, Columns: []ir.IndexColumn{{Column: "email", Length: 20}},
	})
	if err := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{clean, offending}}); err == nil {
		t.Fatal("an unrepresentable index on the SECOND table passed the preflight")
	}
}

func TestPreflightIndexesNilSafe(t *testing.T) {
	if err := (Engine{}).PreflightIndexes(nil); err == nil {
		t.Error("a nil schema should be refused loudly, not silently accepted")
	}
	s := &ir.Schema{Tables: []*ir.Table{nil, {Name: "t", Indexes: []*ir.Index{nil}}}}
	if err := (Engine{}).PreflightIndexes(s); err != nil {
		t.Errorf("nil table / nil index entries must be skipped, not refused: %v", err)
	}
}

// TestPreflightIndexesDoesNotWalkThePrimaryKey records a KNOWN GAP rather than
// a proof, so that reading this file cannot leave the impression the PK is
// covered.
//
// SQLite's table-level PRIMARY KEY clause renders through
// [quoteIndexColumnList], which has never consulted IndexColumn.Length — so a
// MySQL composite PK like `PRIMARY KEY (email(20), id)` is silently widened on
// a SQLite target TODAY, at emit time. That is a separate defect (a silent
// constraint weakening at the emitter), not a timing one, and closing it here
// would make this preflight refuse a shape the run currently accepts.
//
// The test asserts the CURRENT behaviour so the gap is visible and so whoever
// fixes the emitter is told, by a failing test, to revisit the preflight in
// the same pass.
func TestPreflightIndexesDoesNotWalkThePrimaryKey(t *testing.T) {
	tbl := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Column: "email", Length: 20}, {Column: "id"}},
		},
	}
	stmt, emitErr := emitTableDef(tbl)
	if emitErr != nil {
		t.Fatalf("the EMITTER now refuses a prefixed PRIMARY KEY (%v).\n\n"+
			"Good — that closes the silent widening this test documents. Now walk table.PrimaryKey in "+
			"Engine.PreflightIndexes too, and delete this test in favour of a row in sqliteIndexShapes.",
			emitErr)
	}
	if strings.Contains(stmt, "(20)") {
		t.Fatalf("emitted MySQL prefix syntax into SQLite DDL: %q", stmt)
	}
	if err := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{tbl}}); err != nil {
		t.Fatalf("the preflight refused a prefixed PRIMARY KEY the emitter still accepts: %v\n\n"+
			"That is an over-refusal — it breaks migrations that succeed today. Fix the emitter first.", err)
	}
}
