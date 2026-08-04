// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The index-emit preflight must give the SAME answer as the emitter, earlier
// (roadmap item 118).
//
// [TestIndexPrefixLength_UniqueKeysRefuse_PlainIndexesDoNot] next door pins
// WHAT Postgres refuses at each of its four key-emitting sites. This file pins
// that [Engine.PreflightIndexes] — which runs before any data moves — reaches
// the same verdict at each of those four sites, in both directions:
//
//	preflight refuses, emitter accepts   an OVER-refusal, which breaks
//	                                     migrations that work today
//	preflight accepts, emitter refuses   item 118's original bug, still open
//	                                     for that shape
//
// The emitter is the independent expected value; the test never re-states the
// prefix policy. Driven through the REAL [Engine] value the registry holds —
// a pipeline stub would satisfy the interface by construction and prove
// nothing about this engine.

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// pgPreflightTable is one fixture: a table carrying exactly one key, plus the
// emitter that key really travels through on a live run.
type pgPreflightTable struct {
	name        string
	table       *ir.Table
	emit        func(t *ir.Table) error
	wantRefused bool
}

func pgTableWith(pk *ir.Index, idxs ...*ir.Index) *ir.Table {
	return &ir.Table{
		Schema:     "public",
		Name:       "users",
		Columns:    []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 255}}},
		PrimaryKey: pk,
		Indexes:    idxs,
	}
}

// emitViaTableDef / emitViaCreateIndex / emitViaAddConstraint name the route a
// key actually takes, so a row's `emit` is the shipped path and not a stand-in.
func emitViaTableDef(t *ir.Table) error {
	_, err := emitTableDef("public", t, emitOpts{})
	return err
}

func emitViaCreateIndex(t *ir.Table) error {
	_, err := emitCreateIndex("public", t.Name, t.Indexes[0], emitOpts{})
	return err
}

func emitViaAddConstraint(t *ir.Table) error {
	_, err := emitAddUniqueConstraint("public", t.Name, t.Indexes[0])
	return err
}

// pgIndexShapes is the verdict matrix: every PG key-emitting site × {prefix,
// no prefix}, plus the expression entry (where a Length is meaningless and
// must not fire) and the non-unique control.
var pgIndexShapes = []pgPreflightTable{
	{
		name: "PRIMARY KEY with a prefix",
		table: pgTableWith(&ir.Index{
			Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{prefixCol("email", 10)},
		}),
		emit: emitViaTableDef, wantRefused: true,
	},
	{
		name: "PRIMARY KEY without a prefix",
		table: pgTableWith(&ir.Index{
			Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
		}),
		emit: emitViaTableDef,
	},
	{
		name: "UNIQUE index with a prefix (CREATE INDEX route)",
		table: pgTableWith(nil, &ir.Index{
			Name: "uq_email", Unique: true, Columns: []ir.IndexColumn{prefixCol("email", 10)},
		}),
		emit: emitViaCreateIndex, wantRefused: true,
	},
	{
		name: "UNIQUE index without a prefix",
		table: pgTableWith(nil, &ir.Index{
			Name: "uq_email", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
		}),
		emit: emitViaCreateIndex,
	},
	{
		// The route a real MySQL→PG migrate takes for a constraint-backed
		// unique key, and the deferred one item 118 was filed against: it runs
		// in the CONSTRAINTS phase, two phases after the copy.
		name: "constraint-backed UNIQUE with a prefix (ADD CONSTRAINT route)",
		table: pgTableWith(nil, &ir.Index{
			Name: "uq_email", Unique: true, ConstraintBacked: true,
			Columns: []ir.IndexColumn{prefixCol("email", 10)},
		}),
		emit: emitViaAddConstraint, wantRefused: true,
	},
	{
		name: "constraint-backed UNIQUE without a prefix",
		table: pgTableWith(nil, &ir.Index{
			Name: "uq_email", Unique: true, ConstraintBacked: true,
			Columns: []ir.IndexColumn{{Column: "email"}},
		}),
		emit: emitViaAddConstraint,
	},
	{
		// The control that makes the gate usable: prefix indexes on TEXT
		// columns are routine in MySQL schemas, and on a NON-unique index the
		// prefix is a size choice. Refusing here would be the over-fire.
		name: "non-unique index with a prefix",
		table: pgTableWith(nil, &ir.Index{
			Name: "idx_email", Columns: []ir.IndexColumn{prefixCol("email", 10)},
		}),
		emit: emitViaCreateIndex,
	},
	{
		// A Length riding an EXPRESSION entry indexes no column prefix at all,
		// so neither path may fire on it.
		name: "UNIQUE expression index carrying a stray Length",
		table: pgTableWith(nil, &ir.Index{
			Name: "uq_expr", Unique: true,
			Columns: []ir.IndexColumn{{Expression: "lower(email)", Length: 10}},
		}),
		emit: emitViaCreateIndex,
	},
}

func TestPreflightIndexesAgreesWithTheEmitter(t *testing.T) {
	for _, tc := range pgIndexShapes {
		t.Run(tc.name, func(t *testing.T) {
			emitErr := tc.emit(tc.table)
			preErr := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{tc.table}})

			if (emitErr != nil) != tc.wantRefused {
				t.Fatalf("the EMITTER's verdict changed: refused=%v, want %v (%v).\n\n"+
					"This matrix records what Postgres can represent; if the emitter's answer moved, the "+
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
					"That is roadmap item 118's original bug, still open for this shape: the operator pays "+
					"for the whole bulk copy before finding out.", emitErr)
			}
		})
	}
}

// TestPreflightIndexesRefusalIsActionable — item 118 changes WHEN the refusal
// fires, never what it says.
func TestPreflightIndexesRefusalIsActionable(t *testing.T) {
	tbl := pgTableWith(nil, &ir.Index{
		Name: "uq_email", Unique: true, ConstraintBacked: true,
		Columns: []ir.IndexColumn{prefixCol("email", 10)},
	})
	err := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{tbl}})
	if err == nil {
		t.Fatal("a prefixed constraint-backed UNIQUE passed the preflight")
	}
	for _, want := range []string{"uq_email", "email", "prefix", "left("} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — it must name the index, the column, the offending "+
				"prefix and a way forward, exactly as the emit-time refusal does.\ngot: %v", want, err)
		}
	}
}

// TestPreflightIndexesWalksEveryTable — a refusal that only fired for table
// zero would leave every later table unchecked.
func TestPreflightIndexesWalksEveryTable(t *testing.T) {
	clean := pgTableWith(nil, &ir.Index{
		Name: "idx_email", Columns: []ir.IndexColumn{{Column: "email"}},
	})
	clean.Name = "clean"
	offending := pgTableWith(nil, &ir.Index{
		Name: "uq_email", Unique: true, Columns: []ir.IndexColumn{prefixCol("email", 10)},
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
