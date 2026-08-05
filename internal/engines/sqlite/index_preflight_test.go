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

// The PRIMARY KEY half (roadmap item 120). SQLite's table-level PRIMARY KEY
// clause renders through [quoteIndexColumnList], which never consulted
// IndexColumn.Length — so a MySQL `PRIMARY KEY (email(20), id)` was SILENTLY
// widened on a SQLite target: the source forbids two rows sharing the first 20
// characters of email, the target admitted them, and nothing said so.
//
// The matrix is the same verdict-agreement shape as [sqliteIndexShapes], run
// against [emitTableDef] instead of [emitCreateIndex]. It has to drive the
// emitter and the preflight together because the two arms landed together: the
// preflight could not refuse a prefixed PK while the emitter still accepted one.
var sqlitePKShapes = []struct {
	name        string
	pk          *ir.Index
	wantRefused bool
}{
	{
		// The defect, in the shape a MySQL source produces: composite, so it
		// takes the table-level clause.
		name: "composite PRIMARY KEY with a prefix",
		pk: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Column: "email", Length: 20}, {Column: "id"}},
		},
		wantRefused: true,
	},
	{
		name: "composite PRIMARY KEY without a prefix",
		pk: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Column: "email"}, {Column: "id"}},
		},
	},
	{
		// Single-column and non-integer, so it ALSO takes the table-level
		// clause — the same rendering as the composite case, one column wide.
		name: "single-column non-integer PRIMARY KEY with a prefix",
		pk: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Column: "email", Length: 20}},
		},
		wantRefused: true,
	},
	{
		// The rowid-alias path: a single INTEGER PK renders INLINE on the
		// column and never reaches [quoteIndexColumnList]. The refusal sits
		// ahead of that branch, so a (source-impossible, IR-expressible)
		// prefix here is refused too rather than depending on MySQL's errno
		// 1089 to keep it away.
		name: "single-column INTEGER PRIMARY KEY (rowid alias) with a prefix",
		pk: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Column: "id", Length: 4}},
		},
		wantRefused: true,
	},
	{
		name: "single-column INTEGER PRIMARY KEY (rowid alias), no prefix",
		pk: &ir.Index{
			Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}},
		},
	},
	{
		// Unique UNSET, which is what the MySQL and PG CDC readers build
		// (`&ir.Index{Columns: pkCols}`). A PRIMARY KEY enforces uniqueness by
		// definition, so the verdict must not consult that field — this row is
		// what fails if someone passes pk.Unique instead of primaryKeyKey.
		name: "prefixed PRIMARY KEY with ir.Index.Unique unset",
		pk: &ir.Index{
			Columns: []ir.IndexColumn{{Column: "email", Length: 20}, {Column: "id"}},
		},
		wantRefused: true,
	},
	{
		// A Length riding an expression entry constrains no column prefix.
		name: "PRIMARY KEY expression entry carrying a stray Length",
		pk: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Expression: "lower(email)", Length: 20}, {Column: "id"}},
		},
	},
}

func sqlitePKTable(pk *ir.Index) *ir.Table {
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk,
	}
}

func TestPreflightIndexesAgreesWithTheEmitterOnThePrimaryKey(t *testing.T) {
	for _, tc := range sqlitePKShapes {
		t.Run(tc.name, func(t *testing.T) {
			stmt, emitErr := emitTableDef(sqlitePKTable(tc.pk))
			preErr := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{sqlitePKTable(tc.pk)}})

			if (emitErr != nil) != tc.wantRefused {
				t.Fatalf("the EMITTER's verdict changed: refused=%v, want %v (%v)\nDDL: %s",
					emitErr != nil, tc.wantRefused, emitErr, stmt)
			}
			if emitErr == nil && strings.Contains(stmt, "(20)") {
				t.Fatalf("emitted MySQL prefix syntax into SQLite DDL: %q", stmt)
			}
			if (preErr != nil) != (emitErr != nil) {
				if preErr != nil {
					t.Fatalf("PreflightIndexes REFUSED a PRIMARY KEY the emitter accepts: %v\n\n"+
						"An over-refusal breaks migrations that work today.", preErr)
				}
				t.Fatalf("PreflightIndexes accepted a PRIMARY KEY the emitter REFUSES (%v).\n\n"+
					"The two must agree: whichever one is right, the operator should hear it before "+
					"any DDL runs, not at CREATE TABLE.", emitErr)
			}
		})
	}
}

// The PK refusal must not be the index refusal with a different subject line:
// a SQLite PRIMARY KEY takes column names, so "rewrite it as an expression
// index" — true and useful for a secondary index — is not something the
// operator can do to the key that is refused here.
func TestPrimaryKeyRefusalNamesAPrimaryKeyRemedy(t *testing.T) {
	pk := &ir.Index{
		Name: "PRIMARY", Unique: true,
		Columns: []ir.IndexColumn{{Column: "email", Length: 20}, {Column: "id"}},
	}
	err := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{sqlitePKTable(pk)}})
	if err == nil {
		t.Fatal("a prefixed PRIMARY KEY passed the preflight")
	}
	for _, want := range []string{"users", "primary key", "email", "substr(", "20"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — it must name the table, the KEY KIND, the column "+
				"and a way forward.\ngot: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "Rewrite it as a unique index over an expression") {
		t.Errorf("the PRIMARY KEY refusal offers the SECONDARY-INDEX remedy, which SQLite does not "+
			"accept for a PK.\ngot: %v", err)
	}
}
