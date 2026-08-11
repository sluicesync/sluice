// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The index-emit preflight must give the SAME answer as the emitter, earlier
// (roadmap item 118).
//
// The whole change is a timing change, so the property worth pinning is not
// "the preflight refuses a partial unique index" — it is that the preflight's
// verdict and the emitter's verdict AGREE, shape by shape. Both directions of
// disagreement are real defects and they are not equally obvious:
//
//	preflight refuses, emitter accepts   an OVER-refusal — it breaks
//	                                     migrations that work today, which is
//	                                     worse than the late refusal it replaces
//	preflight accepts, emitter refuses   the original bug, unfixed for that shape
//
// So these tests drive the REAL [Engine] value (the same one the registry
// holds — a pipeline test stub would satisfy the interface by construction and
// prove nothing about this engine) and compare it against the real emitter over
// the shape matrix for MySQL's one unrepresentable index attribute,
// Index.Predicate: {unique, non-unique} × {predicate, no predicate} × {plain
// column, portable SQLite expression, NON-portable SQLite expression}.

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// preflightSchema wraps one index into the single-table schema shape the
// orchestrator hands the preflight.
func preflightSchema(idx *ir.Index) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "public", Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "deleted_at", Type: ir.Timestamp{}},
		},
		Indexes: []*ir.Index{idx},
	}}}
}

// mysqlIndexShapes is the verdict matrix. wantRefused is what BOTH the emitter
// and the preflight must say.
var mysqlIndexShapes = []struct {
	name        string
	idx         *ir.Index
	wantRefused bool
}{
	{
		name: "plain non-unique index",
		idx: &ir.Index{
			Name: "users_email_idx", Columns: []ir.IndexColumn{{Column: "email"}},
		},
	},
	{
		name: "plain UNIQUE index",
		idx: &ir.Index{
			Name: "users_email_uniq", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
		},
	},
	{
		// The predicate is a size choice here: the widened index covers a
		// superset of the rows and every query still answers correctly. It is
		// dropped with a WARN, never refused — on BOTH paths.
		name: "PARTIAL non-unique index",
		idx: &ir.Index{
			Name: "users_email_live_idx", Columns: []ir.IndexColumn{{Column: "email"}},
			Predicate: "deleted_at IS NULL", PredicateDialect: "postgres",
		},
	},
	{
		// The load-bearing case (Bug 224): the predicate is part of the
		// constraint, and MySQL's whole-table UNIQUE rejects rows the source
		// holds legally.
		name: "PARTIAL UNIQUE index",
		idx: &ir.Index{
			Name: "users_email_live_uniq", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
			Predicate: "deleted_at IS NULL", PredicateDialect: "postgres",
		},
		wantRefused: true,
	},
	{
		// A SQLite-source expression MySQL can render: the index is emitted,
		// so its partial-unique predicate is refused exactly like a plain
		// column's.
		name: "PARTIAL UNIQUE over a PORTABLE SQLite expression",
		idx: &ir.Index{
			Name: "users_expr_live_uniq", Unique: true,
			Columns:   []ir.IndexColumn{{Expression: "coalesce(email, '')", ExpressionDialect: sqliteSourceDialect}},
			Predicate: "deleted_at IS NULL", PredicateDialect: "sqlite",
		},
		wantRefused: true,
	},
	{
		// A SQLite-source expression MySQL CANNOT render is WARN-skipped by
		// the build path, so the preflight must skip it too. This is the
		// over-refusal trap: a preflight that walked every index blindly would
		// refuse a migration that succeeds today.
		name: "PARTIAL UNIQUE over a NON-PORTABLE SQLite expression",
		idx: &ir.Index{
			Name: "users_expr_bs_uniq", Unique: true,
			Columns:   []ir.IndexColumn{{Expression: `coalesce(email, 'x\y')`, ExpressionDialect: sqliteSourceDialect}},
			Predicate: "deleted_at IS NULL", PredicateDialect: "sqlite",
		},
	},
}

// TestPreflightIndexesAgreesWithTheEmitter is the verdict-agreement gate. The
// emitter is the INDEPENDENT expected value here: the preflight's answer is
// compared against what the shipped emit path actually does with the same
// index, not against a second copy of the policy written into the test.
func TestPreflightIndexesAgreesWithTheEmitter(t *testing.T) {
	for _, tc := range mysqlIndexShapes {
		t.Run(tc.name, func(t *testing.T) {
			_, emitErr := emitCreateIndex("users", tc.idx, true)
			preErr := Engine{}.PreflightIndexes(preflightSchema(tc.idx))

			if (emitErr != nil) != tc.wantRefused {
				t.Fatalf("the EMITTER's verdict changed: refused=%v, want %v (%v).\n\n"+
					"This matrix records what MySQL can represent; if the emitter's answer moved, the "+
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

// TestPreflightIndexesRefusalIsActionable pins the message content: item 118
// changes WHEN the refusal fires, never what it says. An operator still needs
// the index name, the offending predicate, and the way forward.
func TestPreflightIndexesRefusalIsActionable(t *testing.T) {
	idx := &ir.Index{
		Name: "users_email_live_uniq", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
		Predicate: "deleted_at IS NULL", PredicateDialect: "postgres",
	}
	err := Engine{}.PreflightIndexes(preflightSchema(idx))
	if err == nil {
		t.Fatal("a PARTIAL UNIQUE index passed the preflight")
	}
	for _, want := range []string{"users", "users_email_live_uniq", "deleted_at IS NULL", "generated column"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — it must name the table, the index, the predicate and a "+
				"way forward, exactly as the emit-time refusal does.\ngot: %v", want, err)
		}
	}
}

// TestPreflightIndexesWalksEveryTable guards the walk itself: a refusal that
// only fired for the first table would leave every later table's index
// unchecked, and a schema's problem index is rarely in table zero.
func TestPreflightIndexesWalksEveryTable(t *testing.T) {
	clean := &ir.Table{
		Schema: "public", Name: "a",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		Indexes: []*ir.Index{{Name: "a_id_idx", Columns: []ir.IndexColumn{{Column: "id"}}}},
	}
	offending := preflightSchema(&ir.Index{
		Name: "users_email_live_uniq", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}},
		Predicate: "deleted_at IS NULL", PredicateDialect: "postgres",
	}).Tables[0]

	s := &ir.Schema{Tables: []*ir.Table{clean, offending}}
	if err := (Engine{}).PreflightIndexes(s); err == nil {
		t.Fatal("an unrepresentable index on the SECOND table passed the preflight")
	}
}

// TestPreflightIndexesNilSafe — the orchestrator calls this on whatever the
// schema read produced, and a nil schema or a nil index must not panic the
// run before it starts.
func TestPreflightIndexesNilSafe(t *testing.T) {
	if err := (Engine{}).PreflightIndexes(nil); err == nil {
		t.Error("a nil schema should be refused loudly, not silently accepted")
	}
	s := &ir.Schema{Tables: []*ir.Table{nil, {Name: "t", Indexes: []*ir.Index{nil}}}}
	if err := (Engine{}).PreflightIndexes(s); err != nil {
		t.Errorf("nil table / nil index entries must be skipped, not refused: %v", err)
	}
}

// TestSQLiteNonPortableFixtureIsActuallyNonPortable guards the matrix's own
// premise. If [sqliteIndexPortableMySQL] ever renders that expression, the
// "must be skipped" row above silently starts testing the opposite thing.
func TestSQLiteNonPortableFixtureIsActuallyNonPortable(t *testing.T) {
	for _, tc := range mysqlIndexShapes {
		if tc.name != "PARTIAL UNIQUE over a NON-PORTABLE SQLite expression" {
			continue
		}
		if _, portable := sqliteIndexPortableMySQL(tc.idx); portable {
			t.Fatal("the fixture's SQLite expression is now PORTABLE to MySQL, so the row that pins the " +
				"WARN-skip carve-out no longer exercises it; pick an expression that still fails to translate")
		}
		return
	}
	t.Fatal("the non-portable row is gone from mysqlIndexShapes")
}
