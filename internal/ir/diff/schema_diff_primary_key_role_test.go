// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The PRIMARY-KEY half of the index-name axis (Bug 234).
//
// The class here is the NAMING CONVENTION, and pinning one representative
// of it is exactly the mistake this fix exists to correct: `schema diff`
// was verified against same-engine pairs (where both sides spell the key
// the same way) and shipped a guaranteed false positive for every pair
// whose conventions differ. So the matrix is every convention on the
// expected side × every convention on the actual side, and it grades
// BOTH directions — no phantom drift when the keys agree, and real drift
// still reported when they do not.
//
// SCOPE: this grades [Schemas]'s index comparison only. The secondary-
// index half of the same axis (a Postgres target table-prefixing the
// names it creates) is closed in internal/translate and graded by
// TestRetargetForShapeCompare_RenamesIndexesForAPostgresTarget there.

// pkNamingConventions is the set of spellings a real schema reader
// produces for a primary-key index, derived from the readers rather than
// invented:
//
//   - "PRIMARY"      — internal/engines/mysql.populateIndexes
//   - "<table>_pkey" — PostgreSQL's auto-generated constraint name
//   - "orders_pk"    — a PG source whose operator NAMED the constraint
//   - ""             — internal/engines/sqlite.readTables (and the two
//     CDC relation decoders) leave the key unnamed
var pkNamingConventions = map[string]string{
	"mysql PRIMARY":    "PRIMARY",
	"pg auto pkey":     "orders_pkey",
	"pg named":         "orders_pk",
	"sqlite unnamed":   "",
	"other table pkey": "customers_pkey",
}

func tableWithPK(pkName string, pkCols []string, secondary ...*ir.Index) *ir.Table {
	cols := make([]ir.IndexColumn, 0, len(pkCols))
	for _, c := range pkCols {
		cols = append(cols, ir.IndexColumn{Column: c})
	}
	return &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Name: pkName, Unique: true, Columns: cols},
		Indexes:    secondary,
	}
}

func schemaOf(t *ir.Table) *ir.Schema { return &ir.Schema{Tables: []*ir.Table{t}} }

// TestPrimaryKeyMatchedByRoleAcrossEveryEngineNamingConvention is the
// no-phantom direction: for EVERY ordered pair of naming conventions, a
// primary key over the same columns is not drift.
func TestPrimaryKeyMatchedByRoleAcrossEveryEngineNamingConvention(t *testing.T) {
	graded := 0
	for expLabel, expName := range pkNamingConventions {
		for actLabel, actName := range pkNamingConventions {
			graded++
			exp := schemaOf(tableWithPK(expName, []string{"id"}))
			act := schemaOf(tableWithPK(actName, []string{"id"}))
			d := Schemas(exp, act, Options{})
			if d.HasChanges() {
				t.Errorf("expected %s (%q) vs actual %s (%q): reported drift on an identical primary key: %+v",
					expLabel, expName, actLabel, actName, d.TablesMismatched)
			}
		}
	}
	if graded != len(pkNamingConventions)*len(pkNamingConventions) {
		t.Fatalf("graded %d pairs; want %d — the matrix has gone vacuous", graded, len(pkNamingConventions)*len(pkNamingConventions))
	}
}

// TestPrimaryKeyRoleStillReportsRealDrift is the over-refusal guard, and
// it is the half that matters most: a diff that stops reporting real
// drift to silence a false positive is worse than the false positive.
// Every one of these must still surface, under EVERY naming convention.
func TestPrimaryKeyRoleStillReportsRealDrift(t *testing.T) {
	for expLabel, expName := range pkNamingConventions {
		for actLabel, actName := range pkNamingConventions {
			label := expLabel + " -> " + actLabel

			t.Run("different columns/"+label, func(t *testing.T) {
				exp := schemaOf(tableWithPK(expName, []string{"id"}))
				act := schemaOf(tableWithPK(actName, []string{"id", "email"}))
				d := Schemas(exp, act, Options{})
				if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].IndexesMismatched) != 1 {
					t.Fatalf("a primary key over different columns was not reported: %+v", d)
				}
				got := d.TablesMismatched[0].IndexesMismatched[0]
				if got.ExpectedColumns != "(id)" || got.ActualColumns != "(id, email)" {
					t.Errorf("columns = %q/%q; want %q/%q", got.ExpectedColumns, got.ActualColumns, "(id)", "(id, email)")
				}
			})

			t.Run("missing on target/"+label, func(t *testing.T) {
				exp := schemaOf(tableWithPK(expName, []string{"id"}))
				actTbl := tableWithPK(actName, []string{"id"})
				actTbl.PrimaryKey = nil
				d := Schemas(exp, schemaOf(actTbl), Options{})
				if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].IndexesMissing) != 1 {
					t.Fatalf("a target with NO primary key was not reported as missing one: %+v", d)
				}
				want := expName
				if want == "" {
					want = "PRIMARY KEY"
				}
				if got := d.TablesMismatched[0].IndexesMissing[0]; got != want {
					t.Errorf("missing index reported as %q; want %q", got, want)
				}
			})

			t.Run("extra on target/"+label, func(t *testing.T) {
				expTbl := tableWithPK(expName, []string{"id"})
				expTbl.PrimaryKey = nil
				act := schemaOf(tableWithPK(actName, []string{"id"}))
				d := Schemas(schemaOf(expTbl), act, Options{})
				if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].IndexesExtra) != 1 {
					t.Fatalf("a target-only primary key was not reported as extra: %+v", d)
				}
				want := actName
				if want == "" {
					want = "PRIMARY KEY"
				}
				if got := d.TablesMismatched[0].IndexesExtra[0]; got != want {
					t.Errorf("extra index reported as %q; want %q", got, want)
				}
			})
		}
	}
}

// TestSecondaryIndexesStillMatchedByName is the containment check: the
// role key applies to the PRIMARY KEY and nothing else. A secondary
// index that exists on one side only is still one missing + one extra,
// and a secondary index that happens to be NAMED like a primary key gets
// no special treatment.
func TestSecondaryIndexesStillMatchedByName(t *testing.T) {
	exp := schemaOf(tableWithPK("orders_pkey", []string{"id"},
		&ir.Index{Name: "orders_email_idx", Columns: []ir.IndexColumn{{Column: "email"}}}))
	act := schemaOf(tableWithPK("PRIMARY", []string{"id"},
		&ir.Index{Name: "orders_created_idx", Columns: []ir.IndexColumn{{Column: "created_at"}}}))

	d := Schemas(exp, act, Options{})
	if len(d.TablesMismatched) != 1 {
		t.Fatalf("expected one mismatched table; got %+v", d)
	}
	td := d.TablesMismatched[0]
	if len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "orders_email_idx" {
		t.Errorf("indexes_missing = %v; want [orders_email_idx]", td.IndexesMissing)
	}
	if len(td.IndexesExtra) != 1 || td.IndexesExtra[0] != "orders_created_idx" {
		t.Errorf("indexes_extra = %v; want [orders_created_idx]", td.IndexesExtra)
	}
}

// TestSecondaryIndexNamedLikeAPrimaryKeyIsNotRoleMatched pins the
// sentinel's containment: a table whose SECONDARY index is literally
// named `PRIMARY` (legal on MySQL only for the key itself, but a
// hand-built or future-engine IR can carry it) must not be matched
// against the other side's primary key.
func TestSecondaryIndexNamedLikeAPrimaryKeyIsNotRoleMatched(t *testing.T) {
	expTbl := tableWithPK("", []string{"id"})
	expTbl.PrimaryKey = nil
	expTbl.Indexes = []*ir.Index{{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: "id"}}}}

	act := schemaOf(tableWithPK("PRIMARY", []string{"id"}))

	d := Schemas(schemaOf(expTbl), act, Options{})
	if len(d.TablesMismatched) != 1 {
		t.Fatalf("expected one mismatched table; got %+v", d)
	}
	td := d.TablesMismatched[0]
	if len(td.IndexesMissing) != 1 || td.IndexesMissing[0] != "PRIMARY" {
		t.Errorf("indexes_missing = %v; want [PRIMARY] (the secondary index, unmatched)", td.IndexesMissing)
	}
	if len(td.IndexesExtra) != 1 || td.IndexesExtra[0] != "PRIMARY" {
		t.Errorf("indexes_extra = %v; want [PRIMARY] (the target's real key, unmatched)", td.IndexesExtra)
	}
}

// TestPrimaryKeyUniquenessAndPredicateStillCompared closes the rest of
// [diffIndexDefinitions]'s attribute set on the role-matched pair — the
// attributes are compared for secondary indexes and it would be easy for
// the primary key to reach them by a path that dropped one.
func TestPrimaryKeyUniquenessAndPredicateStillCompared(t *testing.T) {
	exp := schemaOf(tableWithPK("orders_pkey", []string{"id"}))
	actTbl := tableWithPK("PRIMARY", []string{"id"})
	actTbl.PrimaryKey.Unique = false
	actTbl.PrimaryKey.Predicate = "id > 0"

	d := Schemas(exp, schemaOf(actTbl), Options{})
	if len(d.TablesMismatched) != 1 || len(d.TablesMismatched[0].IndexesMismatched) != 1 {
		t.Fatalf("uniqueness/predicate drift on a role-matched primary key was not reported: %+v", d)
	}
	got := d.TablesMismatched[0].IndexesMismatched[0]
	if !got.UniqueMismatched || got.ExpectedUnique != true || got.ActualUnique != false {
		t.Errorf("uniqueness drift not reported: %+v", got)
	}
	if got.ExpectedPredicate != "" || got.ActualPredicate != "id > 0" {
		t.Errorf("predicate drift = %q/%q; want \"\"/%q", got.ExpectedPredicate, got.ActualPredicate, "id > 0")
	}
	// The report is framed expected-side, so the entry carries the name
	// sluice would have produced.
	if got.Name != "orders_pkey" {
		t.Errorf("IndexDiff.Name = %q; want the expected side's spelling %q", got.Name, "orders_pkey")
	}
}

// TestIgnoreExtrasStillSuppressesAnExtraPrimaryKey keeps the option
// semantics intact on the newly-matched entry.
func TestIgnoreExtrasStillSuppressesAnExtraPrimaryKey(t *testing.T) {
	expTbl := tableWithPK("orders_pkey", []string{"id"})
	expTbl.PrimaryKey = nil
	act := schemaOf(tableWithPK("PRIMARY", []string{"id"}))

	d := Schemas(schemaOf(expTbl), act, Options{IgnoreExtras: true})
	if d.HasChanges() {
		t.Errorf("IgnoreExtras did not suppress the target-only primary key: %+v", d)
	}
}
