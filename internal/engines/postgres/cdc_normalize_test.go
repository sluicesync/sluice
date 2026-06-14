// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestNormalizeForCDCComparison_PG pins the per-field zeroing rules
// for the PG CDC-comparison normalizer (ADR-0054 Bug 84 fix v0.73.2).
// Each subtest covers one known-asymmetric field; the catch-all case
// confirms unrelated types pass through unchanged.
func TestNormalizeForCDCComparison_PG(t *testing.T) {
	t.Parallel()

	eng := Engine{}

	t.Run("Integer_AutoIncrement_zeroed", func(t *testing.T) {
		in := &ir.Table{
			Schema: "public",
			Name:   "widgets",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}, Nullable: false},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got, ok := out.Columns[0].Type.(ir.Integer)
		if !ok {
			t.Fatalf("got type %T; want ir.Integer", out.Columns[0].Type)
		}
		if got.AutoIncrement {
			t.Errorf("Integer.AutoIncrement = true; want false (pgoutput RelationMessage cannot carry IDENTITY)")
		}
		if got.Width != 64 {
			t.Errorf("Integer.Width = %d; want 64 (Width must pass through unchanged)", got.Width)
		}
		// Input must NOT be mutated.
		inGot, _ := in.Columns[0].Type.(ir.Integer)
		if !inGot.AutoIncrement {
			t.Errorf("input Integer.AutoIncrement was mutated; normalizer must return a new struct")
		}
	})

	t.Run("Varchar_Collation_zeroed", func(t *testing.T) {
		in := &ir.Table{
			Schema: "public",
			Name:   "widgets",
			Columns: []*ir.Column{
				{Name: "name", Type: ir.Varchar{Length: 64, Collation: "en_US.utf8"}, Nullable: false},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got, ok := out.Columns[0].Type.(ir.Varchar)
		if !ok {
			t.Fatalf("got type %T; want ir.Varchar", out.Columns[0].Type)
		}
		if got.Collation != "" {
			t.Errorf("Varchar.Collation = %q; want empty (pgoutput RelationMessage cannot carry collation OID)", got.Collation)
		}
		if got.Length != 64 {
			t.Errorf("Varchar.Length = %d; want 64", got.Length)
		}
	})

	t.Run("Char_Collation_zeroed", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "code", Type: ir.Char{Length: 8, Collation: "C"}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got := out.Columns[0].Type.(ir.Char)
		if got.Collation != "" {
			t.Errorf("Char.Collation = %q; want empty", got.Collation)
		}
		if got.Length != 8 {
			t.Errorf("Char.Length = %d; want 8", got.Length)
		}
	})

	t.Run("Text_Collation_zeroed", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "notes", Type: ir.Text{Size: ir.TextLong, Collation: "en_US.utf8"}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got := out.Columns[0].Type.(ir.Text)
		if got.Collation != "" {
			t.Errorf("Text.Collation = %q; want empty", got.Collation)
		}
		if got.Size != ir.TextLong {
			t.Errorf("Text.Size = %v; want TextLong", got.Size)
		}
	})

	t.Run("Decimal_Unconstrained_collapsed", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "amount", Type: ir.Decimal{Unconstrained: true}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got := out.Columns[0].Type.(ir.Decimal)
		if got.Unconstrained {
			t.Errorf("Decimal.Unconstrained = true; want false (pgoutput emits typmod=-1 as (0,0))")
		}
		if got.Precision != 0 || got.Scale != 0 {
			t.Errorf("Decimal{P=%d S=%d}; want (0,0) after normalize", got.Precision, got.Scale)
		}
	})

	t.Run("DateTime_default_precision_collapsed_bug86", func(t *testing.T) {
		// PG `TIMESTAMP` (no explicit precision) → SchemaReader reads
		// information_schema.datetime_precision=6; CDC reads
		// atttypmod=-1 → Precision=0. Collapse 6 → 0 on the seed.
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "ts", Type: ir.DateTime{Precision: 6}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got := out.Columns[0].Type.(ir.DateTime)
		if got.Precision != 0 {
			t.Errorf("DateTime.Precision = %d; want 0 (default-precision TIMESTAMP collapses to match CDC's typmod=-1 → 0)", got.Precision)
		}
	})

	t.Run("DateTime_explicit_nondefault_precision_passthrough", func(t *testing.T) {
		// Explicit `TIMESTAMP(3)` should NOT be collapsed — both
		// cold-start and CDC see Precision=3.
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "ts", Type: ir.DateTime{Precision: 3}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got := out.Columns[0].Type.(ir.DateTime)
		if got.Precision != 3 {
			t.Errorf("DateTime.Precision = %d; want 3 (explicit non-default precision must pass through)", got.Precision)
		}
	})

	t.Run("Time_default_precision_collapsed_bug86", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "t", Type: ir.Time{Precision: 6}},
				{Name: "tz", Type: ir.Time{Precision: 6, WithTimeZone: true}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		for i, col := range out.Columns {
			got := col.Type.(ir.Time)
			if got.Precision != 0 {
				t.Errorf("col[%d] Time.Precision = %d; want 0", i, got.Precision)
			}
		}
		// WithTimeZone must pass through.
		gotTz := out.Columns[1].Type.(ir.Time)
		if !gotTz.WithTimeZone {
			t.Errorf("Time.WithTimeZone was lost during normalization")
		}
	})

	t.Run("Timestamp_default_precision_collapsed_bug86", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "tz", Type: ir.Timestamp{Precision: 6, WithTimeZone: true}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got := out.Columns[0].Type.(ir.Timestamp)
		if got.Precision != 0 {
			t.Errorf("Timestamp.Precision = %d; want 0", got.Precision)
		}
		if !got.WithTimeZone {
			t.Errorf("Timestamp.WithTimeZone was lost during normalization")
		}
	})

	t.Run("Decimal_constrained_passthrough", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "price", Type: ir.Decimal{Precision: 10, Scale: 2}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		got := out.Columns[0].Type.(ir.Decimal)
		if got.Precision != 10 || got.Scale != 2 {
			t.Errorf("Decimal{P=%d S=%d}; want (10,2) passthrough on constrained decimal", got.Precision, got.Scale)
		}
	})

	t.Run("unrelated_types_passthrough", func(t *testing.T) {
		// Types with no known-asymmetric field on PG: Boolean, Date,
		// Float, JSON, UUID, etc. The normalizer must not touch them.
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "flag", Type: ir.Boolean{}},
				{Name: "d", Type: ir.Date{}},
				{Name: "f", Type: ir.Float{Precision: ir.FloatDouble}},
				{Name: "j", Type: ir.JSON{Binary: true}},
				{Name: "uid", Type: ir.UUID{}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		if _, ok := out.Columns[0].Type.(ir.Boolean); !ok {
			t.Errorf("Boolean column type changed: %T", out.Columns[0].Type)
		}
		if _, ok := out.Columns[1].Type.(ir.Date); !ok {
			t.Errorf("Date column type changed: %T", out.Columns[1].Type)
		}
		gotF, ok := out.Columns[2].Type.(ir.Float)
		if !ok || gotF.Precision != ir.FloatDouble {
			t.Errorf("Float column changed: %#v", out.Columns[2].Type)
		}
		gotJ, ok := out.Columns[3].Type.(ir.JSON)
		if !ok || !gotJ.Binary {
			t.Errorf("JSON column changed: %#v", out.Columns[3].Type)
		}
		if _, ok := out.Columns[4].Type.(ir.UUID); !ok {
			t.Errorf("UUID column type changed: %T", out.Columns[4].Type)
		}
	})

	// Bug 86 (v0.78.1): column-level fields pgoutput cannot carry must
	// be zeroed on the seed so the classifier doesn't surface a false
	// `altered-col=true` on every CDC-projected boundary. The catalogued
	// trigger was a nullable NUMERIC/TEXT column on a RENAME boundary;
	// these subtests pin the class (Nullable / Default / Comment) so the
	// next "pgoutput doesn't carry X" gap doesn't regress.
	t.Run("Nullable_zeroed_bug86", func(t *testing.T) {
		// The smoking-gun Bug 86 field: a nullable existing column like
		// NUMERIC(10,2) or TEXT made the classifier fire
		// ShapeKindAlterColumnNullability on every CDC boundary because
		// the cold-start SchemaReader populated Nullable=true while
		// projectRelation left Nullable=false (pgoutput's
		// RelationMessage doesn't carry attnotnull).
		in := &ir.Table{
			Schema: "public",
			Name:   "widgets",
			Columns: []*ir.Column{
				{Name: "price", Type: ir.Decimal{Precision: 10, Scale: 2}, Nullable: true},
				{Name: "notes", Type: ir.Text{Size: ir.TextLong}, Nullable: true},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		for i, col := range out.Columns {
			if col.Nullable {
				t.Errorf("col[%d] Nullable = true; want false (pgoutput RelationMessage cannot carry attnotnull)", i)
			}
		}
		// Input must NOT be mutated.
		for i, col := range in.Columns {
			if !col.Nullable {
				t.Errorf("input col[%d] Nullable was mutated; normalizer must return a new struct", i)
			}
		}
	})

	t.Run("Default_zeroed_bug86", func(t *testing.T) {
		// pgoutput's RelationMessage does not carry attdefault. Cold-
		// start SchemaReader populates Default with DefaultNone /
		// DefaultLiteral / DefaultExpression; projectRelation leaves
		// it nil. Zero on the seed so equality holds.
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "a", Default: ir.DefaultNone{}},
				{Name: "b", Default: ir.DefaultLiteral{Value: "0"}},
				{Name: "c", Default: ir.DefaultExpression{Expr: "now()"}},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		for i, col := range out.Columns {
			if col.Default != nil {
				t.Errorf("col[%d] Default = %#v; want nil (pgoutput RelationMessage cannot carry attdefault)", i, col.Default)
			}
		}
	})

	t.Run("Comment_zeroed_bug86", func(t *testing.T) {
		// pgoutput's RelationMessage does not carry pg_description.
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "x", Comment: "operator comment"},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		if out.Columns[0].Comment != "" {
			t.Errorf("Comment = %q; want empty (pgoutput RelationMessage cannot carry pg_description)", out.Columns[0].Comment)
		}
	})

	t.Run("generated_column_dropped_adr0091", func(t *testing.T) {
		// pgoutput's RelationMessage EXCLUDES generated columns (pre-PG18
		// they are not published), so projectRelation never sees them. A
		// seed that keeps a generated column would diff as a phantom
		// ShapeKindDropColumn — silent destruction under ADR-0091's
		// default-on forwarding. The normalizer must drop generated
		// columns; IDENTITY columns (AutoIncrement, not GeneratedExpr)
		// are published and must NOT be dropped.
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "qty", Type: ir.Integer{Width: 32}},
				{Name: "total", Type: ir.Decimal{Precision: 20, Scale: 2}, GeneratedExpr: "qty * price", GeneratedStored: true},
			},
		}
		out := eng.NormalizeForCDCComparison(in)
		names := make([]string, 0, len(out.Columns))
		for _, c := range out.Columns {
			names = append(names, c.Name)
		}
		if len(out.Columns) != 2 || names[0] != "id" || names[1] != "qty" {
			t.Errorf("columns = %v; want [id qty] (generated 'total' must be dropped; IDENTITY 'id' kept)", names)
		}
	})

	t.Run("secondary_indexes_dropped_adr0091", func(t *testing.T) {
		// pgoutput's RelationMessage carries no secondary index metadata
		// (only the replica-identity key-flag), so projectRelation leaves
		// Indexes nil. A seed that keeps secondary indexes would diff as a
		// phantom ShapeKindDropIndex. PrimaryKey is preserved (the
		// key-flag carries it).
		in := &ir.Table{
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "email", Type: ir.Varchar{Length: 100}}},
			PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes:    []*ir.Index{{Name: "ix_email", Columns: []ir.IndexColumn{{Column: "email"}}}},
		}
		out := eng.NormalizeForCDCComparison(in)
		if out.Indexes != nil {
			t.Errorf("Indexes = %v; want nil (pgoutput carries no secondary index metadata)", out.Indexes)
		}
		if out.PrimaryKey == nil {
			t.Errorf("PrimaryKey was dropped; want preserved (carried via the replica-identity key-flag)")
		}
	})

	t.Run("nil_table", func(t *testing.T) {
		if got := eng.NormalizeForCDCComparison(nil); got != nil {
			t.Errorf("NormalizeForCDCComparison(nil) = %v; want nil", got)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "name", Type: ir.Varchar{Length: 64, Collation: "en_US.utf8"}, Nullable: true},
				{Name: "amount", Type: ir.Decimal{Unconstrained: true}, Nullable: true, Default: ir.DefaultLiteral{Value: "0"}, Comment: "money"},
			},
		}
		once := eng.NormalizeForCDCComparison(in)
		twice := eng.NormalizeForCDCComparison(once)
		for i := range once.Columns {
			if once.Columns[i].Type != twice.Columns[i].Type {
				t.Errorf("col %d: once.Type=%#v twice.Type=%#v; normalizer must be idempotent",
					i, once.Columns[i].Type, twice.Columns[i].Type)
			}
			if once.Columns[i].Nullable != twice.Columns[i].Nullable {
				t.Errorf("col %d: once.Nullable=%v twice.Nullable=%v; normalizer must be idempotent",
					i, once.Columns[i].Nullable, twice.Columns[i].Nullable)
			}
			if once.Columns[i].Default != twice.Columns[i].Default {
				t.Errorf("col %d: once.Default=%#v twice.Default=%#v; normalizer must be idempotent",
					i, once.Columns[i].Default, twice.Columns[i].Default)
			}
			if once.Columns[i].Comment != twice.Columns[i].Comment {
				t.Errorf("col %d: once.Comment=%q twice.Comment=%q; normalizer must be idempotent",
					i, once.Columns[i].Comment, twice.Columns[i].Comment)
			}
		}
	})

	t.Run("interface_satisfied", func(_ *testing.T) {
		// Compile-time check that Engine satisfies the optional
		// ir.CDCSchemaSnapshotNormalizer interface.
		var _ ir.CDCSchemaSnapshotNormalizer = Engine{}
	})
}
