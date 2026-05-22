// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
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

	t.Run("nil_table", func(t *testing.T) {
		if got := eng.NormalizeForCDCComparison(nil); got != nil {
			t.Errorf("NormalizeForCDCComparison(nil) = %v; want nil", got)
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		in := &ir.Table{
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "name", Type: ir.Varchar{Length: 64, Collation: "en_US.utf8"}},
				{Name: "amount", Type: ir.Decimal{Unconstrained: true}},
			},
		}
		once := eng.NormalizeForCDCComparison(in)
		twice := eng.NormalizeForCDCComparison(once)
		for i := range once.Columns {
			if once.Columns[i].Type != twice.Columns[i].Type {
				t.Errorf("col %d: once=%#v twice=%#v; normalizer must be idempotent",
					i, once.Columns[i].Type, twice.Columns[i].Type)
			}
		}
	})

	t.Run("interface_satisfied", func(_ *testing.T) {
		// Compile-time check that Engine satisfies the optional
		// ir.CDCSchemaSnapshotNormalizer interface.
		var _ ir.CDCSchemaSnapshotNormalizer = Engine{}
	})
}
