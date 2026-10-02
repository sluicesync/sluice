// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// temporalClassFamilies enumerates every IR temporal family the PG
// normalizer's bare≡(6) collapse dispatches on — the Bug-74 "pin
// the class, not the representative" discipline. make builds the
// family's type at a given (precision, unspecified) pair; canonical is
// the family's ONE collapsed form ({Precision:0,
// PrecisionUnspecified:true} with tz-ness preserved).
var temporalClassFamilies = []struct {
	name      string
	make      func(prec int, unspec bool) ir.Type
	canonical ir.Type
}{
	{
		name:      "DateTime",
		make:      func(p int, u bool) ir.Type { return ir.DateTime{Precision: p, PrecisionUnspecified: u} },
		canonical: ir.DateTime{PrecisionUnspecified: true},
	},
	{
		name:      "Time",
		make:      func(p int, u bool) ir.Type { return ir.Time{Precision: p, PrecisionUnspecified: u} },
		canonical: ir.Time{PrecisionUnspecified: true},
	},
	{
		name: "TimeTZ",
		make: func(p int, u bool) ir.Type {
			return ir.Time{Precision: p, WithTimeZone: true, PrecisionUnspecified: u}
		},
		canonical: ir.Time{WithTimeZone: true, PrecisionUnspecified: true},
	},
	{
		name: "TimestampTZ",
		make: func(p int, u bool) ir.Type {
			return ir.Timestamp{Precision: p, WithTimeZone: true, PrecisionUnspecified: u}
		},
		canonical: ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true},
	},
	{
		// PG maps `timestamp without time zone` to ir.DateTime, so this
		// shape is not produced by the PG reader — but the normalizer
		// dispatches on the ir.Timestamp FAMILY regardless of tz-ness,
		// so the tz-less variant is pinned too (class, not
		// representative).
		name:      "Timestamp_no_tz",
		make:      func(p int, u bool) ir.Type { return ir.Timestamp{Precision: p, PrecisionUnspecified: u} },
		canonical: ir.Timestamp{PrecisionUnspecified: true},
	},
}

// temporalClassMembers are every representation of the bare≡(6)
// equivalence class that any IR source has ever produced for a
// same-class column:
//
//   - {0, unspecified}: the TRIAGE-#3 bare form — what BOTH the
//     SchemaReader (atttypmod=-1) and the CDC projection (typmod=-1)
//     emit today.
//   - {6, explicit}: the pre-TRIAGE-#3 SchemaReader materialization of
//     a bare column (information_schema datetime_precision=6), old
//     persisted schema-history/lease rows, and an explicitly declared
//     (6).
//
// An explicit {0, false} is NOT a member (GC-44 F3): it is a declared
// (0), which holds whole seconds only — see temporalClassOutsider.
var temporalClassMembers = []struct {
	name   string
	prec   int
	unspec bool
}{
	{name: "bare_unspecified", prec: 0, unspec: true},
	{name: "explicit_6", prec: 6, unspec: false},
}

// temporalClassOutsider is the declared (0): it used to sit in the class
// (a pre-TRIAGE-#3 CDC mapper decoded typmod -1 as 0), and keeping it
// there made `timestamp(0)` → `timestamp(6)` classify as no change, so the
// forward never ran and the target rounded every following value (GC-44
// F3). It must stay distinct from every member, in every family.
var temporalClassOutsider = struct {
	name   string
	prec   int
	unspec bool
}{name: "explicit_0", prec: 0, unspec: false}

// TestNormalizeForCDCComparison_TemporalZeroIsNotInTheClass pins GC-44 F3
// across every temporal family: a declared (0) survives the lens
// unchanged, and against each class member it classifies as an ALTER
// COLUMN TYPE in both directions — the boundary the forward needs.
func TestNormalizeForCDCComparison_TemporalZeroIsNotInTheClass(t *testing.T) {
	t.Parallel()
	eng := Engine{}
	o := temporalClassOutsider
	for _, fam := range temporalClassFamilies {
		t.Run(fam.name+"/passthrough", func(t *testing.T) {
			in := &ir.Table{Columns: []*ir.Column{{Name: "ts", Type: fam.make(o.prec, o.unspec)}}}
			out := eng.NormalizeForCDCComparison(in)
			if got := out.Columns[0].Type; !reflect.DeepEqual(got, fam.make(o.prec, o.unspec)) {
				t.Errorf("normalize(%#v) = %#v; want unchanged — a declared (0) is not in the bare≡(6) class",
					fam.make(o.prec, o.unspec), got)
			}
		})
		for _, m := range temporalClassMembers {
			for _, dir := range []struct {
				name      string
				pre, post ir.Type
			}{
				{"zero_to_" + m.name, fam.make(o.prec, o.unspec), fam.make(m.prec, m.unspec)},
				{m.name + "_to_zero", fam.make(m.prec, m.unspec), fam.make(o.prec, o.unspec)},
			} {
				t.Run(fam.name+"/"+dir.name, func(t *testing.T) {
					mk := func(ts ir.Type) *ir.Table {
						return eng.NormalizeForCDCComparison(&ir.Table{Schema: "public", Name: "w", Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 32}},
							{Name: "ts", Type: ts, Nullable: true},
						}})
					}
					shape, err := pipeline.ClassifyShape(mk(dir.pre), mk(dir.post))
					if err != nil {
						t.Fatalf("ClassifyShape: %v", err)
					}
					if shape.Kind != pipeline.ShapeKindAlterColumnType {
						t.Errorf("ClassifyShape(%v → %v) = %s; want alter-column-type — the change would never be "+
							"forwarded and the target would keep its precision", dir.pre, dir.post, shape.Kind)
					}
				})
			}
		}
	}
}

// TestNormalizeForCDCComparison_TemporalClassCanonicalForm pins the
// TRIAGE-#3 regression fix: every member of the temporal bare≡(6)
// class collapses to the ONE canonical bare form, for every temporal
// family. The canonical form MUST be the bare/unspecified one — it is
// what both current IR sources natively emit, and it is the only class
// member that stays value-safe when a normalized type reaches a shape
// payload the ShapeDeltaApplier materializes on the target (bare emits
// bare, 6-behaving; an explicit (0) would emit a TRUNCATING
// timestamp(0) since TRIAGE #3 made known precisions always emit).
//
// The pre-fix canonical was {Precision:0, PrecisionUnspecified:false},
// which no current IR source emits for a bare column — that mismatch
// against the CDC projection's {0, unspecified} was the phantom
// altered-col=true that stalled the rename coordination on every bare
// temporal column (TypeFamilyMatrix/extra_timestamp_nullable).
func TestNormalizeForCDCComparison_TemporalClassCanonicalForm(t *testing.T) {
	t.Parallel()
	eng := Engine{}
	for _, fam := range temporalClassFamilies {
		for _, m := range temporalClassMembers {
			t.Run(fam.name+"/"+m.name, func(t *testing.T) {
				in := &ir.Table{Columns: []*ir.Column{{Name: "ts", Type: fam.make(m.prec, m.unspec)}}}
				out := eng.NormalizeForCDCComparison(in)
				if got := out.Columns[0].Type; !reflect.DeepEqual(got, fam.canonical) {
					t.Errorf("normalize(%#v) = %#v; want canonical %#v", fam.make(m.prec, m.unspec), got, fam.canonical)
				}
			})
		}
		t.Run(fam.name+"/mid_precision_passthrough", func(t *testing.T) {
			// Precisions outside the class (1–5) stay distinct: a genuine
			// timestamp(3) → timestamp(5) ALTER must still classify.
			in := &ir.Table{Columns: []*ir.Column{{Name: "ts", Type: fam.make(3, false)}}}
			out := eng.NormalizeForCDCComparison(in)
			if got := out.Columns[0].Type; !reflect.DeepEqual(got, fam.make(3, false)) {
				t.Errorf("normalize(%#v) = %#v; want unchanged", fam.make(3, false), got)
			}
		})
	}
}

// TestClassifyShape_TemporalClass_NoPhantomAlter_BothSidesNormalized
// pins the full comparison contract the live-coordination intercepts
// now implement: BOTH classifier sides pass through
// NormalizeForCDCComparison (the seed at synthesis, each CDC snapshot
// at intake), so ANY two representations of a same-class temporal
// column compare equal — {bare, (6)} × {bare, (6)}, for
// every temporal family:
//
//   - An otherwise-identical table must classify ShapeKindNone (no
//     phantom altered-col).
//   - The Bug-86 shape — a genuine RENAME COLUMN alongside the
//     same-class temporal column — must classify ShapeKindRenameColumn
//     instead of degrading into the multi-shape combo refusal
//     (added=1 dropped=1 altered-col=true) that stalled the
//     TypeFamilyMatrix extra_timestamp_nullable cell.
func TestClassifyShape_TemporalClass_NoPhantomAlter_BothSidesNormalized(t *testing.T) {
	t.Parallel()
	eng := Engine{}
	for _, fam := range temporalClassFamilies {
		for _, seedRep := range temporalClassMembers {
			for _, cdcRep := range temporalClassMembers {
				name := fmt.Sprintf("%s/seed_%s_vs_cdc_%s", fam.name, seedRep.name, cdcRep.name)
				t.Run(name, func(t *testing.T) {
					mkTable := func(nameCol string, tsType ir.Type) *ir.Table {
						return &ir.Table{
							Schema: "public",
							Name:   "widgets",
							Columns: []*ir.Column{
								{Name: "id", Type: ir.Integer{Width: 32}},
								{Name: nameCol, Type: ir.Varchar{Length: 64}},
								{Name: "ts", Type: tsType, Nullable: true},
							},
						}
					}
					// Both sides normalized — exactly what the intercepts do
					// (seed at synthesis, CDC post at intake).
					pre := eng.NormalizeForCDCComparison(mkTable("name", fam.make(seedRep.prec, seedRep.unspec)))

					samePost := eng.NormalizeForCDCComparison(mkTable("name", fam.make(cdcRep.prec, cdcRep.unspec)))
					shape, err := pipeline.ClassifyShape(pre, samePost)
					if err != nil {
						t.Fatalf("ClassifyShape(same table): %v", err)
					}
					if shape.Kind != pipeline.ShapeKindNone {
						t.Errorf("ClassifyShape(same table) = %s; want none (phantom alter on the %s column)",
							shape.Kind, fam.name)
					}

					renamePost := eng.NormalizeForCDCComparison(mkTable("product_name", fam.make(cdcRep.prec, cdcRep.unspec)))
					shape, err = pipeline.ClassifyShape(pre, renamePost)
					if err != nil {
						t.Fatalf("ClassifyShape(rename boundary): %v (the Bug-86 combo-refusal regression shape)", err)
					}
					if shape.Kind != pipeline.ShapeKindRenameColumn {
						t.Errorf("ClassifyShape(rename boundary) = %s; want rename-column", shape.Kind)
					}
				})
			}
		}
	}
}
