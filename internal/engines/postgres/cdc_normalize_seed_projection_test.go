// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// typmodFamilyShape is one typmod-capable family in one declaration shape:
// the information_schema metadata the SchemaReader builds its columnMeta
// from, and the (OID, typmod) pgoutput's RelationMessage carries for the
// same column.
type typmodFamilyShape struct {
	name     string
	oid      uint32
	typmod   int32
	dataType string     // information_schema data_type
	meta     columnMeta // the information_schema modifiers for a SCALAR column
}

// typmodFamilyShapes is the universe of TestNormalizeForCDCComparison_PG_SeedAgreesWithBoundaryProjection:
// every PG built-in with a typmod input function (pg_type.typmodin != 0)
// inside oidToType's domain — the same PG-catalog roster the
// TYPMOD-PROJECTION-GATE uses, deliberately not derived from either
// registry — each DECLARED and BARE (typmod -1). The bare shape is the one
// GC-36 was: the two registries decode different inputs there
// (information_schema NULLs vs typmod -1), and the representatives every
// earlier pin used were all declared.
func typmodFamilyShapes() []typmodFamilyShape {
	numericTM := int32(((10 << 16) | 2) + 4)
	intervalTM := int32((0x7FFF << 16) | 3)
	return []typmodFamilyShape{
		{"numeric(10,2)", pgtype.NumericOID, numericTM, "numeric", columnMeta{NumPrec: i64p(10), NumScale: i64p(2)}},
		{"numeric", pgtype.NumericOID, -1, "numeric", columnMeta{}},
		{"varchar(10)", pgtype.VarcharOID, 14, "character varying", columnMeta{CharMaxLen: i64p(10)}},
		{"varchar", pgtype.VarcharOID, -1, "character varying", columnMeta{}},
		{"char(10)", pgtype.BPCharOID, 14, "character", columnMeta{CharMaxLen: i64p(10)}},
		{"bpchar", pgtype.BPCharOID, -1, "character", columnMeta{}},
		{"bit(3)", pgtype.BitOID, 3, "bit", columnMeta{CharMaxLen: i64p(3)}},
		{"varbit(3)", pgtype.VarbitOID, 3, "bit varying", columnMeta{CharMaxLen: i64p(3)}},
		{"varbit", pgtype.VarbitOID, -1, "bit varying", columnMeta{}},
		// information_schema reports datetime_precision 6 (the engine
		// default) for a bare temporal column — the reader keys
		// declaredness on atttypmod, not on that.
		{"time(3)", pgtype.TimeOID, 3, "time without time zone", columnMeta{DTPrec: i64p(3)}},
		{"time", pgtype.TimeOID, -1, "time without time zone", columnMeta{DTPrec: i64p(6)}},
		{"timetz(3)", pgtype.TimetzOID, 3, "time with time zone", columnMeta{DTPrec: i64p(3)}},
		{"timetz", pgtype.TimetzOID, -1, "time with time zone", columnMeta{DTPrec: i64p(6)}},
		{"timestamp(3)", pgtype.TimestampOID, 3, "timestamp without time zone", columnMeta{DTPrec: i64p(3)}},
		{"timestamp", pgtype.TimestampOID, -1, "timestamp without time zone", columnMeta{DTPrec: i64p(6)}},
		{"timestamptz(3)", pgtype.TimestamptzOID, 3, "timestamp with time zone", columnMeta{DTPrec: i64p(3)}},
		{"timestamptz", pgtype.TimestamptzOID, -1, "timestamp with time zone", columnMeta{DTPrec: i64p(6)}},
		{"interval(3)", pgtype.IntervalOID, intervalTM, "interval", columnMeta{}},
		{"interval", pgtype.IntervalOID, -1, "interval", columnMeta{}},
	}
}

// TestNormalizeForCDCComparison_PG_SeedAgreesWithBoundaryProjection is the
// Postgres counterpart of the binlog pin of the same name, and the GC-36
// gate. For every typmod-capable family × {declared, bare} × {scalar,
// array}, the SchemaReader's type (the cold-start seed) and the pgoutput
// projection of the SAME column must agree at two levels:
//
//   - RAW, for scalars: the forwarded ADD COLUMN is emitted from the raw
//     projection, so this is what makes a mid-stream column land exactly as
//     a cold-start migrate would land it. GC-36 was a raw split — a bare
//     numeric seeded Decimal{Unconstrained} and projected Decimal{0,0}, so
//     the forward emitted NUMERIC(0,0) (PG target: refused) / DECIMAL(0,0)
//     (MySQL target: every value truncated to an integer, silently).
//     Arrays are exempt from this level BY DESIGN — the projection resolves
//     the element at typmod -1 (oidToType's array arm; the
//     TYPMOD-PROJECTION-GATE refuse list) — and instead must project the
//     element at the family's widest, modifier-free form.
//   - THROUGH THE LENS, for all: seed vs projection classifies None, and
//     seed vs projection-plus-one-column classifies AddColumn (the GC-1
//     probe — a phantom alter alongside a real ADD COLUMN is the multi-shape
//     combo refusal, which halts the stream at its first forwarded column).
//
// The seed side is built the way the SchemaReader builds it (schema_reader.go:
// the array column's atttypmod threaded onto the element meta, with no
// information_schema modifiers), and the projection through the production
// buildRelationCacheEntry + projectRelation. The real-server leg of the same
// argument is TestSeedAgreesWithPgoutputProjection_TypmodFamilies
// (integration), which lets PG itself supply both sides.
func TestNormalizeForCDCComparison_PG_SeedAgreesWithBoundaryProjection(t *testing.T) {
	t.Parallel()
	eng := Engine{}
	shapes := typmodFamilyShapes()

	// Array universe derived from the production element map, so an element
	// family added there joins the matrix without an edit here.
	elemSpelling := map[uint32]string{}
	for _, s := range shapes {
		elemSpelling[s.oid] = s.dataType
	}
	type cell struct {
		name    string
		oid     uint32
		typmod  int32
		seed    columnMeta
		isArray bool
	}
	var cells []cell
	for _, s := range shapes {
		m := s.meta
		m.DataType = s.dataType
		m.AttTypmod = s.typmod
		cells = append(cells, cell{name: s.name, oid: s.oid, typmod: s.typmod, seed: m})
	}
	arrays := 0
	for arrOID, elemOID := range pgArrayElementOID {
		for _, s := range shapes {
			if s.oid != elemOID {
				continue
			}
			arrays++
			cells = append(cells, cell{
				name: s.name + "[]", oid: arrOID, typmod: s.typmod, isArray: true,
				seed: columnMeta{
					DataType:     "ARRAY",
					AttTypmod:    s.typmod,
					ArrayElement: &columnMeta{DataType: elemSpelling[elemOID], AttTypmod: s.typmod},
				},
			})
		}
	}
	// Anti-vacuity floors: 19 scalar shapes, and at least the numeric /
	// varchar / bpchar / four temporal arrays in both shapes (interval[] is not
	// an array family either registry resolves).
	if len(shapes) != 19 || arrays < 14 {
		t.Fatalf("matrix holds %d scalar shapes / %d array shapes; floors 19 / 14 — the universe went vacuous", len(shapes), arrays)
	}

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			seedType, err := translateType(c.seed)
			if err != nil {
				t.Fatalf("schema reader: %v", err)
			}
			entry, err := buildRelationCacheEntry(pglogrepl.RelationMessage{
				Namespace: "public", RelationName: "w", ColumnNum: 2,
				Columns: []*pglogrepl.RelationMessageColumn{
					{Flags: 1, Name: "id", DataType: pgtype.Int4OID, TypeModifier: -1},
					{Name: "v", DataType: c.oid, TypeModifier: c.typmod},
				},
			}, 0, nil, nil)
			if err != nil {
				t.Fatalf("buildRelationCacheEntry: %v", err)
			}
			proj := projectRelation(entry)
			projType := proj.Columns[1].Type

			if !c.isArray && !reflect.DeepEqual(seedType, projType) {
				t.Errorf("raw split: schema reader %#v, pgoutput projection %#v — a forwarded ADD COLUMN of this column lands differently from a migrate of it", seedType, projType)
			}
			if c.isArray {
				elem := projType.(ir.Array).Element
				if want := eraseArrayElementModifier(elem); !reflect.DeepEqual(elem, want) {
					t.Errorf("projected array element %#v carries a modifier; the projection resolves elements at typmod -1 and must land on the widest form %#v", elem, want)
				}
			}

			seedTbl := &ir.Table{Schema: "public", Name: "w", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "v", Type: seedType, Nullable: true},
			}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}}
			pre := eng.NormalizeForCDCComparison(seedTbl)

			shape, err := pipeline.ClassifyShape(pre, eng.NormalizeForCDCComparison(proj))
			if err != nil || shape.Kind != pipeline.ShapeKindNone {
				t.Errorf("seed vs projection of the SAME column = %v (err %v); want None — a phantom alter", shape.Kind, err)
			}

			withAdded := *proj
			withAdded.Columns = append(append([]*ir.Column(nil), proj.Columns...), &ir.Column{Name: "b", Type: ir.Integer{Width: 32}})
			shape, err = pipeline.ClassifyShape(pre, eng.NormalizeForCDCComparison(&withAdded))
			if err != nil {
				t.Fatalf("ADD COLUMN beside this column refused: %v (the phantom-alter combo refusal)", err)
			}
			if shape.Kind != pipeline.ShapeKindAddColumn {
				t.Errorf("ADD COLUMN beside this column = %s; want add-column", shape.Kind)
			}
		})
	}
}
