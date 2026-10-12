// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// col is a column spec for deferredShape: a name, its IR type, and whether
// the shape typed it from the TABLE_MAP alone.
type col struct {
	name        string
	typ         ir.Type
	approximate bool
	// unnamedID, when set, is a recorded collation ID the source's table did
	// not name (review F4).
	unnamedID uint64
}

func c(name string, typ ir.Type) col { return col{name: name, typ: typ} }

func tm(name string, typ ir.Type) col { return col{name: name, typ: typ, approximate: true} }

// tmUnnamed is tm for a character column whose recorded collation ID could
// not be named, so the type carries no collation.
func tmUnnamed(name string, typ ir.Type, id uint64) col {
	return col{name: name, typ: typ, approximate: true, unnamedID: id}
}

var (
	tInt  = ir.Integer{Width: 32}
	tBig  = ir.Integer{Width: 64}
	tDT0  = ir.DateTime{Precision: 0}
	tDT6  = ir.DateTime{Precision: 6}
	tDec2 = ir.Decimal{Precision: 10, Scale: 2}
	tDec4 = ir.Decimal{Precision: 12, Scale: 4}
	tVC20 = ir.Varchar{Length: 20, Charset: "utf8mb4"}
	tVC5  = ir.Varchar{Length: 5, Charset: "utf8mb4"}
	tVC80 = ir.Varchar{Length: 80, Charset: "utf8mb4"} // VARCHAR(20) utf8mb4 as the TABLE_MAP records it, in bytes
	// tVCci / tVC80bin: a key column before and (recorded) after
	// `MODIFY … COLLATE utf8mb4_bin` (DEFERRED-KEY-COLLATION).
	tVCci    = ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	tVC80bin = ir.Varchar{Length: 80, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
)

// deferredShape builds a shape; synthesized when any column is approximate
// or force is set.
func deferredShape(force bool, cols ...col) *tableSchema {
	tbl := &tableSchema{Schema: "app", Name: "kd", synthesized: force}
	for _, k := range cols {
		tbl.Columns = append(tbl.Columns, &ir.Column{Name: k.name, Type: k.typ})
		if k.approximate {
			tbl.synthesized = true
			tbl.tableMapTyped = append(tbl.tableMapTyped, k.name)
		}
		if k.unnamedID != 0 {
			if tbl.unnamedCollations == nil {
				tbl.unnamedCollations = map[string]uint64{}
			}
			tbl.unnamedCollations[k.name] = k.unnamedID
		}
	}
	return tbl
}

func baseline(cols ...col) []*ir.Table {
	t := &ir.Table{Name: "kd"} // no Schema: takes the reader's bound database
	for _, k := range cols {
		t.Columns = append(t.Columns, &ir.Column{Name: k.name, Type: k.typ})
	}
	return []*ir.Table{t}
}

// TestHoldDecodedOrder_DeferredShapeMatrix pins the reader-side door for
// shapes that will never be a boundary (BUG301-FULL-DEFERRED-SWAP,
// BUG301-DEFERRED-RETYPE): such a shape may add or lose columns, but may
// not reorder or retype one it shares with the basis; a column it typed
// from the TABLE_MAP is compared only on what the TABLE_MAP records (every
// such property has a case, review H1); a shape that IS a boundary passes
// and becomes the basis; once a baseline is given, a table missing from it
// refuses its first deferred shape.
func TestHoldDecodedOrder_DeferredShapeMatrix(t *testing.T) {
	type step struct {
		shape *tableSchema
		want  error // nil: passes
	}
	order, typed := ir.ErrColumnOrderChanged, ir.ErrDeferredSchemaChange
	cases := []struct {
		name     string
		baseline []*ir.Table // nil: none handed over
		steps    []step
	}{
		{"swap", baseline(c("id", tInt), c("b", tInt), c("c", tInt)), []step{{deferredShape(true, c("id", tInt), c("c", tInt), tm("b", tInt)), order}}},
		{"rotation", baseline(c("id", tInt), c("b", tInt), c("c", tInt), c("d", tInt)), []step{{deferredShape(true, c("id", tInt), c("c", tInt), c("d", tInt), tm("b", tInt)), order}}},
		{"pre-drop row keeps order and types", baseline(c("id", tInt), c("b", tVC20), c("c", tInt)), []step{{deferredShape(false, c("id", tInt), c("b", tVC20), tm("c", tInt)), nil}}},
		{"added column", baseline(c("id", tInt), c("b", tInt)), []step{{deferredShape(false, c("id", tInt), c("b", tInt), c("d", tInt), tm("x", tInt)), nil}}},
		{"one shared name cannot flip", baseline(c("id", tInt), c("b", tInt), c("c", tInt)), []step{{deferredShape(false, tm("x", tInt), c("c", tInt)), nil}}},
		{"datetime precision", baseline(c("id", tInt), c("b", tDT0), c("c", tInt)), []step{{deferredShape(false, c("id", tInt), c("b", tDT6), tm("c", tInt)), typed}}},
		{"decimal scale", baseline(c("id", tInt), c("b", tDec2), c("c", tInt)), []step{{deferredShape(false, c("id", tInt), c("b", tDec4), tm("c", tInt)), typed}}},
		{"varchar shrink", baseline(c("id", tInt), c("b", tVC20), c("c", tInt)), []step{{deferredShape(false, c("id", tInt), c("b", tVC5), tm("c", tInt)), typed}}},
		{"integer widen", baseline(c("id", tInt), c("b", tInt), c("c", tInt)), []step{{deferredShape(false, c("id", tInt), c("b", tBig), tm("c", tInt)), typed}}},
		{"approximate: a byte length within the basis holds", baseline(c("id", tInt), c("b", tVC20), c("c", tInt)), []step{{deferredShape(false, c("id", tInt), tm("b", tVC80), c("c", tInt)), nil}}},
		{"retype that is a boundary passes", baseline(c("id", tInt), c("b", tDT0)), []step{{deferredShape(false, c("id", tInt), c("b", tDT6)), nil}}},
		{"swap that is a boundary passes", baseline(c("id", tInt), c("b", tInt), c("c", tInt)), []step{{deferredShape(false, c("id", tInt), c("c", tInt), c("b", tInt)), nil}}},
		{"no baseline handed over, nothing to compare", nil, []step{{deferredShape(true, c("id", tInt), c("c", tInt), tm("b", tInt)), nil}}},
		{"baseline lacks the table", []*ir.Table{}, []step{{deferredShape(true, c("id", tInt), c("b", tInt), tm("c", tInt)), typed}}},
		{
			"previous decoded event is the basis", nil,
			[]step{
				{deferredShape(false, c("id", tInt), c("b", tDT0), tm("c", tInt)), nil},
				{deferredShape(false, c("id", tInt), c("b", tDT6), tm("c", tInt)), typed},
			},
		},
		{
			"a boundary becomes the basis", baseline(c("id", tInt), c("b", tDT0), c("c", tInt)),
			[]step{
				{deferredShape(false, c("id", tInt), c("b", tDT6), c("c", tInt)), nil},
				{deferredShape(false, c("id", tInt), c("b", tDT6), tm("c", tInt)), nil},
			},
		},
		{
			"an approximate column carries its known type forward", baseline(c("id", tInt), c("b", tVC20), c("c", tInt)),
			[]step{
				{deferredShape(false, c("id", tInt), tm("b", tVC80), c("c", tInt)), nil},
				{deferredShape(false, c("id", tInt), c("b", tVC5), tm("c", tInt)), typed},
			},
		},
		// Review H1: an approximate column is still held on what the
		// TABLE_MAP records — a column retyped twice must not hide its first
		// retype behind the second.
		{"approximate: temporal precision", baseline(c("id", tInt), c("b", tDT0)), []step{{deferredShape(false, c("id", tInt), tm("b", tDT6)), typed}}},
		{"approximate: decimal scale", baseline(c("id", tInt), c("b", tDec2)), []step{{deferredShape(false, c("id", tInt), tm("b", tDec4)), typed}}},
		{"approximate: integer width", baseline(c("id", tInt), c("b", tInt)), []step{{deferredShape(false, c("id", tInt), tm("b", tBig)), typed}}},
		{"approximate: signedness", baseline(c("id", tInt), c("b", tInt)), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Integer{Width: 32, Unsigned: true})), typed}}},
		{"approximate: family", baseline(c("id", tInt), c("b", tDT6)), []step{{deferredShape(false, c("id", tInt), tm("b", tVC80)), typed}}},
		{"approximate: float precision", baseline(c("id", tInt), c("b", ir.Float{Precision: ir.FloatSingle})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Float{Precision: ir.FloatDouble})), typed}}},
		{"approximate: enum labels", baseline(c("id", tInt), c("b", ir.Enum{Values: []string{"a", "b"}})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Enum{Values: []string{"b", "a"}})), typed}}},
		{"approximate: character set", baseline(c("id", tInt), c("b", ir.Varchar{Length: 20, Charset: "latin1"})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Varchar{Length: 80, Charset: "utf8mb4"})), typed}}},
		{"approximate: text size class", baseline(c("id", tInt), c("b", ir.Text{Size: ir.TextMedium})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Text{Size: ir.TextLong})), typed}}},
		// Review of 3e9de74a: the TABLE_MAP records a character column's
		// length in bytes; a widen past what the basis could hold refuses,
		// a shrink holds (the target stays wider than any value written).
		{"approximate: varchar widened past the basis", baseline(c("id", tInt), c("b", tVC20)), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Varchar{Length: 400, Charset: "utf8mb4"})), typed}}},
		{"approximate: varchar one byte past the basis", baseline(c("id", tInt), c("b", ir.Varchar{Length: 20, Charset: "latin1"})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Varchar{Length: 21, Charset: "latin1"})), typed}}},
		{"approximate: varchar shrunk", baseline(c("id", tInt), c("b", tVC20)), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Varchar{Length: 20, Charset: "utf8mb4"})), nil}}},
		{"approximate: char widened", baseline(c("id", tInt), c("b", ir.Char{Length: 8, Charset: "utf8mb4"})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Char{Length: 36, Charset: "utf8mb4"})), typed}}},
		{"approximate: char at its byte bound", baseline(c("id", tInt), c("b", ir.Char{Length: 8, Charset: "utf8mb4"})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Char{Length: 32, Charset: "utf8mb4"})), nil}}},
		{"approximate: a character set sluice cannot size", baseline(c("id", tInt), c("b", ir.Varchar{Length: 20, Charset: "x-unknown"})), []step{{deferredShape(false, c("id", tInt), tm("b", ir.Varchar{Length: 1, Charset: "x-unknown"})), typed}}},
		// DEFERRED-KEY-COLLATION: the TABLE_MAP records a character column's
		// collation ID, which the reader names from the source's collation
		// table; a recorded collation that differs from the basis refuses.
		{"approximate: varchar collation changed", baseline(c("id", tInt), c("k", tVCci)), []step{{deferredShape(false, c("id", tInt), tm("k", tVC80bin)), typed}}},
		{"approximate: char collation changed", baseline(c("id", tInt), c("k", ir.Char{Length: 8, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"})), []step{{deferredShape(false, c("id", tInt), tm("k", ir.Char{Length: 32, Charset: "utf8mb4", Collation: "utf8mb4_bin"})), typed}}},
		{"approximate: text collation changed", baseline(c("id", tInt), c("k", ir.Text{Size: ir.TextRegular, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"})), []step{{deferredShape(false, c("id", tInt), tm("k", ir.Text{Size: ir.TextRegular, Charset: "utf8mb4", Collation: "utf8mb4_bin"})), typed}}},
		{"approximate: PAD SPACE to NO PAD", baseline(c("id", tInt), c("k", ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"})), []step{{deferredShape(false, c("id", tInt), tm("k", ir.Varchar{Length: 80, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"})), typed}}},
		{"approximate: the same collation holds", baseline(c("id", tInt), c("k", tVCci)), []step{{deferredShape(false, c("id", tInt), tm("k", ir.Varchar{Length: 80, Charset: "utf8mb4", Collation: "UTF8MB4_0900_AI_CI"})), nil}}},
		// Review F4: a recorded collation ID the source's table did not name
		// (or a table that could not be read) is not a pass where the basis
		// declares a collation; it is where the basis declares none.
		{"approximate: an unnamed collation ID against a declared basis", baseline(c("id", tInt), c("k", tVCci)), []step{{deferredShape(false, c("id", tInt), tmUnnamed("k", tVC80, 9999)), typed}}},
		{"approximate: an unnamed collation ID against an undeclared basis", baseline(c("id", tInt), c("k", tVC20)), []step{{deferredShape(false, c("id", tInt), tmUnnamed("k", tVC80, 9999)), nil}}},
		{
			"approximate: a collation carried forward is held",
			baseline(c("id", tInt), c("k", tVCci), c("c", tInt)),
			[]step{
				{deferredShape(false, c("id", tInt), tm("k", ir.Varchar{Length: 80, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}), c("c", tInt)), nil},
				{deferredShape(false, c("id", tInt), tm("k", tVC80bin), c("c", tInt)), typed},
			},
		},
		{
			"approximate: what the TABLE_MAP does not record is not compared",
			baseline(c("id", tInt),
				c("v", ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}),
				c("e", ir.Enum{Values: []string{"a"}, Collation: "utf8mb4_0900_ai_ci"}),
				c("n", ir.Integer{Width: 64, AutoIncrement: true})),
			[]step{{deferredShape(false, c("id", tInt),
				tm("v", ir.Varchar{Length: 80, Charset: "utf8mb4"}),
				tm("e", ir.Enum{Values: []string{"a"}}),
				tm("n", ir.Integer{Width: 64})), nil}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &CDCReader{schema: "app"}
			if tc.baseline != nil {
				r.SetDecodedShapeBaseline(tc.baseline)
			}
			for i, s := range tc.steps {
				err := r.holdDecodedOrder("app.kd", s.shape)
				switch {
				case s.want == nil && err != nil:
					t.Fatalf("step %d: err = %v, want none", i, err)
				case s.want != nil && !errors.Is(err, s.want):
					t.Fatalf("step %d: err = %v, want %v", i, err, s.want)
				case s.want != nil && !strings.Contains(err.Error(), "--reset-target-data"):
					t.Fatalf("step %d: refusal names no re-copy remedy: %v", i, err)
				}
			}
		})
	}
}
