// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"math"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// mustPrepare is the value-shaping oracle the per-row path used
// (prepareApplierValue); the batched builder MUST bind byte-identical values.
func mustPrepare(t *testing.T, v any, colTypes map[string]*ir.Column, col string) any {
	t.Helper()
	out, err := prepareApplierValue(v, colTypes, col)
	if err != nil {
		t.Fatalf("prepareApplierValue(%v, %q): %v", v, col, err)
	}
	return out
}

// TestBuildFloatRepairBatchSQL_SinglePK pins the batched MySQL statement shape
// and the row-major arg order/values for a single-column PK with two FLOAT
// SET columns, across the value-shaping families the FLOAT repair hits: a
// normal float, a NULL float, and −0.0 (which prepareValue renders as the
// literal "-0" so the sign survives interpolation).
func TestBuildFloatRepairBatchSQL_SinglePK(t *testing.T) {
	colTypes := map[string]*ir.Column{
		"id": {Name: "id", Type: ir.Integer{Width: 64}},
		"a":  {Name: "a", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		"b":  {Name: "b", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
	}
	batch := []ir.Row{
		{"id": int64(1), "a": float64(float32(8388608)), "b": math.Copysign(0, -1)},
		{"id": int64(2), "a": nil, "b": float64(float32(-123456.789))},
	}
	pkColumns := []string{"id"}
	setColumns := []string{"a", "b"} // sorted, as the skeleton derives them

	gotSQL, gotArgs, err := buildFloatRepairBatchSQL("`frepair`", pkColumns, setColumns, batch, colTypes)
	if err != nil {
		t.Fatal(err)
	}

	wantSQL := "UPDATE `frepair` AS tgt JOIN (" +
		"SELECT ? AS `id`, ? AS `a`, ? AS `b`" +
		" UNION ALL SELECT ?, ?, ?" +
		") AS v ON tgt.`id` = v.`id`" +
		" SET tgt.`a` = v.`a`, tgt.`b` = v.`b`"
	if gotSQL != wantSQL {
		t.Errorf("SQL mismatch:\n got: %s\nwant: %s", gotSQL, wantSQL)
	}

	// Args are row-major over [pk..., set...] = [id, a, b].
	wantArgs := []any{
		mustPrepare(t, batch[0]["id"], colTypes, "id"),
		mustPrepare(t, batch[0]["a"], colTypes, "a"),
		mustPrepare(t, batch[0]["b"], colTypes, "b"),
		mustPrepare(t, batch[1]["id"], colTypes, "id"),
		mustPrepare(t, batch[1]["a"], colTypes, "a"),
		mustPrepare(t, batch[1]["b"], colTypes, "b"),
	}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Errorf("args mismatch:\n got: %#v\nwant: %#v", gotArgs, wantArgs)
	}

	// Cross-check byte-identity against the per-row builders: for each row the
	// batched values for the SET columns match buildSetClause, and the values
	// for the PK columns match buildWhereClause.
	for r, row := range batch {
		after := ir.Row{"a": row["a"], "b": row["b"]}
		before := ir.Row{"id": row["id"]}
		_, setArgs, err := buildSetClause(after, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		_, whereArgs, err := buildWhereClause(before, colTypes)
		if err != nil {
			t.Fatal(err)
		}
		// batched row slice = [id, a, b]; per-row set = [a, b], where = [id].
		base := r * 3
		if !reflect.DeepEqual(gotArgs[base+1:base+3], setArgs) {
			t.Errorf("row %d: SET values diverge from per-row: %#v vs %#v", r, gotArgs[base+1:base+3], setArgs)
		}
		if !reflect.DeepEqual(gotArgs[base:base+1], whereArgs) {
			t.Errorf("row %d: PK values diverge from per-row: %#v vs %#v", r, gotArgs[base:base+1], whereArgs)
		}
	}
}

// TestBuildFloatRepairBatchSQL_CompositePK pins the multi-column join and the
// arg order when the PK spans two columns.
func TestBuildFloatRepairBatchSQL_CompositePK(t *testing.T) {
	colTypes := map[string]*ir.Column{
		"x":  {Name: "x", Type: ir.Integer{Width: 32}},
		"y":  {Name: "y", Type: ir.Integer{Width: 32}},
		"fl": {Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
	}
	batch := []ir.Row{
		{"x": int64(1), "y": int64(2), "fl": float64(float32(1.5))},
		{"x": int64(3), "y": int64(4), "fl": float64(float32(2.5))},
	}
	gotSQL, gotArgs, err := buildFloatRepairBatchSQL("`t`", []string{"x", "y"}, []string{"fl"}, batch, colTypes)
	if err != nil {
		t.Fatal(err)
	}
	wantSQL := "UPDATE `t` AS tgt JOIN (" +
		"SELECT ? AS `x`, ? AS `y`, ? AS `fl`" +
		" UNION ALL SELECT ?, ?, ?" +
		") AS v ON tgt.`x` = v.`x` AND tgt.`y` = v.`y`" +
		" SET tgt.`fl` = v.`fl`"
	if gotSQL != wantSQL {
		t.Errorf("SQL mismatch:\n got: %s\nwant: %s", gotSQL, wantSQL)
	}
	if len(gotArgs) != 6 {
		t.Fatalf("want 6 args, got %d", len(gotArgs))
	}
}
