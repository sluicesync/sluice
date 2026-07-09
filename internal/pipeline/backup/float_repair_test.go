// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"math"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// exactSourceEngine is a stubEngine whose OpenRowReader hands back a fake
// cursor reader over a fixed set of EXACT (PK + FLOAT) rows — standing in
// for the source vtgate the backup re-reads through the `(col * 1E0)`
// projection.
type exactSourceEngine struct {
	stubEngine
	rows []ir.Row // must be sorted by the single int64 PK "id"
}

func (e exactSourceEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return &fakeCursorReader{rows: e.rows}, nil
}

// fakeCursorReader implements ir.BatchedRowReader over an int64 "id" PK.
type fakeCursorReader struct {
	rows []ir.Row
	err  error
}

func (r *fakeCursorReader) Err() error { return r.err }

func (r *fakeCursorReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return r.emit(r.rows), nil
}

func (r *fakeCursorReader) ReadRowsBatch(_ context.Context, _ *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	var cursor int64 = math.MinInt64
	if len(after) == 1 {
		cursor = after[0].(int64)
	}
	var out []ir.Row
	for _, row := range r.rows {
		if row["id"].(int64) > cursor {
			out = append(out, row)
			if len(out) >= limit {
				break
			}
		}
	}
	return r.emit(out), nil
}

func (r *fakeCursorReader) emit(rows []ir.Row) <-chan ir.Row {
	ch := make(chan ir.Row, len(rows))
	for _, row := range rows {
		ch <- row
	}
	close(ch)
	return ch
}

// fakeInnerReader is the VStream COPY reader stand-in: it emits ROUNDED
// FLOAT values (what vttablet's rowstreamer would hand back).
type fakeInnerReader struct{ rows []ir.Row }

func (r *fakeInnerReader) Err() error { return nil }
func (r *fakeInnerReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row, len(r.rows))
	for _, row := range r.rows {
		ch <- row
	}
	close(ch)
	return ch, nil
}

// TestFloatExactPatchReader_PatchesRoundedFloats pins the backup default:
// the wrapper replaces each repairable table row's FLOAT columns with the
// EXACT source values keyed by PK, leaves non-FLOAT columns at their COPY
// values, and leaves a row absent from the exact map (inserted after the
// re-read) at its rounded COPY value.
func TestFloatExactPatchReader_PatchesRoundedFloats(t *testing.T) {
	table := &ir.Table{
		Name: "metrics",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
			{Name: "label", Type: ir.Varchar{Length: 8}},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	// Inner (COPY) rows carry ROUNDED floats + a label; id 3 exists only in
	// the COPY (inserted after the exact re-read window).
	inner := &fakeInnerReader{rows: []ir.Row{
		{"id": int64(1), "fl": float64(8388610), "label": "a"},
		{"id": int64(2), "fl": float64(-123457), "label": "b"},
		{"id": int64(3), "fl": float64(999), "label": "c"},
	}}
	// Exact source rows carry the TRUE float32 values for id 1 and 2.
	src := exactSourceEngine{rows: []ir.Row{
		{"id": int64(1), "fl": float64(float32(8388608))},
		{"id": int64(2), "fl": float64(float32(-123456.789))},
	}}

	plan := planBackupFloatRepair(&ir.Schema{Tables: []*ir.Table{table}})
	if _, ok := plan["metrics"]; !ok {
		t.Fatal("metrics must be in the backup float plan")
	}
	pr := newFloatExactPatchReader(inner, src, "dsn", plan)

	ch, err := pr.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := map[int64]ir.Row{}
	for row := range ch {
		got[row["id"].(int64)] = row
	}
	if err := pr.Err(); err != nil {
		t.Fatalf("patch reader Err: %v", err)
	}

	// id 1, 2: FLOAT patched EXACT, label untouched.
	for _, tc := range []struct {
		id    int64
		want  float64
		label string
	}{
		{1, float64(float32(8388608)), "a"},
		{2, float64(float32(-123456.789)), "b"},
	} {
		row := got[tc.id]
		fl := row["fl"].(float64)
		if math.Float32bits(float32(fl)) != math.Float32bits(float32(tc.want)) {
			t.Errorf("id %d: fl = %v; want exact %v", tc.id, fl, tc.want)
		}
		if row["label"] != tc.label {
			t.Errorf("id %d: label = %v; want %q (non-FLOAT column must be untouched)", tc.id, row["label"], tc.label)
		}
	}
	// id 3: not in the exact map → keeps its rounded COPY value.
	if got[3]["fl"].(float64) != 999 {
		t.Errorf("id 3 (absent from exact map): fl = %v; want the rounded COPY value 999", got[3]["fl"])
	}
}

// TestFloatExactPatchReader_NonPlanTablePassthrough pins that a table with
// no repairable FLOAT column streams through the wrapper unchanged (the
// inner reader is used directly, no exact scan).
func TestFloatExactPatchReader_NonPlanTablePassthrough(t *testing.T) {
	table := &ir.Table{
		Name:       "plain",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	inner := &fakeInnerReader{rows: []ir.Row{{"id": int64(1)}}}
	// Empty plan → passthrough; source OpenRowReader must never be called
	// (stubEngine panics if it is).
	pr := newFloatExactPatchReader(inner, exactSourceEngine{}, "dsn", map[string]floatPatchTable{})
	ch, err := pr.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	n := 0
	for range ch {
		n++
	}
	if n != 1 {
		t.Errorf("passthrough row count = %d; want 1", n)
	}
}

// TestPlanBackupFloatRepair_ShapeMatrix pins the backup plan across shapes:
// repairable, DOUBLE-only (omitted), keyless (omitted), float-PK-only
// (omitted), composite PK (non-PK float only).
func TestPlanBackupFloatRepair_ShapeMatrix(t *testing.T) {
	fs := func(n string) *ir.Column { return &ir.Column{Name: n, Type: ir.Float{Precision: ir.FloatSingle}} }
	fd := func(n string) *ir.Column { return &ir.Column{Name: n, Type: ir.Float{Precision: ir.FloatDouble}} }
	i := func(n string) *ir.Column { return &ir.Column{Name: n, Type: ir.Integer{Width: 64}} }
	pkOf := func(cols ...string) *ir.Index {
		ic := make([]ir.IndexColumn, len(cols))
		for k, c := range cols {
			ic[k] = ir.IndexColumn{Column: c}
		}
		return &ir.Index{Name: "pk", Columns: ic}
	}

	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "ok", Columns: []*ir.Column{i("id"), fs("fl")}, PrimaryKey: pkOf("id")},
		{Name: "double_only", Columns: []*ir.Column{i("id"), fd("d")}, PrimaryKey: pkOf("id")},
		{Name: "keyless", Columns: []*ir.Column{i("id"), fs("fl")}},
		{Name: "float_pk", Columns: []*ir.Column{fs("fl"), i("v")}, PrimaryKey: pkOf("fl")},
		{Name: "composite", Columns: []*ir.Column{i("a"), fs("b"), fs("c")}, PrimaryKey: pkOf("a", "b")},
	}}
	plan := planBackupFloatRepair(schema)

	for _, omit := range []string{"double_only", "keyless", "float_pk"} {
		if _, ok := plan[omit]; ok {
			t.Errorf("%s must be omitted from the backup plan", omit)
		}
	}
	if _, ok := plan["ok"]; !ok {
		t.Error("ok must be in the plan")
	}
	comp, ok := plan["composite"]
	if !ok {
		t.Fatal("composite must be in the plan")
	}
	if len(comp.floatCols) != 1 || comp.floatCols[0] != "c" {
		t.Errorf("composite floatCols = %v; want [c] (b is a PK member)", comp.floatCols)
	}
}

// TestFloatPatchKey pins the PK-key rendering: distinct tuples map to
// distinct keys across mixed families, order-sensitive.
func TestFloatPatchKey(t *testing.T) {
	k := func(row ir.Row, cols ...string) string { return floatPatchKey(row, cols) }
	if k(ir.Row{"a": int64(1), "b": "x"}, "a", "b") == k(ir.Row{"a": int64(1), "b": "y"}, "a", "b") {
		t.Error("distinct b values must produce distinct keys")
	}
	if k(ir.Row{"a": int64(1), "b": int64(2)}, "a", "b") == k(ir.Row{"a": int64(2), "b": int64(1)}, "a", "b") {
		t.Error("order-swapped tuples must produce distinct keys")
	}
	// The NUL separator keeps "1","2" distinct from "12","".
	if k(ir.Row{"a": "1", "b": "2"}, "a", "b") == k(ir.Row{"a": "12", "b": ""}, "a", "b") {
		t.Error("NUL separator must keep concatenation-ambiguous tuples distinct")
	}
}
