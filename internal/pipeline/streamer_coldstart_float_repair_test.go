// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// captureHandler records slog records at WARN+ for assertion.
type captureHandler struct{ records *[]slog.Record }

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

func columnAttr(r slog.Record) string {
	var col string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "column" {
			col = a.Value.String()
			return false
		}
		return true
	})
	return col
}

func floatCol(name string, p ir.FloatPrecision) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Float{Precision: p}, Nullable: true}
}

func intCol(name string) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Integer{Width: 64}}
}

func pk(cols ...string) *ir.Index {
	ic := make([]ir.IndexColumn, len(cols))
	for i, c := range cols {
		ic[i] = ir.IndexColumn{Column: c}
	}
	return &ir.Index{Name: "pk", Columns: ic}
}

// TestPlanFloatRepair_FamilyAndShapeMatrix pins the FLOAT re-read plan
// against the family (single vs double precision) and shape (has-PK,
// keyless, float-is-PK) matrix — the Bug-74 discipline for a type-family-
// dispatched detector.
func TestPlanFloatRepair_FamilyAndShapeMatrix(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		// (a) single-precision FLOAT with an int PK → repairable.
		{
			Name:       "repairable",
			Columns:    []*ir.Column{intCol("id"), floatCol("fl", ir.FloatSingle), floatCol("dbl", ir.FloatDouble)},
			PrimaryKey: pk("id"),
		},
		// (b) ONLY a DOUBLE FLOAT → omitted (double transits COPY exact).
		{
			Name:       "double_only",
			Columns:    []*ir.Column{intCol("id"), floatCol("dbl", ir.FloatDouble)},
			PrimaryKey: pk("id"),
		},
		// (c) single-precision FLOAT but NO primary key → present (for the
		// WARN) but NOT repairable.
		{
			Name:    "keyless",
			Columns: []*ir.Column{intCol("id"), floatCol("fl", ir.FloatSingle)},
		},
		// (d) the ONLY FLOAT column IS the PK → present but not repairable
		// (a float PK can't be repaired by keying on itself).
		{
			Name:       "float_pk",
			Columns:    []*ir.Column{floatCol("fl", ir.FloatSingle), intCol("v")},
			PrimaryKey: pk("fl"),
		},
		// (e) composite PK + two single floats (one a PK member) → repairable
		// on the non-PK float only.
		{
			Name:       "composite",
			Columns:    []*ir.Column{intCol("a"), floatCol("b", ir.FloatSingle), floatCol("c", ir.FloatSingle)},
			PrimaryKey: pk("a", "b"),
		},
		// (f) no FLOAT at all → omitted entirely.
		{
			Name:       "plain",
			Columns:    []*ir.Column{intCol("id")},
			PrimaryKey: pk("id"),
		},
	}}

	plan := planFloatRepair(schema)
	byName := map[string]floatRepairTable{}
	for _, ft := range plan {
		byName[ft.name] = ft
	}

	if _, ok := byName["double_only"]; ok {
		t.Error("double_only: DOUBLE-only table must be omitted from the plan")
	}
	if _, ok := byName["plain"]; ok {
		t.Error("plain: no-FLOAT table must be omitted from the plan")
	}

	rep := byName["repairable"]
	if !rep.repairable {
		t.Error("repairable: want repairable=true")
	}
	if got := rep.floatColumns; len(got) != 1 || got[0] != "fl" {
		t.Errorf("repairable: floatColumns = %v; want [fl] (dbl is DOUBLE, excluded)", got)
	}
	if rep.srcRead == nil {
		t.Fatal("repairable: srcRead must be non-nil")
	}
	// srcRead must project PK + the single-precision FLOAT (not the DOUBLE).
	gotCols := map[string]bool{}
	for _, c := range rep.srcRead.Columns {
		gotCols[c.Name] = true
	}
	if !gotCols["id"] || !gotCols["fl"] || gotCols["dbl"] {
		t.Errorf("repairable: srcRead columns = %v; want {id, fl} only", gotCols)
	}

	kl := byName["keyless"]
	if kl.repairable {
		t.Error("keyless: a table with no PK must NOT be repairable")
	}
	if len(kl.floatColumns) != 1 {
		t.Errorf("keyless: floatColumns = %v; want the column still named for the WARN", kl.floatColumns)
	}

	fpk := byName["float_pk"]
	if fpk.repairable {
		t.Error("float_pk: a table whose only FLOAT is the PK must NOT be repairable")
	}

	comp := byName["composite"]
	if !comp.repairable {
		t.Error("composite: want repairable=true (non-PK float c)")
	}
	// floatColumns names BOTH single floats (b and c) for the WARN; the
	// re-read set excludes the PK member b, so srcRead projects a, b, c but
	// UpdateFloatColumnsByPK only SETs c (b is in the WHERE via the PK).
	if len(comp.floatColumns) != 2 {
		t.Errorf("composite: floatColumns = %v; want both single floats named", comp.floatColumns)
	}
	compCols := map[string]bool{}
	for _, c := range comp.srcRead.Columns {
		compCols[c.Name] = true
	}
	if !compCols["a"] || !compCols["c"] {
		t.Errorf("composite: srcRead must project PK cols + repairable float c; got %v", compCols)
	}
}

// TestTrimmedFloatReadTable_TypeCaptureIsIndependent proves the plan's
// captured FLOAT type survives a later in-place mutation of the source
// schema (the ApplyMappings FLOAT→DOUBLE type-override race the plan is
// captured BEFORE): the trimmed read table must still see FloatSingle so
// selectColumnExpr keeps the `(col * 1E0)` projection.
func TestTrimmedFloatReadTable_TypeCaptureIsIndependent(t *testing.T) {
	src := &ir.Table{
		Name:       "t",
		Columns:    []*ir.Column{intCol("id"), floatCol("fl", ir.FloatSingle)},
		PrimaryKey: pk("id"),
	}
	plan := planFloatRepair(&ir.Schema{Tables: []*ir.Table{src}})
	if len(plan) != 1 || plan[0].srcRead == nil {
		t.Fatalf("plan = %+v; want one repairable table", plan)
	}

	// Simulate ApplyMappings rewriting the source FLOAT column in place.
	src.Columns[1].Type = ir.Float{Precision: ir.FloatDouble}

	var flType ir.Type
	for _, c := range plan[0].srcRead.Columns {
		if c.Name == "fl" {
			flType = c.Type
		}
	}
	f, ok := flType.(ir.Float)
	if !ok || f.Precision != ir.FloatSingle {
		t.Errorf("srcRead fl type = %#v; want ir.Float{FloatSingle} captured independently of the later mutation", flType)
	}
}

// TestWarnFloatDisplayRounding pins the schema-triggered WARN: once per
// FLOAT column (not per row), naming table.column, with the right message
// variant for repairable / repair-disabled / un-repairable (keyless).
func TestWarnFloatDisplayRounding(t *testing.T) {
	plan := []floatRepairTable{
		{name: "a", floatColumns: []string{"fl"}, repairable: true},
		{name: "b", floatColumns: []string{"x", "y"}, repairable: false}, // keyless: two columns
	}

	// Repair ENABLED.
	var recs []slog.Record
	restore := swapDefaultLogger(&recs)
	warnFloatDisplayRounding(context.Background(), plan, false)
	restore()

	if len(recs) != 3 {
		t.Fatalf("want 3 WARNs (a.fl + b.x + b.y), got %d", len(recs))
	}
	byCol := map[string]slog.Record{}
	for _, r := range recs {
		if r.Level != slog.LevelWarn {
			t.Errorf("record level = %v; want WARN", r.Level)
		}
		byCol[columnAttr(r)] = r
	}
	for _, want := range []string{"a.fl", "b.x", "b.y"} {
		if _, ok := byCol[want]; !ok {
			t.Errorf("missing WARN for column %q", want)
		}
	}
	if !strings.Contains(byCol["a.fl"].Message, "will repair") {
		t.Errorf("a.fl (repairable, enabled) message should promise a repair: %q", byCol["a.fl"].Message)
	}
	if !strings.Contains(byCol["b.x"].Message, "CANNOT be repaired") {
		t.Errorf("b.x (keyless) message should say it cannot be repaired: %q", byCol["b.x"].Message)
	}

	// Repair DISABLED (--no-float-exact-reread): the repairable column's
	// message must say the rounding is retained / repair disabled.
	var recs2 []slog.Record
	restore2 := swapDefaultLogger(&recs2)
	warnFloatDisplayRounding(context.Background(), plan[:1], true)
	restore2()
	if len(recs2) != 1 {
		t.Fatalf("want 1 WARN, got %d", len(recs2))
	}
	if !strings.Contains(recs2[0].Message, "DISABLED") {
		t.Errorf("disabled-repair message should name the disabled repair: %q", recs2[0].Message)
	}
}

func swapDefaultLogger(sink *[]slog.Record) func() {
	prev := slog.Default()
	slog.SetDefault(slog.New(captureHandler{records: sink}))
	return func() { slog.SetDefault(prev) }
}
