// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
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
	pr := newFloatExactPatchReader(inner, src, "dsn", plan, 0, false)

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
	pr := newFloatExactPatchReader(inner, exactSourceEngine{}, "dsn", map[string]floatPatchTable{}, 0, false)
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

// floatTable + floatRows build a small repairable metrics table + its
// inner (rounded) rows and exact source, shared by the over-cap pins.
func floatTable() *ir.Table {
	return &ir.Table{
		Name: "metrics",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// TestFloatExactPatchReader_OverCap_DefaultFallsBackRounded pins the
// bounded-memory floor: a table whose exact re-read would exceed the row
// cap falls back to the rounded COPY values (no unbounded buffer, no error)
// under the default posture.
func TestFloatExactPatchReader_OverCap_DefaultFallsBackRounded(t *testing.T) {
	table := floatTable()
	inner := &fakeInnerReader{rows: []ir.Row{
		{"id": int64(1), "fl": float64(8388610)}, // rounded
		{"id": int64(2), "fl": float64(-123457)},
	}}
	src := exactSourceEngine{rows: []ir.Row{
		{"id": int64(1), "fl": float64(float32(8388608))},
		{"id": int64(2), "fl": float64(float32(-123456.789))},
	}}
	plan := planBackupFloatRepair(&ir.Schema{Tables: []*ir.Table{table}})

	// maxRows = 1, but the exact scan has 2 rows → over cap → rounded fallback.
	pr := newFloatExactPatchReader(inner, src, "dsn", plan, 1, false)
	ch, err := pr.ReadRows(context.Background(), table)
	if err != nil {
		t.Fatalf("over-cap default must NOT error; got: %v", err)
	}
	got := map[int64]float64{}
	for row := range ch {
		got[row["id"].(int64)] = row["fl"].(float64)
	}
	// Values are the inner (rounded) ones — the exact map was never applied.
	if got[1] != 8388610 || got[2] != -123457 {
		t.Errorf("over-cap default: fl values = %v; want the rounded COPY values (no exact patch)", got)
	}
}

// TestFloatExactPatchReader_OverCap_StrictRefuses pins that under
// --strict-float an over-cap table refuses with the coded error rather than
// archiving rounded.
func TestFloatExactPatchReader_OverCap_StrictRefuses(t *testing.T) {
	table := floatTable()
	inner := &fakeInnerReader{rows: []ir.Row{{"id": int64(1), "fl": float64(8388610)}, {"id": int64(2), "fl": float64(1)}}}
	src := exactSourceEngine{rows: []ir.Row{
		{"id": int64(1), "fl": float64(float32(8388608))},
		{"id": int64(2), "fl": float64(float32(1))},
	}}
	plan := planBackupFloatRepair(&ir.Schema{Tables: []*ir.Table{table}})

	pr := newFloatExactPatchReader(inner, src, "dsn", plan, 1, true)
	_, err := pr.ReadRows(context.Background(), table)
	if err == nil {
		t.Fatal("over-cap under --strict-float must refuse; got nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeVStreamFloatLossy {
		t.Errorf("over-cap strict refusal code = %v (ok=%v); want %s", func() any {
			if ce != nil {
				return ce.Code
			}
			return nil
		}(), ok, sluicecode.CodeVStreamFloatLossy)
	}
}

// TestFloatExactPatchReader_ZeroPatchedTripwire pins the M0.1 0-patched-of-N
// tripwire (audit item 58): a repairable table whose exact re-read returned
// rows but NONE of whose streamed rows matched a PK key is a systemic
// PK-rendering divergence that silently leaves every FLOAT display-rounded.
// Under --strict-float it refuses (surfaced via Err() after the stream
// drains); the default archives rounded; a PARTIAL match is tolerated; and a
// legitimately empty exact map (empty source table) never trips.
func TestFloatExactPatchReader_ZeroPatchedTripwire(t *testing.T) {
	table := floatTable()
	// Inner (COPY) rows carry ids {1,2}; the exact source carries DISJOINT
	// ids {10,11} → a non-empty exact map, zero PK matches.
	disjointInner := func() *fakeInnerReader {
		return &fakeInnerReader{rows: []ir.Row{
			{"id": int64(1), "fl": float64(8388610)},
			{"id": int64(2), "fl": float64(-123457)},
		}}
	}
	disjointExact := exactSourceEngine{rows: []ir.Row{
		{"id": int64(10), "fl": float64(float32(8388608))},
		{"id": int64(11), "fl": float64(float32(-123456.789))},
	}}
	plan := planBackupFloatRepair(&ir.Schema{Tables: []*ir.Table{table}})

	drain := func(t *testing.T, pr *floatExactPatchReader) error {
		t.Helper()
		ch, err := pr.ReadRows(context.Background(), table)
		if err != nil {
			t.Fatalf("ReadRows: %v", err)
		}
		n := 0
		for range ch {
			n++
		}
		if n == 0 {
			t.Fatal("wrapper streamed no rows — the tripwire needs the inner rows to flow")
		}
		return pr.Err()
	}

	t.Run("strict zero-patched refuses", func(t *testing.T) {
		pr := newFloatExactPatchReader(disjointInner(), disjointExact, "dsn", plan, 0, true)
		err := drain(t, pr)
		if err == nil {
			t.Fatal("strict + 0-patched-of-N must refuse via Err(); got nil")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeVStreamFloatLossy {
			t.Errorf("tripwire code = %v (ok=%v); want %s", ce, ok, sluicecode.CodeVStreamFloatLossy)
		}
	})

	t.Run("default zero-patched archives rounded but WARNs (audit-2026-07-11 M-2)", func(t *testing.T) {
		// The default posture must not stay SILENT where strict refuses —
		// symmetric with the over-cap / unrepairable rounded-fallbacks, which
		// both WARN. No error, but a loud WARN naming the table.
		var logBuf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		t.Cleanup(func() { slog.SetDefault(prev) })

		pr := newFloatExactPatchReader(disjointInner(), disjointExact, "dsn", plan, 0, false)
		if err := drain(t, pr); err != nil {
			t.Fatalf("default posture must NOT error on 0-patched; got: %v", err)
		}
		if got := logBuf.String(); !strings.Contains(got, "matched none of the streamed rows") ||
			!strings.Contains(got, table.Name) {
			t.Errorf("default 0-patched must WARN loudly naming the table; log = %q", got)
		}
	})

	t.Run("streamed==0 with non-empty exact map WARNs, never refuses (audit-2026-07-11 M-2)", func(t *testing.T) {
		// The COPY delivered no rows though the exact scan found some: a
		// whole-table copy dropout OR a table empty at the snapshot position
		// and filled during the window (legit). Ambiguous → WARN loudly in
		// BOTH postures, but never refuse (refusing would false-positive the
		// legit empty-at-snapshot case).
		emptyInner := func() *fakeInnerReader { return &fakeInnerReader{rows: nil} }
		for _, strict := range []bool{true, false} {
			var logBuf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))

			pr := newFloatExactPatchReader(emptyInner(), disjointExact, "dsn", plan, 0, strict)
			ch, err := pr.ReadRows(context.Background(), table)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			for range ch { //nolint:revive // drain
			}
			gotErr := pr.Err()
			slog.SetDefault(prev)

			if gotErr != nil {
				t.Fatalf("strict=%v: streamed==0 must NOT refuse (legit empty-at-snapshot); got: %v", strict, gotErr)
			}
			if got := logBuf.String(); !strings.Contains(got, "COPY streamed none") || !strings.Contains(got, table.Name) {
				t.Errorf("strict=%v: streamed==0 must WARN loudly naming the table; log = %q", strict, got)
			}
		}
	})

	t.Run("strict partial-patch tolerated (no error)", func(t *testing.T) {
		// Inner ids {1,2}; exact carries id 1 (matches) + id 11 (miss) → one
		// row patched → NOT a total miss → tolerated even under --strict-float.
		partialExact := exactSourceEngine{rows: []ir.Row{
			{"id": int64(1), "fl": float64(float32(8388608))},
			{"id": int64(11), "fl": float64(float32(1))},
		}}
		pr := newFloatExactPatchReader(disjointInner(), partialExact, "dsn", plan, 0, true)
		if err := drain(t, pr); err != nil {
			t.Fatalf("strict + partial patch must NOT refuse (only a TOTAL miss trips); got: %v", err)
		}
	})

	t.Run("strict empty exact map never trips", func(t *testing.T) {
		// An empty source table → empty exact map → nothing to patch → the
		// exactCount>0 guard keeps --strict-float from a false refusal.
		pr := newFloatExactPatchReader(disjointInner(), exactSourceEngine{}, "dsn", plan, 0, true)
		if err := drain(t, pr); err != nil {
			t.Fatalf("strict + empty exact map must NOT refuse (nothing to patch); got: %v", err)
		}
	})
}

// TestPlanBackupFloatRepair_ShapeMatrix pins the backup plan across shapes:
// repairable, DOUBLE-only (omitted), keyless (omitted), float-PK-only
// (omitted), float-in-composite-PK (omitted — SL-F1), int-composite PK
// (repairable, non-PK float only).
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
		// SL-F1: a FLOAT member in a composite PK omits the WHOLE table — the
		// display-rounded PK on the target can never match the exact re-read
		// key, so patching the non-PK float c would silently no-op.
		{Name: "float_in_composite_pk", Columns: []*ir.Column{i("a"), fs("b"), fs("c")}, PrimaryKey: pkOf("a", "b")},
		// An int-only composite PK + a non-PK float IS repairable.
		{Name: "int_composite", Columns: []*ir.Column{i("a"), i("b"), fs("c")}, PrimaryKey: pkOf("a", "b")},
	}}
	plan := planBackupFloatRepair(schema)

	for _, omit := range []string{"double_only", "keyless", "float_pk", "float_in_composite_pk"} {
		if _, ok := plan[omit]; ok {
			t.Errorf("%s must be omitted from the backup plan", omit)
		}
	}
	if _, ok := plan["ok"]; !ok {
		t.Error("ok must be in the plan")
	}
	comp, ok := plan["int_composite"]
	if !ok {
		t.Fatal("int_composite must be in the plan")
	}
	if len(comp.floatCols) != 1 || comp.floatCols[0] != "c" {
		t.Errorf("int_composite floatCols = %v; want [c] (a,b are int PK members)", comp.floatCols)
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
