// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package floatrepair

import (
	"context"
	"fmt"
	"math"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// recordingExecer captures every batch the skeleton flushes so the tests can
// assert the round-trip count and the exact rows/columns handed to the engine.
type recordingExecer struct {
	calls      int
	pkColumns  [][]string
	setColumns [][]string
	batchSizes []int
	rows       []ir.Row
}

func (r *recordingExecer) ExecBatch(_ context.Context, _ *ir.Table, pkColumns, setColumns []string, batch []ir.Row) error {
	r.calls++
	r.pkColumns = append(r.pkColumns, append([]string(nil), pkColumns...))
	r.setColumns = append(r.setColumns, append([]string(nil), setColumns...))
	r.batchSizes = append(r.batchSizes, len(batch))
	// Copy the rows out — the skeleton reuses its batch slice's backing array
	// after a flush, so a reference kept here would be clobbered.
	for _, row := range batch {
		cp := make(ir.Row, len(row))
		for k, v := range row {
			cp[k] = v
		}
		r.rows = append(r.rows, cp)
	}
	return nil
}

// errExecer fails on the Nth (1-based) ExecBatch call.
type errExecer struct {
	failOn int
	calls  int
}

func (e *errExecer) ExecBatch(context.Context, *ir.Table, []string, []string, []ir.Row) error {
	e.calls++
	if e.calls == e.failOn {
		return fmt.Errorf("boom on call %d", e.calls)
	}
	return nil
}

func floatRepairTable() *ir.Table {
	return &ir.Table{
		Name: "frepair",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func streamRows(rows []ir.Row) <-chan ir.Row {
	ch := make(chan ir.Row, len(rows))
	for _, r := range rows {
		ch <- r
	}
	close(ch)
	return ch
}

// TestRepairByPK_WideTableBatchCap pins PERF-F1: a very wide single-precision
// FLOAT table shrinks the batch so rows × (PK + SET) params stays under the
// 65535 bind-param ceiling (Postgres/MySQL), instead of overflowing at the
// full 500; a narrow table is unaffected.
func TestRepairByPK_WideTableBatchCap(t *testing.T) {
	const floatCols = 200 // 1 PK + 200 SET = 201 params/row
	cols := []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}
	setNames := make([]string, floatCols)
	for i := 0; i < floatCols; i++ {
		n := fmt.Sprintf("fl%d", i)
		setNames[i] = n
		cols = append(cols, &ir.Column{Name: n, Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true})
	}
	table := &ir.Table{Name: "wide", Columns: cols, PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}}}

	const n = 600
	rows := make([]ir.Row, n)
	for i := range rows {
		r := ir.Row{"id": int64(i + 1)}
		for _, s := range setNames {
			r[s] = float64(1.5)
		}
		rows[i] = r
	}

	rec := &recordingExecer{}
	if err := RepairByPK(context.Background(), table, []string{"id"}, streamRows(rows), 500, rec); err != nil {
		t.Fatal(err)
	}
	perRow := 1 + floatCols
	wantBatch := 60000 / perRow // 298 (< 500 → capped)
	wantCalls := (n + wantBatch - 1) / wantBatch
	if rec.calls != wantCalls {
		t.Errorf("wide table (%d params/row): %d ExecBatch calls, want %d (capped batch %d)", perRow, rec.calls, wantCalls, wantBatch)
	}
	for i, bs := range rec.batchSizes {
		if bs > wantBatch {
			t.Errorf("batch %d size %d exceeds the derived cap %d", i, bs, wantBatch)
		}
		if bs*perRow > 65535 {
			t.Errorf("batch %d: %d rows × %d params = %d exceeds the 65535 ceiling", i, bs, perRow, bs*perRow)
		}
	}

	// Control: a narrow table (id + one FLOAT = 2 params/row) keeps the full
	// 500-row batch — the cap must not shrink a normal table.
	narrowRows := make([]ir.Row, n)
	for i := range narrowRows {
		narrowRows[i] = ir.Row{"id": int64(i + 1), "fl": float64(1.5)}
	}
	rec2 := &recordingExecer{}
	if err := RepairByPK(context.Background(), floatRepairTable(), []string{"id"}, streamRows(narrowRows), 500, rec2); err != nil {
		t.Fatal(err)
	}
	if want := (n + 499) / 500; rec2.calls != want {
		t.Errorf("narrow table: %d calls, want %d — the cap must not shrink a normal table", rec2.calls, want)
	}
}

func TestRepairByPK_NilTable(t *testing.T) {
	err := RepairByPK(context.Background(), nil, []string{"id"}, streamRows(nil), 500, &recordingExecer{})
	if err == nil {
		t.Fatal("want error on nil table")
	}
}

func TestRepairByPK_NoPK(t *testing.T) {
	err := RepairByPK(context.Background(), floatRepairTable(), nil, streamRows(nil), 500, &recordingExecer{})
	if err == nil {
		t.Fatal("want error on empty pkColumns")
	}
}

// TestRepairByPK_Batching pins the PERF-P1 round-trip reduction: N rows are
// flushed in ceil(N/batchRows) ExecBatch calls, with every row carried
// through exactly once and no explicit transaction round-trips (the skeleton
// never begins one).
func TestRepairByPK_Batching(t *testing.T) {
	for _, tc := range []struct {
		n, batch, wantCalls int
	}{
		{0, 500, 0},
		{1, 500, 1},
		{500, 500, 1},
		{501, 500, 2},
		{1000, 500, 2},
		{1001, 500, 3},
		{7, 3, 3},
	} {
		rows := make([]ir.Row, tc.n)
		for i := range rows {
			rows[i] = ir.Row{"id": int64(i + 1), "fl": float64(float32(i) + 0.5)}
		}
		rec := &recordingExecer{}
		if err := RepairByPK(context.Background(), floatRepairTable(), []string{"id"}, streamRows(rows), tc.batch, rec); err != nil {
			t.Fatalf("n=%d: %v", tc.n, err)
		}
		if rec.calls != tc.wantCalls {
			t.Errorf("n=%d batch=%d: got %d ExecBatch calls, want %d", tc.n, tc.batch, rec.calls, tc.wantCalls)
		}
		if len(rec.rows) != tc.n {
			t.Errorf("n=%d: carried %d rows, want %d", tc.n, len(rec.rows), tc.n)
		}
		// Every batch but possibly the last is full.
		for i, sz := range rec.batchSizes {
			if i < len(rec.batchSizes)-1 && sz != tc.batch {
				t.Errorf("n=%d: batch %d size %d, want full %d", tc.n, i, sz, tc.batch)
			}
			if sz > tc.batch {
				t.Errorf("n=%d: batch %d size %d exceeds cap %d", tc.n, i, sz, tc.batch)
			}
		}
	}
}

// TestRepairByPK_RoundTripReduction is the headline PERF-P1 metric: it prints
// the per-row (before) vs batched (after) statement count for realistic N and
// asserts the batched path issues exactly ceil(N/500) statements — the direct
// WAN-latency win, since round-trips dominate cross-region latency.
func TestRepairByPK_RoundTripReduction(t *testing.T) {
	const batch = floatRepairBatchRowsForTest
	t.Logf("%8s | %12s | %12s | %10s", "rows", "before(/row)", "after(/batch)", "reduction")
	for _, n := range []int{500, 5000, 50000} {
		rows := make([]ir.Row, n)
		for i := range rows {
			rows[i] = ir.Row{"id": int64(i + 1), "fl": float64(float32(i))}
		}
		rec := &recordingExecer{}
		if err := RepairByPK(context.Background(), floatRepairTable(), []string{"id"}, streamRows(rows), batch, rec); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		wantAfter := (n + batch - 1) / batch
		if rec.calls != wantAfter {
			t.Errorf("n=%d: got %d statements, want %d", n, rec.calls, wantAfter)
		}
		t.Logf("%8d | %12d | %12d | %9.1fx", n, n, rec.calls, float64(n)/float64(rec.calls))
	}
}

// floatRepairBatchRowsForTest mirrors the engines' floatRepairBatchRows (500)
// so the round-trip table uses the production cap without importing an engine.
const floatRepairBatchRowsForTest = 500

// TestRepairByPK_PKOnlyRowSkipped: a row with only PK keys (no FLOAT to SET)
// is skipped, never emitted as an empty UPDATE — matching the per-row path.
func TestRepairByPK_PKOnlyRowSkipped(t *testing.T) {
	rows := []ir.Row{
		{"id": int64(1)}, // PK-only — skip
		{"id": int64(2), "fl": float64(float32(1.5))},
	}
	rec := &recordingExecer{}
	if err := RepairByPK(context.Background(), floatRepairTable(), []string{"id"}, streamRows(rows), 500, rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.rows) != 1 {
		t.Fatalf("carried %d rows, want 1 (the PK-only row skipped)", len(rec.rows))
	}
	if rec.rows[0]["id"] != int64(2) {
		t.Errorf("wrong row carried: %v", rec.rows[0])
	}
}

// TestRepairByPK_InconsistentColumns: a row whose non-PK column set differs
// from the established one is refused loudly, not silently applied.
func TestRepairByPK_InconsistentColumns(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "a", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
			{Name: "b", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	rows := []ir.Row{
		{"id": int64(1), "a": float64(1)},
		{"id": int64(2), "b": float64(2)}, // different SET column set
	}
	err := RepairByPK(context.Background(), table, []string{"id"}, streamRows(rows), 500, &recordingExecer{})
	if err == nil {
		t.Fatal("want loud error on inconsistent repair column set")
	}
}

// TestRepairByPK_GeneratedColumnFiltered: a STORED generated non-PK column is
// excluded from the SET columns (the same NonGeneratedRowKeys filter the
// per-row path used).
func TestRepairByPK_GeneratedColumnFiltered(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "fl", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true},
			{Name: "gen", Type: ir.Float{Precision: ir.FloatSingle}, Nullable: true, GeneratedExpr: "fl * 2"},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	rows := []ir.Row{{"id": int64(1), "fl": float64(1.5), "gen": float64(3.0)}}
	rec := &recordingExecer{}
	if err := RepairByPK(context.Background(), table, []string{"id"}, streamRows(rows), 500, rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.setColumns) != 1 || len(rec.setColumns[0]) != 1 || rec.setColumns[0][0] != "fl" {
		t.Fatalf("setColumns should be [fl] with the generated column filtered, got %v", rec.setColumns)
	}
}

// TestRepairByPK_ExecErrorPropagates: an ExecBatch failure aborts the repair
// loudly.
func TestRepairByPK_ExecErrorPropagates(t *testing.T) {
	rows := make([]ir.Row, 10)
	for i := range rows {
		rows[i] = ir.Row{"id": int64(i + 1), "fl": float64(float32(i))}
	}
	err := RepairByPK(context.Background(), floatRepairTable(), []string{"id"}, streamRows(rows), 3, &errExecer{failOn: 2})
	if err == nil {
		t.Fatal("want the ExecBatch error to propagate")
	}
}

// TestRepairByPK_NaNValueCarried ensures the skeleton does not itself inspect
// values — a NaN reaches the engine's ExecBatch, where value shaping refuses
// it (the skeleton is value-agnostic).
func TestRepairByPK_NaNValueCarried(t *testing.T) {
	rows := []ir.Row{{"id": int64(1), "fl": math.NaN()}}
	rec := &recordingExecer{}
	if err := RepairByPK(context.Background(), floatRepairTable(), []string{"id"}, streamRows(rows), 500, rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.rows) != 1 {
		t.Fatalf("carried %d rows, want 1", len(rec.rows))
	}
}
