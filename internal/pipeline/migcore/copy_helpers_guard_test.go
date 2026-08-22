// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// unboundedFakeReader implements ir.BatchedRowReader but NOT
// ir.BoundedBatchedRowReader, so ReadChunkBatch must take the Go-side
// filterByUpperBound fallback for it.
type unboundedFakeReader struct {
	rows []ir.Row
}

func (r *unboundedFakeReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	return r.ReadRowsBatch(ctx, table, nil, len(r.rows))
}

func (r *unboundedFakeReader) ReadRowsBatch(_ context.Context, _ *ir.Table, _ []any, limit int) (<-chan ir.Row, error) {
	ch := make(chan ir.Row, len(r.rows))
	for i, row := range r.rows {
		if i >= limit {
			break
		}
		ch <- row
	}
	close(ch)
	return ch, nil
}

func (r *unboundedFakeReader) Err() error { return nil }

// boundedFakeReader adds the bounded surface, so ReadChunkBatch takes the
// SQL-side branch and the guard must NOT fire regardless of PK family.
type boundedFakeReader struct {
	unboundedFakeReader
}

func (r *boundedFakeReader) ReadRowsBatchBounded(ctx context.Context, table *ir.Table, after, _ []any, limit int) (<-chan ir.Row, error) {
	return r.ReadRowsBatch(ctx, table, after, limit)
}

func guardTestTable(pkType ir.Type) *ir.Table {
	return &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "pk", Type: pkType},
			{Name: "v", Type: ir.Integer{Width: 32}},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "pk"}}},
	}
}

// TestReadChunkBatch_FallbackRefusesOrderDivergentPK pins the ADR-0096
// chokepoint completeness guard (invariant sweep 2026-08-22): when a reader
// WITHOUT the bounded surface reaches the Go-side upper-bound clip with a PK
// whose server order can diverge from comparePKValue's bytewise order, the
// read REFUSES loudly instead of silently mis-clipping the chunk boundary.
//
// Both planner gates (migrate's shouldParallelChunk, backup's
// planBackupTableChunks) keep this branch unreachable for those families
// today; the guard is what turns a future planner miss — or a third caller
// that never consulted a planner — from a silent boundary-row loss into a
// loud error. The refusal cells and the pass cells are BOTH pinned, so the
// guard cannot silently widen into refusing the integer MIN/MAX path that
// legitimately uses the fallback.
//
// Mutation-verified (2026-08-22): with the fallbackClipOrderUnsafeColumn
// guard removed from ReadChunkBatch, the varchar cell streams its 1 row with
// a bytewise clip and no error — the silent mis-clip shape — and this test
// fails on the missing refusal.
func TestReadChunkBatch_FallbackRefusesOrderDivergentPK(t *testing.T) {
	ctx := context.Background()

	unsafeCells := []struct {
		name   string
		pkType ir.Type
		upper  []any
	}{
		{"varchar collated", ir.Varchar{Length: 20, Collation: "utf8mb4_0900_ai_ci"}, []any{"m"}},
		{"decimal as text", ir.Decimal{Precision: 10, Scale: 0}, []any{"9"}},
		{"float fmt text", ir.Float{Precision: ir.FloatDouble}, []any{float64(-1)}},
		{"mysql TIME text", ir.Time{}, []any{"99:00:00"}},
		{"enum ordinal vs label", ir.Enum{Values: []string{"b", "a"}}, []any{"a"}},
		{"domain over varchar", ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 5}}, []any{"m"}},
	}
	for _, c := range unsafeCells {
		c := c
		t.Run("refuses/"+c.name, func(t *testing.T) {
			rr := &unboundedFakeReader{rows: []ir.Row{{"pk": c.upper[0], "v": int32(1)}}}
			_, err := ReadChunkBatch(ctx, rr, guardTestTable(c.pkType), nil, c.upper, []string{"pk"}, 100)
			if err == nil {
				t.Fatalf("ReadChunkBatch accepted a Go-side clip on a %s PK — the order-divergence guard is gone "+
					"and a boundary-straddling row can silently land in no chunk (ADR-0096)", c.name)
			}
			if !strings.Contains(err.Error(), "cannot honour the server's ORDER BY") {
				t.Fatalf("refusal fired but with the wrong message: %v", err)
			}
		})
	}

	safeCells := []struct {
		name   string
		pkType ir.Type
		rows   []ir.Row
		upper  []any
		want   int
	}{
		// The one family the planners legitimately route to the fallback
		// (single-Integer MIN/MAX): must still clip, not refuse.
		{
			"integer clips",
			ir.Integer{Width: 64},
			[]ir.Row{{"pk": int64(1)}, {"pk": int64(2)}, {"pk": int64(3)}},
			[]any{int64(2)},
			2,
		},
		// Defense-in-depth families the allow list vouches for.
		{
			"uuid canonical hex",
			ir.UUID{},
			[]ir.Row{{"pk": "0a…"}, {"pk": "ff…"}},
			[]any{"0a…"},
			1,
		},
		{
			"varbinary bytewise",
			ir.Varbinary{Length: 4},
			[]ir.Row{{"pk": []byte{0x01}}, {"pk": []byte{0xff}}},
			[]any{[]byte{0x01}},
			1,
		},
		// The remaining allow-listed families — pinned so a future refactor
		// that mis-moves any of them into the refuse default (a silent
		// over-refusal regression on a value-fidelity guard) fails HERE, not
		// only the three representatives above (the Bug-74 "pin the class"
		// rule; the whole-universe backstop is TestFallbackClipOrder_
		// ClassifiesEveryIRType below).
		{
			"boolean numeric",
			ir.Boolean{},
			[]ir.Row{{"pk": false}, {"pk": true}},
			[]any{false},
			1,
		},
		{
			"date chronological",
			ir.Date{},
			[]ir.Row{{"pk": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}, {"pk": time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}},
			[]any{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
			1,
		},
		{
			"datetime chronological",
			ir.DateTime{},
			[]ir.Row{{"pk": time.Date(2020, 1, 1, 8, 30, 0, 0, time.UTC)}, {"pk": time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)}},
			[]any{time.Date(2020, 1, 1, 8, 30, 0, 0, time.UTC)},
			1,
		},
		{
			"timestamp chronological",
			ir.Timestamp{WithTimeZone: true},
			[]ir.Row{{"pk": time.Date(2020, 1, 1, 8, 30, 0, 0, time.UTC)}, {"pk": time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)}},
			[]any{time.Date(2020, 1, 1, 8, 30, 0, 0, time.UTC)},
			1,
		},
		{
			"binary bytewise",
			ir.Binary{Length: 1},
			[]ir.Row{{"pk": []byte{0x01}}, {"pk": []byte{0xff}}},
			[]any{[]byte{0x01}},
			1,
		},
		{
			"blob bytewise",
			ir.Blob{},
			[]ir.Row{{"pk": []byte{0x01}}, {"pk": []byte{0xff}}},
			[]any{[]byte{0x01}},
			1,
		},
		{
			// The IR-canonical fixed-width '0'/'1' bit-string; bytewise
			// reproduces both MySQL numeric and PG lexicographic bit order.
			"bit canonical bit-string",
			ir.Bit{},
			[]ir.Row{{"pk": "00000001"}, {"pk": "00000010"}},
			[]any{"00000001"},
			1,
		},
		{
			// Domain unwraps to its base's verdict — over a clip-safe base it
			// must PASS, the mirror of the domain-over-varchar refuse cell.
			"domain over integer unwraps to safe",
			ir.Domain{Name: "d", BaseType: ir.Integer{Width: 64}},
			[]ir.Row{{"pk": int64(1)}, {"pk": int64(2)}},
			[]any{int64(1)},
			1,
		},
	}
	for _, c := range safeCells {
		c := c
		t.Run("passes/"+c.name, func(t *testing.T) {
			rr := &unboundedFakeReader{rows: c.rows}
			ch, err := ReadChunkBatch(ctx, rr, guardTestTable(c.pkType), nil, c.upper, []string{"pk"}, 100)
			if err != nil {
				t.Fatalf("guard over-refused a clip-safe %s PK: %v", c.name, err)
			}
			n := 0
			for range ch {
				n++
			}
			if n != c.want {
				t.Fatalf("clipped stream carried %d rows; want %d", n, c.want)
			}
		})
	}

	t.Run("passes/nil upper bound needs no clip", func(t *testing.T) {
		rr := &unboundedFakeReader{rows: []ir.Row{{"pk": "z"}}}
		ch, err := ReadChunkBatch(ctx, rr, guardTestTable(ir.Varchar{Length: 5}), nil, nil, []string{"pk"}, 100)
		if err != nil {
			t.Fatalf("nil upperPK performs no Go-side clip and must not refuse: %v", err)
		}
		for range ch {
		}
	})

	t.Run("passes/bounded reader takes the SQL-side branch", func(t *testing.T) {
		rr := &boundedFakeReader{unboundedFakeReader{rows: []ir.Row{{"pk": "z"}}}}
		ch, err := ReadChunkBatch(ctx, rr, guardTestTable(ir.Varchar{Length: 5}), nil, []any{"m"}, []string{"pk"}, 100)
		if err != nil {
			t.Fatalf("a BoundedBatchedRowReader clips in SQL; the guard must not fire: %v", err)
		}
		for range ch {
		}
	})

	t.Run("refuses/pk column missing from the column list", func(t *testing.T) {
		table := guardTestTable(ir.Integer{Width: 64})
		_, err := ReadChunkBatch(ctx, &unboundedFakeReader{}, table, nil, []any{int64(1)}, []string{"ghost"}, 100)
		if err == nil {
			t.Fatal("a PK column the table cannot grade must refuse, not guess")
		}
	})
}

// TestFallbackClipOrder_ClassifiesEveryIRType is the whole-universe backstop
// for the ADR-0096 clip guard's allow list. It walks ir.AllTypes() — the
// canonical enumeration held exhaustive by
// TestAllTypes_CoversEveryIsTypeImplementor — and requires every family to be
// classified clip-safe or clip-unsafe by fallbackClipOrderUnsafeColumn. So
// ADDING an ir.Type (it lands in AllTypes or that test fails) forces a
// classification decision here, and MOVING a family between the allow list
// and the refuse default fails here rather than silently shipping an
// over-refusal (or, worse, a silent-clip widening). The per-value pins above
// prove the CLIP is correct for the safe families; this proves the SET is.
func TestFallbackClipOrder_ClassifiesEveryIRType(t *testing.T) {
	// The families fallbackClipOrderUnsafeColumn vouches for as clip-safe,
	// each justified in that function's doc and value-pinned in safeCells
	// above. Every other family — and a bare Domain{} with a nil base, which
	// is unclassifiable — must refuse.
	clipSafe := map[string]bool{
		"Boolean": true, "Integer": true,
		"Date": true, "DateTime": true, "Timestamp": true,
		"UUID": true, "Binary": true, "Varbinary": true, "Blob": true, "Bit": true,
	}
	seen, safeCount := 0, 0
	for _, typ := range ir.AllTypes() {
		name := reflect.TypeOf(typ).Name()
		col, _ := fallbackClipOrderUnsafeColumn(guardTestTable(typ), []string{"pk"})
		refused := col != ""
		if !refused {
			safeCount++
		}
		if want := !clipSafe[name]; refused != want {
			t.Errorf("family %s: fallbackClipOrderUnsafeColumn refused=%v, want refused=%v — a clip-safe "+
				"family must pass and every other family must refuse; reclassify it or the guard's allow list",
				name, refused, want)
		}
		seen++
	}
	// Anti-vacuity floor: the type universe is 27 families (ir.AllTypes). If
	// this trips, the enumeration shrank or the loop broke, and the roster is
	// grading almost nothing.
	if seen < 27 {
		t.Fatalf("classified only %d ir.Type families; the universe is 27 (ir.AllTypes) — roster near-vacuous", seen)
	}
	// The allow list must be neither emptied nor widened: exactly the 10
	// families above may pass. A guard whose whole switch was deleted would
	// refuse everything (safeCount 0) and slip past the per-family loop only
	// if clipSafe were also emptied — this catches that shape directly.
	if safeCount != len(clipSafe) {
		t.Fatalf("guard classified %d families clip-safe; expected exactly %d (the allow list) — a family moved sides",
			safeCount, len(clipSafe))
	}
}
