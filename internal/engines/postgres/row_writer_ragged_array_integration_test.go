//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The real-Postgres half of the array-rectangularity gate.
//
// The unit matrix (row_writer_array_shape_test.go) pins what
// buildPGArray HANDS pgx — Dims plus a flat row-major element list.
// This pins what the SERVER ends up holding, because the loss the check
// prevents was only ever visible on the far side of the wire: with the
// dimensions taken from the first sub-array and every leaf appended
// regardless, a long row's extra elements were dropped by pgx's encoder
// (Dims won), a short first row truncated every later row, and a short
// TRAILING row underran the element slice and panicked. `array_dims` +
// `::text` are the ground truth here — not sluice re-reading its own
// value, which is exactly the writer-verifying-writer trap.
//
// The matrix is per element FAMILY, not one representative: pgx plans
// the element encode against the target column's element OID, so a
// green `int[][]` is not evidence for `numeric[][]` (Bug 74, which
// silently flattened exactly that pair).

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// raggedArrayFamily is one element family: the PG column type, the IR
// element type the writer is handed, and a leaf generator.
type raggedArrayFamily struct {
	name   string
	pgType string
	elem   ir.Type
	leaf   func(i int) any
	// wantText is the expected `col::text` for the rectangular 2-D
	// value built from leaves 0..3.
	want2DText string
}

func raggedArrayFamilyMatrix() []raggedArrayFamily {
	return []raggedArrayFamily{
		{
			name: "int", pgType: "BIGINT[]", elem: ir.Integer{Width: 64},
			leaf:       func(i int) any { return int64(i + 1) },
			want2DText: "{{1,2},{3,4}}",
		},
		{
			name: "float", pgType: "DOUBLE PRECISION[]", elem: ir.Float{Precision: ir.FloatDouble},
			leaf:       func(i int) any { return float64(i) + 0.5 },
			want2DText: "{{0.5,1.5},{2.5,3.5}}",
		},
		{
			name: "bool", pgType: "BOOLEAN[]", elem: ir.Boolean{},
			leaf:       func(i int) any { return i%2 == 0 },
			want2DText: "{{t,f},{t,f}}",
		},
		{
			name: "text", pgType: "TEXT[]", elem: ir.Text{Size: ir.TextRegular},
			leaf:       func(i int) any { return string(rune('a' + i)) },
			want2DText: "{{a,b},{c,d}}",
		},
		{
			// The Bug 74 family: identical sluice code path, a DIFFERENT
			// pgx target-OID codec, which is what silently flattened.
			name: "numeric", pgType: "NUMERIC(20,4)[]", elem: ir.Decimal{Precision: 20, Scale: 4},
			leaf:       func(i int) any { return string(rune('1'+i)) + ".5000" },
			want2DText: "{{1.5000,2.5000},{3.5000,4.5000}}",
		},
		{
			name: "uuid", pgType: "UUID[]", elem: ir.UUID{},
			leaf: func(i int) any {
				return "00000000-0000-0000-0000-00000000000" + string(rune('1'+i))
			},
			want2DText: "{{00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000002}," +
				"{00000000-0000-0000-0000-000000000003,00000000-0000-0000-0000-000000000004}}",
		},
		{
			name: "date", pgType: "DATE[]", elem: ir.Date{},
			leaf: func(i int) any {
				return time.Date(2026, 3, 1+i, 0, 0, 0, 0, time.UTC)
			},
			want2DText: "{{2026-03-01,2026-03-02},{2026-03-03,2026-03-04}}",
		},
		{
			name: "timestamp", pgType: "TIMESTAMP[]", elem: ir.Timestamp{},
			leaf: func(i int) any {
				return time.Date(2026, 3, 1, 12, i, 0, 0, time.UTC)
			},
			want2DText: `{{"2026-03-01 12:00:00","2026-03-01 12:01:00"},` +
				`{"2026-03-01 12:02:00","2026-03-01 12:03:00"}}`,
		},
	}
}

// TestRowWriter_RectangularArraysRoundTripPerFamily writes the valid
// shapes for every element family and ground-truths what Postgres
// stored with array_dims + ::text.
func TestRowWriter_RectangularArraysRoundTripPerFamily(t *testing.T) {
	for _, fam := range raggedArrayFamilyMatrix() {
		t.Run(fam.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForApplier(t)
			defer cleanup()
			applyPGApplier(t, dsn, "CREATE TABLE arr (id BIGINT PRIMARY KEY, v "+fam.pgType+");")

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			rw, err := Engine{}.OpenRowWriter(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenRowWriter: %v", err)
			}
			defer closeIf(rw)

			l := fam.leaf
			cases := []struct {
				id       int64
				value    []any
				wantDims string
			}{
				{1, []any{l(0), l(1), l(2)}, "[1:3]"},
				{2, []any{[]any{l(0), l(1)}, []any{l(2), l(3)}}, "[1:2][1:2]"},
				{3, []any{
					[]any{[]any{l(0), l(1)}, []any{l(2), l(3)}},
					[]any{[]any{l(0), l(1)}, []any{l(2), l(3)}},
				}, "[1:2][1:2][1:2]"},
				// Deeply nested rectangular.
				{4, []any{
					[]any{[]any{[]any{l(0)}}},
					[]any{[]any{[]any{l(1)}}},
				}, "[1:2][1:1][1:1][1:1]"},
				// Empty array: PG reports NULL array_dims for a 0-element array.
				{5, []any{}, ""},
				// NULL elements.
				{6, []any{[]any{l(0), nil}, []any{nil, l(3)}}, "[1:2][1:2]"},
			}

			rows := make(chan ir.Row, len(cases))
			for _, c := range cases {
				rows <- ir.Row{"id": c.id, "v": c.value}
			}
			close(rows)
			if err := rw.WriteRows(ctx, raggedArrayTable(fam.elem), rows); err != nil {
				t.Fatalf("WriteRows: %v", err)
			}

			for _, c := range cases {
				got := pgArrayScalarString(t, dsn,
					"SELECT COALESCE(array_dims(v), '') FROM arr WHERE id = $1", c.id)
				if got != c.wantDims {
					t.Errorf("id=%d array_dims = %q; want %q", c.id, got, c.wantDims)
				}
			}
			// The 2-D row's full text is the value-level ground truth: a
			// dropped or reordered element shows up here even when the
			// dimensions happen to look right.
			if got := pgArrayScalarString(t, dsn, "SELECT v::text FROM arr WHERE id = 2"); got != fam.want2DText {
				t.Errorf("2-D value = %s; want %s", got, fam.want2DText)
			}
		})
	}
}

// TestRowWriter_RaggedArrayRefusedPerFamily drives every ragged shape
// through the real writer for every element family and requires the
// coded refusal — never a panic, and never a partial write.
func TestRowWriter_RaggedArrayRefusedPerFamily(t *testing.T) {
	for _, fam := range raggedArrayFamilyMatrix() {
		t.Run(fam.name, func(t *testing.T) {
			dsn, cleanup := startPostgresForApplier(t)
			defer cleanup()
			applyPGApplier(t, dsn, "CREATE TABLE arr (id BIGINT PRIMARY KEY, v "+fam.pgType+");")

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			rw, err := Engine{}.OpenRowWriter(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenRowWriter: %v", err)
			}
			defer closeIf(rw)

			l := fam.leaf
			ragged := []struct {
				name  string
				value []any
			}{
				{"long second row", []any{[]any{l(0), l(1)}, []any{l(2), l(3), l(0)}}},
				{"short first row", []any{[]any{l(0)}, []any{l(1), l(2), l(3)}}},
				// Panicked before the fix.
				{"short trailing row", []any{[]any{l(0), l(1)}, []any{l(2)}}},
			}
			for i, rc := range ragged {
				t.Run(rc.name, func(t *testing.T) {
					rows := make(chan ir.Row, 1)
					rows <- ir.Row{"id": int64(100 + i), "v": rc.value}
					close(rows)

					err := rw.WriteRows(ctx, raggedArrayTable(fam.elem), rows)
					if err == nil {
						t.Fatal("ragged array was written; want a coded refusal")
					}
					ce, ok := sluicecode.FromError(err)
					if !ok || ce.Code != sluicecode.CodeValueRaggedArray {
						t.Fatalf("error %v; want %s", err, sluicecode.CodeValueRaggedArray)
					}
					if ce.ExitCode() != sluicecode.ExitRefusal {
						t.Errorf("exit code = %d; want %d", ce.ExitCode(), sluicecode.ExitRefusal)
					}
					// The caller wraps with the column name; an operator
					// cannot act on a refusal that does not say which one.
					if !raggedRefusalMentions(err.Error(), `"v"`, "not rectangular") {
						t.Errorf("refusal %q does not name the column and the shape", err.Error())
					}
					if got := pgScalarInt(t, dsn, "SELECT COUNT(*) FROM arr"); got != 0 {
						t.Errorf("%d rows landed; a refused ragged value must not write anything", got)
					}
				})
			}
		})
	}
}

func raggedArrayTable(elem ir.Type) *ir.Table {
	return &ir.Table{
		Name: "arr",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Array{Element: elem}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "arr_pkey", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func raggedRefusalMentions(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

// pgArrayScalarString runs a single-value query with args (the shared
// pgScalarInt helper takes none) and returns the scalar as a string.
func pgArrayScalarString(t *testing.T, dsn, query string, args ...any) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var out string
	if err := db.QueryRow(query, args...).Scan(&out); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return out
}
