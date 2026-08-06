// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shape pins for convertArray / buildPGArray — the family × shape
// matrix the Bug-74 discipline demands for an array-element codec.
//
// The class under test is RECTANGULARITY. buildPGArray derives one
// length per dimension from the FIRST sub-array at each depth
// (arrayDims) and then flattens every leaf it finds. Without a
// per-level length check that is not an error but a silent rewrite of
// the value — measured against real pgx v5.10.0 before the fix:
//
//	[[1,2],[3,4,5]] → {{1,2},{3,4}}  (the 5 vanished)
//	[[1],[2,3,4]]   → {{1},{2}}      (the 3 and 4 vanished)
//	[[1,2],[3]]     → panic: index out of range
//
// The matrix runs EVERY element family convertArray dispatches on, not
// one representative: the ragged check lives in the shared generic, but
// each family instantiates it with a different leaf type T and reaches
// it through a different conv closure, so a green int64 pin does not
// cover pgtype.Numeric / pgtype.Timestamptz (exactly the Bug 74 shape).
// Shapes cover rectangular 1-D/2-D/3-D/deeply-nested, empty, NULL
// elements, and every ragged variant including the one that panicked.
package postgres

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// arrayLeafFamily names one convertArray dispatch arm plus a generator for
// a valid leaf value of that family (i makes successive leaves
// distinct so a dropped element is visible in the count).
type arrayLeafFamily struct {
	name string
	elem ir.Type
	leaf func(i int) any
}

// arrayLeafFamilies is the full set of element families convertArray
// dispatches on, EXCEPT timetz — ir.Time{WithTimeZone:true} has no
// faithful binary array leaf and is refused before buildPGArray runs
// (see convertArray); TestConvertArrayTimetzArrayRefused pins that
// arm separately below.
func arrayLeafFamilies() []arrayLeafFamily {
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	return []arrayLeafFamily{
		{"bool", ir.Boolean{}, func(i int) any { return i%2 == 0 }},
		{"int8", ir.Integer{Width: 64}, func(i int) any { return int64(i) }},
		{"float8", ir.Float{Precision: ir.FloatDouble}, func(i int) any { return float64(i) + 0.5 }},
		{"text", ir.Text{Size: ir.TextRegular}, func(i int) any { return fmt.Sprintf("t%d", i) }},
		{"varchar", ir.Varchar{Length: 32}, func(i int) any { return fmt.Sprintf("v%d", i) }},
		{"char", ir.Char{Length: 8}, func(i int) any { return fmt.Sprintf("c%d", i) }},
		{"uuid", ir.UUID{}, func(i int) any { return fmt.Sprintf("00000000-0000-0000-0000-0000000000%02d", i) }},
		{"inet", ir.Inet{}, func(i int) any { return fmt.Sprintf("10.0.0.%d", i) }},
		{"cidr", ir.Cidr{}, func(i int) any { return fmt.Sprintf("10.0.%d.0/24", i) }},
		{"macaddr", ir.Macaddr{}, func(i int) any { return fmt.Sprintf("08:00:2b:01:02:%02d", i) }},
		// The three bytea spellings share ONE convertArray arm, so they are
		// the same code path by construction — but they are listed
		// separately because "by construction" is the reasoning that Bug 74
		// made untrustworthy, and the cost of three rows is nil (item 141).
		{"bytea/blob", ir.Blob{}, func(i int) any { return []byte{0x00, byte(i)} }},
		{"bytea/binary", ir.Binary{Length: 2}, func(i int) any { return []byte{0xff, byte(i)} }},
		{"bytea/varbinary", ir.Varbinary{Length: 4}, func(i int) any { return []byte{byte(i)} }},
		{"numeric", ir.Decimal{Precision: 20, Scale: 4}, func(i int) any { return fmt.Sprintf("%d.5000", i) }},
		{"date", ir.Date{}, func(i int) any { return base.AddDate(0, 0, i) }},
		{"datetime", ir.DateTime{}, func(i int) any { return base.Add(time.Duration(i) * time.Hour) }},
		{"timestamp", ir.Timestamp{}, func(i int) any { return base.Add(time.Duration(i) * time.Hour) }},
		{"timestamptz", ir.Timestamp{WithTimeZone: true}, func(i int) any { return base.Add(time.Duration(i) * time.Hour) }},
		{"time", ir.Time{}, func(i int) any { return fmt.Sprintf("12:34:%02d", i) }},
	}
}

// arrayShapeCase is one input shape, described structurally so it can
// be materialised for any family. Each entry of nested is either an
// int (a run of that many leaves) or another []any (a deeper level);
// a nil entry is a SQL NULL element.
type arrayShapeCase struct {
	name string
	// build returns the []any input; next is a counter the family leaf
	// generator is fed so every leaf differs.
	build func(leaf func(i int) any) []any
	// wantDims is the expected pgtype.Array Dims for a valid shape.
	wantDims []int32
	// wantNilAt lists the row-major indices expected to be SQL NULL.
	wantNilAt []int
	// ragged, when set, expects the coded refusal instead, at this
	// depth with these lengths.
	ragged                bool
	raggedDepth           int
	raggedWant, raggedGot int
}

func arrayShapeCases() []arrayShapeCase {
	// counter-backed leaf helper: successive leaves are distinct.
	run := func(leaf func(i int) any, from, n int) []any {
		out := make([]any, n)
		for i := 0; i < n; i++ {
			out[i] = leaf(from + i)
		}
		return out
	}
	return []arrayShapeCase{
		{
			name:     "rectangular 1-D",
			build:    func(l func(int) any) []any { return run(l, 0, 3) },
			wantDims: []int32{3},
		},
		{
			name:     "rectangular 2-D",
			build:    func(l func(int) any) []any { return []any{run(l, 0, 2), run(l, 2, 2)} },
			wantDims: []int32{2, 2},
		},
		{
			name: "rectangular 3-D",
			build: func(l func(int) any) []any {
				return []any{
					[]any{run(l, 0, 2), run(l, 2, 2)},
					[]any{run(l, 4, 2), run(l, 6, 2)},
				}
			},
			wantDims: []int32{2, 2, 2},
		},
		{
			name: "deeply nested rectangular 4-D",
			build: func(l func(int) any) []any {
				return []any{
					[]any{[]any{run(l, 0, 2)}, []any{run(l, 2, 2)}},
					[]any{[]any{run(l, 4, 2)}, []any{run(l, 6, 2)}},
				}
			},
			wantDims: []int32{2, 2, 1, 2},
		},
		{
			name:     "empty array",
			build:    func(func(int) any) []any { return []any{} },
			wantDims: []int32{0},
		},
		{
			name:     "1-D with NULL elements",
			build:    func(l func(int) any) []any { return []any{l(0), nil, l(2)} },
			wantDims: []int32{3},
			// index 1 is the NULL slot.
			wantNilAt: []int{1},
		},
		{
			name: "2-D with NULL elements",
			build: func(l func(int) any) []any {
				return []any{[]any{l(0), nil}, []any{nil, l(3)}}
			},
			wantDims:  []int32{2, 2},
			wantNilAt: []int{1, 2},
		},
		{
			name:       "ragged: long second row",
			build:      func(l func(int) any) []any { return []any{run(l, 0, 2), run(l, 2, 3)} },
			ragged:     true,
			raggedWant: 2, raggedGot: 3, raggedDepth: 1,
		},
		{
			name:       "ragged: short first row",
			build:      func(l func(int) any) []any { return []any{run(l, 0, 1), run(l, 1, 3)} },
			ragged:     true,
			raggedWant: 1, raggedGot: 3, raggedDepth: 1,
		},
		{
			// The shape that PANICKED before the fix (Elements underran
			// the Dims pgx encodes against).
			name:       "ragged: short trailing row",
			build:      func(l func(int) any) []any { return []any{run(l, 0, 2), run(l, 2, 1)} },
			ragged:     true,
			raggedWant: 2, raggedGot: 1, raggedDepth: 1,
		},
		{
			name: "ragged at inner depth of a 3-D array",
			build: func(l func(int) any) []any {
				return []any{
					[]any{run(l, 0, 1), run(l, 1, 1)},
					[]any{run(l, 2, 1), run(l, 3, 2)},
				}
			},
			ragged:     true,
			raggedWant: 1, raggedGot: 2, raggedDepth: 2,
		},
		{
			// First row empty, second not: arrayDims reads dimension 1 as
			// zero-wide, so every later leaf would have vanished.
			name:       "ragged: empty first row",
			build:      func(l func(int) any) []any { return []any{[]any{}, run(l, 0, 1)} },
			ragged:     true,
			raggedWant: 0, raggedGot: 1, raggedDepth: 1,
		},
	}
}

func TestConvertArrayShapeMatrixAcrossFamilies(t *testing.T) {
	for _, fam := range arrayLeafFamilies() {
		for _, sc := range arrayShapeCases() {
			t.Run(fam.name+"/"+sc.name, func(t *testing.T) {
				got, err := convertArray(sc.build(fam.leaf), fam.elem)
				if sc.ragged {
					assertRaggedRefusal(t, err, sc)
					if got != nil {
						t.Errorf("ragged input returned a value %#v; want nil alongside the refusal", got)
					}
					return
				}
				if err != nil {
					t.Fatalf("convertArray: %v", err)
				}
				dims, elems := pgArrayShape(t, got)
				if !reflect.DeepEqual(dims, sc.wantDims) {
					t.Errorf("Dims = %v; want %v", dims, sc.wantDims)
				}
				// The invariant the bug broke: the flat element list must
				// have EXACTLY the cardinality Dims claims. More means
				// elements pgx will never write (silent drop); fewer means
				// an out-of-range read at encode time.
				want := 1
				for _, d := range sc.wantDims {
					want *= int(d)
				}
				if len(elems) != want {
					t.Errorf("len(Elements) = %d; want %d (product of %v)", len(elems), want, sc.wantDims)
				}
				assertNilSlots(t, elems, sc.wantNilAt)
			})
		}
	}
}

// TestConvertArrayIntegerRowMajorOrder pins that a valid multi-dim
// array flattens row-major with every leaf present and in order — the
// positive half of what the ragged check protects. One family is
// enough here: ordering is family-independent (the shared flatten),
// whereas the SHAPE matrix above is not (per-family leaf type T).
func TestConvertArrayIntegerRowMajorOrder(t *testing.T) {
	in := []any{
		[]any{int64(1), int64(2), int64(3)},
		[]any{int64(4), int64(5), int64(6)},
	}
	got, err := convertArray(in, ir.Integer{Width: 64})
	if err != nil {
		t.Fatalf("convertArray: %v", err)
	}
	dims, elems := pgArrayShape(t, got)
	if !reflect.DeepEqual(dims, []int32{2, 3}) {
		t.Fatalf("Dims = %v; want [2 3]", dims)
	}
	for i, want := range []int64{1, 2, 3, 4, 5, 6} {
		p, ok := elems[i].(*int64)
		if !ok || p == nil {
			t.Fatalf("Elements[%d] = %#v; want *int64", i, elems[i])
		}
		if *p != want {
			t.Errorf("Elements[%d] = %d; want %d", i, *p, want)
		}
	}
}

// TestConvertArrayTimetzArrayRefused pins the one convertArray arm that
// never reaches buildPGArray: timetz has no faithful binary array leaf,
// so it is refused ahead of any shape handling (the loud-failure tenet).
func TestConvertArrayTimetzArrayRefused(t *testing.T) {
	_, err := convertArray([]any{"12:34:56+02"}, ir.Time{WithTimeZone: true})
	if err == nil {
		t.Fatal("timetz array was accepted; want a refusal")
	}
}

// assertRaggedRefusal checks the refusal is the coded, operator-facing
// one — not a bare error, not a panic — and that its message carries
// the depth and the expected-vs-actual lengths an operator needs.
func assertRaggedRefusal(t *testing.T, err error, sc arrayShapeCase) {
	t.Helper()
	if err == nil {
		t.Fatal("ragged array was accepted; want a coded refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("error %v carries no sluicecode; want %s", err, sluicecode.CodeValueRaggedArray)
	}
	if ce.Code != sluicecode.CodeValueRaggedArray {
		t.Errorf("code = %s; want %s", ce.Code, sluicecode.CodeValueRaggedArray)
	}
	if got := ce.ExitCode(); got != sluicecode.ExitRefusal {
		t.Errorf("exit code = %d; want %d (a refusal, not a generic failure)", got, sluicecode.ExitRefusal)
	}
	msg := err.Error()
	for _, want := range []string{
		fmt.Sprintf("depth %d", sc.raggedDepth),
		fmt.Sprintf("has %d element", sc.raggedGot),
		fmt.Sprintf("dimension is %d wide", sc.raggedWant),
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q does not name %q", msg, want)
		}
	}
}

// assertNilSlots verifies exactly the expected row-major positions hold
// a typed-nil element pointer (the SQL NULL slot).
func assertNilSlots(t *testing.T, elems []any, wantNilAt []int) {
	t.Helper()
	want := map[int]bool{}
	for _, i := range wantNilAt {
		want[i] = true
	}
	for i, e := range elems {
		isNil := reflect.ValueOf(e).IsNil()
		if isNil != want[i] {
			t.Errorf("Elements[%d] nil = %v; want %v", i, isNil, want[i])
		}
	}
}

// pgArrayShape reads Dims and Elements off a pgtype.Array[*T] without
// naming T — convertArray returns a different instantiation per element
// family, and the shape matrix has to inspect all of them uniformly.
func pgArrayShape(t *testing.T, v any) (dims []int32, elems []any) {
	t.Helper()
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Struct {
		t.Fatalf("convertArray returned %T; want a pgtype.Array[*T] struct", v)
	}
	d := rv.FieldByName("Dims")
	e := rv.FieldByName("Elements")
	if !d.IsValid() || !e.IsValid() {
		t.Fatalf("convertArray returned %T; want a value with Dims and Elements", v)
	}
	for i := 0; i < d.Len(); i++ {
		dims = append(dims, int32(d.Index(i).FieldByName("Length").Int()))
	}
	for i := 0; i < e.Len(); i++ {
		elems = append(elems, e.Index(i).Interface())
	}
	return dims, elems
}
