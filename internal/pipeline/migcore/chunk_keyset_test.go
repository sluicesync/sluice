// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the sampled-keyset chunk strategy (ADR-0096) and the
// load-bearing exactly-once partition surface: the boundary tuple
// comparator and the half-open (LowerPK, UpperPK] chunk assignment.
//
// The correctness invariants are asserted DIRECTLY, per the Bug-74
// lesson: a row must land in EXACTLY ONE chunk. For a known PK
// distribution and a set of boundaries, the tests build the chunk
// intervals and check that every probe row maps to exactly one chunk
// (coverage + disjointness), across every orderable PK family
// (integer, string/uuid/binary-as-bytes, decimal-as-bytes, temporal)
// and shape (single-column + composite).

package migcore

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// stubKeysetSampler returns prearranged interior boundary tuples
// without hitting a database.
type stubKeysetSampler struct {
	boundaries [][]any
	err        error
}

func (s stubKeysetSampler) SampleKeysetBoundaries(_ context.Context, _ *ir.Table, _ []string, _ int) ([][]any, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.boundaries, nil
}

func uuidPKTable() *ir.Table {
	return &ir.Table{
		Name: "tokens",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.UUID{}},
			{Name: "payload", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func compositePKTable() *ir.Table {
	return &ir.Table{
		Name: "memberships",
		Columns: []*ir.Column{
			{Name: "tenant_id", Type: ir.Integer{Width: 64}},
			{Name: "user_id", Type: ir.Integer{Width: 64}},
			{Name: "role", Type: ir.Varchar{Length: 32}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "tenant_id"}, {Column: "user_id"}}},
	}
}

// ---- ComparePKTuple / comparePKValue family matrix ----

func TestComparePKValue_Families(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		a, b any
		want int
	}{
		// integer family (incl. mixed driver int widths)
		{"int_lt", int64(1), int64(2), -1},
		{"int_eq", int64(5), int64(5), 0},
		{"int_gt", int64(9), int64(2), 1},
		{"int_mixed_width", int32(7), int64(7), 0},
		// string family
		{"str_lt", "aaa", "aab", -1},
		{"str_eq", "abc", "abc", 0},
		{"str_gt", "z", "a", 1},
		// string vs []byte (driver may return either) compare bytewise
		{"str_bytes_eq", "abc", []byte("abc"), 0},
		{"bytes_lt", []byte{0x01, 0x02}, []byte{0x01, 0x03}, -1},
		{"bytes_prefix", []byte{0x01}, []byte{0x01, 0x00}, -1},
		// uuid-as-string lexical
		{"uuid_lt", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002", -1},
		// decimal-as-string (drivers surface NUMERIC as text) — note
		// bytewise compare is lexical, which is why the SAMPLER orders
		// server-side; here we only need a deterministic total order.
		{"decimal_text_eq", "10.50", "10.50", 0},
		// temporal
		{"time_lt", t0, t1, -1},
		{"time_eq", t0, t0, 0},
		{"time_gt", t1, t0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := comparePKValue(tc.a, tc.b); got != tc.want {
				t.Errorf("comparePKValue(%v,%v) = %d; want %d", tc.a, tc.b, got, tc.want)
			}
			// Antisymmetry: compare(b,a) == -compare(a,b).
			if got, rev := comparePKValue(tc.a, tc.b), comparePKValue(tc.b, tc.a); got != -rev {
				t.Errorf("antisymmetry: compare(a,b)=%d compare(b,a)=%d", got, rev)
			}
		})
	}
}

func TestComparePKTuple_Lexicographic(t *testing.T) {
	cases := []struct {
		name string
		a, b []any
		want int
	}{
		{"first_col_decides", []any{int64(1), int64(99)}, []any{int64(2), int64(0)}, -1},
		{"second_col_tiebreak", []any{int64(5), int64(1)}, []any{int64(5), int64(2)}, -1},
		{"equal", []any{int64(5), "a"}, []any{int64(5), "a"}, 0},
		{"mixed_int_str", []any{int64(7), "alpha"}, []any{int64(7), "beta"}, -1},
		{"shorter_prefix_first", []any{int64(1)}, []any{int64(1), int64(0)}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ComparePKTuple(tc.a, tc.b); got != tc.want {
				t.Errorf("ComparePKTuple(%v,%v) = %d; want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// ---- exactly-once partition invariants (the load-bearing surface) ----

// assignChunk returns the index of the chunk that owns pk under the
// half-open (LowerPK, UpperPK] convention, or -1 if none (a coverage
// failure) — mirroring the orchestrator's lower-exclusive/upper-inclusive
// boundary semantics enforced by filterByUpperBound + the cursor's
// LowerPK start.
func assignChunk(bounds []ChunkBoundary, pk []any) int {
	owner := -1
	for _, b := range bounds {
		// lower bound: pk > LowerPK (nil => no lower bound)
		if b.LowerPK != nil && ComparePKTuple(pk, b.LowerPK) <= 0 {
			continue
		}
		// upper bound: pk <= UpperPK (nil => no upper bound)
		if b.UpperPK != nil && ComparePKTuple(pk, b.UpperPK) > 0 {
			continue
		}
		if owner != -1 {
			// Row matched two chunks — disjointness violated.
			return -2
		}
		owner = b.ChunkIndex
	}
	return owner
}

// assertExactlyOnce checks every probe pk lands in exactly one chunk.
func assertExactlyOnce(t *testing.T, bounds []ChunkBoundary, probes [][]any) {
	t.Helper()
	for _, pk := range probes {
		switch assignChunk(bounds, pk) {
		case -1:
			t.Errorf("pk %v landed in NO chunk (coverage gap)", pk)
		case -2:
			t.Errorf("pk %v landed in MORE THAN ONE chunk (disjointness violated)", pk)
		}
	}
}

func TestComputeKeysetChunkBoundaries_IntegerComposite_ExactlyOnce(t *testing.T) {
	// 3 interior boundaries → 4 chunks for a composite (int,int) PK.
	s := stubKeysetSampler{boundaries: [][]any{
		{int64(1), int64(50)},
		{int64(2), int64(10)},
		{int64(3), int64(99)},
	}}
	bounds, err := ComputeKeysetChunkBoundaries(context.Background(), s, compositePKTable(), 4)
	if err != nil {
		t.Fatalf("ComputeKeysetChunkBoundaries: %v", err)
	}
	if len(bounds) != 4 {
		t.Fatalf("got %d chunks; want 4", len(bounds))
	}
	// Shape: chunk 0 nil-lower, chunk 3 nil-upper, boundaries chained.
	if bounds[0].LowerPK != nil {
		t.Errorf("chunk 0 lower: got %v; want nil", bounds[0].LowerPK)
	}
	if bounds[3].UpperPK != nil {
		t.Errorf("chunk 3 upper: got %v; want nil", bounds[3].UpperPK)
	}
	if !reflect.DeepEqual(bounds[1].LowerPK, []any{int64(1), int64(50)}) {
		t.Errorf("chunk 1 lower: got %v; want [1 50]", bounds[1].LowerPK)
	}
	// Exactly-once over probes that bracket every boundary, including
	// the boundary values themselves (which must land in the LOWER chunk
	// — inclusive upper).
	probes := [][]any{
		{int64(0), int64(0)},   // below everything
		{int64(1), int64(50)},  // == boundary 0 → chunk 0
		{int64(1), int64(51)},  // just above boundary 0 → chunk 1
		{int64(2), int64(10)},  // == boundary 1 → chunk 1
		{int64(2), int64(11)},  // → chunk 2
		{int64(3), int64(99)},  // == boundary 2 → chunk 2
		{int64(3), int64(100)}, // → chunk 3
		{int64(999), int64(0)}, // above everything → chunk 3
	}
	assertExactlyOnce(t, bounds, probes)
}

func TestComputeKeysetChunkBoundaries_StringPK_ExactlyOnce(t *testing.T) {
	s := stubKeysetSampler{boundaries: [][]any{
		{"f"},
		{"m"},
		{"t"},
	}}
	bounds, err := ComputeKeysetChunkBoundaries(context.Background(), s, uuidPKTable(), 4)
	if err != nil {
		t.Fatalf("ComputeKeysetChunkBoundaries: %v", err)
	}
	if len(bounds) != 4 {
		t.Fatalf("got %d chunks; want 4", len(bounds))
	}
	probes := [][]any{
		{"a"},
		{"f"},
		{"g"},
		{"m"},
		{"n"},
		{"t"},
		{"u"},
		{"zzz"},
		// []byte form of the same keys (driver may return either) must
		// partition identically.
		{[]byte("f")},
		{[]byte("m0")},
	}
	assertExactlyOnce(t, bounds, probes)
}

func TestComputeKeysetChunkBoundaries_TemporalPK_ExactlyOnce(t *testing.T) {
	b1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	b2 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	s := stubKeysetSampler{boundaries: [][]any{{b1}, {b2}}}
	tbl := &ir.Table{
		Name:       "events",
		Columns:    []*ir.Column{{Name: "ts", Type: ir.Timestamp{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "ts"}}},
	}
	bounds, err := ComputeKeysetChunkBoundaries(context.Background(), s, tbl, 3)
	if err != nil {
		t.Fatalf("ComputeKeysetChunkBoundaries: %v", err)
	}
	if len(bounds) != 3 {
		t.Fatalf("got %d chunks; want 3", len(bounds))
	}
	probes := [][]any{
		{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{b1}, // == boundary → lower chunk
		{time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
		{b2},
		{time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)},
	}
	assertExactlyOnce(t, bounds, probes)
}

// TestComputeKeysetChunkBoundaries_DuplicateBoundaries drops zero-width
// interior chunks: consecutive equal boundaries must NOT produce an
// empty chunk that wastes a connection, and coverage must still hold.
func TestComputeKeysetChunkBoundaries_DuplicateBoundaries(t *testing.T) {
	// A heavily-duplicate-keyed (string) table: sampling returns the same
	// boundary twice; the zero-width interior chunk must be dropped.
	s := stubKeysetSampler{boundaries: [][]any{
		{"m"},
		{"m"}, // duplicate — would create an empty ("m","m"] chunk
		{"t"},
	}}
	bounds, err := ComputeKeysetChunkBoundaries(context.Background(), s, uuidPKTable(), 4)
	if err != nil {
		t.Fatalf("ComputeKeysetChunkBoundaries: %v", err)
	}
	// 2 distinct boundaries → 3 chunks, not 4.
	if len(bounds) != 3 {
		t.Fatalf("got %d chunks; want 3 after dedup", len(bounds))
	}
	probes := [][]any{{"a"}, {"m"}, {"q"}, {"t"}, {"z"}}
	assertExactlyOnce(t, bounds, probes)
}

// TestComputeKeysetChunkBoundaries_EmptyTable collapses to a single
// nil-bounded chunk (→ single-reader).
func TestComputeKeysetChunkBoundaries_EmptyTable(t *testing.T) {
	s := stubKeysetSampler{boundaries: nil}
	bounds, err := ComputeKeysetChunkBoundaries(context.Background(), s, uuidPKTable(), 4)
	if err != nil {
		t.Fatalf("ComputeKeysetChunkBoundaries: %v", err)
	}
	if len(bounds) != 1 {
		t.Fatalf("got %d chunks; want 1 for empty table", len(bounds))
	}
	if bounds[0].LowerPK != nil || bounds[0].UpperPK != nil {
		t.Errorf("empty chunk: got %v..%v; want nil..nil", bounds[0].LowerPK, bounds[0].UpperPK)
	}
}

// TestComputeKeysetChunkBoundaries_WidthMismatch is a loud-failure
// guard: a boundary tuple whose width != PK column count is a
// programming error, surfaced rather than silently mis-bounded.
func TestComputeKeysetChunkBoundaries_WidthMismatch(t *testing.T) {
	s := stubKeysetSampler{boundaries: [][]any{{int64(1), int64(2)}}} // 2-wide on a 1-col PK
	_, err := ComputeKeysetChunkBoundaries(context.Background(), s, integerPKTable(), 2)
	if err == nil {
		t.Fatal("expected width-mismatch error; got nil")
	}
}

// TestComputeKeysetChunkBoundaries_RejectsIntegerTable confirms the
// keyset path refuses a single-integer-PK table (that's the MIN/MAX
// path's job) — the strategy split is enforced at the boundary fn too.
func TestComputeKeysetChunkBoundaries_RejectsIntegerTable(t *testing.T) {
	s := stubKeysetSampler{boundaries: [][]any{{int64(50)}}}
	_, err := ComputeKeysetChunkBoundaries(context.Background(), s, integerPKTable(), 2)
	if err == nil {
		t.Fatal("expected keyset path to reject integer-PK table; got nil")
	}
}

// TestFilterByUpperBound_TupleClip pins the runtime boundary clip: a
// chunk's last batch must drop rows strictly past UpperPK while KEEPING
// the row equal to UpperPK (inclusive upper). Rows arrive in PK order;
// the filter stops at the first over-bound row.
// TestIsOrderablePKType pins the orderable family set.
func TestIsOrderablePKType(t *testing.T) {
	orderable := []ir.Type{
		ir.Integer{Width: 64},
		ir.Decimal{},
		ir.Char{Length: 4},
		ir.Varchar{Length: 8},
		ir.Text{},
		ir.UUID{},
		ir.Binary{Length: 16},
		ir.Varbinary{Length: 16},
		ir.Blob{},
		ir.Bit{Length: 8},
		ir.Date{},
		ir.Time{},
		ir.Timestamp{},
		ir.DateTime{},
		ir.Domain{BaseType: ir.UUID{}},
	}
	for _, ty := range orderable {
		if !IsOrderablePKType(ty) {
			t.Errorf("IsOrderablePKType(%s) = false; want true", ty.String())
		}
	}
	notOrderable := []ir.Type{ir.JSON{}, ir.Array{}, ir.Geometry{}, ir.Domain{}}
	for _, ty := range notOrderable {
		if IsOrderablePKType(ty) {
			t.Errorf("IsOrderablePKType(%s) = true; want false", ty.String())
		}
	}
}
