// Unit tests for the chunk-boundary computation.

package pipeline

import (
	"context"
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// stubRangeQuerier returns prearranged min/max values without hitting
// a database. Tests construct one per fixture so the eligibility
// branches and the value paths are exercised independently.
type stubRangeQuerier struct {
	minVal any
	maxVal any
	err    error
}

func (s stubRangeQuerier) RangeBounds(_ context.Context, _ *ir.Table, _ string) (minVal, maxVal any, err error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	return s.minVal, s.maxVal, nil
}

// integerPKTable is a tiny fixture for the integer-PK eligible path.
func integerPKTable() *ir.Table {
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{
			Name:    "pk",
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
}

// TestCanParallelChunkTable_Eligibility covers the happy-path and the
// four documented fall-back branches.
func TestCanParallelChunkTable_Eligibility(t *testing.T) {
	cases := []struct {
		name        string
		table       *ir.Table
		parallelism int
		want        bool
		wantSubstr  string
	}{
		{"happy", integerPKTable(), 4, true, ""},
		{"parallelism_1", integerPKTable(), 1, false, "single-reader"},
		{"no_pk", &ir.Table{Name: "log", Columns: []*ir.Column{{Name: "x", Type: ir.Integer{Width: 64}}}}, 4, false, "no primary key"},
		{
			"composite_pk",
			&ir.Table{
				Name: "join",
				Columns: []*ir.Column{
					{Name: "a", Type: ir.Integer{Width: 64}},
					{Name: "b", Type: ir.Integer{Width: 64}},
				},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
			},
			4, false, "composite",
		},
		{
			"non_integer_pk",
			&ir.Table{
				Name: "tokens",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.UUID{}},
				},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
			4, false, "supports integer PKs",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := canParallelChunkTable(tc.table, tc.parallelism)
			if ok != tc.want {
				t.Errorf("canParallelChunkTable: got %v, %q; want %v", ok, reason, tc.want)
			}
			if !tc.want && tc.wantSubstr != "" && !contains(reason, tc.wantSubstr) {
				t.Errorf("reason %q does not contain %q", reason, tc.wantSubstr)
			}
		})
	}
}

// TestComputeChunkBoundaries_HappyPath confirms an integer PK divides
// into N near-equal slices with the expected nil-bound shape.
func TestComputeChunkBoundaries_HappyPath(t *testing.T) {
	q := stubRangeQuerier{minVal: int64(1), maxVal: int64(100)}
	bounds, err := computeChunkBoundaries(context.Background(), q, integerPKTable(), 4)
	if err != nil {
		t.Fatalf("computeChunkBoundaries: %v", err)
	}
	if len(bounds) != 4 {
		t.Fatalf("got %d chunks; want 4", len(bounds))
	}
	// Chunk 0: lower=nil, upper=26 (1 + 100/4 = 26)
	if bounds[0].lowerPK != nil {
		t.Errorf("chunk 0 lower: got %v; want nil", bounds[0].lowerPK)
	}
	if !reflect.DeepEqual(bounds[0].upperPK, []any{int64(26)}) {
		t.Errorf("chunk 0 upper: got %v; want [26]", bounds[0].upperPK)
	}
	// Chunk 3 (last): lower=76, upper=nil
	if !reflect.DeepEqual(bounds[3].lowerPK, []any{int64(76)}) {
		t.Errorf("chunk 3 lower: got %v; want [76]", bounds[3].lowerPK)
	}
	if bounds[3].upperPK != nil {
		t.Errorf("chunk 3 upper: got %v; want nil", bounds[3].upperPK)
	}
	// Chunk indices are monotonic.
	for i, b := range bounds {
		if b.chunkIndex != i {
			t.Errorf("bounds[%d].chunkIndex = %d; want %d", i, b.chunkIndex, i)
		}
	}
}

// TestComputeChunkBoundaries_EmptyTable confirms an empty source
// (MIN/MAX both NULL) returns a single empty chunk so the parallel
// path collapses to single-reader without a separate code path.
func TestComputeChunkBoundaries_EmptyTable(t *testing.T) {
	q := stubRangeQuerier{minVal: nil, maxVal: nil}
	bounds, err := computeChunkBoundaries(context.Background(), q, integerPKTable(), 4)
	if err != nil {
		t.Fatalf("computeChunkBoundaries: %v", err)
	}
	if len(bounds) != 1 {
		t.Fatalf("got %d chunks; want 1 for empty table", len(bounds))
	}
	if bounds[0].lowerPK != nil || bounds[0].upperPK != nil {
		t.Errorf("empty-table chunk: got bounds %v..%v; want nil..nil",
			bounds[0].lowerPK, bounds[0].upperPK)
	}
}

// TestComputeChunkBoundaries_FewerRowsThanChunks confirms that a
// table with fewer rows than the requested chunk count collapses
// gracefully rather than producing empty chunks.
func TestComputeChunkBoundaries_FewerRowsThanChunks(t *testing.T) {
	q := stubRangeQuerier{minVal: int64(1), maxVal: int64(3)}
	bounds, err := computeChunkBoundaries(context.Background(), q, integerPKTable(), 8)
	if err != nil {
		t.Fatalf("computeChunkBoundaries: %v", err)
	}
	// span = 3 (rows 1, 2, 3); we asked for 8 chunks but got 3.
	if len(bounds) != 3 {
		t.Errorf("got %d chunks; want 3 for 3-row table", len(bounds))
	}
}

// TestComputeChunkBoundaries_SingleRow confirms degenerate cases
// (min==max) produce one chunk covering the whole row.
func TestComputeChunkBoundaries_SingleRow(t *testing.T) {
	q := stubRangeQuerier{minVal: int64(42), maxVal: int64(42)}
	bounds, err := computeChunkBoundaries(context.Background(), q, integerPKTable(), 4)
	if err != nil {
		t.Fatalf("computeChunkBoundaries: %v", err)
	}
	if len(bounds) != 1 {
		t.Errorf("got %d chunks; want 1 for single-row table", len(bounds))
	}
}

// TestResolveBulkParallelism covers the "0 = use min(default, NumCPU)"
// rule and the negative-clamp.
func TestResolveBulkParallelism(t *testing.T) {
	cases := []struct {
		configured, numCPU, want int
	}{
		{0, 16, 8},   // capped at default
		{0, 4, 4},    // small CPU host
		{4, 16, 4},   // explicit override
		{16, 16, 16}, // explicit override above default
		{-1, 16, 1},  // negative clamps to 1
		{1, 16, 1},   // explicit 1 honoured
	}
	for _, c := range cases {
		got := resolveBulkParallelism(c.configured, c.numCPU)
		if got != c.want {
			t.Errorf("resolveBulkParallelism(%d, %d) = %d; want %d",
				c.configured, c.numCPU, got, c.want)
		}
	}
}

// TestCoerceInt64 covers the realistic driver-return shapes plus the
// rejected ones.
func TestCoerceInt64(t *testing.T) {
	cases := []struct {
		in   any
		want int64
		ok   bool
	}{
		{int64(42), 42, true},
		{int32(7), 7, true},
		{int(99), 99, true},
		{uint64(100), 100, true},
		{uint32(11), 11, true},
		{[]byte("123"), 0, false}, // numeric-as-bytes shape rejected
		{"42", 0, false},          // string shape rejected
		{nil, 0, false},           // defensive
	}
	for _, c := range cases {
		got, ok := coerceInt64(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("coerceInt64(%v %T): got (%d, %v); want (%d, %v)",
				c.in, c.in, got, ok, c.want, c.ok)
		}
	}
}

// contains is the std library's strings.Contains hand-rolled to keep
// the test file's import list minimal.
func contains(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
