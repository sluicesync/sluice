// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the chunk-boundary computation.

package pipeline

import (
	"context"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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

// TestCanParallelChunkTable_Eligibility covers the happy-path, the
// strategy classification (ADR-0096), and the documented fall-backs.
func TestCanParallelChunkTable_Eligibility(t *testing.T) {
	jsonPK := &ir.Table{
		Name:       "blobs",
		Columns:    []*ir.Column{{Name: "doc", Type: ir.JSON{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "doc"}}},
	}
	cases := []struct {
		name         string
		table        *ir.Table
		parallelism  int
		want         bool
		wantStrategy chunkStrategy
		wantSubstr   string
	}{
		{"happy_integer", integerPKTable(), 4, true, strategyMinMaxDivide, ""},
		{"parallelism_1", integerPKTable(), 1, false, strategyNone, "single-reader"},
		{"no_pk", &ir.Table{Name: "log", Columns: []*ir.Column{{Name: "x", Type: ir.Integer{Width: 64}}}}, 4, false, strategyNone, "no primary key"},
		{
			// Composite PK is now keyset-eligible (ADR-0096).
			"composite_pk_keyset",
			&ir.Table{
				Name: "join",
				Columns: []*ir.Column{
					{Name: "a", Type: ir.Integer{Width: 64}},
					{Name: "b", Type: ir.Integer{Width: 64}},
				},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
			},
			4, true, strategyKeysetSample, "",
		},
		{
			// Single non-integer orderable PK → keyset (ADR-0096).
			"uuid_pk_keyset",
			&ir.Table{
				Name:       "tokens",
				Columns:    []*ir.Column{{Name: "id", Type: ir.UUID{}}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
			4, true, strategyKeysetSample, "",
		},
		{
			"varchar_pk_keyset",
			&ir.Table{
				Name:       "users",
				Columns:    []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 255}}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "email"}}},
			},
			4, true, strategyKeysetSample, "",
		},
		{
			// Non-orderable PK type → single-reader fallback.
			"json_pk_fallback", jsonPK, 4, false, strategyNone, "not an orderable chunk key",
		},
		{
			// ADR-0048 Shape A: --inject-shard-column rewrites the PK as a
			// composite (injected discriminator, …source PK). The injected
			// leading column exists only on the target-planning schema, NOT
			// the source, so the keyset boundary sample / bounded read would
			// reference it against the source and fail SQLSTATE 42703. Must
			// route to single-reader (pre-ADR-0096 behaviour). This is the
			// regression the shard_injection cold-start integration test hit.
			"shard_injected_composite_pk_fallback",
			&ir.Table{
				Name: "widgets",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true},
				},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "shard_id"}, {Column: "id"}}},
			},
			4, false, strategyNone, "sluice-injected",
		},
		{
			// Pin the CLASS, not the shard-leading representative: ANY
			// sluice-injected PK column (even a hypothetical single-column or
			// trailing-position one) is unreadable from the source and must
			// fall back, independent of position in the PK tuple.
			"single_injected_pk_fallback",
			&ir.Table{
				Name:       "injected",
				Columns:    []*ir.Column{{Name: "shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "shard_id"}}},
			},
			4, false, strategyNone, "sluice-injected",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, strategy, reason := canParallelChunkTable(tc.table, tc.parallelism)
			if ok != tc.want {
				t.Errorf("canParallelChunkTable: got %v, %q; want %v", ok, reason, tc.want)
			}
			if ok && strategy != tc.wantStrategy {
				t.Errorf("strategy: got %d; want %d", strategy, tc.wantStrategy)
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

// TestUseFastLoader is the ADR-0043 four-gate truth table. The gate
// is "fast loader IFF NOT resuming AND zero prior progress AND NOT
// force-cold-start". Gate (4) (live-add) is structurally vacuous for
// copyChunk and intentionally not a parameter (see useFastLoader's
// doc-comment), so it does not appear here.
func TestUseFastLoader(t *testing.T) {
	fresh := ir.TableChunkProgress{State: ir.TableProgressInProgress}
	withCursor := ir.TableChunkProgress{
		State:      ir.TableProgressInProgress,
		LastPK:     []any{int64(500)},
		RowsCopied: 500,
	}
	rowsOnly := ir.TableChunkProgress{
		State:      ir.TableProgressInProgress,
		RowsCopied: 12,
	}
	pkOnly := ir.TableChunkProgress{
		State:  ir.TableProgressInProgress,
		LastPK: []any{int64(1)},
	}
	complete := ir.TableChunkProgress{State: ir.TableProgressComplete}

	cases := []struct {
		name           string
		resuming       bool
		forceColdStart bool
		chunk          ir.TableChunkProgress
		want           bool
	}{
		// The single true row: cold, fresh, no force, zero progress.
		{"cold_fresh_noforce", false, false, fresh, true},

		// Gate (1): resume always disables the fast loader, even on a
		// zero-progress chunk (the crash-replay safety property).
		{"resume_even_if_fresh", true, false, fresh, false},
		{"resume_with_cursor", true, false, withCursor, false},

		// Gate (2): any recorded prior progress disables it.
		{"prior_cursor_and_rows", false, false, withCursor, false},
		{"prior_rows_only", false, false, rowsOnly, false},
		{"prior_pk_only", false, false, pkOnly, false},
		{"prior_state_complete", false, false, complete, false},

		// Gate (3): --force-cold-start disables it (target may be
		// populated; non-upsert WriteRows would collide).
		{"force_cold_start_even_if_fresh", false, true, fresh, false},

		// All gates failing simultaneously.
		{"resume_and_force_and_progress", true, true, withCursor, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := useFastLoader(tc.resuming, tc.forceColdStart, tc.chunk)
			if got != tc.want {
				t.Errorf("useFastLoader(resuming=%v, force=%v, chunk=%+v) = %v; want %v",
					tc.resuming, tc.forceColdStart, tc.chunk, got, tc.want)
			}
		})
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

// TestResolveBulkParallelMinRows pins the roadmap item 3 phase (b) adaptive
// threshold: explicit values are honoured verbatim (never auto-lowered),
// while the 0=auto sentinel scales the threshold down as the table count
// rises — preserving the single-table default and flooring at 10k.
func TestResolveBulkParallelMinRows(t *testing.T) {
	cases := []struct {
		name       string
		configured int64
		tableCount int
		want       int64
	}{
		{"explicit honoured verbatim, ignores tableCount", 100_000, 30, 100_000},
		{"explicit honoured for single table", 100_000, 1, 100_000},
		{"explicit small honoured (not auto-lowered)", 5_000, 50, 5_000},
		{"explicit 1 honoured", 1, 100, 1},
		{"auto single table = full default", 0, 1, 80_000},
		{"auto zero tables (defensive) = full default", 0, 0, 80_000},
		{"auto 2 tables = default/2", 0, 2, 40_000},
		{"auto 4 tables = default/4", 0, 4, 20_000},
		{"auto 5 tables = default/5", 0, 5, 16_000},
		{"auto 8 tables = floor (default/8 == floor)", 0, 8, 10_000},
		{"auto 30 tables = floored at 10k", 0, 30, 10_000},
		{"auto 1000 tables = floored at 10k", 0, 1000, 10_000},
		{"negative treated as auto", -1, 1, 80_000},
	}
	prev := int64(1<<62 - 1)
	for _, c := range cases {
		got := resolveBulkParallelMinRows(c.configured, c.tableCount)
		if got != c.want {
			t.Errorf("%s: resolveBulkParallelMinRows(%d, %d) = %d; want %d",
				c.name, c.configured, c.tableCount, got, c.want)
		}
		if got < adaptiveBulkParallelMinRowsFloor && c.configured <= 0 {
			t.Errorf("%s: auto result %d below floor %d", c.name, got, adaptiveBulkParallelMinRowsFloor)
		}
	}
	// Monotonic non-increasing as the table count rises (auto path).
	for tc := 1; tc <= 64; tc++ {
		got := resolveBulkParallelMinRows(0, tc)
		if got > prev {
			t.Errorf("auto threshold not monotonic: tableCount=%d gave %d > previous %d", tc, got, prev)
		}
		prev = got
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

// countProbeReader is a stub [ir.RowReader] that records which row-count
// surface the orchestrator probed. It can selectively implement
// [ir.RowCounter] and/or [ir.RowCountEstimator] so the dispatch preference
// is observable.
type countProbeReader struct {
	estimateVal  int64
	estimateUsed *bool
	countVal     int64
	countUsed    *bool
}

func (r *countProbeReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return nil, nil
}
func (r *countProbeReader) Err() error { return nil }

// countOnlyReader implements only [ir.RowCounter].
type countOnlyReader struct{ countProbeReader }

func (r *countOnlyReader) CountRows(context.Context, *ir.Table) (int64, error) {
	if r.countUsed != nil {
		*r.countUsed = true
	}
	return r.countVal, nil
}

// estimatorReader implements BOTH surfaces — the orchestrator must prefer
// the estimator for the chunk decision.
type estimatorReader struct{ countProbeReader }

func (r *estimatorReader) CountRows(context.Context, *ir.Table) (int64, error) {
	if r.countUsed != nil {
		*r.countUsed = true
	}
	return r.countVal, nil
}

func (r *estimatorReader) EstimateRowCount(context.Context, *ir.Table) (int64, error) {
	if r.estimateUsed != nil {
		*r.estimateUsed = true
	}
	return r.estimateVal, nil
}

// plainReader implements neither surface.
type plainReader struct{ countProbeReader }

// TestApproximateRowCount_PrefersEstimator pins the ADR-0079 v1.1
// orchestrator change: the chunk-DECISION probe prefers
// [ir.RowCountEstimator] over [ir.RowCounter] (the latter is also the ETA
// probe and returns 0 on pinned readers by design), falls back to
// CountRows when only that exists, and returns 0 when neither does.
func TestApproximateRowCount_PrefersEstimator(t *testing.T) {
	table := integerPKTable()

	t.Run("prefers estimator when both implemented", func(t *testing.T) {
		var estUsed, cntUsed bool
		r := &estimatorReader{countProbeReader{
			estimateVal: 5000, estimateUsed: &estUsed,
			countVal: 0, countUsed: &cntUsed,
		}}
		got, err := approximateRowCount(context.Background(), r, table)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !estUsed {
			t.Error("expected EstimateRowCount to be used")
		}
		if cntUsed {
			t.Error("CountRows must NOT be used when EstimateRowCount is present (it is the ETA path; on a pinned reader it returns 0 → would wrongly single-stream)")
		}
		if got != 5000 {
			t.Errorf("got %d; want 5000 (the estimate)", got)
		}
	})

	t.Run("falls back to CountRows when only RowCounter", func(t *testing.T) {
		var cntUsed bool
		r := &countOnlyReader{countProbeReader{countVal: 1234, countUsed: &cntUsed}}
		got, err := approximateRowCount(context.Background(), r, table)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cntUsed {
			t.Error("expected CountRows to be used")
		}
		if got != 1234 {
			t.Errorf("got %d; want 1234", got)
		}
	})

	t.Run("returns 0 when neither implemented", func(t *testing.T) {
		got, err := approximateRowCount(context.Background(), &plainReader{}, table)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 0 {
			t.Errorf("got %d; want 0", got)
		}
	})
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
