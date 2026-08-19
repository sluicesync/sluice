// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0184 per-index ALTER split: the pure flavor×size
// decision, the pure emission dispatcher (combined vs per-index shape,
// FULLTEXT/SPATIAL standalone in both, order preserved), the standalone
// per-index emitter, and — driven through the real buildTableIndexes
// chokepoint against the fake MySQL — that the split engages ONLY for a large
// table on a VStream flavor, reaches both the whole-schema CreateIndexes and
// the VStream serial-overlap entry points, and that a --resume emits one
// bounded ALTER per STILL-MISSING index (never re-touching a present one).
//
// The fb* fakes (fbRecorder / newFBFakeDB / fbSchemaOneTable / fbBTreeIdx /
// fbFulltextIdx) are shared with schema_writer_index_fallback_test.go.

package mysql

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// splitSpatialIdx builds a SPATIAL index fixture (kept out of the shared
// fallback-test helpers, which only need BTREE/FULLTEXT).
func splitSpatialIdx(name, col string) *ir.Index {
	return &ir.Index{Name: name, Columns: []ir.IndexColumn{{Column: col}}, Kind: ir.IndexKindSpatial}
}

func splitUniqueIdx(name, col string) *ir.Index {
	return &ir.Index{Name: name, Unique: true, Columns: []ir.IndexColumn{{Column: col}}}
}

// execsOf snapshots the recorder's attempted ALTERs under its lock.
func execsOf(rec *fbRecorder) []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.execs...)
}

// TestShouldSplitIndexBuildPerIndex pins the PURE ADR-0184 decision across the
// flavor × size family matrix: the split fires iff the flavor is VStream
// (PlanetScale/Vitess) AND DATA_LENGTH is at or above the threshold. Every
// other cell keeps the ADR-0080 combined ALTER. Anti-vacuity: the matrix
// carries at least one true and one false expectation.
func TestShouldSplitIndexBuildPerIndex(t *testing.T) {
	const thr = indexSplitPerIndexTableBytes
	cases := []struct {
		name   string
		flavor Flavor
		bytes  int64
		want   bool
	}{
		{"vanilla + small", FlavorVanilla, 1 << 20, false},
		{"vanilla + at threshold", FlavorVanilla, thr, false},
		{"vanilla + huge", FlavorVanilla, thr * 4, false},
		{"planetscale + small", FlavorPlanetScale, thr - 1, false},
		{"planetscale + at threshold", FlavorPlanetScale, thr, true},
		{"planetscale + huge", FlavorPlanetScale, thr * 4, true},
		{"vitess + small", FlavorVitess, thr - 1, false},
		{"vitess + at threshold", FlavorVitess, thr, true},
		{"vitess + huge", FlavorVitess, thr * 4, true},
		{"planetscale + zero (probe unavailable)", FlavorPlanetScale, 0, false},
	}
	var sawTrue, sawFalse bool
	for _, c := range cases {
		if got := shouldSplitIndexBuildPerIndex(c.flavor, c.bytes); got != c.want {
			t.Errorf("%s: shouldSplitIndexBuildPerIndex(%v, %d) = %v; want %v", c.name, c.flavor, c.bytes, got, c.want)
		}
		sawTrue = sawTrue || c.want
		sawFalse = sawFalse || !c.want
	}
	if !sawTrue || !sawFalse {
		t.Fatalf("anti-vacuity: matrix must carry both a true and a false cell (true=%v false=%v)", sawTrue, sawFalse)
	}
}

// TestBuildIndexStatementsFor_FlavorSizeMatrix pins the PURE emission
// dispatcher for every flavor × {small,large} cell using a mixed index set
// (two combinable BTREE/UNIQUE + a FULLTEXT + a SPATIAL). Only the large +
// VStream cells split; each emitted shape is asserted, not just the flag:
//
//   - combined: ONE ALTER groups the two combinable clauses, FULLTEXT and
//     SPATIAL each standalone → 3 statements.
//   - per-index: every index its own ALTER, in incoming order → 4 statements,
//     FULLTEXT and SPATIAL still standalone.
func TestBuildIndexStatementsFor_FlavorSizeMatrix(t *testing.T) {
	idxs := []*ir.Index{
		fbBTreeIdx("a_idx", "a"),
		splitUniqueIdx("b_uniq", "b"),
		fbFulltextIdx("ft_body", "body"),
		splitSpatialIdx("geo_sp", "geo"),
	}
	wantCombined := []string{
		"ALTER TABLE `t` ADD INDEX `a_idx` (`a`), ADD UNIQUE INDEX `b_uniq` (`b`);",
		"ALTER TABLE `t` ADD FULLTEXT INDEX `ft_body` (`body`);",
		"ALTER TABLE `t` ADD SPATIAL INDEX `geo_sp` (`geo`);",
	}
	wantPerIndex := []string{
		"ALTER TABLE `t` ADD INDEX `a_idx` (`a`);",
		"ALTER TABLE `t` ADD UNIQUE INDEX `b_uniq` (`b`);",
		"ALTER TABLE `t` ADD FULLTEXT INDEX `ft_body` (`body`);",
		"ALTER TABLE `t` ADD SPATIAL INDEX `geo_sp` (`geo`);",
	}

	const thr = indexSplitPerIndexTableBytes
	cases := []struct {
		name      string
		flavor    Flavor
		bytes     int64
		wantSplit bool
	}{
		{"vanilla large stays combined", FlavorVanilla, thr * 4, false},
		{"vanilla small stays combined", FlavorVanilla, 1 << 20, false},
		{"planetscale small stays combined", FlavorPlanetScale, thr - 1, false},
		{"planetscale large splits", FlavorPlanetScale, thr, true},
		{"vitess large splits", FlavorVitess, thr * 8, true},
		{"vitess small stays combined", FlavorVitess, 1 << 10, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stmts, split, err := buildIndexStatementsFor(c.flavor, c.bytes, "t", idxs, false)
			if err != nil {
				t.Fatalf("buildIndexStatementsFor: %v", err)
			}
			if split != c.wantSplit {
				t.Fatalf("split = %v; want %v", split, c.wantSplit)
			}
			want := wantCombined
			if c.wantSplit {
				want = wantPerIndex
			}
			if !equalStmts(stmts, want) {
				t.Errorf("statements mismatch\n got  %#v\n want %#v", stmts, want)
			}
		})
	}
}

// TestEmitCreateIndexesPerIndex pins the standalone emitter directly: every
// kind gets its own ALTER, incoming order is preserved, and — the resume
// property at the emit layer — passing only a SUBSET (the still-missing
// indexes the caller computes after the detect-then-skip probe) yields exactly
// one bounded ALTER per remaining index.
func TestEmitCreateIndexesPerIndex(t *testing.T) {
	t.Run("every kind standalone, order preserved", func(t *testing.T) {
		idxs := []*ir.Index{
			splitUniqueIdx("u_first", "u"),
			fbBTreeIdx("b_second", "b"),
			fbFulltextIdx("ft_third", "body"),
			splitSpatialIdx("sp_fourth", "geo"),
		}
		stmts, err := emitCreateIndexesPerIndex("t", idxs, false)
		if err != nil {
			t.Fatalf("emitCreateIndexesPerIndex: %v", err)
		}
		want := []string{
			"ALTER TABLE `t` ADD UNIQUE INDEX `u_first` (`u`);",
			"ALTER TABLE `t` ADD INDEX `b_second` (`b`);",
			"ALTER TABLE `t` ADD FULLTEXT INDEX `ft_third` (`body`);",
			"ALTER TABLE `t` ADD SPATIAL INDEX `sp_fourth` (`geo`);",
		}
		if !equalStmts(stmts, want) {
			t.Errorf("\n got  %#v\n want %#v", stmts, want)
		}
	})

	t.Run("resume subset emits only the still-missing indexes", func(t *testing.T) {
		// The caller (buildTableIndexes) filters out already-present indexes
		// before calling emit, so on a resume it passes only [3rd, 4th].
		pending := []*ir.Index{
			fbBTreeIdx("idx_3", "c"),
			fbBTreeIdx("idx_4", "d"),
		}
		stmts, err := emitCreateIndexesPerIndex("t", pending, false)
		if err != nil {
			t.Fatalf("emitCreateIndexesPerIndex: %v", err)
		}
		want := []string{
			"ALTER TABLE `t` ADD INDEX `idx_3` (`c`);",
			"ALTER TABLE `t` ADD INDEX `idx_4` (`d`);",
		}
		if !equalStmts(stmts, want) {
			t.Errorf("\n got  %#v\n want %#v", stmts, want)
		}
	})
}

// newSplitWriter builds a SchemaWriter over the fake MySQL for a chosen
// flavor. No fallback is injected, so buildTableIndexes runs the DIRECT path
// (the ADR-0184 decision), and the DATA_LENGTH probe is answered from
// rec.dataLength.
func newSplitWriter(t *testing.T, rec *fbRecorder, flavor Flavor) *SchemaWriter {
	t.Helper()
	if rec.failContains == nil {
		rec.failContains = map[string]error{}
	}
	if rec.preExisting == nil {
		rec.preExisting = map[string]bool{}
	}
	if rec.dataLength == nil {
		rec.dataLength = map[string]int64{}
	}
	return &SchemaWriter{
		db:     newFBFakeDB(t, rec),
		schema: "testdb",
		flavor: flavor,
	}
}

// TestBuildTableIndexes_SplitEngagesEndToEnd drives the real buildTableIndexes
// chokepoint (via CreateIndexes — the whole-schema migrate/sync-cold-start
// entry point) against the fake MySQL and asserts the emitted ALTERs by shape
// for the flavor × size matrix. Three BTREE indexes make combined (1 ALTER,
// commas) and per-index (3 ALTERs, no commas) unambiguously distinguishable.
func TestBuildTableIndexes_SplitEngagesEndToEnd(t *testing.T) {
	const thr = indexSplitPerIndexTableBytes
	schema := func() *ir.Schema {
		return fbSchemaOneTable(fbBTreeIdx("idx_a", "id"), fbBTreeIdx("idx_b", "val"), fbBTreeIdx("idx_c", "body"))
	}
	cases := []struct {
		name          string
		flavor        Flavor
		bytes         int64
		wantAlterN    int  // number of ALTER statements executed
		wantCommaJoin bool // true => a single combined ALTER (contains ", ADD")
	}{
		{"planetscale large splits per-index", FlavorPlanetScale, thr, 3, false},
		{"vitess large splits per-index", FlavorVitess, thr * 2, 3, false},
		{"planetscale small stays combined", FlavorPlanetScale, thr - 1, 1, true},
		{"vanilla large stays combined", FlavorVanilla, thr * 4, 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &fbRecorder{dataLength: map[string]int64{"orders": c.bytes}}
			w := newSplitWriter(t, rec, c.flavor)
			if err := w.CreateIndexes(context.Background(), schema()); err != nil {
				t.Fatalf("CreateIndexes = %v; want nil", err)
			}
			got := execsOf(rec)
			if len(got) != c.wantAlterN {
				t.Fatalf("executed %d ALTER(s) %q; want %d", len(got), got, c.wantAlterN)
			}
			for _, s := range got {
				hasComma := strings.Contains(s, ", ADD")
				if hasComma != c.wantCommaJoin {
					t.Errorf("statement comma-join = %v; want %v: %q", hasComma, c.wantCommaJoin, s)
				}
			}
			// Every index must appear exactly once regardless of shape.
			joined := strings.Join(got, "\n")
			for _, name := range []string{"idx_a", "idx_b", "idx_c"} {
				if strings.Count(joined, "`"+name+"`") != 1 {
					t.Errorf("index %q not built exactly once: %q", name, got)
				}
			}
		})
	}
}

// TestBuildTableIndexes_SplitResumeSkipsPresent pins the resume property END
// TO END: on a large VStream table where two of three indexes already exist,
// the per-index split emits exactly ONE bounded ALTER — for the still-missing
// index — and never re-touches a present one. This is the "wall on index 3 of
// 4 resumes into just the remainder" behaviour the ADR calls for, proven
// through the real detect-then-skip probe + emitIndexBuildStatements.
func TestBuildTableIndexes_SplitResumeSkipsPresent(t *testing.T) {
	rec := &fbRecorder{
		dataLength:  map[string]int64{"orders": indexSplitPerIndexTableBytes * 2},
		preExisting: map[string]bool{"idx_a": true, "idx_b": true},
	}
	w := newSplitWriter(t, rec, FlavorPlanetScale)

	schema := fbSchemaOneTable(fbBTreeIdx("idx_a", "id"), fbBTreeIdx("idx_b", "val"), fbBTreeIdx("idx_c", "body"))
	if err := w.CreateIndexes(context.Background(), schema); err != nil {
		t.Fatalf("CreateIndexes = %v; want nil", err)
	}
	got := execsOf(rec)
	if len(got) != 1 {
		t.Fatalf("executed %d ALTER(s) %q; want exactly 1 (only the missing index)", len(got), got)
	}
	if !strings.Contains(got[0], "`idx_c`") {
		t.Errorf("resume ALTER should build the missing idx_c: %q", got[0])
	}
	if strings.Contains(got[0], "`idx_a`") || strings.Contains(got[0], "`idx_b`") {
		t.Errorf("resume re-touched an already-present index: %q", got[0])
	}
}

// TestBuildTableIndexes_SplitReachesVStreamSerialOverlap is the sibling-sweep
// pin: the split lives in the shared buildTableIndexes chokepoint, so the
// SECOND direct entry point — the VStream serial build-as-copied overlap
// (BuildTableIndexesFromChannel, the path every production PlanetScale-target
// migrate actually takes) — must split identically to the whole-schema
// CreateIndexes path. (The vanilla concurrent-overlap workers reach the same
// chokepoint but never split, being non-VStream, which is correct.)
func TestBuildTableIndexes_SplitReachesVStreamSerialOverlap(t *testing.T) {
	rec := &fbRecorder{dataLength: map[string]int64{"orders": indexSplitPerIndexTableBytes}}
	w := newSplitWriter(t, rec, FlavorPlanetScale)

	schema := fbSchemaOneTable(fbBTreeIdx("idx_a", "id"), fbBTreeIdx("idx_b", "val"))
	ch := make(chan *ir.Table, 1)
	ch <- schema.Tables[0]
	close(ch)
	if err := w.BuildTableIndexesFromChannel(context.Background(), schema, ch); err != nil {
		t.Fatalf("BuildTableIndexesFromChannel = %v; want nil", err)
	}
	got := execsOf(rec)
	if len(got) != 2 {
		t.Fatalf("VStream serial overlap executed %d ALTER(s) %q; want 2 (per-index split)", len(got), got)
	}
	for _, s := range got {
		if strings.Contains(s, ", ADD") {
			t.Errorf("VStream serial overlap emitted a combined ALTER, split did not reach it: %q", s)
		}
	}
}

// equalStmts compares two statement slices order-sensitively.
func equalStmts(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
