// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the within-table chunk fan-out (ADR-0112): chunk
// partitioning, the two-axis (table × chunk) budget resolution through
// the single connection-budget chokepoint, the chunk-axis dispatch
// observer, and the Run-level wiring (a multi-chunk table fanned across
// workers lands every row; a single-chunk table stays serial; a
// row-count mismatch still fails hard). CI runs these under -race;
// locally (CGO=0 Windows) they pin shape only.

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// observeRestoreChunkDispatch installs the test-only within-table-axis
// dispatch observer and returns pointers to the captured decision.
// Restores the seam via t.Cleanup. Tests using it must not run in
// t.Parallel (package precedent: restoreDispatchObserver).
func observeRestoreChunkDispatch(t *testing.T) (gotParallelism *int, gotReason *string) {
	t.Helper()
	p, r := 0, ""
	restoreChunkDispatchObserver = func(chunkParallelism int, reason string) {
		p, r = chunkParallelism, reason
	}
	t.Cleanup(func() { restoreChunkDispatchObserver = nil })
	return &p, &r
}

// chunkInfos builds a slice of n distinct ChunkInfo pointers (only the
// identity matters for partitioning tests).
func chunkInfos(n int) []*irbackup.ChunkInfo {
	cs := make([]*irbackup.ChunkInfo, n)
	for i := range cs {
		cs[i] = &irbackup.ChunkInfo{File: fmt.Sprintf("chunk-%02d", i), RowCount: int64(i + 1)}
	}
	return cs
}

// TestPartitionChunks pins the contiguous-disjoint partition contract
// across even/uneven counts, P > chunks, and P = 1: every chunk appears
// exactly once, in manifest order, with no empty groups, and the group
// sizes are near-even (the first len%P groups carry one extra).
func TestPartitionChunks(t *testing.T) {
	cases := []struct {
		name      string
		chunks, p int
		wantSizes []int
	}{
		{"P=1 single group", 5, 1, []int{5}},
		{"P=0 clamps to 1", 5, 0, []int{5}},
		{"even split", 6, 3, []int{2, 2, 2}},
		{"uneven split front-loaded", 7, 3, []int{3, 2, 2}},
		{"P equals chunks", 4, 4, []int{1, 1, 1, 1}},
		{"P exceeds chunks clamps", 3, 8, []int{1, 1, 1}},
		{"two chunks two workers", 2, 2, []int{1, 1}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			in := chunkInfos(c.chunks)
			groups := partitionChunks(in, c.p)
			if len(groups) != len(c.wantSizes) {
				t.Fatalf("group count = %d; want %d", len(groups), len(c.wantSizes))
			}
			// Sizes match and the concatenation reproduces the input in
			// order (disjoint + complete + ordered).
			var flat []*irbackup.ChunkInfo
			for i, g := range groups {
				if len(g) != c.wantSizes[i] {
					t.Errorf("group %d size = %d; want %d", i, len(g), c.wantSizes[i])
				}
				if len(g) == 0 {
					t.Errorf("group %d is empty; partition must never produce empty groups", i)
				}
				flat = append(flat, g...)
			}
			if len(flat) != len(in) {
				t.Fatalf("flattened %d chunks; want %d (disjoint+complete)", len(flat), len(in))
			}
			for i := range in {
				if flat[i] != in[i] {
					t.Errorf("flattened[%d] = %s; want %s (order preserved)", i, flat[i].File, in[i].File)
				}
			}
		})
	}
}

// TestResolveRestoreParallelism_TwoAxisBudget pins the load-bearing
// invariant: BOTH axes resolve through the single connection-budget
// chokepoint and their PRODUCT never exceeds the measured CopyBudget,
// with the within-table axis satisfied first and the table axis taking
// the remainder (mirroring migrate's split). MySQL (no prober) passes
// both axes through unclamped.
func TestResolveRestoreParallelism_TwoAxisBudget(t *testing.T) {
	t.Run("product capped to copy budget", func(t *testing.T) {
		// CopyBudget=6: within wants 4 (capped to 4 by the prober's
		// EffectiveParallelism), table wants 8 → table clamped to
		// floor(6/4)=1 so 1×4=4 <= 6. The prober caps the within request
		// to EffectiveParallelism first.
		eng := &budgetProberEngine{report: ir.ConnectionBudget{
			EffectiveParallelism: 4,
			CopyBudget:           6,
		}}
		r := &Restore{Target: eng, TargetDSN: "dsn", Store: &LocalStore{}, TableParallelism: 8, ChunkParallelism: 4}
		gotTableP, gotReason := observeRestoreDispatch(t)
		gotChunkP, _ := observeRestoreChunkDispatch(t)
		table, chunk, err := r.resolveRestoreParallelism(context.Background(), 12)
		if err != nil {
			t.Fatalf("resolveRestoreParallelism: %v", err)
		}
		if chunk != 4 {
			t.Errorf("chunk parallelism = %d (observer %d); want 4 (within satisfied first)", chunk, *gotChunkP)
		}
		if table != 1 {
			t.Errorf("table parallelism = %d; want 1 (remainder after within)", table)
		}
		if table*chunk > 6 {
			t.Errorf("product %d×%d = %d exceeds CopyBudget 6", table, chunk, table*chunk)
		}
		// Observer agreement.
		if *gotTableP != table {
			t.Errorf("table observer = %d (reason %q); want %d", *gotTableP, *gotReason, table)
		}
	})

	t.Run("budget room for both axes", func(t *testing.T) {
		// CopyBudget=12, within capped to 3, table wants 4 → table
		// clamped to floor(12/3)=4 so 4×3=12 <= 12.
		eng := &budgetProberEngine{report: ir.ConnectionBudget{
			EffectiveParallelism: 3,
			CopyBudget:           12,
		}}
		r := &Restore{Target: eng, TargetDSN: "dsn", Store: &LocalStore{}, TableParallelism: 4, ChunkParallelism: 3}
		table, chunk, err := r.resolveRestoreParallelism(context.Background(), 10)
		if err != nil {
			t.Fatalf("resolveRestoreParallelism: %v", err)
		}
		if table != 4 || chunk != 3 {
			t.Errorf("(table, chunk) = (%d, %d); want (4, 3)", table, chunk)
		}
		if table*chunk > 12 {
			t.Errorf("product = %d exceeds CopyBudget 12", table*chunk)
		}
	})

	t.Run("mysql no-prober passes both axes unclamped", func(t *testing.T) {
		r := &Restore{Target: noProberEngine{}, TargetDSN: "dsn", Store: &LocalStore{}, TableParallelism: 5, ChunkParallelism: 7}
		table, chunk, err := r.resolveRestoreParallelism(context.Background(), 10)
		if err != nil {
			t.Fatalf("resolveRestoreParallelism: %v", err)
		}
		if table != 5 {
			t.Errorf("table parallelism = %d; want 5 (no budget clamp)", table)
		}
		if chunk != 7 {
			t.Errorf("chunk parallelism = %d; want 7 (no budget clamp)", chunk)
		}
	})

	t.Run("bulk-parallelism=1 collapses within axis loudly", func(t *testing.T) {
		r := &Restore{Target: noProberEngine{}, TargetDSN: "dsn", Store: &LocalStore{}, TableParallelism: 4, ChunkParallelism: 1}
		gotChunkP, gotChunkReason := observeRestoreChunkDispatch(t)
		_, chunk, err := r.resolveRestoreParallelism(context.Background(), 10)
		if err != nil {
			t.Fatalf("resolveRestoreParallelism: %v", err)
		}
		if chunk != 1 || *gotChunkP != 1 {
			t.Errorf("chunk parallelism = %d (observer %d); want 1", chunk, *gotChunkP)
		}
		if !strings.Contains(*gotChunkReason, "--bulk-parallelism=1") {
			t.Errorf("chunk reason = %q; want contains \"--bulk-parallelism=1\"", *gotChunkReason)
		}
	})
}

// TestResolveTableChunkParallelism_PerTableEngagement pins the per-table
// engage decision: the within-table fan-out only engages for a table
// with >= 2 chunks; a single-chunk table collapses to serial with a
// loud INFO (named reason), even when the axis is globally eligible.
func TestResolveTableChunkParallelism_PerTableEngagement(t *testing.T) {
	r := &Restore{Target: noProberEngine{}, TargetDSN: "dsn", Store: &LocalStore{}}
	tbl := &ir.Table{Name: "t"}

	if got := r.resolveTableChunkParallelism(context.Background(), tbl, 5, 4); got != 4 {
		t.Errorf("5 chunks, P=4 → %d; want 4 (engaged)", got)
	}
	if got := r.resolveTableChunkParallelism(context.Background(), tbl, 3, 8); got != 3 {
		t.Errorf("3 chunks, P=8 → %d; want 3 (clamped to chunk count)", got)
	}
	if got := r.resolveTableChunkParallelism(context.Background(), tbl, 1, 4); got != 1 {
		t.Errorf("1 chunk, P=4 → %d; want 1 (serial — too few chunks)", got)
	}
	if got := r.resolveTableChunkParallelism(context.Background(), tbl, 9, 1); got != 1 {
		t.Errorf("9 chunks, P=1 → %d; want 1 (axis disabled)", got)
	}
}

// restoreChunkFixture backs up nTables tables of rowsPerTable rows each,
// chunked at chunkRows so every table has multiple chunks, and returns a
// Restore wired to the resulting store plus the source row set per table
// (for content comparison). The recorder source/target are deterministic
// so a parallel restore must reproduce the exact row set per table.
func restoreChunkFixture(t *testing.T, nTables, rowsPerTable, chunkRows int) (store irbackup.Store, want map[string][]ir.Row) {
	t.Helper()
	s, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{}
	rows := map[string][]ir.Row{}
	for ti := 0; ti < nTables; ti++ {
		name := fmt.Sprintf("c%02d", ti)
		schema.Tables = append(schema.Tables, &ir.Table{
			Name:    name,
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		})
		for ri := 0; ri < rowsPerTable; ri++ {
			rows[name] = append(rows[name], ir.Row{"id": int64(ti*1_000_000 + ri)})
		}
	}
	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: s, ChunkRows: chunkRows}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	return s, rows
}

// rowSetEqual compares two row slices as multisets keyed on the "id"
// column — order-independent (the parallel restore applies chunk groups
// concurrently, so the recorded order is not the source order).
func rowSetEqual(t *testing.T, table string, got, want []ir.Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("table %s: rows = %d; want %d", table, len(got), len(want))
		return
	}
	seen := make(map[int64]int, len(got))
	for _, r := range got {
		seen[r["id"].(int64)]++
	}
	for _, r := range want {
		id := r["id"].(int64)
		if seen[id] == 0 {
			t.Errorf("table %s: missing row id=%d", table, id)
			return
		}
		seen[id]--
	}
}

// TestRestore_WithinTableParallel_RoundTrip pins the Run-level within-
// table wiring: a multi-chunk table restored with ChunkParallelism>1
// engages the chunk fan-out (observer-asserted) and every row of every
// table arrives exactly once. TableParallelism=1 isolates the within-
// table axis from the cross-table one.
func TestRestore_WithinTableParallel_RoundTrip(t *testing.T) {
	// 2 tables × 50 rows, chunked at 10 → 5 chunks/table.
	store, want := restoreChunkFixture(t, 2, 50, 10)

	tgt := newRestoreRecorderEngine("postgres")
	gotChunkP, gotChunkReason := observeRestoreChunkDispatch(t)
	if err := (&Restore{
		Target:           tgt,
		TargetDSN:        "tgt",
		Store:            store,
		TableParallelism: 1, // isolate the within-table axis
		ChunkParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	if *gotChunkP <= 1 {
		t.Fatalf("chunk dispatch = serial (reason %q); want the within-table fan-out eligible", *gotChunkReason)
	}
	_, gotRows := tgt.snapshot()
	for name, w := range want {
		rowSetEqual(t, name, gotRows[name], w)
	}
}

// TestRestore_WithinTableParallel_SingleChunkStaysSerial pins that a
// table with one chunk stays single-stream even when ChunkParallelism>1
// — the fan-out engages only where it helps. The axis is globally
// eligible (observer fires >1), but the per-table decision collapses to
// one worker, and every row still arrives.
func TestRestore_WithinTableParallel_SingleChunkStaysSerial(t *testing.T) {
	// 3 tables × 5 rows, chunked at 100 → 1 chunk/table.
	store, want := restoreChunkFixture(t, 3, 5, 100)

	tgt := newRestoreRecorderEngine("postgres")
	gotChunkP, _ := observeRestoreChunkDispatch(t)
	if err := (&Restore{
		Target:           tgt,
		TargetDSN:        "tgt",
		Store:            store,
		TableParallelism: 1,
		ChunkParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	// The axis is eligible (a budget-less target), but each single-chunk
	// table collapses to one worker inside restoreTable — the rows still
	// arrive intact, which is the load-bearing property.
	if *gotChunkP <= 1 {
		t.Fatalf("axis observer = %d; want >1 (eligible; per-table engage gated on chunk count)", *gotChunkP)
	}
	_, gotRows := tgt.snapshot()
	for name, w := range want {
		rowSetEqual(t, name, gotRows[name], w)
	}
	// Exactly one WriteRows per single-chunk table (no fan-out workers).
	phases, _ := tgt.snapshot()
	for name := range want {
		var n int
		for _, p := range phases {
			if p == "WriteRows:"+name {
				n++
			}
		}
		if n != 1 {
			t.Errorf("table %s: WriteRows calls = %d; want 1 (single-chunk stays serial)", name, n)
		}
	}
}

// TestRestore_WithinTableParallel_RowCountMismatchFailsHard pins that
// the layer-2 row-count cross-check stays a HARD failure under the
// fan-out: when the manifest's table RowCount is corrupted to disagree
// with the chunks' actual total, the restore refuses loudly rather than
// silently landing a short table.
func TestRestore_WithinTableParallel_RowCountMismatchFailsHard(t *testing.T) {
	store, _ := restoreChunkFixture(t, 1, 40, 10) // 1 table, 4 chunks

	// Corrupt the manifest's table-level RowCount so Σ(decoded) != RowCount.
	manifest, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	manifest.Tables[0].RowCount = 9999 // lie about the row count
	if err := writeManifestAt(context.Background(), store, ManifestFileName, manifest); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}

	tgt := newRestoreRecorderEngine("postgres")
	err = (&Restore{
		Target:           tgt,
		TargetDSN:        "tgt",
		Store:            store,
		TableParallelism: 1,
		ChunkParallelism: 4,
	}).Run(context.Background())
	if err == nil {
		t.Fatal("Restore.Run returned nil; want a hard layer-2 row-count mismatch failure")
	}
	if !strings.Contains(err.Error(), "layer-2 row-count mismatch") {
		t.Errorf("err = %v; want a layer-2 row-count mismatch", err)
	}
}
