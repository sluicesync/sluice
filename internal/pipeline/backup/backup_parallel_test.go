// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for ADR-0149 within-table backup read chunking: the mode
// gate, every per-table fallback reason (single-stream routing), the
// table-first budget split, the shared chunk-index allocator's
// collision-freedom + row-count summation under concurrent flushes,
// and the committer's concurrent same-entry appends. CI runs these
// under -race; locally (CGO=0 Windows) they pin shape only.

package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestBackupWithinChunkingEligible(t *testing.T) {
	cases := []struct {
		name       string
		snap       *irbackup.Snapshot
		source     ir.Engine
		wantOK     bool
		wantReason string
	}{
		{
			name:       "nil snapshot (v0.17.x fallback)",
			snap:       nil,
			source:     importerStubEngine{},
			wantReason: "not shareable",
		},
		{
			name:       "empty snapshot name (per-session)",
			snap:       &irbackup.Snapshot{},
			source:     importerStubEngine{},
			wantReason: "not shareable",
		},
		{
			name:       "eager coordinated readers (MySQL FTWRL) — fixed supply",
			snap:       &irbackup.Snapshot{ExtraReaders: []ir.RowReader{&backupPoolFakeReader{}}},
			source:     stubEngine{},
			wantReason: "fixed supply",
		},
		{
			name:       "shareable snapshot but no importer",
			snap:       &irbackup.Snapshot{SnapshotName: "00000003-0000001B-1"},
			source:     stubEngine{},
			wantReason: "no snapshot importer",
		},
		{
			name:   "eligible (PG lazy importer)",
			snap:   &irbackup.Snapshot{SnapshotName: "00000003-0000001B-1"},
			source: importerStubEngine{},
			wantOK: true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			ok, reason := backupWithinChunkingEligible(c.snap, c.source)
			if ok != c.wantOK {
				t.Errorf("ok = %v; want %v (reason %q)", ok, c.wantOK, reason)
			}
			if !strings.Contains(reason, c.wantReason) {
				t.Errorf("reason = %q; want contains %q", reason, c.wantReason)
			}
		})
	}
}

// ---- plan-stage fakes: each type implements exactly the surfaces its
// test case needs, so the presence-driven assertions in
// planBackupTableChunks can be exercised both ways. ----

// planReaderPlain implements only [ir.RowReader].
type planReaderPlain struct{}

func (planReaderPlain) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	panic("planReader.ReadRows called — plan tests are pre-stream only")
}
func (planReaderPlain) Err() error { return nil }

// planReaderBatched adds [ir.BatchedRowReader] (but no boundary surfaces).
type planReaderBatched struct{ planReaderPlain }

func (planReaderBatched) ReadRowsBatch(context.Context, *ir.Table, []any, int) (<-chan ir.Row, error) {
	panic("planReader.ReadRowsBatch called — plan tests are pre-stream only")
}

// planReaderIntPK adds the MIN/MAX-divide surfaces: RangeBoundsQuerier +
// RowCountEstimator.
type planReaderIntPK struct {
	planReaderBatched

	est        int64
	estErr     error
	minV, maxV any
}

func (r *planReaderIntPK) RangeBounds(context.Context, *ir.Table, string) (minVal, maxVal any, err error) {
	return r.minV, r.maxV, nil
}

func (r *planReaderIntPK) EstimateRowCount(context.Context, *ir.Table) (int64, error) {
	return r.est, r.estErr
}

// planReaderDisqualifying is an int-PK-capable reader that vetoes cursor
// reads (the SQLite temporal/decimal-PK shape).
type planReaderDisqualifying struct{ planReaderIntPK }

func (planReaderDisqualifying) DisqualifiesBatchedRead(*ir.Table) (disqualified bool, reason string) {
	return true, "decoded PK value cannot round-trip a cursor"
}

// planReaderKeysetNoBound has the keyset sampler but NOT
// BoundedBatchedRowReader — the ADR-0096 collation-clip refusal shape.
type planReaderKeysetNoBound struct {
	planReaderBatched

	est        int64
	boundaries [][]any
}

func (r *planReaderKeysetNoBound) SampleKeysetBoundaries(context.Context, *ir.Table, []string, int) ([][]any, error) {
	return r.boundaries, nil
}

func (r *planReaderKeysetNoBound) EstimateRowCount(context.Context, *ir.Table) (int64, error) {
	return r.est, nil
}

// planReaderKeyset is the fully-capable keyset reader.
type planReaderKeyset struct{ planReaderKeysetNoBound }

func (planReaderKeyset) ReadRowsBatchBounded(context.Context, *ir.Table, []any, []any, int) (<-chan ir.Row, error) {
	panic("planReader.ReadRowsBatchBounded called — plan tests are pre-stream only")
}

func planIntPKTable() *ir.Table {
	return &ir.Table{
		Name:       "big",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func planTextPKTable() *ir.Table {
	return &ir.Table{
		Name:       "big_text",
		Columns:    []*ir.Column{{Name: "code", Type: ir.Text{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "code"}}},
	}
}

// TestPlanBackupTableChunks_FallbackReasons pins every single-stream
// routing clause of the ADR-0149 per-table gate — each fallback
// condition must produce ≤1 bounds with its named reason, never an
// error (chunking is a perf detail; the data path is load-bearing).
func TestPlanBackupTableChunks_FallbackReasons(t *testing.T) {
	b := &Backup{}
	within := &backupWithinTable{parallelism: 4, minRows: 100, readBudget: 8}
	noPK := planIntPKTable()
	noPK.PrimaryKey = nil

	cases := []struct {
		name       string
		rr         ir.RowReader
		table      *ir.Table
		within     *backupWithinTable
		wantReason string
	}{
		{
			name:       "within-table chunking not engaged (nil config)",
			rr:         &planReaderIntPK{est: 1_000},
			table:      planIntPKTable(),
			within:     nil,
			wantReason: "not engaged",
		},
		{
			name:       "no primary key",
			rr:         &planReaderIntPK{est: 1_000},
			table:      noPK,
			within:     within,
			wantReason: "no primary key",
		},
		{
			name:       "reader lacks BatchedRowReader",
			rr:         planReaderPlain{},
			table:      planIntPKTable(),
			within:     within,
			wantReason: "BatchedRowReader",
		},
		{
			name:       "int PK, reader lacks RangeBoundsQuerier",
			rr:         planReaderBatched{},
			table:      planIntPKTable(),
			within:     within,
			wantReason: "RangeBoundsQuerier",
		},
		{
			name:       "text PK, reader lacks KeysetSampler",
			rr:         planReaderBatched{},
			table:      planTextPKTable(),
			within:     within,
			wantReason: "KeysetSampler",
		},
		{
			name:       "text PK, keyset sampler without BoundedBatchedRowReader",
			rr:         &planReaderKeysetNoBound{est: 1_000},
			table:      planTextPKTable(),
			within:     within,
			wantReason: "BoundedBatchedRowReader",
		},
		{
			name:       "reader disqualifies cursor reads",
			rr:         &planReaderDisqualifying{planReaderIntPK{est: 1_000}},
			table:      planIntPKTable(),
			within:     within,
			wantReason: "disqualifies cursor reads",
		},
		{
			name:       "estimate below threshold",
			rr:         &planReaderIntPK{est: 99},
			table:      planIntPKTable(),
			within:     within,
			wantReason: "below --bulk-parallel-min-rows",
		},
		{
			name:       "row-count probe failed",
			rr:         &planReaderIntPK{estErr: errors.New("boom")},
			table:      planIntPKTable(),
			within:     within,
			wantReason: "row-count probe failed",
		},
		{
			name:       "single-row range collapses to one chunk",
			rr:         &planReaderIntPK{est: 1_000, minV: int64(7), maxV: int64(7)},
			table:      planIntPKTable(),
			within:     within,
			wantReason: "single range",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			bounds, reason, err := b.planBackupTableChunks(context.Background(), c.rr, c.table, c.within)
			if err != nil {
				t.Fatalf("planBackupTableChunks: %v", err)
			}
			if len(bounds) > 1 {
				t.Fatalf("bounds = %d; want <= 1 (single-stream fallback)", len(bounds))
			}
			if !strings.Contains(reason, c.wantReason) {
				t.Errorf("reason = %q; want contains %q", reason, c.wantReason)
			}
		})
	}
}

// TestPlanBackupTableChunks_Engages pins the happy paths: both
// strategies produce >1 disjoint bounds when every gate condition
// holds, and the chunk count follows the shared
// [migcore.ClampParallelChunkCount] semantics (est/threshold ceiling, floored
// at parallelism, capped at the read budget).
func TestPlanBackupTableChunks_Engages(t *testing.T) {
	b := &Backup{}
	within := &backupWithinTable{parallelism: 4, minRows: 100, readBudget: 8}

	t.Run("int PK via MIN/MAX divide", func(t *testing.T) {
		rr := &planReaderIntPK{est: 350, minV: int64(1), maxV: int64(1_000)}
		bounds, reason, err := b.planBackupTableChunks(context.Background(), rr, planIntPKTable(), within)
		if err != nil {
			t.Fatalf("planBackupTableChunks: %v", err)
		}
		// clamp(ceil(350/100)=4, floor 4, cap max(64, 8)) = 4.
		if len(bounds) != 4 || reason != "" {
			t.Fatalf("bounds = %d (reason %q); want 4 chunked ranges", len(bounds), reason)
		}
		if bounds[0].LowerPK != nil || bounds[len(bounds)-1].UpperPK != nil {
			t.Errorf("outer bounds must be nil-unbounded: first.lower=%v last.upper=%v",
				bounds[0].LowerPK, bounds[len(bounds)-1].UpperPK)
		}
	})

	t.Run("text PK via sampled keyset", func(t *testing.T) {
		rr := &planReaderKeyset{planReaderKeysetNoBound{
			est:        500,
			boundaries: [][]any{{"g"}, {"n"}, {"t"}},
		}}
		bounds, reason, err := b.planBackupTableChunks(context.Background(), rr, planTextPKTable(), within)
		if err != nil {
			t.Fatalf("planBackupTableChunks: %v", err)
		}
		if len(bounds) != 4 || reason != "" {
			t.Fatalf("bounds = %d (reason %q); want 4 (3 interior boundaries)", len(bounds), reason)
		}
	})
}

// TestResolveBackupReadParallelism_TableFirstSplit pins the ADR-0149
// budget split: the cross-table axis is satisfied FIRST (its resolved
// value is byte-identical to the pre-ADR behaviour), the within axis
// gets whole multiples of the remainder, one slot stays reserved for
// the coordinator conn, and the product never exceeds the reserved
// budget. The single-huge-table case gets the full remaining width.
func TestResolveBackupReadParallelism_TableFirstSplit(t *testing.T) {
	cases := []struct {
		name       string
		copyBudget int
		tables     int
		reqTable   int // Backup.TableParallelism (0 = auto)
		reqWithin  int // Backup.BulkParallelism (0 = auto)
		wantTable  int
		wantWithin int
	}{
		{
			// Budget 10 → 9 after the coordinator reservation. Table axis
			// keeps its auto 4 (pre-ADR value); within gets 9/4 = 2.
			name:       "many tables split",
			copyBudget: 10, tables: 8, reqWithin: 8,
			wantTable: 4, wantWithin: 2,
		},
		{
			// ONE huge table: tableP clamps to 1, within gets the whole
			// reserved budget (9), clamped by the request (8).
			name:       "single huge table gets full width",
			copyBudget: 10, tables: 1, reqWithin: 8,
			wantTable: 1, wantWithin: 8,
		},
		{
			// Tight budget: table axis is clamped first (4→2), within
			// floors at 1 — the product (2) fits the reserved budget (2).
			name:       "tight budget clamps table first",
			copyBudget: 3, tables: 8, reqWithin: 8,
			wantTable: 2, wantWithin: 1,
		},
		{
			// --bulk-parallelism=1 keeps the pre-ADR resolution exactly.
			name:       "within disabled",
			copyBudget: 10, tables: 8, reqWithin: 1,
			wantTable: 4, wantWithin: 1,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			eng := &budgetProberEngine{report: ir.ConnectionBudget{
				EffectiveParallelism: min(migcore.ResolveTableParallelism(c.reqTable), min(c.tables, c.copyBudget)),
				CopyBudget:           c.copyBudget,
			}}
			b := &Backup{
				Source: eng, SourceDSN: "dsn", Store: &blobcodec.LocalStore{},
				TableParallelism: c.reqTable,
				BulkParallelism:  c.reqWithin,
			}
			tableP, withinP, err := b.resolveBackupReadParallelism(context.Background(), c.tables)
			if err != nil {
				t.Fatalf("resolveBackupReadParallelism: %v", err)
			}
			if tableP != c.wantTable || withinP != c.wantWithin {
				t.Errorf("(tableP, withinP) = (%d, %d); want (%d, %d)", tableP, withinP, c.wantTable, c.wantWithin)
			}
			if reserved := c.copyBudget - 1; reserved >= 1 && tableP*withinP > reserved {
				t.Errorf("product %d exceeds reserved budget %d", tableP*withinP, reserved)
			}
		})
	}
}

// TestBackupChunkStreamer_ConcurrentIndexAllocation drives N streamers
// (one per simulated range worker) over ONE shared (chunkIdx, rowsTotal)
// pair, flushing concurrently, and pins the ADR-0149 allocation
// contract: every chunk file path is unique, the index sequence is
// gapless, the manifest entry records every flush exactly once, and the
// row-count sum is exact.
func TestBackupChunkStreamer_ConcurrentIndexAllocation(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	b := &Backup{Source: stubEngine{}, SourceDSN: "dsn", Store: store}
	manifest := &irbackup.Manifest{PartialState: irbackup.BackupStateInProgress, Kind: irbackup.BackupKindFull}
	committer := &manifestCommitter{store: store, manifest: manifest}
	table := planIntPKTable()
	entry := &irbackup.TableManifest{Name: table.Name, Partial: true}
	committer.stageTable(entry)

	const (
		workers      = 8
		rowsPer      = 10
		chunkRows    = 3 // 10 rows → 4 flushes per worker (3+3+3+1)
		wantChunks   = workers * 4
		wantRowTotal = workers * rowsPer
	)
	var (
		chunkIdx  atomic.Int64
		rowsTotal atomic.Int64
		wg        sync.WaitGroup
		errMu     sync.Mutex
		firstErr  error
	)
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := b.newBackupChunkStreamer(table, entry, chunkRows, committer, nil, &chunkIdx, &rowsTotal)
			for i := 0; i < rowsPer; i++ {
				if err := s.writeRow(context.Background(), ir.Row{"id": int64(w*rowsPer + i)}); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
			}
			if err := s.flush(context.Background()); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("streamer error: %v", firstErr)
	}

	if got := rowsTotal.Load(); got != wantRowTotal {
		t.Errorf("rowsTotal = %d; want %d", got, wantRowTotal)
	}
	if len(entry.Chunks) != wantChunks {
		t.Fatalf("entry.Chunks = %d; want %d", len(entry.Chunks), wantChunks)
	}
	seen := map[string]bool{}
	var rowSum int64
	for _, ci := range entry.Chunks {
		if seen[ci.File] {
			t.Errorf("duplicate chunk path %q (index collision)", ci.File)
		}
		seen[ci.File] = true
		rowSum += ci.RowCount
	}
	if rowSum != wantRowTotal {
		t.Errorf("sum of per-chunk RowCount = %d; want %d", rowSum, wantRowTotal)
	}
	// Gapless allocation: every index 0..wantChunks-1 must be on disk.
	for i := 0; i < wantChunks; i++ {
		if !seen[chunkFilePath(table, i)] {
			t.Errorf("chunk index %d missing from manifest (allocation gap)", i)
		}
	}
}

// TestManifestCommitter_ConcurrentSameEntryAppends pins the ADR-0149
// committer contract the range workers rely on: concurrent appendChunk
// calls against the SAME table entry lose nothing (arrival order is
// unspecified; the SET is complete), and finishTable records the
// terminal count.
func TestManifestCommitter_ConcurrentSameEntryAppends(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	manifest := &irbackup.Manifest{PartialState: irbackup.BackupStateInProgress, Kind: irbackup.BackupKindFull}
	committer := &manifestCommitter{store: store, manifest: manifest}
	entry := &irbackup.TableManifest{Name: "t", Partial: true}
	committer.stageTable(entry)

	const appends = 64
	var wg sync.WaitGroup
	errs := make(chan error, appends)
	for i := 0; i < appends; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- committer.appendChunk(context.Background(), entry, &irbackup.ChunkInfo{
				File:     fmt.Sprintf("chunks/t/t-%d.jsonl.gz", i),
				RowCount: 1,
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("appendChunk: %v", err)
		}
	}
	if len(entry.Chunks) != appends {
		t.Fatalf("entry.Chunks = %d; want %d (lost concurrent appends)", len(entry.Chunks), appends)
	}
	files := map[string]bool{}
	for _, ci := range entry.Chunks {
		files[ci.File] = true
	}
	if len(files) != appends {
		t.Errorf("distinct chunk files = %d; want %d", len(files), appends)
	}
	if err := committer.finishTable(context.Background(), entry, appends); err != nil {
		t.Fatalf("finishTable: %v", err)
	}
	if entry.Partial || entry.RowCount != appends {
		t.Errorf("terminal entry = {Partial:%v RowCount:%d}; want {false %d}", entry.Partial, entry.RowCount, appends)
	}
}
