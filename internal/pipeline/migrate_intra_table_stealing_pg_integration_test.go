//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for ADR-0123: budget-wide intra-table PK-range work-stealing
// on the PG→PG `migrate` parallel copy.
//
// The MEASURED problem (ADR-0123 Context): a skewed corpus (one big table +
// several small) with --table-parallelism 4 --bulk-parallelism 4 (budget 16)
// drained the small tables in ~1.5 s, then copied the big table pinned at 4
// readers (= --bulk-parallelism) for its whole duration while 12 of 16 slots
// idled — because the big table split into exactly --bulk-parallelism chunks
// behind a per-table fixed-width gate that could not expand into the freed
// budget.
//
// This test reproduces the corpus and asserts the fix: a chunk-lifecycle
// dispatch observer records the PEAK concurrent chunk reads of the big table
// and confirms it EXPANDS WELL PAST --bulk-parallelism toward the full budget
// at the tail (not pinned at 4), while never exceeding the budget — the proof
// the taper is gone — plus exactly-once (src==dst exact count + an
// order-independent content checksum: no missing/duplicated/corrupted rows).

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// chunkConcurrencyMeter tracks, per table, the number of concurrently-active
// within-table chunk copies and the peak reached, plus the global peak across
// all tables. It is driven by [copyChunkLifecycleObserver].
type chunkConcurrencyMeter struct {
	mu         sync.Mutex
	active     map[string]int
	peak       map[string]int
	globalNow  int
	globalPeak int
}

func newChunkConcurrencyMeter() *chunkConcurrencyMeter {
	return &chunkConcurrencyMeter{active: map[string]int{}, peak: map[string]int{}}
}

func (m *chunkConcurrencyMeter) observe(table string, _ int, start bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if start {
		m.active[table]++
		m.globalNow++
		if m.active[table] > m.peak[table] {
			m.peak[table] = m.active[table]
		}
		if m.globalNow > m.globalPeak {
			m.globalPeak = m.globalNow
		}
		return
	}
	m.active[table]--
	m.globalNow--
}

func (m *chunkConcurrencyMeter) peakFor(table string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peak[table]
}

func (m *chunkConcurrencyMeter) globalPeakValue() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.globalPeak
}

// TestMigrate_PG_IntraTableStealing_TailReclaim is the headline ADR-0123 proof.
func TestMigrate_PG_IntraTableStealing_TailReclaim(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const (
		bigRows       = 800_000
		smallRows     = 1_000
		smallTables   = 6
		bulkP         = 4
		tableP        = 4
		budget        = bulkP * tableP    // 16
		minRows       = 50_000            // big (800k) chunks; smalls (1k) stay whole
		expectMChunks = bigRows / minRows // ceil(800k/50k)=16 == budget
	)

	// One dominant integer-PK table + several tiny ones (the skewed corpus).
	applyPGDDL(t, sourceDSN, fmt.Sprintf(`
		CREATE TABLE big (
			id      BIGINT PRIMARY KEY,
			payload TEXT NOT NULL
		);
		INSERT INTO big (id, payload)
			SELECT g, 'p-' || g FROM generate_series(1, %d) AS g;
		ANALYZE big;
	`, bigRows))
	for i := 0; i < smallTables; i++ {
		applyPGDDL(t, sourceDSN, fmt.Sprintf(`
			CREATE TABLE small_%d (
				id    BIGINT PRIMARY KEY,
				label TEXT NOT NULL
			);
			INSERT INTO small_%d (id, label)
				SELECT g, 'l-' || g FROM generate_series(1, %d) AS g;
			ANALYZE small_%d;
		`, i, i, smallRows, i))
	}

	// Install the chunk-lifecycle meter for the duration of this test.
	meter := newChunkConcurrencyMeter()
	copyChunkLifecycleObserver = meter.observe
	t.Cleanup(func() { copyChunkLifecycleObserver = nil })

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source:               pgEng,
		Target:               pgEng,
		SourceDSN:            sourceDSN,
		TargetDSN:            targetDSN,
		BulkParallelism:      bulkP,
		TableParallelism:     tableP,
		BulkParallelMinRows:  minRows,
		MaxTargetConnections: budget, // pin the product budget to exactly 16
		MigrationID:          "test-adr0123-tail-reclaim",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Exactly-once on the big table (the chunked one) AND a sample small table.
	assertCountAndChecksum(t, sourceDSN, targetDSN, "big", "id::text || '|' || payload")
	assertCountAndChecksum(t, sourceDSN, targetDSN, "small_0", "id::text || '|' || label")

	peakBig := meter.peakFor("big")
	globalPeak := meter.globalPeakValue()
	t.Logf("ADR-0123 tail-reclaim: big-table peak concurrent chunk reads = %d (bulk-parallelism=%d, budget=%d, M=%d); global chunk peak = %d",
		peakBig, bulkP, budget, expectMChunks, globalPeak)

	// THE PROOF the taper is gone: the big table expanded BEYOND the old fixed
	// --bulk-parallelism width. Pre-ADR-0123 this was pinned at exactly bulkP.
	if peakBig <= bulkP {
		t.Fatalf("big-table peak concurrency = %d; want > --bulk-parallelism (%d) — the tail taper is NOT fixed (still pinned at bulk-parallelism)", peakBig, bulkP)
	}
	// Real reclaim toward the full budget (the big table runs alone at the tail
	// with all %d tokens free + %d chunks to claim). Half-budget is a safe,
	// non-flaky floor; the log shows the actual (typically == budget).
	if peakBig < budget/2 {
		t.Fatalf("big-table peak concurrency = %d; want >= %d (real reclaim toward the full budget=%d), not just barely above bulk-parallelism", peakBig, budget/2, budget)
	}
	// Budget is REDISTRIBUTED, never INFLATED: a single table's chunk fan-out
	// (chunk 0 base + chunks) never exceeds the budget.
	if peakBig > budget {
		t.Fatalf("big-table peak concurrency = %d EXCEEDS the budget %d — work-stealing inflated the budget instead of redistributing it", peakBig, budget)
	}
	if globalPeak > budget {
		t.Fatalf("global chunk peak %d EXCEEDS the budget %d", globalPeak, budget)
	}
}
