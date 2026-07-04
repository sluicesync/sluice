//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the P-1 batched change-log prune against a real PG.
// The batching-loop shape (keyset steps, budget early-exit, ctx checks) is
// unit-pinned in prune_test.go; what needs a live server is the bounded
// `DELETE ... WHERE id > $1 AND id <= $2` statement itself at a
// larger-than-one-batch backlog, and the auto-prune sidecar path's pooled
// connection reuse + never-above-the-frontier invariant.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// seedSyntheticChangeLog inserts n synthetic change-log rows (BIGSERIAL ids
// 1..n) directly — generate_series is far faster than driving n trigger
// captures, and the prune path only cares about ids.
func seedSyntheticChangeLog(t *testing.T, dsn string, n int) {
	t.Helper()
	applyPGSQL(t, dsn, fmt.Sprintf(
		`INSERT INTO public.%s (txid, schema_name, table_name, op, pk_jsonb)
		 SELECT 0, 'public', 't', 'I', '{}'::jsonb FROM generate_series(1, %d)`,
		ChangeLogTable, n,
	))
}

// pgChangeLogIDs returns the sorted ids still in the change-log.
func pgChangeLogIDs(t *testing.T, dsn string) []int64 {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id FROM public."+ChangeLogTable+" ORDER BY id")
	if err != nil {
		t.Fatalf("query ids: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return ids
}

// setupTriggerSource creates a minimal user table and installs the trigger
// engine's source-side artifacts (incl. the change-log) on a fresh container.
func setupTriggerSource(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	dsn, cleanup = startPGForTrigger(t)
	applyPGSQL(t, dsn, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"t"}}); err != nil {
		cleanup()
		t.Fatalf("Setup: %v", err)
	}
	return dsn, cleanup
}

// TestPrune_BatchedLargeBacklog_PG runs the operator-facing [Prune] over a
// backlog larger than 2×pgPruneBatchSize, so the production batch size (not a
// test-shrunk step) provably multi-batches to completion on a real PG with the
// exact same outcome the old monolithic DELETE gave.
func TestPrune_BatchedLargeBacklog_PG(t *testing.T) {
	dsn, cleanup := setupTriggerSource(t)
	defer cleanup()

	const (
		total = 2*pgPruneBatchSize + 5_000 // 45k rows
		cut   = 2*pgPruneBatchSize + 1_000 // 41k — forces 3 batches (20k, 20k, 1k)
	)
	seedSyntheticChangeLog(t, dsn, total)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := Prune(ctx, dsn, PruneOptions{Cut: cut})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.Deleted != cut {
		t.Errorf("Deleted = %d; want %d", res.Deleted, cut)
	}
	if res.RemainingMin != cut+1 {
		t.Errorf("RemainingMin = %d; want %d", res.RemainingMin, cut+1)
	}
	if res.Remaining != total-cut {
		t.Errorf("Remaining = %d; want %d", res.Remaining, total-cut)
	}
}

// TestPruneConsumedChangeLog_PooledTicksAndFrontierInvariant drives the
// ADR-0137 Phase-B sidecar entry point across two ticks on a real PG and pins:
// (1) the prune pool is opened ONCE and reused (the P-1 fix for
// dial+ping-per-tick), (2) rows above the durable frontier are NEVER deleted no
// matter how the batching steps, (3) the remaining-rows estimate tracks the
// true count, and (4) Close releases the pool.
func TestPruneConsumedChangeLog_PooledTicksAndFrontierInvariant(t *testing.T) {
	dsn, cleanup := setupTriggerSource(t)
	defer cleanup()
	seedSyntheticChangeLog(t, dsn, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := openCDCReader(ctx, dsn, "")
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	cr := r.(*CDCReader)
	defer func() { _ = cr.Close() }()

	// Tick 1: frontier last_id=8, keep=3 ⇒ cut=5 ⇒ delete 1..5.
	deleted, err := cr.PruneConsumedChangeLog(ctx, `{"last_id":8}`, 3)
	if err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if deleted != 5 {
		t.Errorf("tick 1 deleted = %d; want 5", deleted)
	}
	cr.pruneMu.Lock()
	pool := cr.pruneDB
	cr.pruneMu.Unlock()
	if pool == nil {
		t.Fatal("tick 1 did not open the prune pool")
	}

	// Tick 2: frontier last_id=8, keep=0 ⇒ cut=8 ⇒ delete 6..8; ids 9,10
	// (> frontier) MUST survive — they are not yet durably applied.
	deleted, err = cr.PruneConsumedChangeLog(ctx, `{"last_id":8}`, 0)
	if err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if deleted != 3 {
		t.Errorf("tick 2 deleted = %d; want 3", deleted)
	}
	cr.pruneMu.Lock()
	reused := cr.pruneDB == pool
	cr.pruneMu.Unlock()
	if !reused {
		t.Error("tick 2 opened a NEW prune pool; want the tick-1 pool reused (P-1)")
	}

	if ids := pgChangeLogIDs(t, dsn); len(ids) != 2 || ids[0] != 9 || ids[1] != 10 {
		t.Errorf("remaining ids = %v; want [9 10] — rows above the durable frontier must never be pruned", ids)
	}
	// Tick 1 was the recount tick (anchored at 5 remaining); tick 2's
	// arithmetic subtracts its 3 deletions.
	if cr.pruneBook.remaining != 2 {
		t.Errorf("remaining estimate = %d; want 2 (matches the true count)", cr.pruneBook.remaining)
	}

	if err := cr.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	cr.pruneMu.Lock()
	released := cr.pruneDB == nil
	cr.pruneMu.Unlock()
	if !released {
		t.Error("Close did not release the prune pool")
	}
}
