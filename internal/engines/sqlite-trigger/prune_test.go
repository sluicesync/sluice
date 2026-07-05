// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver — no cgo, no container needed

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
)

// These are real-SQLite-file UNIT tests: modernc.org/sqlite is pure Go, so a
// temp file gives the actual DELETE/VACUUM/stats path without Docker. The
// id <= cut boundary (the off-by-one that would either leak the frontier row or
// — worse — over-delete) is pinned here so it can't regress silently.

// seedChangeLog writes a temp SQLite file with a change-log table holding rows
// id=1..n and returns the file path.
func seedChangeLog(t *testing.T, n int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE "`+ChangeLogTable+`" (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, tbl TEXT)`); err != nil {
		t.Fatalf("create change-log: %v", err)
	}
	for i := int64(1); i <= n; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO "`+ChangeLogTable+`" (id, op, tbl) VALUES (?, 'I', 't')`, i); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	return path
}

// remainingIDs returns the sorted id set still in the change-log.
func remainingIDs(t *testing.T, path string) []int64 {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(), `SELECT id FROM "`+ChangeLogTable+`" ORDER BY id`)
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

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPrune_DeletesAtMostCut_Inclusive pins the load-bearing boundary: cut=5
// removes ids 1..5 and KEEPS 6..10 — `id <= cut`, not `id < cut` (which would
// leak id=5) and not `id < cut+1`-style over-deletes.
func TestPrune_DeletesAtMostCut_Inclusive(t *testing.T) {
	path := seedChangeLog(t, 10)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.Deleted != 5 {
		t.Errorf("Deleted = %d; want 5", res.Deleted)
	}
	if res.RemainingMin != 6 {
		t.Errorf("RemainingMin = %d; want 6", res.RemainingMin)
	}
	if res.Remaining != 5 {
		t.Errorf("Remaining = %d; want 5", res.Remaining)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{6, 7, 8, 9, 10}) {
		t.Errorf("remaining ids = %v; want [6 7 8 9 10] (id <= cut deletes through 5 inclusive)", got)
	}
}

// TestPrune_Idempotent re-runs the same cut and asserts nothing new is deleted.
func TestPrune_Idempotent(t *testing.T) {
	path := seedChangeLog(t, 10)
	if _, err := Prune(context.Background(), path, PruneOptions{Cut: 5}); err != nil {
		t.Fatalf("Prune 1: %v", err)
	}
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5})
	if err != nil {
		t.Fatalf("Prune 2: %v", err)
	}
	if res.Deleted != 0 {
		t.Errorf("second Prune Deleted = %d; want 0 (idempotent)", res.Deleted)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{6, 7, 8, 9, 10}) {
		t.Errorf("remaining ids after re-prune = %v; want [6 7 8 9 10]", got)
	}
}

// TestPrune_Vacuum exercises the --vacuum path (it must not error and must not
// change which rows remain).
func TestPrune_Vacuum(t *testing.T) {
	path := seedChangeLog(t, 10)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5, Vacuum: true})
	if err != nil {
		t.Fatalf("Prune with vacuum: %v", err)
	}
	if !res.Vacuumed {
		t.Error("Vacuumed = false; want true")
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{6, 7, 8, 9, 10}) {
		t.Errorf("remaining ids after vacuum = %v; want [6 7 8 9 10]", got)
	}
}

// TestPrune_DryRun asserts a dry-run deletes nothing and reports current stats.
func TestPrune_DryRun(t *testing.T) {
	path := seedChangeLog(t, 10)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: 5, DryRun: true})
	if err != nil {
		t.Fatalf("Prune dry-run: %v", err)
	}
	if res.Deleted != 0 {
		t.Errorf("dry-run Deleted = %d; want 0", res.Deleted)
	}
	if res.Remaining != 10 || res.RemainingMin != 1 {
		t.Errorf("dry-run stats = (min %d, count %d); want (1, 10)", res.RemainingMin, res.Remaining)
	}
	if got := remainingIDs(t, path); len(got) != 10 {
		t.Errorf("dry-run mutated the change-log: %v", got)
	}
}

// TestPrune_RefusesMissingChangeLog asserts a prune against a source without the
// change-log table refuses loudly (not a silent no-op).
func TestPrune_RefusesMissingChangeLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// Create some unrelated table so the file is a valid DB.
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create t: %v", err)
	}
	_ = db.Close()

	if _, err := Prune(context.Background(), path, PruneOptions{Cut: 5}); err == nil {
		t.Fatal("Prune against a source with no change-log returned nil; want a loud error")
	}
}

// TestPruneConsumedChangeLog_ComputesCutFromFrontier pins the ADR-0137 Phase-B
// auto-prune bound: PruneConsumedChangeLog derives cut = AppliedLastID(token) -
// keep and reaps id <= cut, keying off the durable frontier the sidecar passes
// in. A CDCReader with only its backend set is enough — the method opens its own
// writable executor. modernc.org/sqlite gives the real DELETE path with no
// container.
func TestPruneConsumedChangeLog_ComputesCutFromFrontier(t *testing.T) {
	path := seedChangeLog(t, 10)
	r := &CDCReader{b: localBackend(path)}

	// frontier last_id=8, keep=3 ⇒ cut=5 ⇒ delete 1..5, keep 6..10.
	deleted, err := r.PruneConsumedChangeLog(context.Background(), `{"last_id":8}`, 3)
	if err != nil {
		t.Fatalf("PruneConsumedChangeLog: %v", err)
	}
	if deleted != 5 {
		t.Errorf("deleted = %d; want 5 (cut = 8 - 3 = 5, inclusive)", deleted)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{6, 7, 8, 9, 10}) {
		t.Errorf("remaining = %v; want [6 7 8 9 10]", got)
	}
}

// TestPruneConsumedChangeLog_NeverAboveFrontier is the load-bearing silent-loss
// pin: even at keep=0 (cut == frontier), rows with id > frontier — which may be
// read but NOT yet durably applied — are NEVER deleted.
func TestPruneConsumedChangeLog_NeverAboveFrontier(t *testing.T) {
	path := seedChangeLog(t, 10)
	r := &CDCReader{b: localBackend(path)}

	// frontier last_id=8, keep=0 ⇒ cut=8 ⇒ delete 1..8; ids 9,10 (> frontier)
	// MUST survive — they are not yet durably applied.
	deleted, err := r.PruneConsumedChangeLog(context.Background(), `{"last_id":8}`, 0)
	if err != nil {
		t.Fatalf("PruneConsumedChangeLog: %v", err)
	}
	if deleted != 8 {
		t.Errorf("deleted = %d; want 8 (cut == frontier, inclusive)", deleted)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{9, 10}) {
		t.Errorf("remaining = %v; want [9 10] — rows above the durable frontier must never be pruned", got)
	}
}

// TestPruneConsumedChangeLog_NonPositiveCutIsNoOp asserts that when the margin
// exceeds the frontier (cut <= 0), nothing is deleted (a safe no-op, no error).
func TestPruneConsumedChangeLog_NonPositiveCutIsNoOp(t *testing.T) {
	path := seedChangeLog(t, 10)
	r := &CDCReader{b: localBackend(path)}

	deleted, err := r.PruneConsumedChangeLog(context.Background(), `{"last_id":2}`, 1000)
	if err != nil {
		t.Fatalf("PruneConsumedChangeLog: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d; want 0 (cut = 2 - 1000 <= 0 ⇒ no-op)", deleted)
	}
	if got := remainingIDs(t, path); len(got) != 10 {
		t.Errorf("remaining = %v; a non-positive cut must delete nothing", got)
	}
}

// TestPruneConsumedChangeLog_RefusesForeignToken asserts a non-trigger-CDC token
// is refused loudly via the shared AppliedLastID decode (never a blind prune
// against the wrong stream).
func TestPruneConsumedChangeLog_RefusesForeignToken(t *testing.T) {
	path := seedChangeLog(t, 10)
	r := &CDCReader{b: localBackend(path)}

	if _, err := r.PruneConsumedChangeLog(context.Background(), `{"slot":"s","lsn":"0/16B3748"}`, 0); err == nil {
		t.Error("PruneConsumedChangeLog(foreign token) returned nil; want a loud refuse")
	}
	if got := remainingIDs(t, path); len(got) != 10 {
		t.Errorf("remaining = %v; a refused prune must delete nothing", got)
	}
}

// --- P-1 batched-prune pins --------------------------------------------------
// The pure batching + bookkeeping pins (StepsBoundedKeyset, NothingBelowCut,
// BudgetStopsEarly, CtxCanceled, the Bookkeeper cadence) live in the shared
// internal/engines/internal/triggercdc package now. The pins BELOW are
// engine-specific: they drive the REAL bounded-DELETE SQL against a temp SQLite
// file (or the fake D1 executor) through [triggercdc.InBatches].

// recordedBatch is one (floor, upper] keyset step a prune DELETE stepped through
// — used by the SQLite + D1 prune pins to assert the batching bounds.
type recordedBatch struct{ floor, upper int64 }

// TestPruneInBatches_RealExecutorBudgetResumes drives the REAL bounded-DELETE
// SQL through a budget-exhausted first tick and a resuming second tick, and is
// the P-1 invariant pin at the SQL layer: rows above the cut (the durable
// frontier) survive EVERY intermediate batching state, and the resume — floor
// re-derived from MIN(id) — completes to exactly the cut, no further.
func TestPruneInBatches_RealExecutorBudgetResumes(t *testing.T) {
	path := seedChangeLog(t, 10)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()
	e := &localExecutor{db: db}
	ctx := context.Background()

	// Tick 1: cut=8 (frontier), step=3, 1ns budget; the slowed del exhausts the
	// budget after one batch → only (0,3] reaped.
	slowDel := func(ctx context.Context, floor, upper int64) (int64, error) {
		n, err := e.pruneChangeLogBatch(ctx, floor, upper)
		time.Sleep(2 * time.Millisecond)
		return n, err
	}
	deleted, done, err := triggercdc.InBatches(ctx, 1, 8, 3, time.Nanosecond, slowDel)
	if err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if done || deleted != 3 {
		t.Fatalf("tick 1: (deleted=%d, done=%v); want (3, false)", deleted, done)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{4, 5, 6, 7, 8, 9, 10}) {
		t.Fatalf("after tick 1 remaining = %v; want [4..10] (ids 9,10 above the frontier MUST survive)", got)
	}

	// Tick 2 (the resume): floor re-derives from MIN(id); no budget → completes.
	minID, err := e.minChangeLogID(ctx)
	if err != nil {
		t.Fatalf("minChangeLogID: %v", err)
	}
	if minID != 4 {
		t.Fatalf("minChangeLogID = %d; want 4", minID)
	}
	deleted, done, err = triggercdc.InBatches(ctx, minID, 8, 3, 0, e.pruneChangeLogBatch)
	if err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if !done || deleted != 5 {
		t.Fatalf("tick 2: (deleted=%d, done=%v); want (5, true)", deleted, done)
	}
	if got := remainingIDs(t, path); !equalIDs(got, []int64{9, 10}) {
		t.Errorf("after resume remaining = %v; want [9 10] — rows above the durable frontier must never be pruned", got)
	}
}

// seedChangeLogBulk seeds n change-log rows (id=1..n) in one recursive-CTE
// INSERT — fast enough to exercise the REAL localPruneBatchSize multi-batch
// path without 45k round-trips.
func seedChangeLogBulk(t *testing.T, n int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE "`+ChangeLogTable+`" (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, tbl TEXT)`); err != nil {
		t.Fatalf("create change-log: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < ?)
		 INSERT INTO "`+ChangeLogTable+`" (id, op, tbl) SELECT x, 'I', 't' FROM cnt`, n); err != nil {
		t.Fatalf("bulk seed: %v", err)
	}
	return path
}

// TestPrune_LargeBacklogBatchesToCompletion runs the operator-facing [Prune]
// over a backlog larger than 2×localPruneBatchSize, so the production batch
// size (not a test-shrunk step) provably multi-batches to completion with the
// exact same outcome the old monolithic DELETE gave.
func TestPrune_LargeBacklogBatchesToCompletion(t *testing.T) {
	const (
		total = 2*localPruneBatchSize + 5_000 // 45k rows
		cut   = 2*localPruneBatchSize + 1_000 // 41k — forces 3 batches (20k, 20k, 1k)
	)
	path := seedChangeLogBulk(t, total)
	res, err := Prune(context.Background(), path, PruneOptions{Cut: cut})
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
	got := remainingIDs(t, path)
	if len(got) != total-cut || got[0] != cut+1 || got[len(got)-1] != total {
		t.Errorf("remaining ids span [%d..%d] (n=%d); want [%d..%d] (n=%d)",
			got[0], got[len(got)-1], len(got), cut+1, total, total-cut)
	}
}

// TestPruneConsumedChangeLog_RemainingEstimateConsistent drives the sidecar
// entry point across ticks and pins that the arithmetic estimate matches the
// change-log's TRUE row count at every step, and that a forced recount tick
// re-anchors to the same truth (drift-free with no concurrent inserts).
func TestPruneConsumedChangeLog_RemainingEstimateConsistent(t *testing.T) {
	path := seedChangeLog(t, 100)
	r := &CDCReader{b: localBackend(path)}
	ctx := context.Background()

	// Tick 1 (recount tick): frontier 30, keep 0 ⇒ delete 1..30, anchor at 70.
	if _, err := r.PruneConsumedChangeLog(ctx, `{"last_id":30}`, 0); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	if !r.pruneBook.Anchored() || r.pruneBook.Remaining() != 70 {
		t.Fatalf("after tick 1 estimate = %+v; want anchored remaining=70", r.pruneBook)
	}

	// Tick 2 (arithmetic tick): frontier 50 ⇒ delete 31..50 ⇒ estimate 50.
	if _, err := r.PruneConsumedChangeLog(ctx, `{"last_id":50}`, 0); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if truth := int64(len(remainingIDs(t, path))); r.pruneBook.Remaining() != 50 || truth != 50 {
		t.Fatalf("after tick 2 estimate = %d, truth = %d; want both 50", r.pruneBook.Remaining(), truth)
	}

	// Force the next tick onto the recount cadence and assert the true COUNT
	// re-anchors to the same value the arithmetic would have reached.
	r.pruneBook.PrimeRecount()
	if _, err := r.PruneConsumedChangeLog(ctx, `{"last_id":60}`, 0); err != nil {
		t.Fatalf("tick 3: %v", err)
	}
	if truth := int64(len(remainingIDs(t, path))); r.pruneBook.Remaining() != 40 || truth != 40 {
		t.Errorf("after recount tick estimate = %d, truth = %d; want both 40", r.pruneBook.Remaining(), truth)
	}
}

// TestPollBatch_TransportCeilings pins the P-3 seam constants: the local
// transport declares NO poll ceiling (the reader keeps defaultBatchSize) while
// the D1 transport clamps to d1PollBatchSize (the reader-side wiring is pinned
// end-to-end in TestD1Poll_BatchClampedToTransportCeiling).
func TestPollBatch_TransportCeilings(t *testing.T) {
	if got := (&localExecutor{}).maxPollBatch(); got != 0 {
		t.Errorf("localExecutor.maxPollBatch() = %d; want 0 (no ceiling)", got)
	}
	if got := (&d1Executor{}).maxPollBatch(); got != d1PollBatchSize {
		t.Errorf("d1Executor.maxPollBatch() = %d; want %d", got, d1PollBatchSize)
	}
}

// TestAppliedLastID covers the token decode used to derive the prune bound.
func TestAppliedLastID(t *testing.T) {
	got, err := AppliedLastID(`{"last_id":42}`)
	if err != nil {
		t.Fatalf("AppliedLastID valid token: %v", err)
	}
	if got != 42 {
		t.Errorf("AppliedLastID = %d; want 42", got)
	}

	if _, err := AppliedLastID(""); err == nil {
		t.Error("AppliedLastID(empty) returned nil; want a loud error")
	}
	if _, err := AppliedLastID("not-json"); err == nil {
		t.Error("AppliedLastID(malformed) returned nil; want a loud error")
	}
	// A negative last_id is rejected by decodePos (the persisted watermark must be >= 0).
	if _, err := AppliedLastID(`{"last_id":-1}`); err == nil {
		t.Error("AppliedLastID(negative) returned nil; want a loud error")
	}
	// A FOREIGN token that happens to unmarshal cleanly (a pgoutput {slot,lsn},
	// a broker envelope) must REFUSE — not silently decode to last_id=0 and look
	// like "nothing to prune" against the wrong stream.
	for _, foreign := range []string{
		`{"slot":"sluice_slot","lsn":"0/16B3748"}`,
		`{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}`,
		`{"chain_id":"c1","segment":3}`,
	} {
		if _, err := AppliedLastID(foreign); err == nil {
			t.Errorf("AppliedLastID(%q) returned nil; want a loud refuse (no last_id key)", foreign)
		}
	}
}
