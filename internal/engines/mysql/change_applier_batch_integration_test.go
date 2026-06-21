//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the MySQL batched change applier
// (ApplyBatch). Boots a MySQL container, drives a stream of
// Insert events through ApplyBatch, and asserts:
//
//   - The N rows land on the dest with idempotency on replay.
//   - A Truncate event mid-stream flushes the in-flight batch
//     and applies alone.
//   - Channel close mid-batch commits partial work and persists
//     the position of the last applied change.
//
// MySQL TRUNCATE TABLE is DDL and implicit-commits any open
// transaction; the batched applier flushes the in-flight non-DDL
// changes before dispatching the truncate so the previous batch's
// position write lands before the truncate's implicit commit.

package mysql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// batchSizeRecorder is a test ir.BatchObserver that records the row
// count of every committed batch — a reliable, in-process way to assert
// that the ADR-0089 keyless guard commits keyless changes one-per-tx.
type batchSizeRecorder struct {
	mu   sync.Mutex
	rows []int
}

func (r *batchSizeRecorder) ObserveBatch(_ context.Context, _ time.Duration, rows int, _ error) {
	if rows <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows = append(r.rows, rows)
}

func (r *batchSizeRecorder) maxRows() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := 0
	for _, n := range r.rows {
		if n > m {
			m = n
		}
	}
	return m
}

func (r *batchSizeRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rows)
}

// pumpBatchedChanges feeds a slice of changes through ApplyBatch.
// Mirrors the per-change pumpChanges helper.
func pumpBatchedChanges(t *testing.T, ctx context.Context, applier ir.ChangeApplier, events []ir.Change, batchSize int) {
	t.Helper()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	batched, ok := applier.(ir.BatchedChangeApplier)
	if !ok {
		t.Fatalf("applier does not implement BatchedChangeApplier")
	}
	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	if err := batched.ApplyBatch(ctx, testStreamID, ch, batchSize); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
}

// TestChangeApplier_ApplyBatch_IdempotentReplay confirms a batched
// stream applies and that replaying the same stream is a no-op
// (the upsert / tolerant-zero-rows path from ADR-0010 still
// holds when changes are batched into one tx).
func TestChangeApplier_ApplyBatch_IdempotentReplay(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	const totalRows = 50
	const batchSize = 10
	events := make([]ir.Change, 0, totalRows)
	for i := int64(1); i <= totalRows; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("token-%d", i)},
			Schema:   "target_db",
			Table:    "users",
			Row:      ir.Row{"id": i, "email": fmt.Sprintf("u%d@x", i)},
		})
	}

	pumpBatchedChanges(t, ctx, applier, events, batchSize)

	if got := countAllRows(t, dsn, "target_db", "users"); got != totalRows {
		t.Errorf("after batched apply: rows = %d; want %d", got, totalRows)
	}

	// Idempotency: replay the same stream.
	pumpBatchedChanges(t, ctx, applier, events, batchSize)
	if got := countAllRows(t, dsn, "target_db", "users"); got != totalRows {
		t.Errorf("after replay: rows = %d; want %d (idempotency violated)", got, totalRows)
	}
}

// TestChangeApplier_ApplyBatch_KeylessTableNotBatched pins the ADR-0089
// keyless guard on MySQL: a table with NO PRIMARY KEY and NO UNIQUE
// index makes ON DUPLICATE KEY UPDATE inert (effective plain,
// non-idempotent INSERT), so the batch loop must commit each such change
// in its own transaction even at a large batch size — otherwise a
// crash-replay would amplify duplicates from 1 to up to N. Asserted via a
// BatchObserver: every committed batch has exactly 1 row.
func TestChangeApplier_ApplyBatch_KeylessTableNotBatched(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// No PRIMARY KEY and no UNIQUE index → truly keyless (Bug 125 class 3).
	applyMySQLApplier(t, dsn, `
		CREATE TABLE events_log (
			kind    VARCHAR(32)  NOT NULL,
			payload VARCHAR(255)
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	rec := &batchSizeRecorder{}
	applier.(ir.BatchObserverSetter).SetBatchObserver(rec)

	const totalRows = 40
	const batchSize = 1000 // would batch ALL rows in one tx if unguarded
	events := make([]ir.Change, 0, totalRows)
	for i := int64(1); i <= totalRows; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("token-%d", i)},
			Schema:   "target_db",
			Table:    "events_log",
			Row:      ir.Row{"kind": "k", "payload": fmt.Sprintf("p%d", i)},
		})
	}

	pumpBatchedChanges(t, ctx, applier, events, batchSize)

	// The guard must force every keyless change into its own batch: max
	// observed batch size is 1, one batch per row. Without the guard,
	// batchSize=1000 would yield a single 40-row batch (maxRows=40).
	if got := rec.maxRows(); got != 1 {
		t.Errorf("max batch rows = %d; want 1 (keyless table must NOT batch — ADR-0089)", got)
	}
	if got := rec.count(); got != totalRows {
		t.Errorf("committed batches = %d; want %d (one keyless change per batch)", got, totalRows)
	}
	if got := countAllRows(t, dsn, "target_db", "events_log"); got != totalRows {
		t.Errorf("after keyless apply: rows = %d; want %d", got, totalRows)
	}
}

// TestChangeApplier_ApplyBatch_TruncateFlushesBatch verifies the
// MySQL-specific truncate handling: TRUNCATE implicit-commits, so
// the batched applier flushes the in-flight non-DDL changes first,
// then dispatches the truncate alone.
func TestChangeApplier_ApplyBatch_TruncateFlushesBatch(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	events := []ir.Change{
		ir.Insert{Position: ir.Position{Token: "p1"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "p2"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: "p3"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
		ir.Truncate{Position: ir.Position{Token: "p4"}, Schema: "target_db", Table: "users"},
		ir.Insert{Position: ir.Position{Token: "p5"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(4), "email": "d@x"}},
		ir.Insert{Position: ir.Position{Token: "p6"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(5), "email": "e@x"}},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	if got := countAllRows(t, dsn, "target_db", "users"); got != 2 {
		t.Errorf("after truncate-flush batched apply: rows = %d; want 2 (truncate should have wiped the pre-truncate inserts)", got)
	}
}

// TestChangeApplier_ApplyBatch_TxCommitFlushesBatch verifies the
// source-transaction-boundary aware flush path (ADR-0027). A
// TxCommit event mid-stream flushes the in-flight target tx so the
// target commit boundary aligns with the source's XIDEvent. The
// test feeds two non-empty source transactions plus one empty
// (TxBegin → TxCommit with no rows) through a large batchSize and
// asserts the rows land and the position written is the last
// TxCommit's position. Commit-count assertions on MySQL are not as
// clean as on PG (no `pg_stat_database` equivalent), so the load-
// bearing assertions are correctness + position alignment.
func TestChangeApplier_ApplyBatch_TxCommitFlushesBatch(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	const lastToken = "tx3-commit-token"
	events := []ir.Change{
		ir.TxBegin{Position: ir.Position{Token: "tx1-begin"}},
		ir.Insert{Position: ir.Position{Token: "tx1-r1"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "tx1-r2"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: "tx1-r3"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
		ir.TxCommit{Position: ir.Position{Token: "tx1-commit"}},
		ir.TxBegin{Position: ir.Position{Token: "tx2-begin"}},
		ir.TxCommit{Position: ir.Position{Token: "tx2-commit"}},
		ir.TxBegin{Position: ir.Position{Token: "tx3-begin"}},
		ir.Insert{Position: ir.Position{Token: "tx3-r1"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(4), "email": "d@x"}},
		ir.Insert{Position: ir.Position{Token: "tx3-r2"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(5), "email": "e@x"}},
		ir.TxCommit{Position: ir.Position{Token: lastToken}},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	if got := countAllRows(t, dsn, "target_db", "users"); got != 5 {
		t.Errorf("after tx-aligned batched apply: rows = %d; want 5", got)
	}

	pos, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil {
		t.Fatalf("ReadPosition: %v", err)
	}
	if !ok {
		t.Fatal("ReadPosition: no row found; expected TxCommit-flush to persist position")
	}
	if pos.Token != lastToken {
		t.Errorf("position token = %q; want %q (last source TxCommit's position)", pos.Token, lastToken)
	}
}

// TestChangeApplier_ApplyBatch_PartialFlushPersistsPosition checks that
// a partial (sub-maxBatchSize) batch that ends at a source-transaction
// boundary commits its rows and persists the BOUNDARY position. Post
// item 29 the serial loop checkpoints only at tx boundaries (a mid-tx
// position is not a valid MySQL file/pos resume point), so the stream
// ends with a TxCommit and the persisted token is that boundary's — not
// the last row's. The channel then closes after the boundary flush.
func TestChangeApplier_ApplyBatch_PartialFlushPersistsPosition(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	const lastToken = "the-last-token"
	// A partial (3 rows ≪ 100) source transaction: BEGIN, three rows, COMMIT.
	// The TxCommit flushes the sub-cap batch and persists the BOUNDARY token;
	// the channel then closes after the boundary has been checkpointed.
	events := []ir.Change{
		ir.TxBegin{Position: ir.Position{Token: "tx-begin"}},
		ir.Insert{Position: ir.Position{Token: "p1"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "p2"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: "p3"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
		ir.TxCommit{Position: ir.Position{Token: lastToken}},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	if got := countAllRows(t, dsn, "target_db", "users"); got != 3 {
		t.Errorf("after partial flush: rows = %d; want 3", got)
	}

	pos, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil {
		t.Fatalf("ReadPosition: %v", err)
	}
	if !ok {
		t.Fatal("ReadPosition: no row found; expected the TxCommit-boundary flush to persist position")
	}
	if pos.Token != lastToken {
		t.Errorf("position token = %q; want %q (the source TxCommit boundary, not a mid-tx row)", pos.Token, lastToken)
	}
}

// TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial is the MySQL
// mirror of the PG idle-flush test. It pins roadmap item 18 Fix B: a
// partial batch (n < maxBatchSize) on a quiet stream commits its DATA
// within the short idle grace (now 100ms, was 5s) even when the channel
// hasn't closed and no Truncate has arrived — so the target stays fresh
// and the unapplied-data window stays small.
//
// Pre-fix shape: the applier waited indefinitely for maxBatchSize,
// channel close, or a Truncate; a 3-of-100 batch sat in memory for 5s.
//
// The stream here is an IN-FLIGHT source transaction that paused (BEGIN +
// three rows, no COMMIT yet). The idle flush commits the three rows'
// data promptly — but because the transaction has not committed, the
// resume position is NOT advanced (item 29: the serial loop checkpoints
// only at a tx boundary; a mid-tx MySQL file/pos position is not a valid
// resume point). So the assertion surface is the DATA landing within the
// grace, plus the position deliberately NOT persisting mid-transaction.
// The channel is kept OPEN so the idle timer (not channel-close) is what
// fires. The 2s deadline is far above 100ms + CI jitter but well below
// the pre-fix 5s, so a regression to the old grace fails loudly.
func TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		);
	`)

	parentCtx, cancelParent := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelParent()

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(parentCtx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(parentCtx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	batched, ok := applier.(ir.BatchedChangeApplier)
	if !ok {
		t.Fatalf("applier does not implement BatchedChangeApplier")
	}

	// An in-flight source transaction that paused mid-stream: BEGIN + three
	// rows, no COMMIT. The idle flush must commit the rows' DATA promptly.
	events := []ir.Change{
		ir.TxBegin{Position: ir.Position{Token: "tx-begin"}},
		ir.Insert{Position: ir.Position{Token: "p1"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "p2"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: "p3"}, Schema: "target_db", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
	}

	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	// IMPORTANT: do NOT close the channel — we want to assert idle-flush
	// fires on its own, not the channel-close flush.

	applyCtx, cancelApply := context.WithCancel(parentCtx)
	defer cancelApply()

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- batched.ApplyBatch(applyCtx, testStreamID, ch, 100)
	}()

	// Poll for the DATA: the idle flush commits the three rows promptly even
	// though no COMMIT has arrived (item 18 Fix B). Position persistence is
	// NOT the signal here — a mid-tx flush deliberately does not advance it.
	const flushDeadline = 2 * time.Second
	var rows int
	deadline := time.Now().Add(flushDeadline)
	for time.Now().Before(deadline) {
		if rows = countAllRows(t, dsn, "target_db", "users"); rows == 3 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	flushElapsed := time.Since(start)

	if rows != 3 {
		cancelApply()
		<-done
		t.Fatalf("idle-flush did not commit the partial batch's data within %v (got %d rows; Fix B grace is %v; a regression to the 5s grace would manifest here)",
			flushDeadline, rows, defaultIdleFlushPeriod)
	}
	if flushElapsed >= 5*time.Second {
		cancelApply()
		<-done
		t.Errorf("idle flush took %v; want well under the pre-fix 5s grace (item 18 Fix B = %v)", flushElapsed, defaultIdleFlushPeriod)
	}

	// item 29: the transaction has NOT committed, so the resume position must
	// NOT have advanced into the transaction (a mid-tx MySQL file/pos point is
	// an invalid resume anchor). It stays unset until a TxCommit boundary.
	_, found, err := applier.ReadPosition(parentCtx, testStreamID)
	if err != nil {
		cancelApply()
		<-done
		t.Fatalf("ReadPosition: %v", err)
	}
	if found {
		cancelApply()
		<-done
		t.Error("position was persisted mid-transaction; want NO checkpoint until a TxCommit boundary (item 29)")
	}

	// Cancel and drain so the goroutine exits cleanly.
	cancelApply()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("ApplyBatch did not return after ctx cancel post-idle-flush")
	}
}
