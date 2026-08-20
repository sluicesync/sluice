//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_PipelinedExecTimeout_CancelsCleanly pins the audit-2026-08-19
// Option-B fix for the pipelined exec-timeout race. When the pipelined batch
// flush blocks past the per-exec timeout, the fix bounds SendBatch/Commit with
// the CONTEXT the pgx op already accepts (not appliershared.RunWithDeadline's
// timer+orphan), so pgx cancels the in-flight op and closes the conn — leaving
// NO orphaned goroutine racing the caller's Rollback/release on the raw
// *pgx.Conn. Under the Integration shard's -race, the pre-fix orphan (an
// abandoned SendBatch goroutine still on the conn while flushAndCommit rolls
// back + pool-releases it) is a data race this test provokes DETERMINISTICALLY:
// a second session holds ACCESS EXCLUSIVE on the target table, so the applier's
// INSERT is guaranteed still blocked in the flush at the moment the timeout
// fires. The test also asserts the two properties the fix must preserve: the
// timeout classifies RETRIABLE (so the stream reopens rather than dying), and a
// retry after the block clears applies the row exactly once.
func TestChangeApplier_PipelinedExecTimeout_CancelsCleanly(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT);`)

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

	// A short per-exec timeout so the lock-blocked flush trips it quickly.
	setter, ok := applier.(interface{ SetExecTimeout(time.Duration) })
	if !ok {
		t.Fatal("applier does not expose SetExecTimeout")
	}
	setter.SetExecTimeout(750 * time.Millisecond)

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	batched, ok := applier.(ir.BatchedChangeApplier)
	if !ok {
		t.Fatal("applier is not a BatchedChangeApplier")
	}

	insert := func(id int64) <-chan ir.Change {
		ch := make(chan ir.Change, 1)
		ch <- ir.Insert{
			Position: ir.Position{Engine: engineNamePostgres, Token: tokenForInt(id)},
			Schema:   "public",
			Table:    "t",
			Row:      ir.Row{"id": id, "v": "x"},
		}
		close(ch)
		return ch
	}

	// Session B holds ACCESS EXCLUSIVE on t: the applier's INSERT will block in
	// the pipelined flush until the timeout cancels it (a lock wait IS
	// cancellable), guaranteeing the flush is still in-flight when the timeout
	// fires — the window the pre-fix orphan raced.
	lockDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open lock conn: %v", err)
	}
	defer func() { _ = lockDB.Close() }()
	lockConn, err := lockDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire lock conn: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN on lock conn: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, "LOCK TABLE t IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("LOCK TABLE: %v", err)
	}

	// First apply: blocks on the lock, trips the exec timeout, and must return a
	// RETRIABLE error — cancelled cleanly, no orphan (the -race check runs here).
	err = batched.ApplyBatch(ctx, testStreamID, insert(1), 1)
	if err == nil {
		t.Fatal("ApplyBatch returned nil while the target was lock-blocked past the exec timeout; want a retriable timeout error")
	}
	var reErr ir.RetriableError
	if !errors.As(err, &reErr) || !reErr.Retriable() {
		t.Fatalf("timeout ApplyBatch error = %v; want a RETRIABLE error so the stream reopens on a fresh conn", err)
	}

	// Release the lock; the retry must apply the row exactly once (idempotent —
	// the timed-out attempt rolled back and moved no data).
	if _, err := lockConn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK on lock conn: %v", err)
	}
	_ = lockConn.Close()

	if err := batched.ApplyBatch(ctx, testStreamID, insert(1), 1); err != nil {
		t.Fatalf("retry ApplyBatch after the lock released: %v", err)
	}
	if got := countAllRows(t, dsn, "t"); got != 1 {
		t.Fatalf("after timeout + retry: rows = %d; want exactly 1", got)
	}
}
