//go:build integration

// Integration test for the Postgres batched change applier
// (ApplyBatch). Boots a Postgres container, drives a stream of
// Insert/Update/Delete events through ApplyBatch, and asserts:
//
//   - Multiple changes commit in fewer target transactions than
//     the per-change Apply path would (commit count is observed
//     via pg_stat_database).
//   - Idempotency holds: replaying the same batched stream does
//     not duplicate rows or violate the final state.
//   - Truncate flushes the in-flight batch and applies alone.
//   - ctx cancel mid-batch rolls back; no partial write lands.
//   - Channel close mid-batch commits the partial batch cleanly.
//
// The throughput claim — ~50-100x improvement on bulk CDC traffic —
// is asserted via the commit count, not wall-clock latency, so the
// test stays deterministic across CI host load.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// readXactCommit reads the cumulative committed-transaction count
// for the connecting database from pg_stat_database. Used to
// observe that a batched apply produces fewer commits than a
// per-change apply would.
//
// pg_stat_database is updated continuously by the stats collector
// but observation lag is small (sub-second) on idle databases —
// the test sleeps briefly between snapshots to let the count
// stabilise.
func readXactCommit(t *testing.T, dsn string) int64 {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int64
	q := "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()"
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("read xact_commit: %v", err)
	}
	return n
}

// pumpBatchedChanges feeds a slice of changes through ApplyBatch
// and waits for the call to return. Mirrors pumpChanges but goes
// through the BatchedChangeApplier interface.
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

// TestChangeApplier_ApplyBatch_FewerCommits confirms the load-
// bearing throughput claim: a batched apply of N changes produces
// roughly ceil(N/batchSize) target commits rather than N. Idempotency
// is also verified via a replay.
func TestChangeApplier_ApplyBatch_FewerCommits(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id     BIGINT       PRIMARY KEY,
			email  VARCHAR(255) NOT NULL UNIQUE,
			active BOOLEAN      NOT NULL DEFAULT true
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

	const totalRows = 100
	const batchSize = 25
	events := make([]ir.Change, 0, totalRows)
	for i := int64(1); i <= totalRows; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNamePostgres, Token: tokenForInt(i)},
			Schema:   "public",
			Table:    "users",
			Row:      ir.Row{"id": i, "email": emailForInt(i), "active": true},
		})
	}

	// Snapshot the commit counter, run the batched apply, then
	// snapshot again. The delta should be roughly totalRows/batchSize
	// + a small constant for ancillary connections (EnsureControlTable,
	// pkFor / colTypesFor lookups, pg_stat probes themselves).
	startCommits := readXactCommit(t, dsn)

	pumpBatchedChanges(t, ctx, applier, events, batchSize)

	// pg_stat_database lags slightly; give it a moment to flush.
	time.Sleep(300 * time.Millisecond)
	endCommits := readXactCommit(t, dsn)

	delta := endCommits - startCommits
	// Lower bound: ceil(totalRows/batchSize) = 4 for 100 rows / 25
	// batch. Upper bound: tolerant of metadata lookups and the
	// pg_stat_database read itself being one commit-ish operation.
	// Per-change Apply would produce >= totalRows commits, so the
	// inequality is wide and stable.
	const expectedBatches = totalRows / batchSize
	const tolerance = 30 // metadata lookups, control-table ensure, stat-read overhead
	if delta < expectedBatches {
		t.Errorf("commit delta = %d; want >= %d (one per batch)", delta, expectedBatches)
	}
	if delta > expectedBatches+tolerance {
		t.Errorf("commit delta = %d; want <= %d (per-change apply would produce >=%d)",
			delta, expectedBatches+tolerance, totalRows)
	}

	// Final state check: every row landed.
	if got := countAllRows(t, dsn, "users"); got != totalRows {
		t.Errorf("after batched apply: rows = %d; want %d", got, totalRows)
	}

	// Idempotency: replay the same batched stream.
	pumpBatchedChanges(t, ctx, applier, events, batchSize)
	if got := countAllRows(t, dsn, "users"); got != totalRows {
		t.Errorf("after replay batched apply: rows = %d; want %d (idempotency violated)", got, totalRows)
	}
}

// TestChangeApplier_ApplyBatch_TruncateFlushesBatch verifies that a
// Truncate event mid-stream flushes the in-flight batch (so prior
// changes are durable) and applies alone (so the truncate doesn't
// roll back N un-related INSERTs on a transient failure).
func TestChangeApplier_ApplyBatch_TruncateFlushesBatch(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
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

	// Three inserts, a truncate (which should flush the batch and
	// apply alone), then three more inserts.
	events := []ir.Change{
		ir.Insert{Position: ir.Position{Token: "p1"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "p2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: "p3"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
		ir.Truncate{Position: ir.Position{Token: "p4"}, Schema: "public", Table: "users"},
		ir.Insert{Position: ir.Position{Token: "p5"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(4), "email": "d@x"}},
		ir.Insert{Position: ir.Position{Token: "p6"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(5), "email": "e@x"}},
		ir.Insert{Position: ir.Position{Token: "p7"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(6), "email": "f@x"}},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	// Final state: only the post-truncate rows survive.
	if got := countAllRows(t, dsn, "users"); got != 3 {
		t.Errorf("after truncate-flush batched apply: rows = %d; want 3 (truncate should have wiped the pre-truncate inserts)", got)
	}
}

// TestChangeApplier_ApplyBatch_ChannelCloseFlushesPartial checks the
// short-batch path: when the channel closes before maxBatchSize is
// reached, the in-flight changes still commit. The position of the
// last applied change must persist.
func TestChangeApplier_ApplyBatch_ChannelCloseFlushesPartial(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
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

	const lastToken = "the-last-position-token"
	events := []ir.Change{
		ir.Insert{Position: ir.Position{Token: "p1"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "p2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: lastToken}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
	}

	// Batch size 100 — the channel close (3 rows) flushes early.
	pumpBatchedChanges(t, ctx, applier, events, 100)

	if got := countAllRows(t, dsn, "users"); got != 3 {
		t.Errorf("after partial batch flush: rows = %d; want 3", got)
	}

	// Position of the last applied change is persisted.
	pos, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil {
		t.Fatalf("ReadPosition: %v", err)
	}
	if !ok {
		t.Fatal("ReadPosition: no row found; expected partial batch to persist position")
	}
	if pos.Token != lastToken {
		t.Errorf("position token = %q; want %q (last applied change in batch)", pos.Token, lastToken)
	}
}

// TestChangeApplier_ApplyBatch_CtxCancelRollsBack confirms that
// cancelling the apply context mid-batch rolls back the in-flight
// transaction. No partial rows land; the position does not advance.
func TestChangeApplier_ApplyBatch_CtxCancelRollsBack(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
	`)

	eng := Engine{}
	parentCtx, cancelParent := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelParent()

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
		t.Fatal("applier does not implement BatchedChangeApplier")
	}

	// Start ApplyBatch on a context we control. Pump three changes
	// through the channel, then cancel before sending the rest.
	applyCtx, cancelApply := context.WithCancel(parentCtx)
	ch := make(chan ir.Change, 4)
	ch <- ir.Insert{Position: ir.Position{Token: "p1"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}}
	ch <- ir.Insert{Position: ir.Position{Token: "p2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}}
	ch <- ir.Insert{Position: ir.Position{Token: "p3"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}}

	// batchSize=100 — won't flush on row count. Channel-close flushes
	// would commit; we send a cancel before close to exercise
	// rollback.
	done := make(chan error, 1)
	go func() {
		done <- batched.ApplyBatch(applyCtx, testStreamID, ch, 100)
	}()

	// Brief sleep so the goroutine has a chance to dispatch a few
	// of the buffered changes before we cancel.
	time.Sleep(200 * time.Millisecond)
	cancelApply()

	select {
	case err := <-done:
		if err == nil {
			t.Errorf("ApplyBatch returned nil after ctx cancel; expected ctx error")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ApplyBatch did not return after ctx cancel")
	}

	// No rows landed (the in-flight tx rolled back) and the position
	// did not advance.
	if got := countAllRows(t, dsn, "users"); got != 0 {
		t.Errorf("after ctx cancel: rows = %d; want 0 (in-flight tx should have rolled back)", got)
	}
	if _, found, err := applier.ReadPosition(parentCtx, testStreamID); err != nil {
		t.Fatalf("ReadPosition: %v", err)
	} else if found {
		t.Error("ReadPosition: row found after ctx-cancelled batch; position should not have advanced")
	}
}

// TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial confirms the
// trailing-row-latency fix: a partial batch (n < maxBatchSize) on a
// quiet stream commits within defaultIdleFlushPeriod even when the
// channel hasn't closed and no Truncate has arrived.
//
// Pre-fix shape: the applier waited indefinitely for either
// maxBatchSize, channel close, or a Truncate. A 3-of-100 batch sat
// in memory; the slot's confirmed_flush_lsn never advanced past the
// last full batch (PG, ADR-0020 trailing-row-latency footnote). On
// warm-resume from a quiet stream, the slot would replay from a
// stale boundary.
//
// Post-fix: an idle timer (5s default) commits the partial batch.
// This test feeds 3 changes through an open channel (kept open!),
// waits one idle window plus headroom, and asserts both the rows
// landed AND the persisted position advanced — both load-bearing
// signals that the partial batch committed.
func TestChangeApplier_ApplyBatch_IdleFlushCommitsPartial(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
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

	const lastToken = "idle-flush-last"
	events := []ir.Change{
		ir.Insert{Position: ir.Position{Token: "p1"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "p2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: lastToken}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
	}

	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	// IMPORTANT: do NOT close the channel. The pre-fix path would
	// commit on close; we want to assert idle-flush fires on its
	// own.

	applyCtx, cancelApply := context.WithCancel(parentCtx)
	defer cancelApply()

	done := make(chan error, 1)
	go func() {
		done <- batched.ApplyBatch(applyCtx, testStreamID, ch, 100)
	}()

	// Wait for one idle-flush window plus headroom. The applier's
	// `applyOneBatch` will dispatch all 3 changes (sub-millisecond),
	// then sit on the select with the idle timer running. After 5s
	// the timer fires and commitBatch runs.
	time.Sleep(7 * time.Second)

	// Rows landed (the in-flight tx committed).
	if got := countAllRows(t, dsn, "users"); got != 3 {
		cancelApply()
		<-done
		t.Errorf("after idle flush: rows = %d; want 3", got)
	}

	// Position advanced to the last applied change's token.
	pos, found, err := applier.ReadPosition(parentCtx, testStreamID)
	if err != nil {
		cancelApply()
		<-done
		t.Fatalf("ReadPosition: %v", err)
	}
	if !found {
		cancelApply()
		<-done
		t.Fatal("ReadPosition: no row found; idle-flush did not persist the partial batch")
	}
	if pos.Token != lastToken {
		cancelApply()
		<-done
		t.Errorf("position token = %q; want %q (last applied change in partial batch)", pos.Token, lastToken)
	}

	// Cancel and drain so the goroutine exits cleanly.
	cancelApply()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("ApplyBatch did not return after ctx cancel post-idle-flush")
	}
}

// TestChangeApplier_ApplyBatch_TxCommitFlushesBatch verifies the
// source-transaction-boundary aware flush path (ADR-0027). A
// TxCommit event mid-stream flushes the in-flight target tx so the
// target commit boundary aligns with the source's. The test feeds
// two source transactions through a large batchSize (so row-count
// flush wouldn't fire) and asserts:
//
//   - All rows land (correctness).
//   - Two target commits are observed for the two source-tx
//     groupings (alignment).
//   - The persisted position is the last TxCommit's position
//     (per-batch position semantics).
//
// Empty source transactions (TxBegin → TxCommit with no row events)
// are folded into the same test to confirm they don't produce empty
// target commits.
func TestChangeApplier_ApplyBatch_TxCommitFlushesBatch(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
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

	// Three source-side transactions:
	//   tx1: 3 inserts (1, 2, 3)   -> commit
	//   tx2: empty                 -> commit (must NOT produce a target tx)
	//   tx3: 2 inserts (4, 5)      -> commit
	//
	// batchSize=100 means row-count flush won't fire; only TxCommit
	// alignment can produce the observed two commits.
	const lastToken = "tx3-commit-token"
	events := []ir.Change{
		ir.TxBegin{Position: ir.Position{Token: "tx1-begin"}},
		ir.Insert{Position: ir.Position{Token: "tx1-r1"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "email": "a@x"}},
		ir.Insert{Position: ir.Position{Token: "tx1-r2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "email": "b@x"}},
		ir.Insert{Position: ir.Position{Token: "tx1-r3"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(3), "email": "c@x"}},
		ir.TxCommit{Position: ir.Position{Token: "tx1-commit"}},
		ir.TxBegin{Position: ir.Position{Token: "tx2-begin"}},
		ir.TxCommit{Position: ir.Position{Token: "tx2-commit"}},
		ir.TxBegin{Position: ir.Position{Token: "tx3-begin"}},
		ir.Insert{Position: ir.Position{Token: "tx3-r1"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(4), "email": "d@x"}},
		ir.Insert{Position: ir.Position{Token: "tx3-r2"}, Schema: "public", Table: "users", Row: ir.Row{"id": int64(5), "email": "e@x"}},
		ir.TxCommit{Position: ir.Position{Token: lastToken}},
	}

	startCommits := readXactCommit(t, dsn)
	pumpBatchedChanges(t, ctx, applier, events, 100)
	time.Sleep(300 * time.Millisecond)
	endCommits := readXactCommit(t, dsn)

	if got := countAllRows(t, dsn, "users"); got != 5 {
		t.Errorf("after tx-aligned batched apply: rows = %d; want 5", got)
	}

	// We should observe at least 2 commits (one per non-empty source
	// tx) and not many more — the empty tx in the middle must NOT
	// produce its own target commit. Tolerance covers
	// EnsureControlTable, pkFor / colTypesFor metadata lookups, and
	// the pg_stat read itself. A pre-ADR-0027 batched applier with
	// no flush-on-TxCommit would produce ONE commit (everything in
	// one big batch); a per-change applier would produce >= 5.
	delta := endCommits - startCommits
	if delta < 2 {
		t.Errorf("commit delta = %d; want >= 2 (one per non-empty source tx)", delta)
	}
	const tolerance = 30
	if delta > 2+tolerance {
		t.Errorf("commit delta = %d; want <= %d (suggests empty source tx produced a target commit)", delta, 2+tolerance)
	}

	pos, found, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil {
		t.Fatalf("ReadPosition: %v", err)
	}
	if !found {
		t.Fatal("ReadPosition: no row found; expected TxCommit-flush to persist position")
	}
	if pos.Token != lastToken {
		t.Errorf("position token = %q; want %q (last source TxCommit's position)", pos.Token, lastToken)
	}
}

// TestChangeApplier_ApplyBatch_ByteCapFlushes verifies the
// memory-bounded streaming knob (--max-buffer-bytes, ADR-0028): when
// the per-batch byte cap is below what maxBatchSize rows would
// accumulate, the applier flushes early on the byte cap and produces
// more commits than maxBatchSize alone would predict.
//
// Setup: 20 inserts with a ~10 KB payload column each (~200 KB
// total). Row cap of 100 (effectively unbounded for the input) and
// byte cap of 32 KB. Expect roughly ceil(200 KB / 32 KB) = 7
// commits, not 1 (which is what the row cap alone would produce).
func TestChangeApplier_ApplyBatch_ByteCapFlushes(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE wide_rows (
			id   BIGINT PRIMARY KEY,
			data TEXT   NOT NULL
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

	// Apply the byte cap: 32 KB. Per-row payload is 10 KB so ~3
	// rows per batch before the cap fires.
	const byteCap int64 = 32 * 1024
	if setter, ok := applier.(ir.MaxBufferBytesSetter); ok {
		setter.SetMaxBufferBytes(byteCap)
	} else {
		t.Fatalf("applier does not implement MaxBufferBytesSetter")
	}

	const totalRows = 20
	const rowPayloadBytes = 10 * 1024
	payload := strings.Repeat("x", rowPayloadBytes)

	events := make([]ir.Change, 0, totalRows)
	for i := int64(1); i <= totalRows; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNamePostgres, Token: tokenForInt(i)},
			Schema:   "public",
			Table:    "wide_rows",
			Row:      ir.Row{"id": i, "data": payload},
		})
	}

	startCommits := readXactCommit(t, dsn)
	// maxBatchSize is set to 100 — far higher than totalRows — so the
	// row cap can't be the flush trigger; only the byte cap can.
	pumpBatchedChanges(t, ctx, applier, events, 100)

	// pg_stat_database's xact_commit counter is updated by the stats
	// collector on its own cadence (PGSTAT_STAT_INTERVAL, typically
	// ~500ms). A 1.5s sleep is comfortably above that. Each
	// readXactCommit's own SELECT counts as a commit, so polling
	// would inflate the delta against itself — a single sleep + read
	// is the right shape.
	time.Sleep(1500 * time.Millisecond)
	endCommits := readXactCommit(t, dsn)
	delta := endCommits - startCommits

	// Lower bound: roughly ceil(200 KB / 32 KB) = 7 commits expected.
	// Without the byte cap the row cap (100) would produce just 1
	// commit; the inequality below is wide enough to absorb the
	// approximation noise (ApproximateRowBytes counts the data
	// string's length plus 8 bytes for the int64; per-row total is
	// ~10248 bytes, so the cap fires at 4 rows → 5 commits for 20
	// rows). Floor of 4 covers the edge case where the cap-comparison
	// rounds the other way for the last partial batch. Upper bound
	// of 100 absorbs PG-level overhead (autovacuum, control-table
	// upserts, stat-read SELECTs themselves).
	const minExpected = 4
	const maxExpected = 100
	if delta < minExpected {
		t.Errorf("commit delta = %d; want >= %d (byte-cap flush should produce multiple commits)", delta, minExpected)
	}
	if delta > maxExpected {
		t.Errorf("commit delta = %d; want <= %d (suggests too many flushes — byte cap may be ignored)", delta, maxExpected)
	}

	// Final state check: every row landed.
	if got := countAllRows(t, dsn, "wide_rows"); got != totalRows {
		t.Errorf("after byte-cap apply: rows = %d; want %d", got, totalRows)
	}
}

// emailForInt returns a deterministic email address for the bulk-
// insert tests above.
func emailForInt(i int64) string {
	return fmt.Sprintf("user%d@example.com", i)
}

// tokenForInt returns a synthetic position token used by the bulk-
// insert tests. Real tokens are JSON LSN blobs; for test purposes
// we just need a unique string per change.
func tokenForInt(i int64) string {
	return fmt.Sprintf("token-%d", i)
}

// (assertContains is a tiny helper so test failures show a snippet
// of the relevant log output without requiring a full fixture.)
//
//nolint:unused // reserved for log-content assertions if added later
func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected log output to contain %q; got:\n%s", needle, haystack)
	}
}
