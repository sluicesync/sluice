// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests pin the SHARED batch loop's control flow at the seam
// level (ADR-0081): commit ordering (ADR-0007/0010), flush triggers,
// the TransactionalDDL divergence, and rollback/classify shapes. The
// engine packages keep their own behaviour oracles — the item-18 AIMD
// timing pins (change_applier_aimd_test.go) and the integration
// batch suites — which must pass unchanged across the extraction.
//
// Mechanism: a no-op database/sql fake driver supplies real *sql.Tx
// values (the loop only ever Begin/Commit/Rollbacks them; every
// statement runs through the Dispatch / WritePosition hooks, which
// here just record), so the loop runs end to end without a database
// and the recorded event order is the assertion surface.

// recorder collects ordered events from hooks and the fake driver.
// Mutex'd so -race stays clean if a future test feeds from another
// goroutine while the loop records.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// fakeConnector / fakeConn / fakeTx are the minimal database/sql
// driver surface the loop touches: Begin → Commit / Rollback. Prepare
// is unreachable (all SQL goes through the hooks).
type fakeConnector struct{ rec *recorder }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return fakeConn(c), nil
}
func (c fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("appliershared: fake driver opens via connector only")
}

type fakeConn struct{ rec *recorder }

func (fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("appliershared: fake conn has no statements")
}
func (fakeConn) Close() error                { return nil }
func (c fakeConn) Begin() (driver.Tx, error) { return fakeTx(c), nil }

type fakeTx struct{ rec *recorder }

func (fakeTx) Commit() error     { return nil }
func (t fakeTx) Rollback() error { t.rec.add("tx.Rollback"); return nil }

// testConfig builds a BatchConfig whose hooks record into rec. Tests
// override individual hooks for failure injection.
func testConfig(t *testing.T, rec *recorder, transactionalDDL bool) *BatchConfig {
	t.Helper()
	db := sql.OpenDB(fakeConnector{rec: rec})
	t.Cleanup(func() { _ = db.Close() })
	return &BatchConfig{
		EngineName:       "fake",
		TransactionalDDL: transactionalDDL,
		ByteCap:          1 << 30,
		BeginTx: func(ctx context.Context) (BatchTx, error) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("fake: applier: begin tx: %w", err)
			}
			rec.add("begin")
			return tx, nil
		},
		Dispatch: func(_ context.Context, _ BatchTx, _ string, c ir.Change) error {
			rec.add("dispatch:" + c.Pos().Token)
			return nil
		},
		ApplyOne: func(_ context.Context, _ string, c ir.Change) error {
			rec.add("applyOne:" + c.Pos().Token)
			return nil
		},
		Redact:     func(context.Context, ir.Change) error { return nil },
		StampShard: func(ir.Change) {},
		Classify:   func(err error) error { return fmt.Errorf("classified: %w", err) },
		WritePosition: func(_ context.Context, _ BatchTx, _ string, token string) error {
			rec.add("writePosition:" + token)
			return nil
		},
		Commit: func(tx BatchTx) error {
			rec.add("commit")
			return tx.(*sql.Tx).Commit()
		},
	}
}

func pos(token string) ir.Position { return ir.Position{Token: token} }

func insertAt(token string) ir.Change {
	return ir.Insert{Position: pos(token), Schema: "s", Table: "t", Row: ir.Row{"id": int64(1)}}
}

// feed returns a buffered channel pre-loaded with changes; closed
// when close is true.
func feed(closeAfter bool, changes ...ir.Change) chan ir.Change {
	ch := make(chan ir.Change, len(changes))
	for _, c := range changes {
		ch <- c
	}
	if closeAfter {
		close(ch)
	}
	return ch
}

func assertEvents(t *testing.T, rec *recorder, want []string) {
	t.Helper()
	got := rec.list()
	if len(got) != len(want) {
		t.Fatalf("event count = %d, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event[%d] = %q, want %q\n got: %v\nwant: %v", i, got[i], want[i], got, want)
		}
	}
}

// schemaEvents is the schema-changing family the loop dispatches on.
// Pin the class, not the representative: both members exercise every
// branch the isSchemaEvent / TransactionalDDL paths gate on.
func schemaEvents(token string) map[string]ir.Change {
	return map[string]ir.Change{
		"Truncate":       ir.Truncate{Position: pos(token), Schema: "s", Table: "t"},
		"SchemaSnapshot": ir.SchemaSnapshot{Position: pos(token), Schema: "s", Table: "t"},
	}
}

// TestRunBatchLoop_PositionWriteThenCommitOrdering pins the ADR-0007 /
// ADR-0010 ordering at the seam: every batch is data dispatches →
// position write on the SAME tx → commit → AfterCommit hook, in that
// order, exactly once per batch.
func TestRunBatchLoop_PositionWriteThenCommitOrdering(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.AfterCommit = func(_ context.Context, token string) { rec.add("afterCommit:" + token) }

	ch := feed(true, insertAt("p1"), insertAt("p2"))
	if err := RunBatchLoop(context.Background(), cfg, "stream", ch, 10); err != nil {
		t.Fatalf("RunBatchLoop: %v", err)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "dispatch:p2", "writePosition:p2", "commit", "afterCommit:p2",
	})
}

// TestRunOneBatch_RowCapFlushes pins the row-cap flush trigger: the
// batch commits at exactly maxBatchSize changes, leaving the rest on
// the channel.
func TestRunOneBatch_RowCapFlushes(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)

	ch := feed(false, insertAt("p1"), insertAt("p2"), insertAt("p3"))
	n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 2)
	if err != nil {
		t.Fatalf("RunOneBatch: %v", err)
	}
	if n != 2 || lastPos.Token != "p2" || closed {
		t.Fatalf("n=%d lastPos=%q closed=%v; want n=2 lastPos=p2 closed=false", n, lastPos.Token, closed)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "dispatch:p2", "writePosition:p2", "commit",
	})
}

// TestRunOneBatch_KeylessFirstChangeFlushesAlone pins the ADR-0089
// keyless guard on the first-change path: a change to a truly-keyless
// table commits as a batch of 1 (even though maxBatchSize is 100 and
// more changes are queued), so its crash-replay blast radius stays at
// one row. The next change is left on the channel for the following
// batch.
func TestRunOneBatch_KeylessFirstChangeFlushesAlone(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.IsKeylessTable = func(_ context.Context, c ir.Change) bool {
		return c.Pos().Token == "p1"
	}

	ch := feed(false, insertAt("p1"), insertAt("p2"))
	n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
	if err != nil {
		t.Fatalf("RunOneBatch: %v", err)
	}
	if n != 1 || lastPos.Token != "p1" || closed {
		t.Fatalf("n=%d lastPos=%q closed=%v; want n=1 lastPos=p1 closed=false", n, lastPos.Token, closed)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "writePosition:p1", "commit",
	})
}

// TestRunOneBatch_KeylessMidBatchFlushesIncludingIt pins the ADR-0089
// keyless guard on the mid-batch path: a keyed change (p1) accumulates,
// then a keyless change (p2) is dispatched and forces an immediate
// commit that INCLUDES it — so on replay only p2 (the non-idempotent
// one) could duplicate (blast radius 1), while p1 replays idempotently.
// p3 is left for the next batch.
func TestRunOneBatch_KeylessMidBatchFlushesIncludingIt(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.IsKeylessTable = func(_ context.Context, c ir.Change) bool {
		return c.Pos().Token == "p2"
	}

	ch := feed(false, insertAt("p1"), insertAt("p2"), insertAt("p3"))
	n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
	if err != nil {
		t.Fatalf("RunOneBatch: %v", err)
	}
	if n != 2 || lastPos.Token != "p2" || closed {
		t.Fatalf("n=%d lastPos=%q closed=%v; want n=2 lastPos=p2 closed=false", n, lastPos.Token, closed)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "dispatch:p2", "writePosition:p2", "commit",
	})
}

// TestRunOneBatch_IdleFlushCommitsPartial pins the item-18 Fix B
// shape at the seam: a partial batch on a quiet channel commits
// within the short idle grace, not the pre-fix 5s. The engine
// integration suites pin the same behaviour against real targets.
func TestRunOneBatch_IdleFlushCommitsPartial(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)

	ch := feed(false, insertAt("p1")) // never closed, nothing follows
	start := time.Now()
	n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("RunOneBatch: %v", err)
	}
	if n != 1 || lastPos.Token != "p1" || closed {
		t.Fatalf("n=%d lastPos=%q closed=%v; want n=1 lastPos=p1 closed=false", n, lastPos.Token, closed)
	}
	// Generous bound for CI scheduling jitter while still failing
	// loudly on a regression back to a seconds-scale grace.
	if elapsed >= 2*time.Second {
		t.Errorf("idle flush took %v; want well under 2s (item 18 Fix B grace = %v)", elapsed, DefaultIdleFlushPeriod)
	}
	assertEvents(t, rec, []string{"begin", "dispatch:p1", "writePosition:p1", "commit"})
}

// TestRunOneBatch_TxCommitBoundaryFlushes pins ADR-0027 alignment:
// a TxCommit boundary flushes the in-flight batch and the position
// written is the boundary's (the source commit), not the last row's.
func TestRunOneBatch_TxCommitBoundaryFlushes(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)

	ch := feed(false, insertAt("p1"), ir.TxCommit{Position: pos("p2")})
	n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
	if err != nil {
		t.Fatalf("RunOneBatch: %v", err)
	}
	if n != 1 || lastPos.Token != "p2" || closed {
		t.Fatalf("n=%d lastPos=%q closed=%v; want n=1 lastPos=p2 closed=false", n, lastPos.Token, closed)
	}
	assertEvents(t, rec, []string{"begin", "dispatch:p1", "writePosition:p2", "commit"})
}

// txBegin / txCommit build the source-transaction boundary markers the
// reader emits around row events (ADR-0027).
func txBegin(token string) ir.Change  { return ir.TxBegin{Position: pos(token)} }
func txCommit(token string) ir.Change { return ir.TxCommit{Position: pos(token)} }

// TestRunBatchLoop_CheckpointOnlyAtTxBoundary_NeverPersistsMidTxPosition
// is the regression pin for the HIGH native-MySQL warm-resume bug found
// live on the large-scale program: under CheckpointOnlyAtTxBoundary, a
// source transaction split across batches (here forced by batch size 1)
// must NEVER persist a mid-transaction position — each row's batch commits
// its DATA but skips the position write, and the resume checkpoint advances
// only when the trailing TxCommit boundary is seen (persisted here via the
// dedicated position-only tx, since the COMMIT lands on an empty batch).
//
// Before the fix the row-cap flush persisted the last ROW's position
// (mid-tx); go-mysql then could not warm-resume from it ("no corresponding
// table map event") and crash-looped. The assertion surface is exact: the
// ONLY writePosition is the boundary token, never p1 / p2.
func TestRunBatchLoop_CheckpointOnlyAtTxBoundary_NeverPersistsMidTxPosition(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.CheckpointOnlyAtTxBoundary = true

	// One source tx of two rows then COMMIT, forced to split by cap=1.
	ch := feed(true, txBegin("tb"), insertAt("p1"), insertAt("p2"), txCommit("tc"))
	if err := RunBatchLoop(context.Background(), cfg, "stream", ch, 1); err != nil {
		t.Fatalf("RunBatchLoop: %v", err)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "commit", // row 1: data only — NO mid-tx position
		"begin", "dispatch:p2", "commit", // row 2: data only — NO mid-tx position
		"begin", "writePosition:tc", "commit", // boundary: dedicated position-only tx
	})
}

// TestRunBatchLoop_CheckpointDefault_PersistsMidTxPosition is the Postgres
// regression guard: with the flag OFF (the default), behaviour is byte-for-
// byte what it was before the fix — every flush persists its position
// (PG logical-replication resume is by LSN and tolerates a mid-tx restart
// point), and a TxCommit on an empty batch stays a pure no-op. This pins
// that the MySQL fix did NOT change the PG path.
func TestRunBatchLoop_CheckpointDefault_PersistsMidTxPosition(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false) // CheckpointOnlyAtTxBoundary defaults false

	ch := feed(true, txBegin("tb"), insertAt("p1"), insertAt("p2"), txCommit("tc"))
	if err := RunBatchLoop(context.Background(), cfg, "stream", ch, 1); err != nil {
		t.Fatalf("RunBatchLoop: %v", err)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "writePosition:p1", "commit",
		"begin", "dispatch:p2", "writePosition:p2", "commit",
		// trailing TxCommit on an empty batch: no-op (no dedicated write)
	})
}

// TestRunOneBatch_CheckpointOnlyAtTxBoundary_BoundaryWritesAtomically pins
// the common case: when the whole source tx fits in one batch, the
// TxCommit boundary flush writes the boundary position atomically with the
// rows' data (NO dedicated position-only tx) — the dedicated write is only
// the empty-batch fallback. Resume checkpoint = the boundary (tc), not the
// last row.
func TestRunOneBatch_CheckpointOnlyAtTxBoundary_BoundaryWritesAtomically(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.CheckpointOnlyAtTxBoundary = true

	ch := feed(false, insertAt("p1"), insertAt("p2"), txCommit("tc"))
	n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
	if err != nil {
		t.Fatalf("RunOneBatch: %v", err)
	}
	if n != 2 || lastPos.Token != "tc" || closed {
		t.Fatalf("n=%d lastPos=%q closed=%v; want n=2 lastPos=tc closed=false", n, lastPos.Token, closed)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "dispatch:p2", "writePosition:tc", "commit",
	})
}

// TestRunOneBatch_SchemaEventOutsideTx_FirstAppliesAlone pins the
// TransactionalDDL=false (MySQL) first-change shape: the event runs
// via ApplyOne with NO batch tx, and the AIMD observer does not fire
// (batchStart stays zero — these were never row-throughput signals).
func TestRunOneBatch_SchemaEventOutsideTx_FirstAppliesAlone(t *testing.T) {
	for name, ev := range schemaEvents("p1") {
		t.Run(name, func(t *testing.T) {
			rec := &recorder{}
			cfg := testConfig(t, rec, false)
			obs := &countingObserver{}
			cfg.BatchObserver = obs

			ch := feed(false, ev)
			n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
			if err != nil {
				t.Fatalf("RunOneBatch: %v", err)
			}
			if n != 1 || lastPos.Token != "p1" || closed {
				t.Fatalf("n=%d lastPos=%q closed=%v; want n=1 lastPos=p1 closed=false", n, lastPos.Token, closed)
			}
			assertEvents(t, rec, []string{"applyOne:p1"})
			if obs.calls.Load() != 0 {
				t.Errorf("ObserveBatch calls = %d; want 0 (schema-event batch of 1 is not observed)", obs.calls.Load())
			}
		})
	}
}

// TestRunOneBatch_SchemaEventOutsideTx_MidBatchFlushesFirst pins the
// TransactionalDDL=false (MySQL) mid-batch shape: the in-flight batch
// commits FIRST (the previous changes' position write lands before
// the DDL's implicit commit could destroy the tx), then the event
// applies alone via ApplyOne, and the cycle reports the event as its
// own batch of 1.
func TestRunOneBatch_SchemaEventOutsideTx_MidBatchFlushesFirst(t *testing.T) {
	for name, ev := range schemaEvents("p2") {
		t.Run(name, func(t *testing.T) {
			rec := &recorder{}
			cfg := testConfig(t, rec, false)

			ch := feed(false, insertAt("p1"), ev)
			n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
			if err != nil {
				t.Fatalf("RunOneBatch: %v", err)
			}
			if n != 1 || lastPos.Token != "p2" || closed {
				t.Fatalf("n=%d lastPos=%q closed=%v; want n=1 lastPos=p2 closed=false", n, lastPos.Token, closed)
			}
			assertEvents(t, rec, []string{
				"begin", "dispatch:p1", "writePosition:p1", "commit", "applyOne:p2",
			})
		})
	}
}

// TestRunOneBatch_TransactionalDDL_SchemaEventJoinsTx pins the
// TransactionalDDL=true (PG) shape for both first-change and
// mid-batch arrivals: the event dispatches onto the batch tx and the
// batch flushes immediately after, so the event's position write
// rides the SAME tx (ADR-0049 locked decision #4a). A SchemaSnapshot
// additionally fires the cache hook strictly AFTER the commit
// (Chunk C cache-after-commit invariant).
func TestRunOneBatch_TransactionalDDL_SchemaEventJoinsTx(t *testing.T) {
	for evName, ev := range schemaEvents("pX") {
		for _, mid := range []bool{false, true} {
			shape := "First"
			if mid {
				shape = "MidBatch"
			}
			t.Run(evName+"_"+shape, func(t *testing.T) {
				rec := &recorder{}
				cfg := testConfig(t, rec, true)
				cfg.CacheSchemaSnapshot = func(snap ir.SchemaSnapshot) { rec.add("cacheSnapshot:" + snap.Pos().Token) }

				var changes []ir.Change
				want := []string{"begin"}
				wantN := 1
				if mid {
					changes = append(changes, insertAt("p1"))
					want = append(want, "dispatch:p1")
					wantN = 2
				}
				changes = append(changes, ev)
				want = append(want, "dispatch:pX", "writePosition:pX", "commit")
				if evName == "SchemaSnapshot" {
					want = append(want, "cacheSnapshot:pX")
				}

				ch := feed(false, changes...)
				n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
				if err != nil {
					t.Fatalf("RunOneBatch: %v", err)
				}
				if n != wantN || lastPos.Token != "pX" || closed {
					t.Fatalf("n=%d lastPos=%q closed=%v; want n=%d lastPos=pX closed=false", n, lastPos.Token, closed, wantN)
				}
				assertEvents(t, rec, want)
			})
		}
	}
}

// TestRunOneBatch_DispatchErrorRollsBackAndClassifies pins the
// failure shape: a dispatch error rolls the tx back (no position
// write, no commit) and the returned error is routed through the
// engine's classifier.
func TestRunOneBatch_DispatchErrorRollsBackAndClassifies(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	boom := errors.New("boom")
	cfg.Dispatch = func(_ context.Context, _ BatchTx, _ string, c ir.Change) error {
		if c.Pos().Token == "p2" {
			return boom
		}
		rec.add("dispatch:" + c.Pos().Token)
		return nil
	}

	ch := feed(false, insertAt("p1"), insertAt("p2"))
	n, _, _, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
	if n != 0 {
		t.Fatalf("n = %d; want 0 on dispatch failure", n)
	}
	if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "classified: ") {
		t.Fatalf("err = %v; want the dispatch error routed through Classify", err)
	}
	assertEvents(t, rec, []string{"begin", "dispatch:p1", "tx.Rollback"})
}

// TestRunOneBatch_ByteCapFlushes pins the ADR-0028 byte-cap flush and
// the ADR-0052 DP-4(b) byte-cap-dominant advisory to the provider.
func TestRunOneBatch_ByteCapFlushes(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.ByteCap = 1 // any accumulated row bytes trip the cap
	prov := &hintingProvider{}
	cfg.BatchSizeProvider = prov

	ch := feed(false, insertAt("p1"), insertAt("p2"), insertAt("p3"))
	n, lastPos, closed, err := RunOneBatch(context.Background(), cfg, "stream", ch, 100)
	if err != nil {
		t.Fatalf("RunOneBatch: %v", err)
	}
	// The cap is checked after each SUBSEQUENT dispatch (the first
	// change always applies), so the flush lands at n=2.
	if n != 2 || lastPos.Token != "p2" || closed {
		t.Fatalf("n=%d lastPos=%q closed=%v; want n=2 lastPos=p2 closed=false", n, lastPos.Token, closed)
	}
	if prov.hintHits != 1 || prov.hintRows != 2 {
		t.Errorf("NoteByteCapDominant hits=%d rows=%d; want hits=1 rows=2", prov.hintHits, prov.hintRows)
	}
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "dispatch:p2", "writePosition:p2", "commit",
	})
}

// TestRunBatchLoop_ProviderClampsBatchSize pins the ADR-0052 outer-
// loop consult: the provider's target drives the per-batch row cap
// (clamped under the static maxBatchSize ceiling).
func TestRunBatchLoop_ProviderClampsBatchSize(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	prov := &hintingProvider{next: 1}
	cfg.BatchSizeProvider = prov

	ch := feed(true, insertAt("p1"), insertAt("p2"))
	if err := RunBatchLoop(context.Background(), cfg, "stream", ch, 10); err != nil {
		t.Fatalf("RunBatchLoop: %v", err)
	}
	// Provider says 1 → each insert commits as its own batch.
	assertEvents(t, rec, []string{
		"begin", "dispatch:p1", "writePosition:p1", "commit",
		"begin", "dispatch:p2", "writePosition:p2", "commit",
	})
	if prov.hits != 3 {
		t.Errorf("NextBatchSize hits = %d; want 3 (one per outer-loop iteration incl. the closing one)", prov.hits)
	}
}

// hintingProvider is a minimal ir.BatchSizeProvider that also exposes
// the optional NoteByteCapDominant advisory surface. Single-goroutine
// use only (the loop runs synchronously in these tests).
type hintingProvider struct {
	next     int
	hits     int
	hintHits int
	hintRows int
}

func (p *hintingProvider) NextBatchSize() int {
	p.hits++
	return p.next
}

func (p *hintingProvider) NoteByteCapDominant(_ context.Context, rows int, _, _ int64) {
	p.hintHits++
	p.hintRows = rows
}

// countingObserver is a race-safe ir.BatchObserver call counter.
type countingObserver struct{ calls atomic.Int64 }

func (o *countingObserver) ObserveBatch(context.Context, time.Duration, int, error) {
	o.calls.Add(1)
}
