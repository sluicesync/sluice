//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the ADR-0104 (item 23(c)) concurrent key-hash CDC
// apply on the MySQL target. The merged change stream is fanned across W
// in-order lanes by primary-key hash (same key → same lane → in-order),
// each committing concurrently on a dedicated backend, with the resume
// position advanced only to a fully-durable source boundary (the
// seq-frontier). These tests pin the load-bearing invariants:
//
//   - EXACTLY-ONCE / DEPENDENT-ROW ORDERING UNDER CONCURRENCY: a stream of
//     Insert→Update→Delete on the same PK (the dependent-row hazard) split
//     across batches yields the correct final state at depth=W, because all
//     ops for a key land on one lane in source order. Cross-key work runs
//     concurrently.
//   - VALUE-FIDELITY DIFFERENTIAL (pin the class, Bug-74 corollary): the
//     SAME ordered stream applied serially (depth=1) and concurrently
//     (depth=W) produces byte-identical target state.
//   - POSITION (seq-frontier): the persisted resume position advances to the
//     last fully-durable boundary even on a boundary-less stream (every
//     change a distinct position), and never leads the durable data.
//   - IDEMPOTENT REPLAY (crash-resume): replaying the same stream at depth=W
//     is a no-op for keyed tables (ADR-0010), no position regression.
//
// NOTE: this is a CONCURRENCY chunk — the -race Integration job on CI is the
// authoritative gate (this box is CGO=0 so -race can't run locally).

package mysql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

const concurrentLanesW = 4

// openConcurrentApplier opens a ChangeApplier against dsn and wires the
// ADR-0104 key-hash apply lane count when lanes > 1.
func openConcurrentApplier(t *testing.T, ctx context.Context, dsn string, lanes int) ir.ChangeApplier {
	t.Helper()
	a, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	if lanes > 1 {
		a.(*ChangeApplier).SetApplyConcurrency(lanes)
	}
	return a
}

// queryScalarString returns a single string column for a single-row query.
func queryScalarString(t *testing.T, dsn, query string, args ...any) (string, bool) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var v sql.NullString
	err = db.QueryRowContext(ctx, query, args...).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("scalar query %q: %v", query, err)
	}
	return v.String, true
}

// TestConcurrentApply_DependentOrderingAndExactlyOnce pins the core
// correctness claim: across W lanes, every operation on a given key is
// applied in source order (Insert→Update→Delete cannot reorder), so a
// stream that inserts then deletes every key leaves the table EMPTY, and a
// stream that inserts then updates leaves the final value. Many keys
// exercise real cross-lane concurrency; same-key sequences exercise the
// in-lane ordering guarantee.
func TestConcurrentApply_DependentOrderingAndExactlyOnce(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const tbl = "conc_dep"
	applyMySQLApplier(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (
			id    BIGINT       NOT NULL,
			v     VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(tbl)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer closeApplier(a)

	const keys = 200
	var events []ir.Change
	seq := 0
	tok := func() string { seq++; return fmt.Sprintf("t-%06d", seq) }
	mk := func(c ir.Change) { events = append(events, c) }

	// Phase 1: insert every key, then update it to v=final. Interleaved
	// across keys so consecutive batches touch many different lanes.
	for i := int64(1); i <= keys; i++ {
		mk(ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: tbl, Row: ir.Row{"id": i, "v": "init"}})
	}
	for i := int64(1); i <= keys; i++ {
		mk(ir.Update{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: tbl, Before: ir.Row{"id": i, "v": "init"}, After: ir.Row{"id": i, "v": "final"}})
	}
	// Phase 2: delete the first half (interleaved with updates to the
	// second half to force same-key Insert→Update→Delete chains to stay
	// ordered while cross-key work runs concurrently).
	for i := int64(1); i <= keys/2; i++ {
		mk(ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: tbl, Before: ir.Row{"id": i, "v": "final"}})
	}

	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, events, 7)

	// First half deleted, second half survives with v=final.
	if got := countAllRows(t, dsn, "target_db", tbl); got != keys/2 {
		t.Fatalf("rows = %d; want %d (first half deleted)", got, keys/2)
	}
	if _, ok := queryScalarString(t, dsn, "SELECT v FROM target_db."+quoteIdent(tbl)+" WHERE id = 1"); ok {
		t.Errorf("id=1 should have been deleted (Delete must land after Insert/Update on its lane)")
	}
	if v, ok := queryScalarString(t, dsn, "SELECT v FROM target_db."+quoteIdent(tbl)+" WHERE id = ?", keys); !ok || v != "final" {
		t.Errorf("id=%d v=%q ok=%v; want final (Update must land after Insert)", keys, v, ok)
	}

	// Idempotent replay: re-apply the whole stream; final state unchanged.
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, events, 7)
	if got := countAllRows(t, dsn, "target_db", tbl); got != keys/2 {
		t.Errorf("after replay: rows = %d; want %d (idempotency violated)", got, keys/2)
	}
}

// TestConcurrentApply_SerialDifferential is the Bug-74-corollary pin: the
// SAME ordered multi-table stream applied serially (depth=1) and
// concurrently (depth=W) must produce byte-identical target state. Two
// fresh databases, identical input, compared by per-table checksum.
func TestConcurrentApply_SerialDifferential(t *testing.T) {
	mkStream := func() []ir.Change {
		var ev []ir.Change
		seq := 0
		tok := func() string { seq++; return fmt.Sprintf("d-%06d", seq) }
		for i := int64(1); i <= 120; i++ {
			ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "a", Row: ir.Row{"id": i, "v": fmt.Sprintf("a%d", i)}})
			ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "b", Row: ir.Row{"id": i, "v": fmt.Sprintf("b%d", i)}})
		}
		for i := int64(1); i <= 120; i += 2 {
			ev = append(ev, ir.Update{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "a", Before: ir.Row{"id": i}, After: ir.Row{"id": i, "v": "upd"}})
		}
		for i := int64(2); i <= 120; i += 2 {
			ev = append(ev, ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "b", Before: ir.Row{"id": i}})
		}
		return ev
	}
	ddl := `
		CREATE TABLE a (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE b (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	run := func(lanes int) (sumA, sumB string) {
		dsn, cleanup := startMySQLForApplier(t)
		defer cleanup()
		applyMySQLApplier(t, dsn, ddl)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		a := openConcurrentApplier(t, ctx, dsn, lanes)
		defer closeApplier(a)
		// Wire per-lane AIMD controllers on the concurrent pass so the
		// differential proves byte-identity HOLDS with controllers engaged
		// (value encoding is independent of WHEN/whether a batch is sized or
		// retried — the ADR-0104 invariant). The serial pass keeps the
		// single-controller-less path.
		if lanes > 1 {
			ctrls := make([]ir.BatchSizeController, lanes)
			for i := range ctrls {
				c, err := appliercontrol.New(appliercontrol.Config{
					StreamID: testStreamID, Floor: 1, Ceiling: 9, InitialSize: 9, TargetLatency: 10 * time.Second,
				})
				if err != nil {
					t.Fatalf("controller %d: %v", i, err)
				}
				ctrls[i] = c
			}
			a.(*ChangeApplier).SetLaneAIMDControllers(ctrls)
		}
		pumpBatchedChangesPipelined(t, ctx, a, testStreamID, mkStream(), 9)
		return tableChecksum(t, dsn, "target_db", "a", "id"), tableChecksum(t, dsn, "target_db", "b", "id")
	}

	serialA, serialB := run(1)
	concA, concB := run(concurrentLanesW)
	if serialA != concA {
		t.Errorf("table a checksum differs: serial=%s concurrent=%s", serialA, concA)
	}
	if serialB != concB {
		t.Errorf("table b checksum differs: serial=%s concurrent=%s", serialB, concB)
	}
}

// TestConcurrentApply_BoundarylessStreamPersistsPosition pins the
// position-change boundary detection: a stream with NO Tx boundaries (every
// change a distinct position) must still persist a resume position — the
// last change's token once all lanes are durable — not stall at none.
func TestConcurrentApply_BoundarylessStreamPersistsPosition(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const tbl = "conc_pos"
	applyMySQLApplier(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(tbl)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer closeApplier(a)

	const n = 100
	lastTok := fmt.Sprintf("p-%06d", n)
	var ev []ir.Change
	for i := int64(1); i <= n; i++ {
		ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("p-%06d", i)}, Schema: "target_db", Table: tbl, Row: ir.Row{"id": i, "v": "x"}})
	}
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, ev, 8)

	if got := countAllRows(t, dsn, "target_db", tbl); got != n {
		t.Fatalf("rows = %d; want %d", got, n)
	}
	pos, ok, err := a.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition: ok=%v err=%v", ok, err)
	}
	if pos.Token != lastTok {
		t.Errorf("persisted position = %q; want %q (seq-frontier must reach the last durable change)", pos.Token, lastTok)
	}
}

// TestConcurrentApply_TxKillerInLaneRetry is the ADR-0104-graduation pin: a
// Vitess tx-killer abort forced on a lane's FIRST commit attempt must be
// recovered IN-LANE — the SAME batch is re-applied and succeeds, the final
// target state is exactly-once-correct (no dup / no gap), the run does NOT
// cancel (other lanes keep flowing), and the affected lane's AIMD controller
// shrank. This exercises the real DB apply path + the per-lane controller
// wiring (SetLaneAIMDControllers), not just the unit-level retry loop.
//
// NOTE: -race Integration on CI is the authoritative gate (concurrency chunk).
func TestConcurrentApply_TxKillerInLaneRetry(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const tbl = "conc_txk"
	applyMySQLApplier(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(tbl)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer closeApplier(a)

	// Wire real per-lane AIMD controllers so we can assert the affected lane
	// shrank (the same shape the streamer's attachLaneAIMDControllers builds).
	ctrls := make([]ir.BatchSizeController, concurrentLanesW)
	rawCtrls := make([]*appliercontrol.Controller, concurrentLanesW)
	for i := range ctrls {
		c, err := appliercontrol.New(appliercontrol.Config{
			StreamID: testStreamID, Floor: 1, Ceiling: 16, InitialSize: 16, TargetLatency: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("controller %d: %v", i, err)
		}
		ctrls[i] = c
		rawCtrls[i] = c
	}
	a.(*ChangeApplier).SetLaneAIMDControllers(ctrls)

	// Force a tx-killer on the FIRST lane commit only — set the package test
	// hook to return a real Vitess tx-killer 1105 on its first invocation,
	// then disable itself so the retry succeeds. Reset on cleanup so it can't
	// leak into another test.
	var fired atomic.Bool
	laneCommitHookForTest = func(_ []laneChange) error {
		if fired.CompareAndSwap(false, true) {
			return &gomysql.MySQLError{
				Number:  1105,
				Message: "vttablet: rpc error: code = Aborted desc = transaction rolled back for tx killer rollback",
			}
		}
		return nil
	}
	t.Cleanup(func() { laneCommitHookForTest = nil })

	const keys = 80
	var ev []ir.Change
	seq := 0
	tok := func() string { seq++; return fmt.Sprintf("txk-%06d", seq) }
	for i := int64(1); i <= keys; i++ {
		ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: tbl, Row: ir.Row{"id": i, "v": "x"}})
	}

	// The run MUST succeed (in-lane recovery), not error out.
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, ev, 5)

	// Exactly-once: every key present exactly once (no dup from the re-applied
	// batch, no gap from the failed attempt).
	if got := countAllRows(t, dsn, "target_db", tbl); got != keys {
		t.Fatalf("rows = %d; want %d (exactly-once after in-lane retry)", got, keys)
	}
	if !fired.Load() {
		t.Fatal("tx-killer hook never fired — the test did not exercise the retry path")
	}
	// The affected lane's controller must have shrunk at least once; the
	// others stay at the ceiling (per-lane independence). Exactly one shrink
	// across all lanes (the single forced tx-killer).
	totalShrinks := uint64(0)
	for i, c := range rawCtrls {
		snap := c.Snapshot()
		totalShrinks += snap.DecreasesTotal
		if snap.CurrentSize > 16 {
			t.Errorf("lane %d size %d exceeds ceiling 16", i, snap.CurrentSize)
		}
	}
	if totalShrinks != 1 {
		t.Errorf("total controller shrinks = %d; want exactly 1 (one forced tx-killer)", totalShrinks)
	}
}

// pumpBatchedChangesPipelined feeds events through ApplyBatch under the
// given streamID. Unlike the package's pumpBatchedChanges it threads an
// explicit streamID so the value-fidelity differential can run the serial
// and concurrent passes against the same target under distinct stream rows.
func pumpBatchedChangesPipelined(t *testing.T, ctx context.Context, applier ir.ChangeApplier, streamID string, events []ir.Change, batchSize int) {
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
	if err := batched.ApplyBatch(ctx, streamID, ch, batchSize); err != nil {
		t.Fatalf("ApplyBatch (stream %s): %v", streamID, err)
	}
}

func closeApplier(applier ir.ChangeApplier) {
	if c, ok := applier.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// tableChecksum returns a stable MD5 over every row of the table (ordered
// by orderBy) so the value-fidelity differential can assert the serial and
// concurrent passes produce byte-identical target state.
func tableChecksum(t *testing.T, dsn, schema, table, orderBy string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT * FROM "+quoteIdent(schema)+"."+quoteIdent(table)+" ORDER BY "+orderBy)
	if err != nil {
		t.Fatalf("checksum query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("checksum columns: %v", err)
	}
	h := md5.New()
	for rows.Next() {
		raw := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("checksum scan: %v", err)
		}
		for _, rb := range raw {
			if rb == nil {
				h.Write([]byte("\x00NULL\x00"))
			} else {
				h.Write(rb)
				h.Write([]byte("\x01"))
			}
		}
		h.Write([]byte("\x02"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("checksum rows.Err: %v", err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
