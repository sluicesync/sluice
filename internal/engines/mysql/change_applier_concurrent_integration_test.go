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
	"database/sql"
	"fmt"
	"testing"
	"time"

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
