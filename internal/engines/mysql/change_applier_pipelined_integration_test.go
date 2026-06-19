//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the ADR-0104 Phase-1 pipelined CDC apply on the
// MySQL target. The pipeline overlaps cross-region commit RTTs across a
// bounded window of W in-flight transactions committed strictly in
// submission (source) order; these tests pin the load-bearing
// invariants the ADR names:
//
//   - VALUE-FIDELITY DIFFERENTIAL (pin the class, Bug-74 corollary): the
//     SAME ordered change stream applied through depth=1 (serial) and
//     depth=W (pipelined) produces BYTE-IDENTICAL target state across the
//     full type-family × shape matrix (JSON incl. the Bug-6 CAST(? AS
//     JSON) WHERE form, ENUM, SET, geometry, temporal µs, decimal
//     extremes, unicode, NULLs).
//   - ORDERING UNDER CONCURRENCY: dependent rows (INSERT→UPDATE→DELETE on
//     the same PK split across batch boundaries AND across the in-flight
//     window) yield the correct final state — out-of-order commit is
//     structurally impossible (single FIFO commit worker).
//   - KEYLESS DRAIN: a truly-keyless table stays clamped to the
//     --apply-batch-size=1 window even under depth=W (ADR-0089 guard).
//   - CRASH-RESUME IDEMPOTENCY: replaying the same stream under depth=W is
//     a no-op for keyed tables (ADR-0010), and the persisted position is
//     the highest contiguously-committed batch's token (never a gap).
//
// NOTE: this is a CONCURRENCY chunk — the -race Integration job on CI is
// the authoritative gate (this box is CGO=0 so -race can't run locally).

package mysql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

const pipelineDepthW = 4

// openPipelinedApplier opens a ChangeApplier against dsn and wires the
// ADR-0104 apply-pipeline depth when depth > 1. Mirrors the streamer's
// applyApplyPipelineDepth plumbing.
func openPipelinedApplier(t *testing.T, ctx context.Context, dsn string, depth int) ir.ChangeApplier {
	t.Helper()
	a, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	if depth > 1 {
		a.(ir.ApplyPipelineDepthSetter).SetApplyPipelineDepth(depth)
	}
	return a
}

// tableChecksum returns a stable digest of every row in schema.table:
// ordered by PK, every column rendered as text, concatenated and
// MD5'd. It is the differential oracle — two runs that produce
// byte-identical state produce the same checksum.
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

// matrixDDL is the type-family × shape table the value-fidelity
// differential exercises. One table covers every family the ADR names so
// the pin is the CLASS, not a representative.
func matrixDDL(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE %s (
			id        BIGINT        NOT NULL,
			j         JSON          NULL,
			e         ENUM('a','b','c') NULL,
			s         SET('x','y','z')  NULL,
			g         POINT         NULL,
			ts        DATETIME(6)   NULL,
			dec_lo    DECIMAL(38,10) NULL,
			dec_hi    DECIMAL(38,10) NULL,
			uni       VARCHAR(255)  NULL,
			nullable  VARCHAR(64)   NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(table))
}

// matrixEvents builds an ordered change stream over the matrix table that
// exercises every family across Insert / Update / Delete (so the Bug-6
// JSON WHERE form and the geometry/decimal/temporal/SET round-trips all
// run on both the value-bind AND the WHERE-predicate path). The stream is
// long enough to span many pipelined batches at the depth-W window.
func matrixEvents(table string) []ir.Change {
	mkRow := func(i int64, j string) ir.Row {
		// Raw WKB for POINT(1 2) — the value shape a CDC reader emits
		// (raw WKB bytes per docs/value-types.md). MySQL's geometry codec
		// path prepends the SRID in prepareValue.
		wkb := []byte{
			0x01,                   // little-endian
			0x01, 0x00, 0x00, 0x00, // wkbPoint
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // X=1.0
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Y=2.0
		}
		var nullable any // exercises NULL across the window
		if i%3 == 0 {
			nullable = nil
		} else {
			nullable = fmt.Sprintf("v%d", i)
		}
		return ir.Row{
			"id":       i,
			"j":        []byte(j),
			"e":        "b",                // ENUM as string
			"s":        []string{"x", "z"}, // SET as []string
			"g":        []byte(wkb),        // raw WKB; prepareValue prepends SRID
			"ts":       "2026-02-02 02:02:02.020202",
			"dec_lo":   "-9999999999999999999999999999.9999999999",
			"dec_hi":   "9999999999999999999999999999.9999999999",
			"uni":      fmt.Sprintf("héllo-世界-%d-🚀", i),
			"nullable": nullable,
		}
	}
	const n = 60
	events := make([]ir.Change, 0, n*2)
	for i := int64(1); i <= n; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("ins-%d", i)},
			Schema:   "target_db", Table: table,
			Row: mkRow(i, fmt.Sprintf(`{"k":%d,"nested":{"a":[1,2,3]}}`, i)),
		})
	}
	// Update every row's JSON, keyed on the PK + the JSON Before-image so
	// the Bug-6 CAST(? AS JSON) WHERE form runs on BOTH paths (a JSON
	// equality predicate that silently never matches would leave the row
	// stale identically on both, but the After-image checksum would then
	// differ from a correct serial run — the differential plus the
	// row-applied sanity check together pin the WHERE form). Geometry /
	// SET / decimal value-bind fidelity is covered by the Insert above;
	// keeping them out of the WHERE avoids a geometry-equality predicate
	// MySQL handles inconsistently across versions.
	for i := int64(1); i <= n; i++ {
		events = append(events, ir.Update{
			Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("upd-%d", i)},
			Schema:   "target_db", Table: table,
			Before: ir.Row{"id": i, "j": []byte(fmt.Sprintf(`{"k":%d,"nested":{"a":[1,2,3]}}`, i))},
			After:  ir.Row{"id": i, "j": []byte(fmt.Sprintf(`{"k":%d,"updated":true}`, i))},
		})
	}
	return events
}

// TestPipelined_ValueFidelityDifferential is the load-bearing pin: the
// SAME ordered change stream through depth=1 (serial) and depth=W
// (pipelined) yields BYTE-IDENTICAL target state across the type-family ×
// shape matrix. A divergence in the pipelined path's encoding — for ANY
// family — fails the checksum (the Bug-74 "pin the class" corollary,
// applied to an apply-path change).
func TestPipelined_ValueFidelityDifferential(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const serialTbl = "matrix_serial"
	const pipeTbl = "matrix_pipe"
	applyMySQLApplier(t, dsn, matrixDDL(serialTbl))
	applyMySQLApplier(t, dsn, matrixDDL(pipeTbl))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Serial (depth=1).
	serialApplier := openPipelinedApplier(t, ctx, dsn, 1)
	defer closeApplier(serialApplier)
	pumpBatchedChangesPipelined(t, ctx, serialApplier, "stream-serial", matrixEvents(serialTbl), 16)

	// Pipelined (depth=W).
	pipeApplier := openPipelinedApplier(t, ctx, dsn, pipelineDepthW)
	defer closeApplier(pipeApplier)
	pumpBatchedChangesPipelined(t, ctx, pipeApplier, "stream-pipe", matrixEvents(pipeTbl), 16)

	serialSum := tableChecksum(t, dsn, "target_db", serialTbl, "id")
	pipeSum := tableChecksum(t, dsn, "target_db", pipeTbl, "id")
	if serialSum != pipeSum {
		t.Fatalf("value-fidelity differential FAILED: serial checksum %s != pipelined checksum %s "+
			"(the pipelined path encoded a value differently from the serial path — ADR-0104 must change "+
			"only WHEN a tx commits, never HOW a value is encoded)", serialSum, pipeSum)
	}
	// Sanity: both actually applied the rows (a checksum match on two
	// empty tables would be a false green).
	if got := countAllRows(t, dsn, "target_db", pipeTbl); got != 60 {
		t.Fatalf("pipelined rows = %d; want 60", got)
	}
}

// TestPipelined_OrderingDependentRows pins strict submission-order commit:
// a long stream of INSERT→UPDATE→DELETE on the SAME set of PKs, split
// across many batches and across the in-flight window, must yield the
// correct final state. If the pipeline ever committed a batch out of
// order, a DELETE would land before its INSERT (or an UPDATE before its
// INSERT) and the final state would diverge.
func TestPipelined_OrderingDependentRows(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const tbl = "ordering_dep"
	applyMySQLApplier(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (
			id  BIGINT NOT NULL,
			v   BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(tbl)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	a := openPipelinedApplier(t, ctx, dsn, pipelineDepthW)
	defer closeApplier(a)

	// For each PK: INSERT v=1, UPDATE v=2, UPDATE v=3 (final), then for the
	// EVEN PKs an extra DELETE — so even PKs end absent, odd PKs end at v=3.
	// Interleave PKs so dependent rows for one PK land in DIFFERENT batches
	// and across the window.
	const nKeys = 50
	var events []ir.Change
	add := func(c ir.Change) { events = append(events, c) }
	for i := int64(1); i <= nKeys; i++ {
		add(ir.Insert{Position: ir.Position{Token: fmt.Sprintf("i%d", i)}, Schema: "target_db", Table: tbl, Row: ir.Row{"id": i, "v": int64(1)}})
	}
	for i := int64(1); i <= nKeys; i++ {
		add(ir.Update{Position: ir.Position{Token: fmt.Sprintf("u1-%d", i)}, Schema: "target_db", Table: tbl, Before: ir.Row{"id": i, "v": int64(1)}, After: ir.Row{"id": i, "v": int64(2)}})
	}
	for i := int64(1); i <= nKeys; i++ {
		add(ir.Update{Position: ir.Position{Token: fmt.Sprintf("u2-%d", i)}, Schema: "target_db", Table: tbl, Before: ir.Row{"id": i, "v": int64(2)}, After: ir.Row{"id": i, "v": int64(3)}})
	}
	for i := int64(2); i <= nKeys; i += 2 {
		add(ir.Delete{Position: ir.Position{Token: fmt.Sprintf("d%d", i)}, Schema: "target_db", Table: tbl, Before: ir.Row{"id": i, "v": int64(3)}})
	}

	pumpBatchedChangesPipelined(t, ctx, a, "stream-order", events, 7)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Odd PKs present at v=3; even PKs absent.
	for i := int64(1); i <= nKeys; i++ {
		var v sql.NullInt64
		err := db.QueryRowContext(ctx, "SELECT v FROM "+quoteIdent(tbl)+" WHERE id = ?", i).Scan(&v)
		if i%2 == 0 {
			if err != sql.ErrNoRows {
				t.Fatalf("PK %d (even) should be DELETEd; got v=%v err=%v (out-of-order commit would leave it present)", i, v, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("PK %d (odd) should be present; query err=%v", i, err)
		}
		if !v.Valid || v.Int64 != 3 {
			t.Fatalf("PK %d final v = %v; want 3 (an out-of-order UPDATE/INSERT would diverge)", i, v)
		}
	}
}

// TestPipelined_KeylessDrainStaysSingleRow pins the ADR-0089 keyless guard
// under depth=W: a truly-keyless table must still commit one change per
// transaction (its at-least-once replay window stays the
// --apply-batch-size=1 baseline; the pipeline never widens it). Asserted
// via a BatchObserver — on the pipelined path the observer is the commit
// worker's per-tx observation, so every observed batch must be exactly 1
// row for the keyless table.
func TestPipelined_KeylessDrainStaysSingleRow(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const tbl = "keyless_pipe"
	applyMySQLApplier(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (
			kind    VARCHAR(32) NOT NULL,
			payload VARCHAR(255)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(tbl)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	a := openPipelinedApplier(t, ctx, dsn, pipelineDepthW)
	defer closeApplier(a)

	rec := &batchSizeRecorder{}
	a.(ir.BatchObserverSetter).SetBatchObserver(rec)

	const totalRows = 30
	events := make([]ir.Change, 0, totalRows)
	for i := int64(1); i <= totalRows; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Token: fmt.Sprintf("k%d", i)},
			Schema:   "target_db", Table: tbl,
			Row: ir.Row{"kind": "k", "payload": fmt.Sprintf("p%d", i)},
		})
	}
	pumpBatchedChangesPipelined(t, ctx, a, "stream-keyless", events, 1000)

	if got := rec.maxRows(); got != 1 {
		t.Errorf("keyless under depth=W: max observed batch rows = %d; want 1 "+
			"(the pipeline must drain to a single-row commit for keyless tables — ADR-0089)", got)
	}
	if got := countAllRows(t, dsn, "target_db", tbl); got != totalRows {
		t.Errorf("keyless rows = %d; want %d", got, totalRows)
	}
}

// TestPipelined_IdempotentReplayAndPosition pins crash-resume safety: a
// keyed stream applied under depth=W and then REPLAYED (the warm-resume
// shape: the window grows 1→W on resume, ADR-0010 idempotent upsert
// absorbs the overlap) leaves the row count unchanged and the persisted
// position at the last applied change's token (highest
// contiguously-committed — never a gap, since strict-order commit means
// batch i is durable before i+1 commits).
func TestPipelined_IdempotentReplayAndPosition(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const tbl = "replay_pipe"
	applyMySQLApplier(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (
			id    BIGINT       NOT NULL,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(tbl)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	a := openPipelinedApplier(t, ctx, dsn, pipelineDepthW)
	defer closeApplier(a)

	const totalRows = 80
	const lastToken = "row-80"
	events := make([]ir.Change, 0, totalRows)
	for i := int64(1); i <= totalRows; i++ {
		tok := fmt.Sprintf("row-%d", i)
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNameMySQL, Token: tok},
			Schema:   "target_db", Table: tbl,
			Row: ir.Row{"id": i, "email": fmt.Sprintf("u%d@x", i)},
		})
	}

	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, events, 9)
	if got := countAllRows(t, dsn, "target_db", tbl); got != totalRows {
		t.Fatalf("after first pipelined apply: rows = %d; want %d", got, totalRows)
	}
	pos, ok, err := a.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition after first apply: ok=%v err=%v", ok, err)
	}
	if pos.Token != lastToken {
		t.Fatalf("position token after first apply = %q; want %q (last contiguously-committed batch)", pos.Token, lastToken)
	}

	// Replay the SAME stream (warm-resume overlap). Idempotent upsert must
	// absorb it: no duplicate rows, no position regression.
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, events, 9)
	if got := countAllRows(t, dsn, "target_db", tbl); got != totalRows {
		t.Errorf("after replay under depth=W: rows = %d; want %d (idempotency violated)", got, totalRows)
	}
	pos2, ok, err := a.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition after replay: ok=%v err=%v", ok, err)
	}
	if pos2.Token != lastToken {
		t.Errorf("position token after replay = %q; want %q (no regression)", pos2.Token, lastToken)
	}
}

// pumpBatchedChangesPipelined feeds events through ApplyBatch under the
// given streamID. Unlike the package's pumpBatchedChanges it threads an
// explicit streamID so the value-fidelity differential can run the serial
// and pipelined passes against the same target under distinct stream rows.
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
