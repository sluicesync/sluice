//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for ADR-0139 multi-row INSERT coalescing on the MySQL
// target. The load-bearing claims:
//
//   - VALUE-FIDELITY DIFFERENTIAL (pin the class, Bug-74 corollary): the SAME
//     ordered mixed insert/update/delete stream over the full value-family
//     matrix (decimals, JSON, bool→TINYINT(1), BLOB, microsecond timestamps,
//     big-ints > 2^53) applied via the serial per-change path (batchSize=1),
//     the single-lane COALESCING batch path (large batchSize), and the W=4
//     concurrent lane path must all produce BYTE-IDENTICAL target state. The
//     coalesced multi-row INSERT binds every value to a `?` through the same
//     prepareApplierValue codec as the single-row path, so the differential is
//     the oracle that "only WHEN, never HOW" holds.
//   - THE MULTI-ROW PATH IS ACTUALLY TAKEN: the multiRowFlushHookForTest counter
//     observes at least one coalesced flush with rows > 1 on both the batch and
//     concurrent passes — so a silent regression to per-row apply fails loudly.
//   - IDEMPOTENT REPLAY (ADR-0010): re-applying the whole stream is a no-op.
//   - KEYLESS NOT COALESCED (ADR-0089): a keyless table still commits one row
//     per tx even through the coalescing path.
//
// NOTE: the concurrent lane path is a CONCURRENCY chunk — the -race Integration
// job on CI is the authoritative gate (this box is CGO=0 so -race can't run
// locally).

package mysql

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// valueFamilyDDL is a table exercising every value family that has a distinct
// MySQL wire/codec path, so the differential pins the class, not one
// representative type.
const valueFamilyDDL = `
	CREATE TABLE vf (
		id     BIGINT          NOT NULL,
		decv    DECIMAL(30,8)   NULL,
		js     JSON            NULL,
		flag   TINYINT(1)      NULL,
		blb   VARBINARY(64)   NULL,
		ts     TIMESTAMP(6)    NULL,
		big    BIGINT          NULL,
		txt    VARCHAR(64)     NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// mkValueFamilyStream builds a mixed insert/update/delete workload over the
// value-family table. Consecutive inserts (interleaved across keys, same shape)
// give the coalescer long runs; the updates/deletes are the flush boundaries.
func mkValueFamilyStream() []ir.Change {
	var ev []ir.Change
	seq := 0
	tok := func() string { seq++; return fmt.Sprintf("vf-%06d", seq) }

	// microsecond-precision timestamp string (MySQL TIMESTAMP(6) wire form).
	ts := func(i int64) string { return fmt.Sprintf("2026-06-28 12:00:%02d.%06d", i%60, i*7%1000000) }
	insRow := func(i int64) ir.Row {
		return ir.Row{
			"id":   i,
			"decv": fmt.Sprintf("%d.%08d", i*1000+i, i*97%100000000), // wide decimal, exact text
			"js":   fmt.Sprintf(`{"k": %d, "s": "v%d"}`, i, i),
			"flag": i%2 == 0, // bool → TINYINT(1)
			"blb":  []byte{byte(i), 0x00, 0xff, byte(i * 3)},
			"ts":   ts(i),
			"big":  int64(1)<<53 + i, // big-int > 2^53 (JS-unsafe range)
			"txt":  fmt.Sprintf("row-%d", i),
		}
	}

	const keys = 60
	// Phase 1: insert every key (one long coalescable run).
	for i := int64(1); i <= keys; i++ {
		ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "vf", Row: insRow(i)})
	}
	// Phase 2: update half (each Update is a flush boundary; the deletes too).
	for i := int64(1); i <= keys; i += 2 {
		ev = append(ev, ir.Update{
			Position: ir.Position{Engine: engineNameMySQL, Token: tok()},
			Schema:   "target_db", Table: "vf",
			Before: ir.Row{"id": i},
			After: ir.Row{
				"id": i, "decv": "0.00000001", "js": `{"k": -1}`, "flag": true,
				"blb": []byte{0x01, 0x02}, "ts": "2026-06-28 23:59:59.999999",
				"big": int64(1)<<60 + i, "txt": "updated",
			},
		})
	}
	// Phase 3: delete every third key.
	for i := int64(3); i <= keys; i += 3 {
		ev = append(ev, ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "vf", Before: ir.Row{"id": i}})
	}
	// Phase 4: a fresh insert run after the non-insert boundaries (exercises a
	// new coalesced run starting mid-stream).
	for i := int64(keys + 1); i <= keys+20; i++ {
		ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "vf", Row: insRow(i)})
	}
	// Phase 5: a contiguous coalescable run that MIXES NULL and non-NULL in every
	// nullable column (the Bug-74 NULL shape). Each row carries the SAME 8 keys —
	// the NULL rows are present-with-nil, NOT absent, so NonGeneratedRowKeys keeps
	// the identical column set and the run coalesces into one multi-row statement
	// rather than flushing on a shape change. This binds NULL inside a coalesced
	// group, and across rows of one statement each nullable column is NULL in some
	// rows and non-NULL in others — the gap a per-representative (all-non-null)
	// pin would miss (value-fidelity review, ADR-0139).
	insRowNull := func(i int64) ir.Row {
		return ir.Row{
			"id": i, "decv": nil, "js": nil, "flag": nil,
			"blb": nil, "ts": nil, "big": nil, "txt": fmt.Sprintf("n-%d", i),
		}
	}
	for i := int64(keys + 21); i <= keys+40; i++ {
		row := insRow(i)
		if i%2 == 0 {
			row = insRowNull(i)
		}
		ev = append(ev, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "vf", Row: row})
	}
	return ev
}

// multiRowFlushCounter records coalesced-flush observations for a test pass.
type multiRowFlushCounter struct {
	mu       sync.Mutex
	flushes  int
	multiRow int // flushes that coalesced > 1 row
	maxRows  int
}

func (c *multiRowFlushCounter) observe(rows int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushes++
	if rows > 1 {
		c.multiRow++
	}
	if rows > c.maxRows {
		c.maxRows = rows
	}
}

// TestMultiRowApply_ValueFamilyDifferential is the Bug-74-corollary pin: the
// serial, single-lane-coalescing, and W=4 concurrent paths must produce
// byte-identical target state over the full value-family matrix, and the
// coalescing paths must actually take the multi-row path.
func TestMultiRowApply_ValueFamilyDifferential(t *testing.T) {
	stream := mkValueFamilyStream()

	// run applies the stream to a fresh DB at the given batch size / lane count
	// and returns the per-table checksum plus the flush observations.
	run := func(batchSize, lanes int) (string, *multiRowFlushCounter) {
		dsn, cleanup := startMySQLForApplier(t)
		defer cleanup()
		applyMySQLApplier(t, dsn, valueFamilyDDL)

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		a := openConcurrentApplier(t, ctx, dsn, lanes)
		defer closeApplier(a)

		ctr := &multiRowFlushCounter{}
		multiRowFlushHookForTest = func(rows int) { ctr.observe(rows) }
		defer func() { multiRowFlushHookForTest = nil }()

		pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, batchSize)
		return tableChecksum(t, dsn, "target_db", "vf", "id"), ctr
	}

	// Serial reference: batchSize=1 routes through the per-change Apply path,
	// which does NOT use the coalescing handle — the oracle.
	serialSum, serialCtr := run(1, 1)
	if serialCtr.multiRow != 0 {
		t.Errorf("serial pass should never coalesce; saw %d multi-row flushes", serialCtr.multiRow)
	}

	batchSum, batchCtr := run(64, 1)
	if batchSum != serialSum {
		t.Errorf("single-lane batch checksum %s != serial %s (value fidelity broken)", batchSum, serialSum)
	}
	if batchCtr.multiRow == 0 {
		t.Errorf("single-lane batch pass never coalesced (maxRows=%d) — the multi-row path was not taken", batchCtr.maxRows)
	}

	concSum, concCtr := run(64, concurrentLanesW)
	if concSum != serialSum {
		t.Errorf("W=%d concurrent checksum %s != serial %s (value fidelity broken)", concurrentLanesW, concSum, serialSum)
	}
	if concCtr.multiRow == 0 {
		t.Errorf("concurrent pass never coalesced (maxRows=%d) — the multi-row path was not taken", concCtr.maxRows)
	}
}

// TestMultiRowApply_IdempotentReplay confirms the coalescing batch path stays
// idempotent (ADR-0010): replaying the whole stream is a no-op for the final
// state, even though inserts are now packed into multi-row statements.
func TestMultiRowApply_IdempotentReplay(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, valueFamilyDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, 1)
	defer closeApplier(a)

	stream := mkValueFamilyStream()
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, 64)
	sum1 := tableChecksum(t, dsn, "target_db", "vf", "id")

	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, 64)
	sum2 := tableChecksum(t, dsn, "target_db", "vf", "id")
	if sum1 != sum2 {
		t.Errorf("checksum changed on replay (%s -> %s); idempotency violated", sum1, sum2)
	}
}

// TestMultiRowApply_KeylessNotCoalesced pins ADR-0089 through the coalescing
// path: a truly-keyless table must still commit one row per tx (the guard
// flushes the run and applies single-row), so the coalescer never groups a
// keyless insert.
func TestMultiRowApply_KeylessNotCoalesced(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, `
		CREATE TABLE events_log (
			kind    VARCHAR(32)  NOT NULL,
			payload VARCHAR(255)
		);`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, 1)
	defer closeApplier(a)

	rec := &batchSizeRecorder{}
	a.(ir.BatchObserverSetter).SetBatchObserver(rec)

	var coalesced atomic.Int64
	multiRowFlushHookForTest = func(rows int) {
		if rows > 1 {
			coalesced.Add(1)
		}
	}
	defer func() { multiRowFlushHookForTest = nil }()

	const totalRows = 40
	events := make([]ir.Change, 0, totalRows)
	for i := int64(1); i <= totalRows; i++ {
		events = append(events, ir.Insert{
			Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("k-%d", i)},
			Schema:   "target_db", Table: "events_log",
			Row: ir.Row{"kind": "k", "payload": fmt.Sprintf("p%d", i)},
		})
	}
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, events, 1000)

	if got := rec.maxRows(); got != 1 {
		t.Errorf("max batch rows = %d; want 1 (keyless must NOT batch/coalesce — ADR-0089)", got)
	}
	if got := coalesced.Load(); got != 0 {
		t.Errorf("keyless inserts were coalesced %d times; want 0 (ADR-0089)", got)
	}
	if got := countAllRows(t, dsn, "target_db", "events_log"); got != totalRows {
		t.Errorf("after keyless apply: rows = %d; want %d", got, totalRows)
	}
}
