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

// multiRowFlushCounter records coalesced-flush observations for a test pass —
// both the ADR-0139 upsert-run flushes and the ADR-0140 delete-run flushes, so
// a pin can assert each coalescing kind actually engaged (a flush with > 1
// row/key) rather than silently falling back to serial per-row apply.
type multiRowFlushCounter struct {
	mu       sync.Mutex
	flushes  int
	multiRow int // upsert flushes that coalesced > 1 row
	maxRows  int

	delFlushes int
	delMulti   int // delete flushes that coalesced > 1 key
	delMaxKeys int
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

func (c *multiRowFlushCounter) observeDelete(keys int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delFlushes++
	if keys > 1 {
		c.delMulti++
	}
	if keys > c.delMaxKeys {
		c.delMaxKeys = keys
	}
}

// TestMultiRowApply_ValueFamilyDifferential is the Bug-74-corollary pin: the
// serial, single-lane-coalescing, and W=4 concurrent paths must produce
// byte-identical target state over the full value-family matrix, and the
// coalescing paths must actually take BOTH the multi-row upsert path (inserts +
// non-PK-changing updates as after-image upserts, ADR-0139/0140) and the
// coalesced DELETE … IN path (ADR-0140).
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
		multiRowDeleteFlushHookForTest = func(keys int) { ctr.observeDelete(keys) }
		defer func() {
			multiRowFlushHookForTest = nil
			multiRowDeleteFlushHookForTest = nil
		}()

		pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, batchSize)
		return tableChecksum(t, dsn, "target_db", "vf", "id"), ctr
	}

	// Serial reference: batchSize=1 routes through the per-change Apply path,
	// which does NOT use the coalescing handle — the oracle.
	serialSum, serialCtr := run(1, 1)
	if serialCtr.multiRow != 0 || serialCtr.delMulti != 0 {
		t.Errorf("serial pass should never coalesce; saw %d multi-row upserts, %d multi-key deletes", serialCtr.multiRow, serialCtr.delMulti)
	}

	batchSum, batchCtr := run(64, 1)
	if batchSum != serialSum {
		t.Errorf("single-lane batch checksum %s != serial %s (value fidelity broken)", batchSum, serialSum)
	}
	if batchCtr.multiRow == 0 {
		t.Errorf("single-lane batch pass never coalesced an upsert (maxRows=%d) — the multi-row INSERT path was not taken", batchCtr.maxRows)
	}
	if batchCtr.delMulti == 0 {
		t.Errorf("single-lane batch pass never coalesced a delete (delMaxKeys=%d) — the DELETE … IN path was not taken", batchCtr.delMaxKeys)
	}

	concSum, concCtr := run(64, concurrentLanesW)
	if concSum != serialSum {
		t.Errorf("W=%d concurrent checksum %s != serial %s (value fidelity broken)", concurrentLanesW, concSum, serialSum)
	}
	if concCtr.multiRow == 0 {
		t.Errorf("concurrent pass never coalesced an upsert (maxRows=%d) — the multi-row INSERT path was not taken", concCtr.maxRows)
	}
	if concCtr.delMulti == 0 {
		t.Errorf("concurrent pass never coalesced a delete (delMaxKeys=%d) — the DELETE … IN path was not taken", concCtr.delMaxKeys)
	}
}

// compositeFamilyPKDDL is a table whose COMPOSITE primary key spans value
// families — VARBINARY (embedded NUL/0xFF), DECIMAL-as-text, and VARCHAR — so
// the differential executes the row-value tuple form
// `(pk_bin, pk_dec, pk_txt) IN ((?,?,?), …)` and every PK-binding family against
// a real server (value-fidelity review, ADR-0140: the DELETE … IN builder is a
// new family-dispatched SQL shape; the unit test only checks its string).
const compositeFamilyPKDDL = `
	CREATE TABLE cpk (
		pk_bin  VARBINARY(16)   NOT NULL,
		pk_dec  DECIMAL(20,4)   NOT NULL,
		pk_txt  VARCHAR(40)     NOT NULL,
		v       BIGINT          NOT NULL,
		PRIMARY KEY (pk_bin, pk_dec, pk_txt)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// TestCoalesce_CompositeFamilyPK_DeleteIN_Differential pins the composite-PK
// `DELETE … WHERE (a,b,c) IN ((?,?,?), …)` path (ADR-0140) against real MySQL
// across binary/decimal/text PK families: a contiguous run of deletes (so the
// run coalesces — delMulti>0) applied serially (the oracle, full-before WHERE)
// and coalesced (single-lane + W=4) must leave byte-identical target state.
func TestCoalesce_CompositeFamilyPK_DeleteIN_Differential(t *testing.T) {
	const keys = 40
	pkRow := func(i int64) ir.Row {
		return ir.Row{
			"pk_bin": []byte{byte(i), 0x00, 0xff, byte(i * 7)},    // embedded NUL + 0xFF
			"pk_dec": fmt.Sprintf("%d.%04d", i*100+i, i*37%10000), // decimal-as-text
			"pk_txt": fmt.Sprintf("k-%04d", i),
			"v":      i * 1000,
		}
	}
	var stream []ir.Change
	seq := 0
	tok := func() string { seq++; return fmt.Sprintf("cpk-%06d", seq) }
	for i := int64(1); i <= keys; i++ {
		stream = append(stream, ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "cpk", Row: pkRow(i)})
	}
	// Contiguous run of deletes (full Before image — so the serial oracle's
	// full-before WHERE and the coalesced PK-tuple IN target the same rows).
	for i := int64(1); i <= keys/2; i++ {
		stream = append(stream, ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: "cpk", Before: pkRow(i)})
	}

	run := func(batchSize, lanes int) (string, *multiRowFlushCounter) {
		dsn, cleanup := startMySQLForApplier(t)
		defer cleanup()
		applyMySQLApplier(t, dsn, compositeFamilyPKDDL)

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		a := openConcurrentApplier(t, ctx, dsn, lanes)
		defer closeApplier(a)

		ctr := &multiRowFlushCounter{}
		multiRowDeleteFlushHookForTest = func(k int) { ctr.observeDelete(k) }
		defer func() { multiRowDeleteFlushHookForTest = nil }()

		pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, batchSize)
		if got := countAllRows(t, dsn, "target_db", "cpk"); got != keys/2 {
			t.Fatalf("rows = %d; want %d (half deleted)", got, keys/2)
		}
		return tableChecksum(t, dsn, "target_db", "cpk", "pk_txt"), ctr
	}

	serialSum, serialCtr := run(1, 1)
	if serialCtr.delMulti != 0 {
		t.Errorf("serial pass should never coalesce a delete; saw %d", serialCtr.delMulti)
	}
	batchSum, batchCtr := run(64, 1)
	if batchSum != serialSum {
		t.Errorf("single-lane checksum %s != serial %s (composite-PK DELETE … IN value fidelity broken)", batchSum, serialSum)
	}
	if batchCtr.delMulti == 0 {
		t.Errorf("single-lane pass never coalesced the composite-PK delete run (delMaxKeys=%d)", batchCtr.delMaxKeys)
	}
	concSum, concCtr := run(64, concurrentLanesW)
	if concSum != serialSum {
		t.Errorf("W=%d concurrent checksum %s != serial %s (composite-PK DELETE … IN value fidelity broken)", concurrentLanesW, concSum, serialSum)
	}
	if concCtr.delMulti == 0 {
		t.Errorf("concurrent pass never coalesced the composite-PK delete run (delMaxKeys=%d)", concCtr.delMaxKeys)
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

// reorderDDL is a minimal keyed table for the ADR-0140 ordering pins.
const reorderDDL = `
	CREATE TABLE reorder (
		id  BIGINT       NOT NULL,
		v   VARCHAR(64)  NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// TestCoalesce_DeleteThenUpdate_NoResurrection pins the load-bearing ADR-0140
// correctness claim: across the upsert↔delete kind switches the apply order is
// preserved, so (a) a key whose final op is a DELETE stays GONE — an earlier
// buffered upsert never resurrects it — and (b) a key DELETEd and then re-used
// (re-INSERT + UPDATE of the same PK) ends at the new value, never the stale
// pre-delete one. The coalescing target state must equal the serial oracle.
func TestCoalesce_DeleteThenUpdate_NoResurrection(t *testing.T) {
	ins := func(id int64, v string) ir.Change {
		return ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("i-%d-%s", id, v)}, Schema: "target_db", Table: "reorder", Row: ir.Row{"id": id, "v": v}}
	}
	upd := func(id int64, v string) ir.Change {
		return ir.Update{Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("u-%d-%s", id, v)}, Schema: "target_db", Table: "reorder", Before: ir.Row{"id": id}, After: ir.Row{"id": id, "v": v}}
	}
	del := func(id int64) ir.Change {
		return ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: fmt.Sprintf("d-%d", id)}, Schema: "target_db", Table: "reorder", Before: ir.Row{"id": id}}
	}
	stream := []ir.Change{
		ins(1, "A"), upd(1, "A2"), del(1), // key 1: final op delete -> GONE (no resurrection)
		ins(2, "B"),                                                    // key 2: present "B"
		del(3),                                                         // key 3: delete first (no-op on empty), then reuse
		ins(3, "C1"), upd(3, "C2"), del(3), ins(3, "C3"), upd(3, "C4"), // -> "C4"
	}

	apply := func(batchSize int) string {
		dsn, cleanup := startMySQLForApplier(t)
		defer cleanup()
		applyMySQLApplier(t, dsn, reorderDDL)
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		a := openConcurrentApplier(t, ctx, dsn, 1)
		defer closeApplier(a)
		pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, batchSize)

		if _, ok := queryScalarString(t, dsn, "SELECT v FROM `target_db`.`reorder` WHERE id = 1"); ok {
			t.Errorf("key 1 was resurrected — its final op is DELETE, so the row must be gone")
		}
		if v, ok := queryScalarString(t, dsn, "SELECT v FROM `target_db`.`reorder` WHERE id = 2"); !ok || v != "B" {
			t.Errorf("key 2 v = %q (ok=%v); want \"B\"", v, ok)
		}
		if v, ok := queryScalarString(t, dsn, "SELECT v FROM `target_db`.`reorder` WHERE id = 3"); !ok || v != "C4" {
			t.Errorf("key 3 v = %q (ok=%v); want \"C4\" (delete-then-reuse must land the new value)", v, ok)
		}
		return tableChecksum(t, dsn, "target_db", "reorder", "id")
	}

	serialSum := apply(1)
	coalescedSum := apply(64)
	if serialSum != coalescedSum {
		t.Errorf("coalesced checksum %s != serial %s (ordering across kind switches broken)", coalescedSum, serialSum)
	}
}

// TestCoalesce_PKChangingUpdate_SerialNoOrphan pins that a PK-changing UPDATE is
// NOT coalesced as an upsert (which would insert the after-image at the new PK
// and leave the old-PK row orphaned). It takes the serial full-before path: the
// old-PK row migrates to the new PK, leaving exactly one row and no orphan.
func TestCoalesce_PKChangingUpdate_SerialNoOrphan(t *testing.T) {
	stream := []ir.Change{
		ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: "i1"}, Schema: "target_db", Table: "reorder", Row: ir.Row{"id": int64(1), "v": "x"}},
		ir.Update{
			Position: ir.Position{Engine: engineNameMySQL, Token: "u1"}, Schema: "target_db", Table: "reorder",
			Before: ir.Row{"id": int64(1), "v": "x"}, After: ir.Row{"id": int64(2), "v": "x"},
		},
	}

	apply := func(batchSize int) string {
		dsn, cleanup := startMySQLForApplier(t)
		defer cleanup()
		applyMySQLApplier(t, dsn, reorderDDL)
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		a := openConcurrentApplier(t, ctx, dsn, 1)
		defer closeApplier(a)
		pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, batchSize)

		if got := countAllRows(t, dsn, "target_db", "reorder"); got != 1 {
			t.Errorf("after PK-changing update: rows = %d; want 1 (no orphaned old-PK row)", got)
		}
		if _, ok := queryScalarString(t, dsn, "SELECT v FROM `target_db`.`reorder` WHERE id = 1"); ok {
			t.Errorf("old-PK row id=1 still present — PK-changing update left an orphan")
		}
		if v, ok := queryScalarString(t, dsn, "SELECT v FROM `target_db`.`reorder` WHERE id = 2"); !ok || v != "x" {
			t.Errorf("new-PK row id=2 v = %q (ok=%v); want \"x\"", v, ok)
		}
		return tableChecksum(t, dsn, "target_db", "reorder", "id")
	}

	serialSum := apply(1)
	coalescedSum := apply(64)
	if serialSum != coalescedSum {
		t.Errorf("coalesced checksum %s != serial %s (PK-changing update mishandled)", coalescedSum, serialSum)
	}
}

// TestCoalesce_KeylessUpdateDelete_Serial pins that U/D on a keyless table (no
// PK to key on) stay on the serial full-before path — never the coalesced
// DELETE … IN — and still produce the correct final state.
func TestCoalesce_KeylessUpdateDelete_Serial(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, `
		CREATE TABLE keyless_ud (
			k  VARCHAR(8)   NOT NULL,
			v  VARCHAR(32)  NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, 1)
	defer closeApplier(a)

	var coalescedDeletes atomic.Int64
	multiRowDeleteFlushHookForTest = func(int) { coalescedDeletes.Add(1) }
	defer func() { multiRowDeleteFlushHookForTest = nil }()

	stream := []ir.Change{
		ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: "i-a"}, Schema: "target_db", Table: "keyless_ud", Row: ir.Row{"k": "a", "v": "1"}},
		ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: "i-b"}, Schema: "target_db", Table: "keyless_ud", Row: ir.Row{"k": "b", "v": "2"}},
		ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: "i-c"}, Schema: "target_db", Table: "keyless_ud", Row: ir.Row{"k": "c", "v": "3"}},
		ir.Update{Position: ir.Position{Engine: engineNameMySQL, Token: "u-a"}, Schema: "target_db", Table: "keyless_ud", Before: ir.Row{"k": "a", "v": "1"}, After: ir.Row{"k": "a", "v": "1-updated"}},
		ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: "d-b"}, Schema: "target_db", Table: "keyless_ud", Before: ir.Row{"k": "b", "v": "2"}},
	}
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, 64)

	if got := coalescedDeletes.Load(); got != 0 {
		t.Errorf("keyless deletes hit the coalesced DELETE … IN path %d times; want 0 (no PK to key on)", got)
	}
	if got := countAllRows(t, dsn, "target_db", "keyless_ud"); got != 2 {
		t.Errorf("after keyless U/D: rows = %d; want 2 (a updated, b deleted, c kept)", got)
	}
	if v, ok := queryScalarString(t, dsn, "SELECT v FROM `target_db`.`keyless_ud` WHERE k = 'a'"); !ok || v != "1-updated" {
		t.Errorf("keyless update: a.v = %q (ok=%v); want \"1-updated\"", v, ok)
	}
	if _, ok := queryScalarString(t, dsn, "SELECT v FROM `target_db`.`keyless_ud` WHERE k = 'b'"); ok {
		t.Errorf("keyless delete: row b still present; want deleted")
	}
}
