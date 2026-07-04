//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the WRITE-side parallel fan-out on the
// idempotent cold-start COPY path (ADR-0097). These boot a real MySQL
// container and drive WriteRowsIdempotentParallel across N worker
// channels against the real target, asserting:
//
//   - exactly-once landing: target COUNT(*) and a content checksum
//     match the source EXACTLY (no missing/dup rows), INCLUDING when
//     the input re-emits PKs (the Bug-125 VStream COPY catchup shape)
//     — the idempotent upsert must absorb the re-emissions across
//     workers;
//   - a forced worker error fails the copy LOUDLY (non-nil return).
//
// The READ side here is a hand-built partition over an in-test row
// generator — the same PK-hash partition the pipeline applies — so the
// test exercises the engine's N-worker execution + the real
// ON DUPLICATE KEY UPDATE wire path under concurrency. A true vtgate
// VStream container is impractical in the default integration shard;
// the re-emission timing against a real vtgate is covered by the
// `vstream` / `vitesscluster` engine suites (cdc_vstream_bug125_*).
//
// To run:
//   go test -tags=integration ./internal/engines/mysql/ -run Fanout

package mysql

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func fanoutTestTable() *ir.Table {
	return &ir.Table{
		Name: "fanout_samples",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// fanoutPartition routes rows to `degree` channels by PK hash — the
// engine-side mirror of the pipeline's partitionRowsByPK (kept local so
// this engine test doesn't import the pipeline package). Same FNV-1a /
// NUL-separated encoding contract.
func fanoutPartition(rows []ir.Row, degree int) []<-chan ir.Row {
	chans := make([]chan ir.Row, degree)
	out := make([]<-chan ir.Row, degree)
	for i := range chans {
		chans[i] = make(chan ir.Row, 64)
		out[i] = chans[i]
	}
	go func() {
		defer func() {
			for _, ch := range chans {
				close(ch)
			}
		}()
		for _, r := range rows {
			h := fnv.New64a()
			fmt.Fprintf(h, "%v\x00", r["id"])
			chans[int(h.Sum64()%uint64(degree))] <- r
		}
	}()
	return out
}

func TestRowWriter_Fanout_ExactlyOnceWithReemissions(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		DROP TABLE IF EXISTS fanout_samples;
		CREATE TABLE fanout_samples (
			id BIGINT NOT NULL,
			v  VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	const n = 5000
	rows := make([]ir.Row, 0, n+500)
	for id := 0; id < n; id++ {
		rows = append(rows, ir.Row{"id": int64(id), "v": fmt.Sprintf("v%d", id)})
	}
	// Re-emit a subset of PKs (the Bug-125 VStream COPY catchup shape):
	// the FINAL value for each re-emitted PK must win (upsert), and the
	// row count must stay exactly n (no duplicate rows).
	for id := 0; id < 500; id++ {
		rows = append(rows, ir.Row{"id": int64(id), "v": fmt.Sprintf("v%d-final", id)})
	}

	const degree = 4
	workers := fanoutPartition(rows, degree)

	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	if err := w.WriteRowsIdempotentParallel(ctx, fanoutTestTable(), workers); err != nil {
		t.Fatalf("WriteRowsIdempotentParallel: %v", err)
	}

	// Exactly-once: COUNT(*) == n despite the re-emissions.
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM fanout_samples").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != n {
		t.Fatalf("target COUNT(*) = %d; want exactly %d (no missing/dup rows)", count, n)
	}

	// Content checksum: every re-emitted PK carries its FINAL value, and
	// every non-re-emitted PK its original — assert a couple of each plus
	// a full checksum so a wrong-value-won regression is caught.
	var v string
	if err := db.QueryRowContext(ctx, "SELECT v FROM fanout_samples WHERE id=0").Scan(&v); err != nil {
		t.Fatalf("read id=0: %v", err)
	}
	if v != "v0-final" {
		t.Fatalf("re-emitted PK 0 has v=%q; want the final upsert value v0-final", v)
	}
	if err := db.QueryRowContext(ctx, "SELECT v FROM fanout_samples WHERE id=4999").Scan(&v); err != nil {
		t.Fatalf("read id=4999: %v", err)
	}
	if v != "v4999" {
		t.Fatalf("PK 4999 has v=%q; want v4999", v)
	}

	// Full content checksum over the whole table: an order-independent
	// SUM of per-row CRC32(id:v). Computed on the target via MySQL's
	// CRC32 and recomputed in Go via the identical IEEE polynomial, so a
	// wrong-value-won or a missing/extra-row regression anywhere in the
	// table (not just the spot-checked PKs) is caught.
	var targetSum int64
	if err := db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(CRC32(CONCAT(id, ':', v))), 0) FROM fanout_samples").Scan(&targetSum); err != nil {
		t.Fatalf("checksum: %v", err)
	}
	final := map[int64]string{}
	for _, r := range rows {
		final[r["id"].(int64)] = r["v"].(string)
	}
	var wantSum int64
	for id, val := range final {
		wantSum += int64(crc32.ChecksumIEEE([]byte(fmt.Sprintf("%d:%s", id, val))))
	}
	if targetSum != wantSum {
		t.Fatalf("content checksum mismatch: target=%d want=%d", targetSum, wantSum)
	}
}

// TestRowWriter_Fanout_MidCopyDurableCheckpointDisabled is the Bug-1
// silent-loss-on-resume pin (ADR-0097 §3). Under fan-out the mid-COPY
// durable-progress watermark (ADR-0072 Phase B) MUST stay inert: the flat
// flushed-row count is not order-equivalent to the snapshot reader's
// enqueue-order breadcrumb frontier, so a mid-COPY breadcrumb could
// checkpoint past an early-enqueued row a LAGGING worker has not yet
// flushed — a crash after that checkpoint would resume PAST the un-flushed
// row (silent loss).
//
// This pins the guarantee at its tightest point: a recording
// copyDurableProgress is wired on the writer (exactly as the pipeline
// wires the snapshot reader's AdvanceDurableRows on the cold-start path),
// then a fan-out copy is driven with worker 0 made to LAG — its
// early-enqueued rows are dripped in slowly while the other workers drain
// and flush their later-enqueued rows immediately. If the watermark were
// live, the fast workers' flushes would advance it ahead of worker 0's
// un-flushed frontier. The assertion is that the callback fires ZERO times
// for the whole fan-out copy (the watermark is provably inert), so no
// mid-COPY breadcrumb can ever be persisted past the slowest worker.
func TestRowWriter_Fanout_MidCopyDurableCheckpointDisabled(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		DROP TABLE IF EXISTS fanout_durable;
		CREATE TABLE fanout_durable (
			id BIGINT NOT NULL,
			v  VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	const degree = 3
	chans := make([]chan ir.Row, degree)
	workers := make([]<-chan ir.Row, degree)
	for i := range chans {
		chans[i] = make(chan ir.Row, 256)
		workers[i] = chans[i]
	}

	// Worker 0 is the LAGGING worker: it gets EARLY-enqueued rows (the ones
	// whose durability a mid-COPY breadcrumb would claim first) but they are
	// dripped in slowly. Workers 1..N get LATER-enqueued rows delivered
	// immediately so they flush ahead. If the watermark advanced on the fast
	// workers' flushes, it would cross worker 0's un-flushed frontier — the
	// exact silent-loss precondition.
	const lagRows = 600
	const fastRowsPerWorker = 2000
	go func() {
		defer func() {
			for _, ch := range chans {
				close(ch)
			}
		}()
		// Fill the fast workers up front so they flush many batches while
		// worker 0 still lags.
		for w := 1; w < degree; w++ {
			for i := 0; i < fastRowsPerWorker; i++ {
				id := int64(1_000_000 + w*1_000_000 + i)
				chans[w] <- ir.Row{"id": id, "v": fmt.Sprintf("fast%d", id)}
			}
		}
		// Now drip the lagging worker's early rows in slowly.
		for i := 0; i < lagRows; i++ {
			chans[0] <- ir.Row{"id": int64(i), "v": fmt.Sprintf("lag%d", i)}
			time.Sleep(time.Millisecond)
		}
	}()

	var durableCalls atomic.Int64
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	// Wire the watermark exactly as the pipeline does on the cold-start
	// path. Under fan-out it MUST never be invoked.
	w.SetCopyDurableProgress(func(int64) { durableCalls.Add(1) })

	table := &ir.Table{
		Name: "fanout_durable",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	if err := w.WriteRowsIdempotentParallel(ctx, table, workers); err != nil {
		t.Fatalf("WriteRowsIdempotentParallel: %v", err)
	}

	if n := durableCalls.Load(); n != 0 {
		t.Fatalf("copyDurableProgress invoked %d times on the fan-out path; "+
			"want 0 (mid-COPY durable watermark MUST be disabled under fan-out — "+
			"a fast worker's flush could otherwise checkpoint past the lagging "+
			"worker's un-flushed early rows, silent-loss-on-resume; ADR-0097 §3)", n)
	}

	// Sanity: all rows landed (the fan-out copy itself is correct).
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM fanout_durable").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if want := lagRows + (degree-1)*fastRowsPerWorker; count != want {
		t.Fatalf("target COUNT(*) = %d; want %d", count, want)
	}
}

// erroringRow is a channel that yields one bad row whose value violates
// the column (a value the driver rejects), forcing a worker error.
func TestRowWriter_Fanout_WorkerErrorFailsLoudly(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		DROP TABLE IF EXISTS fanout_err;
		CREATE TABLE fanout_err (
			id BIGINT NOT NULL,
			v  INT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	table := &ir.Table{
		Name: "fanout_err",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Integer{Width: 32}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	// One worker gets a row whose `v` is a value the INT column rejects
	// under strict sql_mode (a non-numeric string) → the upsert errors,
	// the worker fails, and the whole parallel write must return non-nil.
	good := make(chan ir.Row, 2)
	good <- ir.Row{"id": int64(1), "v": int64(10)}
	close(good)
	bad := make(chan ir.Row, 1)
	bad <- ir.Row{"id": int64(2), "v": "not-an-int"}
	close(bad)

	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	err = w.WriteRowsIdempotentParallel(ctx, table, []<-chan ir.Row{good, bad})
	if err == nil {
		t.Fatal("WriteRowsIdempotentParallel with a failing worker: err=nil; want a loud failure")
	}
	if !errors.Is(err, context.Canceled) && err.Error() == "" {
		t.Fatalf("unexpected empty error: %v", err)
	}
}
