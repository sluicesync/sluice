//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-table VStream cold-copy WRITE concurrency (ADR-0100) — the
// FULL-pipeline proof that W = K tables are WRITTEN concurrently, against a
// real MySQL target. ADR-0099 made the READ side concurrent (K independent
// VStreams over disjoint table groups), but the serial bulk-copy consumer
// still drained one table at a time, so live measurement held at ~1.4× and
// the target PROCESSLIST showed exactly ONE table receiving rows. ADR-0100
// turns the K read streams into K read→write pipelines (one consumer per
// disjoint group), so up to W tables are written at once.
//
// This test seeds a multi-table keyspace, opens the snapshot with K > 1, runs
// runBulkCopyWithOpts (the exact cold-copy consumer) into a real MySQL
// target, and:
//   - polls the target's PER-TABLE row counts while the copy runs and asserts
//     ≥2 tables are advancing CONCURRENTLY in an overlapping window (the
//     thing the PROCESSLIST showed missing — one at a time);
//   - asserts every table's final target COUNT(*) == source (no gap/dup).
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestMigrate_VStreamWriteConcurrency' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestMigrate_VStreamWriteConcurrency_TablesWrittenConcurrently is the
// load-bearing ADR-0100 integration pin: it proves the WRITE side is
// concurrent across tables (W = K), not just the read side (ADR-0099).
func TestMigrate_VStreamWriteConcurrency_TablesWrittenConcurrently(t *testing.T) {
	const keyspace = "warehouse"
	// Single-shard vttestserver is enough: the lever is cross-TABLE
	// concurrency, independent of shard count.
	mysqlDSN, grpcEndpoint, _, vtCleanup := startShardedVTTestServer(t, keyspace, 1)
	defer vtCleanup()

	tables := []string{"wc_a", "wc_b", "wc_c", "wc_d"}
	// Distinct, sizable per-table row counts so (a) the copy lasts long
	// enough to observe overlap, and (b) a mis-routed/dropped table shows as
	// a count mismatch, not a coincidental match.
	rowsPer := map[string]int{"wc_a": 4000, "wc_b": 6000, "wc_c": 3000, "wc_d": 5000}
	for _, tbl := range tables {
		applySQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (
			id   BIGINT        NOT NULL,
			body VARCHAR(2048) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tbl))
		seedWideTable(t, mysqlDSN, tbl, rowsPer[tbl])
	}
	time.Sleep(3 * time.Second) // let vreplication settle

	targetDSN, tgtCleanup := startMySQLTarget(t)
	defer tgtCleanup()

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("source engine \"planetscale\" not registered")
	}
	tgtEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("target engine \"mysql\" not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	// K = 4 concurrent COPY streams → W = 4 concurrent consumer pipelines.
	sluiceSrcDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_copy_table_parallelism=4",
		mysqlDSN, grpcEndpoint,
	)

	// Read + shape the source schema via the engine.
	sr, err := srcEng.OpenSchemaReader(ctx, sluiceSrcDSN)
	if err != nil {
		t.Fatalf("open source schema reader: %v", err)
	}
	schema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		t.Fatalf("read source schema: %v", err)
	}

	// Open the VStream snapshot scoped to the in-scope tables (the same path
	// the streamer cold-start takes). The reader surfaces the ADR-0099/0100
	// concurrent partition that runBulkCopyWithOpts consumes.
	scoper, ok := srcEng.(ir.TableScopedSnapshotOpener)
	if !ok {
		t.Fatal("planetscale engine must implement TableScopedSnapshotOpener")
	}
	stream, err := scoper.OpenSnapshotStreamForTables(ctx, sluiceSrcDSN, tables)
	if err != nil {
		t.Fatalf("open snapshot stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Confirm the engine actually surfaced a ≥2-group partition (else the
	// test would silently exercise the serial path and prove nothing).
	if p, ok := stream.Rows.(ir.ConcurrentCopyPartitioner); !ok {
		t.Fatal("snapshot Rows must implement ir.ConcurrentCopyPartitioner (ADR-0100)")
	} else if g := p.ConcurrentCopyGroups(); len(g) < 2 {
		t.Fatalf("expected ≥2 concurrent-copy groups for K=4 over 4 tables; got %v", g)
	}

	// Tight per-stream cap so the producers stay bounded (K × cap/K = cap).
	if setter, ok := stream.Rows.(ir.MaxBufferBytesSetter); ok {
		setter.SetMaxBufferBytes(8 << 20) // 8 MiB total
	}

	sw, err := tgtEng.OpenSchemaWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("open target schema writer: %v", err)
	}
	defer closeIf(sw)
	rw, err := tgtEng.OpenRowWriter(ctx, targetDSN)
	if err != nil {
		t.Fatalf("open target row writer: %v", err)
	}
	defer closeIf(rw)

	// Poll the target's per-table counts WHILE the copy runs; record the peak
	// number of tables simultaneously "in progress" (count > 0 but < final).
	tdb, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target poll db: %v", err)
	}
	defer func() { _ = tdb.Close() }()

	var (
		pollMu       sync.Mutex
		peakInFlight int
	)
	pollCtx, stopPoll := context.WithCancel(ctx)
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		tick := time.NewTicker(150 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-pollCtx.Done():
				return
			case <-tick.C:
				inFlight := 0
				for _, tbl := range tables {
					var n int
					// CREATE TABLE may not exist yet on the very first ticks;
					// ignore errors and treat as 0.
					_ = tdb.QueryRowContext(pollCtx, "SELECT COUNT(*) FROM "+tbl).Scan(&n)
					if n > 0 && n < rowsPer[tbl] {
						inFlight++
					}
				}
				pollMu.Lock()
				if inFlight > peakInFlight {
					peakInFlight = inFlight
				}
				pollMu.Unlock()
			}
		}
	}()

	// Drive the EXACT cold-copy consumer with a D-way fan-out so W × D both
	// engage (W from the surfaced partition, D = 4 here).
	copyErr := runBulkCopyWithOpts(ctx, schema, stream.Rows, sw, rw, bulkCopyOpts{CopyFanoutDegree: 4})
	stopPoll()
	<-pollDone
	if copyErr != nil {
		t.Fatalf("runBulkCopyWithOpts: %v", copyErr)
	}

	pollMu.Lock()
	peak := peakInFlight
	pollMu.Unlock()
	if peak < 2 {
		t.Fatalf("peak concurrently-in-progress tables = %d; want ≥ 2 "+
			"(ADR-0100: the serial consumer wrote ONE table at a time — this is the regression guard)", peak)
	}
	t.Logf("ADR-0100: peak %d tables written concurrently (W>1 confirmed)", peak)

	// Exactly-once: every table's final target COUNT(*) == source (no
	// gap/dup, no dropped/double-copied table = silent loss).
	for _, tbl := range tables {
		var got int
		if err := tdb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&got); err != nil {
			t.Fatalf("count target %s: %v", tbl, err)
		}
		if got != rowsPer[tbl] {
			t.Fatalf("target %s rows = %d; want %d (gap/dup = silent loss)", tbl, got, rowsPer[tbl])
		}
	}

	// The handoff Position is the per-shard set-min across the union of all
	// streams' per-table snapshots — must be a valid resume token, recorded
	// only after ALL pipelines + ALL producers completed (ADR-0100 §4).
	if stream.Position.Engine == "" || stream.Position.Token == "" {
		t.Fatalf("handoff Position empty after concurrent copy: %+v", stream.Position)
	}
}

// seedWideTable inserts n rows (id, body) into table via the target/source
// DSN. Mirrors the engine-level seedAutoShardWide but lives in the pipeline
// package (which can't import the mysql test helper).
func seedWideTable(t *testing.T, dsn, table string, n int) {
	t.Helper()
	const body = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" // 62 bytes
	const batch = 500
	for start := 0; start < n; start += batch {
		end := start + batch
		if end > n {
			end = n
		}
		stmt := "INSERT INTO " + table + " (id, body) VALUES "
		for i := start; i < end; i++ {
			if i > start {
				stmt += ","
			}
			stmt += fmt.Sprintf("(%d,'%s')", i, body)
		}
		applySQL(t, dsn, stmt)
	}
}
