//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Auto-shard-by-table VStream cold-copy (ADR-0095) — the program's #1
// finding fix. The default VStream cold-copy used ONE keyspace-wide
// stream (`/.*/`) that interleaves every table; the orchestrator drains
// one table at a time, so every not-yet-drained table buffered in RAM and
// a large multi-table keyspace overflowed --max-buffer-bytes (ADR-0071
// loud refusal). Auto-shard copies each table as its OWN single-table
// VStream (constant memory, no interleave), so a full multi-table
// keyspace copies in ONE command at bounded memory.
//
// This test seeds TWO tables that EACH dwarf a tiny byte cap, opens the
// snapshot scoped to BOTH (the default full-keyspace shape — >1 table
// engages auto-shard), and asserts BOTH drain cleanly (every row,
// Rows.Err()==nil, NO loud refusal). Under the pre-ADR-0095 single-stream
// path the second table would trip the multi-table-interleaving refusal
// the instant it started buffering behind the first.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_AutoShardSnapshot' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStream_AutoShardSnapshot_MultiTableBoundedMemory(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// Two tables, each with enough wide rows that — interleaved behind one
	// another under the tiny cap below — the single-stream path would trip
	// the ADR-0071 multi-table-interleaving loud refusal. Auto-shard copies
	// them one at a time, so neither ever interleaves.
	for _, tbl := range []string{"wide_a", "wide_b"} {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (
			id   BIGINT        NOT NULL,
			blob VARCHAR(4096) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tbl))
	}

	const rowsPerTable = 300
	seedAutoShardWide(t, mysqlDSN, "wide_a", rowsPerTable)
	seedAutoShardWide(t, mysqlDSN, "wide_b", rowsPerTable)

	// Let vttestserver's async schema tracker pick the tables up before
	// COPY enumerates them.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Open scoped to BOTH tables — this is the default full-keyspace shape
	// (the orchestrator passes the whole filtered table list). >1 table
	// engages auto-shard.
	tables := []string{"wide_a", "wide_b"}
	stream, err := eng.OpenSnapshotStreamForTables(ctx, sluiceDSN, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// A tiny byte cap. Under the legacy single-stream path the second
	// table buffering behind the first would trip the loud refusal here;
	// auto-shard keeps exactly one table in flight, so the cap is never
	// crossed by a not-yet-consumed table.
	if setter, ok := stream.Rows.(ir.MaxBufferBytesSetter); ok {
		setter.SetMaxBufferBytes(16 << 10) // 16 KiB
	} else {
		t.Fatal("snapshot Rows must implement ir.MaxBufferBytesSetter")
	}

	mkTable := func(name string) *ir.Table {
		return &ir.Table{
			Name: name,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
				{Name: "blob", Type: ir.Varchar{Length: 4096}, Nullable: false},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		}
	}

	// Drain both tables in order (the orchestrator's per-table ReadRows).
	for _, name := range tables {
		ch, err := stream.Rows.ReadRows(ctx, mkTable(name))
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", name, err)
		}
		got := 0
		for range ch {
			got++
		}
		if err := stream.Rows.Err(); err != nil {
			t.Fatalf("snapshot Rows.Err after draining %s: %v "+
				"(a multi-table-interleaving refusal here means auto-shard didn't engage)", name, err)
		}
		if got != rowsPerTable {
			t.Fatalf("%s rows received = %d; want %d", name, got, rowsPerTable)
		}
	}

	// The handoff position must be a valid VStream position (the stitched
	// per-table minimum) so the CDC tail can resume.
	if stream.Position.Engine == "" || stream.Position.Token == "" {
		t.Fatalf("handoff Position is empty after auto-shard copy: %+v", stream.Position)
	}

	// CDC handoff must open cleanly from the stitched position (keyspace-
	// wide tail). We don't drive change events here — the streaming + the
	// cross-engine zero-loss seam are covered by the pipeline integration
	// suite; this asserts the handoff doesn't error.
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges from stitched position: %v", err)
	}
	_ = changes
}

// seedAutoShardWide inserts n wide rows (4000-char blob) into table.
func seedAutoShardWide(t *testing.T, dsn, table string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed %s open: %v", table, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	blob := make([]byte, 4000)
	for i := range blob {
		blob[i] = 'x'
	}
	for i := 1; i <= n; i++ {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (id, blob) VALUES (?, ?)", table), i, string(blob)); err != nil {
			t.Fatalf("seed %s row %d: %v", table, i, err)
		}
	}
}
