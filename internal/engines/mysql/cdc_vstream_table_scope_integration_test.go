//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Table-scoped VStream snapshot COPY — the included-table allowlist
// feature. The default VStream snapshot copies EVERY table in the
// keyspace (filter rule `/.*/`), so copying one small table out of a
// keyspace that also holds a large table streams + buffers the large
// table too and overflows --max-buffer-bytes (the ADR-0071 multi-table
// interleaving cap). Scoping the COPY filter to the included tables makes
// vtgate copy only those tables, so the large unrelated table is never
// streamed/buffered.
//
// This test seeds a keyspace with a small table (small_t) and a larger
// table (big_t), opens a snapshot SCOPED to small_t via
// OpenSnapshotStreamForTables under a tiny byte cap, and asserts that
// small_t drains cleanly (every row, Rows.Err()==nil) and big_t is never
// delivered — i.e. big_t was never streamed. With the default whole-
// keyspace open the tiny cap would instead trip the multi-table-
// interleaving loud refusal once big_t started buffering behind small_t.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_TableScopedSnapshot' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStream_TableScopedSnapshot_DoesNotStreamUnscopedTable(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// small_t: a handful of rows, all of which we expect to receive.
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE small_t (
		id  BIGINT      NOT NULL,
		val VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	// big_t: enough wide rows that, were it streamed interleaved behind
	// small_t under the tiny cap below, it would trip the ADR-0071
	// multi-table-interleaving loud refusal. With the scope it is never
	// streamed at all.
	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE big_t (
		id   BIGINT       NOT NULL,
		blob VARCHAR(4096) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	const smallRows = 200
	seedTableScopeSmall(t, mysqlDSN, smallRows)
	seedTableScopeBig(t, mysqlDSN, 500)

	// Let vttestserver's async schema tracker pick the tables up before
	// COPY enumerates tables.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	if _, ok := any(eng).(ir.TableScopedSnapshotOpener); !ok {
		t.Fatal("Engine{Flavor: FlavorPlanetScale} must implement ir.TableScopedSnapshotOpener")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Scope the snapshot COPY to small_t only.
	stream, err := eng.OpenSnapshotStreamForTables(ctx, sluiceDSN, []string{"small_t"})
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// A tiny byte cap: were big_t streamed interleaved behind small_t, this
	// would trip the multi-table-interleaving loud refusal. Because the
	// COPY is scoped to small_t, big_t never streams, so small_t drains
	// cleanly regardless of the cap.
	if setter, ok := stream.Rows.(ir.MaxBufferBytesSetter); ok {
		setter.SetMaxBufferBytes(16 << 10) // 16 KiB
	} else {
		t.Fatal("snapshot Rows must implement ir.MaxBufferBytesSetter")
	}

	smallTable := &ir.Table{
		Name: "small_t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "val", Type: ir.Varchar{Length: 64}, Nullable: false},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	// Drain small_t fully.
	smallCh, err := stream.Rows.ReadRows(ctx, smallTable)
	if err != nil {
		t.Fatalf("ReadRows(small_t): %v", err)
	}
	got := 0
	for range smallCh {
		got++
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("snapshot Rows.Err after draining small_t: %v "+
			"(a multi-table-interleaving refusal here means big_t was streamed despite the scope)", err)
	}
	if got != smallRows {
		t.Fatalf("small_t rows received = %d; want %d", got, smallRows)
	}

	// big_t must never have been streamed: its queue is empty and copy has
	// completed (small_t drained → COPY_COMPLETED), so ReadRows(big_t)
	// returns an immediately-closed empty channel.
	bigTable := &ir.Table{
		Name: "big_t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "blob", Type: ir.Varchar{Length: 4096}, Nullable: false},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	bigCh, err := stream.Rows.ReadRows(ctx, bigTable)
	if err != nil {
		t.Fatalf("ReadRows(big_t): %v", err)
	}
	bigGot := 0
	for range bigCh {
		bigGot++
	}
	if bigGot != 0 {
		t.Fatalf("big_t rows received = %d; want 0 (scoped COPY must not stream big_t)", bigGot)
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("snapshot Rows.Err after big_t check: %v", err)
	}
}

// seedTableScopeSmall inserts n rows into small_t.
func seedTableScopeSmall(t *testing.T, dsn string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed small open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 1; i <= n; i++ {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO small_t (id, val) VALUES (%d, 's%d')", i, i)); err != nil {
			t.Fatalf("seed small row %d: %v", i, err)
		}
	}
}

// seedTableScopeBig inserts n wide rows into big_t. Each row's blob is a
// 4000-char string so a handful of rows already exceeds the tiny test cap
// — were big_t streamed, the interleaving refusal would fire.
func seedTableScopeBig(t *testing.T, dsn string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed big open: %v", err)
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
			"INSERT INTO big_t (id, blob) VALUES (?, ?)", i, string(blob)); err != nil {
			t.Fatalf("seed big row %d: %v", i, err)
		}
	}
}
