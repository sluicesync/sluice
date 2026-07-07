//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Backup-snapshot VStream EndPosition finalize (chain-root regression).
//
// A `backup full` of a Vitess/PlanetScale source captures its anchor
// Position AFTER the concurrent COPY pump drains (ADR-0071), not at
// snapshot open. The pre-fix backup path read snap.Position at
// constructor return — the ZERO value — and recorded an EMPTY
// EndPosition, so a chained `backup incremental` couldn't resume off it
// and started "from current" (a bounded, WARN-loud change-loss window on
// every VStream chain root). Same race #243 the cold-start handoff fixed,
// overlooked on the backup path.
//
// The fix adds [irbackup.Snapshot.FinalizePositionFn]: the orchestrator
// calls it AFTER the row sweep; it joins the copy-completion barrier
// (which happens-before the pump's finalized-Position write) and returns
// the real encodeVStreamPos VGTID. These tests prove the finalizer
// surfaces a NON-EMPTY, decodeVStreamPos-valid position on BOTH the
// unsharded (single-stream finishCopy) AND sharded (finishCopyAutoShard
// stitched-min) paths — the finalize path differs by sharding, so an
// unsharded-only green would not prove the sharded stitch.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_BackupSnapshotFinalize' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestVStream_BackupSnapshotFinalize_Unsharded pins the single-stream
// (finishCopy) finalize path: one unsharded keyspace, one table.
func TestVStream_BackupSnapshotFinalize_Unsharded(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE widgets (
		id   BIGINT       NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	const totalRows = 200
	seedBackupFinalizeRows(t, mysqlDSN, "widgets", totalRows)

	// Let vttestserver's async schema tracker pick the table up before
	// COPY enumerates it.
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	assertBackupFinalizeNonEmpty(t, sluiceDSN, "widgets", totalRows, 1 /* wantShards */)
}

// TestVStream_BackupSnapshotFinalize_Sharded pins the auto-shard
// (finishCopyAutoShard stitched-min) finalize path: a 2-shard keyspace.
// The COPY fans out to BOTH shards, so the finalized position must carry
// one shardGtid entry PER shard — the stitch an unsharded run never
// exercises.
func TestVStream_BackupSnapshotFinalize_Sharded(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServerWithShards(t, 2)
	defer cleanup()

	applyVTTestSQL(t, mysqlDSN, `CREATE TABLE widgets (
		id   BIGINT       NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	// A sharded keyspace needs a primary vindex before vtgate routes
	// INSERTs to a shard (see TestVStream_VTTestServer_MultiShard). The
	// default hash vindex on the integer id distributes rows across both
	// shards.
	applyVTTestSQL(t, mysqlDSN, `ALTER VSCHEMA ON test.widgets ADD VINDEX hash(id) USING hash`)

	const totalRows = 200
	seedBackupFinalizeRows(t, mysqlDSN, "widgets", totalRows)

	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true",
		mysqlDSN, grpcEndpoint,
	)

	assertBackupFinalizeNonEmpty(t, sluiceDSN, "widgets", totalRows, 2 /* wantShards */)
}

// assertBackupFinalizeNonEmpty opens the PlanetScale-flavor backup
// snapshot scoped to one table, proves the anchor is EMPTY at open
// (the load-bearing precondition — the reason the finalizer exists),
// drains every row, then calls FinalizePositionFn and asserts the
// returned anchor is non-empty, decodeVStreamPos-valid, and spans the
// expected shard count.
func assertBackupFinalizeNonEmpty(t *testing.T, sluiceDSN, table string, wantRows, wantShards int) {
	t.Helper()

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Scope the COPY to exactly the one table so copyDone closes as soon
	// as it drains (the whole-keyspace shape would wait on every table).
	snap, err := eng.OpenBackupSnapshotForTables(ctx, sluiceDSN, irbackup.SnapshotOptions{}, []string{table})
	if err != nil {
		t.Fatalf("OpenBackupSnapshotForTables: %v", err)
	}
	defer func() { _ = snap.Close() }()

	// (a) The VStream backup snapshot MUST carry a finalizer — that is
	// the surface the orchestrator dispatches on; nil would silently fall
	// back to recording the empty open-time position.
	if snap.FinalizePositionFn == nil {
		t.Fatal("VStream backup snapshot has a nil FinalizePositionFn — the orchestrator would record the empty open-time anchor (chain-root regression)")
	}

	// The open-time anchor is the ZERO value: proving this is what makes
	// the finalizer load-bearing rather than cosmetic. If this ever
	// becomes non-empty at open, the finalize plumbing can be dropped —
	// but it is empty today, which is exactly why the bug existed.
	if snap.Position.Token != "" || snap.Position.Engine != "" {
		t.Fatalf("open-time snap.Position = %+v; expected the ZERO value (the finalizer supplies the real anchor post-sweep)", snap.Position)
	}

	tbl := &ir.Table{
		Name: table,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "name", Type: ir.Varchar{Length: 255}, Nullable: false},
		},
		PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	ch, err := snap.Rows.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows(%s): %v", table, err)
	}
	got := 0
	for range ch {
		got++
	}
	if err := snap.Rows.Err(); err != nil {
		t.Fatalf("snapshot Rows.Err after draining %s: %v", table, err)
	}
	if got != wantRows {
		t.Fatalf("drained %d rows from %s; want %d", got, table, wantRows)
	}

	// The sweep has fully drained: the finalizer joins copyDone (already
	// closed or about to be) and reads the finalized VGTID.
	finalized, err := snap.FinalizePositionFn(ctx)
	if err != nil {
		t.Fatalf("FinalizePositionFn: %v", err)
	}
	if finalized.Token == "" {
		t.Fatal("finalized EndPosition token is EMPTY — the VStream chain-root regression is unfixed; a chained incremental would start from current")
	}

	shards, ok, err := decodeVStreamPos(finalized)
	if err != nil || !ok {
		t.Fatalf("decodeVStreamPos(finalized=%+v): ok=%v err=%v — EndPosition is not the VGTID shape incremental chain-resume decodes", finalized, ok, err)
	}
	if len(shards) != wantShards {
		t.Fatalf("finalized position spans %d shards (%v); want %d — the %s finalize path did not surface every shard's cursor",
			len(shards), shards, wantShards, map[bool]string{true: "auto-shard stitched-min", false: "single-stream"}[wantShards > 1])
	}
	for i, s := range shards {
		if s.Gtid == "" || s.Gtid == "current" {
			t.Errorf("finalized shards[%d].Gtid = %q; want a concrete post-COPY GTID (an incremental resuming here must not restart from head)", i, s.Gtid)
		}
	}
	t.Logf("finalized VStream backup EndPosition: %s", finalized.Token)
}

// seedBackupFinalizeRows inserts n rows into table via the vtgate MySQL
// endpoint. Batched multi-row INSERTs keep the seed fast.
func seedBackupFinalizeRows(t *testing.T, mysqlDSN, table string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", mysqlDSN+"&multiStatements=true")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const batch = 50
	for start := 1; start <= n; start += batch {
		stmt := fmt.Sprintf("INSERT INTO %s (id, name) VALUES ", table)
		for i := start; i < start+batch && i <= n; i++ {
			if i > start {
				stmt += ","
			}
			stmt += fmt.Sprintf("(%d, 'row-%d')", i, i)
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed insert [%d..): %v", start, err)
		}
	}
}
