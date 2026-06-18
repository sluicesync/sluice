//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-table VStream cold-copy concurrency (ADR-0099) — the read-side
// throughput lever. The ADR-0095 auto-shard COPY copies tables one at a
// time over a SINGLE vtgate VStream (bounded memory, but a single
// un-splittable read stream). ADR-0097's write-side fan-out only reached
// ~1.4× on this path because the write workers starve behind that one read
// stream. K > 1 concurrent streams (vstream_copy_table_parallelism) open K
// INDEPENDENT VStreams over disjoint table groups — N independent read
// streams, the thing that actually scales (validated ~4× near-linear).
//
// This test seeds 4 tables, opens the snapshot with K=4 concurrent streams,
// and asserts EVERY table lands in full (target row count per table, no
// gap/dup), with a clean stitched-min CDC handoff. The cross-engine
// zero-loss seam under concurrent writes is the pipeline integration
// suite's domain; this pins the engine-level coverage + handoff.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_ConcurrentCopy' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStream_ConcurrentCopy_AllTablesLand(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	tables := []string{"conc_a", "conc_b", "conc_c", "conc_d"}
	for _, tbl := range tables {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (
			id   BIGINT        NOT NULL,
			blob VARCHAR(4096) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tbl))
	}

	// Distinct row counts per table so a mis-routed/dropped table shows up as
	// a count mismatch, not a coincidental match.
	rowsPerTable := map[string]int{"conc_a": 120, "conc_b": 200, "conc_c": 80, "conc_d": 160}
	for _, tbl := range tables {
		seedAutoShardWide(t, mysqlDSN, tbl, rowsPerTable[tbl])
	}

	time.Sleep(3 * time.Second)

	// K=4 concurrent COPY streams via the DSN knob (ADR-0099).
	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_copy_table_parallelism=4",
		mysqlDSN, grpcEndpoint,
	)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStreamForTables(ctx, sluiceDSN, tables)
	if err != nil {
		t.Fatalf("OpenSnapshotStreamForTables: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// A tight per-stream cap: K streams × one-table each must stay bounded.
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

	// Drain every table (the orchestrator's per-table ReadRows, schema order).
	// Each table is produced by exactly one of the K streams; the consumer
	// doesn't know or care which.
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
				"(a buffer-cap refusal here means a stream interleaved — concurrent must keep one table per stream in flight)", name, err)
		}
		if got != rowsPerTable[name] {
			t.Fatalf("%s rows received = %d; want %d (a dropped/double-copied table = silent loss)", name, got, rowsPerTable[name])
		}
	}

	// Join the COPY-completion barrier before reading Position — on the
	// concurrent path the per-table ReadRows close does NOT order the
	// producer's stitched-Position write, so a direct read races it (the
	// real cold-start handoff does this join; see WaitCopyComplete).
	if err := stream.WaitCopyComplete(ctx); err != nil {
		t.Fatalf("WaitCopyComplete after concurrent copy: %v", err)
	}

	// The handoff position is the per-shard set-min across the union of all
	// streams' per-table snapshots — must be a valid resume token.
	if stream.Position.Engine == "" || stream.Position.Token == "" {
		t.Fatalf("handoff Position empty after concurrent copy: %+v", stream.Position)
	}

	// CDC handoff opens cleanly (keyspace-wide tail from the stitched min).
	changes, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges from stitched position: %v", err)
	}
	_ = changes
}
