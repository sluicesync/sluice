//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for the CRITICAL silent-index-loss fix: a VStream
// (PlanetScale/Vitess) MySQL TARGET must actually build every secondary index
// on the overlapped index path — the exact seam
// (BuildTableIndexesFromChannel) that both `sluice migrate` and the
// fast-parallel `sync` cold-start funnel through — instead of silently
// draining the completed-tables channel and creating NO index at all.
//
// This boots the cheap single-shard vttestserver as the Vitess target
// (Engine{Flavor: FlavorVitess} → usesVStream() true → the drain-then-serial
// build branch), creates the tables, drives the pipeline's overlap consumer,
// and ground-truths EVERY secondary-index family via
// information_schema.statistics: plain BTREE, UNIQUE single-column, composite
// multi-column, and an FK-backing plain KEY. It then exercises the
// loud-failure safety net (VerifyIndexes) in both directions: green on a
// correctly-built target, and SLUICE-E-INDEX-MISSING when an expected index is
// absent.
//
// The full pipeline-level migrate + sync-cold-start end-to-end against a
// Vitess target is a heavier harness (target-side vtgate DDL under the
// pipeline orchestrator); this engine-level pin exercises the identical
// load-bearing method both paths call, on a real Vitess target. See the report
// note on the remaining pipeline-level e2e vstream pin (CI vstream shard).
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vstream' -v -count=1 -timeout=15m \
//	  -run 'TestVStream_VTTestServer_SecondaryIndexesBuildAndVerify' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestVStream_VTTestServer_SecondaryIndexesBuildAndVerify pins the fix on a
// real Vitess target: the overlapped index path builds EVERY secondary-index
// family, and the verification net flags a missing one.
func TestVStream_VTTestServer_SecondaryIndexesBuildAndVerify(t *testing.T) {
	mysqlDSN, _, _, cleanup := startVTTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// The Vitess flavor → usesVStream() true → the branch the bug lived in.
	eng := Engine{Flavor: FlavorVitess}
	swIface, err := eng.OpenSchemaWriter(ctx, mysqlDSN)
	if err != nil {
		t.Fatalf("OpenSchemaWriter (vitess target): %v", err)
	}
	sw, ok := swIface.(*SchemaWriter)
	if !ok {
		t.Fatalf("OpenSchemaWriter returned %T; want *SchemaWriter", swIface)
	}
	defer func() { _ = sw.Close() }()
	if !sw.flavor.usesVStream() {
		t.Fatal("test precondition: writer flavor must be a VStream flavor so the drain-then-serial build branch is exercised")
	}

	// One table carrying every secondary-index family (pin the class, not one
	// representative): plain BTREE, UNIQUE single-column, composite
	// multi-column, and an FK-backing plain KEY. All integer columns to avoid
	// key-length noise — the fix is about DISPATCH per family, not column type.
	col := func(name string) *ir.Column {
		return &ir.Column{Name: name, Type: ir.Integer{Width: 64}, Nullable: false}
	}
	table := &ir.Table{
		Name:       "orders",
		Columns:    []*ir.Column{col("id"), col("a"), col("b"), col("c")},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "orders_a_idx", Columns: []ir.IndexColumn{{Column: "a"}}},                 // plain BTREE
			{Name: "orders_b_uidx", Unique: true, Columns: []ir.IndexColumn{{Column: "b"}}},  // UNIQUE
			{Name: "orders_ab_idx", Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}}, // composite multi-column
			{Name: "orders_c_idx", Columns: []ir.IndexColumn{{Column: "c"}}},                 // FK-backing plain KEY
		},
	}
	schema := &ir.Schema{Tables: []*ir.Table{table}}

	// Phase 1: create the table (columns + PK only) — the same pre-copy DDL
	// phase the migrator runs.
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}

	// Phase 2: drive the overlap consumer EXACTLY as the pipeline does — feed
	// each just-copied table onto the channel, close it, and let
	// BuildTableIndexesFromChannel build the indexes. On the VStream flavor
	// this is the drain-then-serial-build branch under test.
	ch := make(chan *ir.Table, len(schema.Tables))
	for _, tbl := range schema.Tables {
		ch <- tbl
	}
	close(ch)
	if err := sw.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
		t.Fatalf("BuildTableIndexesFromChannel (vstream serial build): %v", err)
	}

	// Ground truth: EVERY secondary-index family must now exist on the Vitess
	// target. This is the assertion that fails on the bug (the old no-op).
	probe, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	defer func() { _ = probe.Close() }()
	for _, idx := range []string{"orders_a_idx", "orders_b_uidx", "orders_ab_idx", "orders_c_idx"} {
		var n int
		if err := probe.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM information_schema.statistics
			 WHERE table_schema=DATABASE() AND table_name='orders' AND index_name=?`,
			idx,
		).Scan(&n); err != nil {
			t.Fatalf("query statistics %s: %v", idx, err)
		}
		if n == 0 {
			t.Errorf("secondary index %q ABSENT on the Vitess target — the vstream path silently skipped it (the bug)", idx)
		}
	}

	// Loud-failure net, green direction: a correctly-built target passes.
	if err := sw.VerifyIndexes(ctx, schema); err != nil {
		t.Fatalf("VerifyIndexes on a fully-built Vitess target must pass; got %v", err)
	}

	// Loud-failure net, red direction: an EXPECTED-but-never-built index must
	// raise SLUICE-E-INDEX-MISSING (naming the row), not exit clean.
	phantom := &ir.Table{
		Name:       table.Name,
		Columns:    table.Columns,
		PrimaryKey: table.PrimaryKey,
		Indexes: append(append([]*ir.Index(nil), table.Indexes...),
			&ir.Index{Name: "orders_phantom_idx", Columns: []ir.IndexColumn{{Column: "c"}}}),
	}
	err = sw.VerifyIndexes(ctx, &ir.Schema{Tables: []*ir.Table{phantom}})
	if err == nil {
		t.Fatal("VerifyIndexes must FAIL when an expected index is missing; got nil (silent loss)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeIndexMissing {
		t.Fatalf("VerifyIndexes error = %v; want a SLUICE-E-INDEX-MISSING coded error", err)
	}
}
