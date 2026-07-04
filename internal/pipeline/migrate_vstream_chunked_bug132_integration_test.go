//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 132 — END-TO-END coverage of the chunked PlanetScale-flavor migrate
// path (the exact gap that let the bug ship: the existing `vstream` migrate
// pins drive the VANILLA mysql engine and/or sub-threshold tables, so the
// PlanetScale-flavor chunked path was never exercised end-to-end).
//
// v0.99.14 set vtgate `workload=olap` as a session-wide DSN param on the
// PlanetScale/Vitess RowReader (to lift the 100k OLTP cap on a no-PK full
// scan). That also covered the LIMIT-paged ReadRowsBatch the parallel
// chunked bulk-copy uses, where olap streaming truncated each concurrently-
// read chunk's page — so a `--source-driver=planetscale|vitess` migrate of a
// table large enough to CHUNK silently copied a fraction of its rows and
// still reported success (exit 0). The fix scopes olap to ReadRows only.
//
// This test drives the source engine through the PlanetScale flavor (so
// OpenRowReader's olap logic runs) and a table above the parallel-chunk
// threshold at parallelism > 1, asserting EXACT row-count parity. NOTE: the
// catalogued truncation is SCALE-DEPENDENT (the field repro used 1.5M rows;
// a few thousand rows copies fully even with the session-wide olap bug), so
// at CI-tolerable sizes this is functional coverage, NOT a regression
// catcher. The DETERMINISTIC regression pin — that olap is never session-
// wide on a VStream reader — is TestVStream_RowReader_OLAPScopedToFullScan_
// Bug132 (internal/engines/mysql), which fails fast if the session-wide
// setting is reintroduced. The 1.5M-scale truncation is re-validated on the
// PlanetScale rig.
//
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestMigrate_VStreamSource_ChunkedPKParity_Bug132' ./internal/pipeline/...

package pipeline

import (
	"context"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestMigrate_VStreamSource_ChunkedPKParity_Bug132(t *testing.T) {
	const keyspace = "bench"
	// grpcEndpoint feeds the planetscale source DSN's vstream_* params
	// (Bug 126's central strip at openDB keeps them from leaking into the
	// plain-SQL schema/row reads); restartSource is unused (static migrate).
	mysqlDSN, grpcEndpoint, _, vtCleanup := startShardedVTTestServer(t, keyspace, 1)
	defer vtCleanup()

	applySQL(t, mysqlDSN, `
		CREATE TABLE events (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	time.Sleep(3 * time.Second) // vtgate schema-tracker settle

	// Seed enough rows to exceed the lowered --bulk-parallel-min-rows so the
	// table takes the parallel chunked path. Seed in batches to stay under
	// vtgate's statement size limits.
	const total = 4000
	const batch = 500
	for start := 0; start < total; start += batch {
		var vals []string
		for i := start; i < start+batch && i < total; i++ {
			vals = append(vals, "('p"+strconv.Itoa(i)+"')")
		}
		applySQL(t, mysqlDSN+"&multiStatements=true",
			"INSERT INTO events (payload) VALUES "+joinVals(vals))
	}
	time.Sleep(2 * time.Second)

	srcCount := len(mysqlRows(t, mysqlDSN, "SELECT id FROM events"))
	if srcCount != total {
		t.Fatalf("source seed incomplete: have %d rows, want %d", srcCount, total)
	}

	srcDSN := mysqlDSN +
		"&vstream_endpoint=" + grpcEndpoint +
		"&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0"

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal(`source engine "planetscale" not registered`)
	}
	tgtDSN, tgtCleanup := startMySQLTarget(t)
	defer tgtCleanup()
	tgtEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal(`target engine "mysql" not registered`)
	}

	mig := &Migrator{
		Source:              srcEng,
		Target:              tgtEng,
		SourceDSN:           srcDSN,
		TargetDSN:           tgtDSN,
		BulkParallelism:     4,    // force the parallel chunked path
		BulkParallelMinRows: 1000, // so `total` rows chunk into 4 ranges
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (planetscale chunked source): %v", err)
	}

	// THE assertion: every source row reached the target. Pre-fix this
	// landed only a few hundred rows while Run still returned nil — a
	// CRITICAL silent partial copy.
	got := len(mysqlRows(t, tgtDSN, "SELECT id FROM events"))
	if got != total {
		t.Fatalf("Bug 132: target has %d rows; want %d (chunked VStream copy truncated under workload=olap)", got, total)
	}
	t.Logf("Bug 132 regression: chunked PlanetScale migrate copied all %d rows (parallelism=4, min-rows=1000)", total)
}
