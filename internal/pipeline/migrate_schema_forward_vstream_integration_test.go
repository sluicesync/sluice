//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7c) — default-on schema-change forwarding for the single-
// stream CDC path, on a VStream (PlanetScale / Vitess flavor) SOURCE.
//
// F7c context: forwarding was proven end-to-end on MySQL-binlog and
// PG-pgoutput sources (the F7a matrix), but a live PlanetScale soak
// showed the ADD COLUMN never forwarded for a VStream source — the
// post-ADD row hit the F8 drift path (1054 unknown column on MySQL /
// 42703 on PG). This test reproduces a VStream-source forward on a real
// vttestserver and pins it: cold-start, then ALTER TABLE … ADD COLUMN on
// the source mid-stream, and assert the new column FORWARDS to the
// target and the post-ADD row lands.
//
// THE PRIME-THEN-MUTATE PATTERN is NOT needed for ADD COLUMN: ADD is
// additive (not seed-guarded), so the very first post-cold-start
// boundary forwards it. The VStream-specific subtlety this exercises is
// the seed/CDC key alignment: the MySQL/planetscale SchemaReader leaves
// Table.Schema empty (single-DB flat scope) so the cold-start seed is
// keyed under the BARE table name, while the VStream CDC reader emits
// SchemaSnapshot with Schema=keyspace — so the first CDC boundary must
// resolve the bare-name seed via lookupSeedCache's fallback (the F7c
// fix). If the seed isn't found, the ADD is treated as the anchor
// (hadPre=false) and never forwards — the soak's failure shape.
//
// Build tag rationale: reuses the existing `vstream` tag (the
// vitess/vttestserver image is already that tag's defining cost) plus
// the standard integration PG target.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vstream' -v -count=1 -timeout=30m \
//	  -run 'TestStreamer_SchemaForward_AddColumn_VStream' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_SchemaForward_AddColumn_VStream_PG pins the forwarded ADD
// COLUMN shape on a VStream (planetscale-flavor) source → PG target via
// the full Streamer apply loop with default --schema-changes=forward.
func TestStreamer_SchemaForward_AddColumn_VStream_PG(t *testing.T) {
	const keyspace = "commerce"
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
	defer cleanupSrc()

	targetDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()

	// Seed a single-PK table; let the schema tracker pick it up before
	// the COPY phase enumerates tables.
	applySQL(t, mysqlDSN, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	applySQL(t, mysqlDSN, "INSERT INTO users (email) VALUES ('seed@example.com')")
	time.Sleep(3 * time.Second)

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale engine not registered")
	}
	tgtEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// The source DSN carries the VStream gRPC endpoint params so the
	// planetscale flavor's CDC + snapshot readers reach vtgate; the
	// SchemaReader connects via the MySQL (vtgate) port in the same DSN.
	sourceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)

	// Default config — forwarding is on by default (ADR-0091). No flags.
	streamer := &Streamer{
		Source:    srcEng,
		Target:    tgtEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-add-vstream-pg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// Phase A: cold-start bulk copy lands the seed row.
	if !waitForPGRowID(t, tgtDB, "users", 1, 90*time.Second) {
		select {
		case e := <-runErr:
			t.Fatalf("phase A: bulk-copy never landed seed row; Run returned: %v", e)
		default:
		}
		t.Fatalf("phase A: bulk-copy never landed seed row")
	}

	// Phase B: mid-stream ADD COLUMN on the source, then a post-ADD
	// INSERT carrying the new column. The VStream FIELD event for the
	// ALTER fires the SchemaSnapshot boundary; the forward intercept must
	// classify it as ADD COLUMN (against the cold-start seed) and forward
	// the ALTER to the target before the post-ADD row applies.
	applySQL(t, mysqlDSN, "ALTER TABLE users ADD COLUMN signup_country VARCHAR(2) NULL")
	applySQL(t, mysqlDSN,
		"INSERT INTO users (email, signup_country) VALUES ('post-ddl@example.com', 'US')")

	// The forwarded column must appear on the target.
	if !waitForPGColumn(t, tgtDB, "users", "signup_country", true, 90*time.Second) {
		select {
		case e := <-runErr:
			t.Fatalf("ADD COLUMN never forwarded to target; Run returned: %v", e)
		default:
		}
		t.Fatalf("ADD COLUMN signup_country never appeared on target — VStream-source forwarding broken (F7c)")
	}

	// And the post-ADD row must land (proves the post-ADD ROW didn't hit
	// the F8 drift / unknown-column path).
	if !waitForPGRowID(t, tgtDB, "users", 2, 90*time.Second) {
		select {
		case e := <-runErr:
			t.Fatalf("post-ADD row never landed; Run returned: %v", e)
		default:
		}
		t.Fatalf("post-ADD row never landed — the new column's row hit the drift path")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
