//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine-target CHAOS: a Vitess (vttestserver) source migrated to
// a local Postgres target via the REAL pipeline.Migrator, with the
// source container restarted mid-bulk-copy.
//
// This is the cross-engine-write-path counterpart to the engine-level
// chaos suite in internal/engines/mysql/cdc_vstream_cluster_chaos_*.go
// (assertZeroLossOrLoud). That suite is reader-only: it drains a CDC /
// snapshot stream and asserts zero-loss-or-loud on the DELIVERED change
// set. This test exercises the FULL Migrator path instead
// (schema read -> create -> bulk copy -> indexes -> constraints) writing
// into a Postgres target, and faults the SOURCE — the vttestserver whose
// vtgate MySQL connection the Migrator's bulk-copy phase
// (Source.OpenRowReader, plain SQL over vtgate for both MySQL flavors)
// streams rows through. Restarting the container drops that connection
// mid-COPY.
//
// THE INVARIANT (mirrors assertZeroLossOrLoud): after the fault, EITHER
//
//   - Migrator.Run returned a non-nil error (LOUD failure — acceptable;
//     sluice did not silently corrupt, it stopped and said so), OR
//   - the target ends with COUNT(*) == source COUNT(*) (ZERO-LOSS — the
//     gRPC/SQL-transient reconnect, v0.99.4 Gap 1, healed the copy).
//
// The FORBIDDEN outcome is a SILENT PARTIAL: Migrator.Run returns nil
// but the target row count is < source — sluice neither completed nor
// failed loudly (silent-loss tenet violation). We do NOT assume which of
// the two acceptable outcomes happens; we assert the invariant.
//
// Run (heavy + slow — gated by the `vstream` tag, NOT in the per-PR
// unit gate; it pays the vitess/vttestserver image cost the tag already
// gates):
//
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestMigrate_VStreamSource_RestartMidCopy_PGTarget' \
//	  ./internal/pipeline/...

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

// TestMigrate_VStreamSource_RestartMidCopy_PGTarget seeds a single-shard
// Vitess source with enough rows that the cross-engine bulk copy takes
// several seconds, runs the Migrator into a fresh Postgres target, and
// restarts the source container the moment the copy is observably
// in-progress (target has some rows but fewer than the source). It then
// asserts the zero-loss-or-loud invariant.
func TestMigrate_VStreamSource_RestartMidCopy_PGTarget(t *testing.T) {
	const (
		keyspace = "chaos_xeng"
		table    = "xeng"
		// Sized so the bulk copy runs for several seconds (default parallel
		// ~200k rows/s on a local host ⇒ ~10s), giving the fixed-delay
		// restart (below) a wide window to land mid-copy. We do NOT poll the
		// target's COUNT(*) to detect "mid-copy": the cross-engine bulk COPY
		// commits the table in one transaction, so its rows are invisible to
		// a separate polling connection until the very end (MVCC) — a poll
		// never observes a partial. A fixed delay during the copy is the
		// reliable injection point.
		seedRows = 2_000_000
	)

	// Single shard: this test is about SOURCE disruption mid-copy, not
	// scatter/sharding (that is covered by the sharded migrate test).
	mysqlDSN, _, restartSource, vtCleanup := startShardedVTTestServer(t, keyspace, 1)
	defer vtCleanup()

	// Seed: a wide-ish row so 200k rows is a few MB of COPY work, not an
	// instant flush. AUTO_INCREMENT PK keeps the source a clean
	// COUNT(*) ground truth.
	applySQL(t, mysqlDSN, `
		CREATE TABLE xeng (
			id      BIGINT       NOT NULL AUTO_INCREMENT,
			payload VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	time.Sleep(2 * time.Second) // schema-tracker settle

	// Chunked multi-row INSERTs: n single-row INSERTs through vtgate is
	// pathologically slow. ~1k rows per statement is the sweet spot.
	seedChunkedRows(t, mysqlDSN, table, seedRows, 1000)

	sourceCount := mysqlScalar(t, mysqlDSN, "SELECT COUNT(*) FROM xeng")
	if sourceCount != seedRows {
		t.Fatalf("source seed count = %d; want %d (seed bug)", sourceCount, seedRows)
	}
	t.Logf("seeded %d rows in source xeng", sourceCount)

	pgDSN, pgCleanup := startPGTarget(t)
	defer pgCleanup()

	// Cross-engine Migrator: vanilla mysql engine reads vtgate's MySQL
	// frontend (plain SQL bulk copy — the path the source restart
	// disrupts), postgres engine writes the target. Single table in
	// scope (the only table in the keyspace).
	srcEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("source engine \"mysql\" not registered")
	}
	tgtEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("target engine \"postgres\" not registered")
	}

	mig := &Migrator{
		Source:    srcEng,
		Target:    tgtEng,
		SourceDSN: mysqlDSN,
		TargetDSN: pgDSN,
	}

	// Generous outer budget: vttestserver boot is already paid; this
	// covers the 2M copy + restart + reconnect/recovery.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- mig.Run(ctx)
	}()

	// --- Mid-copy fault injection (time-based) ----------------------------
	// Let the Migrator clear the schema phase and get into the bulk copy
	// (the 2M-row copy runs several seconds on a local host), then restart
	// the source container. A FIXED DELAY is the reliable injection point:
	// the cross-engine bulk COPY commits the target table in ONE
	// transaction, so a separate connection polling the target COUNT(*)
	// never observes a partial (MVCC) — there is no mid-copy signal to poll
	// on. ~3s lands well inside the bulk-copy phase for a copy of this size.
	// If the copy somehow finished first, restarting an idle source is
	// harmless and the run still asserts zero-loss.
	time.Sleep(3 * time.Second)
	t.Logf("restarting source mid-copy (fixed ~3s delay, during the bulk-copy phase)")
	restartSource(t)

	// --- Wait for Migrator.Run, bounded by the outer ctx -----------------
	var runErr error
	select {
	case runErr = <-runErrCh:
	case <-ctx.Done():
		t.Fatalf("Migrator.Run did not return within the test budget: %v", ctx.Err())
	}

	// --- Assert the invariant: zero-loss OR loud, never silent partial ---
	if runErr != nil {
		// LOUD failure — acceptable. sluice surfaced the fault rather
		// than silently delivering a partial copy.
		t.Logf("LOUD-FAILURE outcome (acceptable): Migrator.Run returned err=%v "+
			"(source count=%d)", runErr, sourceCount)
		return
	}

	// nil error ⇒ sluice must have copied EVERY row. A short count here
	// is the forbidden SILENT PARTIAL.
	targetCount := pgRowCount(t, pgDSN, "SELECT COUNT(*) FROM xeng")
	if targetCount < sourceCount {
		t.Fatalf("SILENT PARTIAL (the forbidden outcome): Migrator.Run returned nil but target has %d rows, "+
			"source has %d — sluice neither completed nor failed loudly across the source restart "+
			"(silent-loss tenet violation)", targetCount, sourceCount)
	}
	if targetCount > sourceCount {
		// More than source would be a dup-insert bug; the cold-start
		// copy is idempotent on PK, so this should never happen, but
		// flag it loudly if it does (it is not a silent-loss, but it is
		// a correctness signal worth surfacing).
		t.Fatalf("target has %d rows but source has %d — over-delivery (dup) across the restart", targetCount, sourceCount)
	}
	t.Logf("ZERO-LOSS outcome: target count == source count == %d across the source restart", sourceCount)
}

// seedChunkedRows inserts n rows into table via chunked multi-row
// INSERTs of size rowsPerStmt each (single-row INSERTs through vtgate are
// pathologically slow). The payload is a fixed-width filler so the copy
// has real bytes to move.
func seedChunkedRows(t *testing.T, dsn, table string, n, rowsPerStmt int) {
	t.Helper()
	const filler = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" // 100 chars

	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for start := 0; start < n; start += rowsPerStmt {
		end := start + rowsPerStmt
		if end > n {
			end = n
		}
		// id is AUTO_INCREMENT; only payload is supplied.
		vals := ""
		for i := start; i < end; i++ {
			if i > start {
				vals += ","
			}
			vals += fmt.Sprintf("('%s-%d')", filler, i)
		}
		stmt := "INSERT INTO " + table + " (payload) VALUES " + vals
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		_, err := db.ExecContext(ctx, stmt)
		cancel()
		if err != nil {
			t.Fatalf("seed insert [%d,%d): %v", start, end, err)
		}
	}
}

// mysqlScalar runs a single-int-column query against a MySQL/vtgate DSN
// and returns the scalar. Used for the source COUNT(*) ground truth.
func mysqlScalar(t *testing.T, dsn, query string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		t.Fatalf("mysql query %q: %v", query, err)
	}
	return n
}
