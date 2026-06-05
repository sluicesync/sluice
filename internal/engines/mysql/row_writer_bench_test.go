//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Standing performance benchmark isolating the cold-start COPY writer
// overhead the v0.99.4 change introduced: plain batched INSERT vs the
// idempotent INSERT … ON DUPLICATE KEY UPDATE form.
//
// Why this benchmark exists: v0.99.4 made the MySQL VStream cold-start
// COPY path idempotent (ON DUPLICATE KEY UPDATE) so Vitess's catchup-
// phase re-emissions absorb instead of duplicating (Bug 125, ADR-0072).
// That correctness fix adds a per-row clause to every INSERT on the hot
// bulk-copy path. This benchmark measures the throughput cost of that
// clause so the overhead is a tracked, regression-visible number rather
// than an unmeasured assumption.
//
// It exercises BulkLoadBatchedInsert directly (the PlanetScale path and
// vanilla MySQL's per-call LOAD-DATA fallback) by driving the writer's
// two batched code paths against a real MySQL container:
//
//   - plain                — writeBatched (the v0.99.3 cold-start path)
//   - idempotent           — writeBatchedIdempotent into an EMPTY target
//                            (the v0.99.4+ cold-start no-conflict case)
//   - idempotent_conflicts — writeBatchedIdempotent into a PRE-POPULATED
//                            target so every upsert hits the PK unique
//                            key (the re-emission / resume worst case)
//
// All three drive the SAME generated N rows through the SAME PK-shaped
// table, so the only variable is the SQL form (and, for the conflicts
// sub-bench, whether the key collides). rows/s is reported via
// b.ReportMetric alongside the default ns/op.
//
// To run (Windows / Rancher Desktop):
//
//	$env:TESTCONTAINERS_RYUK_DISABLED="true"
//	go test -tags=integration -run '^$' \
//	  -bench 'BenchmarkColdStartCopyWriter' -benchtime=5x \
//	  ./internal/engines/mysql/ -v
//
// It is a Benchmark* function, so a normal `go test` run never executes
// it; it only compiles under the integration tag.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// benchCopyRowCount is the number of rows pushed through each path per
// benchmark iteration. Large enough that the per-batch SQL cost
// dominates fixed per-iteration setup, small enough that -benchtime=5x
// completes in a reasonable wall time on a warm container.
const benchCopyRowCount = 50_000

// benchCopyTable is the representative PK-shaped target: a BIGINT
// PRIMARY KEY plus an int, a varchar(64), and a datetime — the fair
// apples-to-apples shape both the plain and idempotent paths write
// identical rows into.
func benchCopyTable() *ir.Table {
	return &ir.Table{
		Name: "bench_cold_start_copy",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "n", Type: ir.Integer{Width: 32}, Nullable: false},
			{Name: "label", Type: ir.Varchar{Length: 64}, Nullable: false},
			{Name: "created_at", Type: ir.DateTime{}, Nullable: false},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

const benchCopyDDL = `
	CREATE TABLE bench_cold_start_copy (
		id          BIGINT       NOT NULL,
		n           INT          NOT NULL,
		label       VARCHAR(64)  NOT NULL,
		created_at  DATETIME     NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// benchCopyRows generates the deterministic N-row payload shared by
// every path. Returned as a slice (not a channel) so each iteration can
// re-feed the exact same rows without regenerating them inside the
// timed region.
func benchCopyRows(n int) []ir.Row {
	created := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	rows := make([]ir.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = ir.Row{
			"id":         int64(i),
			"n":          int64(i * 7),
			"label":      "row-payload-fixed-width",
			"created_at": created,
		}
	}
	return rows
}

// feedRows pushes rows into a buffered channel from a goroutine and
// returns the read end for the writer to drain. The buffer keeps the
// producer from gating the writer's throughput measurement.
func feedRows(rows []ir.Row) <-chan ir.Row {
	ch := make(chan ir.Row, 1024)
	go func() {
		defer close(ch)
		for _, r := range rows {
			ch <- r
		}
	}()
	return ch
}

// BenchmarkColdStartCopyWriter measures the MySQL batched bulk-copy
// writer's throughput on the plain vs idempotent paths. See the file
// header for the methodology; rows/s is the headline metric.
func BenchmarkColdStartCopyWriter(b *testing.B) {
	db, table, rows := setupColdStartCopyBench(b)
	defer func() { _ = db.Close() }()

	// One *RowWriter shared across iterations: the writer is stateless
	// between WriteRows calls (it holds only the *sql.DB pool), so reusing
	// it avoids re-opening a pool per iteration. BatchedInsert is the path
	// under test — it's what both writeBatched and writeBatchedIdempotent
	// flush through.
	w := &RowWriter{db: db, schema: "sluice_test", bulkLoad: ir.BulkLoadBatchedInsert}

	b.Run("plain", func(b *testing.B) {
		runCopyBench(b, db, w, table, rows, false, false)
	})
	b.Run("idempotent", func(b *testing.B) {
		runCopyBench(b, db, w, table, rows, true, false)
	})
	b.Run("idempotent_conflicts", func(b *testing.B) {
		runCopyBench(b, db, w, table, rows, true, true)
	})
}

// setupColdStartCopyBench boots the shared container, resets the test
// database, creates the target table once, and pre-generates the row
// payload. Returns an open *sql.DB pointed at the test database the
// benchmark writes into.
func setupColdStartCopyBench(b *testing.B) (db *sql.DB, table *ir.Table, rows []ir.Row) {
	b.Helper()

	dsn := sharedDSNForBench(b)

	applyDDLBench(b, dsn, benchCopyDDL)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	return db, benchCopyTable(), benchCopyRows(benchCopyRowCount)
}

// runCopyBench runs b.N iterations of one path. Each iteration resets
// the target to a known state OUTSIDE the timed region (StopTimer /
// StartTimer) so the measurement captures only the writer's INSERT
// throughput:
//
//   - idempotent=false → writeBatched (plain INSERT)
//   - idempotent=true  → writeBatchedIdempotent (ON DUPLICATE KEY UPDATE)
//   - prePopulate=true → fill the target with the same rows first, so
//     every upsert collides on the PK (the conflict/resume worst case)
//
// rows/s is reported as the total rows written divided by the timed
// wall duration; b.N scales the work so the metric stabilises.
func runCopyBench(
	b *testing.B,
	db *sql.DB,
	w *RowWriter,
	table *ir.Table,
	rows []ir.Row,
	idempotent, prePopulate bool,
) {
	b.Helper()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		// Per-iteration reset is untimed: truncate, then (for the
		// conflict case) pre-load the same rows so the upsert collides.
		b.StopTimer()
		truncateBench(b, db, table.Name)
		if prePopulate {
			if err := w.writeBatched(ctx, table, feedRows(rows)); err != nil {
				b.Fatalf("pre-populate: %v", err)
			}
		}
		b.StartTimer()

		var err error
		if idempotent {
			err = w.writeBatchedIdempotent(ctx, table, feedRows(rows))
		} else {
			err = w.writeBatched(ctx, table, feedRows(rows))
		}
		if err != nil {
			b.Fatalf("write (idempotent=%v): %v", idempotent, err)
		}
	}

	// rows/s over the timed region only. b.Elapsed() is the harness's
	// accumulated timed duration (the StopTimer windows are excluded), so
	// b.N full N-row passes / that duration is the per-row write
	// throughput on the path under test.
	nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
	if nsPerOp > 0 {
		rowsPerSec := float64(len(rows)) / (nsPerOp / 1e9)
		b.ReportMetric(rowsPerSec, "rows/s")
	}
}

// --- bench-local helpers ---------------------------------------------
//
// The shared-container helpers in shared_container_integration_test.go
// (newSharedDB / ensureSharedMySQL / resetSharedDB) are all *testing.T-
// typed and one of them calls testcontainers.SkipIfProviderIsNotHealthy,
// which requires a concrete *testing.T — so a benchmark can't reuse them
// directly. These thin helpers boot/reuse the SAME package-global
// sharedMySQL container (so a bench run in the same shard as the tests
// shares the one mysqld) but accept *testing.B.

// sharedDSNForBench boots (once, via the shared sync.Once) or reuses the
// shard's shared mysqld and returns a DSN for a freshly-reset
// sluice_test database.
func sharedDSNForBench(b *testing.B) string {
	b.Helper()
	host, port, user, password := ensureSharedMySQLForBench(b)
	resetSharedDBForBench(b, "sluice_test")
	return sharedDSN(host, port, user, password, "sluice_test")
}

// ensureSharedMySQLForBench is the *testing.B twin of ensureSharedMySQL.
// It drives the same sharedMySQL.once + retry/backoff boot and populates
// the same package globals, so a test and a benchmark in one shard share
// a single container. It skips the testcontainers health gate
// (SkipIfProviderIsNotHealthy is T-only); a missing provider surfaces as
// a boot error from bootSharedMySQLOnce, which b.Fatalf reports loudly.
func ensureSharedMySQLForBench(b *testing.B) (host, port, user, password string) {
	b.Helper()
	sharedMySQL.once.Do(func() {
		const (
			rootUser = "root"
			rootPass = "rootpw"
		)
		var lastErr error
		for attempt := 1; attempt <= sharedMySQLBootAttempts; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), sharedMySQLBootTimeout)
			hostV, portV, container, err := bootSharedMySQLOnce(ctx)
			cancel()
			if err == nil {
				sharedMySQL.host = hostV
				sharedMySQL.port = portV
				sharedMySQL.user = rootUser
				sharedMySQL.password = rootPass
				sharedMySQL.container = container
				return
			}
			lastErr = err
			if attempt < sharedMySQLBootAttempts {
				time.Sleep(sharedMySQLBootBackoff(attempt))
			}
		}
		sharedMySQL.bootErr = lastErr
	})
	if sharedMySQL.bootErr != nil {
		b.Fatalf("shared mysql unavailable: %v", sharedMySQL.bootErr)
	}
	return sharedMySQL.host, sharedMySQL.port, sharedMySQL.user, sharedMySQL.password
}

// resetSharedDBForBench is the *testing.B twin of resetSharedDB: drop +
// recreate the named database on the shared container for fresh-state
// semantics.
func resetSharedDBForBench(b *testing.B, dbName string) {
	b.Helper()
	const sharedSeedDB = "sluice_shared_seed"
	rootDSN := sharedDSN(sharedMySQL.host, sharedMySQL.port, sharedMySQL.user, sharedMySQL.password, sharedSeedDB) +
		"&multiStatements=true"
	db, err := sql.Open("mysql", rootDSN)
	if err != nil {
		b.Fatalf("reset %q: open: %v", dbName, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ddl := "DROP DATABASE IF EXISTS " + quoteIdent(dbName) +
		"; CREATE DATABASE " + quoteIdent(dbName) + " CHARACTER SET utf8mb4;"
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		b.Fatalf("reset %q: %v", dbName, err)
	}
}

func applyDDLBench(b *testing.B, dsn, ddl string) {
	b.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		b.Fatalf("apply ddl: %v", err)
	}
}

func truncateBench(b *testing.B, db *sql.DB, tableName string) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+quoteIdent(tableName)); err != nil {
		b.Fatalf("truncate %q: %v", tableName, err)
	}
}
