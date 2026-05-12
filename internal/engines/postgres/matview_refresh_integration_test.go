//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Phase 2 of view support (`docs/dev/roadmap.md` item 13).
// `sluice matview refresh` is the operator-cadence-agnostic subcommand
// that drives `REFRESH MATERIALIZED VIEW [CONCURRENTLY]` on the
// target. These tests verify the end-to-end flow against a real
// PostgreSQL container — the unit tests in matview_refresh_test.go
// cover the SQL builder + filter logic in isolation.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestMatviewRefresh_PlainRefresh exercises the load-bearing end-to-
// end path: a matview built from a source table; insert rows into
// the source; the matview's row count stays at its create-time
// snapshot; `RefreshMatviews` brings it current. Pin the row count
// pre and post.
func TestMatviewRefresh_PlainRefresh(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const seedDDL = `
		CREATE TABLE base (id INT PRIMARY KEY, val INT NOT NULL);
		INSERT INTO base (id, val) VALUES (1, 100), (2, 200);
		CREATE MATERIALIZED VIEW base_sum AS
		    SELECT COUNT(*) AS n, SUM(val) AS total FROM base;
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Add more rows after the matview is created — they're NOT in
	// the matview yet.
	if _, err := db.ExecContext(ctx, "INSERT INTO base (id, val) VALUES (3, 300), (4, 400)"); err != nil {
		t.Fatalf("insert post-create: %v", err)
	}

	// Pre-refresh: matview's row count + total reflect only the 2
	// rows present at create time.
	var preN, preTotal int
	if err := db.QueryRowContext(ctx, "SELECT n, total FROM base_sum").Scan(&preN, &preTotal); err != nil {
		t.Fatalf("pre-refresh query: %v", err)
	}
	if preN != 2 || preTotal != 300 {
		t.Errorf("pre-refresh matview: n=%d total=%d; want n=2 total=300", preN, preTotal)
	}

	// Run the refresh.
	result, err := RefreshMatviews(ctx, db, MatviewRefreshOptions{
		Schema: "public",
	})
	if err != nil {
		t.Fatalf("RefreshMatviews: %v", err)
	}
	if len(result.Refreshed) != 1 {
		t.Errorf("Refreshed = %d; want 1", len(result.Refreshed))
	}
	if len(result.Refreshed) > 0 && result.Refreshed[0].Name != "base_sum" {
		t.Errorf("Refreshed[0].Name = %q; want base_sum", result.Refreshed[0].Name)
	}
	if len(result.Skipped) != 0 {
		t.Errorf("Skipped = %d; want 0", len(result.Skipped))
	}

	// Post-refresh: matview reflects all 4 rows.
	var postN, postTotal int
	if err := db.QueryRowContext(ctx, "SELECT n, total FROM base_sum").Scan(&postN, &postTotal); err != nil {
		t.Fatalf("post-refresh query: %v", err)
	}
	if postN != 4 || postTotal != 1000 {
		t.Errorf("post-refresh matview: n=%d total=%d; want n=4 total=1000", postN, postTotal)
	}
}

// TestMatviewRefresh_Concurrently exercises the CONCURRENTLY path.
// Requires a unique index on the matview; the test seeds one. Pin
// the same pre/post row-count round-trip as the plain refresh.
func TestMatviewRefresh_Concurrently(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const seedDDL = `
		CREATE TABLE items (id INT PRIMARY KEY, category TEXT NOT NULL, price NUMERIC NOT NULL);
		INSERT INTO items VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30);
		CREATE MATERIALIZED VIEW items_by_category AS
		    SELECT category, COUNT(*) AS n, SUM(price) AS total
		    FROM items GROUP BY category;
		CREATE UNIQUE INDEX items_by_category_pk ON items_by_category (category);
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Add a row that lands in a new category.
	if _, err := db.ExecContext(ctx, "INSERT INTO items VALUES (4, 'c', 40)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := RefreshMatviews(ctx, db, MatviewRefreshOptions{
		Schema:       "public",
		Concurrently: true,
	})
	if err != nil {
		t.Fatalf("RefreshMatviews: %v", err)
	}
	if len(result.Refreshed) != 1 {
		t.Errorf("Refreshed = %d; want 1", len(result.Refreshed))
	}
	if len(result.Skipped) != 0 {
		t.Errorf("Skipped = %d; want 0 (matview has unique index)", len(result.Skipped))
	}

	// Pin: post-refresh the matview reflects 3 categories.
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM items_by_category").Scan(&n); err != nil {
		t.Fatalf("post-refresh query: %v", err)
	}
	if n != 3 {
		t.Errorf("post-refresh category count = %d; want 3", n)
	}
}

// TestMatviewRefresh_ConcurrentlyWithoutUniqueIndex_Skipped pins the
// loud-failure path for concurrent refresh against a matview that
// has no unique index: the matview is skipped with a clear reason
// naming the missing index, and the operator sees an actionable
// error.
func TestMatviewRefresh_ConcurrentlyWithoutUniqueIndex_Skipped(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const seedDDL = `
		CREATE TABLE base (id INT PRIMARY KEY, val INT NOT NULL);
		INSERT INTO base VALUES (1, 10);
		CREATE MATERIALIZED VIEW base_sum AS SELECT SUM(val) AS total FROM base;
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	result, err := RefreshMatviews(ctx, db, MatviewRefreshOptions{
		Schema:       "public",
		Concurrently: true,
	})
	if err != nil {
		t.Fatalf("RefreshMatviews: %v", err)
	}
	if len(result.Refreshed) != 0 {
		t.Errorf("Refreshed = %d; want 0 (skipped for missing unique index)", len(result.Refreshed))
	}
	if len(result.Skipped) != 1 {
		t.Fatalf("Skipped = %d; want 1", len(result.Skipped))
	}
	if result.Skipped[0].Name != "base_sum" {
		t.Errorf("Skipped[0].Name = %q; want base_sum", result.Skipped[0].Name)
	}
	if !strings.Contains(result.Skipped[0].Reason, "unique index") {
		t.Errorf("Skipped[0].Reason = %q; want 'unique index' hint", result.Skipped[0].Reason)
	}
}

// TestMatviewRefresh_FilteredByName pins the --matview filter:
// refreshing only one of two matviews leaves the other stale.
func TestMatviewRefresh_FilteredByName(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const seedDDL = `
		CREATE TABLE base (id INT PRIMARY KEY, val INT NOT NULL);
		INSERT INTO base VALUES (1, 10);
		CREATE MATERIALIZED VIEW mv_a AS SELECT COUNT(*) AS n FROM base;
		CREATE MATERIALIZED VIEW mv_b AS SELECT COUNT(*) AS n FROM base;
		INSERT INTO base VALUES (2, 20);
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Refresh only mv_a.
	result, err := RefreshMatviews(ctx, db, MatviewRefreshOptions{
		Schema:   "public",
		Matviews: []string{"mv_a"},
	})
	if err != nil {
		t.Fatalf("RefreshMatviews: %v", err)
	}
	if len(result.Refreshed) != 1 || result.Refreshed[0].Name != "mv_a" {
		t.Errorf("Refreshed = %v; want one entry for mv_a", result.Refreshed)
	}

	// mv_a is current (2 rows); mv_b is stale (1 row).
	var nA, nB int
	if err := db.QueryRowContext(ctx, "SELECT n FROM mv_a").Scan(&nA); err != nil {
		t.Fatalf("mv_a query: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT n FROM mv_b").Scan(&nB); err != nil {
		t.Fatalf("mv_b query: %v", err)
	}
	if nA != 2 {
		t.Errorf("mv_a.n = %d; want 2 (refreshed)", nA)
	}
	if nB != 1 {
		t.Errorf("mv_b.n = %d; want 1 (NOT refreshed — filter excluded)", nB)
	}
}

// TestMatviewRefresh_MissingMatview_LoudFailure pins the loud-
// failure-on-typo path: a requested matview that doesn't exist
// surfaces as an actionable error before any REFRESH runs.
func TestMatviewRefresh_MissingMatview_LoudFailure(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const seedDDL = `
		CREATE TABLE base (id INT PRIMARY KEY);
		CREATE MATERIALIZED VIEW present AS SELECT COUNT(*) FROM base;
	`
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err = RefreshMatviews(ctx, db, MatviewRefreshOptions{
		Schema:   "public",
		Matviews: []string{"missing_matview"},
	})
	if err == nil {
		t.Fatal("err = nil; want error for missing matview name")
	}
	if !strings.Contains(err.Error(), "missing_matview") {
		t.Errorf("err = %v; want missing name in message", err)
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v; want 'not found' wording", err)
	}
}
