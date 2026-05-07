//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end regression for Bug 8: a composite-PK DELETE on a
// Postgres source under REPLICA IDENTITY DEFAULT must reach the
// destination. Pre-fix, the CDC reader emitted a Before image that
// included non-key columns (set to nil because pgoutput's 'K'
// OldTuple sends 'n' markers for those positions). The applier's
// WHERE clause then included "non_key IS NULL" predicates that
// matched zero rows on the destination, ADR-0010 swallowed the
// zero-rows-affected, and the DELETE silently disappeared.
//
// This test exercises the full Streamer pipeline (source PG → target
// PG, real ChangeApplier, real CDC reader) so the regression is
// pinned at the integration boundary the user actually exercises.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
)

// TestStreamer_PostgresToPostgres_CompositePKDelete is the Bug 8
// end-to-end regression. It seeds a composite-PK table on the
// source under server-default REPLICA IDENTITY (DEFAULT), runs the
// streamer, issues a DELETE on the source, and asserts the row
// disappears from the destination within the deadline.
func TestStreamer_PostgresToPostgres_CompositePKDelete(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE order_items (
			order_id    BIGINT       NOT NULL,
			line_no     SMALLINT     NOT NULL,
			qty         INTEGER      NOT NULL,
			unit_price  NUMERIC(12,4) NOT NULL,
			PRIMARY KEY (order_id, line_no)
		);
		-- Intentionally no ALTER TABLE ... REPLICA IDENTITY: the
		-- server-default 'd' (DEFAULT) is the case that Bug 8
		-- found on user testing.

		INSERT INTO order_items (order_id, line_no, qty, unit_price) VALUES
			(100, 1, 5, 9.99),
			(100, 2, 3, 1.50),
			(101, 1, 1, 19.99);
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-composite-pk-delete",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Bulk copy must land all 3 rows on the target before we exercise CDC.
	if !waitForExactRowCount(targetDSN, "order_items", 3, 60*time.Second) {
		t.Fatalf("bulk copy never delivered the 3 seed rows to the target (got %d)", pollRowCount(targetDSN, "order_items"))
	}

	// CDC DELETE on a composite PK. Pre-fix this propagates as a no-op
	// on the target — the destination row count stays at 3.
	applyDDL(t, sourceDSN,
		"DELETE FROM order_items WHERE order_id = 100 AND line_no = 1;")

	if !waitForExactRowCount(targetDSN, "order_items", 2, 30*time.Second) {
		t.Fatalf("CDC DELETE never propagated: target order_items rows = %d; want 2 within 30s", pollRowCount(targetDSN, "order_items"))
	}

	// Spot-check the right row was deleted: (100, 2) and (101, 1) survive.
	if !rowExistsCompositeKey(t, targetDSN, "order_items", 100, 2) {
		t.Errorf("expected (100, 2) to remain on the target")
	}
	if !rowExistsCompositeKey(t, targetDSN, "order_items", 101, 1) {
		t.Errorf("expected (101, 1) to remain on the target")
	}
	if rowExistsCompositeKey(t, targetDSN, "order_items", 100, 1) {
		t.Errorf("expected (100, 1) to be gone from the target")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// waitForExactRowCount polls the target DSN's named table for an
// exact row count, returning true on a match within the deadline.
// Distinct from the package's waitForRowCount (a "≥ n" rising-edge
// poll); the Bug 8 regression has to detect a decrease (3 → 2 after
// the CDC DELETE), which the inequality version can't tell apart
// from "haven't seen enough yet".
//
// Uses pollRowCount (returns 0 on any query error) rather than
// countRows so the helper tolerates the pre-bulk-copy window where
// the target table doesn't yet exist.
func waitForExactRowCount(dsn, table string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCount(dsn, table) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// rowExistsCompositeKey returns true when a row matching the
// (order_id, line_no) composite key is present in `table`.
func rowExistsCompositeKey(t *testing.T, dsn, table string, orderID, lineNo int) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	err = db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM "+table+" WHERE order_id = $1 AND line_no = $2)",
		orderID, lineNo,
	).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}
