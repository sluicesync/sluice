//go:build integration

// Cross-engine composite-PK regression for the CDC streamer path:
// MySQL source → Postgres target. Companion to
// streamer_composite_pk_delete_integration_test.go (PG → PG, the Bug 8
// fix's primary regression test) — this file pins the cross-engine
// surface where MySQL CDC produces a Delete event that has to land
// cleanly on a PG applier.
//
// MySQL's row-based binlog with binlog_row_image=FULL is unaffected by
// the protocol-detail issue that caused Bug 8 on PG (DELETE
// before-images carry every column with real values, not 'n' markers).
// The PG applier's WHERE clause is built from the Before image; if the
// MySQL reader ever started dropping a PK column on Delete events, this
// test would catch the regression at the cross-engine boundary.

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_MySQLToPostgres_CompositePKDelete seeds a composite-PK
// table on a MySQL source, runs the streamer to a PG target, deletes
// one row on the source, and asserts the row count drops on the
// target.
//
// The schema mirrors the order_items shape used in the large-test
// suite (workspace/large/mysql_schema.sql in the testing repo) so a
// future end-to-end repro can reuse the same column names.
func TestStreamer_MySQLToPostgres_CompositePKDelete(t *testing.T) {
	mysqlSourceDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE order_items (
			order_id    BIGINT       NOT NULL,
			line_no     INT          NOT NULL,
			product_id  BIGINT       NOT NULL,
			qty         INT          NOT NULL,
			PRIMARY KEY (order_id, line_no)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO order_items (order_id, line_no, product_id, qty) VALUES
			(100, 1, 5001, 5),
			(100, 2, 5002, 3),
			(101, 1, 5003, 1);
	`
	applyMySQLDDL(t, mysqlSourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSourceDSN,
		TargetDSN: pgTargetDSN,
		StreamID:  "test-cross-composite-pk-delete",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Bulk copy must land all 3 rows on the target before we exercise CDC.
	if !waitForExactRowCount(pgTargetDSN, "order_items", 3, 60*time.Second) {
		t.Fatalf("bulk copy never delivered the 3 seed rows to the target (got %d)", pollRowCount(pgTargetDSN, "order_items"))
	}

	// CDC DELETE on a composite-PK row. If the MySQL CDC reader
	// ever drops one of the PK columns from Delete.Before, the PG
	// applier's WHERE clause is incomplete and matches no rows;
	// destination row count stays at 3 and this test fails.
	applyMySQLDDL(t, mysqlSourceDSN,
		"DELETE FROM order_items WHERE order_id = 100 AND line_no = 1;")

	if !waitForExactRowCount(pgTargetDSN, "order_items", 2, 30*time.Second) {
		t.Fatalf("CDC DELETE never propagated cross-engine: target order_items rows = %d; want 2 within 30s", pollRowCount(pgTargetDSN, "order_items"))
	}

	// Spot-check the right row was deleted.
	if !rowExistsCompositeKey(t, pgTargetDSN, "order_items", 100, 2) {
		t.Errorf("expected (100, 2) to remain on the target")
	}
	if !rowExistsCompositeKey(t, pgTargetDSN, "order_items", 101, 1) {
		t.Errorf("expected (101, 1) to remain on the target")
	}
	if rowExistsCompositeKey(t, pgTargetDSN, "order_items", 100, 1) {
		t.Errorf("expected (100, 1) to be gone from the target")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
