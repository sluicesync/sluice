//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Task #44 — hard-delete CDC family-matrix pin (MySQL source axis),
// re-derived for Bug 193.
//
// Pins ADR-0057's hard-delete matrix per the Bug-74 family-pin
// discipline. The family-dispatch under test is the MySQL binlog
// rows-event image content, which varies by --binlog-row-image
// setting:
//
//   - FULL    → images contain every column.
//   - MINIMAL → BEFORE-image only carries the PK; the UPDATE
//               AFTER-image only the changed columns.
//   - NOBLOB  → images omit unchanged BLOB/TEXT/JSON columns.
//
// Bug 88 made DELETE correct under MINIMAL/NOBLOB (PK narrowing) and
// this matrix originally pinned those cells as WORKING streams. Bug
// 193 then proved UPDATE cannot be made correct under a partial row
// image (the after-image is partial too — applying it would null out
// unchanged columns), and the production posture changed: sluice now
// REFUSES a MINIMAL/NOBLOB source at CDC start with
// SLUICE-E-CDC-ROW-IMAGE-PARTIAL, before the bulk copy (Azure Database
// for MySQL Flexible Server ships MINIMAL as its platform default, so
// the refusal is the load-bearing cell for an entire provider). The
// MINIMAL/NOBLOB cells therefore now pin the REFUSAL — loud, coded,
// target untouched — and the FULL cells keep pinning exact
// delete/update propagation. See
// internal/engines/mysql/cdc_row_image_preflight.go and
// docs/adr/adr-0057-hard-delete-semantics-across-engines.md.
//
// F18 Reddit-research triage context: silently dropping hard deletes
// was the most-cited operator pain in the dataset (Fivetran, Airbyte,
// AWS DMS all named-and-shamed). sluice does NOT silently drop them —
// on a partial-row-image source it refuses to stream at all rather
// than propagate deletes while silently losing updates.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQLBinlogWithRowImage boots a MySQL container with binlog
// enabled and --binlog-row-image set to the caller-supplied value.
// Mirror of startMySQLBinlog (streamer_resume_mysql_integration_test.go)
// with the row-image flag parameterised. Returns DSNs for
// source_db + target_db plus a cleanup.
//
// The row-image setting is the only difference from startMySQLBinlog;
// every other server flag (server-id, log-bin, binlog-format=ROW,
// net-{read,write}-timeout) matches that helper to keep streaming
// behaviour identical across the matrix cells.
func startMySQLBinlogWithRowImage(t *testing.T, rowImage string) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()

	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"mysqld",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=ROW",
					"--binlog-row-image=" + rowImage,
					"--net-write-timeout=600",
					"--net-read-timeout=600",
				},
			},
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		terminate()
		t.Fatalf("create target_db: %v", err)
	}

	tgtConn, err := buildMySQLDSN(srcConn, "target_db")
	if err != nil {
		terminate()
		t.Fatalf("build target DSN: %v", err)
	}
	return srcConn, tgtConn, terminate
}

// waitForExactRowCountMySQL is the MySQL-driver counterpart of
// waitForExactRowCount (streamer_composite_pk_delete_integration_test.go).
// Distinct from waitForRowCountMySQL (a "≥ n" rising-edge poll); the
// delete matrix needs to detect a DECREASE (e.g. 3 → 2 after CDC
// DELETE), which the inequality version can't tell apart from
// "haven't seen enough yet".
func waitForExactRowCountMySQL(dsn, table string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCountMySQL(dsn, table) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// TestStreamer_CDCDeleteMatrix_MySQLToMySQL pins the row-image matrix
// end to end:
//
//   - FULL × {plain DELETE, UPDATE-then-DELETE}: exact propagation
//     (the pre-Bug-193 behaviour, unchanged).
//   - MINIMAL / NOBLOB: the coded Bug 193 refusal at CDC start —
//     Streamer.Run returns SLUICE-E-CDC-ROW-IMAGE-PARTIAL before the
//     bulk copy, so the target is left untouched. (These cells pinned
//     working delete propagation between Bug 88 and Bug 193; the
//     posture change is deliberate — a stream that propagates deletes
//     while silently losing updates is worse than no stream.)
//
// The FULL update shapes (single-column, multi-column, PK-change) are
// pinned by TestStreamer_CDCRowImageUpdateShapes_MySQLToMySQL below.
func TestStreamer_CDCDeleteMatrix_MySQLToMySQL(t *testing.T) {
	cells := []struct {
		rowImage string
		shape    string
	}{
		{rowImage: "FULL", shape: "plain-delete"},
		{rowImage: "FULL", shape: "update-then-delete"},
		{rowImage: "MINIMAL", shape: "refusal-at-cdc-start"},
		{rowImage: "NOBLOB", shape: "refusal-at-cdc-start"},
	}

	for _, c := range cells {
		c := c
		name := fmt.Sprintf("rowimage=%s/shape=%s", c.rowImage, c.shape)
		t.Run(name, func(t *testing.T) {
			if c.shape == "refusal-at-cdc-start" {
				runMySQLToMySQLRowImageRefusalCell(t, c.rowImage)
				return
			}
			runMySQLToMySQLDeleteCell(t, c.rowImage, c.shape)
		})
	}
}

// runMySQLToMySQLRowImageRefusalCell pins the Bug 193 posture for a
// partial-row-image source: `sync` refuses loudly at CDC start with
// the stable code, BEFORE any schema or data lands on the target.
func runMySQLToMySQLRowImageRefusalCell(t *testing.T, rowImage string) {
	t.Helper()
	sourceDSN, targetDSN, cleanup := startMySQLBinlogWithRowImage(t, rowImage)
	defer cleanup()

	applyMySQLDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id   BIGINT      NOT NULL,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "row-image-refusal-" + rowImage,
	}

	runCtx, runCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer runCancel()
	err := streamer.Run(runCtx)
	if err == nil {
		t.Fatalf("Streamer.Run accepted a binlog_row_image=%s source; want the coded Bug 193 refusal", rowImage)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
		t.Fatalf("Streamer.Run error: want %s; got %T: %v", sluicecode.CodeCDCRowImagePartial, err, err)
	}

	// The refusal fires at the snapshot open — before the target
	// writers exist — so the target database must be untouched.
	if mysqlTableExists(t, targetDSN, "widgets") {
		t.Errorf("target widgets table exists after the refusal; the refusal must precede any target mutation")
	}
}

// mysqlTableExists reports whether the named table exists in the DSN's
// default database, via information_schema (a missing table and a
// zero-row table must be distinguishable for the refusal pin above).
func mysqlTableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	err = db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table,
	).Scan(&n)
	if err != nil {
		t.Fatalf("table-exists probe: %v", err)
	}
	return n > 0
}

// TestStreamer_CDCRowImageUpdateShapes_MySQLToMySQL pins the FULL-row-
// image UPDATE column of the Bug 193 matrix end to end: single-column,
// multi-column, and PK-change UPDATEs must all converge to exact
// values on the target. This is the regression pin for the Bug 193
// Before-image PK narrowing (internal/engines/mysql/cdc_reader.go
// filterBeforeToPK on the UPDATE arm): a narrowing bug in any shape —
// a WHERE that over- or under-matches, or a PK-change UPDATE losing
// the old-key row — fails a value assertion loudly.
func TestStreamer_CDCRowImageUpdateShapes_MySQLToMySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlogWithRowImage(t, "FULL")
	defer cleanup()

	applyMySQLDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id    BIGINT      NOT NULL,
			name  VARCHAR(64) NOT NULL,
			qty   INT         NOT NULL,
			note  VARCHAR(64) NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (id, name, qty, note) VALUES
			(1, 'one',   10, NULL),
			(2, 'two',   20, 'keep'),
			(3, 'three', 30, NULL);
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "row-image-update-shapes-FULL",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCountMySQL(targetDSN, "widgets", 3, 90*time.Second) {
		t.Fatalf("bulk copy never delivered the 3 seed rows (got %d)", pollRowCountMySQL(targetDSN, "widgets"))
	}

	// The three UPDATE shapes. Row 1: single-column. Row 2:
	// multi-column (the exact shape the Azure probe watched silently
	// no-op under MINIMAL — 11 of its 12 lost UPDATEs were
	// multi-column). Row 3: PK-change (the narrowed Before must carry
	// the OLD key so the old row migrates rather than duplicating).
	applyMySQLDDL(t, sourceDSN, `
		UPDATE widgets SET qty = 11 WHERE id = 1;
		UPDATE widgets SET name = 'TWO', qty = 22, note = NULL WHERE id = 2;
		UPDATE widgets SET id = 33 WHERE id = 3;
	`)

	wantRow := func(id int64, wantName string, wantQty int64, wantNote *string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			name, qty, note, err := readWidgetRowMySQL(targetDSN, id)
			if err == nil && name == wantName && qty == wantQty &&
				((note == nil) == (wantNote == nil)) && (note == nil || *note == *wantNote) {
				return
			}
			lastErr = err
			time.Sleep(200 * time.Millisecond)
		}
		name, qty, note, err := readWidgetRowMySQL(targetDSN, id)
		t.Fatalf("target row id=%d never converged: got (name=%q qty=%d note=%v, readErr=%v/%v); want (name=%q qty=%d note=%v)",
			id, name, qty, note, err, lastErr, wantName, wantQty, wantNote)
	}

	wantRow(1, "one", 11, nil)
	wantRow(2, "TWO", 22, nil)
	wantRow(33, "three", 30, nil)

	// The PK-change UPDATE must have MOVED row 3, not copied it.
	if !waitForExactRowCountMySQL(targetDSN, "widgets", 3, 30*time.Second) {
		t.Fatalf("row count after PK-change UPDATE = %d; want 3 (old-key row must be gone)",
			pollRowCountMySQL(targetDSN, "widgets"))
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// readWidgetRowMySQL reads (name, qty, note) for the widgets row with
// the given id on the target, for the update-shape convergence pins.
func readWidgetRowMySQL(dsn string, id int64) (name string, qty int64, note *string, err error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", 0, nil, err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = db.QueryRowContext(ctx, "SELECT name, qty, note FROM widgets WHERE id = ?", id).Scan(&name, &qty, &note)
	return name, qty, note, err
}

// runMySQLToMySQLDeleteCell is the per-cell driver for the FULL delete
// shapes. Extracted so the matrix iteration stays readable. (The
// pre-Bug-193 blob/toast variant existed for the NOBLOB cells, which
// now pin the refusal instead — a NOBLOB stream never runs.)
func runMySQLToMySQLDeleteCell(t *testing.T, rowImage, shape string) {
	t.Helper()
	sourceDSN, targetDSN, cleanup := startMySQLBinlogWithRowImage(t, rowImage)
	defer cleanup()

	// Seed rows. The matrix only DELETEs id=2; ids 1 and 3 are
	// witnesses that the WHERE clause didn't over-match.
	applyMySQLDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id   BIGINT      NOT NULL,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  fmt.Sprintf("delete-matrix-mysql-%s-%s", rowImage, shape),
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for bulk copy to land the 3 seed rows.
	if !waitForExactRowCountMySQL(targetDSN, "widgets", 3, 90*time.Second) {
		t.Fatalf("bulk copy never delivered the 3 seed rows (got %d)", pollRowCountMySQL(targetDSN, "widgets"))
	}

	// Now exercise the shape under test.
	switch shape {
	case "plain-delete":
		applyMySQLDDL(t, sourceDSN, "DELETE FROM widgets WHERE id = 2;")
		if !waitForExactRowCountMySQL(targetDSN, "widgets", 2, 30*time.Second) {
			t.Fatalf("plain DELETE never propagated to target (binlog_row_image=%s); target widget rows = %d, want 2",
				rowImage, pollRowCountMySQL(targetDSN, "widgets"))
		}

	case "update-then-delete":
		// UPDATE the row, THEN delete it. The UPDATE emits an event
		// that varies by row-image; the DELETE must still propagate
		// regardless. Final state: row absent.
		applyMySQLDDL(t, sourceDSN, "UPDATE widgets SET name = 'TWO_UPDATED' WHERE id = 2;")
		applyMySQLDDL(t, sourceDSN, "DELETE FROM widgets WHERE id = 2;")
		if !waitForExactRowCountMySQL(targetDSN, "widgets", 2, 30*time.Second) {
			t.Fatalf("UPDATE-then-DELETE never settled at row count 2 (binlog_row_image=%s); target widget rows = %d",
				rowImage, pollRowCountMySQL(targetDSN, "widgets"))
		}

	default:
		t.Fatalf("unknown shape %q", shape)
	}

	// Witness assertion: ids 1 and 3 are still present (the WHERE
	// clause didn't over-match). A regression that emits the same
	// WHERE for every row (e.g. dropping the PK predicate entirely)
	// would fail this check.
	if !mysqlRowExistsByID(t, streamCtx, targetDSN, "widgets", 1) {
		t.Errorf("witness row id=1 unexpectedly gone from target")
	}
	if !mysqlRowExistsByID(t, streamCtx, targetDSN, "widgets", 3) {
		t.Errorf("witness row id=3 unexpectedly gone from target")
	}
	if mysqlRowExistsByID(t, streamCtx, targetDSN, "widgets", 2) {
		t.Errorf("deleted row id=2 still present on target (silent-drop regression?)")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_CDCDeleteMatrix_MySQLToPostgres is the cross-engine
// sanity cell from the Task #44 matrix. Full matrix on cross-engine
// would be overkill (testcontainer-pairs are slower); one representative
// shape × representative setting verifies the cross-engine apply path
// still propagates DELETEs.
//
// Representative cell: binlog_row_image=FULL × plain DELETE.
func TestStreamer_CDCDeleteMatrix_MySQLToPostgres(t *testing.T) {
	mysqlSourceDSN, _, mysqlCleanup := startMySQLBinlogWithRowImage(t, "FULL")
	defer mysqlCleanup()

	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE widgets (
			id   BIGINT       NOT NULL,
			name VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');
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
		StreamID:  "delete-matrix-mysql-to-pg-FULL",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCount(pgTargetDSN, "widgets", 3, 90*time.Second) {
		t.Fatalf("bulk copy never delivered the 3 seed rows to PG (got %d)", pollRowCount(pgTargetDSN, "widgets"))
	}

	applyMySQLDDL(t, mysqlSourceDSN, "DELETE FROM widgets WHERE id = 2;")
	if !waitForExactRowCount(pgTargetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("cross-engine CDC DELETE never propagated to PG target; rows = %d, want 2",
			pollRowCount(pgTargetDSN, "widgets"))
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// mysqlRowExistsByID returns true when a row with the given integer id
// exists in the named MySQL table on the target. Used as a witness
// check that DELETE WHERE clauses don't over-match.
func mysqlRowExistsByID(t *testing.T, ctx context.Context, dsn, table string, id int) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var exists bool
	q := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = ?)", table)
	if err := db.QueryRowContext(c, q, id).Scan(&exists); err != nil {
		return false
	}
	return exists
}
