//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Task #44 — hard-delete CDC family-matrix pin (MySQL source axis).
//
// Pins ADR-0057's hard-delete matrix per the Bug-74 family-pin
// discipline. The family-dispatch under test is the MySQL binlog
// rows-event BEFORE-image content, which varies by
// --binlog-row-image setting:
//
//   - FULL    → BEFORE-image contains every column.
//   - MINIMAL → BEFORE-image only carries the PK (and changed columns
//               for UPDATE; for DELETE, only the PK).
//   - NOBLOB  → BEFORE-image carries every column except BLOB/TEXT/JSON.
//
// A regression in the "always-emit-DELETE" property could surface
// per setting (e.g. MINIMAL might drop the row because the apply-side
// WHERE clause is malformed when only PK is present; NOBLOB might
// fail to construct WHERE if the apply path tried to use the absent
// BLOB column in WHERE). The matrix exists precisely so the next
// codec-dispatched regression in any cell fails LOUDLY. See
// internal/engines/mysql/cdc_reader.go:708-724 (emit path), and
// docs/adr/adr-0057-hard-delete-semantics-across-engines.md.
//
// F18 Reddit-research triage context: silently dropping hard deletes
// was the most-cited operator pain in the dataset (Fivetran, Airbyte,
// AWS DMS all named-and-shamed). sluice does NOT silently drop them;
// this pin keeps that promise honest across configuration cells.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"

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

// TestStreamer_CDCDeleteMatrix_MySQLToMySQL pins the hard-delete
// always-propagates property across {FULL, MINIMAL, NOBLOB} ×
// {plain DELETE, UPDATE-then-DELETE, DELETE of a row with TOAST'd
// BLOB column (NOBLOB-only — that's the interesting cell)}.
//
// Bug 88 closure: the MINIMAL and NOBLOB-toast cells were previously
// `t.Skip()`'d with a "sluice requires binlog_row_image=FULL"
// finding from Task #44. The CDC reader now narrows the DELETE
// Before-image to PK columns before emit (see
// `internal/engines/mysql/cdc_reader.go` `filterDeleteBefore`),
// mirroring the PG-side helper of the same name. All matrix cells
// now run; a regression that drops the narrowing would fail one or
// more of the previously-skipped cells loudly.
func TestStreamer_CDCDeleteMatrix_MySQLToMySQL(t *testing.T) {
	const big = 100 * 1024 // 100KB MEDIUMTEXT — exercise NOBLOB's drop-blob behaviour.

	cells := []struct {
		rowImage string
		shape    string
		// includeBlob picks the schema variant: when true, the
		// `payload` column is added and seeded with a 100KB string.
		// shape=="toast-delete" requires includeBlob=true.
		includeBlob bool
		// skipReason, when non-empty, makes this cell a documented
		// skip with the production behaviour spelled out. Currently
		// empty for every cell after the Bug 88 fix landed; kept as
		// a field so a future configuration cell that re-introduces
		// a skip has a place to put the reason.
		skipReason string
	}{
		{rowImage: "FULL", shape: "plain-delete"},
		{rowImage: "FULL", shape: "update-then-delete"},
		{rowImage: "MINIMAL", shape: "plain-delete"},
		{rowImage: "MINIMAL", shape: "update-then-delete"},
		{rowImage: "NOBLOB", shape: "plain-delete"},
		{rowImage: "NOBLOB", shape: "update-then-delete"},
		{rowImage: "NOBLOB", shape: "toast-delete", includeBlob: true},
	}

	for _, c := range cells {
		c := c
		name := fmt.Sprintf("rowimage=%s/shape=%s", c.rowImage, c.shape)
		t.Run(name, func(t *testing.T) {
			if c.skipReason != "" {
				t.Skip(c.skipReason)
			}
			runMySQLToMySQLDeleteCell(t, c.rowImage, c.shape, c.includeBlob, big)
		})
	}
}

// runMySQLToMySQLDeleteCell is the per-cell driver. Extracted so the
// matrix iteration stays readable.
func runMySQLToMySQLDeleteCell(t *testing.T, rowImage, shape string, includeBlob bool, blobLen int) {
	t.Helper()
	sourceDSN, targetDSN, cleanup := startMySQLBinlogWithRowImage(t, rowImage)
	defer cleanup()

	cols := "id BIGINT NOT NULL, name VARCHAR(64) NOT NULL, PRIMARY KEY (id)"
	if includeBlob {
		// MEDIUMTEXT (16MB max) rather than TEXT (64KB max) so the
		// 100KB payload fits. MEDIUMTEXT still triggers the binlog
		// MINIMAL/NOBLOB "drop BLOB columns from BEFORE-image"
		// behaviour the same way TEXT would.
		cols = "id BIGINT NOT NULL, name VARCHAR(64) NOT NULL, payload MEDIUMTEXT, PRIMARY KEY (id)"
	}
	seedDDL := fmt.Sprintf(
		"CREATE TABLE widgets (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
		cols,
	)

	// Seed rows. The matrix only DELETEs id=2; ids 1 and 3 are
	// witnesses that the WHERE clause didn't over-match.
	seedInserts := "INSERT INTO widgets (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');"
	if includeBlob {
		bigVal := strings.Repeat("x", blobLen)
		// Seed id=2 with the big payload — that's the row we'll
		// DELETE on the source; under NOBLOB the BEFORE-image won't
		// carry payload at all.
		seedInserts = fmt.Sprintf(
			"INSERT INTO widgets (id, name, payload) VALUES "+
				"(1, 'one', NULL), (2, 'two', '%s'), (3, 'three', NULL);",
			bigVal,
		)
	}
	applyMySQLDDL(t, sourceDSN, seedDDL+seedInserts)

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

	case "toast-delete":
		// DELETE a row whose 100KB payload is absent from the NOBLOB
		// BEFORE-image. The PK-only WHERE clause must still match.
		applyMySQLDDL(t, sourceDSN, "DELETE FROM widgets WHERE id = 2;")
		if !waitForExactRowCountMySQL(targetDSN, "widgets", 2, 30*time.Second) {
			t.Fatalf("DELETE of TOAST'd row never propagated under binlog_row_image=%s; target widget rows = %d",
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
