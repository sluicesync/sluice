//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Task #42: cross-engine case-sensitivity test matrix — MySQL → MySQL
// path.
//
// What this pins (the "always backtick, preserve verbatim" invariant):
//
//   - MySQL sources reading information_schema.tables.table_name into
//     ir.Table.Name without case-folding (engines/mysql/schema_reader.go
//     :143-238; only data_type gets LOWER(), correctly).
//   - MySQL writers (ddl_emit.go emitTableDef → backtick-quoted with
//     doubled-backtick escaping, row_writer.go:426-437 INSERTs).
//   - MySQL change_applier.go quoting on every CDC INSERT/UPDATE/
//     DELETE/TRUNCATE.
//   - MySQL cdc_reader.go (binlog QUERY-event path) preserving the
//     table identifier verbatim.
//
// Critical infra decision: this test forces the MySQL container to
// `lower_case_table_names=0` explicitly. Linux MySQL defaults to 0
// already, but making it explicit is the difference between a hermetic
// test and one that silently degrades if the testcontainer image
// changes its default — and the test name itself states the
// case-sensitivity claim, so the underlying server has to match.
//
// Scope (Phase A discipline): only QUOTED source DDL. Unquoted DDL
// behaviour on MySQL is controlled by lower_case_table_names; explicit
// backtick quoting is the operator's unambiguous "preserve case"
// intent.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

// startMySQLCaseSensitive boots a MySQL container with
// --lower-case-table-names=0 (the load-bearing piece for the
// case-preservation matrix) and binlog disabled. The existing startMySQL
// helper relies on the testcontainers default Cmd, which doesn't pin
// the value — so this test gets its own helper rather than mutating
// shared infrastructure.
//
// Returns DSNs for source_db + target_db plus a cleanup. Mirrors
// startMySQL's shape.
func startMySQLCaseSensitive(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
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
					// The load-bearing flag: 0 = identifiers stored as
					// declared, case-sensitive comparison (the Linux
					// default but pinned explicitly so the test is
					// hermetic). 1 (Windows/macOS default) would fold
					// every identifier to lowercase on disk; this test
					// wouldn't even be expressible.
					"--lower-case-table-names=0",
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

// startMySQLBinlogCaseSensitive boots a MySQL container with both
// --lower-case-table-names=0 AND binlog enabled (for the streamer/CDC
// branch of the matrix). Mirror of startMySQLBinlog in
// streamer_resume_mysql_integration_test.go but with the LCTN=0 flag
// added. The matrix needs LCTN=0 on every MySQL side — source or
// target — so the test name's case-sensitivity claim is hermetic.
func startMySQLBinlogCaseSensitive(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
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
					"--lower-case-table-names=0",
					"--server-id=1",
					"--log-bin=mysql-bin",
					"--binlog-format=ROW",
					"--binlog-row-image=FULL",
					// Mirror the timeouts from the existing
					// startMySQLBinlog for parity with that test's
					// streaming behaviour.
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

// TestMigrate_CasePreservation_MySQLToMySQL pins same-engine MySQL case
// preservation on the bulk-copy (Migrator) path. For each shape:
// create source table with backtick-quoted identifiers, seed two rows,
// run the orchestrator, assert target table+column case-preserved AND
// row data intact.
func TestMigrate_CasePreservation_MySQLToMySQL(t *testing.T) {
	for _, shape := range caseShapes {
		shape := shape
		t.Run(shape.label, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startMySQLCaseSensitive(t)
			defer cleanup()

			seedDDL := fmt.Sprintf(
				"CREATE TABLE `%s` ("+
					"id BIGINT NOT NULL AUTO_INCREMENT, "+
					"`%s` VARCHAR(255) NOT NULL, "+
					"PRIMARY KEY (id)"+
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; "+
					"INSERT INTO `%s` (`%s`) VALUES ('alice'), ('bob');",
				shape.table, shape.column, shape.table, shape.column,
			)
			applyMySQLDDL(t, sourceDSN, seedDDL)

			mysqlEng, ok := engines.Get("mysql")
			if !ok {
				t.Fatal("mysql engine not registered")
			}

			mig := &Migrator{
				Source:    mysqlEng,
				Target:    mysqlEng,
				SourceDSN: sourceDSN,
				TargetDSN: targetDSN,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("Migrator.Run: %v", err)
			}

			assertMySQLTableCasePreserved(t, ctx, targetDSN, shape)
			assertMySQLRowsPreserved(t, ctx, targetDSN, shape, 2)
		})
	}
}

// TestStreamer_CasePreservation_MySQLToMySQL pins same-engine MySQL
// case preservation on the CDC path. Cold-starts a Streamer, waits for
// bulk copy, INSERTs a third row on the source, verifies it lands on
// the case-preserved target table via CDC.
func TestStreamer_CasePreservation_MySQLToMySQL(t *testing.T) {
	for _, shape := range caseShapes {
		shape := shape
		t.Run(shape.label, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startMySQLBinlogCaseSensitive(t)
			defer cleanup()

			seedDDL := fmt.Sprintf(
				"CREATE TABLE `%s` ("+
					"id BIGINT NOT NULL AUTO_INCREMENT, "+
					"`%s` VARCHAR(255) NOT NULL, "+
					"PRIMARY KEY (id)"+
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; "+
					"INSERT INTO `%s` (`%s`) VALUES ('alice'), ('bob');",
				shape.table, shape.column, shape.table, shape.column,
			)
			applyMySQLDDL(t, sourceDSN, seedDDL)

			mysqlEng, ok := engines.Get("mysql")
			if !ok {
				t.Fatal("mysql engine not registered")
			}

			streamer := &Streamer{
				Source:    mysqlEng,
				Target:    mysqlEng,
				SourceDSN: sourceDSN,
				TargetDSN: targetDSN,
				StreamID:  "test-case-pres-mysql-" + shape.label,
			}
			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()
			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			if !waitForRowCountMySQLQuoted(t, targetDSN, shape.table, 2, 60*time.Second) {
				t.Fatalf("bulk copy never delivered seed rows to target `%s`", shape.table)
			}

			assertMySQLTableCasePreserved(t, streamCtx, targetDSN, shape)

			// INSERT a third row on the source — flows through CDC.
			applyMySQLDDL(t, sourceDSN, fmt.Sprintf(
				"INSERT INTO `%s` (`%s`) VALUES ('carol');",
				shape.table, shape.column,
			))
			if !waitForRowCountMySQLQuoted(t, targetDSN, shape.table, 3, 30*time.Second) {
				t.Fatalf("CDC never delivered post-snapshot INSERT to `%s`", shape.table)
			}

			// Verify carol landed at the case-preserved column name on
			// the case-preserved table.
			if !mysqlRowExistsByValueQuoted(t, streamCtx, targetDSN, shape.table, shape.column, "carol") {
				t.Errorf("CDC row for 'carol' not found at `%s`.`%s` on target", shape.table, shape.column)
			}

			streamCancel()
			select {
			case <-runErr:
			case <-time.After(15 * time.Second):
				t.Fatal("Streamer.Run did not return after ctx cancel")
			}
		})
	}
}

// assertMySQLTableCasePreserved queries the MySQL target's
// information_schema for the given case-preserved table + column and
// fails the test if either is missing. Case-sensitive equality (`=`)
// against the operator-supplied identifier — this is the whole point.
//
// On a `lower_case_table_names=0` server (which startMySQLCaseSensitive
// guarantees), the information_schema query treats names
// case-sensitively. If sluice's writer dropped case, the lookup
// returns no row.
func assertMySQLTableCasePreserved(t *testing.T, ctx context.Context, dsn string, shape caseShape) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	var got string
	err = db.QueryRowContext(ctx, `
		SELECT table_name
		FROM   information_schema.tables
		WHERE  table_schema = DATABASE() AND table_name = ?
	`, shape.table).Scan(&got)
	if err != nil {
		listed := listMySQLTables(t, ctx, db)
		t.Fatalf("table `%s` not found on MySQL target (case-folded?); target tables = %v", shape.table, listed)
	}
	if got != shape.table {
		t.Errorf("MySQL target table_name = %q; want %q (case-preserved)", got, shape.table)
	}

	var gotCol string
	err = db.QueryRowContext(ctx, `
		SELECT column_name
		FROM   information_schema.columns
		WHERE  table_schema = DATABASE()
		  AND  table_name   = ?
		  AND  column_name  = ?
	`, shape.table, shape.column).Scan(&gotCol)
	if err != nil {
		listed := listMySQLColumnsOf(t, ctx, db, shape.table)
		t.Fatalf("column `%s` on `%s` not found (case-folded?); columns = %v", shape.column, shape.table, listed)
	}
	if gotCol != shape.column {
		t.Errorf("MySQL target column_name = %q; want %q (case-preserved)", gotCol, shape.column)
	}
}

// assertMySQLRowsPreserved counts rows in the case-preserved target
// table and asserts the count matches `want`. Queries with a
// backtick-quoted identifier — if sluice routed bulk-copy data to a
// folded-name table, the count on the case-preserved name returns 0
// or an undefined-table error.
func assertMySQLRowsPreserved(t *testing.T, ctx context.Context, dsn string, shape caseShape, want int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	q := fmt.Sprintf("SELECT count(*) FROM `%s`", shape.table)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count rows on case-preserved `%s`: %v", shape.table, err)
	}
	if n != want {
		t.Errorf("target `%s` row count = %d; want %d", shape.table, n, want)
	}
}

// listMySQLTables lists every base table in the current database on
// the target. Used for actionable failure messages.
func listMySQLTables(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`)
	if err != nil {
		return []string{"<query failed: " + err.Error() + ">"}
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err == nil {
			out = append(out, n)
		}
	}
	if err := rows.Err(); err != nil {
		out = append(out, "<rows.Err: "+err.Error()+">")
	}
	return out
}

// listMySQLColumnsOf lists every column on the named table on the
// MySQL target (queried with the exact case the caller supplies).
func listMySQLColumnsOf(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?
		ORDER BY ordinal_position
	`, table)
	if err != nil {
		return []string{"<query failed: " + err.Error() + ">"}
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err == nil {
			out = append(out, n)
		}
	}
	if err := rows.Err(); err != nil {
		out = append(out, "<rows.Err: "+err.Error()+">")
	}
	return out
}

// waitForRowCountMySQLQuoted is the case-preservation counterpart of
// waitForRowCountMySQL: queries the target with a backtick-quoted
// identifier (case-sensitive on a `lower_case_table_names=0` server).
func waitForRowCountMySQLQuoted(t *testing.T, dsn, table string, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCountMySQLQuoted(dsn, table) >= n {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// pollRowCountMySQLQuoted returns 0 on any error (table missing during
// the bulk-copy startup window, etc.) so the poll doesn't spam fatals.
func pollRowCountMySQLQuoted(dsn, table string) int {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	q := fmt.Sprintf("SELECT count(*) FROM `%s`", table)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0
	}
	return n
}

// mysqlRowExistsByValueQuoted is the MySQL counterpart to
// readOneRowByValueQuoted: returns true when a row with the given
// value at the given case-preserved column exists on the target.
func mysqlRowExistsByValueQuoted(t *testing.T, ctx context.Context, dsn, table, column, want string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf("SELECT count(*) FROM `%s` WHERE `%s` = ?", table, column)
	var n int
	if err := db.QueryRowContext(ctx, q, want).Scan(&n); err != nil {
		t.Fatalf("read by case-preserved column `%s`.`%s`: %v", table, column, err)
	}
	return n > 0
}
