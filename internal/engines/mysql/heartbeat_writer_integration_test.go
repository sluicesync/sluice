//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the MySQL source-side heartbeat writer
// (ADR-0061, F17). Boots the shared mysql:8.0 container (binlog ROW +
// FULL row-image), runs the heartbeat writer briefly, and asserts:
//
//   - the heartbeat table exists with the expected schema;
//   - rows accumulate at the expected cadence;
//   - the binlog file/position advances past the pre-write capture,
//     proving the heartbeat writes produced binlog events the CDC
//     consumer would see;
//   - PruneHeartbeat removes rows older than the window;
//   - EnsureHeartbeatTable on a low-privilege user surfaces
//     [ir.ErrHeartbeatPermission].
//
// The unit tests cover the loop-lifecycle shape; this file exercises
// the engine-side SQL against real MySQL.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestEnsureHeartbeatTable_CreatesAndIdempotent pins the table-create
// path and the additive-call idempotency.
func TestEnsureHeartbeatTable_CreatesAndIdempotent(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	msr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := msr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}
	if err := msr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable (second call): %v", err)
	}

	// Verify column shape via information_schema.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		`SELECT column_name, data_type
		   FROM information_schema.columns
		   WHERE table_schema = DATABASE() AND table_name = ?
		   ORDER BY ordinal_position`, table)
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	defer func() { _ = rows.Close() }()

	type col struct{ name, dtype string }
	var got []col
	for rows.Next() {
		var c col
		if err := rows.Scan(&c.name, &c.dtype); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, c)
	}
	want := []col{
		{"id", "bigint"},
		{"ts", "timestamp"},
		{"stream_id", "varchar"},
	}
	if len(got) != len(want) {
		t.Fatalf("column count: got %d (%+v); want %d (%+v)", len(got), got, len(want), want)
	}
	for i, w := range want {
		if got[i].name != w.name || got[i].dtype != w.dtype {
			t.Errorf("col[%d]: got %+v; want %+v", i, got[i], w)
		}
	}
}

// TestWriteHeartbeat_RowsAccumulate pins the INSERT path.
func TestWriteHeartbeat_RowsAccumulate(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	msr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := msr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}

	const streamID = "test-stream-mysql"
	for i := 0; i < 3; i++ {
		if err := msr.WriteHeartbeat(ctx, table, streamID); err != nil {
			t.Fatalf("WriteHeartbeat[%d]: %v", i, err)
		}
		time.Sleep(15 * time.Millisecond)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var count int
	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM `sluice_heartbeat` WHERE stream_id = ?", streamID,
	).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 3 {
		t.Errorf("row count: got %d; want 3", count)
	}
}

// TestPruneHeartbeat_DropsOldRows pins the prune path.
func TestPruneHeartbeat_DropsOldRows(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	msr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := msr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Backdated row + fresh row.
	if _, err := db.ExecContext(
		ctx,
		"INSERT INTO `sluice_heartbeat` (ts, stream_id) VALUES (DATE_SUB(NOW(), INTERVAL 5 MINUTE), 'old')",
	); err != nil {
		t.Fatalf("seed old row: %v", err)
	}
	if err := msr.WriteHeartbeat(ctx, table, "fresh"); err != nil {
		t.Fatalf("WriteHeartbeat: %v", err)
	}

	deleted, err := msr.PruneHeartbeat(ctx, table, time.Second)
	if err != nil {
		t.Fatalf("PruneHeartbeat: %v", err)
	}
	if deleted < 1 {
		t.Errorf("PruneHeartbeat: expected >=1 row deleted; got %d", deleted)
	}

	var freshCount, oldCount int
	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM `sluice_heartbeat` WHERE stream_id = 'fresh'",
	).Scan(&freshCount); err != nil {
		t.Fatalf("count fresh: %v", err)
	}
	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM `sluice_heartbeat` WHERE stream_id = 'old'",
	).Scan(&oldCount); err != nil {
		t.Fatalf("count old: %v", err)
	}
	if freshCount != 1 {
		t.Errorf("fresh row count: got %d; want 1", freshCount)
	}
	if oldCount != 0 {
		t.Errorf("old row count: got %d; want 0", oldCount)
	}
}

// TestHeartbeat_AdvancesBinlogPosition pins the load-bearing F17
// promise on MySQL: heartbeat writes produce binlog events. We capture
// the master binlog position before and after the writes and assert
// the position advanced — the byte distance is the binlog footprint of
// F17's writes.
func TestHeartbeat_AdvancesBinlogPosition(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	msr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := msr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// MySQL 8.0+ uses SHOW MASTER STATUS (returns File, Position, ...).
	// We only need (File, Position) pair to confirm the position
	// advanced.
	beforeFile, beforePos, err := readMasterPos(ctx, db)
	if err != nil {
		t.Fatalf("read master pos before: %v", err)
	}

	for i := 0; i < 5; i++ {
		if err := msr.WriteHeartbeat(ctx, table, "advance-test"); err != nil {
			t.Fatalf("WriteHeartbeat[%d]: %v", i, err)
		}
	}

	afterFile, afterPos, err := readMasterPos(ctx, db)
	if err != nil {
		t.Fatalf("read master pos after: %v", err)
	}

	// File may have rotated; if not, position must be strictly greater.
	if afterFile == beforeFile {
		if afterPos <= beforePos {
			t.Errorf("binlog position: expected advance after heartbeat writes; before=%s:%d, after=%s:%d",
				beforeFile, beforePos, afterFile, afterPos)
		} else {
			t.Logf("F17 heartbeat binlog footprint: %d bytes across 5 writes (avg %d bytes/write)",
				afterPos-beforePos, (afterPos-beforePos)/5)
		}
	}
	// If file rotated, the writes are clearly past the pre-write
	// position (any position in a later file is later than any position
	// in an earlier file). No further assertion needed; the rotation
	// itself proves advancement.
}

// readMasterPos returns the master's current binlog (File, Position).
// MySQL 8.0+ supports SHOW BINARY LOG STATUS (8.4+) as an alias; we use
// SHOW MASTER STATUS for 8.0 compatibility.
func readMasterPos(ctx context.Context, db *sql.DB) (file string, pos uint64, err error) {
	row := db.QueryRowContext(ctx, `SHOW MASTER STATUS`)
	// SHOW MASTER STATUS in MySQL 8.0 returns: File, Position,
	// Binlog_Do_DB, Binlog_Ignore_DB, Executed_Gtid_Set
	var binlogDoDB, binlogIgnoreDB, executedGtidSet sql.NullString
	if err := row.Scan(&file, &pos, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet); err != nil {
		return "", 0, err
	}
	return file, pos, nil
}

// TestEnsureHeartbeatTable_PermissionDenied pins the loud-failure path:
// a user lacking CREATE on the database surfaces
// [ir.ErrHeartbeatPermission] so the pipeline wiring can degrade
// gracefully.
func TestEnsureHeartbeatTable_PermissionDenied(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create a restricted user with SELECT-only on the shared db.
	applyMySQL(t, dsn, "CREATE USER 'noddl'@'%' IDENTIFIED BY 'noddlpw'")
	applyMySQL(t, dsn, "GRANT SELECT ON `source_db`.* TO 'noddl'@'%'")
	applyMySQL(t, dsn, "FLUSH PRIVILEGES")

	noddlDSN := rewriteMySQLDSNCredentials(t, dsn, "noddl", "noddlpw")

	sr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, noddlDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader as noddl: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	msr := sr.(*SchemaReader)

	const table = "sluice_heartbeat_perm_test"
	err = msr.EnsureHeartbeatTable(ctx, table)
	if err == nil {
		t.Fatal("EnsureHeartbeatTable as noddl: expected permission error; got nil")
	}
	if !errors.Is(err, ir.ErrHeartbeatPermission) {
		t.Errorf("EnsureHeartbeatTable error: must match ir.ErrHeartbeatPermission via errors.Is; got %v", err)
	}
}

// rewriteMySQLDSNCredentials substitutes the user/password in a MySQL
// DSN of the form `user:pass@tcp(host:port)/db?...`. The shared MySQL
// helper emits credentials as `root:rootpw@tcp(...)/source_db?...`.
func rewriteMySQLDSNCredentials(t *testing.T, dsn, user, pass string) string {
	t.Helper()
	at := strings.Index(dsn, "@tcp(")
	if at < 0 {
		t.Fatalf("rewriteMySQLDSNCredentials: DSN %q does not contain '@tcp(' (helper assumes shared-container shape)", dsn)
	}
	return user + ":" + pass + dsn[at:]
}
