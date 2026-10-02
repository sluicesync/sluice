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

// TestEnsureHeartbeatTable_RefusesATableThatIsNotSluices pins the
// HEARTBEAT-TABLE-NOT-SLUICES door on real MySQL. The heartbeat writer
// INSERTs into its table and its prune DELETEs by ts, so through v0.156.8 a
// --source-heartbeat-table-name naming a user table wrote into and deleted
// from source data. Three arms:
//
//   - user tables under the name — the heartbeat's columns plus one, and the
//     right names with the wrong types — are refused with the sentinel, and
//     their rows are untouched (the ensure is the only call, and it must not
//     write);
//   - a table created with the exact DDL v0.82.0 shipped (the shape never
//     changed since) is accepted, so an upgraded stream keeps its table;
//   - a fresh name is created.
func TestEnsureHeartbeatTable_RefusesATableThatIsNotSluices(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE TABLE hb_extra (id BIGINT NOT NULL AUTO_INCREMENT, ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
		"stream_id VARCHAR(255) NOT NULL, note TEXT, PRIMARY KEY (id)) ENGINE=InnoDB")
	exec("INSERT INTO hb_extra (ts, stream_id, note) VALUES ('2001-01-01 00:00:00', 'x', 'kept-1'), ('2001-01-02 00:00:00', 'y', 'kept-2')")
	exec("CREATE TABLE hb_types (id INT NOT NULL AUTO_INCREMENT, ts DATETIME NOT NULL, stream_id VARCHAR(255) NOT NULL, PRIMARY KEY (id))")
	exec("INSERT INTO hb_types (ts, stream_id) VALUES ('2001-01-01 00:00:00', 'z')")
	// v0.82.0's DDL, verbatim.
	exec("CREATE TABLE hb_legacy (id BIGINT NOT NULL AUTO_INCREMENT, ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
		"stream_id VARCHAR(255) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")

	sr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	msr := sr.(*SchemaReader)

	for table, want := range map[string]string{"hb_extra": "note text", "hb_types": "id int"} {
		err := msr.EnsureHeartbeatTable(ctx, table)
		if !errors.Is(err, ir.ErrHeartbeatTableNotSluices) {
			t.Errorf("%s: EnsureHeartbeatTable = %v; want HEARTBEAT-TABLE-NOT-SLUICES", table, err)
			continue
		}
		for _, frag := range []string{"HEARTBEAT-TABLE-NOT-SLUICES", table, want, "--source-heartbeat-table-name"} {
			if !strings.Contains(err.Error(), frag) {
				t.Errorf("%s: refusal does not name %q: %v", table, frag, err)
			}
		}
	}
	var n int
	var notes string
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*), GROUP_CONCAT(note ORDER BY id) FROM hb_extra").Scan(&n, &notes); err != nil {
		t.Fatalf("read hb_extra: %v", err)
	}
	if n != 2 || notes != "kept-1,kept-2" {
		t.Errorf("hb_extra after the refusal: %d rows %q; want the 2 original rows untouched", n, notes)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM hb_types").Scan(&n); err != nil || n != 1 {
		t.Errorf("hb_types after the refusal: %d rows (err %v); want 1", n, err)
	}

	if err := msr.EnsureHeartbeatTable(ctx, "hb_legacy"); err != nil {
		t.Errorf("a table with the shape every release created was refused: %v", err)
	}
	if err := msr.EnsureHeartbeatTable(ctx, "hb_fresh"); err != nil {
		t.Fatalf("fresh name: %v", err)
	}
	if err := msr.EnsureHeartbeatTable(ctx, "hb_fresh"); err != nil {
		t.Errorf("the table EnsureHeartbeatTable just created was refused on the next start: %v", err)
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

// TestHeartbeat_AdvancesBinlogPosition pins that heartbeat writes reach
// the SERVER's binlog: the tip before and after the writes, read back from
// the server that took them. That is all it proves. It does not show that a
// stream consumes those events, that the stream's PERSISTED position moves
// on them, or that the applier does not trip over them — its evidence is
// the write it just made, so it stayed green through GC-43 (e), where every
// heartbeat row reached the applier as a skipped table and `sync health`
// exited 1. The end-to-end promise is pinned in internal/pipeline by
// TestStreamer_MySQLSourceHeartbeat_AdvancesPositionWithoutSkips: the
// persisted position advances on heartbeats alone and the skip ledger stays
// empty.
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

	// The binlog tip, read through the production version cascade — see
	// readMasterPos. We only need the (File, Position) pair to confirm the
	// position advanced.
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
// readMasterPos returns the source's current binlog (file, position) by
// delegating to the production [masterStatus] helper.
//
// It used to issue `SHOW MASTER STATUS` itself, with a comment asserting
// "MySQL 8.0+ uses SHOW MASTER STATUS". That statement was DEPRECATED in
// 8.0.22 and REMOVED in 8.4, so on 8.4 this failed with error 1064 and took
// the whole MySQL engine suite red on both legs of the version matrix — while
// the code under test was fine, because [masterStatusSpellings] already walks
// `SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS` / `SHOW BINLOG STATUS` and
// picks whichever the server accepts.
//
// So this is the shape a version matrix exists to catch and the reason it is
// worth running: a TEST pinned to one server version while the production path
// it exercises was already version-aware. Delegating rather than re-spelling
// the cascade here means the test cannot drift from the code again — if a
// future server renames the statement once more, both move together.
//
// (The scan shape moved too: the old code scanned exactly five columns, which
// is 8.0's. `masterStatus` discards anything past the first two, so a server
// that returns a different column count no longer breaks the read either.)
func readMasterPos(ctx context.Context, db *sql.DB) (file string, pos uint64, err error) {
	f, p, err := masterStatus(ctx, db)
	if err != nil {
		return "", 0, err
	}
	return f, uint64(p), nil
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
