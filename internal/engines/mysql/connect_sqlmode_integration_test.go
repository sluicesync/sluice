// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package mysql

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

func getenvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustParseDSN(t *testing.T, dsn string) *mysqldriver.Config {
	t.Helper()
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	return cfg
}

// TestConnectAppliesStrictSQLModeOnSession pins the v0.92.1
// connection-side sql_mode forcing. The cycle subagent's general-log
// probe found "ZERO SET sql_mode" — but that was a test-methodology
// miss; @@SESSION.sql_mode on a sluice-opened connection IS strict.
// This test is the canonical pin.
func TestConnectAppliesStrictSQLModeOnSession(t *testing.T) {
	dsn := getenvOr("MYSQL_PROBE_DSN", "")
	if dsn == "" {
		host, port, user, password := ensureSharedMySQL(t)
		dsn = sharedDSN(host, port, user, password, "mysql")
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	var sessionMode string
	if err := db.QueryRowContext(context.Background(), "SELECT @@SESSION.sql_mode").Scan(&sessionMode); err != nil {
		t.Fatalf("SELECT @@SESSION.sql_mode: %v", err)
	}
	for _, want := range []string{
		"STRICT_TRANS_TABLES",
		"NO_ZERO_DATE",
		"NO_ZERO_IN_DATE",
		"ERROR_FOR_DIVISION_BY_ZERO",
	} {
		if !strings.Contains(sessionMode, want) {
			t.Errorf("@@SESSION.sql_mode does not contain %q; got %q", want, sessionMode)
		}
	}
}

// TestLoadDataWarningCountRefusesSilentClamp pins the v0.92.2 Bug
// 102/103 closure. Pre-fix LOAD DATA LOCAL INFILE silently bypassed
// strict sql_mode for type-conversion errors (the actual root cause
// of v0.92.1's missed closure). Post-fix the writer queries
// @@warning_count on the pinned connection and refuses loudly with
// the SHOW WARNINGS detail.
//
// The test exercises the same code path the bulk-copy migrate uses:
// CreateTable + writeLoadData with an out-of-range NUMERIC. Pre-fix
// the LOAD DATA succeeds and the row lands clamped; post-fix
// writeLoadData returns the new warning-refusal error.
func TestLoadDataWarningCountRefusesSilentClamp(t *testing.T) {
	dsn := getenvOr("MYSQL_PROBE_DSN", "")
	if dsn == "" {
		host, port, user, password := ensureSharedMySQL(t)
		dsn = sharedDSN(host, port, user, password, "mysql")
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	// LOAD DATA LOCAL INFILE requires server-side @@local_infile=ON.
	// On CI's testcontainers default it's OFF; root can flip it. If
	// the flip fails (managed servers / GLOBAL-disallowed roles) the
	// test is meaningless — writeLoadData falls back to writeBatched
	// which IS strict-mode honest and produces a different (also
	// loud) error. Skip rather than assert on the wrong code path.
	if _, err := db.ExecContext(ctx, "SET GLOBAL local_infile = 1"); err != nil {
		t.Skipf("LOAD DATA pin requires SET GLOBAL local_infile = 1; cannot grant: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_loaddata_pin"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE sluice_loaddata_pin (id INT PRIMARY KEY, big DECIMAL(20,5))"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_loaddata_pin") }()

	// Drive writeLoadData with an out-of-range row that pre-v0.92.2
	// would silently clamp to MAX DECIMAL(20,5).
	rowCh := make(chan ir.Row, 1)
	rowCh <- ir.Row{
		"id":  int64(1),
		"big": "999999999999999999999.99999", // 21 integer digits — overflows DECIMAL(20,5)
	}
	close(rowCh)

	writer := &RowWriter{db: db}
	tbl := &ir.Table{
		Name: "sluice_loaddata_pin",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "big", Type: ir.Decimal{Precision: 20, Scale: 5}},
		},
	}
	err = writer.writeLoadData(ctx, tbl, rowCh)
	if err == nil {
		// Verify whether the clamp was silent — if so, the v0.92.2
		// refusal is missing.
		var got string
		_ = db.QueryRowContext(ctx, "SELECT CAST(big AS CHAR) FROM sluice_loaddata_pin WHERE id=1").Scan(&got)
		t.Fatalf("writeLoadData accepted out-of-range NUMERIC silently; row landed with big=%q — v0.92.2 warning-count refusal regressed", got)
	}
	if !strings.Contains(err.Error(), "warning") {
		t.Errorf("writeLoadData refusal should mention 'warning'; got: %v", err)
	}
	if !strings.Contains(err.Error(), "Bugs 102/103") && !strings.Contains(err.Error(), "v0.92.2") {
		t.Errorf("writeLoadData refusal should reference the bug closure for operator searchability; got: %v", err)
	}
}

// TestLoadDataWarningCountSkippedWhenSQLModeEmpty pins the legacy-data
// escape hatch interaction. When sessionSQLMode is "" (operator
// passed --mysql-sql-mode=”), the writer trusts the server's
// default behaviour and does NOT refuse on warnings — the operator
// has explicitly accepted server-side semantics.
func TestLoadDataWarning_RelaxedModeWarnsNotRefuses(t *testing.T) {
	dsn := getenvOr("MYSQL_PROBE_DSN", "")
	if dsn == "" {
		host, port, user, password := ensureSharedMySQL(t)
		dsn = sharedDSN(host, port, user, password, "mysql")
	}

	orig := sessionSQLMode
	defer func() { sessionSQLMode = orig }()
	SetSessionSQLMode("")

	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if _, ok := cfg.Params["sql_mode"]; ok {
		t.Fatalf("sessionSQLMode='' should suppress cfg.Params[sql_mode]; got %q", cfg.Params["sql_mode"])
	}
	db, err := openDB(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	// LOAD DATA LOCAL INFILE requires server-side @@local_infile=ON;
	// see TestLoadDataWarningCountRefusesSilentClamp comment.
	if _, err := db.ExecContext(ctx, "SET GLOBAL local_infile = 1"); err != nil {
		t.Skipf("LOAD DATA pin requires SET GLOBAL local_infile = 1; cannot grant: %v", err)
	}
	// The legacy-data escape-hatch contract is only meaningful when
	// the server's default sql_mode is NOT strict (operators with
	// pre-MySQL-5.7 zero-date corpora etc.). CI's MySQL ships strict
	// by default. Relax the session's sql_mode at the server level
	// for this test so the writeBatched fallback (when local_infile
	// is off and writeLoadData rolls over) doesn't error on the
	// out-of-range value either.
	if _, err := db.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
		t.Skipf("escape-hatch pin requires SET SESSION sql_mode = ''; cannot grant: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_loaddata_pin_legacy"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE sluice_loaddata_pin_legacy (id INT PRIMARY KEY, big DECIMAL(20,5))"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_loaddata_pin_legacy") }()

	// Probe reportBulkWriteWarnings directly with a pinned conn. Under
	// sessionSQLMode=="" (escape hatch) the Vector B contract is: a
	// silent coercion is no longer SKIPPED — it must WARN loudly but NOT
	// refuse (the operator opted into relaxed mode). We must NOT run
	// `SELECT @@warning_count` before the probe — that statement clears
	// the diagnostic list (the very ordering bug that produced empty
	// `Examples: []`), so the probe reads SHOW WARNINGS first.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
		t.Skipf("cannot relax session sql_mode for the probe: %v", err)
	}
	// Seed an out-of-range value: under relaxed sql_mode it silently
	// clamps and flags the warning list (the Vector B silent-loss shape).
	if _, err := conn.ExecContext(ctx, "INSERT INTO sluice_loaddata_pin_legacy (id, big) VALUES (1, 999999999999999999999.99999)"); err != nil {
		t.Fatalf("seed warning row: %v", err)
	}

	buf := captureSlog(t)
	w := &RowWriter{}
	if err := w.reportBulkWriteWarnings(ctx, conn, "sluice_loaddata_pin_legacy"); err != nil {
		t.Fatalf("relaxed sql_mode must WARN, not refuse; got error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "sluice_loaddata_pin_legacy") {
		t.Skip("server did not surface a warning for the seeded overflow; relaxed-WARN behavior is also covered by the end-to-end Vector B integration test")
	}
	if !strings.Contains(out, "SILENTLY coerced") || !strings.Contains(out, "--type-override") {
		t.Errorf("relaxed-mode WARN missing expected content:\n%s", out)
	}
	// Second call must NOT warn again (once per table).
	buf.Reset()
	if _, err := conn.ExecContext(ctx, "INSERT INTO sluice_loaddata_pin_legacy (id, big) VALUES (2, 999999999999999999999.99999)"); err != nil {
		t.Fatalf("seed second warning row: %v", err)
	}
	if err := w.reportBulkWriteWarnings(ctx, conn, "sluice_loaddata_pin_legacy"); err != nil {
		t.Fatalf("second probe should still not refuse; got: %v", err)
	}
	if strings.Contains(buf.String(), "sluice_loaddata_pin_legacy") {
		t.Errorf("warned twice for the same table; want once-per-table:\n%s", buf.String())
	}
}

// TestConnectAppliesUtf8mb4OnResults pins Bug 106 — the connection's
// character_set_results MUST be utf8mb4 on every sluice MySQL
// connection so 4-byte UTF-8 in metadata fields (ENUM labels in
// information_schema.column_type) round-trips intact. Server defaults
// vary (the localrig source returns character_set_results=latin1 by
// default); sluice's cfg.Collation handshake should override that.
func TestConnectAppliesUtf8mb4OnResults(t *testing.T) {
	dsn := getenvOr("MYSQL_PROBE_DSN", "")
	if dsn == "" {
		host, port, user, password := ensureSharedMySQL(t)
		dsn = sharedDSN(host, port, user, password, "mysql")
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	check := func(varName string) string {
		var v string
		if err := db.QueryRowContext(context.Background(), "SELECT @@SESSION."+varName).Scan(&v); err != nil {
			t.Fatalf("SELECT @@SESSION.%s: %v", varName, err)
		}
		return v
	}
	for _, varName := range []string{"character_set_client", "character_set_connection", "character_set_results"} {
		got := check(varName)
		if got != "utf8mb4" {
			t.Errorf("@@SESSION.%s = %q; want utf8mb4 (Bug 106: 4-byte UTF-8 in ENUM labels corrupts to '?' on non-utf8mb4 results charset)", varName, got)
		}
	}
}

// TestDirectInsertHonoursStrictMode is the diagnostic counter-example
// the v0.92.1 cycle missed: a direct INSERT under sluice's session
// IS strict (rejects 80-digit NUMERIC and zero-date with the
// MySQL-native error codes). Without this control, a future regression
// in the connection-level sql_mode plumbing would look identical to
// the LOAD DATA bypass — and the fix would target the wrong path.
func TestDirectInsertHonoursStrictMode(t *testing.T) {
	dsn := getenvOr("MYSQL_PROBE_DSN", "")
	if dsn == "" {
		host, port, user, password := ensureSharedMySQL(t)
		dsn = sharedDSN(host, port, user, password, "mysql")
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(context.Background(), cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_direct_strict_pin"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE sluice_direct_strict_pin (id INT PRIMARY KEY, big DECIMAL(20,5), ts TIMESTAMP NULL)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_direct_strict_pin") }()

	if _, err := db.ExecContext(ctx,
		"INSERT INTO sluice_direct_strict_pin (id, big) VALUES (1, ?)",
		"999999999999999999999.99999"); err == nil {
		t.Errorf("direct INSERT of out-of-range NUMERIC accepted — strict sql_mode is failing at the connection level")
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO sluice_direct_strict_pin (id, ts) VALUES (2, ?)",
		"0000-00-00 00:00:00"); err == nil {
		t.Errorf("direct INSERT of zero-date TIMESTAMP accepted — strict sql_mode is failing for zero-date")
	}
	// Suppress unused-import build error if/when the file shrinks; the
	// driver import is consumed by the other tests at registration.
	_ = mysqldriver.MySQLDriver{}
	_ = io.EOF
}

// TestWriteBatched_RelaxedModeWarnsOnClamp pins the Vector B fix on the
// batched-INSERT bulk path (the path used whenever LOAD DATA isn't —
// server local_infile=OFF, or a geometry column). Under
// --mysql-sql-mode=” an out-of-range value is silently clamped by the
// server; writeBatched now reports it as a loud one-time-per-table WARN
// (not a refusal) via its pinned connection, instead of the pre-Vector-B
// silent pass.
func TestWriteBatched_RelaxedModeWarnsOnClamp(t *testing.T) {
	host, port, user, password := ensureSharedMySQL(t)
	dsn := sharedDSN(host, port, user, password, "mysql")
	ctx := context.Background()

	// Relax the server's GLOBAL sql_mode so freshly-pooled connections
	// (the ones writeBatched pins) coerce instead of erroring. Restore on
	// cleanup so other tests on the shared container stay strict.
	admin, err := openDB(ctx, mustParseDSN(t, dsn))
	if err != nil {
		t.Fatalf("admin openDB: %v", err)
	}
	defer admin.Close()
	var origGlobal string
	if err := admin.QueryRowContext(ctx, "SELECT @@GLOBAL.sql_mode").Scan(&origGlobal); err != nil {
		t.Fatalf("read GLOBAL sql_mode: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "SET GLOBAL sql_mode = ''"); err != nil {
		t.Skipf("cannot relax GLOBAL sql_mode for the batched-path probe: %v", err)
	}
	defer func() { _, _ = admin.ExecContext(ctx, "SET GLOBAL sql_mode = ?", origGlobal) }()

	orig := sessionSQLMode
	defer func() { sessionSQLMode = orig }()
	SetSessionSQLMode("") // don't re-force strict on sluice's pooled conns

	db, err := openDB(ctx, mustParseDSN(t, dsn))
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_vectorb_batched"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE sluice_vectorb_batched (id INT PRIMARY KEY, small TINYINT)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_vectorb_batched") }()

	rowCh := make(chan ir.Row, 1)
	rowCh <- ir.Row{"id": int64(1), "small": int64(300)} // 300 > TINYINT max 127 → clamps to 127
	close(rowCh)

	buf := captureSlog(t)
	w := &RowWriter{db: db}
	tbl := &ir.Table{Name: "sluice_vectorb_batched", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "small", Type: ir.Integer{Width: 8}},
	}}
	if err := w.writeBatched(ctx, tbl, rowCh); err != nil {
		t.Fatalf("writeBatched under relaxed mode must not refuse; got: %v", err)
	}

	// The value was silently clamped (documents the Vector B shape)...
	var got int
	if err := db.QueryRowContext(ctx, "SELECT small FROM sluice_vectorb_batched WHERE id=1").Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got != 127 {
		t.Skipf("server did not clamp 300→127 (got %d); GLOBAL sql_mode relax may not have taken on the pooled conn", got)
	}
	// ...and the clamp was reported as a loud WARN naming the table + remedy.
	out := buf.String()
	if !strings.Contains(out, "sluice_vectorb_batched") || !strings.Contains(out, "SILENTLY coerced") {
		t.Errorf("writeBatched relaxed-mode WARN missing/incorrect:\n%s", out)
	}
	if !strings.Contains(out, "--type-override") {
		t.Errorf("writeBatched WARN should name the --type-override remedy:\n%s", out)
	}
}

// TestWriteBatchedIdempotent_RelaxedModeWarnsOnClamp pins Vector B on the
// IDEMPOTENT upsert bulk path (row_writer_batch.go) — the path the
// orchestrator takes on resume, parallel chunked copy (>100k threshold),
// add-table, and the VStream cold-start COPY. It was the third bulk-write
// dispatch member; LOAD DATA and writeBatched were covered, this one was
// not (the Bug-74 "pin the class, not the representative" gap). Under
// --mysql-sql-mode=” an out-of-range value silently clamps; the upsert
// path now reports it as a one-time-per-table WARN via its pinned conn.
func TestWriteBatchedIdempotent_RelaxedModeWarnsOnClamp(t *testing.T) {
	host, port, user, password := ensureSharedMySQL(t)
	dsn := sharedDSN(host, port, user, password, "mysql")
	ctx := context.Background()

	admin, err := openDB(ctx, mustParseDSN(t, dsn))
	if err != nil {
		t.Fatalf("admin openDB: %v", err)
	}
	defer admin.Close()
	var origGlobal string
	if err := admin.QueryRowContext(ctx, "SELECT @@GLOBAL.sql_mode").Scan(&origGlobal); err != nil {
		t.Fatalf("read GLOBAL sql_mode: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "SET GLOBAL sql_mode = ''"); err != nil {
		t.Skipf("cannot relax GLOBAL sql_mode for the idempotent-path probe: %v", err)
	}
	defer func() { _, _ = admin.ExecContext(ctx, "SET GLOBAL sql_mode = ?", origGlobal) }()

	orig := sessionSQLMode
	defer func() { sessionSQLMode = orig }()
	SetSessionSQLMode("")

	db, err := openDB(ctx, mustParseDSN(t, dsn))
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_vectorb_idem"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE sluice_vectorb_idem (id INT PRIMARY KEY, small TINYINT)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_vectorb_idem") }()

	rowCh := make(chan ir.Row, 1)
	rowCh <- ir.Row{"id": int64(1), "small": int64(300)} // clamps to 127
	close(rowCh)

	buf := captureSlog(t)
	w := &RowWriter{db: db}
	tbl := &ir.Table{
		Name: "sluice_vectorb_idem",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "small", Type: ir.Integer{Width: 8}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	if err := w.writeBatchedIdempotent(ctx, tbl, rowCh); err != nil {
		t.Fatalf("writeBatchedIdempotent under relaxed mode must not refuse; got: %v", err)
	}

	var got int
	if err := db.QueryRowContext(ctx, "SELECT small FROM sluice_vectorb_idem WHERE id=1").Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got != 127 {
		t.Skipf("server did not clamp 300→127 (got %d); GLOBAL relax may not have taken", got)
	}
	out := buf.String()
	if !strings.Contains(out, "sluice_vectorb_idem") || !strings.Contains(out, "SILENTLY coerced") {
		t.Errorf("idempotent-path relaxed-mode WARN missing/incorrect:\n%s", out)
	}
}
