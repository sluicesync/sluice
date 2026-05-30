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

	"github.com/orware/sluice/internal/ir"
)

func getenvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
func TestLoadDataWarningCountSkippedWhenSQLModeEmpty(t *testing.T) {
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
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_loaddata_pin_legacy"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE sluice_loaddata_pin_legacy (id INT PRIMARY KEY, big DECIMAL(20,5))"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	defer func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS sluice_loaddata_pin_legacy") }()

	rowCh := make(chan ir.Row, 1)
	rowCh <- ir.Row{
		"id":  int64(1),
		"big": "999999999999999999999.99999",
	}
	close(rowCh)
	writer := &RowWriter{db: db}
	tbl := &ir.Table{
		Name: "sluice_loaddata_pin_legacy",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "big", Type: ir.Decimal{Precision: 20, Scale: 5}},
		},
	}
	if err := writer.writeLoadData(ctx, tbl, rowCh); err != nil {
		t.Fatalf("escape-hatch path should not refuse on warnings; got: %v", err)
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
