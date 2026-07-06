//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine integration pins for Finding C: MySQL BINARY/VARBINARY literal
// defaults that contain a NUL byte. information_schema.COLUMN_DEFAULT
// C-string-truncates such a default at its first NUL (leading NUL → bare `0x`;
// mid/trailing NUL → a well-formed but SHORT hex literal, e.g. 0x2700 → `0x27`),
// so the v0.99.186 hexLiteralDefault path silently carried a wrong-bytes
// default. The reader now re-reads the true bytes from SHOW CREATE TABLE.
//
// These pin the CLASS end-to-end on all three targets a MySQL source can reach
// (MySQL, Postgres, SQLite): a DEFAULT-only inserted row on the target must have
// byte-identical stored bytes to the same insert on the MySQL source. The matrix
// covers leading-NUL, mid-NUL, trailing-NUL, multi-byte-with-NUL, a leading-NUL
// value NARROWER than a fixed BINARY width (compound: truncation + zero-pad),
// printable (no NUL), a VARBINARY NUL case (never zero-padded), and the
// v0.99.186 well-formed case (`'19700101000000'`) as a no-regression guard.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for reading the produced .db file

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// binNULDefaultSeedDDL is the Finding C source shape. Every binary column's
// default is chosen so information_schema mangles it (except `printable` and
// `wellformed`, the faithful/no-regression controls).
const binNULDefaultSeedDDL = `
	CREATE TABLE bin_nul_defaults (
		id           INT          NOT NULL,
		single_nul   BINARY(1)    NOT NULL DEFAULT 0x00,
		multi_nul    BINARY(4)    NOT NULL DEFAULT 0x00FF00FF,
		mid_nul      BINARY(2)    NOT NULL DEFAULT 0x2700,
		trail_nul    BINARY(2)    NOT NULL DEFAULT 0xFF00,
		mid_nul_data BINARY(3)    NOT NULL DEFAULT 0x270041,
		printable    BINARY(3)    NOT NULL DEFAULT 0x414243,
		padded_short BINARY(8)    NOT NULL DEFAULT 0x00FF,
		vb_nul       VARBINARY(4) NOT NULL DEFAULT 0x00FF00FF,
		vb_mid       VARBINARY(4) NOT NULL DEFAULT 0x2700,
		wellformed   BINARY(14)   NOT NULL DEFAULT '19700101000000',
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// binNULDefaultCols is the ordered list of binary columns whose DEFAULT-applied
// bytes the oracle compares.
var binNULDefaultCols = []string{
	"single_nul", "multi_nul", "mid_nul", "trail_nul", "mid_nul_data",
	"printable", "padded_short", "vb_nul", "vb_mid", "wellformed",
}

func TestMigrate_BinaryNULDefault_MySQLToMySQL(t *testing.T) {
	mysqlSource, mysqlTarget, cleanup := startMySQL(t)
	defer cleanup()
	applyMySQLDDL(t, mysqlSource, binNULDefaultSeedDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runBinNULMigrate(t, ctx, "mysql", "mysql", mysqlSource, mysqlTarget)

	want := insertBinNULRowMySQL(ctx, t, mysqlSource)
	got := insertBinNULRowMySQL(ctx, t, mysqlTarget)
	assertBinNULBytes(t, want, got, "mysql")
}

func TestMigrate_BinaryNULDefault_MySQLToPG(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	applyMySQLDDL(t, mysqlSource, binNULDefaultSeedDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runBinNULMigrate(t, ctx, "mysql", "postgres", mysqlSource, pgTarget)

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pg.Close() }()

	want := insertBinNULRowMySQL(ctx, t, mysqlSource)
	got := insertBinNULRowSQL(ctx, t, pg, "encode(%s,'hex')")
	assertBinNULBytes(t, want, got, "postgres")
}

func TestMigrate_BinaryNULDefault_MySQLToSQLite(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	applyMySQLDDL(t, mysqlSource, binNULDefaultSeedDDL)

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	_ = sqliteEng
	dst := filepath.Join(t.TempDir(), "bin_nul.db")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runBinNULMigrate(t, ctx, "mysql", "sqlite", mysqlSource, dst)

	sdb, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open sqlite target: %v", err)
	}
	defer func() { _ = sdb.Close() }()

	want := insertBinNULRowMySQL(ctx, t, mysqlSource)
	got := insertBinNULRowSQL(ctx, t, sdb, "hex(%s)")
	assertBinNULBytes(t, want, got, "sqlite")
}

// runBinNULMigrate runs the simple-mode migration for the named engines.
func runBinNULMigrate(t *testing.T, ctx context.Context, srcName, dstName, srcDSN, dstDSN string) {
	t.Helper()
	srcEng, ok := engines.Get(srcName)
	if !ok {
		t.Fatalf("%s engine not registered", srcName)
	}
	dstEng, ok := engines.Get(dstName)
	if !ok {
		t.Fatalf("%s engine not registered", dstName)
	}
	mig := &Migrator{Source: srcEng, Target: dstEng, SourceDSN: srcDSN, TargetDSN: dstDSN}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (%s→%s) = %v; want SUCCESS (binary NUL-default DDL must be valid)", srcName, dstName, err)
	}
}

// insertBinNULRowMySQL inserts a PK-only row (every binary column takes its
// DEFAULT) into a MySQL DB and returns col→uppercase-hex of the stored bytes.
func insertBinNULRowMySQL(ctx context.Context, t *testing.T, dsn string) map[string]string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	return insertBinNULRowSQL(ctx, t, db, "HEX(%s)")
}

// insertBinNULRowSQL inserts a PK-only row into an already-open target and reads
// each binary column back as hex. hexFmt is the engine's hex function with a
// single %s for the column name (MySQL `HEX(%s)`, PG `encode(%s,'hex')`,
// SQLite `hex(%s)`).
func insertBinNULRowSQL(ctx context.Context, t *testing.T, db *sql.DB, hexFmt string) map[string]string {
	t.Helper()
	if _, err := db.ExecContext(ctx, "INSERT INTO bin_nul_defaults (id) VALUES (1)"); err != nil {
		t.Fatalf("insert default row: %v", err)
	}
	exprs := make([]string, len(binNULDefaultCols))
	for i, c := range binNULDefaultCols {
		exprs[i] = strings.Replace(hexFmt, "%s", c, 1)
	}
	q := "SELECT " + strings.Join(exprs, ", ") + " FROM bin_nul_defaults WHERE id = 1"
	dest := make([]string, len(binNULDefaultCols))
	ptrs := make([]any, len(binNULDefaultCols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	if err := db.QueryRowContext(ctx, q).Scan(ptrs...); err != nil {
		t.Fatalf("read back default-applied row: %v", err)
	}
	out := make(map[string]string, len(binNULDefaultCols))
	for i, c := range binNULDefaultCols {
		out[c] = strings.ToUpper(dest[i])
	}
	return out
}

// assertBinNULBytes fails if any target column's default-applied bytes diverge
// from the MySQL source, and also spot-checks the known values so a silent
// all-empty read can't pass.
func assertBinNULBytes(t *testing.T, want, got map[string]string, target string) {
	t.Helper()
	for _, c := range binNULDefaultCols {
		if want[c] != got[c] {
			t.Errorf("column %q default bytes diverged: mysql=%s %s=%s", c, want[c], target, got[c])
		}
	}
	// Ground-truth spot checks (independent of the MySQL source read): these
	// are exactly the bytes that were silently corrupted pre-fix.
	expect := map[string]string{
		"single_nul":   "00",
		"multi_nul":    "00FF00FF",
		"mid_nul":      "2700",
		"trail_nul":    "FF00",
		"mid_nul_data": "270041",
		"printable":    "414243",
		"padded_short": "00FF000000000000",
		"vb_nul":       "00FF00FF",
		"vb_mid":       "2700",
		"wellformed":   "3139373030313031303030303030",
	}
	for c, e := range expect {
		if got[c] != e {
			t.Errorf("column %q on %s = %s; want %s", c, target, got[c], e)
		}
	}
}
