//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A column type the target cannot render must be refused BEFORE any table is
// created, end to end, on real targets.
//
// # Why this test and not only the unit pins
//
// The per-engine agreement gates prove the preflight's verdict matches its own
// emitter's, shape by shape. What they cannot observe is the property the whole
// change is about: WHEN the refusal arrives, and what the target looks like at
// that moment. Before this, the refusal came from the target's own CREATE TABLE
// — which runs after the plan is printed, after the writers are open, and
// table-by-table, so a schema whose LAST table carries the offending column left
// every table ahead of it created on the target. Each case below therefore puts
// the good table FIRST (`a_…`) and the offending one LAST (`z_…`) and asserts
// that NEITHER exists after the refusal.
//
// # The class, not the representative
//
// The engine pairs that actually differ, and the reason each is here:
//
//	PG source  → SQLite target   ir.Inet        the clearest cell: SQLite has the
//	                                            smallest type surface of any target
//	                                            and NO direction of
//	                                            CheckCrossEngineSupportable covers it
//	MySQL src  → Postgres target VARCHAR(0)     the direction the hand-coded check
//	                                            has never looked at, on the shape
//	                                            long-lived MySQL schemas really carry
//	PG source  → MySQL target    ir.Inet        the CONTROL for the one pair that WAS
//	                                            covered: inet auto-emits to
//	                                            VARCHAR(45) and must still migrate
//
// Each refusing pair also carries its own control — the same schema with the
// offending column replaced by a renderable one — because an over-refusing
// preflight breaks migrations that work today, which is a worse defect than the
// late refusal it replaces.

package pipeline

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	// Every target engine under test must be registered for engines.Get.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// requireColumnTypeRefusalBeforeAnyTableCreated asserts the shape this change
// is about: the run failed, the message names the table, the column and the
// type, and NO table from the schema reached the target — not even the ones
// ahead of the offending one.
func requireColumnTypeRefusalBeforeAnyTableCreated(
	t *testing.T, err error, wants []string, tableExists func(string) bool, tables ...string,
) {
	t.Helper()
	if err == nil {
		t.Fatal("migrate SUCCEEDED with a column type the target cannot render; either the type was " +
			"silently coerced or the refusal was lost entirely")
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — an operator needs the table and column, not just "+
				"the type.\ngot: %v", want, err)
		}
	}
	for _, name := range tables {
		if tableExists(name) {
			t.Errorf("table %q EXISTS on the target after the refusal.\n\nThat means the refusal fired "+
				"at or after the schema-apply phase, which creates tables one at a time — so a schema "+
				"whose last table is the unrenderable one leaves every earlier table behind. The "+
				"preflight is supposed to run beside the other pre-DDL gates.", name)
		}
	}
}

// TestMigrate_ColumnTypePreflight_PGToSQLite covers the clearest cell: a PG
// `inet` column bound for SQLite, which has no faithful storage for it and
// which no direction of CheckCrossEngineSupportable has ever looked at.
func TestMigrate_ColumnTypePreflight_PGToSQLite(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}

	sqliteHasTable := func(dst string) func(string) bool {
		return func(name string) bool {
			db, err := sql.Open("sqlite", dst)
			if err != nil {
				t.Fatalf("open sqlite target: %v", err)
			}
			defer func() { _ = db.Close() }()
			var n int
			if err := db.QueryRow(
				`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name,
			).Scan(&n); err != nil {
				t.Fatalf("probe sqlite table %q: %v", name, err)
			}
			return n > 0
		}
	}

	t.Run("an inet column is refused before any table is created", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			DROP TABLE IF EXISTS a_orders, z_hosts;
			CREATE TABLE a_orders (id BIGINT PRIMARY KEY, note TEXT);
			INSERT INTO a_orders (id, note) VALUES (1, 'x'), (2, 'y');
			CREATE TABLE z_hosts (id BIGINT PRIMARY KEY, addr INET NOT NULL);
			INSERT INTO z_hosts (id, addr) VALUES (1, '10.0.0.1');
		`)

		dst := filepath.Join(t.TempDir(), "coltype-refused.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			MigrationID: "coltype-pg-to-sqlite-inet",
		}
		err := mig.Run(ctx2min(t))
		requireColumnTypeRefusalBeforeAnyTableCreated(t, err,
			[]string{"z_hosts", "addr", "Inet"},
			sqliteHasTable(dst), "z_hosts", "a_orders")
	})

	t.Run("the same schema without it still migrates, rows and all", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			DROP TABLE IF EXISTS a_orders, z_hosts;
			CREATE TABLE a_orders (id BIGINT PRIMARY KEY, note TEXT);
			INSERT INTO a_orders (id, note) VALUES (1, 'x'), (2, 'y');
			CREATE TABLE z_hosts (id BIGINT PRIMARY KEY, addr TEXT NOT NULL);
			INSERT INTO z_hosts (id, addr) VALUES (1, '10.0.0.1');
		`)

		dst := filepath.Join(t.TempDir(), "coltype-allowed.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			MigrationID: "coltype-pg-to-sqlite-text",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("an over-refusing preflight is a worse defect than the late refusal it replaces; "+
				"this schema has a SQLite landing for every column: %v", err)
		}
		db, err := sql.Open("sqlite", dst)
		if err != nil {
			t.Fatalf("open sqlite target: %v", err)
		}
		defer func() { _ = db.Close() }()
		for _, tbl := range []struct {
			name string
			want int
		}{{"a_orders", 2}, {"z_hosts", 1}} {
			var n int
			if err := db.QueryRow(`SELECT COUNT(*) FROM ` + tbl.name).Scan(&n); err != nil {
				t.Fatalf("count %s on the target: %v", tbl.name, err)
			}
			if n != tbl.want {
				t.Errorf("%s row count on the target = %d; want %d", tbl.name, n, tbl.want)
			}
		}
	})
}

// TestMigrate_ColumnTypePreflight_MySQLToPostgres covers the direction the
// hand-coded cross-engine check has never looked at, on the shape long-lived
// MySQL schemas really carry: VARCHAR(0), legal on MySQL and refused by PG at
// CREATE TABLE with SQLSTATE 22023 (catalog Bug 107).
func TestMigrate_ColumnTypePreflight_MySQLToPostgres(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	pgDB, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pgDB.Close() }()
	pgHasTable := func(name string) bool {
		var n int
		if err := pgDB.QueryRow(
			`SELECT COUNT(*) FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_name = $1`, name,
		).Scan(&n); err != nil {
			t.Fatalf("probe pg table %q: %v", name, err)
		}
		return n > 0
	}

	t.Run("a VARCHAR(0) marker column is refused before any table is created", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `
			DROP TABLE IF EXISTS a_orders, z_legacy;
			CREATE TABLE a_orders (id BIGINT NOT NULL PRIMARY KEY, note TEXT)
				ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO a_orders (id, note) VALUES (1, 'x'), (2, 'y');
			CREATE TABLE z_legacy (id BIGINT NOT NULL PRIMARY KEY, marker VARCHAR(0) NULL)
				ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO z_legacy (id, marker) VALUES (1, '');
		`)

		mig := &Migrator{
			Source: mysqlEng, Target: pgEng,
			SourceDSN: mysqlSource, TargetDSN: pgTarget,
			MigrationID: "coltype-mysql-to-pg-varchar0",
		}
		err := mig.Run(ctx2min(t))
		requireColumnTypeRefusalBeforeAnyTableCreated(t, err,
			[]string{"z_legacy", "marker", "VARCHAR(0)", "--type-override"},
			pgHasTable, "z_legacy", "a_orders")
	})

	t.Run("the same schema with a normal VARCHAR still migrates, rows and all", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `
			DROP TABLE IF EXISTS a_orders, z_legacy;
			CREATE TABLE a_orders (id BIGINT NOT NULL PRIMARY KEY, note TEXT)
				ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO a_orders (id, note) VALUES (1, 'x'), (2, 'y');
			CREATE TABLE z_legacy (id BIGINT NOT NULL PRIMARY KEY, marker VARCHAR(1) NULL)
				ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO z_legacy (id, marker) VALUES (1, '');
		`)

		mig := &Migrator{
			Source: mysqlEng, Target: pgEng,
			SourceDSN: mysqlSource, TargetDSN: pgTarget,
			MigrationID: "coltype-mysql-to-pg-varchar1",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("VARCHAR(1) is an ordinary column and must still migrate: %v", err)
		}
		for _, tbl := range []struct {
			name string
			want int
		}{{"a_orders", 2}, {"z_legacy", 1}} {
			var n int
			if err := pgDB.QueryRow(`SELECT COUNT(*) FROM ` + tbl.name).Scan(&n); err != nil {
				t.Fatalf("count %s on the target: %v", tbl.name, err)
			}
			if n != tbl.want {
				t.Errorf("%s row count on the target = %d; want %d", tbl.name, n, tbl.want)
			}
		}
	})
}

// TestMigrate_ColumnTypePreflight_PGToMySQL_AutoEmitsStillMigrate is the
// control for the ONE pair the hand-coded check already covered. Every
// PG-native type here has a documented MySQL auto-emit (v0.7.0), and the new
// preflight sits on the same path as that check — so this is the test that
// fails if it started refusing what MySQL renders happily.
func TestMigrate_ColumnTypePreflight_PGToMySQL_AutoEmitsStillMigrate(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	applyPGDDL(t, pgSource, `
		DROP TABLE IF EXISTS autoemit;
		CREATE TABLE autoemit (
			id   BIGINT PRIMARY KEY,
			ref  UUID NOT NULL,
			addr INET NOT NULL,
			net  CIDR NOT NULL,
			mac  MACADDR NOT NULL,
			tags TEXT[] NOT NULL
		);
		INSERT INTO autoemit (id, ref, addr, net, mac, tags) VALUES
			(1, '00000000-0000-0000-0000-000000000001', '10.0.0.1', '10.0.0.0/8',
			 '08:00:2b:01:02:03', ARRAY['a','b']);
	`)

	mig := &Migrator{
		Source: pgEng, Target: mysqlEng,
		SourceDSN: pgSource, TargetDSN: mysqlTarget,
		MigrationID: "coltype-pg-to-mysql-autoemit",
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("uuid/inet/cidr/macaddr/array all have documented MySQL auto-emits and this "+
			"migration works today; the column-type preflight must not have taken it away: %v", err)
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM autoemit`).Scan(&n); err != nil {
		t.Fatalf("count autoemit on the target: %v", err)
	}
	if n != 1 {
		t.Errorf("autoemit row count on the target = %d; want 1", n)
	}
}
