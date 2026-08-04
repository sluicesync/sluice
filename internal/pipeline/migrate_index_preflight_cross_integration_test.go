//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// An index the target cannot represent must be refused BEFORE any data moves
// (roadmap item 118), end to end, on real targets.
//
// # Why this test and not only the unit pins
//
// Every one of these refusals already existed, was already loud, and was
// already zero-loss. The defect was entirely one of TIMING: each lived in an
// engine's SECONDARY-index emitter, and secondary indexes are built in the
// third phase, so the refusal arrived after the whole table had been copied.
// Two shipped releases said otherwise in their notes.
//
// Timing is not a property a unit test on an emitter can observe. So the
// assertion here is about the STATE OF THE TARGET at the moment of refusal:
// the table must not exist. A refusal that fired at the index phase would
// leave the table created and fully populated, which is exactly what these
// tests would have caught and what nothing did.
//
// # The class, not the representative
//
// Three engines own an index-representability refusal and each has a
// DIFFERENT unrepresentable attribute travelling in a DIFFERENT direction, so
// no single engine pair exercises the class:
//
//	MySQL target     partial UNIQUE predicate    PG source  → refuse
//	Postgres target  index prefix length         MySQL src  → refuse
//	SQLite target    index prefix length         MySQL src  → refuse
//
// Each pair also carries its CONTROL — the same attribute on a NON-unique
// index, which changes cost and not which rows are legal and must still
// migrate, rows and all. The control is the half that matters most here: an
// over-refusing preflight breaks migrations that work today, which is a worse
// defect than the late refusal it replaces.

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

// requireRefusalBeforeAnyDataMoved asserts the shape item 118 is about: the
// run failed, the message is the engine's own actionable refusal, and the
// target carries no trace of the table.
func requireRefusalBeforeAnyDataMoved(t *testing.T, err error, wants []string, tableExists func() bool) {
	t.Helper()
	if err == nil {
		t.Fatal("migrate SUCCEEDED with an index the target cannot represent; the constraint was " +
			"silently weakened (or the refusal was lost entirely)")
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — moving the refusal earlier must not change what it "+
				"says.\ngot: %v", want, err)
		}
	}
	if tableExists() {
		t.Error("the target table EXISTS after the refusal.\n\n" +
			"That means the refusal fired at or after the schema-apply phase — roadmap item 118's whole " +
			"defect. The preflight is supposed to run beside the other pre-DDL gates, so nothing should " +
			"have been created and no row should have been copied.")
	}
}

// TestMigrate_IndexPreflight_PGToMySQL_PartialUnique covers the MySQL target:
// a PG partial UNIQUE index has no MySQL equivalent, and MySQL's whole-table
// UNIQUE would reject rows the source holds legally (Bug 224).
func TestMigrate_IndexPreflight_PGToMySQL_PartialUnique(t *testing.T) {
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

	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()
	mysqlHasTable := func(name string) func() bool {
		return func() bool {
			var n int
			if err := mysqlDB.QueryRow(
				`SELECT COUNT(*) FROM information_schema.tables
				 WHERE table_schema = DATABASE() AND table_name = ?`, name,
			).Scan(&n); err != nil {
				t.Fatalf("probe mysql table %q: %v", name, err)
			}
			return n > 0
		}
	}

	t.Run("partial UNIQUE is refused before any data moves", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			CREATE TABLE softdel (
				id         BIGINT PRIMARY KEY,
				email      VARCHAR(255) NOT NULL,
				deleted_at TIMESTAMP NULL
			);
			CREATE UNIQUE INDEX softdel_email_live_uniq
				ON softdel (email) WHERE deleted_at IS NULL;
			INSERT INTO softdel (id, email, deleted_at)
				VALUES (1, 'a@x', NULL), (2, 'a@x', now()), (3, 'b@x', NULL);
		`)

		mig := &Migrator{
			Source: pgEng, Target: mysqlEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			MigrationID: "item118-pg-to-mysql-partial-unique",
		}
		err := mig.Run(ctx2min(t))
		requireRefusalBeforeAnyDataMoved(t, err,
			[]string{"softdel_email_live_uniq", "deleted_at IS NULL", "generated column"},
			mysqlHasTable("softdel"))
	})

	t.Run("partial NON-unique index still migrates, rows and all", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			DROP TABLE IF EXISTS softdel;
			CREATE TABLE softlog (
				id         BIGINT PRIMARY KEY,
				email      VARCHAR(255) NOT NULL,
				deleted_at TIMESTAMP NULL
			);
			CREATE INDEX softlog_email_live_idx
				ON softlog (email) WHERE deleted_at IS NULL;
			INSERT INTO softlog (id, email, deleted_at)
				VALUES (1, 'a@x', NULL), (2, 'a@x', now());
		`)

		mig := &Migrator{
			Source: pgEng, Target: mysqlEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			MigrationID: "item118-pg-to-mysql-partial-nonunique",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("a partial NON-unique index must still migrate — dropping its predicate changes the "+
				"index's cost, not which rows are legal. An over-refusing preflight is a worse defect "+
				"than the late refusal it replaces: %v", err)
		}
		var n int
		if err := mysqlDB.QueryRow(`SELECT COUNT(*) FROM softlog`).Scan(&n); err != nil {
			t.Fatalf("count softlog on the target: %v", err)
		}
		if n != 2 {
			t.Errorf("softlog row count on the target = %d; want 2", n)
		}
	})
}

// TestMigrate_IndexPreflight_MySQLToPostgres_PrefixUnique covers the Postgres
// target: a MySQL prefix length has no PG equivalent, and a PG key over the
// whole column ADMITS rows the source rejects — silently, at exit 0.
func TestMigrate_IndexPreflight_MySQLToPostgres_PrefixUnique(t *testing.T) {
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
	pgHasTable := func(name string) func() bool {
		return func() bool {
			var n int
			if err := pgDB.QueryRow(
				`SELECT COUNT(*) FROM information_schema.tables
				 WHERE table_schema = 'public' AND table_name = $1`, name,
			).Scan(&n); err != nil {
				t.Fatalf("probe pg table %q: %v", name, err)
			}
			return n > 0
		}
	}

	t.Run("prefixed UNIQUE is refused before any data moves", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `
			CREATE TABLE prefixed (
				id    BIGINT NOT NULL AUTO_INCREMENT,
				email VARCHAR(255) NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY prefixed_email_uniq (email(10))
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO prefixed (email) VALUES ('a@example.com'), ('b@example.com');
		`)

		mig := &Migrator{
			Source: mysqlEng, Target: pgEng,
			SourceDSN: mysqlSource, TargetDSN: pgTarget,
			MigrationID: "item118-mysql-to-pg-prefix-unique",
		}
		err := mig.Run(ctx2min(t))
		requireRefusalBeforeAnyDataMoved(t, err,
			[]string{"email", "prefix", "left("},
			pgHasTable("prefixed"))
	})

	t.Run("prefixed NON-unique index still migrates, rows and all", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `
			DROP TABLE prefixed;
			CREATE TABLE prefixlog (
				id    BIGINT NOT NULL AUTO_INCREMENT,
				email VARCHAR(255) NOT NULL,
				PRIMARY KEY (id),
				KEY prefixlog_email_idx (email(10))
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO prefixlog (email) VALUES ('a@example.com'), ('b@example.com');
		`)

		mig := &Migrator{
			Source: mysqlEng, Target: pgEng,
			SourceDSN: mysqlSource, TargetDSN: pgTarget,
			MigrationID: "item118-mysql-to-pg-prefix-nonunique",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("a prefixed NON-unique index must still migrate — prefix indexes on TEXT columns are "+
				"routine in MySQL schemas and the prefix is a size choice there. An over-refusing "+
				"preflight is a worse defect than the late refusal it replaces: %v", err)
		}
		var n int
		if err := pgDB.QueryRow(`SELECT COUNT(*) FROM prefixlog`).Scan(&n); err != nil {
			t.Fatalf("count prefixlog on the target: %v", err)
		}
		if n != 2 {
			t.Errorf("prefixlog row count on the target = %d; want 2", n)
		}
	})
}

// TestMigrate_IndexPreflight_MySQLToSQLite_PrefixUnique covers the SQLite
// target — the sharpest member of the class, because SQLite's prefix check had
// its ONLY call site in CREATE INDEX and this engine renders no index inline,
// so before item 118 there was no early path at all.
func TestMigrate_IndexPreflight_MySQLToSQLite_PrefixUnique(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}

	sqliteHasTable := func(dst, name string) func() bool {
		return func() bool {
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

	t.Run("prefixed UNIQUE is refused before any data moves", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `
			CREATE TABLE sq_prefixed (
				id    BIGINT NOT NULL AUTO_INCREMENT,
				email VARCHAR(255) NOT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY sq_prefixed_email_uniq (email(10))
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO sq_prefixed (email) VALUES ('a@example.com'), ('b@example.com');
		`)

		dst := filepath.Join(t.TempDir(), "item118-refused.db")
		mig := &Migrator{
			Source: mysqlEng, Target: sqliteEng,
			SourceDSN: mysqlSource, TargetDSN: dst,
			MigrationID: "item118-mysql-to-sqlite-prefix-unique",
		}
		err := mig.Run(ctx2min(t))
		requireRefusalBeforeAnyDataMoved(t, err,
			[]string{"sq_prefixed_email_uniq", "email", "substr("},
			sqliteHasTable(dst, "sq_prefixed"))
	})

	t.Run("prefixed NON-unique index still migrates, rows and all", func(t *testing.T) {
		applyMySQLDDL(t, mysqlSource, `
			DROP TABLE sq_prefixed;
			CREATE TABLE sq_prefixlog (
				id    BIGINT NOT NULL AUTO_INCREMENT,
				email VARCHAR(255) NOT NULL,
				PRIMARY KEY (id),
				KEY sq_prefixlog_email_idx (email(10))
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
			INSERT INTO sq_prefixlog (email) VALUES ('a@example.com'), ('b@example.com');
		`)

		dst := filepath.Join(t.TempDir(), "item118-allowed.db")
		mig := &Migrator{
			Source: mysqlEng, Target: sqliteEng,
			SourceDSN: mysqlSource, TargetDSN: dst,
			MigrationID: "item118-mysql-to-sqlite-prefix-nonunique",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("a prefixed NON-unique index must still migrate to SQLite: %v", err)
		}
		db, err := sql.Open("sqlite", dst)
		if err != nil {
			t.Fatalf("open sqlite target: %v", err)
		}
		defer func() { _ = db.Close() }()
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sq_prefixlog`).Scan(&n); err != nil {
			t.Fatalf("count sq_prefixlog on the target: %v", err)
		}
		if n != 2 {
			t.Errorf("sq_prefixlog row count on the target = %d; want 2", n)
		}
	})
}
