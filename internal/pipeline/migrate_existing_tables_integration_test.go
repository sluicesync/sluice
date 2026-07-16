//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the ADR-0166 pre-create gate (roadmap item 71b)
// on real MySQL and Postgres targets:
//
//   - a pre-created table with a MATCHING column shape (deliberately
//     carrying an EXTRA secondary index — the deploy-ddl bootstrap
//     shape) is skipped at CREATE time and the migrate completes end
//     to end with exact data;
//   - a pre-created table with a DIFFERING column shape refuses
//     upfront (coded SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH) with ZERO
//     rows copied — replacing the pre-gate behavior where the conflict
//     surfaced mid-copy as an Error-1054 retried for the full ADR-0108
//     wall (the v0.99.256 cycle observation).
//
// The equal case is deliberately seeded through the same DDL text on
// both sides so the round-trip property the gate relies on (intended
// IR == target read-back IR for a shape-identical table) is exercised
// against the real catalogs, not fakes.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// existingGateMySQLDDL is the shared table shape for the MySQL leg —
// several type families on purpose (int/unsigned/auto-inc, bool-tier
// TINYINT(1), decimal, varchar, enum, timestamp) so shape equality is
// proven across families, not one representative.
const existingGateMySQLDDL = `
	CREATE TABLE gate_users (
		id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		email   VARCHAR(255)    NOT NULL,
		active  TINYINT(1)      NOT NULL,
		balance DECIMAL(10,2)   NULL,
		tier    ENUM('free','pro') NULL,
		joined  TIMESTAMP(0)    NULL,
		PRIMARY KEY (id),
		UNIQUE KEY gate_users_email_unique (email)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

func TestMigrate_ExistingTableGate_MySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	applyMySQLDDL(t, sourceDSN, existingGateMySQLDDL+`
		CREATE TABLE gate_fresh (
			id   BIGINT NOT NULL,
			note TEXT   NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO gate_users (email, active, balance, tier, joined) VALUES
			('alice@example.com', 1, 12.50, 'pro',  '2026-01-02 03:04:05'),
			('bob@example.com',   0, NULL,  'free', NULL);
		INSERT INTO gate_fresh (id, note) VALUES (1, 'hello'), (2, NULL);
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	t.Run("equal pre-created table with extra index migrates end to end", func(t *testing.T) {
		// Pre-create gate_users on the TARGET with the identical column
		// shape PLUS an extra secondary index — indexes are outside the
		// gate by design (a bootstrapped table legitimately carries
		// them). gate_fresh is NOT pre-created.
		applyMySQLDDL(t, targetDSN, existingGateMySQLDDL+`
			CREATE INDEX gate_users_extra_idx ON gate_users (active, email);
		`)

		mig := &Migrator{Source: mysqlEng, Target: mysqlEng, SourceDSN: sourceDSN, TargetDSN: targetDSN}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run over a pre-created equal table: %v", err)
		}

		db := openSQLDB(t, "mysql", targetDSN)
		assertRowCount(t, db, "gate_users", 2)
		assertRowCount(t, db, "gate_fresh", 2)
		// Exact data landed in the pre-created table.
		var email string
		var balance sql.NullString
		if err := db.QueryRow(`SELECT email, balance FROM gate_users WHERE id = 1`).Scan(&email, &balance); err != nil {
			t.Fatalf("read back gate_users: %v", err)
		}
		if email != "alice@example.com" || !balance.Valid || balance.String != "12.50" {
			t.Errorf("gate_users row 1 = (%q, %v); want alice@example.com / 12.50", email, balance)
		}
		// The extra index survived (the gate never touches indexes).
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics
			 WHERE table_schema = DATABASE() AND table_name = 'gate_users' AND index_name = 'gate_users_extra_idx'`,
		).Scan(&n); err != nil || n != 1 {
			t.Errorf("extra index present = %d (err %v); want 1", n, err)
		}
	})

	t.Run("differing pre-created table refuses upfront with zero rows copied", func(t *testing.T) {
		// Reset the target database from the SOURCE connection (dropping
		// the database you're connected to leaves the session schemaless),
		// then pre-create the conflicting shape.
		applyMySQLDDL(t, sourceDSN, `DROP DATABASE target_db; CREATE DATABASE target_db;`)
		applyMySQLDDL(t, targetDSN, `CREATE TABLE gate_users (only_col VARCHAR(10) NULL) ENGINE=InnoDB;`)

		// Fresh MigrationID for the same reason as the PG leg: the
		// equal-leg subtest completed under the auto-derived ID.
		mig := &Migrator{Source: mysqlEng, Target: mysqlEng, SourceDSN: sourceDSN, TargetDSN: targetDSN, MigrationID: "gate-differ"}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		err := mig.Run(ctx)
		assertShapeMismatchRefusal(t, err, "gate_users")

		db := openSQLDB(t, "mysql", targetDSN)
		assertRowCount(t, db, "gate_users", 0)
		// The refusal fired BEFORE the CREATE phase: the fresh sibling
		// table must not have been created either.
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM information_schema.tables
			 WHERE table_schema = DATABASE() AND table_name = 'gate_fresh'`,
		).Scan(&n); err != nil || n != 0 {
			t.Errorf("gate_fresh created despite the upfront refusal (n=%d, err=%v)", n, err)
		}
	})
}

// existingGatePGDDL is the shared table shape for the PG leg — again
// multiple families (identity int, varchar, boolean, numeric, jsonb,
// timestamptz, int[]).
const existingGatePGDDL = `
	CREATE TABLE gate_users (
		id      BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		email   VARCHAR(255) NOT NULL,
		active  BOOLEAN      NOT NULL,
		balance NUMERIC(10,2),
		meta    JSONB,
		joined  TIMESTAMPTZ,
		scores  INT[]
	);
`

func TestMigrate_ExistingTableGate_PG(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, existingGatePGDDL+`
		CREATE TABLE gate_fresh (id BIGINT PRIMARY KEY, note TEXT);
		INSERT INTO gate_users (email, active, balance, meta, joined, scores) VALUES
			('alice@example.com', true,  12.50, '{"a":1}', '2026-01-02 03:04:05+00', '{1,2,3}'),
			('bob@example.com',   false, NULL,  NULL,      NULL,                     NULL);
		INSERT INTO gate_fresh (id, note) VALUES (1, 'hello'), (2, NULL);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	t.Run("equal pre-created table with extra index migrates end to end", func(t *testing.T) {
		applyPGDDL(t, targetDSN, existingGatePGDDL+`
			CREATE INDEX gate_users_extra_idx ON gate_users (active, email);
		`)

		mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run over a pre-created equal table: %v", err)
		}

		db := openSQLDB(t, "pgx", targetDSN)
		assertRowCount(t, db, "gate_users", 2)
		assertRowCount(t, db, "gate_fresh", 2)
		var email string
		var scores string
		if err := db.QueryRow(`SELECT email, scores::text FROM gate_users WHERE id = 1`).Scan(&email, &scores); err != nil {
			t.Fatalf("read back gate_users: %v", err)
		}
		if email != "alice@example.com" || scores != "{1,2,3}" {
			t.Errorf("gate_users row 1 = (%q, %q); want alice@example.com / {1,2,3}", email, scores)
		}
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'gate_users' AND indexname = 'gate_users_extra_idx'`,
		).Scan(&n); err != nil || n != 1 {
			t.Errorf("extra index present = %d (err %v); want 1", n, err)
		}
	})

	t.Run("differing pre-created table refuses upfront with zero rows copied", func(t *testing.T) {
		applyPGDDL(t, targetDSN, `
			DROP TABLE gate_users, gate_fresh;
			CREATE TABLE gate_users (only_col VARCHAR(10));
		`)

		// Fresh MigrationID: the equal-leg subtest above completed under
		// the auto-derived ID for this DSN pair, and a completed state
		// row would refuse the re-run before the gate gets a say.
		mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN, MigrationID: "gate-differ"}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		err := mig.Run(ctx)
		assertShapeMismatchRefusal(t, err, "gate_users")

		db := openSQLDB(t, "pgx", targetDSN)
		assertRowCount(t, db, "gate_users", 0)
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_name = 'gate_fresh'`,
		).Scan(&n); err != nil || n != 0 {
			t.Errorf("gate_fresh created despite the upfront refusal (n=%d, err=%v)", n, err)
		}
	})
}

// ---- shared assertion helpers ----

func openSQLDB(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func assertRowCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if n != want {
		t.Errorf("%s row count = %d; want %d", table, n, want)
	}
}

func assertShapeMismatchRefusal(t *testing.T, err error, table string) {
	t.Helper()
	if err == nil {
		t.Fatal("want the coded shape-mismatch refusal; got nil")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetTableShapeMismatch {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeTargetTableShapeMismatch)
	}
	if !strings.Contains(err.Error(), table) {
		t.Errorf("refusal %q does not name table %q", err.Error(), table)
	}
}
