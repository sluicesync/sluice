//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for multi-database MySQL migration (ADR-0074,
// Phase 1a). Each boots a MySQL source server with TWO user databases
// and verifies the fan-out:
//
//   (a) MySQL → PG: two source databases land in two same-named PG
//       schemas, every row in the right schema.
//   (b) MySQL → MySQL: two source databases land in two auto-created
//       same-named target databases, rows correct.
//   (c) FK carve-out class pin: same-database FK preserved, cross-IN-
//       scope-database FK preserved (ReferencedSchema populated), and
//       cross-OUT-of-scope-database FK REFUSED LOUDLY at pre-flight.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// mustCreateMySQLDatabase creates a database on the MySQL server the
// baseDSN authorises and returns a DSN pointing at it. Mirrors
// mustCreateDatabase (the PG sibling) for the multi-database fan-out
// tests.
func mustCreateMySQLDatabase(t *testing.T, baseDSN, dbName string) string {
	t.Helper()
	db, err := sql.Open("mysql", baseDSN+"&multiStatements=true")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create database %q: %v", dbName, err)
	}
	dsn, err := buildMySQLDSN(baseDSN, dbName)
	if err != nil {
		t.Fatalf("build DSN for %q: %v", dbName, err)
	}
	return dsn
}

// serverDSN strips the database component from a MySQL DSN so the
// multi-database orchestrator gets a server connection (database
// optional), exercising the relaxed parseServerDSN path.
func serverDSN(t *testing.T, dsn string) string {
	t.Helper()
	s, err := buildMySQLDSN(dsn, "")
	if err != nil {
		t.Fatalf("strip db from DSN: %v", err)
	}
	return s
}

// TestMigrate_MultiDatabase_MySQLToPostgres is scenario (a): two source
// databases → two same-named PG schemas, rows isolated per schema.
func TestMigrate_MultiDatabase_MySQLToPostgres(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// source_db already exists (startMySQL creates it). Seed it plus a
	// second database shop_db. Both have a same-named `widgets` table to
	// prove namespace isolation.
	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('a-one'), ('a-two');
	`)
	shopDSN := mustCreateMySQLDatabase(t, mysqlSource, "shop_db")
	applyMySQLDDL(t, shopDSN, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('b-one'), ('b-two'), ('b-three');
	`)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: serverDSN(t, mysqlSource),
		TargetDSN: pgTarget,
		// Only the two user databases — and prove the filter works by
		// naming them explicitly.
		DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("multi-database Migrator.Run: %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("sql.Open pg: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Each source database landed in its own same-named schema.
	var srcCount, shopCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_db.widgets`).Scan(&srcCount); err != nil {
		t.Fatalf("count source_db.widgets: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM shop_db.widgets`).Scan(&shopCount); err != nil {
		t.Fatalf("count shop_db.widgets: %v", err)
	}
	if srcCount != 2 {
		t.Errorf("source_db.widgets = %d; want 2", srcCount)
	}
	if shopCount != 3 {
		t.Errorf("shop_db.widgets = %d; want 3", shopCount)
	}

	// Data isolation: source_db has the 'a-*' names, shop_db the 'b-*'.
	var srcName string
	if err := db.QueryRowContext(ctx, `SELECT name FROM source_db.widgets ORDER BY id LIMIT 1`).Scan(&srcName); err != nil {
		t.Fatalf("read source_db name: %v", err)
	}
	if !strings.HasPrefix(srcName, "a-") {
		t.Errorf("source_db.widgets first name = %q; want a-* (cross-schema bleed)", srcName)
	}
}

// TestMigrate_MultiDatabase_MySQLToMySQL is scenario (b): two source
// databases → two auto-created same-named target databases.
func TestMigrate_MultiDatabase_MySQLToMySQL(t *testing.T) {
	// startMySQL gives a source and a (pre-created) target_db on the
	// SAME container. For MySQL → MySQL multi-database we need the target
	// to be a SEPARATE server so the auto-CREATE-DATABASE of the source
	// database names doesn't collide with the source databases. Boot a
	// second MySQL.
	srcServer, _, srcCleanup := startMySQL(t)
	defer srcCleanup()
	tgtServer, _, tgtCleanup := startMySQL(t)
	defer tgtCleanup()

	applyMySQLDDL(t, srcServer, `
		CREATE TABLE accounts (
			id     BIGINT NOT NULL AUTO_INCREMENT,
			tag    VARCHAR(32) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO accounts (tag) VALUES ('src-1'), ('src-2');
	`)
	shopDSN := mustCreateMySQLDatabase(t, srcServer, "shop_db")
	applyMySQLDDL(t, shopDSN, `
		CREATE TABLE accounts (
			id     BIGINT NOT NULL AUTO_INCREMENT,
			tag    VARCHAR(32) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO accounts (tag) VALUES ('shop-1'), ('shop-2'), ('shop-3');
	`)

	mysqlEng, _ := engines.Get("mysql")

	mig := &Migrator{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      serverDSN(t, tgtServer),
		DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("multi-database MySQL->MySQL Migrator.Run: %v", err)
	}

	// Verify both target databases were auto-created with the right rows.
	for _, c := range []struct {
		db   string
		want int
		tag  string
	}{
		{"source_db", 2, "src-1"},
		{"shop_db", 3, "shop-1"},
	} {
		dsn, err := buildMySQLDSN(serverDSN(t, tgtServer), c.db)
		if err != nil {
			t.Fatalf("build target DSN %q: %v", c.db, err)
		}
		conn, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("sql.Open %q: %v", c.db, err)
		}
		var n int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM accounts").Scan(&n); err != nil {
			_ = conn.Close()
			t.Fatalf("count %s.accounts: %v", c.db, err)
		}
		var firstTag string
		if err := conn.QueryRowContext(ctx, "SELECT tag FROM accounts ORDER BY id LIMIT 1").Scan(&firstTag); err != nil {
			_ = conn.Close()
			t.Fatalf("read %s tag: %v", c.db, err)
		}
		_ = conn.Close()
		if n != c.want {
			t.Errorf("%s.accounts = %d; want %d", c.db, n, c.want)
		}
		if firstTag != c.tag {
			t.Errorf("%s.accounts first tag = %q; want %q (cross-db bleed)", c.db, firstTag, c.tag)
		}
	}
}

// TestMigrate_MultiDatabase_FK_ClassPin is scenario (c) — the sharpest
// correctness point. It pins the FK carve-out CLASS:
//
//   - same-database FK is preserved (orders → customers in app_db).
//   - cross-IN-scope-database FK is preserved with ReferencedSchema
//     populated (app_db.orders → shared_db.regions, both in scope).
//   - cross-OUT-of-scope-database FK is REFUSED LOUDLY (app_db has an FK
//     into other_db, which is NOT in the selected set).
//
// The three live on different runs so the refusal doesn't mask the
// preserved cases.
func TestMigrate_MultiDatabase_FK_ClassPin(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// Three databases on one server: app_db (children), shared_db
	// (in-scope referent), other_db (out-of-scope referent).
	appDSN := mustCreateMySQLDatabase(t, mysqlSource, "app_db")
	sharedDSN := mustCreateMySQLDatabase(t, mysqlSource, "shared_db")
	otherDSN := mustCreateMySQLDatabase(t, mysqlSource, "other_db")

	// Parents first (FKs reference them).
	applyMySQLDDL(t, sharedDSN, `
		CREATE TABLE regions (
			id   BIGINT NOT NULL,
			name VARCHAR(32) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO regions (id, name) VALUES (1, 'west'), (2, 'east');
	`)
	applyMySQLDDL(t, otherDSN, `
		CREATE TABLE secrets (
			id BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO secrets (id) VALUES (1);
	`)
	// app_db: a same-db FK (orders → customers) AND a cross-IN-scope FK
	// (orders.region_id → shared_db.regions).
	applyMySQLDDL(t, appDSN, `
		CREATE TABLE customers (
			id BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO customers (id) VALUES (10), (11);

		CREATE TABLE orders (
			id          BIGINT NOT NULL,
			customer_id BIGINT NOT NULL,
			region_id   BIGINT NOT NULL,
			PRIMARY KEY (id),
			KEY orders_customer_idx (customer_id),
			KEY orders_region_idx (region_id),
			CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (id),
			CONSTRAINT orders_region_fk   FOREIGN KEY (region_id)   REFERENCES shared_db.regions (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO orders (id, customer_id, region_id) VALUES (100, 10, 1), (101, 11, 2);
	`)

	mysqlEng, _ := engines.Get("mysql")

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// ---- Run 1: app_db + shared_db in scope. Same-db FK + cross-in-
	// scope FK must both migrate cleanly (MySQL → MySQL into a clean,
	// separate target server whose databases are auto-created).
	tgtServer, _, tgtClean := startMySQL(t)
	defer tgtClean()

	migInScope := &Migrator{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, mysqlSource),
		TargetDSN:      serverDSN(t, tgtServer),
		DatabaseFilter: DatabaseFilter{Include: []string{"app_db", "shared_db"}},
	}
	if err := migInScope.Run(ctx); err != nil {
		t.Fatalf("in-scope FK run should succeed: %v", err)
	}

	// Verify both FKs landed on the target app_db.orders.
	tgtAppDSN, err := buildMySQLDSN(serverDSN(t, tgtServer), "app_db")
	if err != nil {
		t.Fatalf("build target app_db DSN: %v", err)
	}
	tdb, err := sql.Open("mysql", tgtAppDSN)
	if err != nil {
		t.Fatalf("open target app_db: %v", err)
	}
	defer func() { _ = tdb.Close() }()

	fkCount, refDBs := readForeignKeys(t, ctx, tdb, "app_db", "orders")
	if fkCount != 2 {
		t.Errorf("app_db.orders FK count = %d; want 2 (same-db + cross-in-scope)", fkCount)
	}
	// The cross-database FK must reference shared_db.regions — proving
	// ReferencedSchema was populated and routed to the same-named target
	// database, not flattened.
	if _, ok := refDBs["shared_db"]; !ok {
		t.Errorf("cross-in-scope FK did not reference shared_db; refs=%v", refDBs)
	}
	if _, ok := refDBs["app_db"]; !ok {
		t.Errorf("same-db FK did not reference app_db; refs=%v", refDBs)
	}

	// ---- Run 2: out-of-scope referent. Add an FK from app_db into
	// other_db, then migrate app_db + shared_db only (other_db OUT of
	// scope). The run MUST refuse loudly.
	applyMySQLDDL(t, appDSN, `
		ALTER TABLE orders ADD COLUMN secret_id BIGINT NULL;
		ALTER TABLE orders ADD KEY orders_secret_idx (secret_id);
		ALTER TABLE orders ADD CONSTRAINT orders_secret_fk
			FOREIGN KEY (secret_id) REFERENCES other_db.secrets (id);
	`)

	tgtServer2, _, tgtClean2 := startMySQL(t)
	defer tgtClean2()

	migOutScope := &Migrator{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, mysqlSource),
		TargetDSN:      serverDSN(t, tgtServer2),
		DatabaseFilter: DatabaseFilter{Include: []string{"app_db", "shared_db"}},
	}
	err = migOutScope.Run(ctx)
	if err == nil {
		t.Fatal("out-of-scope cross-database FK should have been REFUSED; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "other_db") {
		t.Errorf("refusal %q should name the out-of-scope database other_db", msg)
	}
	if !strings.Contains(msg, "OUTSIDE the selected multi-database set") {
		t.Errorf("refusal %q should explain the out-of-scope condition", msg)
	}
	if !strings.Contains(msg, "--include-database") || !strings.Contains(msg, "--exclude-table") {
		t.Errorf("refusal %q should name the --include-database / --exclude-table remedy", msg)
	}
}

// readForeignKeys returns the FK count and the set of referenced
// databases for schema.table, read from the target's
// information_schema.key_column_usage.
func readForeignKeys(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) (int, map[string]struct{}) {
	t.Helper()
	const q = `
		SELECT DISTINCT constraint_name, referenced_table_schema
		FROM   information_schema.key_column_usage
		WHERE  table_schema = ? AND table_name = ?
		  AND  referenced_table_name IS NOT NULL`
	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		t.Fatalf("read FKs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	refs := map[string]struct{}{}
	names := map[string]struct{}{}
	for rows.Next() {
		var name, refSchema string
		if err := rows.Scan(&name, &refSchema); err != nil {
			t.Fatalf("scan FK row: %v", err)
		}
		names[name] = struct{}{}
		refs[refSchema] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("FK rows: %v", err)
	}
	return len(names), refs
}

// TestMigrate_MultiDatabase_DryRun_NoTargetWrites pins that --dry-run in
// multi-database mode mutates NOTHING on the target. The fan-out
// orchestrator auto-creates a target database per source database
// (EnsureDatabase) and runs a final foreign-key pass — both are target
// writes that a dry run must skip. A dry run that created empty target
// databases would violate the dry-run safety contract.
func TestMigrate_MultiDatabase_DryRun_NoTargetWrites(t *testing.T) {
	srcServer, _, srcCleanup := startMySQL(t)
	defer srcCleanup()
	tgtServer, _, tgtCleanup := startMySQL(t)
	defer tgtCleanup()

	// Use database names that do NOT collide with startMySQL's default
	// fixture database (source_db, which exists on BOTH servers), so
	// "exists on the target" can ONLY mean the dry run created it.
	alphaDSN := mustCreateMySQLDatabase(t, srcServer, "dryrun_alpha")
	applyMySQLDDL(t, alphaDSN, `
		CREATE TABLE accounts (
			id  BIGINT NOT NULL AUTO_INCREMENT,
			tag VARCHAR(32) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO accounts (tag) VALUES ('a-1');
	`)
	betaDSN := mustCreateMySQLDatabase(t, srcServer, "dryrun_beta")
	applyMySQLDDL(t, betaDSN, `
		CREATE TABLE accounts (
			id  BIGINT NOT NULL AUTO_INCREMENT,
			tag VARCHAR(32) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	mysqlEng, _ := engines.Get("mysql")
	mig := &Migrator{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      serverDSN(t, tgtServer),
		DatabaseFilter: DatabaseFilter{Include: []string{"dryrun_alpha", "dryrun_beta"}},
		DryRun:         true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("dry-run multi-database Migrator.Run: %v", err)
	}

	// The target server must have NEITHER dryrun_alpha NOR dryrun_beta — a
	// dry run must not have auto-created them.
	probeDSN, err := buildMySQLDSN(serverDSN(t, tgtServer), "information_schema")
	if err != nil {
		t.Fatalf("build target probe DSN: %v", err)
	}
	conn, err := sql.Open("mysql", probeDSN)
	if err != nil {
		t.Fatalf("sql.Open target: %v", err)
	}
	defer func() { _ = conn.Close() }()
	var n int
	if err := conn.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN ('dryrun_alpha', 'dryrun_beta')",
	).Scan(&n); err != nil {
		t.Fatalf("count target databases: %v", err)
	}
	if n != 0 {
		t.Errorf("dry-run created %d target database(s); want 0 (a dry run must not mutate the target)", n)
	}
}
