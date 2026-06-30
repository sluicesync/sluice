//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for multi-schema Postgres migration (ADR-0075,
// Phase 2a). Each boots a Postgres source database with TWO user schemas
// and verifies the fan-out:
//
//   (a) PG → PG: two source schemas land in two same-named target
//       schemas, every row in the right schema, system schemas never
//       copied.
//   (b) PG → MySQL: two source schemas land in two auto-created same-
//       named MySQL databases, rows correct.
//   (c) cross-schema FK class pin: same-schema FK preserved, cross-IN-
//       scope-schema FK preserved (deferred final pass), cross-OUT-of-
//       scope-schema FK REFUSED LOUDLY (target pristine).
//   (d) --dry-run performs no target writes.
//   (e) single-schema mode (no --*-schema flag) is byte-identical to
//       today (back-compat).

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

// TestMigrate_MultiSchema_PostgresToPostgres is scenario (a): two source
// schemas → two same-named target schemas, rows isolated per schema,
// system schemas never copied.
func TestMigrate_MultiSchema_PostgresToPostgres(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()

	// Two user schemas on the source database, each with a same-named
	// `widgets` table to prove namespace isolation. `public` is left
	// untouched and NOT selected, so it must not appear on the target.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.widgets   (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE billing.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO sales.widgets   (id, name) VALUES (1, 'a-one'), (2, 'a-two');
		INSERT INTO billing.widgets (id, name) VALUES (1, 'b-one'), (2, 'b-two'), (3, 'b-three');
	`)

	pgEng, _ := engines.Get("postgres")

	mig := &Migrator{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      pgSource,
		TargetDSN:      pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("multi-schema Migrator.Run: %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("sql.Open pg target: %v", err)
	}
	defer func() { _ = db.Close() }()

	var salesCount, billingCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sales.widgets`).Scan(&salesCount); err != nil {
		t.Fatalf("count sales.widgets: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing.widgets`).Scan(&billingCount); err != nil {
		t.Fatalf("count billing.widgets: %v", err)
	}
	if salesCount != 2 {
		t.Errorf("sales.widgets = %d; want 2", salesCount)
	}
	if billingCount != 3 {
		t.Errorf("billing.widgets = %d; want 3", billingCount)
	}

	// Data isolation: sales has 'a-*', billing has 'b-*'.
	var firstSales string
	if err := db.QueryRowContext(ctx, `SELECT name FROM sales.widgets ORDER BY id LIMIT 1`).Scan(&firstSales); err != nil {
		t.Fatalf("read sales name: %v", err)
	}
	if !strings.HasPrefix(firstSales, "a-") {
		t.Errorf("sales.widgets first name = %q; want a-* (cross-schema bleed)", firstSales)
	}

	// System schemas were never copied as user schemas: there must be no
	// `widgets` (or any sluice-created table) in pg_catalog /
	// information_schema, and no rogue user schema named pg_catalog.
	var sysTables int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE  table_schema IN ('pg_catalog', 'information_schema', 'pg_toast')
		  AND  table_name = 'widgets'`).Scan(&sysTables); err != nil {
		t.Fatalf("count system widgets: %v", err)
	}
	if sysTables != 0 {
		t.Errorf("found %d widgets table(s) in system schemas; want 0", sysTables)
	}
}

// TestMigrate_MultiSchema_RenameMap_PostgresToPostgres pins ADR-0142: a
// map-only run (--map-schema, no --include-schema) selects exactly the map
// keys and lands each source schema's data in its RENAMED target schema —
// the source-named schemas must NOT exist on the target (proving a rename,
// not a same-name copy), and rows must be isolated per renamed schema.
func TestMigrate_MultiSchema_RenameMap_PostgresToPostgres(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()

	// Source schemas sales + billing; `public` is untouched and unmapped, so
	// the map-only selection must leave it out entirely.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.widgets   (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE billing.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO sales.widgets   (id, name) VALUES (1, 'a-one'), (2, 'a-two');
		INSERT INTO billing.widgets (id, name) VALUES (1, 'b-one'), (2, 'b-two'), (3, 'b-three');
	`)

	pgEng, _ := engines.Get("postgres")

	nsMap, err := NewNamespaceRenameMap([]string{"sales=sales_renamed", "billing=billing_renamed"})
	if err != nil {
		t.Fatalf("construct rename map: %v", err)
	}
	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: pgSource,
		TargetDSN: pgTarget,
		// Map-only: no DatabaseFilter, the map keys ARE the selection.
		NamespaceMap: nsMap,
	}
	if !mig.multiDatabaseMode() {
		t.Fatal("a rename map alone should engage multi-schema mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("rename-map multi-schema Migrator.Run: %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("sql.Open pg target: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Data landed in the RENAMED target schemas, isolated per schema.
	var salesCount, billingCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sales_renamed.widgets`).Scan(&salesCount); err != nil {
		t.Fatalf("count sales_renamed.widgets: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_renamed.widgets`).Scan(&billingCount); err != nil {
		t.Fatalf("count billing_renamed.widgets: %v", err)
	}
	if salesCount != 2 {
		t.Errorf("sales_renamed.widgets = %d; want 2", salesCount)
	}
	if billingCount != 3 {
		t.Errorf("billing_renamed.widgets = %d; want 3", billingCount)
	}
	var firstSales string
	if err := db.QueryRowContext(ctx, `SELECT name FROM sales_renamed.widgets ORDER BY id LIMIT 1`).Scan(&firstSales); err != nil {
		t.Fatalf("read sales_renamed name: %v", err)
	}
	if !strings.HasPrefix(firstSales, "a-") {
		t.Errorf("sales_renamed.widgets first name = %q; want a-* (cross-schema bleed)", firstSales)
	}

	// The SOURCE-named schemas must NOT exist on the target — the rename
	// routed only to the new names, and `public` (unmapped) was never selected.
	var staleCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.schemata
		WHERE  schema_name IN ('sales', 'billing')`).Scan(&staleCount); err != nil {
		t.Fatalf("count source-named target schemas: %v", err)
	}
	if staleCount != 0 {
		t.Errorf("found %d source-named target schema(s); want 0 (rename must not create same-named schemas)", staleCount)
	}
}

// TestMigrate_MultiSchema_PostgresToMySQL is scenario (b): two source
// schemas → two auto-created same-named MySQL databases.
func TestMigrate_MultiSchema_PostgresToMySQL(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.accounts   (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
		CREATE TABLE billing.accounts (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
		INSERT INTO sales.accounts   (id, tag) VALUES (1, 'src-1'), (2, 'src-2');
		INSERT INTO billing.accounts (id, tag) VALUES (1, 'bil-1'), (2, 'bil-2'), (3, 'bil-3');
	`)

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")

	mig := &Migrator{
		Source:         pgEng,
		Target:         mysqlEng,
		SourceDSN:      pgSource,
		TargetDSN:      serverDSN(t, mysqlSource),
		DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("multi-schema PG->MySQL Migrator.Run: %v", err)
	}

	for _, c := range []struct {
		db   string
		want int
		tag  string
	}{
		{"sales", 2, "src-1"},
		{"billing", 3, "bil-1"},
	} {
		dsn, err := buildMySQLDSN(serverDSN(t, mysqlSource), c.db)
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
			t.Errorf("%s.accounts first tag = %q; want %q (cross-schema bleed)", c.db, firstTag, c.tag)
		}
	}
}

// TestMigrate_MultiSchema_FK_ClassPin is scenario (c) — the sharpest
// correctness point. It pins the cross-schema FK carve-out CLASS:
//
//   - same-schema FK preserved (app.orders → app.customers).
//   - cross-IN-scope-schema FK preserved via the deferred final pass
//     (app.orders → shared.regions, both in scope).
//   - cross-OUT-of-scope-schema FK REFUSED LOUDLY (app.orders → secret.vault,
//     secret NOT selected), target left pristine.
func TestMigrate_MultiSchema_FK_ClassPin(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()

	// Parents first; cross-schema FKs reference them. The out-of-scope
	// referent (secret.vault) is created but `secret` is never selected.
	applyPGDDL(t, pgSource, `
		CREATE SCHEMA app;
		CREATE SCHEMA shared;
		CREATE SCHEMA secret;

		CREATE TABLE shared.regions (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO shared.regions (id, name) VALUES (1, 'west'), (2, 'east');

		CREATE TABLE secret.vault (id BIGINT PRIMARY KEY);
		INSERT INTO secret.vault (id) VALUES (1);

		CREATE TABLE app.customers (id BIGINT PRIMARY KEY);
		INSERT INTO app.customers (id) VALUES (10), (11);

		CREATE TABLE app.orders (
			id          BIGINT PRIMARY KEY,
			customer_id BIGINT NOT NULL REFERENCES app.customers (id),
			region_id   BIGINT NOT NULL REFERENCES shared.regions (id)
		);
		INSERT INTO app.orders (id, customer_id, region_id) VALUES (100, 10, 1), (101, 11, 2);
	`)

	pgEng, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// ---- Run 1: app + shared in scope. Same-schema FK + cross-in-scope
	// FK must both migrate cleanly (the cross-in-scope FK lands via the
	// deferred final pass).
	migInScope := &Migrator{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      pgSource,
		TargetDSN:      pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"app", "shared"}},
	}
	if err := migInScope.Run(ctx); err != nil {
		t.Fatalf("in-scope cross-schema FK run should succeed: %v", err)
	}

	tdb, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tdb.Close() }()

	refSchemas := readPGForeignKeyRefSchemas(t, ctx, tdb, "app", "orders")
	if _, ok := refSchemas["app"]; !ok {
		t.Errorf("same-schema FK (→ app) not found; refs=%v", refSchemas)
	}
	if _, ok := refSchemas["shared"]; !ok {
		t.Errorf("cross-in-scope FK (→ shared) not found; refs=%v", refSchemas)
	}

	// ---- Run 2: add an FK from app into the OUT-of-scope secret schema,
	// then migrate app + shared only. The run MUST refuse loudly and the
	// target must stay pristine (fresh target db).
	applyPGDDL(t, pgSource, `
		ALTER TABLE app.orders ADD COLUMN vault_id BIGINT NULL REFERENCES secret.vault (id);
	`)

	// A clean second target database so "pristine" is unambiguous.
	rootDB, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatalf("open pg source for create-db: %v", err)
	}
	if _, err := rootDB.ExecContext(ctx, "CREATE DATABASE target_db2"); err != nil {
		_ = rootDB.Close()
		t.Fatalf("create target_db2: %v", err)
	}
	_ = rootDB.Close()
	target2, err := buildPGDSN(pgTarget, "target_db2")
	if err != nil {
		t.Fatalf("build target2 DSN: %v", err)
	}

	migOutScope := &Migrator{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      pgSource,
		TargetDSN:      target2,
		DatabaseFilter: DatabaseFilter{Include: []string{"app", "shared"}},
	}
	err = migOutScope.Run(ctx)
	if err == nil {
		t.Fatal("out-of-scope cross-schema FK should have been REFUSED; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "secret") {
		t.Errorf("refusal %q should name the out-of-scope schema secret", msg)
	}
	if !strings.Contains(msg, "OUTSIDE the selected multi-schema set") {
		t.Errorf("refusal %q should explain the out-of-scope condition", msg)
	}
	if !strings.Contains(msg, "--include-schema") {
		t.Errorf("refusal %q should name the --include-schema remedy", msg)
	}

	// Target pristine: neither app nor shared schema was created on target2
	// (the refusal fired at pre-flight, before any target write).
	t2db, err := sql.Open("pgx", target2)
	if err != nil {
		t.Fatalf("open target2: %v", err)
	}
	defer func() { _ = t2db.Close() }()
	var nsCount int
	if err := t2db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.schemata
		WHERE  schema_name IN ('app', 'shared')`).Scan(&nsCount); err != nil {
		t.Fatalf("count target2 schemas: %v", err)
	}
	if nsCount != 0 {
		t.Errorf("refused run created %d target schema(s); want 0 (target must be pristine)", nsCount)
	}
}

// TestMigrate_MultiSchema_DryRun_NoTargetWrites is scenario (d): a
// --dry-run in multi-schema mode mutates NOTHING on the target — no
// CREATE SCHEMA, no deferred-FK pass.
func TestMigrate_MultiSchema_DryRun_NoTargetWrites(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, pgSource, `
		CREATE SCHEMA dryrun_alpha;
		CREATE SCHEMA dryrun_beta;
		CREATE TABLE dryrun_alpha.t (id BIGINT PRIMARY KEY);
		CREATE TABLE dryrun_beta.t  (id BIGINT PRIMARY KEY);
		INSERT INTO dryrun_alpha.t (id) VALUES (1);
	`)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      pgSource,
		TargetDSN:      pgTarget,
		DatabaseFilter: DatabaseFilter{Include: []string{"dryrun_alpha", "dryrun_beta"}},
		DryRun:         true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("dry-run multi-schema Migrator.Run: %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.schemata
		WHERE  schema_name IN ('dryrun_alpha', 'dryrun_beta')`).Scan(&n); err != nil {
		t.Fatalf("count target schemas: %v", err)
	}
	if n != 0 {
		t.Errorf("dry-run created %d target schema(s); want 0 (a dry run must not mutate the target)", n)
	}
}

// TestMigrate_SingleSchema_BackCompat is scenario (e): with NO --*-schema
// flag the PG → PG migrate is byte-identical to today — a single schema
// from the DSN, no multi-schema path engaged.
func TestMigrate_SingleSchema_BackCompat(t *testing.T) {
	pgSource, pgTarget, cleanup := startPostgres(t)
	defer cleanup()

	// One table in the default `public` schema; no schema-scope flag.
	applyPGDDL(t, pgSource, `
		CREATE TABLE gadgets (id BIGINT PRIMARY KEY, label TEXT NOT NULL);
		INSERT INTO gadgets (id, label) VALUES (1, 'x'), (2, 'y');
	`)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: pgSource,
		TargetDSN: pgTarget,
		// No DatabaseFilter / AllDatabases — single-schema path.
	}
	if mig.multiDatabaseMode() {
		t.Fatal("single-schema migrate reported multiDatabaseMode()=true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("single-schema Migrator.Run: %v", err)
	}

	if got := countRows(t, pgTarget, "public.gadgets"); got != 2 {
		t.Errorf("public.gadgets = %d; want 2", got)
	}
}

// readPGForeignKeyRefSchemas returns the set of referenced schemas for
// schema.table's foreign keys, read from the target's pg_constraint.
func readPGForeignKeyRefSchemas(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) map[string]struct{} {
	t.Helper()
	const q = `
		SELECT DISTINCT pn.nspname
		FROM   pg_constraint con
		JOIN   pg_class      cl  ON cl.oid  = con.conrelid
		JOIN   pg_namespace  n   ON n.oid   = cl.relnamespace
		JOIN   pg_class      pcl ON pcl.oid = con.confrelid
		JOIN   pg_namespace  pn  ON pn.oid  = pcl.relnamespace
		WHERE  n.nspname  = $1
		  AND  cl.relname = $2
		  AND  con.contype = 'f'`
	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		t.Fatalf("read PG FKs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	refs := map[string]struct{}{}
	for rows.Next() {
		var refSchema string
		if err := rows.Scan(&refSchema); err != nil {
			t.Fatalf("scan FK row: %v", err)
		}
		refs[refSchema] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("FK rows: %v", err)
	}
	return refs
}
