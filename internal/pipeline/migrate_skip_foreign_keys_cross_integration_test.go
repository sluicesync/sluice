//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end proof for --skip-foreign-keys (Migrator.SkipForeignKeys):
// a Postgres source with (a) an FK whose referencing column is ALREADY indexed
// and (b) an FK whose referencing column is NOT indexed (PG, unlike MySQL, does
// not auto-create FK backing indexes) is migrated to a MySQL target with the
// flag set. The MySQL target must end up with NO foreign keys, an index covering
// EVERY FK's referencing column (the already-indexed one NOT duplicated, the
// un-indexed one synthesized), and the data copied.
//
// PG source is the deliberate choice: on a MySQL source every FK already carries
// an auto-created backing index, so the "synthesize a missing index" path can
// only be exercised from a source that permits un-indexed FK columns.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestMigrate_SkipForeignKeys_PostgresToMySQL(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// customer_id is explicitly indexed (case a); product_id is NOT (case b).
	const seedDDL = `
		CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE products  (id INT PRIMARY KEY, sku  TEXT NOT NULL);
		CREATE TABLE orders (
			id          INT  PRIMARY KEY,
			customer_id INT  NOT NULL,
			product_id  INT  NOT NULL,
			CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (id),
			CONSTRAINT orders_product_fk  FOREIGN KEY (product_id)  REFERENCES products  (id)
		);
		CREATE INDEX orders_customer_id_idx ON orders (customer_id);

		INSERT INTO customers VALUES (1, 'alice'), (2, 'bob');
		INSERT INTO products  VALUES (10, 'sku-a'), (20, 'sku-b');
		INSERT INTO orders    VALUES (100, 1, 10), (101, 2, 20);
	`
	applyPGDDL(t, pgSource, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:          pgEng,
		Target:          mysqlEng,
		SourceDSN:       pgSource,
		TargetDSN:       mysqlTarget,
		SkipForeignKeys: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	// ---- Verify the MySQL target schema ----
	sr, err := mysqlEng.OpenSchemaReader(ctx, mysqlTarget)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)

	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	orders := findTable(got, "orders")
	if orders == nil {
		t.Fatalf("orders missing on target; have %v", targetTableNames(got))
	}

	// No foreign keys anywhere on the target.
	for _, tbl := range got.Tables {
		if len(tbl.ForeignKeys) != 0 {
			t.Errorf("table %q has %d foreign keys on target; want 0 (--skip-foreign-keys)", tbl.Name, len(tbl.ForeignKeys))
		}
	}

	// Every FK's referencing column is covered by an index: customer_id
	// (already indexed at source — must NOT be duplicated) and product_id
	// (un-indexed at source — must be synthesized).
	if n := countIndexesLeading(orders, "customer_id"); n != 1 {
		t.Errorf("orders indexes leading with customer_id = %d; want exactly 1 (not duplicated): %v", n, indexNames(orders.Indexes))
	}
	if n := countIndexesLeading(orders, "product_id"); n < 1 {
		t.Errorf("orders has no index covering product_id; want a synthesized backing index: %v", indexNames(orders.Indexes))
	}

	// ---- Verify the data copied ----
	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()
	for tbl, want := range map[string]int{"customers": 2, "products": 2, "orders": 2} {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if n != want {
			t.Errorf("%s row count = %d; want %d", tbl, n, want)
		}
	}
}

// countIndexesLeading counts the table's indexes (primary key included) whose
// leading key column is col — the target-side check for "the FK's referencing
// column is indexed."
func countIndexesLeading(t *ir.Table, col string) int {
	n := 0
	if indexLeftPrefixCovers(t.PrimaryKey, []string{col}) {
		n++
	}
	for _, idx := range t.Indexes {
		if indexLeftPrefixCovers(idx, []string{col}) {
			n++
		}
	}
	return n
}
