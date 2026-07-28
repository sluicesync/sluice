//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 84 — the PRIMARY KEY constraint-NAME carry, ground-truthed
// on a real Postgres target through the real Migrator.
//
// The unit matrix (internal/engines/postgres/ddl_emit_pk_constraint_name_
// test.go) pins what the emitter renders. This file pins the thing an
// operator actually experiences: after a PG -> PG migrate, is the target's
// key still called what the source called it, and does the named-constraint
// upsert form the application already ships still work?
//
// The `ON CONFLICT ON CONSTRAINT` assertion is the load-bearing one. It is
// the surface that broke — a migrated application issuing
// `INSERT ... ON CONFLICT ON CONSTRAINT orders_pk` got "constraint
// orders_pk for table orders does not exist" — and it is the assertion a
// catalog-name comparison alone would not prove.
//
// The MySQL-source half of the matrix lives in
// migrate_pk_constraint_name_cross_integration_test.go: the rule is about
// whether the SOURCE engine names primary keys, so both source shapes have
// to reach the same PG target for the matrix to mean anything.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pkConstraintNameSeedDDL covers PK shape x naming on the PG source side:
// explicitly-named / auto-named, crossed with single-column / composite.
// `pk_lineitems` is deliberately NOT `<table>_pkey`-shaped and leads with a
// convention prefix, which is the shape pgIndexName's table-prefix
// disambiguation would mangle into `lineitems_pk_lineitems`.
const pkConstraintNameSeedDDL = `
CREATE TABLE orders (
    id  BIGINT NOT NULL,
    v   TEXT,
    CONSTRAINT orders_pk PRIMARY KEY (id)
);

CREATE TABLE lineitems (
    order_id BIGINT NOT NULL,
    sku      TEXT   NOT NULL,
    qty      INT,
    CONSTRAINT pk_lineitems PRIMARY KEY (order_id, sku)
);

CREATE TABLE autopk (
    id BIGINT PRIMARY KEY,
    v  TEXT
);

CREATE TABLE autopk_composite (
    a BIGINT NOT NULL,
    b BIGINT NOT NULL,
    PRIMARY KEY (a, b)
);

INSERT INTO orders (id, v) VALUES (1, 'a');
INSERT INTO lineitems (order_id, sku, qty) VALUES (1, 'widget', 3);
INSERT INTO autopk (id, v) VALUES (1, 'a');
INSERT INTO autopk_composite (a, b) VALUES (1, 2);
`

// primaryKeyConstraintName reads the target's own pg_constraint entry for
// the table's PRIMARY KEY (contype 'p'). Reading the CATALOG rather than
// sluice's schema reader keeps the assertion independent of the reader
// under test.
func primaryKeyConstraintName(t *testing.T, dsn, table string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var name string
	err = db.QueryRowContext(ctx, `
		SELECT c.conname
		FROM   pg_constraint c
		JOIN   pg_class      t ON t.oid = c.conrelid
		JOIN   pg_namespace  n ON n.oid = t.relnamespace
		WHERE  n.nspname = 'public' AND t.relname = $1 AND c.contype = 'p'`, table).Scan(&name)
	if err != nil {
		t.Fatalf("read PK constraint name for %q: %v", table, err)
	}
	return name
}

// TestMigrate_PKConstraintName_PostgresToPostgres is the same-engine cell
// of the matrix: every PG-source PK name — chosen or PG-generated, single
// or composite — must appear verbatim on the target.
func TestMigrate_PKConstraintName_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, pkConstraintNameSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	for _, c := range []struct {
		table     string
		want      string
		rationale string
	}{
		{"orders", "orders_pk", "explicitly-named single-column PK"},
		{"lineitems", "pk_lineitems", "explicitly-named composite PK, with a name the table-prefix disambiguation would have mangled"},
		{"autopk", "autopk_pkey", "auto-named single-column PK: carrying PG's own default reproduces the name the target would have chosen"},
		{"autopk_composite", "autopk_composite_pkey", "auto-named composite PK"},
	} {
		if got := primaryKeyConstraintName(t, targetDSN, c.table); got != c.want {
			t.Errorf("target %s PK constraint = %q; want %q (%s)", c.table, got, c.want, c.rationale)
		}
	}

	// The operator-facing consequence, on the real target: the
	// named-constraint upsert form the migrated application already ships
	// must resolve. A catalog-name match alone would not prove this.
	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, stmt := range []string{
		`INSERT INTO public.orders (id, v) VALUES (1, 'upserted')
		   ON CONFLICT ON CONSTRAINT orders_pk DO UPDATE SET v = EXCLUDED.v`,
		`INSERT INTO public.lineitems (order_id, sku, qty) VALUES (1, 'widget', 9)
		   ON CONFLICT ON CONSTRAINT pk_lineitems DO UPDATE SET qty = EXCLUDED.qty`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Errorf("ON CONFLICT ON CONSTRAINT against the migrated target failed — "+
				"this is exactly the breakage item 84 closes: %v", err)
		}
	}

	var v string
	if err := db.QueryRowContext(ctx, `SELECT v FROM public.orders WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("read back upserted row: %v", err)
	}
	if v != "upserted" {
		t.Errorf("orders.v = %q; want upserted (the ON CONFLICT arbiter resolved but the DO UPDATE did not fire)", v)
	}
}
