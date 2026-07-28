//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 84 — the OTHER half of the source-engine matrix, and the
// half that decides the design: a MySQL source migrating to the same
// Postgres target must still land an inline, PG-auto-named primary key.
//
// A MySQL PK index is literally named `PRIMARY` — a fixed sentinel every
// InnoDB table shares, not a name any operator chose. `quoteIdent` makes
// `CONSTRAINT "PRIMARY" PRIMARY KEY (...)` perfectly legal Postgres, so
// the naive carry does not fail loudly: it silently renames every migrated
// table's key to `PRIMARY`, colliding conceptually with nothing and
// meaning nothing. That is why this cell exists and why the same-engine
// cell alone would not have caught it.
//
// The rule is therefore about the SOURCE engine, not the target: the exact
// same PG SchemaWriter serves both files.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pkConstraintNameMySQLSeedDDL mirrors the PG seed's shape axis (single
// vs composite) on a source engine that cannot name a primary key at all.
// The `CONSTRAINT mysql_named_pk` spelling on the composite table is
// deliberate: MySQL PARSES it and then discards the name — SHOW CREATE
// TABLE reports a bare `PRIMARY KEY`, and information_schema reports the
// index as `PRIMARY`. If a future reader ever started believing that
// spelling, this cell is where it would surface.
const pkConstraintNameMySQLSeedDDL = `
CREATE TABLE orders (
    id BIGINT NOT NULL,
    v  VARCHAR(64),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE lineitems (
    order_id BIGINT      NOT NULL,
    sku      VARCHAR(64) NOT NULL,
    qty      INT,
    CONSTRAINT mysql_named_pk PRIMARY KEY (order_id, sku)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO orders (id, v) VALUES (1, 'a');
INSERT INTO lineitems (order_id, sku, qty) VALUES (1, 'widget', 3);
`

// TestMigrate_PKConstraintName_MySQLToPostgres_StaysInline pins that no
// MySQL-source PK name reaches the Postgres target.
func TestMigrate_PKConstraintName_MySQLToPostgres_StaysInline(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, pkConstraintNameMySQLSeedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	for _, c := range []struct {
		table     string
		want      string
		rationale string
	}{
		{"orders", "orders_pkey", "single-column MySQL PK: PG names the target's key, because the source had no name to preserve"},
		{"lineitems", "lineitems_pkey", "composite MySQL PK whose DDL even SPELLED a constraint name: MySQL discarded it, so sluice must not resurrect one"},
	} {
		if got := primaryKeyConstraintName(t, pgTarget, c.table); got != c.want {
			t.Errorf("target %s PK constraint = %q; want %q (%s)", c.table, got, c.want, c.rationale)
		}
	}

	// The direct negative: nothing anywhere in the target may be called
	// `PRIMARY`. A naive `emit CONSTRAINT <PrimaryKey.Name>` passes every
	// assertion above's sibling in the PG file and fails only here.
	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM   pg_constraint c
		JOIN   pg_namespace  n ON n.oid = c.connamespace
		WHERE  n.nspname = 'public' AND c.conname = 'PRIMARY'`).Scan(&n); err != nil {
		t.Fatalf("count PRIMARY-named constraints: %v", err)
	}
	if n != 0 {
		t.Errorf("target carries %d constraint(s) named \"PRIMARY\" — the MySQL sentinel index name leaked into a "+
			"Postgres constraint identifier no operator chose", n)
	}
}
