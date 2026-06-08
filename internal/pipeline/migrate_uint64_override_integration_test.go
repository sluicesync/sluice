//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_BigintUnsignedOverflow_DecimalOverride pins the documented
// recovery for a MySQL `BIGINT UNSIGNED` column holding values above
// 2^63-1 (the uint64-no-migration-path finding, 2026-06-08). The default
// `bigint` mapping can't represent them (loud refusal at COPY encode), and
// the unsigned-bigint notice directs operators to
// `--type-override TABLE.COL=decimal(20,0)`. Before the fix
// that path was broken end to end: the notice named a non-existent
// `=numeric` token, and even with `=decimal` the MySQL reader couldn't
// decode a uint64 into a Decimal/text target. This asserts the documented
// override now lands EVERY value EXACTLY on a PG numeric(20,0) target,
// including 2^64-1 (20 digits) which only fits with precision 20.
func TestMigrate_BigintUnsignedOverflow_DecimalOverride(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE ubig (
			id INT PRIMARY KEY,
			u  BIGINT UNSIGNED NOT NULL
		);
		INSERT INTO ubig VALUES
			(1, 0),
			(2, 9223372036854775807),  -- int64 max
			(3, 9223372036854775808),  -- int64 max + 1 (above the signed ceiling)
			(4, 18446744073709551615); -- uint64 max (2^64-1), 20 digits
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
		Mappings: []config.Mapping{
			{Table: "ubig", Column: "u", TargetType: "decimal", TargetTypeOptions: map[string]any{"precision": 20, "scale": 0}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (bigint-unsigned decimal override): %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	want := map[int]string{
		1: "0",
		2: "9223372036854775807",
		3: "9223372036854775808",
		4: "18446744073709551615",
	}
	rows, err := db.QueryContext(ctx, "SELECT id, u::text FROM ubig ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[int]string{}
	for rows.Next() {
		var id int
		var u string
		if err := rows.Scan(&id, &u); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = u
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("target ubig row count = %d; want %d", len(got), len(want))
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("ubig id=%d: target u = %q; want %q (exact unsigned-64 value via decimal(20,0) override)", id, got[id], w)
		}
	}
}
