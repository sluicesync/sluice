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

	_ "github.com/go-sql-driver/mysql"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_TimestamptzOutOfRange_DatetimeOverride pins the data-preserving
// escape hatch for the timestamptz-range finding (2026-06-08). PG TIMESTAMPTZ
// defaults to MySQL TIMESTAMP (1970–2038); values outside that window
// (historical / far-future) can't be represented and the default path refuses
// loudly. Before the fix there was NO override to a type that fits — only the
// silent-loss `--mysql-sql-mode=”` path. `--type-override COL=datetime` now
// maps to MySQL DATETIME(6) (range 1000–9999); this asserts a pre-1900 and a
// post-2038 timestamptz migrate PG→MySQL EXACTLY via that override (the UTC
// instant is preserved; DATETIME stores the normalised value).
func TestMigrate_TimestamptzOutOfRange_DatetimeOverride(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	const seedDDL = `
		CREATE TABLE ts (id INT PRIMARY KEY, label TEXT, t TIMESTAMPTZ);
		INSERT INTO ts VALUES
			(1, 'epoch',     '1970-01-01 00:00:00+00'),
			(2, 'in-range',  '2026-06-08 12:34:56+00'),
			(3, 'pre-1900',  '1899-07-15 08:30:00+00'),
			(4, 'post-2038', '2040-01-01 00:00:00+00');
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
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
		Mappings: []config.Mapping{
			{Table: "ts", Column: "t", TargetType: "datetime"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (timestamptz datetime override): %v", err)
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The target column must be DATETIME (not TIMESTAMP) and carry every
	// UTC instant exactly — including the rows TIMESTAMP could not hold.
	var colType string
	if err := db.QueryRowContext(ctx,
		"SELECT DATA_TYPE FROM information_schema.columns WHERE table_name='ts' AND column_name='t'").
		Scan(&colType); err != nil {
		t.Fatalf("read target column type: %v", err)
	}
	if colType != "datetime" {
		t.Errorf("target ts.t data_type = %q; want datetime (the override)", colType)
	}

	want := map[int]string{
		1: "1970-01-01 00:00:00",
		2: "2026-06-08 12:34:56",
		3: "1899-07-15 08:30:00", // pre-1900 — impossible in MySQL TIMESTAMP
		4: "2040-01-01 00:00:00", // post-2038 — impossible in MySQL TIMESTAMP
	}
	rows, err := db.QueryContext(ctx, "SELECT id, DATE_FORMAT(t, '%Y-%m-%d %H:%i:%s') FROM ts ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[int]string{}
	for rows.Next() {
		var id int
		var ts string
		if err := rows.Scan(&id, &ts); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = ts
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("ts id=%d: target t = %q; want %q (exact UTC instant via datetime override)", id, got[id], w)
		}
	}
}
