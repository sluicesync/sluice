//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine TIMESTAMP / DATETIME precision audit. Bug 19 (v0.8.0)
// closed the silent-corruption hole on the TZ axis — non-UTC hosts no
// longer drift TIMESTAMP values during MySQL→PG CDC. The precision
// axis is the next door to check: do `TIMESTAMP(0)` /
// `TIMESTAMP(3)` / `TIMESTAMP(6)` round-trip cleanly through the
// IR's `Precision` field, across both bulk-copy (cold-start) and CDC
// paths, in both directions?
//
// The IR's `Timestamp{Precision: int}` and `DateTime{Precision: int}`
// have always carried the precision; both engine schema readers
// populate it from `datetime_precision`; both DDL emitters honour
// it. What's missing is an integration test that exercises the
// end-to-end behaviour across varied precisions. This file is that
// test.
//
// Outcome at v0.9.0: cold-start preserves precision in both
// directions across all three precisions tested. The CDC path is
// covered by the streamer-shaped tests in streamer_integration_test.go;
// this file is the cold-start audit.

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

// TestMigrate_MySQLToPostgres_TimestampPrecision exercises the
// MySQL→PG cold-start path across DATETIME(0/3/6) and
// TIMESTAMP(0/3/6) — three precision points each, two type families.
// Asserts that the round-tripped value reads back as the same UTC
// instant (within the column's declared precision; comparisons are
// to the second on Precision=0, to the millisecond on 3, to the
// microsecond on 6).
func TestMigrate_MySQLToPostgres_TimestampPrecision(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// Seed values chosen so every precision tier surfaces a distinct
	// value: 12:34:56.123456 has microseconds; truncating to
	// milliseconds gives 12:34:56.123; to seconds gives 12:34:56.
	// If the round-trip silently truncates more aggressively than the
	// column's declared precision, the assertion catches it.
	const seedDDL = `
		CREATE TABLE temporal_precision (
			id     BIGINT       NOT NULL PRIMARY KEY,
			dt_0   DATETIME(0)  NOT NULL,
			dt_3   DATETIME(3)  NOT NULL,
			dt_6   DATETIME(6)  NOT NULL,
			ts_0   TIMESTAMP(0) NOT NULL,
			ts_3   TIMESTAMP(3) NOT NULL,
			ts_6   TIMESTAMP(6) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO temporal_precision (id, dt_0, dt_3, dt_6, ts_0, ts_3, ts_6) VALUES
			(1,
				'2026-05-06 12:34:56',
				'2026-05-06 12:34:56.123',
				'2026-05-06 12:34:56.123456',
				'2026-05-06 12:34:56',
				'2026-05-06 12:34:56.123',
				'2026-05-06 12:34:56.123456');
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
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgt, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	// Read each column back; expect a time.Time per column (pgx
	// scans timestamps into time.Time directly). Precision-tier
	// values defined alongside the seed.
	var dt0, dt3, dt6, ts0, ts3, ts6 time.Time
	const q = `SELECT dt_0, dt_3, dt_6, ts_0, ts_3, ts_6 FROM temporal_precision WHERE id = 1`
	if err := tgt.QueryRowContext(ctx, q).Scan(&dt0, &dt3, &dt6, &ts0, &ts3, &ts6); err != nil {
		t.Fatalf("scan target row: %v", err)
	}

	cases := []struct {
		name    string
		got     time.Time
		want    time.Time // value at the column's declared precision
		precFmt string    // truncate-to-precision format for the comparison
	}{
		{"DATETIME(0)", dt0, time.Date(2026, 5, 6, 12, 34, 56, 0, time.UTC), "2006-01-02 15:04:05"},
		{"DATETIME(3)", dt3, time.Date(2026, 5, 6, 12, 34, 56, 123000000, time.UTC), "2006-01-02 15:04:05.000"},
		{"DATETIME(6)", dt6, time.Date(2026, 5, 6, 12, 34, 56, 123456000, time.UTC), "2006-01-02 15:04:05.000000"},
		{"TIMESTAMP(0)", ts0, time.Date(2026, 5, 6, 12, 34, 56, 0, time.UTC), "2006-01-02 15:04:05"},
		{"TIMESTAMP(3)", ts3, time.Date(2026, 5, 6, 12, 34, 56, 123000000, time.UTC), "2006-01-02 15:04:05.000"},
		{"TIMESTAMP(6)", ts6, time.Date(2026, 5, 6, 12, 34, 56, 123456000, time.UTC), "2006-01-02 15:04:05.000000"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotTrunc := c.got.UTC().Format(c.precFmt)
			wantTrunc := c.want.UTC().Format(c.precFmt)
			if gotTrunc != wantTrunc {
				t.Errorf("%s: got %s; want %s (raw got=%v want=%v)", c.name, gotTrunc, wantTrunc, c.got, c.want)
			}
		})
	}
}

// TestMigrate_PostgresToMySQL_TimestampPrecision exercises the
// reverse direction: PG TIMESTAMP / TIMESTAMPTZ at varied precisions
// landing on MySQL DATETIME / TIMESTAMP. Same shape as the forward
// test — three precision points, both type families.
//
// PG's `TIMESTAMP(N)` (without TZ) maps to MySQL `DATETIME(N)` per
// the engines/postgres translateType logic; PG's `TIMESTAMPTZ(N)`
// maps to MySQL `TIMESTAMP(N)` (which is TZ-aware via session
// time_zone). v0.8.1's Bug 19 fix pinned all MySQL connections to
// `time_zone='+00:00'`, so TIMESTAMPTZ values land as their UTC
// instant on the MySQL side.
func TestMigrate_PostgresToMySQL_TimestampPrecision(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	const seedDDL = `
		CREATE TABLE temporal_precision (
			id     BIGINT      NOT NULL PRIMARY KEY,
			ts_0   TIMESTAMP(0)  NOT NULL,
			ts_3   TIMESTAMP(3)  NOT NULL,
			ts_6   TIMESTAMP(6)  NOT NULL,
			tstz_0 TIMESTAMPTZ(0) NOT NULL,
			tstz_3 TIMESTAMPTZ(3) NOT NULL,
			tstz_6 TIMESTAMPTZ(6) NOT NULL
		);

		INSERT INTO temporal_precision (id, ts_0, ts_3, ts_6, tstz_0, tstz_3, tstz_6) VALUES
			(1,
				'2026-05-06 12:34:56',
				'2026-05-06 12:34:56.123',
				'2026-05-06 12:34:56.123456',
				'2026-05-06 12:34:56+00',
				'2026-05-06 12:34:56.123+00',
				'2026-05-06 12:34:56.123456+00');
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
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgt, err := sql.Open("mysql", mysqlTarget+"&parseTime=true")
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	var ts0, ts3, ts6, tstz0, tstz3, tstz6 time.Time
	const q = `SELECT ts_0, ts_3, ts_6, tstz_0, tstz_3, tstz_6 FROM temporal_precision WHERE id = 1`
	if err := tgt.QueryRowContext(ctx, q).Scan(&ts0, &ts3, &ts6, &tstz0, &tstz3, &tstz6); err != nil {
		t.Fatalf("scan target row: %v", err)
	}

	cases := []struct {
		name    string
		got     time.Time
		want    time.Time
		precFmt string
	}{
		// PG TIMESTAMP (no TZ) → MySQL DATETIME — wall-clock, not a
		// UTC instant. Same wall-clock components on both sides.
		{"TIMESTAMP(0) → DATETIME(0)", ts0, time.Date(2026, 5, 6, 12, 34, 56, 0, time.UTC), "2006-01-02 15:04:05"},
		{"TIMESTAMP(3) → DATETIME(3)", ts3, time.Date(2026, 5, 6, 12, 34, 56, 123000000, time.UTC), "2006-01-02 15:04:05.000"},
		{"TIMESTAMP(6) → DATETIME(6)", ts6, time.Date(2026, 5, 6, 12, 34, 56, 123456000, time.UTC), "2006-01-02 15:04:05.000000"},
		// PG TIMESTAMPTZ → MySQL TIMESTAMP — UTC instant. v0.8.1 fix
		// (Bug 19) ensures the MySQL session is in UTC so the wire
		// format and the UTC instant align.
		{"TIMESTAMPTZ(0) → TIMESTAMP(0)", tstz0, time.Date(2026, 5, 6, 12, 34, 56, 0, time.UTC), "2006-01-02 15:04:05"},
		{"TIMESTAMPTZ(3) → TIMESTAMP(3)", tstz3, time.Date(2026, 5, 6, 12, 34, 56, 123000000, time.UTC), "2006-01-02 15:04:05.000"},
		{"TIMESTAMPTZ(6) → TIMESTAMP(6)", tstz6, time.Date(2026, 5, 6, 12, 34, 56, 123456000, time.UTC), "2006-01-02 15:04:05.000000"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotTrunc := c.got.UTC().Format(c.precFmt)
			wantTrunc := c.want.UTC().Format(c.precFmt)
			if gotTrunc != wantTrunc {
				t.Errorf("%s: got %s; want %s (raw got=%v want=%v)", c.name, gotTrunc, wantTrunc, c.got, c.want)
			}
		})
	}

	// Also verify the target column types match what we expect — the
	// schema-emit path picks DATETIME for PG TIMESTAMP and TIMESTAMP
	// for PG TIMESTAMPTZ. If a future change rewires the mapping the
	// data-shape test would still pass on equivalent values; this
	// schema-shape check would catch the rewire explicitly.
	wantColTypes := map[string]string{
		"ts_0":   "datetime",
		"ts_3":   "datetime",
		"ts_6":   "datetime",
		"tstz_0": "timestamp",
		"tstz_3": "timestamp",
		"tstz_6": "timestamp",
	}
	for col, want := range wantColTypes {
		var got string
		const colQ = `SELECT data_type FROM information_schema.columns
		              WHERE table_schema = DATABASE()
		                AND table_name   = 'temporal_precision'
		                AND column_name  = ?`
		if err := tgt.QueryRowContext(ctx, colQ, col).Scan(&got); err != nil {
			t.Fatalf("read column type for %q: %v", col, err)
		}
		if got != want {
			t.Errorf("column %q data_type = %q; want %q", col, got, want)
		}
	}
}
