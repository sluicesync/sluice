//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 71 (PG `timetz` / `time with time zone` is
// accepted at schema-read but mis-mapped to plain `time` (OID 1083),
// then hard-fails the PG→PG COPY writer):
//
//   - Pre-fix: translateType collapsed `time with time zone` to
//     ir.Time{} (identical to plain `time`). The PG writer emitted
//     `time` (OID 1083) and pgx's COPY was handed a tz-bearing value it
//     could not encode into the tz-less codec → SQLSTATE 57014, exit 1.
//
//   - Post-fix: ir.Time carries WithTimeZone (mirroring
//     ir.Timestamp.WithTimeZone). The PG reader maps timetz →
//     ir.Time{WithTimeZone:true}; the PG writer emits `TIME WITH TIME
//     ZONE` and registers a per-conn binary codec (pgx ships none for
//     timetz). PG→PG round-trips faithfully; PG→MySQL drops the zone
//     (documented policy, same as timestamptz→MySQL).
//
// This is the verbatim BUG-CATALOG section 71 minimal repro: a `time`
// column (regression guard — must still work) and a `timetz` column.

package pipeline

import (
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const bug71SeedDDL = `
	CREATE TABLE t_times (
	  id  int PRIMARY KEY,
	  v   time,        -- regression guard: plain time must still work
	  vz  timetz       -- the Bug 71 column
	);
	INSERT INTO t_times (id, v, vz) VALUES
	  (1, '13:45:30',        '13:45:30+05'),
	  (2, '00:00:00',        '08:00:00-07:30'),
	  (3, '23:59:59.123456', '23:59:59.123456+00');
`

// TestMigrate_PostgresToPostgres_Bug71Timetz pins the PG→PG closure:
// pre-fix the COPY writer aborted with SQLSTATE 57014 on the timetz
// column. Post-fix migrate exits 0, the target column is `timetz`
// (data_type "time with time zone"), and every value — including the
// zone offset — is ground-truthed EXACT. Plain `time` is unchanged.
func TestMigrate_PostgresToPostgres_Bug71Timetz(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, bug71SeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		// Pre-fix this failed with SQLSTATE 57014 (cannot encode timetz
		// into OID 1083).
		t.Fatalf("Migrator.Run (PG→PG timetz must migrate, no 57014): %v", err)
	}

	pgDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pgDB.Close() }()

	// The vz column must be timetz on the target, not plain time.
	var dataType string
	if err := pgDB.QueryRow(`
		SELECT data_type FROM information_schema.columns
		WHERE table_name = 't_times' AND column_name = 'vz'`).Scan(&dataType); err != nil {
		t.Fatalf("read t_times.vz data_type: %v", err)
	}
	if dataType != "time with time zone" {
		t.Errorf("t_times.vz data_type = %q; want \"time with time zone\" (must not collapse to plain time)", dataType)
	}

	type row struct {
		id int
		v  string
		vz string
	}
	// timetz ::text renders the offset; PG normalises whole-hour zones
	// to ±HH and fractional ones to ±HH:MM. The load-bearing assertion
	// is the zone offset survives.
	want := []row{
		{1, "13:45:30", "13:45:30+05"},
		{2, "00:00:00", "08:00:00-07:30"},
		{3, "23:59:59.123456", "23:59:59.123456+00"},
	}
	rows, err := pgDB.Query(`SELECT id, v::text, vz::text FROM t_times ORDER BY id`)
	if err != nil {
		t.Fatalf("query pg target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.v, &r.vz); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.v != w.v {
			t.Errorf("row[%d] v: got %q; want %q (plain time regression guard)", i, g.v, w.v)
		}
		if g.vz != w.vz {
			t.Errorf("row[%d] vz: got %q; want %q (timetz zone offset must survive)", i, g.vz, w.vz)
		}
	}
}

// TestMigrate_PostgresToMySQL_Bug71TimetzZoneFlatten pins the
// cross-engine policy: MySQL has no tz-aware time type, so timetz is
// emitted as MySQL TIME with the zone dropped (the same documented
// policy as timestamptz→MySQL — flatten, not refuse). The migration
// must exit 0 and the time-of-day portion must be preserved.
func TestMigrate_PostgresToMySQL_Bug71TimetzZoneFlatten(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bug71SeedDDL)

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
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (PG→MySQL timetz zone-flatten must migrate): %v", err)
	}

	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	var colType string
	if err := mysqlDB.QueryRow(`
		SELECT DATA_TYPE FROM information_schema.columns
		WHERE table_name = 't_times' AND column_name = 'vz'`).Scan(&colType); err != nil {
		t.Fatalf("read t_times.vz DATA_TYPE: %v", err)
	}
	if !strings.EqualFold(colType, "time") {
		t.Errorf("t_times.vz DATA_TYPE = %q; want time (zone-flatten policy)", colType)
	}

	// The time-of-day portion must be preserved (the zone is dropped per
	// the documented cross-engine policy — MySQL has no tz-aware time).
	type row struct {
		id int
		vz string
	}
	// timetz carries Precision 6 from PG; the MySQL column is TIME(6),
	// which renders 6 fractional digits (trailing zeros included). The
	// zone offset is dropped (the documented cross-engine policy); the
	// time-of-day is exact.
	want := []row{
		{1, "13:45:30.000000"},
		{2, "08:00:00.000000"},
		{3, "23:59:59.123456"},
	}
	rows, err := mysqlDB.Query(`SELECT id, vz FROM t_times ORDER BY id`)
	if err != nil {
		t.Fatalf("query mysql target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.vz); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.vz != w.vz {
			t.Errorf("row[%d] vz: got %q; want %q (time-of-day must survive zone-flatten)", i, g.vz, w.vz)
		}
	}
}
