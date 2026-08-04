//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Bug 225 end-to-end pin (roadmap item 117).
//
// A Postgres `macaddr8` column could not be migrated OR synced to a Postgres
// target on ANY released binary: the reader mapped both MAC widths onto an
// empty ir.Macaddr, so the emitter rendered MACADDR unconditionally, the
// target column held six bytes, and the COPY of the first 8-byte value failed
// — rc=1, zero rows. The `macaddr8[]` array sibling failed the same way.
//
// This is a whole-run pin rather than an emitter unit test on purpose: the
// unit tests prove the emitter renders MACADDR8 and the reader reports the
// width, and the thing that was actually broken is the composition of the two
// across a real COPY. The independent expected value here is the SOURCE
// database — every assertion compares the target against the source's own
// `::text` rendering, read separately, not against anything the migration
// produced.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToPostgres_Macaddr8 pins Bug 225 closed for both MAC
// widths, scalar and array, and — the row that was never reachable before —
// for the EUI-48 value stored in a macaddr8 column, whose delivered form is
// the widened one.
func TestMigrate_PostgresToPostgres_Macaddr8(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE devices (
			id        BIGINT PRIMARY KEY,
			mac6      MACADDR   NOT NULL,
			mac8      MACADDR8  NOT NULL,
			mac6_arr  MACADDR[],
			mac8_arr  MACADDR8[],
			mac8_null MACADDR8
		);

		INSERT INTO devices (id, mac6, mac8, mac6_arr, mac8_arr, mac8_null) VALUES
			(1, '08:00:2b:01:02:03', '08:00:2b:ff:fe:01:02:03',
			    '{08:00:2b:01:02:03,aa:bb:cc:dd:ee:ff}',
			    '{08:00:2b:ff:fe:01:02:03,01:02:03:04:05:06:07:08}', NULL),
			-- The EUI-48 literal into a macaddr8 column: PG widens it on
			-- input, so this row's mac8 is delivered as the FF:FE form. It is
			-- here because it is the shape the C1 residual was about.
			(2, 'aa:bb:cc:dd:ee:ff', '08:00:2b:01:02:03',
			    '{ff:ff:ff:ff:ff:ff}', '{ff:ff:ff:ff:ff:ff:ff:ff}',
			    '02:00:5e:10:00:00:00:01');
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v\n\nThis is Bug 225: a macaddr8 column narrowed to MACADDR on the "+
			"target, so the COPY of an 8-byte value cannot succeed and the whole migration fails.", err)
	}

	source, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("sql.Open source: %v", err)
	}
	defer func() { _ = source.Close() }()
	target, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open target: %v", err)
	}
	defer func() { _ = target.Close() }()

	// The TYPE, read from the target's own catalog. A migration that "worked"
	// by mapping macaddr8 to text would pass a value comparison and still be
	// the silent type loss this project refuses.
	const typeQ = `
		SELECT t.typname FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_type  t ON a.atttypid = t.oid
		WHERE c.relname = 'devices' AND a.attname = $1 AND a.attnum > 0
	`
	for col, want := range map[string]string{
		"mac6":      "macaddr",
		"mac8":      "macaddr8",
		"mac6_arr":  "_macaddr",
		"mac8_arr":  "_macaddr8",
		"mac8_null": "macaddr8",
	} {
		var got string
		if err := target.QueryRowContext(ctx, typeQ, col).Scan(&got); err != nil {
			t.Fatalf("read target type of devices.%s: %v", col, err)
		}
		if got != want {
			t.Errorf("target devices.%s has type %q, want %q — the width was lost between the reader "+
				"and the emitter", col, got, want)
		}
	}

	// The VALUES, against the SOURCE's own rendering — the independent
	// expected value, read from the database the migration copied FROM rather
	// than from anything the migration wrote.
	const valQ = `
		SELECT id,
		       mac6::text, mac8::text,
		       COALESCE(mac6_arr::text, ''), COALESCE(mac8_arr::text, ''),
		       COALESCE(mac8_null::text, '<null>')
		FROM devices ORDER BY id
	`
	type row struct {
		id                              int64
		mac6, mac8, arr6, arr8, nullCol string
	}
	read := func(db *sql.DB, which string) []row {
		t.Helper()
		rows, err := db.QueryContext(ctx, valQ)
		if err != nil {
			t.Fatalf("query %s: %v", which, err)
		}
		defer func() { _ = rows.Close() }()
		var out []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.mac6, &r.mac8, &r.arr6, &r.arr8, &r.nullCol); err != nil {
				t.Fatalf("scan %s: %v", which, err)
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate %s: %v", which, err)
		}
		return out
	}

	src, dst := read(source, "source"), read(target, "target")
	if len(src) != 2 {
		t.Fatalf("source has %d rows; the fixture seeds 2 — the test is not measuring what it claims", len(src))
	}
	if len(dst) != len(src) {
		t.Fatalf("target has %d rows, source has %d", len(dst), len(src))
	}
	for i := range src {
		if src[i] != dst[i] {
			t.Errorf("row %d diverged:\n  source %+v\n  target %+v", src[i].id, src[i], dst[i])
		}
	}

	// And the widening is visible in the copied data, not merely assumed: row
	// 2's mac8 was WRITTEN as a 6-byte literal and must read back as 8 on both
	// sides. This is the row the --where literal gate now refuses a 6-byte
	// spelling for.
	if got := dst[1].mac8; got != "08:00:2b:ff:fe:01:02:03" {
		t.Errorf("row 2's macaddr8 (seeded from the EUI-48 spelling) reads %q on the target; "+
			"want the widened form %q", got, "08:00:2b:ff:fe:01:02:03")
	}
}
