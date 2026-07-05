//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug-138 real-target pin: `backup full` → `restore` of PG float4 /
// float8 IEEE specials. Before the f64s sentinel the backup REFUSED
// the whole table (`json: unsupported value: NaN`) — loud, but it made
// any database holding one NaN row un-backupable while `migrate`
// carried the identical values. The unit matrix in
// backup_chunk_fast_test.go covers both Go float widths and every
// shape; THIS pin is the Bug-74 real-target leg — the actual pgx
// driver decode → chunk codec → restore write → PG-native assertion
// chain, including the float8send bit fingerprint that proves a
// sluice restore matches what PG's own text round-trip produces.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestBackupRestore_PG_NonFiniteFloats(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE float_specials (
			id BIGINT PRIMARY KEY,
			f4 REAL,
			f8 DOUBLE PRECISION,
			n  NUMERIC
		);
		INSERT INTO float_specials (id, f4, f8, n) VALUES
			(1, 'NaN',       'NaN',       'NaN'), -- the Bug-138 refusal row
			(2, 'infinity',  'infinity',  1.5),
			(3, '-infinity', '-infinity', 2.5),
			(4, 0,           '-0',        3.5),   -- -0 sign-bit control
			(5, 6.25,        6.25,        4.5);   -- finite control
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Pre-fix this was the Bug-138 failure point.
	if err := (&backup.Backup{
		Source:    pgEng,
		SourceDSN: sourceDSN,
		Store:     store,
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run refused the non-finite corpus (the Bug-138 shape): %v", err)
	}
	if err := (&backup.Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var rows int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM float_specials`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 5 {
		t.Fatalf("restored rows = %d; want 5", rows)
	}

	// Per-family PG-native assertions. NaN compares via text rendering
	// (PG defines NaN = NaN as TRUE, so equality predicates can't
	// distinguish a coerced value); the float8send fingerprints pin
	// bit-level parity with PG's own 'NaN'/'infinity' parses — i.e. a
	// sluice restore is bit-identical to a pg_dump/pg_restore round
	// trip, including the IEEE-canonical quiet NaN and the -0 sign bit.
	for _, c := range []struct {
		name  string
		where string
	}{
		{"float4 NaN", `id = 1 AND f4::text = 'NaN' AND encode(float4send(f4),'hex') = '7fc00000'`},
		{"float8 NaN canonical bits", `id = 1 AND f8::text = 'NaN' AND encode(float8send(f8),'hex') = '7ff8000000000000'`},
		{"numeric NaN stays numeric-NaN", `id = 1 AND n::text = 'NaN'`},
		{"float4 +infinity", `id = 2 AND f4 = 'infinity'::real`},
		{"float8 +infinity", `id = 2 AND f8 = 'infinity'::double precision`},
		{"float4 -infinity", `id = 3 AND f4 = '-infinity'::real`},
		{"float8 -infinity", `id = 3 AND f8 = '-infinity'::double precision`},
		{"float8 -0 sign bit", `id = 4 AND encode(float8send(f8),'hex') = '8000000000000000'`},
		{"finite control", `id = 5 AND f4 = 6.25::real AND f8 = 6.25 AND n = 4.5`},
	} {
		t.Run(c.name, func(t *testing.T) {
			var found bool
			q := `SELECT EXISTS (SELECT 1 FROM float_specials WHERE ` + c.where + `)`
			if err := db.QueryRowContext(ctx, q).Scan(&found); err != nil {
				t.Fatalf("query: %v\nquery: %s", err, q)
			}
			if !found {
				t.Fatalf("special value did not survive backup→restore intact (query: %s)", q)
			}
		})
	}
}
