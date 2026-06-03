//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the Bug 73/74 array-element CLASS (PG→PG COPY):
//
//   - Bug 74 (CRITICAL, silent regression): v0.69.3's Bug 70 fix made
//     `numeric[][]` silently FLATTEN to 1-D (exit 0, no WARN) — and the
//     same defect latently affected uuid/inet/cidr/decimal/time at
//     ≥2-D. Root cause (Phase-A, instrumented vs real PG COPY binary
//     path): pgx's ArrayCodec plans the element encode against the
//     TARGET column element OID using the leaf type pgtype.Array[*T]
//     reports; a leaf the OID's codec can't plan makes ArrayCodec
//     decline and pgx falls back through a flattening wrap. A bare
//     *string leaf only survives for text/varchar/char/macaddr.
//
//   - Bug 73 (MEDIUM, loud, pre-existing): timestamp[]/timestamptz[]/
//     date[] had no convertArray case → 57014 hard-fail.
//
// The fix selects, per element family, a leaf type the target element
// codec actually plans (pgtype.Text / Numeric / Date / Timestamp /
// Timestamptz / Time). This is the CLASS-closing pin: every element
// family at 1-D AND multi-dim (≥2-D) AND with a NULL element, src==dst
// ground-truthed via PG ::text rendering on the real target (which
// makes element-NULL and dimensionality observable in one compare).
// timetz[] is asserted to loud-refuse (no faithful binary array leaf).

package pipeline

import (
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// bug7374SeedDDL covers every element FAMILY at 1-D, multi-dim (2x2),
// and with a NULL element. Native (int/float/bool), string-shaped
// (text/uuid/inet/cidr/macaddr — all use the same *pgtype.Text leaf;
// bounded varchar(N)[]/char(N)[] share that leaf and are pinned at
// the value layer by TestConvertArrayPerFamilyLeafAndDims — they are
// omitted here only because the PG array-element DDL emitter doesn't
// carry the element length yet, an unrelated pre-existing emit gap
// outside this value-path batch), numeric/decimal, and temporal
// (date/timestamp/timestamptz/time). row 1 is the dense shape; row 2
// carries NULL elements at 1-D and inside the 2-D matrix.
const bug7374SeedDDL = `
	CREATE TABLE fam (
	  id    int PRIMARY KEY,
	  a_i   int[],          a_i2   int[][],
	  a_f   float8[],       a_f2   float8[][],
	  a_b   boolean[],      a_b2   boolean[][],
	  a_t   text[],         a_t2   text[][],
	  a_u   uuid[],         a_u2   uuid[][],
	  a_in  inet[],         a_in2  inet[][],
	  a_ci  cidr[],         a_ci2  cidr[][],
	  a_ma  macaddr[],      a_ma2  macaddr[][],
	  a_n   numeric[],      a_n2   numeric[][],
	  a_d   date[],         a_d2   date[][],
	  a_ts  timestamp[],    a_ts2  timestamp[][],
	  a_tz  timestamptz[],  a_tz2  timestamptz[][],
	  a_tm  time[],         a_tm2  time[][]
	);
	INSERT INTO fam VALUES
	 (1,
	  ARRAY[1,2,3], ARRAY[ARRAY[1,2],ARRAY[3,4]],
	  ARRAY[1.5,2.5], ARRAY[ARRAY[1.5,2.5],ARRAY[3.5,4.5]],
	  ARRAY[true,false], ARRAY[ARRAY[true,false],ARRAY[false,true]],
	  ARRAY['a','b'], ARRAY[ARRAY['a','b'],ARRAY['c','d']],
	  ARRAY['11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222']::uuid[],
	  ARRAY[ARRAY['11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222'],
	        ARRAY['33333333-3333-3333-3333-333333333333','44444444-4444-4444-4444-444444444444']]::uuid[][],
	  ARRAY['10.0.0.1','10.0.0.2']::inet[], ARRAY[ARRAY['10.0.0.1','10.0.0.2'],ARRAY['10.0.0.3','10.0.0.4']]::inet[][],
	  ARRAY['10.0.0.0/24','10.0.1.0/24']::cidr[], ARRAY[ARRAY['10.0.0.0/24','10.0.1.0/24'],ARRAY['10.0.2.0/24','10.0.3.0/24']]::cidr[][],
	  ARRAY['08:00:2b:01:02:03','08:00:2b:01:02:04']::macaddr[], ARRAY[ARRAY['08:00:2b:01:02:03','08:00:2b:01:02:04'],ARRAY['08:00:2b:01:02:05','08:00:2b:01:02:06']]::macaddr[][],
	  ARRAY[1.5,2.5]::numeric[], ARRAY[ARRAY[1.5,2.5],ARRAY[3.5,4.5]]::numeric[][],
	  ARRAY['2026-01-01','2026-02-01']::date[], ARRAY[ARRAY['2026-01-01','2026-02-01'],ARRAY['2026-03-01','2026-04-01']]::date[][],
	  ARRAY['2026-01-01 00:00:00','2026-02-01 12:34:56']::timestamp[], ARRAY[ARRAY['2026-01-01 00:00:00','2026-02-01 12:34:56'],ARRAY['2026-03-01 01:02:03','2026-04-01 23:59:59']]::timestamp[][],
	  ARRAY['2026-01-01 00:00:00+00','2026-02-01 12:34:56+00']::timestamptz[], ARRAY[ARRAY['2026-01-01 00:00:00+00','2026-02-01 12:34:56+00'],ARRAY['2026-03-01 01:02:03+00','2026-04-01 23:59:59+00']]::timestamptz[][],
	  ARRAY['01:02:03','23:59:59.123456']::time[], ARRAY[ARRAY['01:02:03','23:59:59.123456'],ARRAY['12:00:00','06:30:15']]::time[][]),
	 (2,
	  ARRAY[1,NULL,3], ARRAY[ARRAY[1,NULL],ARRAY[NULL,4]],
	  ARRAY[1.5,NULL], ARRAY[ARRAY[1.5,NULL],ARRAY[NULL,4.5]],
	  ARRAY[true,NULL], ARRAY[ARRAY[true,NULL],ARRAY[NULL,true]],
	  ARRAY['a',NULL], ARRAY[ARRAY['a',NULL],ARRAY[NULL,'d']],
	  ARRAY['11111111-1111-1111-1111-111111111111',NULL]::uuid[],
	  ARRAY[ARRAY['11111111-1111-1111-1111-111111111111',NULL],ARRAY[NULL,'44444444-4444-4444-4444-444444444444']]::uuid[][],
	  ARRAY['10.0.0.1',NULL]::inet[], ARRAY[ARRAY['10.0.0.1',NULL],ARRAY[NULL,'10.0.0.4']]::inet[][],
	  ARRAY['10.0.0.0/24',NULL]::cidr[], ARRAY[ARRAY['10.0.0.0/24',NULL],ARRAY[NULL,'10.0.3.0/24']]::cidr[][],
	  ARRAY['08:00:2b:01:02:03',NULL]::macaddr[], ARRAY[ARRAY['08:00:2b:01:02:03',NULL],ARRAY[NULL,'08:00:2b:01:02:06']]::macaddr[][],
	  ARRAY[1.5,NULL]::numeric[], ARRAY[ARRAY[1.5,NULL],ARRAY[NULL,4.5]]::numeric[][],
	  ARRAY['2026-01-01',NULL]::date[], ARRAY[ARRAY['2026-01-01',NULL],ARRAY[NULL,'2026-04-01']]::date[][],
	  ARRAY['2026-01-01 00:00:00',NULL]::timestamp[], ARRAY[ARRAY['2026-01-01 00:00:00',NULL],ARRAY[NULL,'2026-04-01 23:59:59']]::timestamp[][],
	  ARRAY['2026-01-01 00:00:00+00',NULL]::timestamptz[], ARRAY[ARRAY['2026-01-01 00:00:00+00',NULL],ARRAY[NULL,'2026-04-01 23:59:59+00']]::timestamptz[][],
	  ARRAY['01:02:03',NULL]::time[], ARRAY[ARRAY['01:02:03',NULL],ARRAY[NULL,'06:30:15']]::time[][]);
`

// TestMigrate_PostgresToPostgres_Bug7374ArrayFamilyClass is the
// class-closing pin: every element family at 1-D, multi-dim (2x2),
// and with NULL elements must round-trip src==dst EXACT on the PG
// target. The ::text oracle makes both NULL-element survival and
// dimensionality observable; a flattened multi-dim array (Bug 74)
// renders as a 1-D `{...}` and fails the compare.
func TestMigrate_PostgresToPostgres_Bug7374ArrayFamilyClass(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, bug7374SeedDDL)

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
		t.Fatalf("Migrator.Run (PG→PG array family class must migrate faithfully): %v", err)
	}

	cols := []string{
		"a_i", "a_i2", "a_f", "a_f2", "a_b", "a_b2",
		"a_t", "a_t2",
		"a_u", "a_u2", "a_in", "a_in2", "a_ci", "a_ci2",
		"a_ma", "a_ma2", "a_n", "a_n2", "a_d", "a_d2",
		"a_ts", "a_ts2", "a_tz", "a_tz2", "a_tm", "a_tm2",
	}

	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	sel := "SELECT id"
	for _, c := range cols {
		sel += ", " + c + "::text"
	}
	sel += " FROM fam ORDER BY id"

	readAll := func(db *sql.DB) map[int][]sql.NullString {
		rows, err := db.Query(sel)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer func() { _ = rows.Close() }()
		out := map[int][]sql.NullString{}
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			dest := make([]any, len(cols)+1)
			var id int
			dest[0] = &id
			for i := range vals {
				dest[i+1] = &vals[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out[id] = vals
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		return out
	}

	src := readAll(srcDB)
	dst := readAll(dstDB)
	if len(dst) != len(src) || len(src) != 2 {
		t.Fatalf("row count: src=%d dst=%d (want 2 each)", len(src), len(dst))
	}
	for id, sv := range src {
		dv, ok := dst[id]
		if !ok {
			t.Fatalf("row id=%d missing on target", id)
		}
		for i, c := range cols {
			if sv[i] != dv[i] {
				t.Errorf("row id=%d col %s: src=%q dst=%q (FLATTEN/LOSS — multi-dim or value not faithful)",
					id, c, sv[i].String, dv[i].String)
			}
		}
	}
}

// TestMigrate_PostgresToPostgres_Bug73TimetzArrayLoudRefuse pins the
// loud-failure boundary: a timetz[] column has no faithful binary
// array leaf, so the migration must REFUSE loudly (no silent flatten
// / corruption). A refused migration beats a silently corrupted one.
func TestMigrate_PostgresToPostgres_Bug73TimetzArrayLoudRefuse(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE tza (id int PRIMARY KEY, a timetz[]);
		INSERT INTO tza VALUES (1, ARRAY['01:02:03+05','04:05:06-08']::timetz[]);
	`)

	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN}
	err := mig.Run(ctx2min(t))
	if err == nil {
		t.Fatal("expected loud refusal for timetz[] (no silent flatten); got nil error")
	}
	if !strings.Contains(err.Error(), "timetz") {
		t.Errorf("error should name timetz; got: %v", err)
	}
}
