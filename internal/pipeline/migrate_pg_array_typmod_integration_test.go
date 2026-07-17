//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 195 pin — the PG schema reader dropped the array-element type
// modifier for every non-temporal parameterized family:
// varchar[]/varchar(n)[]/char(n)[] FALSE-REFUSED at create-tables
// (element read as VARCHAR(0)/CHAR(0), even PG→PG) and numeric(p,s)[]
// SILENTLY landed as bare numeric[] (values copied; the precision/scale
// constraint vanished from the target — schema-fidelity loss). The
// original temporal-only typmod thread (TRIAGE #3) was the classic
// pin-the-representative miss; this file pins the CLASS:
//
//   every parameterized element family — varchar(n), char(n),
//   numeric(p,s), plus the bare varchar[]/numeric[] forms and the
//   temporal re-verify — × {1-D, 2-D, NULL-element} ×
//   {PG→PG round-trip exact, PG→MySQL mapping correct}
//
// with format_type ground truth on the real target (the exact probe the
// bug was found with), value equality via ::text, and array_dims for
// the multi-dim shape. bit(n)[]/varbit(n)[] are deliberately NOT in the
// supported matrix — they refuse loudly as an unsupported element
// family (pinned in the unit suite), never silently mis-typed.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// arrayTypmodSeedDDL carries one column per family under test and one
// row per shape: 1-D, 2-D, NULL-element, all-NULL.
const arrayTypmodSeedDDL = `
	CREATE TABLE arrmods (
		id      INT PRIMARY KEY,
		v_bare  varchar[],
		v_len   varchar(20)[],
		c_len   char(5)[],
		n_bare  numeric[],
		n_ps    numeric(10,2)[],
		tz3     timestamptz(3)[]
	);
	INSERT INTO arrmods VALUES
		(1, '{a,b}', '{"up to twenty chars","x"}', '{abc,de}', '{1.5,2.25}', '{12345678.99,0.01}',
			'{2026-01-01 00:00:00.123+00,2026-06-30 23:59:59.999+00}'),
		(2, '{{a,b},{c,d}}', '{{one,two},{three,four}}', '{{ab,cd},{ef,gh}}',
			'{{1.5,2.5},{3.5,4.5}}', '{{10.25,20.50},{30.75,40.00}}',
			'{{2026-01-01 00:00:00+00,2026-01-02 00:00:00+00},{2026-01-03 00:00:00+00,2026-01-04 00:00:00+00}}'),
		(3, '{a,NULL,c}', '{x,NULL,z}', '{ab,NULL}', '{1.5,NULL}', '{9.99,NULL}',
			'{2026-01-01 00:00:00+00,NULL}'),
		(4, NULL, NULL, NULL, NULL, NULL, NULL);
`

// arrayTypmodWantFormatTypes is the format_type ground truth expected on
// a PG TARGET after a PG→PG migrate. Bare varchar[] lands as text[] —
// the IR has no "varchar with no length", so the unbounded form
// deliberately collapses to the value-equivalent unbounded type
// (documented on translateScalarType); everything with a declared
// modifier round-trips it verbatim.
var arrayTypmodWantFormatTypes = map[string]string{
	"v_bare": "text[]",
	"v_len":  "character varying(20)[]",
	"c_len":  "character(5)[]",
	"n_bare": "numeric[]",
	"n_ps":   "numeric(10,2)[]",
	"tz3":    "timestamp(3) with time zone[]",
}

func TestMigrate_PGToPG_ArrayElementModifiers(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, arrayTypmodSeedDDL)

	pg := pgEngineOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mig := &Migrator{Source: pg, Target: pg, SourceDSN: sourceDSN, TargetDSN: targetDSN}
	if err := mig.Run(ctx); err != nil {
		// Pre-fix: refused at create-tables with "column type VARCHAR(0)
		// has no cross-engine PG translation" for v_bare/v_len (and
		// CHAR(0) for c_len) — a false refusal of a legitimate PG schema.
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Schema fidelity: format_type on the real target, per family.
	for col, want := range arrayTypmodWantFormatTypes {
		q := fmt.Sprintf(`
			SELECT format_type(a.atttypid, a.atttypmod)
			FROM pg_attribute a
			WHERE a.attrelid = 'arrmods'::regclass AND a.attname = '%s'`, col)
		if got := pgString(t, targetDSN, q); got != want {
			t.Errorf("target format_type(%s) = %q; want %q", col, got, want)
		}
	}

	// Value fidelity: per-row, per-column ::text equality plus dims for
	// the 2-D row (the Bug-74 flatten check).
	for _, col := range []string{"v_bare", "v_len", "c_len", "n_bare", "n_ps", "tz3"} {
		for id := 1; id <= 4; id++ {
			q := fmt.Sprintf(
				"SELECT COALESCE(%s::text, 'null') FROM arrmods WHERE id = %d", col, id,
			)
			src := pgString(t, sourceDSN, q)
			dst := pgString(t, targetDSN, q)
			if src != dst {
				t.Errorf("value mismatch %s row %d:\n src=%q\n dst=%q", col, id, src, dst)
			}
		}
		dimsQ := fmt.Sprintf(
			"SELECT COALESCE(array_dims(%s)::text, 'null') FROM arrmods WHERE id = 2", col,
		)
		if src, dst := pgString(t, sourceDSN, dimsQ), pgString(t, targetDSN, dimsQ); src != dst {
			t.Errorf("array_dims mismatch %s (2-D row): src=%q dst=%q", col, src, dst)
		}
	}

	// The restored (p,s) constraint must be LIVE, not just displayed:
	// numeric(10,2)[] on the target must reject an out-of-precision
	// element the way the source does.
	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO arrmods (id, n_ps) VALUES (99, '{123456789.99}')`); err == nil {
		t.Error("target numeric(10,2)[] accepted a precision-11 element; the (p,s) constraint was not restored")
	}
}

// TestMigrate_PGToMySQL_ArrayElementModifiers is the cross-engine half:
// the same source schema must not refuse (pre-fix the VARCHAR(0)
// false-refusal was direction-independent — it fired at the reader) and
// each array lands as the documented MySQL JSON mapping with the values
// intact.
func TestMigrate_PGToMySQL_ArrayElementModifiers(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, arrayTypmodSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	mig := &Migrator{Source: pgEng, Target: myEng, SourceDSN: pgSource, TargetDSN: mysqlTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run PG→MySQL: %v", err)
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Every array family maps to a MySQL JSON column; spot-check the
	// column type and the parameterized families' 1-D and 2-D values.
	for _, col := range []string{"v_bare", "v_len", "c_len", "n_bare", "n_ps", "tz3"} {
		var dataType string
		if err := db.QueryRowContext(ctx, `
			SELECT LOWER(data_type) FROM information_schema.columns
			WHERE table_name = 'arrmods' AND column_name = ?`, col).Scan(&dataType); err != nil {
			t.Fatalf("read %s type: %v", col, err)
		}
		if dataType != "json" {
			t.Errorf("mysql column %s data_type = %q; want json", col, dataType)
		}
	}
	checks := []struct {
		query string
		want  string
	}{
		{`SELECT JSON_EXTRACT(v_len, '$[0]') FROM arrmods WHERE id = 1`, `"up to twenty chars"`},
		{`SELECT JSON_EXTRACT(c_len, '$[1]') FROM arrmods WHERE id = 1`, `"de   "`}, // char(5) blank-pads
		// Numeric elements ride the IR as precision-preserving strings
		// (decodeDecimal), so their JSON form is a string — the
		// long-standing ir.Array→JSON convention, not a Bug 195 change.
		{`SELECT JSON_EXTRACT(n_ps, '$[0]') FROM arrmods WHERE id = 1`, `"12345678.99"`},
		{`SELECT JSON_EXTRACT(v_len, '$[1][0]') FROM arrmods WHERE id = 2`, `"three"`}, // 2-D not flattened
		{`SELECT JSON_EXTRACT(n_ps, '$[1]') FROM arrmods WHERE id = 3`, `null`},        // NULL element survives
	}
	for _, c := range checks {
		var got sql.NullString
		if err := db.QueryRowContext(ctx, c.query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		if !got.Valid || got.String != c.want {
			t.Errorf("%s = %q (valid=%v); want %q", c.query, got.String, got.Valid, c.want)
		}
	}
}
