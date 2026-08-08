//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 233 — the TYPE-PATH axis of the array/JSON landing story, on a
// real MySQL server.
//
// The two files next door pin the same fixture through `migrate` and
// through `backup`+`restore`. This one reaches BOTH lanes again with
// one thing changed: every value column is declared through a
// `CREATE DOMAIN` wrapper instead of as the array type directly. On
// v0.115.0 through v0.116.1 that single change took the copy from
// byte-exact to **`Error 3144 (22032): Cannot create a JSON value from
// a string with CHARACTER SET 'binary'` and zero rows**, on migrate and
// on restore alike, for EVERY element family — including `text[]`,
// which works perfectly without the wrapper.
//
// # Why the fixture is SHARED and not copied
//
// It calls [assertBytesArrayMatrix] — the migrate sibling's own
// assertion helper, unchanged — over rows built from
// [bytesArrayMySQLSeedRows], the sibling's own INSERT block. So the
// bare path and the DOMAIN path are the same values compared the same
// way against the same oracle, and the only difference between the
// files is the axis under test. A copied fixture would let the two
// drift and would turn a parity gate into two independent tests that
// happen to look alike.
//
// # The independent expected value, named (the 2026-08-01 rule)
//
// Inherited from [assertBytesArrayMatrix]: every comparison is against
// a rendering PostgreSQL itself produced from the SOURCE row —
// `j[i]::text`, `encode(y[i],'base64')`, `array_dims` — never against
// sluice's own reporting or its row counts. The scalar half below names
// its own the same way (`v::text`, `v` and `v::int` read off the PG
// source).
//
// # What this reaches, stated so the name cannot be read as broader
//
// The `migrate` bulk-copy door and the `restore` bulk-load door, PG
// source → MySQL target. Within migrate it reaches BOTH MySQL
// bulk-write cores and asserts which one ran from the server's own
// `Com_load` counter — load-bearing here more than anywhere, because
// the defect existed ONLY on the LOAD DATA core: the batched-INSERT
// core carried every DOMAIN column faithfully the whole time, so a run
// that silently fell back would have reported this bug as fixed while
// it shipped. (That is not hypothetical — the first Phase-A run of this
// investigation did exactly that, because the test container defaults
// to `local_infile=OFF`.) The unit-level counterpart, which grades the
// whole family × arrival-shape × type-path product rather than these
// rows, is TestDomainTypePath_* in internal/engines/mysql.
package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// domainBytesArraySeedDDL is the bare fixture reached through DOMAINs.
// The domains carry a CHECK so they are realistic wrappers rather than
// bare type aliases — the MySQL writer's CHECK-drop WARN is a separate
// consumer of the same wrapper and must keep seeing it.
var domainBytesArraySeedDDL = bytesArrayMySQLSeedDDLFor(`
	CREATE DOMAIN d_json  AS json[];
	CREATE DOMAIN d_jsonb AS jsonb[];
	CREATE DOMAIN d_bytea AS bytea[];
	CREATE DOMAIN d_text  AS text[] CHECK (VALUE IS NULL OR array_ndims(VALUE) >= 1);
`, "d_json", "d_jsonb", "d_bytea", "d_text")

// domainScalarSeedDDL is the SCALAR half the report names separately:
// a DOMAIN over `json` (which lands on a MySQL JSON column and failed
// identically), over `text` (which survived, and is the control that
// scopes the defect to JSON-landing columns), and over `bit(8)` (whose
// LOAD DATA path is the OTHER half of the same agreement — the TSV
// field spelling and the SET-clause CONV must move together).
const domainScalarSeedDDL = `
	CREATE DOMAIN ds_json AS json;
	CREATE DOMAIN ds_text AS text;
	CREATE DOMAIN ds_bit  AS bit(8);
	CREATE TABLE domscalar (
		id INT PRIMARY KEY,
		j  ds_json,
		t  ds_text,
		b  ds_bit
	);
	INSERT INTO domscalar VALUES
		(1, '{"a":1}'::json,  'hello',            B'10101010'),
		(2, '[1,2]'::json,    'a,b{c}"d" \e',     B'00000001'),
		(3, 'null'::json,     '',                 B'11111111'),
		(4, NULL,             NULL,               NULL);
`

// TestMigrate_PGToMySQL_ArrayFamiliesThroughADomain is Bug 233's
// migrate lane: the sibling's whole family × shape matrix, reached
// through a DOMAIN, through both MySQL bulk-write cores.
func TestMigrate_PGToMySQL_ArrayFamiliesThroughADomain(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, domainBytesArraySeedDDL)
	applyPGDDL(t, pgSource, domainScalarSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	runMigrate := func(t *testing.T, migrationID string) {
		t.Helper()
		mig := &Migrator{
			Source: pgEng, Target: myEng,
			SourceDSN: pgSource, TargetDSN: mysqlTarget,
			MigrationID: migrationID,
		}
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("Migrator.Run PG→MySQL through a DOMAIN: %v\n\nThis is Bug 233's shape if it names "+
				"Error 3144 / CHARACTER SET 'binary': the DDL emitter downgrades the DOMAIN to its base "+
				"type and lands a MySQL JSON column, and the LOAD DATA SET clause has to make the same "+
				"decision or the server refuses the bytes it is handed", err)
		}
	}

	t.Run("load-data core", func(t *testing.T) {
		setMySQLLocalInfile(t, ctx, db, "ON")
		before := mysqlComLoad(t, ctx, db)
		runMigrate(t, "domain-arrays-loaddata")
		if after := mysqlComLoad(t, ctx, db); after == before {
			t.Fatalf("the server's Com_load counter did not move (%d → %d): the copy did NOT run through "+
				"LOAD DATA — and LOAD DATA is the ONLY core Bug 233 ever affected, so this pass would "+
				"report the bug fixed while it shipped", before, after)
		}
		assertBytesArrayMatrix(t, ctx, pgSource, db)
		assertDomainScalarMatrix(t, ctx, pgSource, db)
	})

	t.Run("batched-insert core", func(t *testing.T) {
		for _, tbl := range []string{"bytesarr", "domscalar"} {
			if _, err := db.ExecContext(ctx, "DROP TABLE "+tbl); err != nil {
				t.Fatalf("drop %s before the second pass: %v", tbl, err)
			}
		}
		setMySQLLocalInfile(t, ctx, db, "OFF")
		before := mysqlComLoad(t, ctx, db)
		runMigrate(t, "domain-arrays-batched")
		if after := mysqlComLoad(t, ctx, db); after != before {
			t.Fatalf("the server's Com_load counter moved (%d → %d) with local_infile=OFF: the fallback "+
				"did not fire and this pass re-ran the LOAD DATA core", before, after)
		}
		assertBytesArrayMatrix(t, ctx, pgSource, db)
		assertDomainScalarMatrix(t, ctx, pgSource, db)
	})
}

// TestRestore_PGToMySQL_ArrayFamiliesThroughADomain is Bug 233's
// restore lane. It is not a duplicate of the migrate case: `restore`
// reaches the writer through the manifest's source-native IR put
// through translate.RetargetForEngine, which has no ir.Domain rule at
// all — so the column arrives at the MySQL writer still wrapped, by a
// different route than migrate's, and the fix has to hold for both.
func TestRestore_PGToMySQL_ArrayFamiliesThroughADomain(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, domainBytesArraySeedDDL)
	applyPGDDL(t, pgSource, domainScalarSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: pgSource, Store: store, SluiceVersion: "test",
	}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run (PG): %v", err)
	}
	if err := (&backup.Restore{
		Target: myEng, TargetDSN: mysqlTarget, Store: store,
	}).Run(ctx); err != nil {
		t.Fatalf("Restore.Run (PG DOMAIN chain → MySQL): %v", err)
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Row counts first: the defect left the tables CREATED and EMPTY,
	// which several assertions below would otherwise report as a query
	// error rather than as the finding.
	for tbl, want := range map[string]string{"bytesarr": "8", "domscalar": "4"} {
		if got := mysqlString(t, ctx, db,
			"SELECT CAST(COUNT(*) AS CHAR) FROM "+tbl); got != want {
			t.Fatalf("restored %s row count = %s; want %s (0 is Bug 233's signature)", tbl, got, want)
		}
	}

	assertBytesArrayMatrix(t, ctx, pgSource, db)
	assertDomainScalarMatrix(t, ctx, pgSource, db)
}

// assertDomainScalarMatrix grades the scalar half of the type path,
// against PostgreSQL's own rendering of the same source row.
//
// The `bit` column is the one whose two LOAD DATA halves have to agree:
// the TSV serialiser writes the canonical '0'/'1' bit-string only for a
// column it recognises as ir.Bit, and the SET clause parses base-2 only
// for a column it recognises the same way. Either half alone reading
// the DOMAIN wrapper as "not a bit column" corrupts the value, and
// `CONV('170', 2, 10)` = 1 is what one of those halves produces — a
// silent wrong number, not an error, which is why it is asserted
// numerically against PG rather than eyeballed.
func assertDomainScalarMatrix(t *testing.T, ctx context.Context, pgSource string, db *sql.DB) {
	t.Helper()

	// The columns must have landed on the base type's MySQL shape; a
	// different column type makes every value comparison meaningless.
	for col, want := range map[string]string{"j": "json", "t": "longtext", "b": "bit"} {
		var dataType string
		if err := db.QueryRowContext(ctx, `
			SELECT LOWER(data_type) FROM information_schema.columns
			WHERE table_name = 'domscalar' AND column_name = ?`, col).Scan(&dataType); err != nil {
			t.Fatalf("read domscalar.%s type: %v", col, err)
		}
		if dataType != want {
			t.Errorf("mysql column domscalar.%s data_type = %q; want %q — a DOMAIN must land on its base "+
				"type's MySQL shape", col, dataType, want)
		}
	}

	for _, id := range []int{1, 2, 3} {
		// json: PG's own json_out versus the document MySQL stored. Read
		// back through JSON_UNQUOTE-free CAST so a document that arrived
		// as an opaque string rather than as JSON still compares.
		want := pgString(t, pgSource, fmt.Sprintf(
			"SELECT j::text FROM domscalar WHERE id = %d", id,
		))
		got := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT CAST(j AS CHAR) FROM domscalar WHERE id = %d", id,
		))
		if !jsonEquivalent(t, ctx, db, id, want) {
			t.Errorf("row %d DOMAIN-over-json: MySQL holds %q; PG j::text = %q", id, got, want)
		}

		// text: byte-for-byte, including the punctuation the TSV escaper
		// has to survive and the empty string.
		wantText := pgString(t, pgSource, fmt.Sprintf(
			"SELECT t FROM domscalar WHERE id = %d", id,
		))
		gotText := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT t FROM domscalar WHERE id = %d", id,
		))
		if gotText != wantText {
			t.Errorf("row %d DOMAIN-over-text: MySQL = %q; PG = %q", id, gotText, wantText)
		}

		// bit: PG's own numeric reading of the bit string versus MySQL's.
		wantBit := pgString(t, pgSource, fmt.Sprintf(
			"SELECT (b::bit(8))::int::text FROM domscalar WHERE id = %d", id,
		))
		gotBit := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT CAST(b + 0 AS CHAR) FROM domscalar WHERE id = %d", id,
		))
		if gotBit != wantBit {
			t.Errorf("row %d DOMAIN-over-bit(8): MySQL reads %s; PG reads %s — the TSV field spelling and "+
				"the SET-clause CONV are one agreement and both must see the storage type", id, gotBit, wantBit)
		}
	}

	// The whole-NULL row: a NULL must stay NULL on every column.
	for _, col := range []string{"j", "t", "b"} {
		var raw sql.NullString
		if err := db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT CAST(%s AS CHAR) FROM domscalar WHERE id = 4", col,
		)).Scan(&raw); err != nil {
			t.Fatalf("row 4 col %s: %v", col, err)
		}
		if raw.Valid {
			t.Errorf("row 4 col %s (NULL) = %q; want SQL NULL", col, raw.String)
		}
	}
}

// jsonEquivalent compares the stored document against PG's rendering
// through MySQL's own JSON parser, so an insignificant whitespace
// difference between `json_out` and MySQL's JSON normalisation is not
// read as loss. A value MySQL could not parse as JSON — the shape the
// defect produced — cannot compare equal to anything.
func jsonEquivalent(t *testing.T, ctx context.Context, db *sql.DB, id int, pgText string) bool {
	t.Helper()
	var eq sql.NullInt64
	if err := db.QueryRowContext(ctx,
		"SELECT j = CAST(? AS JSON) FROM domscalar WHERE id = ?", pgText, id).Scan(&eq); err != nil {
		t.Fatalf("row %d JSON compare: %v", id, err)
	}
	return eq.Valid && eq.Int64 == 1
}
