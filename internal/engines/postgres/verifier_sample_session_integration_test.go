//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The sample hash renders values as text server-side, and Postgres text
// output depends on SESSION settings each endpoint inherits from its
// server/database/role defaults. [sampleSessionPins] fixes them; these
// pins put two databases with different defaults on either side.
//
// Independent expected value: which rows hold the same stored value and
// which differ is written here, per row.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// sampleHashesByKey reads every row's sample hash from table on dsn.
func sampleHashesByKey(t *testing.T, ctx context.Context, dsn, table string, algo ir.HashAlgorithm) map[string]string {
	t.Helper()
	rdr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	for _, tb := range schema.Tables {
		if tb.Name != table {
			continue
		}
		rows, err := rdr.(ir.SampleVerifier).SampleRowHashes(ctx, tb, selfSampleColumns(tb), 1000, 42, algo)
		if err != nil {
			t.Fatalf("SampleRowHashes %s: %v", table, err)
		}
		out := map[string]string{}
		for _, r := range rows {
			out[r.PrimaryKey] = r.Hash
		}
		return out
	}
	t.Fatalf("table %s not found", table)
	return nil
}

// TestSampleRowHashes_SessionRenderingPinned: endpoint A defaults to
// DateStyle 'SQL, MDY', IntervalStyle postgres, TimeZone UTC, bytea hex;
// B to 'SQL, DMY', sql_standard, Asia/Tokyo, escape. Unpinned, row 1 — a
// day/month-swapped DATE — renders 01/02/2024 on both and reads CLEAN,
// and rows 2-4, which hold identical values, mismatch.
func TestSampleRowHashes_SessionRenderingPinned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsnA, cleanupA := newSharedPGDB(t, "vsess_a")
	defer cleanupA()
	dsnB, cleanupB := newSharedPGDB(t, "vsess_b")
	defer cleanupB()

	const ddl = `CREATE TABLE sp (id bigint PRIMARY KEY, d date, tz timestamptz, iv interval, by bytea);`
	applyPGSQL(t, dsnA, ddl+`
		INSERT INTO sp VALUES (1, '2024-01-02', NULL, NULL, NULL),
			(2, NULL, '2024-01-02 03:04:05+00', NULL, NULL),
			(3, NULL, NULL, '1 day 02:03:04', NULL),
			(4, NULL, NULL, NULL, '\xdead');`)
	applyPGSQL(t, dsnB, ddl+`
		INSERT INTO sp VALUES (1, '2024-02-01', NULL, NULL, NULL),
			(2, NULL, '2024-01-02 03:04:05+00', NULL, NULL),
			(3, NULL, NULL, '1 day 02:03:04', NULL),
			(4, NULL, NULL, NULL, '\xdead');`)
	applyPGSQL(t, dsnA, `ALTER DATABASE vsess_a SET DateStyle = 'SQL, MDY';
		ALTER DATABASE vsess_a SET IntervalStyle = 'postgres';
		ALTER DATABASE vsess_a SET TimeZone = 'UTC';
		ALTER DATABASE vsess_a SET bytea_output = 'hex';`)
	applyPGSQL(t, dsnB, `ALTER DATABASE vsess_b SET DateStyle = 'SQL, DMY';
		ALTER DATABASE vsess_b SET IntervalStyle = 'sql_standard';
		ALTER DATABASE vsess_b SET TimeZone = 'Asia/Tokyo';
		ALTER DATABASE vsess_b SET bytea_output = 'escape';`)

	for _, algo := range []ir.HashAlgorithm{ir.HashMD5, ir.HashSHA256} {
		a := sampleHashesByKey(t, ctx, dsnA, "sp", algo)
		b := sampleHashesByKey(t, ctx, dsnB, "sp", algo)
		if a["1"] == b["1"] {
			t.Errorf("%s: row 1: a day/month-swapped DATE hashes EQUAL across DateStyle defaults — false clean", algo)
		}
		for _, pk := range []string{"2", "3", "4"} {
			if a[pk] != b[pk] {
				t.Errorf("%s: row %s: an identical value hashes differently across session defaults — false mismatch", algo, pk)
			}
		}
	}
}

// TestSampleRowHashes_StrictHashOnSQLASCIIDatabase: --strict-hash feeds
// sha256() the row's text converted to the database's own encoding. A
// fixed 'UTF8' refused a SQL_ASCII database holding a byte that is not
// valid UTF-8 ("invalid byte sequence"), where MD5 hashed it fine.
func TestSampleRowHashes_StrictHashOnSQLASCIIDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	adminDSN, cleanup := newSharedPGDB(t, "vsascii_admin")
	defer cleanup()
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = admin.Close() }()
	for _, stmt := range []string{
		`DROP DATABASE IF EXISTS vsascii`,
		`CREATE DATABASE vsascii ENCODING 'SQL_ASCII' TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C'`,
	} {
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	host, port, user, password := sharedPrimitives()
	dsn := sharedPGDSN(host, port, user, password, "vsascii")
	applyPGSQL(t, dsn, `CREATE TABLE sa (id bigint PRIMARY KEY, x text);
		INSERT INTO sa VALUES (1, E'\xff\x41'), (2, E'\xfe\x41');`)

	for _, algo := range []ir.HashAlgorithm{ir.HashMD5, ir.HashSHA256} {
		h := sampleHashesByKey(t, ctx, dsn, "sa", algo)
		if len(h) != 2 {
			t.Fatalf("%s: sampled %d rows; want 2", algo, len(h))
		}
		if h["1"] == h["2"] {
			t.Errorf("%s: two different non-UTF-8 values hash equal", algo)
		}
	}
	if _, err := admin.ExecContext(ctx, `DROP DATABASE IF EXISTS vsascii WITH (FORCE)`); err != nil {
		t.Logf("drop vsascii: %v", err)
	}
}
