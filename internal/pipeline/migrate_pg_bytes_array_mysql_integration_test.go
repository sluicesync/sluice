//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL half of the `json[]` / `jsonb[]` / `bytea[]` array-element
// story — the sibling item 145 did not enumerate.
//
// Item 145 gave `json[]`/`jsonb[]` a Postgres-target arm and swept the
// Postgres writer carefully, including refusing the pgtrigger door it
// could not carry faithfully. It never asked what the SAME family does on
// a MySQL target, and the answer was silent loss: MySQL has no array
// type, so an ir.Array lands on a JSON column via
// [mysql.convertArrayLikeToJSON], which json.Marshal'd the IR's `[]any`
// wholesale — and Go marshals a `[]byte` element as a BASE64 STRING.
// Both PG read lanes decode a json/jsonb array leaf to `[]byte`
// (decodeBytes) and a bytea leaf to `[]byte` (decodeBytea), so:
//
//	PG json[]  {"{\"a\":1}"}  →  MySQL  ["eyJhIjoxfQ=="]
//	PG bytea[] {"\\xdead"}    →  MySQL  ["3q0="]
//
// exit 0, row counts matching, a JSON document replaced by an opaque
// token. No pin existed for either family against a MySQL target in any
// shape.
//
// # The independent expected value, named
//
// Every assertion below compares the MySQL target against a rendering
// PostgreSQL itself produced from the SOURCE row — never against
// sluice's own reporting, and never against a constant this file
// computed the same way the writer does:
//
//   - json/jsonb elements: PG's `j[i]::text` (the server's own json_out /
//     jsonb_out) versus MySQL's `JSON_UNQUOTE(JSON_EXTRACT(...))`.
//   - bytea elements: PG's `encode(b[i],'base64')` versus the same MySQL
//     extraction. Two independent base64 implementations (PostgreSQL's C
//     encoder and Go's) agreeing on the byte string is the oracle; the
//     test values are kept short so PG's 76-column line wrapping never
//     fires.
//   - shape: PG's `array_dims` / `array_length` versus MySQL's
//     `JSON_LENGTH`, per row.
//
// # What this file reaches, stated so the name cannot be read as broader
//
// The `migrate` bulk-copy door only, PG source → MySQL target, for the
// two `[]byte`-leaf families plus the string-leaf control column that
// proves the fixture is not vacuous. The CDC doors (pgoutput apply,
// pgtrigger apply) are covered by the roster gate
// TestEveryDecodableArrayElementHasAMySQLLeafVerdict in the mysql
// package, which is a dispatch check and not a fidelity one. The two
// halves are complementary; neither substitutes for the other.
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

// bytesArrayMySQLSeedDDL carries the two `[]byte`-leaf families and a
// text[] control, one row per shape: 1-D, 2-D, NULL-element, empty
// array, whole-column NULL, and the two traps this pair specifically
// carries — a JSON document that IS `null` sitting beside a SQL NULL
// element, and a bytea whose CONTENT spells the `\x`-hex prefix.
const bytesArrayMySQLSeedDDL = `
	CREATE TABLE bytesarr (
		id INT PRIMARY KEY,
		j  json[],
		b  jsonb[],
		y  bytea[],
		t  text[]
	);
	INSERT INTO bytesarr VALUES
		(1, ARRAY[$${"a":1}$$::json, $$[1,2]$$::json, $$"str"$$::json, $$42$$::json],
		    ARRAY[$${"a":1}$$::jsonb, $$[1,2]$$::jsonb, $$"str"$$::jsonb, $$42$$::jsonb],
		    ARRAY['\x00'::bytea, '\xff'::bytea, '\xdeadbeef'::bytea, ''::bytea],
		    ARRAY['a','b','c','d']),
		(2, ARRAY[ARRAY[$${"a":1}$$::json, $${"b":2}$$::json],
		          ARRAY[$${"c":3}$$::json, $${"d":4}$$::json]],
		    ARRAY[ARRAY[$${"a":1}$$::jsonb, $${"b":2}$$::jsonb],
		          ARRAY[$${"c":3}$$::jsonb, $${"d":4}$$::jsonb]],
		    ARRAY[ARRAY['\x01'::bytea, '\x02'::bytea], ARRAY['\x03'::bytea, '\x04'::bytea]],
		    ARRAY[ARRAY['a','b'], ARRAY['c','d']]),
		(3, ARRAY[$${"a":1}$$::json, NULL, $${"b":2}$$::json],
		    ARRAY[$${"a":1}$$::jsonb, NULL, $${"b":2}$$::jsonb],
		    ARRAY['\xaa'::bytea, NULL, '\xbb'::bytea],
		    ARRAY['a', NULL, 'b']),
		(4, ARRAY[]::json[], ARRAY[]::jsonb[], ARRAY[]::bytea[], ARRAY[]::text[]),
		(5, NULL, NULL, NULL, NULL),
		(6, ARRAY[$$null$$::json, NULL, $$true$$::json],
		    ARRAY[$$null$$::jsonb, NULL, $$true$$::jsonb],
		    ARRAY[decode('5c7834313432', 'hex'), NULL, '\x41'::bytea],
		    ARRAY['null', NULL, 'true']),
		(7, ARRAY[$${"k":"a,b{c}\"d\" \\e"}$$::json],
		    ARRAY[$${"k":"a,b{c}\"d\" \\e"}$$::jsonb],
		    ARRAY['\x7b2c7d'::bytea],
		    ARRAY['a,b{c}"d" \e']);
`

// TestMigrate_PGToMySQL_JSONAndByteaArrays is the family × shape matrix
// for the two `[]byte`-leaf array families on a MySQL target.
func TestMigrate_PGToMySQL_JSONAndByteaArrays(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bytesArrayMySQLSeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	// Every array family maps to a MySQL JSON column. A different column
	// type would make every value comparison below meaningless.
	for _, col := range []string{"j", "b", "y", "t"} {
		var dataType string
		if err := db.QueryRowContext(ctx, `
			SELECT LOWER(data_type) FROM information_schema.columns
			WHERE table_name = 'bytesarr' AND column_name = ?`, col).Scan(&dataType); err != nil {
			t.Fatalf("read %s type: %v", col, err)
		}
		if dataType != "json" {
			t.Errorf("mysql column %s data_type = %q; want json", col, dataType)
		}
	}

	// ---- Shape, per row per family: PG's array_length vs MySQL's
	// JSON_LENGTH. A flattened 2-D row reads 4 where PG reads 2.
	for _, id := range []int{1, 2, 3, 4, 6, 7} {
		for _, col := range []string{"j", "b", "y", "t"} {
			want := pgString(t, pgSource, fmt.Sprintf(
				"SELECT COALESCE(array_length(%s, 1)::text, '0') FROM bytesarr WHERE id = %d", col, id,
			))
			got := mysqlString(t, ctx, db, fmt.Sprintf(
				"SELECT CAST(JSON_LENGTH(%s) AS CHAR) FROM bytesarr WHERE id = %d", col, id,
			))
			if got != want {
				t.Errorf("row %d col %s: MySQL JSON_LENGTH = %q; PG array_length = %q", id, col, got, want)
			}
		}
	}
	// The 2-D row's inner length too — JSON_LENGTH alone reads 2 for both
	// a real 2×2 and a flattened-to-2 array.
	for _, col := range []string{"j", "b", "y", "t"} {
		if got := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT CAST(JSON_LENGTH(%s, '$[0]') AS CHAR) FROM bytesarr WHERE id = 2", col,
		)); got != "2" {
			t.Errorf("row 2 col %s: inner JSON_LENGTH = %q; want 2 — the array was FLATTENED", col, got)
		}
	}

	// ---- json / jsonb documents: the exact bytes PG rendered.
	//
	// The carried form is the document's TEXT as a JSON string, so
	// JSON_UNQUOTE of the element must equal PG's own `::text`. Pre-fix
	// this read `eyJhIjoxfQ==`.
	for _, tc := range []struct {
		id                  int
		col, pgExpr, myPath string
	}{
		{1, "j", "j[1]", "$[0]"},
		{1, "j", "j[2]", "$[1]"},
		{1, "j", "j[3]", "$[2]"},
		{1, "j", "j[4]", "$[3]"},
		{1, "b", "b[1]", "$[0]"},
		{1, "b", "b[2]", "$[1]"},
		{1, "b", "b[3]", "$[2]"},
		{1, "b", "b[4]", "$[3]"},
		{2, "j", "j[1][1]", "$[0][0]"},
		{2, "j", "j[2][2]", "$[1][1]"},
		{2, "b", "b[1][1]", "$[0][0]"},
		{2, "b", "b[2][2]", "$[1][1]"},
		{3, "j", "j[1]", "$[0]"},
		{3, "b", "b[3]", "$[2]"},
		{7, "j", "j[1]", "$[0]"},
		{7, "b", "b[1]", "$[0]"},
	} {
		want := pgString(t, pgSource, fmt.Sprintf(
			"SELECT %s::text FROM bytesarr WHERE id = %d", tc.pgExpr, tc.id,
		))
		got := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT JSON_UNQUOTE(JSON_EXTRACT(%s, '%s')) FROM bytesarr WHERE id = %d", tc.col, tc.myPath, tc.id,
		))
		if got != want {
			t.Errorf("row %d %s: MySQL %s = %q; PG %s::text = %q",
				tc.id, tc.col, tc.myPath, got, tc.pgExpr, want)
		}
	}

	// ---- bytea elements: PostgreSQL's own base64 encoder as the oracle.
	for _, tc := range []struct {
		id             int
		pgExpr, myPath string
	}{
		{1, "y[1]", "$[0]"},
		{1, "y[2]", "$[1]"},
		{1, "y[3]", "$[2]"},
		{1, "y[4]", "$[3]"}, // the empty bytea — present, not NULL
		{2, "y[1][1]", "$[0][0]"},
		{2, "y[2][2]", "$[1][1]"},
		{3, "y[1]", "$[0]"},
		{6, "y[1]", "$[0]"}, // content spells the \x-hex prefix
		{7, "y[1]", "$[0]"},
	} {
		want := pgString(t, pgSource, fmt.Sprintf(
			"SELECT encode(%s, 'base64') FROM bytesarr WHERE id = %d", tc.pgExpr, tc.id,
		))
		got := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT JSON_UNQUOTE(JSON_EXTRACT(y, '%s')) FROM bytesarr WHERE id = %d", tc.myPath, tc.id,
		))
		if got != want {
			t.Errorf("row %d bytea %s: MySQL = %q; PG encode(%s,'base64') = %q",
				tc.id, tc.myPath, got, tc.pgExpr, want)
		}
	}

	// ---- A SQL NULL element must stay JSON null, and a JSON `null`
	// DOCUMENT must not collapse into it. This is the distinction that
	// decides the design: carrying the document as a JSON string keeps
	// them apart, nesting it as a JSON value would not.
	for _, col := range []string{"j", "b"} {
		if got := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT JSON_TYPE(JSON_EXTRACT(%s, '$[1]')) FROM bytesarr WHERE id = 3", col,
		)); got != "NULL" {
			t.Errorf("row 3 col %s element 2 (a SQL NULL) has JSON_TYPE %q; want NULL", col, got)
		}
		// Row 6 element 1 is the document `null`; element 2 is a SQL NULL.
		doc := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT JSON_TYPE(JSON_EXTRACT(%s, '$[0]')) FROM bytesarr WHERE id = 6", col,
		))
		sqlNull := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT JSON_TYPE(JSON_EXTRACT(%s, '$[1]')) FROM bytesarr WHERE id = 6", col,
		))
		if doc == sqlNull {
			t.Errorf("row 6 col %s: the JSON document `null` and the SQL NULL element beside it both read "+
				"JSON_TYPE %q — two distinct source values collapsed into one", col, doc)
		}
		if sqlNull != "NULL" {
			t.Errorf("row 6 col %s: the SQL NULL element reads JSON_TYPE %q; want NULL", col, sqlNull)
		}
	}
	if got := mysqlString(t, ctx, db,
		"SELECT JSON_TYPE(JSON_EXTRACT(y, '$[1]')) FROM bytesarr WHERE id = 3"); got != "NULL" {
		t.Errorf("row 3 bytea element 2 (a SQL NULL) has JSON_TYPE %q; want NULL", got)
	}

	// ---- Empty array and whole-column NULL.
	for _, col := range []string{"j", "b", "y", "t"} {
		if got := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT CAST(%s AS CHAR) FROM bytesarr WHERE id = 4", col,
		)); got != "[]" {
			t.Errorf("row 4 col %s (empty array) = %q; want []", col, got)
		}
		var raw sql.NullString
		if err := db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT CAST(%s AS CHAR) FROM bytesarr WHERE id = 5", col,
		)).Scan(&raw); err != nil {
			t.Fatalf("row 5 col %s: %v", col, err)
		}
		if raw.Valid {
			t.Errorf("row 5 col %s (whole-column NULL) = %q; want SQL NULL", col, raw.String)
		}
	}

	// ---- The string-leaf control. If this were the only column that
	// worked the fixture would still look healthy, which is why it is
	// here: it proves the harness compares real values.
	for _, tc := range []struct {
		id             int
		pgExpr, myPath string
	}{
		{1, "t[1]", "$[0]"},
		{2, "t[2][1]", "$[1][0]"},
		{6, "t[1]", "$[0]"}, // the four letters n-u-l-l as TEXT
		{7, "t[1]", "$[0]"},
	} {
		want := pgString(t, pgSource, fmt.Sprintf(
			"SELECT %s FROM bytesarr WHERE id = %d", tc.pgExpr, tc.id,
		))
		got := mysqlString(t, ctx, db, fmt.Sprintf(
			"SELECT JSON_UNQUOTE(JSON_EXTRACT(t, '%s')) FROM bytesarr WHERE id = %d", tc.myPath, tc.id,
		))
		if got != want {
			t.Errorf("row %d text %s: MySQL = %q; PG %s = %q", tc.id, tc.myPath, got, tc.pgExpr, want)
		}
	}
}

// mysqlString runs a single-value query against the MySQL target and
// returns the value as a string, failing the test on error. A SQL NULL
// reads as the literal "<NULL>" so a null never silently compares equal
// to an empty string.
func mysqlString(t *testing.T, ctx context.Context, db *sql.DB, query string) string {
	t.Helper()
	var v sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&v); err != nil {
		t.Fatalf("mysql query %q: %v", query, err)
	}
	if !v.Valid {
		return "<NULL>"
	}
	return v.String
}
