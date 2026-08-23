//go:build d1verify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Live-D1 adversarial value-fidelity matrix for the lossless query-API
// reader (ADR-0132) — the "pin the class, not the representative"
// sweep for the int64 > 2^53 mitigation. The documented loss shape is
// D1's JS/JSON double wire (9007199254740993 → …992); the mitigation is
// the CAST/typeof projection for VALUES and string params for KEYSET
// BOUNDS. These tests drive every reader-side int64 path against a real
// D1 database:
//
//   - value cells (non-key columns): every storage class × worst case,
//     asserted as exact Go values out of the REAL D1RowReader;
//   - keyset bounds (PK path): a > 2^53 integer PK paginated with
//     pageSize=2, so the string-bound discipline is what keeps pages
//     from skipping/duplicating rows;
//   - keyset bounds (rowid path): explicit rowids > 2^53 on a PK-less
//     table, exercising the CAST(rowid AS TEXT) bound.
//
// Ground truth is independent of the reader under test: the seed
// literals are SQL text (the server parses exact digits — no JSON
// number on the way in), and the anti-vacuity floor re-probes the
// stored values server-side via hex()/CAST through raw queryRows.
//
// Same lifecycle as the other d1verify tests: a throwaway database per
// test, created and deleted via the REST API; skip-clean without
// credentials.
//
//	go test -tags=d1verify -v -count=1 -timeout=10m \
//	  -run 'TestD1Verify' ./internal/engines/sqlite/...

package sqlite

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// d1AdvCreds returns the live credentials or skips the test.
func d1AdvCreds(t *testing.T) (account, token string) {
	t.Helper()
	token = os.Getenv("CLOUDFLARE_API_TOKEN")
	account = os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	if token == "" || account == "" {
		t.Skip("CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID not set; d1verify needs live credentials")
	}
	return account, token
}

// d1AdvExec runs one statement through the engine's own client (the
// transport under test is the READ side; writes just need to land).
func d1AdvExec(ctx context.Context, t *testing.T, c *d1Client, sql string) {
	t.Helper()
	if _, err := c.queryRows(ctx, sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// d1AdvReadAll drains a D1RowReader for one table and returns the rows
// in order, failing on any reader error.
func d1AdvReadAll(ctx context.Context, t *testing.T, r *D1RowReader, table *ir.Table) []ir.Row {
	t.Helper()
	ch, err := r.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows(%s): %v", table.Name, err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("reader Err after drain (%s): %v", table.Name, err)
	}
	return out
}

// d1AdvFindTable pulls one table out of a live schema read.
func d1AdvFindTable(ctx context.Context, t *testing.T, client *d1Client, name string) *ir.Table {
	t.Helper()
	sr := &D1SchemaReader{client: client}
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	for _, tbl := range schema.Tables {
		if tbl.Name == name {
			return tbl
		}
	}
	t.Fatalf("table %q not in live schema", name)
	return nil
}

// TestD1Verify_AdversarialValueMatrix seeds every storage class's worst
// case as NON-KEY columns and asserts the exact Go values the real
// D1RowReader delivers — the value half of the > 2^53 mitigation, plus
// blob/text/real/NULL fidelity through the same projection.
func TestD1Verify_AdversarialValueMatrix(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbID := createThrowawayD1Database(ctx, t, account, token)
	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}

	d1AdvExec(ctx, t, client, `CREATE TABLE adv (
		id INTEGER PRIMARY KEY,
		i_53p1 INTEGER,
		i_max INTEGER,
		i_min INTEGER,
		i_snow INTEGER,
		r_17 REAL,
		r_sub REAL,
		r_third REAL,
		t_emoji TEXT,
		t_empty TEXT,
		t_json TEXT,
		b_nul BLOB,
		b_empty BLOB,
		n_dec NUMERIC,
		v_null TEXT
	)`)
	// Seeded via SQL LITERALS: the server parses exact decimal digits, so
	// nothing on the way IN rides a JSON number (independent of the read
	// path under test).
	d1AdvExec(ctx, t, client, `INSERT INTO adv VALUES (
		1,
		9007199254740993,
		9223372036854775807,
		-9223372036854775808,
		1837113234971131904,
		0.1234567890123456789,
		5e-324,
		0.30000000000000004,
		'crab 🦀 café ✅',
		'',
		'{"n":9007199254740993}',
		x'00FF7F00',
		x'',
		19.99,
		NULL
	)`)

	// Anti-vacuity floor: server-side ground truth that the poison
	// landed EXACTLY (CAST digits + hex bytes), independent of the
	// reader's projection below.
	rows, err := client.queryRows(ctx,
		`SELECT CAST(i_53p1 AS TEXT) AS a, CAST(i_max AS TEXT) AS b, hex(b_nul) AS h, typeof(n_dec) AS nt FROM adv`)
	if err != nil {
		t.Fatalf("ground-truth select: %v", err)
	}
	if got := string(rows[0]["a"]); got != `"9007199254740993"` {
		t.Fatalf("server holds i_53p1 = %s; the seed did not land exactly — nothing below measures the reader", got)
	}
	if got := string(rows[0]["b"]); got != `"9223372036854775807"` {
		t.Fatalf("server holds i_max = %s; seed did not land exactly", got)
	}
	if got := string(rows[0]["h"]); got != `"00FF7F00"` {
		t.Fatalf("server holds b_nul = %s; seed did not land exactly", got)
	}

	table := d1AdvFindTable(ctx, t, client, "adv")
	reader := &D1RowReader{client: client}
	got := d1AdvReadAll(ctx, t, reader, table)
	if len(got) != 1 {
		t.Fatalf("read %d rows; want 1", len(got))
	}
	row := got[0]

	wantInts := map[string]int64{
		"id":     1,
		"i_53p1": 9007199254740993, // 2^53 + 1 — the headline
		"i_max":  9223372036854775807,
		"i_min":  -9223372036854775808,
		"i_snow": 1837113234971131904,
	}
	for col, want := range wantInts {
		v, ok := row[col].(int64)
		if !ok {
			t.Errorf("%s: got %T (%v); want int64", col, row[col], row[col])
			continue
		}
		if v != want {
			t.Errorf("SILENT INT ALTERATION on %s: got %d; want %d", col, v, want)
		}
	}
	if v, ok := row["r_17"].(float64); !ok || v != 0.1234567890123456789 {
		t.Errorf("r_17: got %v (%T); want the exact parsed double of 0.1234567890123456789", row["r_17"], row["r_17"])
	}
	if v, ok := row["r_sub"].(float64); !ok || v != 5e-324 {
		t.Errorf("r_sub: got %v (%T); want the subnormal 5e-324", row["r_sub"], row["r_sub"])
	}
	// The value SQLite's post-3.43 format('%.Ng') renders as "0.3" —
	// the cell that catches any regression to a digit-lossy render.
	if v, ok := row["r_third"].(float64); !ok || v != 0.30000000000000004 {
		t.Errorf("SILENT FLOAT ALTERATION on r_third: got %v (%T); want the exact double of 0.30000000000000004",
			row["r_third"], row["r_third"])
	}
	if v, ok := row["t_emoji"].(string); !ok || v != "crab 🦀 café ✅" {
		t.Errorf("t_emoji: got %q (%T)", row["t_emoji"], row["t_emoji"])
	}
	if v, ok := row["t_empty"].(string); !ok || v != "" {
		t.Errorf("t_empty: got %#v (%T); want the empty STRING, not NULL", row["t_empty"], row["t_empty"])
	}
	if v, ok := row["t_json"].(string); !ok || v != `{"n":9007199254740993}` {
		t.Errorf("t_json: got %q; a re-parse through a JSON number would show …992", row["t_json"])
	}
	if v, ok := row["b_nul"].([]byte); !ok || !bytes.Equal(v, []byte{0x00, 0xFF, 0x7F, 0x00}) {
		t.Errorf("b_nul: got %x (%T); want 00ff7f00", row["b_nul"], row["b_nul"])
	}
	if v, ok := row["b_empty"].([]byte); !ok || len(v) != 0 {
		t.Errorf("b_empty: got %#v (%T); want the empty []byte, not NULL", row["b_empty"], row["b_empty"])
	}
	// NUMERIC affinity → ir.Decimal → decimal string; 19.99 must be the
	// shortest round-trip "19.99", never 19.989999999999998.
	if v, ok := row["n_dec"].(string); !ok || v != "19.99" {
		t.Errorf("n_dec: got %#v (%T); want the decimal string \"19.99\"", row["n_dec"], row["n_dec"])
	}
	if row["v_null"] != nil {
		t.Errorf("v_null: got %#v; want untyped nil", row["v_null"])
	}
}

// TestD1Verify_KeysetBigIntPKPagination paginates a table whose INTEGER
// PRIMARY KEY values straddle and exceed 2^53, with pageSize=2 — so
// every page bound is a > 2^53 int64. If the bound rode a JSON number
// (the pre-ADR-0132 §6 shape), the …993/…994/…995 keys would collapse
// to one rounded bound and pages would skip or repeat rows; the
// exactly-once ordered assertion catches either.
func TestD1Verify_KeysetBigIntPKPagination(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbID := createThrowawayD1Database(ctx, t, account, token)
	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}

	d1AdvExec(ctx, t, client, `CREATE TABLE big (id INTEGER PRIMARY KEY, v TEXT)`)
	ids := []int64{
		9007199254740991, // 2^53 - 1: the last float64-exact integer
		9007199254740992, // 2^53
		9007199254740993, // 2^53 + 1: rounds to …992 through a double
		9007199254740994,
		9007199254740995, // rounds to …996 through a double
		9223372036854775806,
		9223372036854775807, // int64 max
	}
	d1AdvExec(ctx, t, client, `INSERT INTO big VALUES
		(9007199254740991,'a'),
		(9007199254740992,'b'),
		(9007199254740993,'c'),
		(9007199254740994,'d'),
		(9007199254740995,'e'),
		(9223372036854775806,'f'),
		(9223372036854775807,'g')`)

	table := d1AdvFindTable(ctx, t, client, "big")
	reader := &D1RowReader{client: client, pageSize: 2} // 4 pages: bounds at …992, …994, …806
	rows := d1AdvReadAll(ctx, t, reader, table)

	if len(rows) != len(ids) {
		t.Fatalf("keyset pagination delivered %d rows; want %d exactly once each (a rounded string bound "+
			"skips or repeats rows)", len(rows), len(ids))
	}
	for i, want := range ids {
		if v, isInt := rows[i]["id"].(int64); !isInt || v != want {
			t.Errorf("row %d: id = %v (%T); want %d", i, rows[i]["id"], rows[i]["id"], want)
		}
	}
}

// TestD1Verify_KeysetBigRowidPagination is the rowid-keyset sibling: a
// PK-less (rowid) table whose EXPLICIT rowids exceed 2^53, pageSize=2,
// exercising the CAST(rowid AS TEXT) projection + string bound.
func TestD1Verify_KeysetBigRowidPagination(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbID := createThrowawayD1Database(ctx, t, account, token)
	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}

	d1AdvExec(ctx, t, client, `CREATE TABLE norow (v TEXT)`)
	d1AdvExec(ctx, t, client, `INSERT INTO norow (rowid, v) VALUES
		(9007199254740993,'a'),
		(9007199254740994,'b'),
		(9007199254740995,'c'),
		(9007199254740996,'d'),
		(9007199254740997,'e')`)

	table := d1AdvFindTable(ctx, t, client, "norow")
	reader := &D1RowReader{client: client, pageSize: 2}
	rows := d1AdvReadAll(ctx, t, reader, table)

	if len(rows) != 5 {
		t.Fatalf("rowid keyset delivered %d rows; want 5 exactly once each", len(rows))
	}
	want := []string{"a", "b", "c", "d", "e"}
	for i, w := range want {
		if v, ok := rows[i]["v"].(string); !ok || v != w {
			t.Errorf("row %d: v = %#v; want %q", i, rows[i]["v"], w)
		}
	}
}
