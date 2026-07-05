//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the LOAD DATA LOCAL INFILE row-writer path.
// These tests boot a real MySQL container (via testcontainers-go) and
// drive the writer through its LOAD DATA branch, the
// local_infile=OFF fallback branch, and the TSV-escape edge cases
// that distinguish this path from the BatchedInsert path.
//
// To run:
//   go test -tags=integration ./internal/engines/mysql/ -run LoadData

package mysql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestRowWriter_LoadDataInfile_RoundTrip(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	enableLocalInfile(t, dsn)

	const ddl = `
		CREATE TABLE samples (
			id          BIGINT UNSIGNED NOT NULL,
			active      TINYINT(1)      NOT NULL,
			name        VARCHAR(64)     NOT NULL,
			price       DECIMAL(10,2)   NOT NULL,
			role        ENUM('admin','user','guest') NOT NULL,
			tags        SET('go','sql','mysql','postgres') NOT NULL,
			payload     JSON            NULL,
			data        BLOB            NULL,
			created_at  TIMESTAMP(0)    NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "samples")
	if table == nil {
		t.Fatalf("samples table not found")
	}

	createdAt := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)
	wantRows := []ir.Row{
		{
			"id":         uint64(1),
			"active":     true,
			"name":       "Alice",
			"price":      "19.95",
			"role":       "admin",
			"tags":       []string{"go", "sql"},
			"payload":    []byte(`{"plan": "free"}`),
			"data":       []byte{0xde, 0xad, 0xbe, 0xef},
			"created_at": createdAt,
		},
		{
			"id":         uint64(2),
			"active":     false,
			"name":       "Bob",
			"price":      "0.00",
			"role":       "user",
			"tags":       []string{},
			"payload":    nil,
			"data":       nil,
			"created_at": createdAt,
		},
	}

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	mustBeLoadData(t, rw)

	in := make(chan ir.Row, len(wantRows))
	for _, r := range wantRows {
		in <- r
	}
	close(in)

	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	out, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var got []ir.Row
	for row := range out {
		got = append(got, row)
	}
	if rrConcrete, ok := rr.(*RowReader); ok {
		if err := rrConcrete.Err(); err != nil {
			t.Fatalf("Err after streaming: %v", err)
		}
	}

	if len(got) != len(wantRows) {
		t.Fatalf("got %d rows; want %d", len(got), len(wantRows))
	}
	for i, w := range wantRows {
		g := got[i]
		for col, wantVal := range w {
			gotVal := g[col]
			if !rowValueEqualLD(gotVal, wantVal) {
				t.Errorf("row[%d].%s = %#v (%T); want %#v (%T)",
					i, col, gotVal, gotVal, wantVal, wantVal)
			}
		}
	}
}

func TestRowWriter_LoadDataInfile_TSVEscapeEdges(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	enableLocalInfile(t, dsn)

	// VARBINARY for the binary column so MySQL preserves bytes
	// verbatim (CHARSET utf8mb4 on TEXT/BLOB doesn't affect this,
	// but VARBINARY makes the round-trip unambiguous).
	const ddl = `
		CREATE TABLE escapes (
			id     INT             NOT NULL,
			s      VARCHAR(255)    NULL,
			b      VARBINARY(255)  NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "escapes")

	wantRows := []ir.Row{
		{"id": int64(1), "s": "plain", "b": []byte("plain bytes")},
		{"id": int64(2), "s": "with\ttab", "b": []byte{'a', '\t', 'b'}},
		{"id": int64(3), "s": "with\nnewline\rline", "b": []byte{'\n', '\r'}},
		{"id": int64(4), "s": `back\slash`, "b": []byte{'\\', '\\', '\\'}},
		{"id": int64(5), "s": "\x00null\x00byte", "b": []byte{0x00, 0x01, 0x02, 0x00}},
		{"id": int64(6), "s": "", "b": []byte{}},
		{"id": int64(7), "s": nil, "b": nil},
		{"id": int64(8), "s": "all\\\t\n\rmix\x00", "b": []byte{0xde, 0xad, '\t', '\n', 0x00, '\\'}},
	}

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	mustBeLoadData(t, rw)

	in := make(chan ir.Row, len(wantRows))
	for _, r := range wantRows {
		in <- r
	}
	close(in)
	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	got := readRowsRaw(t, dsn, "SELECT id, s, b FROM escapes ORDER BY id")
	if len(got) != len(wantRows) {
		t.Fatalf("read back %d rows; want %d", len(got), len(wantRows))
	}
	for i, w := range wantRows {
		gotID := got[i]["id"].(int64)
		wantID := w["id"].(int64)
		if gotID != wantID {
			t.Errorf("row[%d] id = %d; want %d", i, gotID, wantID)
		}
		if !valEqual(got[i]["s"], w["s"]) {
			t.Errorf("row[%d] s = %#v; want %#v", i, got[i]["s"], w["s"])
		}
		if !valEqual(got[i]["b"], w["b"]) {
			t.Errorf("row[%d] b = %#v; want %#v", i, got[i]["b"], w["b"])
		}
	}
}

func TestRowWriter_LoadDataInfile_LargeBatch(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	enableLocalInfile(t, dsn)

	const ddl = `
		CREATE TABLE counts (
			n INT NOT NULL,
			s VARCHAR(32) NOT NULL,
			PRIMARY KEY (n)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "counts")

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	mustBeLoadData(t, rw)

	const total = 5000
	in := make(chan ir.Row, 256)
	go func() {
		defer close(in)
		for i := 0; i < total; i++ {
			in <- ir.Row{"n": int64(i), "s": "row"}
		}
	}()

	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}

	rows := readRowsRaw(t, dsn, "SELECT n FROM counts ORDER BY n")
	if len(rows) != total {
		t.Fatalf("got %d rows; want %d", len(rows), total)
	}
	// Spot-check the first/last/middle to catch off-by-one shrinkage.
	if rows[0]["n"].(int64) != 0 || rows[total-1]["n"].(int64) != int64(total-1) {
		t.Errorf("rows endpoints: first=%v, last=%v", rows[0], rows[total-1])
	}
}

func TestRowWriter_LoadDataInfile_FallbackWhenLocalInfileOff(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Container default is local_infile=OFF on MySQL 8.0+. Belt-
	// and-suspenders: explicitly set it OFF in case the testcontainers
	// image differs.
	disableLocalInfile(t, dsn)

	const ddl = `
		CREATE TABLE fallback_rows (
			n INT NOT NULL,
			s VARCHAR(32) NOT NULL,
			PRIMARY KEY (n)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "fallback_rows")

	rw := openRowWriter(t, ctx, dsn)
	defer closeIf(rw)
	mustBeLoadData(t, rw)

	const total = 250
	in := make(chan ir.Row, 64)
	go func() {
		defer close(in)
		for i := 0; i < total; i++ {
			in <- ir.Row{"n": int64(i), "s": "row"}
		}
	}()

	// Even with local_infile=OFF the call must succeed (fallback to
	// BatchedInsert). The exit signal is "rows landed via SELECT".
	if err := rw.WriteRows(ctx, table, in); err != nil {
		t.Fatalf("WriteRows fallback: %v", err)
	}

	rows := readRowsRaw(t, dsn, "SELECT n FROM fallback_rows ORDER BY n")
	if len(rows) != total {
		t.Fatalf("fallback path landed %d rows; want %d", len(rows), total)
	}
}

// TestRowWriter_LoadDataInfile_NoBackslashEscapes pins Bug 178: a
// MySQL-target bulk copy over the LOAD DATA LOCAL INFILE path hung
// indefinitely (0 rows, no error) when NO_BACKSLASH_ESCAPES was active on
// the target connection. Root cause: buildLoadDataStmt spelled the
// FIELDS/ESCAPED/LINES framing as backslash-escaped SQL string literals
// ('\t'/'\\'/'\n'); under NBE the server parses '\\' as TWO backslashes,
// tripping the "ESCAPED BY must be a single character" check (Error 1083)
// BEFORE the LOCAL INFILE request — so the driver never drained the
// reader and the encoder goroutine deadlocked on its pipe write. The fix
// makes the framing NBE-invariant via hex literals (X'09'/X'5C'/X'0A'),
// plus a liveness guard so any early exec error fails loud, not hangs.
//
// The pin drives the writer with NBE active and asserts it COMPLETES
// byte-exact — including the framing torture class (a literal backslash;
// backslash+field-terminator; backslash+line-terminator; a `\N`-looking
// value that must NOT collapse to SQL NULL; a `\0`-looking value; a real
// NUL byte). WriteRows runs under a hard deadline so a regression re-hangs
// as a test FAILURE, never a hung process.
func TestRowWriter_LoadDataInfile_NoBackslashEscapes(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	enableLocalInfile(t, dsn)

	const ddl = `
		CREATE TABLE nbe_escapes (
			id INT             NOT NULL,
			s  VARCHAR(255)    NULL,
			b  VARBINARY(255)  NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDL(t, dsn, ddl)

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "nbe_escapes")
	if table == nil {
		t.Fatalf("nbe_escapes table not found")
	}

	// Open a target DB whose session carries NO_BACKSLASH_ESCAPES — the
	// exact trigger. Strict is kept alongside NBE so the writer's warning
	// path stays in its refuse-on-clamp mode (none of the torture rows
	// produce a type-conversion warning, so it stays quiet).
	nbeMode := "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES"
	db, err := openDB(ctx, mustParseDSN(t, dsn), &nbeMode)
	if err != nil {
		t.Fatalf("openDB(NBE): %v", err)
	}
	defer db.Close()

	// Self-validate the trigger actually took on sluice's pooled conn;
	// otherwise the pin would be a false green.
	var sessionMode string
	if err := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&sessionMode); err != nil {
		t.Fatalf("read @@SESSION.sql_mode: %v", err)
	}
	if !strings.Contains(sessionMode, "NO_BACKSLASH_ESCAPES") {
		t.Fatalf("session sql_mode %q lacks NO_BACKSLASH_ESCAPES; trigger not active", sessionMode)
	}

	// The framing torture class: every shape where the byte stream the
	// serializer emits interacts with the field/line/escape framing.
	wantRows := []ir.Row{
		{"id": int64(1), "s": "plain", "b": []byte("plain")},
		{"id": int64(2), "s": `one\backslash`, "b": []byte{'\\'}},                // literal backslash
		{"id": int64(3), "s": "tab\tafter", "b": []byte{'\\', '\t'}},             // backslash + field terminator
		{"id": int64(4), "s": "line\nafter", "b": []byte{'\\', '\n'}},            // backslash + line terminator
		{"id": int64(5), "s": `looks\Nlike-null`, "b": []byte{'\\', 'N'}},        // must NOT become SQL NULL
		{"id": int64(6), "s": `looks\0like-nul`, "b": []byte{'\\', '0'}},         // \0-looking
		{"id": int64(7), "s": "carriage\rreturn", "b": []byte{0x00, '\\', 0x00}}, // NUL around backslash
		{"id": int64(8), "s": nil, "b": nil},                                     // real NULL, distinct from \N
	}

	w := &RowWriter{db: db, sqlMode: &nbeMode, bulkLoad: ir.BulkLoadLoadDataInfile}

	in := make(chan ir.Row, len(wantRows))
	for _, r := range wantRows {
		in <- r
	}
	close(in)

	// Hard deadline INDEPENDENT of ctx: pre-fix, WriteRows deadlocked on
	// the encoder pipe write, which no ctx cancel could unwind — so a
	// regression must surface as a FAIL here, not a hung test binary.
	done := make(chan error, 1)
	go func() { done <- w.WriteRows(ctx, table, in) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WriteRows under NBE must complete; got: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatalf("WriteRows under NBE HUNG (Bug 178 regression: LOAD DATA framing not NBE-invariant, or the encoder-deadlock guard removed)")
	}

	got := readRowsRaw(t, dsn, "SELECT id, s, b FROM nbe_escapes ORDER BY id")
	if len(got) != len(wantRows) {
		t.Fatalf("read back %d rows; want %d", len(got), len(wantRows))
	}
	for i, wr := range wantRows {
		if got[i]["id"].(int64) != wr["id"].(int64) {
			t.Errorf("row[%d] id = %d; want %d", i, got[i]["id"], wr["id"])
		}
		if !valEqual(got[i]["s"], wr["s"]) {
			t.Errorf("row[%d] s = %#v; want %#v", i, got[i]["s"], wr["s"])
		}
		if !valEqual(got[i]["b"], wr["b"]) {
			t.Errorf("row[%d] b = %#v; want %#v", i, got[i]["b"], wr["b"])
		}
	}
}

// --- helpers ---------------------------------------------------------

func openRowWriter(t *testing.T, ctx context.Context, dsn string) ir.RowWriter {
	t.Helper()
	rw, err := Engine{}.OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	return rw
}

// mustBeLoadData asserts the writer is configured for the LOAD DATA
// strategy. Failing early here makes the diagnosis "test ran the wrong
// path" fall out of test output instead of debug-by-symptom.
func mustBeLoadData(t *testing.T, rw ir.RowWriter) {
	t.Helper()
	concrete, ok := rw.(*RowWriter)
	if !ok {
		t.Fatalf("RowWriter is not a *RowWriter: %T", rw)
	}
	if concrete.bulkLoad != ir.BulkLoadLoadDataInfile {
		t.Fatalf("RowWriter.bulkLoad = %v; want LoadDataInfile (vanilla flavor should declare it)", concrete.bulkLoad)
	}
}

func enableLocalInfile(t *testing.T, dsn string) {
	t.Helper()
	setSessionVar(t, dsn, "SET GLOBAL local_infile = 1")
}

func disableLocalInfile(t *testing.T, dsn string) {
	t.Helper()
	setSessionVar(t, dsn, "SET GLOBAL local_infile = 0")
}

// setSessionVar runs a SET GLOBAL via the root account. The
// testcontainers mysql module's default `test` user lacks
// SYSTEM_VARIABLES_ADMIN; rewriting the DSN to root keeps the test
// surface narrow without a separate WithConfigFile fixture.
func setSessionVar(t *testing.T, dsn, stmt string) {
	t.Helper()
	rootDSN := rewriteDSNToRoot(t, dsn)
	db, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}

// rewriteDSNToRoot swaps the `<user>:<pass>@` prefix of a `user:pass@tcp(...)`
// DSN to `root:<pass>@`. The testcontainers mysql module's
// WithDefaultCredentials sets MYSQL_ROOT_PASSWORD equal to the
// configured user password, so the same password works for both
// accounts.
func rewriteDSNToRoot(t *testing.T, dsn string) string {
	t.Helper()
	at := strings.Index(dsn, "@")
	colon := strings.Index(dsn, ":")
	if at < 0 || colon < 0 || colon >= at {
		t.Fatalf("rewriteDSNToRoot: unexpected DSN shape %q", dsn)
	}
	return "root" + dsn[colon:]
}

// readRowsRaw runs a SELECT and returns each row as map[col]any.
// Used to validate writes without taking a dependency on the IR
// reader's value-shaping; that path has its own coverage and we want
// the writer test to assert wire-form fidelity.
func readRowsRaw(t *testing.T, dsn, query string) []map[string]any {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rs, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rs.Close()

	colNames, err := rs.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}

	var out []map[string]any
	for rs.Next() {
		holders := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range holders {
			ptrs[i] = &holders[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		row := make(map[string]any, len(colNames))
		for i, c := range colNames {
			row[c] = holders[i]
		}
		out = append(out, row)
	}
	if err := rs.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}

func rowValueEqualLD(got, want any) bool {
	if gt, ok := got.(time.Time); ok {
		if wt, ok := want.(time.Time); ok {
			return gt.Equal(wt)
		}
		return false
	}
	return reflect.DeepEqual(got, want)
}

// valEqual handles two minor quirks of database/sql's raw scan path:
//   - A nullable string column comes back as []byte; compare against
//     the want's string form.
//   - An empty []byte and nil are distinct in MySQL but database/sql
//     happens to scan an empty VARBINARY as []byte{} so direct equality
//     works.
func valEqual(got, want any) bool {
	if want == nil {
		return got == nil
	}
	switch w := want.(type) {
	case string:
		if gb, ok := got.([]byte); ok {
			return string(gb) == w
		}
		return got == want
	case []byte:
		if gb, ok := got.([]byte); ok {
			return reflect.DeepEqual(gb, w)
		}
		return false
	}
	return reflect.DeepEqual(got, want)
}
