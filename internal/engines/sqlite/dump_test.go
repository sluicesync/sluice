// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/ir"
)

// d1LikeDump mirrors the shape of a real `wrangler d1 export` output: a leading
// PRAGMA defer_foreign_keys, D1's internal `_cf_KV` table, two user tables with
// PKs and a FK, a handful of INSERTs, and an embedded ';' inside a string
// literal (the value-path fidelity check the splitter must not break on).
const d1LikeDump = `PRAGMA defer_foreign_keys=TRUE;
CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB);
INSERT INTO _cf_KV (key, value) VALUES ('flags', x'00');
CREATE TABLE users (
	id    INTEGER PRIMARY KEY,
	email TEXT NOT NULL,
	note  TEXT
);
INSERT INTO users (id, email, note) VALUES (1, 'a@example.com', 'hi; there');
INSERT INTO users (id, email, note) VALUES (2, 'b@example.com', NULL);
CREATE TABLE posts (
	id      INTEGER PRIMARY KEY,
	user_id INTEGER NOT NULL,
	body    TEXT,
	FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
INSERT INTO posts (id, user_id, body) VALUES (1, 1, 'first');
INSERT INTO posts (id, user_id, body) VALUES (2, 2, 'second');
`

// writeDump writes content to a temp `.sql` file and returns its path.
func writeDump(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write dump %q: %v", name, err)
	}
	return path
}

// TestDumpIngest_SchemaSkipsCfTables pins ADR-0130: a `.sql` dump opens via the
// engine, the schema reader returns the USER tables and SKIPS D1's `_cf_KV`,
// and the materialized temp DB exists while open and is GONE after Close.
func TestDumpIngest_SchemaSkipsCfTables(t *testing.T) {
	path := writeDump(t, "d1export.sql", d1LikeDump)
	eng := Engine{}
	ctx := context.Background()

	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	scr := sr.(*SchemaReader)

	// The materialized temp DB must exist while the reader is open.
	if scr.tempPath == "" {
		t.Fatal("tempPath empty; a .sql dump must materialize a temp DB")
	}
	if _, err := os.Stat(scr.tempPath); err != nil {
		t.Fatalf("temp DB %q should exist while open: %v", scr.tempPath, err)
	}
	tempPath := scr.tempPath

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	names := map[string]bool{}
	for _, tbl := range schema.Tables {
		names[tbl.Name] = true
	}
	if names["_cf_KV"] {
		t.Error("_cf_KV must be auto-skipped (D1 internal table)")
	}
	if !names["users"] || !names["posts"] {
		t.Errorf("user tables missing; got %v", names)
	}

	if err := scr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The temp DB must be gone after Close.
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Errorf("temp DB %q should be removed after Close; stat err = %v", tempPath, err)
	}
	// Close is idempotent (no error on a second call).
	if err := scr.Close(); err != nil {
		t.Errorf("second Close should be a no-op; got %v", err)
	}
}

// TestDumpIngest_RowsReadAndTempRemoved pins that rows decode correctly through
// the existing path from a materialized dump (incl. the embedded-';' string and
// a NULL), `_cf_KV` is not readable as a user table, and the row reader's temp
// DB is removed on Close.
func TestDumpIngest_RowsReadAndTempRemoved(t *testing.T) {
	path := writeDump(t, "d1export.sql", d1LikeDump)
	eng := Engine{}
	ctx := context.Background()

	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	users := tableByName(schema, "users")
	if users == nil {
		t.Fatal("users table missing")
	}

	rr, err := eng.OpenRowReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	rrd := rr.(*RowReader)
	if rrd.tempPath == "" {
		t.Fatal("row reader tempPath empty; expected a materialized dump")
	}
	tempPath := rrd.tempPath

	ch, err := rr.ReadRows(ctx, users)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var rows []ir.Row
	for row := range ch {
		rows = append(rows, row)
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("Err after read: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("users rows = %d; want 2", len(rows))
	}
	// Row 1: the embedded-';' note must round-trip intact (splitter fidelity).
	if got, _ := rows[0]["note"].(string); got != "hi; there" {
		t.Errorf("users[0].note = %#v; want %q", rows[0]["note"], "hi; there")
	}
	if got, _ := rows[0]["email"].(string); got != "a@example.com" {
		t.Errorf("users[0].email = %#v; want a@example.com", rows[0]["email"])
	}
	// Row 2: NULL note preserved as nil, not a coerced empty string.
	if rows[1]["note"] != nil {
		t.Errorf("users[1].note = %#v; want nil", rows[1]["note"])
	}

	if err := rrd.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Errorf("temp DB %q should be removed after Close; stat err = %v", tempPath, err)
	}
}

// TestDumpIngest_BinaryDBOpensUnchanged pins the magic-sniff routing: a REAL
// binary `.db` (seeded via modernc) opens with NO temp materialized — the
// pre-ADR-0130 path is untouched.
func TestDumpIngest_BinaryDBOpensUnchanged(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO t (id, v) VALUES (1, 'hello')`,
	)
	eng := Engine{}
	ctx := context.Background()

	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	scr := sr.(*SchemaReader)
	if scr.tempPath != "" {
		t.Errorf("binary .db must NOT materialize a temp DB; tempPath = %q", scr.tempPath)
	}
	if err := scr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rr, err := eng.OpenRowReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	rrd := rr.(*RowReader)
	if rrd.tempPath != "" {
		t.Errorf("binary .db row reader must NOT materialize a temp DB; tempPath = %q", rrd.tempPath)
	}
	_ = rrd.Close()
}

// TestDumpIngest_MalformedDumpLoudNoTempLeak pins ADR-0130 §5: a malformed dump
// (truncated CREATE TABLE) fails LOUDLY at OpenSchemaReader/OpenRowReader,
// naming the dump, and leaves NO temp file behind.
func TestDumpIngest_MalformedDumpLoudNoTempLeak(t *testing.T) {
	const bad = "CREATE TABLE oops ("
	path := writeDump(t, "bad.sql", bad)
	eng := Engine{}
	ctx := context.Background()

	before := tempDBCount(t)

	for _, open := range []struct {
		name string
		fn   func() error
	}{
		{"OpenSchemaReader", func() error { _, err := eng.OpenSchemaReader(ctx, path); return err }},
		{"OpenRowReader", func() error { _, err := eng.OpenRowReader(ctx, path); return err }},
	} {
		err := open.fn()
		if err == nil {
			t.Fatalf("%s: want a loud error on a malformed dump, got nil", open.name)
		}
		// Check the basename: the full path is %q-quoted in the error, which
		// doubles backslashes on Windows, but the basename has none.
		if !strings.Contains(err.Error(), filepath.Base(path)) {
			t.Errorf("%s error %q must name the dump %q", open.name, err, path)
		}
		if !strings.Contains(err.Error(), "materialize") {
			t.Errorf("%s error %q should mention materialize", open.name, err)
		}
	}

	// No sluice-sqlite-*.db temp file may have leaked.
	if after := tempDBCount(t); after != before {
		t.Errorf("temp DB leak: %d sluice-sqlite-*.db before, %d after", before, after)
	}
}

// tempDBCount counts leftover sluice-sqlite-*.db files in os.TempDir(), the
// leak detector for the malformed-dump path.
func tempDBCount(t *testing.T) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "sluice-sqlite-*.db"))
	if err != nil {
		t.Fatalf("glob temp dir: %v", err)
	}
	return len(matches)
}

// TestSniffSQLiteBinary pins the magic-header sniff over real files: a seeded
// binary DB sniffs binary; a text dump and a too-short file sniff non-binary; a
// missing file is a loud error.
func TestSniffSQLiteBinary(t *testing.T) {
	bin := seedDB(t, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	if ok, err := sniffSQLiteBinary(bin); err != nil || !ok {
		t.Errorf("sniff(binary) = %v, %v; want true, nil", ok, err)
	}

	dump := writeDump(t, "d.sql", d1LikeDump)
	if ok, err := sniffSQLiteBinary(dump); err != nil || ok {
		t.Errorf("sniff(dump) = %v, %v; want false, nil", ok, err)
	}

	short := writeDump(t, "short.sql", "DROP TABLE x;") // < 16 bytes is fine too
	if ok, err := sniffSQLiteBinary(short); err != nil || ok {
		t.Errorf("sniff(short) = %v, %v; want false, nil", ok, err)
	}
	tiny := writeDump(t, "tiny.sql", "hi")
	if ok, err := sniffSQLiteBinary(tiny); err != nil || ok {
		t.Errorf("sniff(tiny) = %v, %v; want false, nil", ok, err)
	}

	if _, err := sniffSQLiteBinary(filepath.Join(t.TempDir(), "nope.sql")); err == nil {
		t.Error("sniff(missing) should return a read error")
	}
}

// TestSplitSQLStatements pins the fallback splitter respects string literals,
// quoted identifiers, and comments so a ';' inside any of them does not split.
func TestSplitSQLStatements(t *testing.T) {
	script := `CREATE TABLE "weird;name" (v TEXT); -- a; comment
INSERT INTO "weird;name" (v) VALUES ('a;b'); /* block; comment */
INSERT INTO "weird;name" (v) VALUES ('it''s; ok');`
	got := splitSQLStatements(script)
	if len(got) != 3 {
		t.Fatalf("split = %d statements; want 3\n%#v", len(got), got)
	}
	if !strings.Contains(got[1], "'a;b'") {
		t.Errorf("statement 2 lost its embedded ';': %q", got[1])
	}
	if !strings.Contains(got[2], "'it''s; ok'") {
		t.Errorf("statement 3 lost its escaped-quote/';': %q", got[2])
	}

	// Empty / whitespace-only input yields no statements.
	if s := splitSQLStatements("   \n  ;;  \n"); len(s) != 0 {
		t.Errorf("split(blank) = %#v; want none", s)
	}
}
