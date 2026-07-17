// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// TestDumpIngest_StageDirHonored pins the --stage-dir thread for the
// `.sql`-dump materialize (roadmap item 72 leftover): with
// [Engine.WithStageDir] set, the materialized temp DB is created under the
// named directory (and removed on Close, same lifecycle as the default), and
// a MISSING stage dir refuses loudly naming the flag — never a silent
// fallback to the system temp dir (the flatfile staging posture).
func TestDumpIngest_StageDirHonored(t *testing.T) {
	ctx := context.Background()

	t.Run("staged copy lands under the stage dir", func(t *testing.T) {
		stage := t.TempDir()
		path := writeDump(t, "d1export.sql", d1LikeDump)
		eng := Engine{}.WithStageDir(stage)
		sr, err := eng.OpenSchemaReader(ctx, path)
		if err != nil {
			t.Fatalf("OpenSchemaReader: %v", err)
		}
		scr := sr.(*SchemaReader)
		if got := filepath.Dir(scr.tempPath); got != stage {
			_ = scr.Close()
			t.Fatalf("materialized temp DB dir = %q; want the stage dir %q", got, stage)
		}
		tempPath := scr.tempPath
		if err := scr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
			t.Errorf("staged temp DB %q should be removed after Close; stat err = %v", tempPath, err)
		}
	})

	t.Run("missing stage dir refuses naming --stage-dir", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "does-not-exist")
		path := writeDump(t, "d1export.sql", d1LikeDump)
		eng := Engine{}.WithStageDir(missing)
		_, err := eng.OpenSchemaReader(ctx, path)
		if err == nil || !strings.Contains(err.Error(), "--stage-dir") {
			t.Fatalf("open with a missing --stage-dir = %v; want a loud refusal naming the flag", err)
		}
	})

	t.Run("binary .db source ignores the stage dir (nothing materializes)", func(t *testing.T) {
		stage := t.TempDir()
		dbPath := filepath.Join(t.TempDir(), "real.db")
		seed, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open seed db: %v", err)
		}
		if _, err := seed.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_ = seed.Close()
		eng := Engine{}.WithStageDir(stage)
		sr, err := eng.OpenSchemaReader(ctx, dbPath)
		if err != nil {
			t.Fatalf("OpenSchemaReader: %v", err)
		}
		scr := sr.(*SchemaReader)
		if scr.tempPath != "" {
			_ = scr.Close()
			t.Fatalf("binary .db source materialized %q; want no temp DB", scr.tempPath)
		}
		if err := scr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
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

// TestDumpIngest_CountRowsCloseNoRace pins the fix for the data race the -race
// CI gate caught on the dump path: the fire-and-forget ETA probe
// ([kickOffRowCount] → CountRows → chunkDisqualified) reads tempPath while the
// table-pool's deferred Close runs. The original Close cleared tempPath, racing
// that read; Close now removes the temp file via a sync.Once WITHOUT mutating
// tempPath. This test races CountRows against Close on a dump-materialized
// reader; it fails the -race detector if tempPath is ever written post-
// construction. (Runs under the unit-test -race job; no Docker needed.)
func TestDumpIngest_CountRowsCloseNoRace(t *testing.T) {
	path := writeDump(t, "d1export.sql", d1LikeDump)
	eng := Engine{}
	ctx := context.Background()

	rr, err := eng.OpenRowReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	rrd := rr.(*RowReader)
	if rrd.tempPath == "" {
		t.Fatal("row reader tempPath empty; expected a materialized dump")
	}
	// A dump source is chunk-disqualified, so CountRows reads tempPath (via
	// chunkDisqualified) and returns (0, nil) — exactly the concurrent read
	// that raced Close's write. The table need only carry a name.
	tbl := &ir.Table{Name: "users"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = rrd.CountRows(ctx, tbl)
		}
	}()
	// Close concurrently with the probe loop; both touch tempPath.
	if err := rrd.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	wg.Wait()
	// Probe after Close too (the ETA goroutine can outlive the copy/Close).
	if _, err := rrd.CountRows(ctx, tbl); err != nil {
		t.Errorf("CountRows after Close: %v", err)
	}
	// Close stays idempotent.
	if err := rrd.Close(); err != nil {
		t.Errorf("second Close: %v", err)
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

// TestStreamMaterializeDump_CrossBlockAndTransaction pins the streaming
// materializer: a `sqlite3 .dump`-shaped script (wrapped in a single
// BEGIN TRANSACTION … COMMIT, with embedded ';' in strings, an escaped quote,
// line + block comments, and a multi-line CREATE) must materialize IDENTICALLY
// no matter how small the read block is — proving (a) statements/strings/
// comments that span block boundaries split correctly, and (b) the wrapping
// transaction COMMITS (all rows present), which is the exact case a per-chunk-
// process loader gets wrong (chunk-1 rollback → "no such table"). Block sizes of
// 1/3/7 force a boundary mid-token everywhere.
func TestStreamMaterializeDump_CrossBlockAndTransaction(t *testing.T) {
	const dump = `PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE t (
	id INTEGER PRIMARY KEY,
	v  TEXT
);
INSERT INTO t VALUES (1, 'a;b');  -- trailing; comment
INSERT INTO t VALUES (2, 'it''s; ok'); /* block ; comment */
INSERT INTO t VALUES (3, NULL);
COMMIT;
`
	ctx := context.Background()
	for _, bs := range []int{1, 3, 7, 64, 1 << 20} {
		t.Run("block="+itoa(bs), func(t *testing.T) {
			tmp := filepath.Join(t.TempDir(), "out.db")
			db, err := sql.Open("sqlite", tmp)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("conn: %v", err)
			}
			defer func() { _ = conn.Close() }()

			if err := streamMaterializeDump(ctx, conn, strings.NewReader(dump), bs); err != nil {
				t.Fatalf("streamMaterializeDump(block=%d): %v", bs, err)
			}

			// The BEGIN…COMMIT data must have persisted — count==3 fails if the
			// transaction was rolled back (the pscale-style bug).
			var n int
			if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil {
				t.Fatalf("count (block=%d): %v", bs, err)
			}
			if n != 3 {
				t.Fatalf("rows = %d; want 3 (transaction did not commit?)", n)
			}
			// Embedded ';' and escaped-quote values must round-trip exactly.
			var v1, v2 string
			if err := conn.QueryRowContext(ctx, "SELECT v FROM t WHERE id=1").Scan(&v1); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRowContext(ctx, "SELECT v FROM t WHERE id=2").Scan(&v2); err != nil {
				t.Fatal(err)
			}
			if v1 != "a;b" {
				t.Errorf("id=1 v = %q; want %q", v1, "a;b")
			}
			if v2 != "it's; ok" {
				t.Errorf("id=2 v = %q; want %q", v2, "it's; ok")
			}
			var v3 sql.NullString
			if err := conn.QueryRowContext(ctx, "SELECT v FROM t WHERE id=3").Scan(&v3); err != nil {
				t.Fatal(err)
			}
			if v3.Valid {
				t.Errorf("id=3 v = %q; want NULL", v3.String)
			}
		})
	}
}

// itoa is a tiny int→string for subtest names (avoids an strconv import here).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
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
