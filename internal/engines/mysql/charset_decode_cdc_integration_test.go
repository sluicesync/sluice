//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) on the binlog change stream: every non-UTF-8 charset MySQL and
// MariaDB ship × {CHAR, VARCHAR, TEXT, ENUM, SET} × {insert, NULL,
// update-by-text-key, delete-by-text-key}.
//
// # What this pins, and the independent expected value
//
// Before the fix every one of these cells except ascii/utf8 carried the
// column's STORED bytes as a Go string (MEASURED on MySQL 8.0.46 and MariaDB
// 11.4: latin1 'é' as "\xE9", latin1 'Ã©' as 'é', utf16 'é' as "\x00\xE9",
// swe7 'ä' as '{'). The expected value of every cell is the SOURCE SERVER's
// own conversion, `HEX(CONVERT(col USING utf8mb4))`, read on a utf8mb4
// connection — never sluice's table, which is what is under test. The
// keyed UPDATE/DELETE grade the BEFORE image too: a key carried in the
// wrong encoding is a WHERE that matches no target row.

package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// cdcCharsetCase is one charset under test: its sample stored-byte
// sequences (hex), the first also used as the text key and the ENUM/SET
// label.
type cdcCharsetCase struct {
	cs      string
	samples []string
}

// cdcCharsetCases covers every non-UTF-8 charset MySQL 8.0 ships that
// sluice has a faithful table for, with the "silent" shapes — stored bytes
// that happen to be valid UTF-8 (latin1/cp1250/cp1251/gbk C3A9, every swe7
// value) — named explicitly.
var cdcCharsetCases = []cdcCharsetCase{
	{"latin1", []string{"E9", "C3A9", "80", "81"}},
	{"latin2", []string{"E9"}},
	{"latin5", []string{"E9"}},
	{"latin7", []string{"E9"}},
	{"cp1250", []string{"8A", "C3A9"}},
	{"cp1251", []string{"C0", "C3A9"}},
	{"cp1256", []string{"C7"}},
	{"cp1257", []string{"C0"}},
	{"cp850", []string{"82"}},
	{"cp852", []string{"82"}},
	{"cp866", []string{"80"}},
	{"dec8", []string{"C4"}},
	{"geostd8", []string{"C0"}},
	{"greek", []string{"E1"}},
	{"hebrew", []string{"E0"}},
	{"hp8", []string{"C4"}},
	{"keybcs2", []string{"80"}},
	{"koi8r", []string{"C1"}},
	{"koi8u", []string{"C1"}},
	{"macce", []string{"80"}},
	{"macroman", []string{"80"}},
	{"armscii8", []string{"B2"}},
	{"swe7", []string{"7B", "7D"}},
	{"tis620", []string{"A1"}},
	{"sjis", []string{"82A0"}},
	{"cp932", []string{"8740"}},
	{"ujis", []string{"A4A2", "8FB0A1"}},
	{"eucjpms", []string{"A4A2"}},
	{"euckr", []string{"B0A1"}},
	{"gb2312", []string{"D6D0"}},
	{"gbk", []string{"D6D0", "C3A9"}},
	{"gb18030", []string{"81308130", "D6D0"}},
	{"utf16", []string{"00E9", "D83DDE00"}},
	{"utf16le", []string{"E900"}},
	{"utf32", []string{"000000E9", "0001F600"}},
	{"ucs2", []string{"00E9"}},
}

// serverHasCharset reports whether the source server ships cs.
func serverHasCharset(t *testing.T, db *sql.DB, cs string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.character_sets WHERE character_set_name = ?`, cs).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// serverUTF8 is the source server's own UTF-8 for sample bytes in cs.
func serverUTF8(t *testing.T, db *sql.DB, cs, sampleHex string) string {
	t.Helper()
	var h string
	if err := db.QueryRow(fmt.Sprintf("SELECT HEX(CONVERT(_%s X'%s' USING utf8mb4))", cs, sampleHex)).Scan(&h); err != nil {
		t.Fatalf("%s %s: server conversion: %v", cs, sampleHex, err)
	}
	b, _ := hex.DecodeString(h)
	return string(b)
}

// cdcCharsetLane creates one table per charset, then streams an insert, a
// NULL row, a keyed update and a keyed delete per table, grading every
// value and key against the server. drain returns the next n row changes;
// streamErr reports the reader's terminal error.
func cdcCharsetLane(t *testing.T, dsn string, skip map[string]bool, start func(ctx context.Context) (drain func(n int) []ir.Change, streamErr func() error)) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	type prepared struct {
		cdcCharsetCase
		table string
		truth []string // server UTF-8 per sample
	}
	var cases []prepared
	for _, c := range cdcCharsetCases {
		if skip[c.cs] {
			continue
		}
		if !serverHasCharset(t, db, c.cs) {
			t.Logf("%s: not shipped by this server; skipped", c.cs)
			continue
		}
		p := prepared{cdcCharsetCase: c, table: "cs_" + c.cs}
		for _, s := range c.samples {
			p.truth = append(p.truth, serverUTF8(t, db, c.cs, s))
		}
		label := strings.ReplaceAll(p.truth[0], "'", "''")
		ddl := fmt.Sprintf(`CREATE TABLE %s (
			k VARCHAR(16) CHARACTER SET %[2]s NOT NULL PRIMARY KEY,
			c CHAR(6) CHARACTER SET %[2]s NULL,
			v VARCHAR(16) CHARACTER SET %[2]s NULL,
			x TEXT CHARACTER SET %[2]s NULL,
			e ENUM('%[3]s','b') CHARACTER SET %[2]s NULL,
			s SET('%[3]s','b') CHARACTER SET %[2]s NULL,
			cb CHAR(6) CHARACTER SET %[2]s COLLATE %[2]s_bin NULL,
			vb VARCHAR(16) CHARACTER SET %[2]s COLLATE %[2]s_bin NULL,
			xb TEXT CHARACTER SET %[2]s COLLATE %[2]s_bin NULL,
			eb ENUM('%[3]s','b') CHARACTER SET %[2]s COLLATE %[2]s_bin NULL,
			sb SET('%[3]s','b') CHARACTER SET %[2]s COLLATE %[2]s_bin NULL)`, p.table, c.cs, label)
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("%s: create: %v", c.cs, err)
		}
		cases = append(cases, p)
	}
	if len(cases) < 30-len(skip) {
		t.Fatalf("only %d charsets under test; want every non-UTF-8 charset the server ships", len(cases))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	drain, streamErr := start(ctx)

	next := func(what string) ir.Change {
		t.Helper()
		got := drain(1)
		if len(got) != 1 {
			t.Fatalf("%s: no change (stream error: %v)", what, streamErr())
		}
		return got[0]
	}
	wantStr := func(what string, got any, want string) {
		t.Helper()
		if s, ok := got.(string); !ok || s != want {
			t.Errorf("%s = %T %X; want the server's UTF-8 %X (%q)", what, got, fmt.Sprint(got), want, want)
		}
	}
	lit := func(cs, h string) string { return fmt.Sprintf("_%s X'%s'", cs, h) }

	for _, p := range cases {
		s1, s2 := p.samples[0], p.samples[len(p.samples)-1]
		t1, t2 := p.truth[0], p.truth[len(p.truth)-1]
		all := make([]string, 0, len(p.samples))
		for _, s := range p.samples {
			all = append(all, lit(p.cs, s))
		}
		insert := fmt.Sprintf("INSERT INTO %s (k, c, v, x, e, s, cb, vb, xb, eb, sb) VALUES (%s, %s, %s, CONCAT(%s), 1, 1, %s, %s, CONCAT(%s), 1, 1)",
			p.table, lit(p.cs, s1), lit(p.cs, s1), lit(p.cs, s2), strings.Join(all, ","),
			lit(p.cs, s1), lit(p.cs, s2), strings.Join(all, ","))
		if _, err := db.Exec(insert); err != nil {
			t.Fatalf("%s: insert: %v", p.cs, err)
		}
		// The x column's expected value, from the server — never assembled
		// by the test.
		var xh string
		if err := db.QueryRow(fmt.Sprintf("SELECT HEX(CONVERT(x USING utf8mb4)) FROM %s", p.table)).Scan(&xh); err != nil {
			t.Fatal(err)
		}
		xb, _ := hex.DecodeString(xh)

		ins, ok := next(p.cs + " insert").(ir.Insert)
		if !ok {
			t.Fatalf("%s: first change is not an insert", p.cs)
		}
		wantStr(p.cs+" insert k (VARCHAR key)", ins.Row["k"], t1)
		wantStr(p.cs+" insert c (CHAR)", ins.Row["c"], t1)
		wantStr(p.cs+" insert v (VARCHAR)", ins.Row["v"], t2)
		wantStr(p.cs+" insert x (TEXT)", ins.Row["x"], string(xb))
		wantStr(p.cs+" insert e (ENUM)", ins.Row["e"], t1)
		if !reflect.DeepEqual(ins.Row["s"], []string{t1}) {
			t.Errorf("%s insert s (SET) = %#v; want [%q]", p.cs, ins.Row["s"], t1)
		}
		// The `_bin`-collated columns: vttablet types them BINARY/VARBINARY/
		// BLOB (third review, MEASURED); they are text all the same.
		wantStr(p.cs+" insert cb (CHAR _bin)", ins.Row["cb"], t1)
		wantStr(p.cs+" insert vb (VARCHAR _bin)", ins.Row["vb"], t2)
		wantStr(p.cs+" insert xb (TEXT _bin)", ins.Row["xb"], string(xb))
		// And `_bin` ENUM/SET (fourth review): binary-typed on VStream, an
		// index into the catalog's labels on the binlog.
		wantStr(p.cs+" insert eb (ENUM _bin)", ins.Row["eb"], t1)
		if !reflect.DeepEqual(ins.Row["sb"], []string{t1}) {
			t.Errorf("%s insert sb (SET _bin) = %#v; want [%q]", p.cs, ins.Row["sb"], t1)
		}

		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (k) VALUES ('n')", p.table)); err != nil {
			t.Fatal(err)
		}
		nullRow, _ := next(p.cs + " NULL insert").(ir.Insert)
		for _, col := range []string{"c", "v", "x", "e", "s", "cb", "vb", "xb", "eb", "sb"} {
			if nullRow.Row[col] != nil {
				t.Errorf("%s NULL row %s = %#v; want NULL", p.cs, col, nullRow.Row[col])
			}
		}

		// A value the row does not already hold, so the UPDATE writes an
		// event; its expected value is read back from the server.
		if _, err := db.Exec(fmt.Sprintf("UPDATE %s SET v = CONCAT(%s, 'u') WHERE k = %s", p.table, lit(p.cs, s1), lit(p.cs, s1))); err != nil {
			t.Fatal(err)
		}
		var vh string
		if err := db.QueryRow(fmt.Sprintf("SELECT HEX(CONVERT(v USING utf8mb4)) FROM %s WHERE k = %s", p.table, lit(p.cs, s1))).Scan(&vh); err != nil {
			t.Fatal(err)
		}
		vb, _ := hex.DecodeString(vh)
		up, ok := next(p.cs + " update").(ir.Update)
		if !ok {
			t.Fatalf("%s: update is not an ir.Update", p.cs)
		}
		wantStr(p.cs+" update BEFORE k (the WHERE key)", up.Before["k"], t1)
		wantStr(p.cs+" update AFTER v", up.After["v"], string(vb))

		if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s WHERE k = %s", p.table, lit(p.cs, s1))); err != nil {
			t.Fatal(err)
		}
		del, ok := next(p.cs + " delete").(ir.Delete)
		if !ok {
			t.Fatalf("%s: delete is not an ir.Delete", p.cs)
		}
		wantStr(p.cs+" delete BEFORE k (the WHERE key)", del.Before["k"], t1)
	}
}

// cdcCharsetRefusalLane pins the charsets sluice has no faithful table
// for: a pure-ASCII value streams exactly, a non-ASCII one ends the stream
// with CHARSET-NOT-DECODABLE — never raw bytes.
func cdcCharsetRefusalLane(t *testing.T, dsn, cs, nonASCIIHex string, start func(ctx context.Context) (drain func(n int) []ir.Change, streamErr func() error)) {
	t.Helper()
	applyMySQL(t, dsn, fmt.Sprintf("CREATE TABLE refuse_%[1]s (id INT PRIMARY KEY, v VARCHAR(16) CHARACTER SET %[1]s NULL)", cs))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	drain, streamErr := start(ctx)

	applyMySQL(t, dsn, fmt.Sprintf("INSERT INTO refuse_%s VALUES (1, 'plain ascii')", cs))
	got := drain(1)
	if len(got) != 1 {
		t.Fatalf("%s: the ASCII row did not stream (stream error: %v)", cs, streamErr())
	}
	if ins, _ := got[0].(ir.Insert); ins.Row["v"] != "plain ascii" {
		t.Fatalf("%s: ASCII row v = %#v; want \"plain ascii\"", cs, ins.Row["v"])
	}
	applyMySQL(t, dsn, fmt.Sprintf("INSERT INTO refuse_%s VALUES (2, _%s X'%s')", cs, cs, nonASCIIHex))
	if got := drain(1); len(got) != 0 {
		t.Fatalf("%s: a non-ASCII value was emitted as %#v; want a refusal", cs, got)
	}
	if err := streamErr(); err == nil || !strings.Contains(err.Error(), charsetNotDecodableMarker) || !strings.Contains(err.Error(), cs) {
		t.Fatalf("%s: stream error = %v; want %s naming the charset", cs, err, charsetNotDecodableMarker)
	}
}

// binlogStart opens the binlog reader for dsn and returns the lane hooks.
func binlogStart(t *testing.T, dsn string, flavor Flavor) func(ctx context.Context) (func(int) []ir.Change, func() error) {
	return func(ctx context.Context) (func(int) []ir.Change, func() error) {
		rdr, err := Engine{Flavor: flavor}.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		t.Cleanup(func() {
			if c, ok := rdr.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		})
		changes, err := rdr.StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
		return func(n int) []ir.Change { return drainChanges(t, ctx, changes, n, 30*time.Second) },
			func() error { return rdr.(*CDCReader).Err() }
	}
}

func TestCDCCharsetDecode_Binlog_MySQL(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	cdcCharsetLane(t, dsn, nil, binlogStart(t, dsn, FlavorVanilla))
}

func TestCDCCharsetDecode_Binlog_MariaDB(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	cdcCharsetLane(t, dsn, nil, binlogStart(t, dsn, FlavorMariaDB))
}

func TestCDCCharsetDecode_Binlog_Big5Refuses_MySQL(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	cdcCharsetRefusalLane(t, dsn, "big5", "A4A4", binlogStart(t, dsn, FlavorVanilla))
}
