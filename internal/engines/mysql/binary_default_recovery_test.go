// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the binary column-default recovery path (Finding C): the
// SHOW CREATE TABLE re-read that works around information_schema's NUL
// truncation of BINARY/VARBINARY literal defaults. These don't need a live
// MySQL — they cover the pure parse + gate helpers. The end-to-end round-trip
// (SHOW CREATE re-read → emitter → byte-exact target default) is pinned by the
// integration tests.

package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDecodeMySQLQuotedString pins the escaped-string → bytes decoder against
// EVERY form SHOW CREATE TABLE was observed to emit on MySQL 8.0 (Phase A
// ground truth), plus the documented-but-not-emitted escapes the decoder also
// accepts for robustness. The load-bearing cases are the escapes MySQL DOES
// emit: `\0 \b \t \n \r \\` and the doubled single-quote; every other byte < 0x80 is
// emitted raw.
func TestDecodeMySQLQuotedString(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []byte
	}{
		// --- forms MySQL emits (Phase A) ---
		{"single NUL", `'\0'`, []byte{0x00}},
		{"backspace + NUL", "'\\b\\0'", []byte{0x08, 0x00}},
		{"tab + NUL", "'\\t\\0'", []byte{0x09, 0x00}},
		{"newline + NUL", "'\\n\\0'", []byte{0x0A, 0x00}},
		{"CR + NUL", "'\\r\\0'", []byte{0x0D, 0x00}},
		{"backslash + NUL (escaped)", `'\\\0'`, []byte{0x5C, 0x00}},
		{"single-quote + NUL (doubled quote)", `'''\0'`, []byte{0x27, 0x00}},
		{"printable ASCII", `'ABC'`, []byte{0x41, 0x42, 0x43}},
		{"timestamp string (v0.99.186 well-formed case)", `'19700101000000'`, []byte("19700101000000")},
		{"ab NUL-padded to BINARY(8)", "'ab\\0\\0\\0\\0\\0\\0'", []byte{0x61, 0x62, 0, 0, 0, 0, 0, 0}},
		// Raw control/other bytes MySQL emits WITHOUT escaping.
		{"raw ctrl-Z (0x1A) + NUL", "'\x1a\\0'", []byte{0x1A, 0x00}},
		{"raw double-quote + NUL", "'\"\\0'", []byte{0x22, 0x00}},
		{"raw SOH (0x01) + NUL", "'\x01\\0'", []byte{0x01, 0x00}},
		{"raw DEL (0x7F) + NUL", "'\x7f\\0'", []byte{0x7F, 0x00}},
		{"raw space + NUL", "' \\0'", []byte{0x20, 0x00}},
		{"raw percent + NUL", "'%\\0'", []byte{0x25, 0x00}},
		{"raw underscore + NUL", "'_\\0'", []byte{0x5F, 0x00}},
		{"three raw low controls", "'\x01\x02\x03'", []byte{0x01, 0x02, 0x03}},
		// --- documented escapes the decoder also accepts (robustness) ---
		{"escaped ctrl-Z \\Z", `'\Z'`, []byte{0x1A}},
		{"escaped quote \\'", `'\''`, []byte{0x27}},
		{"escaped double-quote", `'\"'`, []byte{0x22}},
		{"unknown escape drops backslash", `'\x'`, []byte{'x'}},
		// MySQL KEEPS the backslash for the LIKE-pattern escapes \% and \_
		// (manual: string-literal escape table) — load-bearing for the
		// mydumper flat-file decode (ADR-0161 §4), pinned here beside the
		// decoder it guards.
		{"LIKE-escape percent keeps backslash", `'\%'`, []byte(`\%`)},
		{"LIKE-escape underscore keeps backslash", `'\_'`, []byte(`\_`)},
		{"LIKE-escapes embedded", `'a\%b\_c'`, []byte(`a\%b\_c`)},
		// Trailing text after the closing quote is ignored.
		{"trailing comma ignored", `'ABC',`, []byte{0x41, 0x42, 0x43}},
		{"empty string", `''`, []byte{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := decodeMySQLQuotedString(c.in)
			if !ok {
				t.Fatalf("decodeMySQLQuotedString(%q) ok=false; want %x", c.in, c.want)
			}
			if !bytes.Equal(got, c.want) {
				t.Errorf("decodeMySQLQuotedString(%q) = %x; want %x", c.in, got, c.want)
			}
		})
	}
}

func TestDecodeMySQLQuotedString_Malformed(t *testing.T) {
	cases := []struct{ name, in string }{
		{"no opening quote", `ABC'`},
		{"no closing quote", `'ABC`},
		{"dangling backslash", `'ab\`},
		{"empty input", ``},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, ok := decodeMySQLQuotedString(c.in); ok {
				t.Errorf("decodeMySQLQuotedString(%q) ok=true; want false", c.in)
			}
		})
	}
}

// TestScanQuotedStringDelim_Matrix pins the delimiter-generalized scanner
// (the Bug-191 rewrite) across BOTH delimiters × every escape shape — the
// class, not one representative: the single-quote form is the
// information_schema/pscale shape, the double-quote form is mydumper ≥1.0's
// default emit shape, and the rewrite split decoding into an escape-free
// fast path and an escape-aware path, so each shape is pinned under both
// delimiters with the reported end index asserted too.
func TestScanQuotedStringDelim_Matrix(t *testing.T) {
	// Cases are written for delim '\'' with Q standing for the delimiter
	// and D for the other quote char; the runner materializes both forms.
	cases := []struct {
		name    string
		in      string // Q = delimiter, D = the other quote char
		want    string // decoded bytes, same placeholders
		wantEnd int    // -1 = len(in)
	}{
		// escape-free fast path
		{"plain", "QabcQ", "abc", -1},
		{"empty", "QQ", "", -1},
		{"raw NUL/newline/high bytes", "Qa\x00\nb\xf0\x9f\x8d\x8a\xffQ", "a\x00\nb\xf0\x9f\x8d\x8a\xff", -1},
		{"raw other quote char", "QaDbQ", "aDb", -1},
		{"trailing text ignored", "QabcQ,(2,", "abc", 5},
		// escape-aware path
		{"backslash escapes", `Q\0\b\t\n\r\Z\\Q`, "\x00\x08\x09\x0A\x0D\x1A\\", -1},
		{"escaped delimiter", `Qa\Qb\DcQ`, "aQbDc", -1},
		{"doubled delimiter", "QaQQbQ", "aQb", -1},
		{"only a doubled delimiter", "QQQQ", "Q", -1},
		{"unknown escape drops backslash", `Q\qQ`, "q", -1},
		{"LIKE escapes keep backslash", `Qa\%b\_cQ`, `a\%b\_c`, -1},
		{"escape then trailing text", `Qa\QbQ)`, "aQb", 6},
	}
	for _, delim := range []byte{'\'', '"'} {
		other := byte('"')
		if delim == '"' {
			other = '\''
		}
		expand := func(s string) string {
			s = strings.ReplaceAll(s, "Q", string(delim))
			return strings.ReplaceAll(s, "D", string(other))
		}
		scan := ScanQuotedString
		if delim == '"' {
			scan = ScanDoubleQuotedString
		}
		for _, c := range cases {
			t.Run(fmt.Sprintf("delim_%c/%s", delim, c.name), func(t *testing.T) {
				in, want := expand(c.in), expand(c.want)
				wantEnd := c.wantEnd
				if wantEnd == -1 {
					wantEnd = len(in)
				}
				raw, end, ok := scan(in)
				if !ok {
					t.Fatalf("scan(%q) ok=false; want %q", in, want)
				}
				if string(raw) != want || end != wantEnd {
					t.Errorf("scan(%q) = %q, end %d; want %q, end %d", in, raw, end, want, wantEnd)
				}
			})
		}
		// Malformed inputs refuse under both delimiters.
		for _, c := range []struct{ name, in string }{
			{"unterminated", "Qabc"},
			{"dangling backslash", `Qab\`},
			{"unterminated via escaped closer", `Qab\Q`},
			{"wrong opener", "DabcD"},
			{"empty input", ""},
		} {
			t.Run(fmt.Sprintf("delim_%c/malformed_%s", delim, c.name), func(t *testing.T) {
				in := expand(c.in)
				if _, _, ok := scan(in); ok {
					t.Errorf("scan(%q) ok=true; want false", in)
				}
			})
		}
	}
}

// TestScanQuotedString_BufferSizedToValue is the Bug-191 regression pin at
// the allocation level: decoding a small value from a HUGE remaining
// statement tail must size the output to the VALUE, not the tail. Before
// the fix the capacity below was len(s) (~1 MiB per value), which made the
// mydumper flat-file read O(rows × statement_size).
func TestScanQuotedString_BufferSizedToValue(t *testing.T) {
	tail := "'value'," + strings.Repeat("x", 1<<20)
	for name, scan := range map[string]func(string) ([]byte, int, bool){
		"single": ScanQuotedString,
		"double": func(s string) ([]byte, int, bool) {
			return ScanDoubleQuotedString(strings.ReplaceAll(s, "'", `"`))
		},
	} {
		t.Run(name+"/fast-path", func(t *testing.T) {
			raw, _, ok := scan(tail)
			if !ok || string(raw) != "value" {
				t.Fatalf("scan = %q, ok=%v", raw, ok)
			}
			// The allocator rounds to a size class, so pin "value-sized,
			// nowhere near tail-sized" rather than the exact length.
			if cap(raw) > 64 {
				t.Errorf("cap = %d; want ≤ 64 (buffer must be sized to the value, not the ~1 MiB tail)", cap(raw))
			}
		})
		t.Run(name+"/escape-path", func(t *testing.T) {
			raw, _, ok := scan(`'va\nlue',` + strings.Repeat("x", 1<<20))
			if !ok || string(raw) != "va\nlue" {
				t.Fatalf("scan = %q, ok=%v", raw, ok)
			}
			if cap(raw) > 64 {
				t.Errorf("cap = %d; want ≤ 64 (buffer must be sized to the literal body, not the ~1 MiB tail)", cap(raw))
			}
		})
	}
}

// TestDecodeMySQLHexToken pins the `0x<hex>` decoder, including the leading-NUL
// and trailing-NUL bytes that make this the recoverable form. Trailing text is
// stopped at the first non-hex byte.
func TestDecodeMySQLHexToken(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []byte
	}{
		{"leading NUL multi-byte", "0x00FF00FF", []byte{0x00, 0xFF, 0x00, 0xFF}},
		{"all high bytes", "0xFFEEDD", []byte{0xFF, 0xEE, 0xDD}},
		{"trailing NUL", "0xFF00", []byte{0xFF, 0x00}},
		{"padded BINARY(8)", "0x00FF000000000000", []byte{0x00, 0xFF, 0, 0, 0, 0, 0, 0}},
		{"lowercase digits", "0xdeadbeef", []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{"uppercase X prefix", "0XFF00", []byte{0xFF, 0x00}},
		{"stops at trailing comma", "0xFFEE,", []byte{0xFF, 0xEE}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := decodeMySQLHexToken(c.in)
			if !ok {
				t.Fatalf("decodeMySQLHexToken(%q) ok=false; want %x", c.in, c.want)
			}
			if !bytes.Equal(got, c.want) {
				t.Errorf("decodeMySQLHexToken(%q) = %x; want %x", c.in, got, c.want)
			}
		})
	}
}

func TestDecodeMySQLHexToken_Malformed(t *testing.T) {
	for _, in := range []string{"0x", "0xABC" /* odd */, "0x,"} {
		if _, ok := decodeMySQLHexToken(in); ok {
			t.Errorf("decodeMySQLHexToken(%q) ok=true; want false", in)
		}
	}
}

// TestParseShowCreateColumnDefault pins the per-column extraction from a
// realistic SHOW CREATE TABLE body — including prefix-collision safety
// (`log` must not match `log_ts`), the hex and quoted forms side by side, a
// column with no DEFAULT, and a padded fixed-width case.
func TestParseShowCreateColumnDefault(t *testing.T) {
	const createStmt = "CREATE TABLE `m` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `log` binary(2) DEFAULT 0xFF00,\n" +
		"  `log_ts` binary(14) NOT NULL DEFAULT '19700101000000',\n" +
		"  `lead_nul` binary(4) DEFAULT 0x00FF00FF,\n" +
		"  `single_nul` binary(1) DEFAULT '\\0',\n" +
		"  `padded` binary(8) DEFAULT 'ab\\0\\0\\0\\0\\0\\0',\n" +
		"  `quote_nul` binary(2) DEFAULT '''\\0',\n" +
		"  `commented` binary(2) DEFAULT 0xAB00 COMMENT 'has DEFAULT in text',\n" +
		"  `esc` binary(2) DEFAULT '\\t\\\\',\n" +
		"  `x DEFAULT 0xAA` binary(2) DEFAULT 0xCC00,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

	cases := []struct {
		col     string
		want    []byte
		wantOK  bool
		comment string
	}{
		{"log", []byte{0xFF, 0x00}, true, "hex form, prefix of log_ts"},
		{"log_ts", []byte("19700101000000"), true, "quoted ASCII"},
		{"lead_nul", []byte{0x00, 0xFF, 0x00, 0xFF}, true, "leading-NUL hex, the bare-0x truncation case"},
		{"single_nul", []byte{0x00}, true, "single NUL quoted"},
		{"padded", []byte{0x61, 0x62, 0, 0, 0, 0, 0, 0}, true, "BINARY(8) padded"},
		{"quote_nul", []byte{0x27, 0x00}, true, "doubled-quote + NUL"},
		{"commented", []byte{0xAB, 0x00}, true, "COMMENT after DEFAULT must not confuse the parse"},
		{"esc", []byte{0x09, 0x5C}, true, "tab + backslash escapes decoded end-to-end"},
		{"x DEFAULT 0xAA", []byte{0xCC, 0x00}, true, "name containing ' DEFAULT ' + hex must not mislocate the keyword"},
		{"id", nil, false, "no DEFAULT clause"},
		{"absent", nil, false, "column not present"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.col, func(t *testing.T) {
			got, ok := parseShowCreateColumnDefault(createStmt, c.col)
			if ok != c.wantOK {
				t.Fatalf("ok=%v; want %v (%s)", ok, c.wantOK, c.comment)
			}
			if c.wantOK && !bytes.Equal(got, c.want) {
				t.Errorf("%s: got %x; want %x", c.comment, got, c.want)
			}
		})
	}
}

// TestBinaryLiteralDefaultNeedsRecovery pins the detection gate. The critical
// property (broader than the original Finding C statement): the gate fires for
// EVERY binary-family hex-literal default, not just the bare `0x` — because a
// mid/trailing-NUL truncation (0x2700 → `0x27`) is a WELL-FORMED but wrong
// literal that cannot be distinguished from a genuine short value in
// information_schema, so it too must be re-read.
func TestBinaryLiteralDefaultNeedsRecovery(t *testing.T) {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	cases := []struct {
		name  string
		typ   ir.Type
		extra string
		def   sql.NullString
		want  bool
	}{
		{"binary bare 0x (leading-NUL truncation)", ir.Binary{Length: 4}, "", valid("0x"), true},
		{"binary short well-formed hex (trailing-NUL truncation)", ir.Binary{Length: 2}, "", valid("0xFF"), true},
		{"binary short well-formed hex (mid-NUL truncation)", ir.Binary{Length: 2}, "", valid("0x27"), true},
		{"binary faithful hex (no NUL)", ir.Binary{Length: 3}, "", valid("0xFFEEDD"), true},
		{"varbinary hex", ir.Varbinary{Length: 20}, "", valid("0x68656C6C6F"), true},
		{"binary uppercase 0X prefix", ir.Binary{Length: 2}, "", valid("0XFF00"), true},
		// Excluded cases:
		{"NULL default", ir.Binary{Length: 4}, "", sql.NullString{Valid: false}, false},
		{"binary expression default (DEFAULT_GENERATED)", ir.Binary{Length: 16}, "DEFAULT_GENERATED", valid("(uuid_to_bin(uuid()))"), false},
		{"varbinary empty-string default (no 0x prefix)", ir.Varbinary{Length: 4}, "", valid(""), false},
		{"varchar default that looks like hex", ir.Varchar{Length: 20}, "", valid("0x1234"), false},
		{"integer default", ir.Integer{}, "", valid("0"), false},
		{"blob is not binary-family here", ir.Blob{}, "", valid("0xFF"), false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := binaryLiteralDefaultNeedsRecovery(c.typ, c.extra, c.def); got != c.want {
				t.Errorf("binaryLiteralDefaultNeedsRecovery(%T, %q, %q) = %v; want %v",
					c.typ, c.extra, c.def.String, got, c.want)
			}
		})
	}
}

// TestParseShowCreateTableComment pins the table-level COMMENT extraction from
// SHOW CREATE output: the NUL-bearing comment that TABLE_COMMENT truncates (the
// bug), the escaped forms MySQL emits, the "no comment" case, and the two
// false-match traps — a COLUMN-level COMMENT (must NOT be picked up) and a table
// comment whose TEXT contains `COMMENT=` (the first ` COMMENT=` is still the real
// option opener).
func TestParseShowCreateTableComment(t *testing.T) {
	head := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	cases := []struct {
		name    string
		stmt    string
		want    string
		wantOK  bool
		comment string
	}{
		{
			name:    "NUL-bearing comment (the truncation bug)",
			stmt:    head + " COMMENT='a\\0b'",
			want:    "a\x00b",
			wantOK:  true,
			comment: "0x00 escaped as \\0 by SHOW CREATE; TABLE_COMMENT would truncate to 'a'",
		},
		{"plain comment", head + " COMMENT='hello world'", "hello world", true, "ordinary ASCII"},
		{"doubled quote", head + " COMMENT='it''s here'", "it's here", true, "SQL-standard doubled quote"},
		{"newline + tab escapes", head + " COMMENT='line1\\nline2\\tend'", "line1\nline2\tend", true, "control escapes decoded"},
		{
			name:    "comment text contains COMMENT=",
			stmt:    head + " COMMENT='see COMMENT=''x'''",
			want:    "see COMMENT='x'",
			wantOK:  true,
			comment: "first ` COMMENT=` is the real option; the embedded one is inside the quoted value",
		},
		{
			name:    "trailing table option after COMMENT",
			stmt:    head + " COMMENT='c' ROW_FORMAT=DYNAMIC",
			want:    "c",
			wantOK:  true,
			comment: "decoder stops at the closing quote; trailing options ignored",
		},
		{"no comment", head, "", false, "no COMMENT clause on the closing line"},
		{
			name: "column COMMENT must not be matched",
			stmt: "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `c` int DEFAULT NULL COMMENT 'col cmt',\n" +
				"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			want:    "",
			wantOK:  false,
			comment: "column COMMENT is `COMMENT 'x'` (space, no =) on a column line; only the `)` line is inspected",
		},
		{
			name: "column COMMENT present AND table COMMENT present",
			stmt: "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `c` int DEFAULT NULL COMMENT 'col cmt',\n" +
				"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='table cmt'",
			want:    "table cmt",
			wantOK:  true,
			comment: "only the table-level comment is returned",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseShowCreateTableComment(c.stmt)
			if ok != c.wantOK {
				t.Fatalf("ok=%v; want %v (%s)", ok, c.wantOK, c.comment)
			}
			if c.wantOK && got != c.want {
				t.Errorf("%s: got %q; want %q", c.comment, got, c.want)
			}
		})
	}
}

// TestTablesNeedingShowCreate pins the amortization gate: a table pays the extra
// SHOW CREATE ONLY when it has a binary default to recover or a non-empty
// comment. The common no-comment / no-binary-default table is skipped, a table
// needing both is fetched once, and the result is sorted+deduped.
func TestTablesNeedingShowCreate(t *testing.T) {
	tables := map[string]*ir.Table{
		"plain":         {Name: "plain"},                      // no comment, no pending → skipped
		"commented":     {Name: "commented", Comment: "hi"},   // comment → fetch
		"bindefault":    {Name: "bindefault"},                 // pending only → fetch
		"both":          {Name: "both", Comment: "c"},         // comment + pending → fetch once
		"empty_comment": {Name: "empty_comment", Comment: ""}, // empty comment → skipped
	}
	pendingCols := map[string][]*ir.Column{
		"bindefault": {{Name: "b"}},
		"both":       {{Name: "b"}},
	}
	got := tablesNeedingShowCreate(tables, pendingCols)
	want := []string{"bindefault", "both", "commented"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tablesNeedingShowCreate = %v; want %v (plain/empty_comment must be skipped; both deduped)", got, want)
	}
}

func TestBytesToHexLiteral(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
	}{
		{[]byte{0x00, 0xFF, 0x00, 0xFF}, "0x00FF00FF"},
		{[]byte{0x27, 0x00}, "0x2700"},
		{[]byte{0x61, 0x62, 0, 0, 0, 0, 0, 0}, "0x6162000000000000"},
		{[]byte("19700101000000"), "0x3139373030313031303030303030"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.want, func(t *testing.T) {
			if got := bytesToHexLiteral(c.in); got != c.want {
				t.Errorf("bytesToHexLiteral(%x) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}
