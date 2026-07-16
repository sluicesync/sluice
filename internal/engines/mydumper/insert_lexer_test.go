// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLiteralToRowValue_FamilyMatrix is the Bug-74 pin for the dump value
// decode: EVERY value family × the literal shapes a mydumper/pscale dump
// can carry for it, asserted against the docs/value-types.md runtime
// contract. A representative type is not enough — the driver-shape
// preconditioning dispatches per IR type family, so each family gets its
// own rows.
func TestLiteralToRowValue_FamilyMatrix(t *testing.T) {
	str := func(s string) literal { return literal{kind: litString, bytes: []byte(s)} }
	num := func(s string) literal { return literal{kind: litNumber, text: s} }
	hexb := func(b ...byte) literal { return literal{kind: litHex, bytes: b} }
	bitb := func(b ...byte) literal { return literal{kind: litBit, bytes: b} }

	cases := []struct {
		name string
		typ  ir.Type
		lit  literal
		want any
	}{
		// ---- Integer family: exact decimal-text parsing, never a float ----
		{"int64/positive", ir.Integer{Width: 64}, num("42"), int64(42)},
		{"int64/negative", ir.Integer{Width: 64}, num("-9223372036854775808"), int64(-9223372036854775808)},
		{"int64/max", ir.Integer{Width: 64}, num("9223372036854775807"), int64(9223372036854775807)},
		// > 2^53: the exact class a float round-trip silently corrupts (D1 lesson)
		{"int64/gt-2^53", ir.Integer{Width: 64}, num("9007199254740993"), int64(9007199254740993)},
		{"uint64/max", ir.Integer{Width: 64, Unsigned: true}, num("18446744073709551615"), uint64(18446744073709551615)},
		{"uint64/below-int64-max-stays-int64", ir.Integer{Width: 64, Unsigned: true}, num("7"), int64(7)},
		{"int8", ir.Integer{Width: 8}, num("-128"), int64(-128)},

		// ---- Boolean (tinyint(1) numbers; BIT(1) raw bytes in all three
		// quoted shapes — a quoted "\x00" must be FALSE via the bytes
		// branch, not "non-empty string" true (real-dump oracle catch) ----
		{"bool/1", ir.Boolean{}, num("1"), true},
		{"bool/0", ir.Boolean{}, num("0"), false},
		{"bool/bit-byte-0", ir.Boolean{}, str("\x00"), false},
		{"bool/bit-byte-1", ir.Boolean{}, str("\x01"), true},
		{"bool/bit-literal", ir.Boolean{}, bitb(0x00), false},
		{"bool/hex", ir.Boolean{}, hexb(0x01), true},

		// ---- Decimal: text passthrough, precision preserved ----
		{"decimal/quoted", ir.Decimal{Precision: 20, Scale: 4}, str("12345.6789"), "12345.6789"},
		{"decimal/bare", ir.Decimal{Precision: 20, Scale: 4}, num("-0.0001"), "-0.0001"},
		{"decimal/uint64-range", ir.Decimal{Precision: 20}, num("18446744073709551615"), "18446744073709551615"},

		// ---- Float family ----
		{"float/single", ir.Float{Precision: ir.FloatSingle}, num("8388608"), float64(8388608)},
		{"double/exp", ir.Float{Precision: ir.FloatDouble}, num("-1.25e10"), float64(-1.25e10)},
		{"double/quoted", ir.Float{Precision: ir.FloatDouble}, str("2.5"), float64(2.5)},

		// ---- Temporal family (incl. fractional seconds) ----
		{
			"datetime",
			ir.DateTime{},
			str("2026-01-02 03:04:05"),
			time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		{
			"datetime/frac",
			ir.DateTime{Precision: 6},
			str("2026-01-02 03:04:05.123456"),
			time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC),
		},
		{
			"timestamp/frac",
			ir.Timestamp{Precision: 6, WithTimeZone: true},
			str("2001-02-03 04:05:06.5"),
			time.Date(2001, 2, 3, 4, 5, 6, 500000000, time.UTC),
		},
		{"date", ir.Date{}, str("1999-12-31"), time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"time/string-contract", ir.Time{}, str("838:59:59"), "838:59:59"},
		{"time/frac", ir.Time{Precision: 6}, str("-01:02:03.000004"), "-01:02:03.000004"},

		// ---- String family: escape decoding is exercised in
		// TestScanValue_EscapeSequences; here the family contract ----
		{"varchar", ir.Varchar{Length: 20}, str("héllo"), "héllo"},
		{"char", ir.Char{Length: 4}, str("ab"), "ab"},
		{"text", ir.Text{Size: ir.TextLong}, str("body"), "body"},
		// the string 'NULL' is data, not SQL NULL
		{"varchar/NULL-string", ir.Varchar{Length: 10}, str("NULL"), "NULL"},

		// ---- Binary family: BOTH dump shapes ----
		{"blob/hex-blob", ir.Blob{Size: ir.BlobRegular}, hexb(0x00, 0xFF, 0x27, 0x1A), []byte{0x00, 0xFF, 0x27, 0x1A}},
		{"varbinary/escaped", ir.Varbinary{Length: 8}, str("\x00\xff'\x1a"), []byte{0x00, 0xFF, 0x27, 0x1A}},
		{"binary/hex", ir.Binary{Length: 2}, hexb(0xAB, 0xCD), []byte{0xAB, 0xCD}},
		{"blob/empty-vs-null", ir.Blob{}, str(""), []byte{}},

		// ---- JSON ----
		{"json", ir.JSON{Binary: true}, str(`{"a": [1, 2]}`), []byte(`{"a": [1, 2]}`)},

		// ---- ENUM / SET ----
		{"enum", ir.Enum{Values: []string{"active", "inactive"}}, str("active"), "active"},
		{"set/two", ir.Set{Values: []string{"a", "b", "c"}}, str("a,c"), []string{"a", "c"}},
		{"set/empty", ir.Set{Values: []string{"a", "b"}}, str(""), []string{}},

		// ---- BIT: every literal shape that can carry it ----
		{"bit/bit-literal", ir.Bit{Length: 5}, bitb(0x15), "10101"},
		{"bit/hex-blob", ir.Bit{Length: 5}, hexb(0x15), "10101"},
		{"bit/escaped-bytes", ir.Bit{Length: 8}, str("\x1a"), "00011010"},
		{"bit/bare-number", ir.Bit{Length: 5}, num("21"), "10101"},

		// ---- Geometry: dump carries <SRID LE><WKB>; IR wants bare WKB ----
		{
			"geometry/hex",
			ir.Geometry{Subtype: ir.GeometryPoint},
			hexb(0xE6, 0x10, 0x00, 0x00, 0x01, 0x01, 0x02),
			[]byte{0x01, 0x01, 0x02},
		},
		{
			"geometry/escaped",
			ir.Geometry{Subtype: ir.GeometryPoint},
			str("\xe6\x10\x00\x00\x01\x01\x02"),
			[]byte{0x01, 0x01, 0x02},
		},

		// ---- NULL is nil for every family ----
		{"null/int", ir.Integer{Width: 32}, literal{kind: litNull}, nil},
		{"null/text", ir.Text{}, literal{kind: litNull}, nil},
		{"null/blob", ir.Blob{}, literal{kind: litNull}, nil},
		{"null/temporal", ir.Timestamp{}, literal{kind: litNull}, nil},
		{"null/set", ir.Set{Values: []string{"a"}}, literal{kind: litNull}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := &ir.Column{Name: "c", Type: tc.typ, Nullable: true}
			got, err := literalToRowValue(tc.lit, col)
			if err != nil {
				t.Fatalf("literalToRowValue: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v (%T); want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

// TestLiteralToRowValue_Refusals pins the loud-failure side: literal-kind ×
// type pairings outside the faithful matrix refuse, never coerce.
func TestLiteralToRowValue_Refusals(t *testing.T) {
	cases := []struct {
		name string
		typ  ir.Type
		lit  literal
	}{
		{"int/overflow", ir.Integer{Width: 64}, literal{kind: litNumber, text: "9223372036854775808"}},
		{
			"int/unsigned-overflow",
			ir.Integer{Width: 64, Unsigned: true},
			literal{kind: litNumber, text: "18446744073709551616"},
		},
		{"int/hex-literal", ir.Integer{Width: 32}, literal{kind: litHex, bytes: []byte{0x01}}},
		{"float/garbage", ir.Float{}, literal{kind: litString, bytes: []byte("wat")}},
		{"temporal/number", ir.Date{}, literal{kind: litNumber, text: "20260102"}},
		{"temporal/zero-date", ir.Date{}, literal{kind: litString, bytes: []byte("0000-00-00")}},
		{"temporal/partial-date", ir.DateTime{}, literal{kind: litString, bytes: []byte("2026-00-15 00:00:00")}},
		{"blob/number", ir.Blob{}, literal{kind: litNumber, text: "5"}},
		{"decimal/hex", ir.Decimal{}, literal{kind: litHex, bytes: []byte{0x01}}},
		// Boolean quoted bytes: only the single wire byte 0x00/0x01 is
		// faithful — the TEXT digit '0' (0x30) would silently invert to
		// true through a lenient any-non-zero-byte branch.
		{"bool/text-zero-refused", ir.Boolean{}, literal{kind: litString, bytes: []byte("0")}},
		{"bool/text-one-refused", ir.Boolean{}, literal{kind: litString, bytes: []byte("1")}},
		{"bool/empty-refused", ir.Boolean{}, literal{kind: litString, bytes: []byte{}}},
		{"bool/multibyte-refused", ir.Boolean{}, literal{kind: litHex, bytes: []byte{0x00, 0x01}}},
		// BIT width overflow: more significant bits than the declared
		// BIT(N) would silently drop the high bits in BitBytesToString.
		{"bit/overflow-bitlit", ir.Bit{Length: 5}, literal{kind: litBit, bytes: []byte{0x3F}}},
		{"bit/overflow-hex", ir.Bit{Length: 5}, literal{kind: litHex, bytes: []byte{0xFF}}},
		{"bit/overflow-number", ir.Bit{Length: 5}, literal{kind: litNumber, text: "63"}},
		{"bit/overflow-wide-bytes", ir.Bit{Length: 8}, literal{kind: litHex, bytes: []byte{0x01, 0x00}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := &ir.Column{Name: "c", Type: tc.typ, Nullable: true}
			if _, err := literalToRowValue(tc.lit, col); err == nil {
				t.Fatalf("want a loud refusal for %s literal on %T; got nil error", tc.lit.kind, tc.typ)
			}
		})
	}
}

// TestScanValue_EscapeSequences pins the full MySQL string-literal escape
// set — the binary-fidelity contract the pscale no-hex-blob shape rides on
// (ADR-0161 §4). Each escape is exercised alone AND inside surrounding
// bytes.
func TestScanValue_EscapeSequences(t *testing.T) {
	cases := []struct {
		src  string
		want []byte
	}{
		{`'\0'`, []byte{0x00}},
		{`'\''`, []byte{'\''}},
		{`'\"'`, []byte{'"'}},
		{`'\b'`, []byte{0x08}},
		{`'\n'`, []byte{0x0A}},
		{`'\r'`, []byte{0x0D}},
		{`'\t'`, []byte{0x09}},
		{`'\Z'`, []byte{0x1A}},
		{`'\\'`, []byte{'\\'}},
		// MySQL KEEPS the backslash for the LIKE-pattern escapes.
		{`'\%'`, []byte(`\%`)},
		{`'\_'`, []byte(`\_`)},
		// SQL-standard doubled quote
		{`''''`, []byte{'\''}},
		// unknown escape drops the backslash
		{`'\q'`, []byte{'q'}},
		// everything mixed, with raw high bytes riding through untouched
		{"'a\\0b\\'c''d\\\\e\xf0\x9f\x8d\x8af'", append([]byte("a\x00b'c'd\\e"), append([]byte("\xf0\x9f\x8d\x8a"), 'f')...)},
		// --- the DOUBLE-quoted twin (mydumper ≥1.0's default emit shape;
		// decoded via the delimiter-aware scanner since Bug 191) ---
		{`"\0"`, []byte{0x00}},
		{`"\""`, []byte{'"'}},
		{`"\'"`, []byte{'\''}},
		{`"\n"`, []byte{0x0A}},
		{`"\Z"`, []byte{0x1A}},
		{`"\\"`, []byte{'\\'}},
		{`"\%"`, []byte(`\%`)},
		{`"\_"`, []byte(`\_`)},
		{`""""`, []byte{'"'}}, // SQL-standard doubled quote, "-flavoured
		{`"\q"`, []byte{'q'}},
		{`""`, []byte{}},           // empty value
		{`"it's"`, []byte("it's")}, // raw single quote rides through
		{"\"a\\0b\\\"c\"\"d\\\\e\xf0\x9f\x8d\x8af\"", append([]byte("a\x00b\"c\"d\\e"), append([]byte("\xf0\x9f\x8d\x8a"), 'f')...)},
	}
	for _, tc := range cases {
		t.Run(tc.src, func(t *testing.T) {
			sc := &insertScan{s: tc.src, file: "test"}
			lit, err := sc.scanValue()
			if err != nil {
				t.Fatalf("scanValue(%q): %v", tc.src, err)
			}
			if lit.kind != litString {
				t.Fatalf("kind = %v; want string", lit.kind)
			}
			if !reflect.DeepEqual(lit.bytes, tc.want) {
				t.Fatalf("bytes = %q; want %q", lit.bytes, tc.want)
			}
			if sc.i != len(tc.src) {
				t.Fatalf("cursor stopped at %d of %d", sc.i, len(tc.src))
			}
		})
	}
}

// TestParseInsert_TupleLexing pins the tuple-lexing edges: separators and
// quotes inside strings, multi-row extended INSERTs, both header forms,
// and trailing-content refusals.
func TestParseInsert_TupleLexing(t *testing.T) {
	table := &ir.Table{Name: "t", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "s", Type: ir.Text{}, Nullable: true},
	}}

	t.Run("separators-inside-strings", func(t *testing.T) {
		stmt := "INSERT INTO `t` VALUES (1,'a,b),(''c'' \\'d\\''),(2,NULL)"
		rows := lexAllRows(t, stmt, table)
		if len(rows) != 2 {
			t.Fatalf("rows = %d; want 2", len(rows))
		}
		if got := rows[0]["s"]; got != "a,b),('c' 'd'" {
			t.Fatalf("row0 s = %q", got)
		}
		if got := rows[1]["s"]; got != nil {
			t.Fatalf("row1 s = %#v; want nil", got)
		}
	})

	t.Run("column-list-reordered", func(t *testing.T) {
		stmt := "INSERT INTO `t` (`s`,`id`) VALUES ('x',7)"
		rows := lexAllRows(t, stmt, table)
		if rows[0]["id"] != int64(7) || rows[0]["s"] != "x" {
			t.Fatalf("row = %#v", rows[0])
		}
	})

	t.Run("insert-ignore-and-replace", func(t *testing.T) {
		for _, stmt := range []string{
			"INSERT IGNORE INTO `t` VALUES (1,'a')",
			"REPLACE INTO `t` VALUES (1,'a')",
		} {
			if rows := lexAllRows(t, stmt, table); len(rows) != 1 {
				t.Fatalf("%q: rows = %d; want 1", stmt, len(rows))
			}
		}
	})

	t.Run("db-qualified-table", func(t *testing.T) {
		stmt := "INSERT INTO `db`.`t` VALUES (1,'a')"
		sc, name, _, err := parseInsertHeader(stmt, "test")
		if err != nil {
			t.Fatalf("parseInsertHeader: %v", err)
		}
		if name != "t" {
			t.Fatalf("table = %q; want t", name)
		}
		_ = sc
	})

	t.Run("mydumper-v1-real-shape", func(t *testing.T) {
		// Byte-for-byte the statement shape mydumper v1.0.3 emits
		// (ground-truthed against the real tool): no space after VALUES,
		// `_binary "…"` introducers, double-quoted strings with backslash
		// escapes, newline + leading-comma tuple separators.
		wide := &ir.Table{Name: "corpus", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Varbinary{Length: 16}, Nullable: true},
			{Name: "s", Type: ir.Varchar{Length: 32}, Nullable: true},
			{Name: "ts", Type: ir.Timestamp{Precision: 6, WithTimeZone: true}, Nullable: true},
			{Name: "e", Type: ir.Enum{Values: []string{"a", "b"}}, Nullable: true},
		}}
		stmt := "INSERT INTO `corpus` VALUES(1,_binary \"\\0\xff\\Z\\'\",\"it\\'s\\na\"," +
			"_binary \"2026-01-02 03:04:05.123456\",\"a\")\n,(2,NULL,\"NULL\",NULL,\"b\")\n"
		rows := lexAllRows(t, stmt, wide)
		if len(rows) != 2 {
			t.Fatalf("rows = %d; want 2", len(rows))
		}
		if !reflect.DeepEqual(rows[0]["b"], []byte{0x00, 0xFF, 0x1A, '\''}) {
			t.Fatalf("b = %#v", rows[0]["b"])
		}
		if rows[0]["s"] != "it's\na" {
			t.Fatalf("s = %#v", rows[0]["s"])
		}
		if rows[0]["ts"] != time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC) {
			t.Fatalf("ts = %#v", rows[0]["ts"])
		}
		if rows[1]["s"] != "NULL" || rows[1]["ts"] != nil {
			t.Fatalf("row1 = %#v", rows[1])
		}
	})

	t.Run("convert-wrapped-json", func(t *testing.T) {
		// mydumper ≥1.0 wraps JSON values: CONVERT("…" USING UTF8MB4)
		// (ground-truthed against v1.0.3).
		wide := &ir.Table{Name: "j", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "js", Type: ir.JSON{Binary: true}, Nullable: true},
		}}
		stmt := "INSERT INTO `j` VALUES(1,CONVERT(\"{\\\"a\\\": [1, true]}\" USING UTF8MB4))\n,(2,NULL)\n"
		rows := lexAllRows(t, stmt, wide)
		if len(rows) != 2 {
			t.Fatalf("rows = %d; want 2", len(rows))
		}
		if !reflect.DeepEqual(rows[0]["js"], []byte(`{"a": [1, true]}`)) {
			t.Fatalf("js = %q", rows[0]["js"])
		}
		if rows[1]["js"] != nil {
			t.Fatalf("row1 js = %#v", rows[1]["js"])
		}
	})

	t.Run("convert-foreign-charset-refused", func(t *testing.T) {
		stmt := "INSERT INTO `t` (`id`,`s`) VALUES (1,CONVERT(\"x\" USING latin1))"
		if _, err := tryLexRows(stmt, table); err == nil ||
			!strings.Contains(err.Error(), "latin1") {
			t.Fatalf("want a CONVERT charset refusal; got %v", err)
		}
	})

	t.Run("foreign-introducer-refused", func(t *testing.T) {
		stmt := "INSERT INTO `t` (`id`,`s`) VALUES (1,_latin1\"x\")"
		if _, err := tryLexRows(stmt, table); err == nil ||
			!strings.Contains(err.Error(), "_latin1") {
			t.Fatalf("want a _latin1 introducer refusal; got %v", err)
		}
	})

	t.Run("tuple-arity-mismatch", func(t *testing.T) {
		stmt := "INSERT INTO `t` VALUES (1)"
		if _, err := tryLexRows(stmt, table); err == nil ||
			!strings.Contains(err.Error(), "expects 2") {
			t.Fatalf("want an arity refusal; got %v", err)
		}
	})

	t.Run("trailing-garbage-refused", func(t *testing.T) {
		stmt := "INSERT INTO `t` VALUES (1,'a') ON DUPLICATE KEY UPDATE s='b'"
		if _, err := tryLexRows(stmt, table); err == nil {
			t.Fatal("want a trailing-content refusal; got nil")
		}
	})

	t.Run("unknown-column-refused", func(t *testing.T) {
		stmt := "INSERT INTO `t` (`id`,`nope`) VALUES (1,'a')"
		if _, err := tryLexRows(stmt, table); err == nil {
			t.Fatal("want an unknown-column refusal; got nil")
		}
	})

	t.Run("missing-column-refused", func(t *testing.T) {
		// Omitting a non-generated column would silently land target
		// defaults where the source had values.
		stmt := "INSERT INTO `t` (`id`) VALUES (1)"
		if _, err := tryLexRows(stmt, table); err == nil {
			t.Fatal("want a coverage refusal; got nil")
		}
	})
}

// lexAllRows drives header+tuple lexing for one statement, mapping tuples
// through the table's columns exactly as the RowReader does.
func lexAllRows(t *testing.T, stmt string, table *ir.Table) []ir.Row {
	t.Helper()
	rows, err := tryLexRows(stmt, table)
	if err != nil {
		t.Fatalf("lexAllRows(%q): %v", stmt, err)
	}
	return rows
}

func tryLexRows(stmt string, table *ir.Table) ([]ir.Row, error) {
	sc, _, columns, err := parseInsertHeader(stmt, "test")
	if err != nil {
		return nil, err
	}
	targets, err := resolveInsertColumns(table, columns)
	if err != nil {
		return nil, err
	}
	var (
		out  []ir.Row
		vals []literal
	)
	for {
		var done bool
		vals, done, err = sc.nextTuple(vals)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
		if len(vals) != len(targets) {
			return nil, fmt.Errorf("tuple has %d values; table %s expects %d", len(vals), table.Name, len(targets))
		}
		row := make(ir.Row, len(targets))
		for i, col := range targets {
			v, err := literalToRowValue(vals[i], col)
			if err != nil {
				return nil, err
			}
			row[col.Name] = v
		}
		out = append(out, row)
	}
}
