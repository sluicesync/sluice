// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestDecodeBinlogRow_ConvertsNonUTF8CharsetColumns pins GC-37 (j) at the
// binlog decoder: a character column's row-image bytes are in the column's
// OWN charset and must reach the IR as UTF-8 — the expected strings are the
// server's own conversions, measured on MySQL 8.0.46 (charset_decode_cdc_
// integration_test.go grades the same class on a live server).
func TestDecodeBinlogRow_ConvertsNonUTF8CharsetColumns(t *testing.T) {
	cases := []struct {
		name string
		typ  ir.Type
		raw  any
		want string
	}{
		{"latin1 é (invalid-UTF-8 bytes)", ir.Varchar{Length: 16, Charset: "latin1"}, []byte{0xE9}, "é"},
		{"latin1 Ã© (bytes that happen to be valid UTF-8)", ir.Varchar{Length: 16, Charset: "latin1"}, "\xC3\xA9", "Ã©"},
		{"latin1 0x81 is MySQL's C1 control", ir.Char{Length: 4, Charset: "latin1"}, []byte{0x81}, "\u0081"},
		{"cp1251 C3A9", ir.Text{Charset: "cp1251"}, []byte{0xC3, 0xA9}, "Г©"},
		{"gbk C3A9", ir.Varchar{Length: 16, Charset: "gbk"}, []byte{0xC3, 0xA9}, "茅"},
		{"swe7 is not ASCII-compatible", ir.Varchar{Length: 16, Charset: "swe7"}, []byte{0x7B}, "ä"},
		{"utf16 é", ir.Varchar{Length: 16, Charset: "utf16"}, []byte{0x00, 0xE9}, "é"},
		{"utf16 surrogate pair", ir.Varchar{Length: 16, Charset: "utf16"}, []byte{0xD8, 0x3D, 0xDE, 0x00}, "😀"},
		{"tis620", ir.Varchar{Length: 16, Charset: "tis620"}, []byte{0xA1}, "ก"},
		{"big5 pure ASCII", ir.Varchar{Length: 16, Charset: "big5"}, []byte("plain"), "plain"},
		{"utf8mb4 passes through", ir.Varchar{Length: 16, Charset: "utf8mb4"}, "😀", "😀"},
		{"no charset recorded passes through", ir.Varchar{Length: 16}, "x", "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := []*ir.Column{{Name: "c", Type: tc.typ}}
			row, err := decodeBinlogRow([]any{tc.raw}, cols, nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{}, nil)
			if err != nil {
				t.Fatalf("decodeBinlogRow: %v", err)
			}
			if row["c"] != tc.want {
				t.Errorf("c = %q (%X); want %q", row["c"], row["c"], tc.want)
			}
		})
	}
}

// TestBinlogColumnCharsets_WrittenCharsetWins pins review F1 at the unit
// level: the TABLE_MAP's per-column collation (what the bytes were WRITTEN
// in) decides the decode even where the catalog disagrees, an ID nothing
// names refuses as a CDC-4 replay mismatch, and an absent TABLE_MAP
// collation (MariaDB NO_LOG) falls back to the catalog, where the
// charset-DDL guard takes over.
func TestBinlogColumnCharsets_WrittenCharsetWins(t *testing.T) {
	tbl := &tableSchema{Schema: "d", Name: "t", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "v", Type: ir.Varchar{Length: 16, Charset: "latin1"}},
	}}
	tm := func(cols ...uint64) *replication.TableMapEvent {
		return &replication.TableMapEvent{
			ColumnCount:   2,
			ColumnType:    []byte{gomysql.MYSQL_TYPE_LONG, gomysql.MYSQL_TYPE_VARCHAR},
			ColumnMeta:    []uint16{0, 16},
			ColumnCharset: cols,
		}
	}

	cs, err := binlogColumnCharsets(tbl, tm(8), nil) // latin1_swedish_ci, as the catalog says
	if err != nil || cs[1] == nil || cs[1].name != "latin1" {
		t.Fatalf("matching TABLE_MAP: cs=%v err=%v; want latin1", cs, err)
	}
	row, err := decodeBinlogRow([]any{int32(1), []byte{0xE9}}, tbl.Columns, nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{}, cs)
	if err != nil || row["v"] != "é" {
		t.Fatalf("decode by the written latin1 = %q, %v; want é", row["v"], err)
	}

	// Written utf8mb4 (255), catalog now latin1 — a replay across the DDL:
	// review item 4, the WRITTEN charset decides, value-exact, no refusal.
	cs, err = binlogColumnCharsets(tbl, tm(255), nil)
	if err != nil {
		t.Fatalf("replayed utf8mb4 under a latin1 catalog: err = %v; want the written charset used", err)
	}
	row, err = decodeBinlogRow([]any{int32(1), "\xC3\xA9"}, tbl.Columns, nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{}, cs)
	if err != nil || row["v"] != "é" {
		t.Fatalf("utf8mb4 C3A9 under a latin1 catalog decoded as %q, %v; want é (the written charset)", row["v"], err)
	}

	// No TABLE_MAP charsets (MariaDB NO_LOG): fall back to the catalog.
	if cs, err := binlogColumnCharsets(tbl, tm(), nil); err != nil || cs != nil {
		t.Fatalf("no TABLE_MAP charsets: cs=%v err=%v; want nil (decode by the catalog)", cs, err)
	}

	// An ID only the server's own table names (MariaDB utf8mb4_uca1400_ai_ci)
	// decodes by that charset.
	cs, err = binlogColumnCharsets(tbl, tm(2304), func(id uint64) string {
		if id == 2304 {
			return "utf8mb4"
		}
		return ""
	})
	if err != nil || cs[1] != nil { // utf8mb4 is the passthrough (nil converter)
		t.Fatalf("a server-named utf8mb4 collation: cs=%v err=%v; want the utf8mb4 passthrough", cs, err)
	}

	// A written collation NO table names: the evidence exists and cannot be
	// read, so it refuses rather than falling back to the catalog.
	var ce *sluicecode.CodedError
	if _, err := binlogColumnCharsets(tbl, tm(9999), func(uint64) string { return "" }); !errors.As(err, &ce) ||
		ce.Code != sluicecode.CodeCDCSchemaReplayMismatch || !strings.Contains(err.Error(), "9999") {
		t.Fatalf("an unnameable written collation: err = %v; want %s naming the ID", err, sluicecode.CodeCDCSchemaReplayMismatch)
	}
}

// TestDecodeBinlogRow_RefusesWhatItCannotDecode: a big5 value that is not
// pure ASCII (no library matches MySQL's big5 table) and a byte sequence
// invalid in its declared charset refuse, naming the table, column and
// charset — never a guess, never the raw bytes. NULL stays NULL.
func TestDecodeBinlogRow_RefusesWhatItCannotDecode(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  ir.Type
		raw  any
	}{
		{"big5 non-ASCII", ir.Varchar{Length: 16, Charset: "big5"}, []byte{0xA4, 0xA4}},
		{"sjis truncated lead byte", ir.Varchar{Length: 16, Charset: "sjis"}, []byte{0x82}},
		{"tis620 hole", ir.Varchar{Length: 16, Charset: "tis620"}, []byte{0xDB}},
		{"a charset sluice has no table for", ir.Varchar{Length: 16, Charset: "klingon"}, []byte("a")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cols := []*ir.Column{{Name: "c", Type: tc.typ}}
			_, err := decodeBinlogRow([]any{tc.raw}, cols, nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{}, nil)
			if !errors.Is(err, errCharsetNotDecodable) || !strings.Contains(err.Error(), `table "t" column "c"`) {
				t.Fatalf("err = %v; want %s naming the table and column", err, charsetNotDecodableMarker)
			}
		})
	}
	row, err := decodeBinlogRow([]any{nil}, []*ir.Column{{Name: "c", Type: ir.Varchar{Length: 1, Charset: "latin1"}}},
		nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{}, nil)
	if err != nil || row["c"] != nil {
		t.Fatalf("NULL latin1 = %#v, %v; want nil", row["c"], err)
	}
}

// TestDecodeVStreamRow_CharsetByCollation pins the VStream arm: a
// character cell is converted by its field's collation; collation 0 — what
// vttablet sends for a charset it does not name (gbk, big5, tis620,
// gb18030; MEASURED) — passes pure ASCII and refuses anything else; a
// non-UTF-8 ENUM/SET cell is resolved against the column's own labels in
// both the CDC (UTF-8) and COPY (stored bytes) forms.
func TestDecodeVStreamRow_CharsetByCollation(t *testing.T) {
	row := func(vals ...[]byte) *query.Row {
		r := &query.Row{}
		for _, v := range vals {
			r.Lengths = append(r.Lengths, int64(len(v)))
			r.Values = append(r.Values, v...)
		}
		return r
	}
	latin1 := &query.Field{Name: "c", Type: query.Type_VARCHAR, Charset: 8}
	unnamed := &query.Field{Name: "c", Type: query.Type_VARCHAR, Charset: 0}
	enumLatin1 := &query.Field{Name: "e", Type: query.Type_ENUM, Charset: 8}

	got, _, err := decodeVStreamRow(row([]byte{0xE9}), []*query.Field{latin1}, "t", zeroDateInherit)
	if err != nil || got["c"] != "é" {
		t.Errorf("latin1 0xE9 = %q, %v; want é", got["c"], err)
	}
	got, _, err = decodeVStreamRow(row([]byte("ascii")), []*query.Field{unnamed}, "t", zeroDateInherit)
	if err != nil || got["c"] != "ascii" {
		t.Errorf("collation-0 ASCII = %q, %v; want ascii", got["c"], err)
	}
	if _, _, err := decodeVStreamRow(row([]byte{0xD6, 0xD0}), []*query.Field{unnamed}, "t", zeroDateInherit); !errors.Is(err, errCharsetNotDecodable) || !strings.Contains(err.Error(), `table "t"`) {
		t.Errorf("collation-0 non-ASCII: err = %v; want %s naming the table", err, charsetNotDecodableMarker)
	}
	got, _, err = decodeVStreamRow(row([]byte("é")), []*query.Field{enumLatin1}, "t", zeroDateInherit)
	if err != nil || got["e"] != "é" {
		t.Errorf("latin1 ENUM label with no parseable column_type = %q, %v; want é unconverted", got["e"], err)
	}

	// Review F2: a non-UTF-8 ENUM/SET cell is UTF-8 label text in CDC and
	// the STORED bytes in COPY; both must resolve to the member label, and
	// a cell that is a member under both readings refuses.
	enumCT := &query.Field{Name: "e", Type: query.Type_ENUM, Charset: 8, ColumnType: "enum('é','x')"}
	setCT := &query.Field{Name: "s", Type: query.Type_SET, Charset: 8, ColumnType: "set('é','x')"}
	for _, tc := range []struct {
		name  string
		field *query.Field
		raw   []byte
		want  any
	}{
		{"ENUM CDC form (UTF-8 label)", enumCT, []byte("é"), "é"},
		{"ENUM COPY form (stored latin1)", enumCT, []byte{0xE9}, "é"},
		{"SET CDC form", setCT, []byte("é,x"), []string{"é", "x"}},
		{"SET COPY form", setCT, []byte{0xE9, ',', 'x'}, []string{"é", "x"}},
		{"ENUM index 0 ('')", enumCT, []byte{}, ""},
	} {
		got, _, err := decodeVStreamRow(row(tc.raw), []*query.Field{tc.field}, "t", zeroDateInherit)
		if err != nil || fmt.Sprint(got[tc.field.Name]) != fmt.Sprint(tc.want) {
			t.Errorf("%s = %#v, %v; want %#v", tc.name, got[tc.field.Name], err, tc.want)
		}
	}
	amb := &query.Field{Name: "e", Type: query.Type_ENUM, Charset: 8, ColumnType: "enum('é','Ã©')"}
	if _, _, err := decodeVStreamRow(row([]byte{0xC3, 0xA9}), []*query.Field{amb}, "t", zeroDateInherit); !errors.Is(err, errCharsetNotDecodable) {
		t.Errorf("ambiguous ENUM cell: err = %v; want %s", err, charsetNotDecodableMarker)
	}
	if cc := lookupCollationCharset(9999); cc == nil || !cc.unsupported {
		t.Errorf("an unnamed non-zero collation resolved to %+v; want an unsupported charset that refuses", cc)
	}
}
