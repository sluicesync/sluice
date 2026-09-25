// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
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
			row, err := decodeBinlogRow([]any{tc.raw}, cols, nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{})
			if err != nil {
				t.Fatalf("decodeBinlogRow: %v", err)
			}
			if row["c"] != tc.want {
				t.Errorf("c = %q (%X); want %q", row["c"], row["c"], tc.want)
			}
		})
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
			_, err := decodeBinlogRow([]any{tc.raw}, cols, nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{})
			if !errors.Is(err, errCharsetNotDecodable) || !strings.Contains(err.Error(), `table "t" column "c"`) {
				t.Fatalf("err = %v; want %s naming the table and column", err, charsetNotDecodableMarker)
			}
		})
	}
	row, err := decodeBinlogRow([]any{nil}, []*ir.Column{{Name: "c", Type: ir.Varchar{Length: 1, Charset: "latin1"}}},
		nil, FlavorVanilla, "t", zeroDateInherit, binlogLabelGuard{})
	if err != nil || row["c"] != nil {
		t.Fatalf("NULL latin1 = %#v, %v; want nil", row["c"], err)
	}
}

// TestDecodeVStreamRow_CharsetByCollation pins the VStream arm: a
// character cell is converted by its field's collation; collation 0 — what
// vttablet sends for a charset it does not name (gbk, big5, tis620,
// gb18030; MEASURED) — passes pure ASCII and refuses anything else; an
// ENUM cell, which vttablet renders as UTF-8 label text, is NOT converted.
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
		t.Errorf("latin1 ENUM label = %q, %v; want é unconverted (vttablet renders labels as UTF-8)", got["e"], err)
	}
	if cc := lookupCollationCharset(9999); cc == nil || !cc.unsupported {
		t.Errorf("an unnamed non-zero collation resolved to %+v; want an unsupported charset that refuses", cc)
	}
}
