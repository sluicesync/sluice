// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestByteaDefaultAsHexBytes pins the read-boundary half of GC-36 item 4:
// a bytea column's literal DEFAULT — which pg_get_expr renders in hex under
// the reader session's pinned bytea_output — reaches the IR as the
// engine-neutral hex-bytes expression, so no writer has to guess whether
// `\x00ab00` is two-and-a-half bytes of text or three bytes of data. Every
// shape that is not the pinned rendering, and every non-bytea column, is
// left exactly as translateDefault produced it.
func TestByteaDefaultAsHexBytes(t *testing.T) {
	bytea := ir.Blob{Size: ir.BlobLong}
	def := func(s string) ir.DefaultValue {
		return translateDefault(sql.NullString{String: s, Valid: true}, false)
	}
	cases := []struct {
		name string
		typ  ir.Type
		raw  string // information_schema.columns.column_default
		want ir.DefaultValue
	}{
		{"hex bytes", bytea, `'\x00ab00'::bytea`, ir.DefaultExpression{Expr: "0x00AB00", Dialect: hexLiteralDialect}},
		{"text-looking bytes", bytea, `'\x616263'::bytea`, ir.DefaultExpression{Expr: "0x616263", Dialect: hexLiteralDialect}},
		{"empty bytea is the empty literal", bytea, `'\x'::bytea`, ir.DefaultLiteral{Value: ""}},
		{"domain over bytea", ir.Domain{Name: "d", BaseType: bytea}, `'\xdead'::bytea`, ir.DefaultExpression{Expr: "0xDEAD", Dialect: hexLiteralDialect}},
		// Not the pinned rendering: left for the writer, which refuses a
		// backslash-bearing bytes literal (BLOB-DEFAULT-LITERAL-ENCODING).
		{"escape format is left alone", bytea, `'\000\253'::bytea`, ir.DefaultLiteral{Value: `\000\253`}},
		{"odd hex is left alone", bytea, `'\xabc'::bytea`, ir.DefaultLiteral{Value: `\xabc`}},
		{"an expression is left alone", bytea, `decode('00ab'::text, 'hex'::text)`, def(`decode('00ab'::text, 'hex'::text)`)},
		// A text column that happens to spell a rendering is text.
		{"text column is untouched", ir.Text{Size: ir.TextLong}, `'\x00ab00'::text`, ir.DefaultLiteral{Value: `\x00ab00`}},
	}
	for _, c := range cases {
		got := byteaDefaultAsHexBytes(c.typ, def(c.raw))
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: byteaDefaultAsHexBytes(%s) = %#v; want %#v", c.name, c.raw, got, c.want)
		}
	}
}
