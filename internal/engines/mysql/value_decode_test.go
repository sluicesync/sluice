// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestDecodeValue(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)

	cases := []struct {
		name string
		raw  any
		t    ir.Type
		want any
	}{
		// ---- NULL ----
		{"null int", nil, ir.Integer{Width: 32}, nil},
		{"null string", nil, ir.Varchar{Length: 64}, nil},

		// ---- Boolean ----
		{"bool from int64=0", int64(0), ir.Boolean{}, false},
		{"bool from int64=1", int64(1), ir.Boolean{}, true},
		{"bool from int64=2", int64(2), ir.Boolean{}, true}, // any nonzero
		{"bool from bit zero byte", []byte{0x00}, ir.Boolean{}, false},
		{"bool from bit one byte", []byte{0x01}, ir.Boolean{}, true},
		{"bool from bit set byte", []byte{0x80}, ir.Boolean{}, true},
		// Binlog returns native-width ints rather than database/sql's
		// widened int64. The decoder must accept both paths.
		{"bool from int8=1", int8(1), ir.Boolean{}, true},
		{"bool from int8=0", int8(0), ir.Boolean{}, false},
		{"bool from uint8=1", uint8(1), ir.Boolean{}, true},

		// ---- Integer ----
		{"int64 passthrough", int64(42), ir.Integer{Width: 32}, int64(42)},
		{"uint64 passthrough", uint64(0xffffffffffffffff), ir.Integer{Width: 64, Unsigned: true}, uint64(0xffffffffffffffff)},
		// Binlog narrow-width passthroughs widen to int64/uint64 so
		// downstream consumers see a uniform shape.
		{"int8 widened", int8(7), ir.Integer{Width: 8}, int64(7)},
		{"int16 widened", int16(-300), ir.Integer{Width: 16}, int64(-300)},
		{"int32 widened", int32(70000), ir.Integer{Width: 32}, int64(70000)},
		{"uint32 widened", uint32(70000), ir.Integer{Width: 32, Unsigned: true}, uint64(70000)},

		// ---- Decimal ----
		{"decimal as string", []byte("3.14159"), ir.Decimal{Precision: 6, Scale: 5}, "3.14159"},
		{"decimal already string", "2.71828", ir.Decimal{Precision: 6, Scale: 5}, "2.71828"},
		// BIGINT UNSIGNED overridden to a wide DECIMAL to keep the full
		// unsigned-64 range: go-sql-driver hands back uint64/int64, which
		// must render as exact decimal text (the uint64-no-path finding).
		{"decimal from uint64 max", uint64(18446744073709551615), ir.Decimal{Precision: 20, Scale: 0}, "18446744073709551615"},
		{"decimal from int64", int64(9223372036854775807), ir.Decimal{Precision: 20, Scale: 0}, "9223372036854775807"},

		// ---- Float ----
		{"double passthrough", float64(2.71828), ir.Float{Precision: ir.FloatDouble}, float64(2.71828)},
		{"single widened", float32(1.5), ir.Float{Precision: ir.FloatSingle}, float64(1.5)},

		// ---- Strings ----
		{"varchar from bytes", []byte("hello"), ir.Varchar{Length: 32}, "hello"},
		{"text from bytes", []byte("a long string"), ir.Text{Size: ir.TextRegular}, "a long string"},
		{"char from string", "world", ir.Char{Length: 5}, "world"},
		// BIGINT UNSIGNED carried as TEXT (--type-override COL=text): the
		// driver's uint64/int64 must render as exact decimal text.
		{"text from uint64 max", uint64(18446744073709551615), ir.Text{Size: ir.TextRegular}, "18446744073709551615"},
		{"varchar from int64", int64(42), ir.Varchar{Length: 32}, "42"},

		// ---- Bytes ----
		{"blob from bytes", []byte{0xde, 0xad, 0xbe, 0xef}, ir.Blob{Size: ir.BlobRegular}, []byte{0xde, 0xad, 0xbe, 0xef}},
		{"varbinary from bytes", []byte{0x01, 0x02}, ir.Varbinary{Length: 8}, []byte{0x01, 0x02}},

		// ---- Temporal ----
		{"timestamp passthrough", now, ir.Timestamp{Precision: 0, WithTimeZone: true}, now},
		{"datetime passthrough", now, ir.DateTime{Precision: 0}, now},
		{"date passthrough", now, ir.Date{}, now},
		{"time as string", []byte("12:34:56"), ir.Time{Precision: 0}, "12:34:56"},

		// ---- JSON ----
		{"json as bytes", []byte(`{"k":"v"}`), ir.JSON{Binary: true}, []byte(`{"k":"v"}`)},

		// ---- Enum (Bug 145) ----
		// Snapshot / VStream hand back the LABEL (string/bytes) — passthrough.
		{"enum label as bytes", []byte("admin"), ir.Enum{Values: []string{"admin", "user"}}, "admin"},
		{"enum label as string", "user", ir.Enum{Values: []string{"admin", "user"}}, "user"},
		// Binlog hands back the 1-based ordinal INDEX (int family) — map to label.
		{"enum index int64 → label", int64(2), ir.Enum{Values: []string{"admin", "user"}}, "user"},
		{"enum index uint8 → label", uint8(1), ir.Enum{Values: []string{"admin", "user"}}, "admin"},
		{"enum index int8 → label", int8(2), ir.Enum{Values: []string{"admin", "user"}}, "user"},
		// MySQL index 0 is the '' empty/error member.
		{"enum index 0 → empty member", int64(0), ir.Enum{Values: []string{"admin", "user"}}, ""},

		// ---- Set ----
		{"set with members", []byte("a,b,c"), ir.Set{Values: []string{"a", "b", "c", "d"}}, []string{"a", "b", "c"}},
		{"set empty", []byte(""), ir.Set{Values: []string{"a", "b"}}, []string{}},

		// ---- Geometry ----
		// MySQL on the wire delivers `<srid uint32 LE><wkb>`; the
		// IR contract for ir.Geometry values is "raw WKB". The
		// decoder strips the 4-byte SRID prefix. The test fixture
		// here is a 4-byte SRID(0) followed by a 5-byte stub WKB
		// payload so length checks pass.
		{
			"geometry strips srid prefix",
			[]byte{
				// SRID = 0 (4 bytes LE)
				0x00, 0x00, 0x00, 0x00,
				// WKB stub: byte_order(LE) + type(1=POINT, LE)
				0x01, 0x01, 0x00, 0x00, 0x00,
			},
			ir.Geometry{Subtype: ir.GeometryPoint},
			[]byte{0x01, 0x01, 0x00, 0x00, 0x00},
		},

		// ---- VARCHAR-mapped extension types ----
		{"uuid as string", []byte("1234-5678"), ir.UUID{}, "1234-5678"},
		{"inet as string", []byte("10.0.0.1"), ir.Inet{}, "10.0.0.1"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeValue(c.raw, c.t)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("decodeValue(%#v, %T) = %#v; want %#v", c.raw, c.t, got, c.want)
			}
		})
	}
}

// TestDecodeValueErrors verifies the decoder surfaces a clear error
// (rather than silently corrupting) when the driver returns something
// unexpected for the column's IR type.
func TestDecodeValueErrors(t *testing.T) {
	cases := []struct {
		name string
		raw  any
		t    ir.Type
	}{
		{"bool from float64", float64(1), ir.Boolean{}},
		{"int from string", "42", ir.Integer{Width: 32}},
		// Note: TIMESTAMP/DATETIME from string is now SUPPORTED — the
		// binlog reader hands back string-form temporals regardless of
		// the schema-cache DSN's parseTime setting (Bug 12).
		// TestDecodeTimeFromString covers the success path.
		{"timestamp from unparseable string", "definitely-not-a-date", ir.Timestamp{}},
		// Bug 145: an ENUM index past the value list, or a wholly
		// unexpected type, must fail loudly rather than carry a bad value.
		{"enum index out of range", int64(5), ir.Enum{Values: []string{"admin", "user"}}},
		{"enum non-int non-string", float64(1), ir.Enum{Values: []string{"admin", "user"}}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeValue(c.raw, c.t); err == nil {
				t.Errorf("decodeValue(%#v, %T) returned no error; expected one", c.raw, c.t)
			}
		})
	}
}

// TestDecodeTimeFromString is the regression guard for Bug 12: the
// binlog reader hands timestamp values back as strings, so decodeTime
// must parse the canonical MySQL temporal forms rather than only
// accepting time.Time directly.
func TestDecodeTimeFromString(t *testing.T) {
	cases := []struct {
		name string
		raw  any
		want time.Time
	}{
		{
			"timestamp string with second precision",
			"2026-05-05 17:20:13",
			time.Date(2026, 5, 5, 17, 20, 13, 0, time.UTC),
		},
		{
			"timestamp string with microsecond precision",
			"2026-05-05 17:20:13.123456",
			time.Date(2026, 5, 5, 17, 20, 13, 123456000, time.UTC),
		},
		{
			"timestamp bytes (driver may return either)",
			[]byte("2026-05-05 17:20:13"),
			time.Date(2026, 5, 5, 17, 20, 13, 0, time.UTC),
		},
		{
			"date string",
			"2026-05-05",
			time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeValue(c.raw, ir.Timestamp{})
			if err != nil {
				t.Fatalf("decodeValue: %v", err)
			}
			gotTime, ok := got.(time.Time)
			if !ok {
				t.Fatalf("decodeValue returned %T; want time.Time", got)
			}
			if !gotTime.Equal(c.want) {
				t.Errorf("decodeValue = %v; want %v", gotTime, c.want)
			}
		})
	}
}

// TestDecodeBytesIsCopy ensures the returned []byte does not alias the
// driver's buffer. Driver buffers may be reused across rows; aliasing
// would corrupt earlier values.
func TestDecodeBytesIsCopy(t *testing.T) {
	src := []byte{0xaa, 0xbb, 0xcc}
	got, err := decodeValue(src, ir.Blob{Size: ir.BlobRegular})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := got.([]byte)
	if &out[0] == &src[0] {
		t.Fatal("decodeValue returned the driver's slice; expected a copy")
	}
	src[0] = 0x00
	if out[0] != 0xaa {
		t.Errorf("mutating source mutated decoded value: got %#v", out)
	}
}
