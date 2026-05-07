// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestDecodeValue(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)
	prefix := netip.MustParsePrefix("192.168.1.0/24")
	mac, _ := net.ParseMAC("08:00:2b:01:02:03")

	cases := []struct {
		name string
		raw  any
		t    ir.Type
		want any
	}{
		// ---- NULL ----
		{"null int", nil, ir.Integer{Width: 32}, nil},
		{"null array", nil, ir.Array{Element: ir.Integer{Width: 32}}, nil},

		// ---- Boolean ----
		{"bool true", true, ir.Boolean{}, true},
		{"bool false", false, ir.Boolean{}, false},

		// ---- Integer (widening) ----
		{"int16 → int64", int16(7), ir.Integer{Width: 16}, int64(7)},
		{"int32 → int64", int32(42), ir.Integer{Width: 32}, int64(42)},
		{"int64 passthrough", int64(99), ir.Integer{Width: 64}, int64(99)},

		// ---- Decimal ----
		{"decimal as string", "3.14159", ir.Decimal{Precision: 6, Scale: 5}, "3.14159"},
		{"decimal from bytes", []byte("19.95"), ir.Decimal{Precision: 8, Scale: 2}, "19.95"},

		// ---- Float ----
		{"float64 passthrough", 2.71828, ir.Float{Precision: ir.FloatDouble}, 2.71828},
		{"float32 widened", float32(1.5), ir.Float{Precision: ir.FloatSingle}, float64(1.5)},

		// ---- Strings ----
		{"varchar string", "hello", ir.Varchar{Length: 32}, "hello"},
		{"text string", "longer text", ir.Text{Size: ir.TextLong}, "longer text"},

		// ---- Bytes ----
		{"bytea bytes", []byte{0xde, 0xad}, ir.Blob{Size: ir.BlobLong}, []byte{0xde, 0xad}},

		// ---- Temporal ----
		{"timestamp passthrough", now, ir.Timestamp{Precision: 0, WithTimeZone: true}, now},
		{"date passthrough", now, ir.Date{}, now},
		{
			"time as string",
			time.Date(0, 1, 1, 8, 30, 0, 0, time.UTC),
			ir.Time{Precision: 0},
			"08:30:00",
		},
		// pgoutput CDC tuple values arrive as []byte in Postgres
		// canonical text form. The decoder is shared with the
		// row-reader path that gives us time.Time, so both shapes
		// must round-trip. (TIMESTAMPTZ parsing is exercised by the
		// integration test — the location pointer comparison here
		// is too brittle for a unit test.)
		{
			"timestamp from text bytes",
			[]byte("2026-05-01 12:34:56"),
			ir.DateTime{Precision: 0},
			time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC),
		},
		{
			"date from text bytes",
			[]byte("2026-05-01"),
			ir.Date{},
			time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		},

		// ---- JSON ----
		{"json bytes", []byte(`{"k":"v"}`), ir.JSON{Binary: true}, []byte(`{"k":"v"}`)},

		// ---- Enum ----
		{"enum string", "admin", ir.Enum{Values: []string{"admin", "user"}}, "admin"},

		// ---- UUID ----
		{
			"uuid [16]byte → string",
			[16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
			ir.UUID{},
			"01234567-89ab-cdef-0123-456789abcdef",
		},
		{"uuid string passthrough", "11111111-2222-3333-4444-555555555555", ir.UUID{}, "11111111-2222-3333-4444-555555555555"},

		// ---- Network types ----
		{"inet from netip.Prefix", prefix, ir.Inet{}, "192.168.1.0/24"},
		{"cidr from netip.Prefix", prefix, ir.Cidr{}, "192.168.1.0/24"},
		{"macaddr from net.HardwareAddr", mac, ir.Macaddr{}, "08:00:2b:01:02:03"},

		// ---- Arrays ----
		{
			"int32 array",
			[]int32{1, 2, 3},
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(1), int64(2), int64(3)},
		},
		{
			"text array",
			[]string{"a", "b", "c"},
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{"a", "b", "c"},
		},
		{
			"any-typed array (pgx fast-path)",
			[]any{int64(7), int64(8)},
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(7), int64(8)},
		},

		// ---- Array text form (pgx stdlib *any scan path) ----
		{
			"int array from text",
			"{10,20,30}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(10), int64(20), int64(30)},
		},
		{
			"empty array from text",
			"{}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{},
		},
		{
			"int array with NULL",
			"{1,NULL,3}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(1), nil, int64(3)},
		},
		{
			"text array from text",
			`{"alpha","beta","gamma"}`,
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{"alpha", "beta", "gamma"},
		},
		{
			"text array with embedded comma",
			`{"a, b","c"}`,
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{"a, b", "c"},
		},
		{
			"text array with escaped quote",
			`{"he said \"hi\"","plain"}`,
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{`he said "hi"`, "plain"},
		},
		{
			"bool array from text",
			"{t,f,t}",
			ir.Array{Element: ir.Boolean{}},
			[]any{true, false, true},
		},

		// ---- Scalar string fallbacks ----
		{"int from numeric string", "42", ir.Integer{Width: 32}, int64(42)},
		{"float from numeric string", "3.14", ir.Float{Precision: ir.FloatDouble}, 3.14},
		{"bool from t", "t", ir.Boolean{}, true},
		{"bool from f", "f", ir.Boolean{}, false},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeValue(c.raw, c.t)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("decodeValue(%#v, %T)\n got = %#v\nwant = %#v", c.raw, c.t, got, c.want)
			}
		})
	}
}

func TestDecodeValueErrors(t *testing.T) {
	cases := []struct {
		name string
		raw  any
		t    ir.Type
	}{
		{"bool from int", int64(1), ir.Boolean{}},
		{"int from non-numeric string", "not a number", ir.Integer{Width: 32}},
		{"bool from gibberish string", "maybe", ir.Boolean{}},
		{"timestamp from gibberish string", "not a date", ir.Timestamp{}},
		{"uuid wrong length bytes", []byte{1, 2, 3}, ir.UUID{}},
		{"array from string without braces", "not an array literal", ir.Array{Element: ir.Integer{}}},
		{"array nil element type", []int32{1}, ir.Array{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeValue(c.raw, c.t); err == nil {
				t.Errorf("expected error for %s; got nil", c.name)
			}
		})
	}
}

func TestDecodeBytesIsCopy(t *testing.T) {
	src := []byte{0xaa, 0xbb, 0xcc}
	got, err := decodeValue(src, ir.Blob{Size: ir.BlobLong})
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

func TestFormatUUIDBytes(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
		err  bool
	}{
		{
			[]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			"00112233-4455-6677-8899-aabbccddeeff",
			false,
		},
		{[]byte{1, 2, 3}, "", true},
	}
	for _, c := range cases {
		got, err := formatUUIDBytes(c.in)
		if c.err {
			if err == nil {
				t.Errorf("formatUUIDBytes(%v): expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("formatUUIDBytes: unexpected error: %v", err)
		}
		if got != c.want {
			t.Errorf("formatUUIDBytes:\n got  %q\n want %q", got, c.want)
		}
	}
}
