// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/hex"
	"strings"
	"testing"
)

// TestSwapWKBAxes_EveryGeometryFamily pins the walker against MySQL's OWN
// answer (Bug 238).
//
// Every expectation below is `HEX(ST_AsWKB(ST_SwapXY(...)))` read off a real
// `mysql:8`, paired with the unswapped `HEX(ST_AsWKB(...))` as input. That
// matters more than the usual family-matrix argument: a hand-rolled expectation
// would be my own encoder checked against my own walker, and the two would
// agree on any structural misreading they happened to share. MySQL's ST_SwapXY
// is an independent implementation of exactly this transform.
//
// The families are the Bug 74 lesson applied to a STRUCTURAL dispatch rather
// than a value one — this walker branches on the WKB type code, so one
// representative proves nothing about the others. MultiPoint is the cell that
// earns its place: its elements are complete Point geometries with their own
// 5-byte headers, and a walker that treats them as bare coordinate pairs reads
// those headers as coordinates and silently corrupts the value.
func TestSwapWKBAxes_EveryGeometryFamily(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"POINT(1 2)",
			"0101000000000000000000F03F0000000000000040",
			"01010000000000000000000040000000000000F03F",
		},
		{
			"LINESTRING(1 2,3 4)",
			"010200000002000000000000000000F03F000000000000004000000000000008400000000000001040",
			"0102000000020000000000000000000040000000000000F03F00000000000010400000000000000840",
		},
		{
			"POLYGON((0 0,0 1,1 1,1 0,0 0))",
			"01030000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000",
			"0103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000",
		},
		{
			"MULTIPOINT((1 2),(3 4)) — elements are full Point geometries",
			"0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040",
			"01040000000200000001010000000000000000000040000000000000F03F010100000000000000000010400000000000000840",
		},
		{
			"MULTILINESTRING((1 2,3 4),(5 6,7 8))",
			"010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000144000000000000018400000000000001C400000000000002040",
			"0105000000020000000102000000020000000000000000000040000000000000F03F000000000000104000000000000008400102000000020000000000000000001840000000000000144000000000000020400000000000001C40",
		},
		{
			"MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))",
			"01060000000100000001030000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000",
			"0106000000010000000103000000010000000500000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000000000",
		},
		{
			"GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(3 4,5 6))",
			"0107000000020000000101000000000000000000F03F00000000000000400102000000020000000000000000000840000000000000104000000000000014400000000000001840",
			"01070000000200000001010000000000000000000040000000000000F03F0102000000020000000000000000001040000000000000084000000000000018400000000000001440",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := mustWKBHex(t, tc.in)
			got, err := swapWKBAxes(in)
			if err != nil {
				t.Fatalf("swapWKBAxes: %v", err)
			}
			if want := mustWKBHex(t, tc.want); !equalBytes(got, want) {
				t.Errorf("swapped  = %X\nMySQL's  = %X", got, want)
			}

			// Swapping twice is the identity — which is what makes
			// MySQL→MySQL byte-identical once the reader and writer both
			// normalise.
			back, err := swapWKBAxes(got)
			if err != nil {
				t.Fatalf("swapWKBAxes (second pass): %v", err)
			}
			if !equalBytes(back, in) {
				t.Errorf("swap is not an involution:\n  once  = %X\n  twice = %X\n  input = %X", got, back, in)
			}
		})
	}
}

// TestSwapWKBAxes_DoesNotMutateInput pins that the caller's buffer survives —
// these bytes come straight off a binlog row image or a driver scan buffer, and
// rewriting them in place would corrupt whatever else reads them.
func TestSwapWKBAxes_DoesNotMutateInput(t *testing.T) {
	in := mustWKBHex(t, "0101000000000000000000F03F0000000000000040")
	orig := append([]byte(nil), in...)
	if _, err := swapWKBAxes(in); err != nil {
		t.Fatalf("swapWKBAxes: %v", err)
	}
	if !equalBytes(in, orig) {
		t.Errorf("input was mutated:\n  before = %X\n  after  = %X", orig, in)
	}
}

// TestSwapWKBAxes_RefusesRatherThanPassingThrough is the direction that decides
// whether this is a safety feature or a liability.
//
// A walker that returns unrecognised input unchanged would silently emit a
// geometry with its axes NOT normalised — the exact silent relocation the
// normalisation exists to prevent, now wearing a successful return value. Every
// shape it cannot walk must be an error.
func TestSwapWKBAxes_RefusesRatherThanPassingThrough(t *testing.T) {
	cases := []struct {
		name, in, wantSubstr string
	}{
		{"empty", "", "truncated header"},
		{"truncated header", "010100", "truncated header"},
		{"bogus byte-order byte", "0201000000000000000000F03F0000000000000040", "byte-order byte"},
		{"truncated coordinate", "0101000000000000000000F03F00000000", "truncated coordinate"},
		{"trailing bytes after a complete geometry", "0101000000000000000000F03F0000000000000040FF", "trailing byte"},
		{
			// Type 1001 = POINT Z. MySQL 8 cannot produce it (ERROR 3037), so
			// its arrival means this walker's 2D premise has stopped holding.
			"POINT Z type code", "01E9030000000000000000F03F00000000000000400000000000000840", "unsupported WKB type",
		},
		{"truncated element count", "010200000002", "truncated element count"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := swapWKBAxes(mustWKBHex(t, tc.in))
			if err == nil {
				t.Fatal("expected a refusal; got nil — an unswapped geometry returned as success is the silent bug")
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("error = %v; want one mentioning %q", err, tc.wantSubstr)
			}
		})
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// mustWKBHex decodes a fixture hex string. Named distinctly from
// value_decode_mariadb_test.go's mustHex, which serves a different fixture set
// in the same package.
func mustWKBHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad test fixture hex %q: %v", s, err)
	}
	return b
}
