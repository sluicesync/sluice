// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func col(t ir.Type) *ir.Column { return &ir.Column{Name: "c", Type: t} }

// TestEncodeValue pins the IR value → SQLite binding for every IR type
// family the writer accepts (the Bug-74 "pin the class" discipline: each
// family + NULL, not one representative), plus the loud-refusal paths.
func TestEncodeValue(t *testing.T) {
	utc := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	plus5 := time.FixedZone("plus5", 5*3600)
	cases := []struct {
		name string
		typ  ir.Type
		in   any
		want any
	}{
		{"bool true", ir.Boolean{}, true, int64(1)},
		{"bool false", ir.Boolean{}, false, int64(0)},
		{"int64", ir.Integer{Width: 64}, int64(-42), int64(-42)},
		{"uint64 in range", ir.Integer{Width: 64, Unsigned: true}, uint64(42), int64(42)},
		{"int widened", ir.Integer{Width: 32}, 7, int64(7)},
		{"float64", ir.Float{Precision: ir.FloatDouble}, 3.5, 3.5},
		{"text string", ir.Text{}, "héllo", "héllo"},
		{"varchar string", ir.Varchar{Length: 10}, "x", "x"},
		{"char string", ir.Char{Length: 2}, "ab", "ab"},
		{"uuid string", ir.UUID{}, "01234567-89ab-cdef-0123-456789abcdef", "01234567-89ab-cdef-0123-456789abcdef"},
		{"enum string", ir.Enum{Values: []string{"a", "b"}}, "a", "a"},
		{"json bytes→text", ir.JSON{Binary: true}, []byte(`{"a":1}`), `{"a":1}`},
		{"set join", ir.Set{Values: []string{"a", "b"}}, []string{"a", "b"}, "a,b"},
		{"set empty", ir.Set{Values: []string{"a"}}, []string{}, ""},
		{"blob bytes", ir.Blob{}, []byte{0xca, 0xfe}, []byte{0xca, 0xfe}},
		{"date utc", ir.Date{}, utc, "2024-01-02"},
		{"datetime utc", ir.DateTime{}, utc, "2024-01-02 03:04:05"},
		{"timestamp utc", ir.Timestamp{}, utc, "2024-01-02 03:04:05"},
		{"timestamptz→utc", ir.Timestamp{WithTimeZone: true}, time.Date(2024, 1, 2, 8, 4, 5, 0, plus5), "2024-01-02 03:04:05"},
		{"time string verbatim", ir.Time{}, "03:04:05", "03:04:05"},
		{"decimal money", ir.Decimal{Unconstrained: true}, "123.45", "123.45"},
		{"decimal int", ir.Decimal{Precision: 5}, "100", "100"},
		{"decimal int64 max", ir.Decimal{Unconstrained: true}, "9223372036854775807", "9223372036854775807"},
		// NULL is faithful for every type — one representative is enough.
		{"null text", ir.Text{}, nil, nil},
		{"null int", ir.Integer{Width: 64}, nil, nil},
		{"null blob", ir.Blob{}, nil, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := encodeValue(col(c.typ), c.in)
			if err != nil {
				t.Fatalf("encodeValue(%v) error: %v", c.in, err)
			}
			if b, ok := c.want.([]byte); ok {
				gb, ok := got.([]byte)
				if !ok || !bytes.Equal(gb, b) {
					t.Fatalf("got %#v; want bytes %#v", got, b)
				}
				return
			}
			if got != c.want {
				t.Fatalf("encodeValue(%v) = %#v; want %#v", c.in, got, c.want)
			}
		})
	}
}

// TestEncodeValueRefusals pins the loud-failure paths: a value SQLite
// cannot faithfully hold is refused (naming the column), never coerced.
func TestEncodeValueRefusals(t *testing.T) {
	cases := []struct {
		name string
		typ  ir.Type
		in   any
		want string // substring the error must contain
	}{
		{"uint64 overflow", ir.Integer{Unsigned: true}, uint64(math.MaxInt64) + 1, "unsigned"},
		{"decimal too precise", ir.Decimal{Unconstrained: true}, "12345678901234567890.12345", "exceeds SQLite's exact storage range"},
		{"decimal 16 sig digits", ir.Decimal{Unconstrained: true}, "1.234567890123456", "exact storage range"},
		{"bool from int", ir.Boolean{}, int64(1), "is not a bool"},
		{"float from string", ir.Float{}, "3.5", "is not a float64"},
		{"blob from string", ir.Blob{}, "x", "is not a []byte"},
		{"text from int", ir.Text{}, int64(1), "is not a string"},
		{"decimal from float", ir.Decimal{}, 1.5, "is not a decimal string"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := encodeValue(col(c.typ), c.in)
			if err == nil {
				t.Fatalf("encodeValue(%v) err = nil; want a loud refusal", c.in)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error = %v; want it to contain %q", err, c.want)
			}
			if !strings.Contains(err.Error(), `"c"`) {
				t.Errorf("error %v should name the column", err)
			}
		})
	}
}

// TestDecimalFitsSQLite pins the decimal-survival guard at the exact
// boundary (15 significant digits) and the int64-exactness exemption.
func TestDecimalFitsSQLite(t *testing.T) {
	cases := []struct {
		s    string
		fits bool
	}{
		{"0", true},
		{"100", true},
		{"-42", true},
		{"9223372036854775807", true},         // int64 max — exact INTEGER
		{"9223372036854775808", false},        // beyond int64, 19 digits → REAL lossy
		{"123.45", true},                      // 5 sig
		{"0.1", true},                         // 1 sig
		{"123.40", true},                      // trailing zero not significant → 4
		{"0.000001", true},                    // 1 sig
		{"1.23456789012345", true},            // 15 sig — the boundary
		{"1.234567890123456", false},          // 16 sig — refused (safe side)
		{"12345678901234567890.12345", false}, // 25 sig
		{"not-a-number", false},               // unparseable → refuse
	}
	for _, c := range cases {
		if got := decimalFitsSQLite(c.s); got != c.fits {
			t.Errorf("decimalFitsSQLite(%q) = %v; want %v (sig=%d)", c.s, got, c.fits, decimalSignificantDigits(c.s))
		}
	}
}
