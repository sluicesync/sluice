// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTinyBoolOutOfRange pins the Vector D detector: integer-family values
// outside {0,1} are flagged with the underlying value; 0/1 and the
// inherently-boolean sources (bool, BIT(1) bytes, string) are not.
func TestTinyBoolOutOfRange(t *testing.T) {
	cases := []struct {
		name    string
		raw     any
		wantN   int64
		wantOOB bool
	}{
		{"int64 zero", int64(0), 0, false},
		{"int64 one", int64(1), 0, false},
		{"int64 two", int64(2), 2, true},
		{"int64 127", int64(127), 127, true},
		{"int8 negative one", int8(-1), -1, true},
		{"int8 min", int8(-128), -128, true},
		{"int32 in-range", int32(1), 0, false},
		{"int16 oob", int16(42), 42, true},
		{"int oob", 99, 99, true},
		{"uint8 oob", uint8(2), 2, true},
		{"uint64 in-range", uint64(1), 0, false},
		{"uint64 oob", uint64(200), 200, true},
		{"uint64 absurd clamps", uint64(math.MaxUint64), math.MaxInt64, true},
		// Inherently boolean sources are never "out of range".
		{"bool true", true, 0, false},
		{"bit byte non-zero", []byte{0x01}, 0, false},
		{"string", "1", 0, false},
		{"nil", nil, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, oob := tinyBoolOutOfRange(c.raw)
			if oob != c.wantOOB {
				t.Fatalf("tinyBoolOutOfRange(%#v) oob = %v; want %v", c.raw, oob, c.wantOOB)
			}
			if oob && n != c.wantN {
				t.Errorf("tinyBoolOutOfRange(%#v) n = %d; want %d", c.raw, n, c.wantN)
			}
		})
	}
}

// TestBoolRangeWarner_WarnsOncePerColumn pins that the warner emits exactly
// one WARN per offending column (not per row), names the column and an
// example value, and points at the --type-override remedy — and that
// in-range values produce no output.
func TestBoolRangeWarner_WarnsOncePerColumn(t *testing.T) {
	buf := captureSlog(t)
	w := newBoolRangeWarner()
	col := &ir.Column{Name: "is_active", Type: ir.Boolean{}}
	other := &ir.Column{Name: "flag", Type: ir.Boolean{}}

	// First out-of-range value warns; in-range values never do.
	w.observe("users", col, int64(0))
	w.observe("users", col, int64(1))
	w.observe("users", col, int8(2)) // first OOB -> warns
	w.observe("users", col, int64(127))
	w.observe("users", col, int8(-1))
	// A different column warns independently (once).
	w.observe("users", other, int64(5))
	w.observe("users", other, int64(6))

	out := buf.String()

	if got := strings.Count(out, `column=users.is_active`); got != 1 {
		t.Errorf("users.is_active warned %d times; want exactly 1\n%s", got, out)
	}
	if got := strings.Count(out, `column=users.flag`); got != 1 {
		t.Errorf("users.flag warned %d times; want exactly 1\n%s", got, out)
	}
	// Names an example value (the first OOB seen for the column).
	if !strings.Contains(out, "example_value=2") {
		t.Errorf("want example_value=2 for is_active; got\n%s", out)
	}
	// Points at the data-preserving remedy with the table-qualified column.
	if !strings.Contains(out, "--type-override users.is_active=smallint") {
		t.Errorf("want the --type-override hint naming the column; got\n%s", out)
	}
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("want a WARN-level record; got\n%s", out)
	}
}

// TestBoolRangeWarner_InRangeSilent pins that a column that only ever holds
// 0/1 produces no log output at all.
func TestBoolRangeWarner_InRangeSilent(t *testing.T) {
	buf := captureSlog(t)
	w := newBoolRangeWarner()
	col := &ir.Column{Name: "ok", Type: ir.Boolean{}}
	for _, v := range []any{int64(0), int64(1), true, false, []byte{0x00}, []byte{0x01}, "0", "1"} {
		w.observe("t", col, v)
	}
	if out := buf.String(); strings.TrimSpace(out) != "" {
		t.Errorf("in-range/boolean-source values produced log output:\n%s", out)
	}
}
