// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
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

// TestCheckTinyBoolRange pins the refusal: a TINYINT(1)/ir.Boolean cell
// holding a value outside {0,1} returns a coded SLUICE-E-VALUE-TINYINT1-RANGE
// error that names the table-qualified column, carries the example value in
// its message, and points at the --type-override remedy in its Hint. In-range
// 0/1 and inherently-boolean sources return nil (no refusal).
func TestCheckTinyBoolRange(t *testing.T) {
	t.Run("out of range refuses, coded, named, with remedy", func(t *testing.T) {
		err := checkTinyBoolRange("users", "is_active", int8(2))
		if err == nil {
			t.Fatal("checkTinyBoolRange(2): want a refusal, got nil")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeValueTinyint1Range {
			t.Fatalf("want CodeValueTinyint1Range; got ok=%v err=%v", ok, err)
		}
		if !strings.Contains(err.Error(), `"users.is_active"`) {
			t.Errorf("message must name the column; got %q", err.Error())
		}
		if !strings.Contains(err.Error(), "2") {
			t.Errorf("message must carry the example value 2; got %q", err.Error())
		}
		if !strings.Contains(ce.Hint, "--type-override users.is_active=smallint") {
			t.Errorf("hint must carry the --type-override remedy; got %q", ce.Hint)
		}
	})

	t.Run("a negative value is out of range too", func(t *testing.T) {
		if err := checkTinyBoolRange("t", "c", int8(-1)); err == nil {
			t.Error("checkTinyBoolRange(-1): want a refusal")
		}
	})

	t.Run("in-range and boolean sources never refuse", func(t *testing.T) {
		for _, v := range []any{int64(0), int64(1), true, false, []byte{0x00}, []byte{0x01}, "0", "1", nil} {
			if err := checkTinyBoolRange("t", "c", v); err != nil {
				t.Errorf("checkTinyBoolRange(%#v) = %v; want nil (in-range / boolean source)", v, err)
			}
		}
	})
}
