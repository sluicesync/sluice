// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestDecodeTimeZeroDateFamily pins the Vector A contract: MySQL zero and
// partial dates surface as a *zeroDateValueError sentinel from the pure
// decoder (never a silently-normalized time.Time), across the whole
// temporal type family — Date/DateTime/Timestamp — and every zero shape.
// This is the Bug-74 "pin the class, not the representative" discipline:
// the same code path runs for all three IR temporal types, but pinning
// one would not prove the others.
func TestDecodeTimeZeroDateFamily(t *testing.T) {
	temporalTypes := []struct {
		name string
		typ  ir.Type
	}{
		{"Date", ir.Date{}},
		{"DateTime", ir.DateTime{}},
		{"Timestamp", ir.Timestamp{}},
	}
	zeroShapes := []struct {
		name string
		raw  string
	}{
		{"all-zero date", "0000-00-00"},
		{"all-zero datetime", "0000-00-00 00:00:00"},
		{"all-zero datetime micros", "0000-00-00 00:00:00.000000"},
		{"zero month", "2026-00-15"},
		{"zero day", "2026-06-00"},
		{"zero month and day", "2026-00-00"},
		{"empty string", ""},
	}
	for _, tt := range temporalTypes {
		for _, zs := range zeroShapes {
			t.Run(tt.name+"/"+zs.name, func(t *testing.T) {
				// Both the string and []byte forms reach decodeTime
				// (the CAST(... AS CHAR) read path returns []byte; the
				// binlog path returns strings).
				for _, raw := range []any{zs.raw, []byte(zs.raw)} {
					_, err := decodeValue(raw, tt.typ)
					var zd *zeroDateValueError
					if !errors.As(err, &zd) {
						t.Fatalf("decodeValue(%q as %s) err = %v; want *zeroDateValueError", zs.raw, tt.name, err)
					}
				}
			})
		}
	}
}

// TestDecodeTimeValidStillParses guards against the zero-date detection
// swallowing legitimate values: valid dates must still decode to the
// correct time.Time, and a non-NULL temporal column never produces a
// silently-wrong result.
func TestDecodeTimeValidStillParses(t *testing.T) {
	cases := []struct {
		raw  string
		want time.Time
	}{
		{"2026-06-07", time.Date(2026, 6, 7, 0, 0, 0, 0, time.UTC)},
		{"2026-06-07 12:34:56", time.Date(2026, 6, 7, 12, 34, 56, 0, time.UTC)},
		{"2026-06-07 12:34:56.123456", time.Date(2026, 6, 7, 12, 34, 56, 123456000, time.UTC)},
		// Year 0000 with a valid month/day is a representable historical
		// date, NOT a zero date — it must parse rather than refuse.
		{"0000-12-25", time.Date(0, 12, 25, 0, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			got, err := decodeValue(c.raw, ir.Timestamp{})
			if err != nil {
				t.Fatalf("decodeValue(%q) err = %v; want valid time", c.raw, err)
			}
			gt, ok := got.(time.Time)
			if !ok {
				t.Fatalf("decodeValue(%q) = %T; want time.Time", c.raw, got)
			}
			if !gt.Equal(c.want) {
				t.Errorf("decodeValue(%q) = %v; want %v", c.raw, gt, c.want)
			}
		})
	}
}

// TestDecodeTimeMalformedIsHardError confirms a genuinely out-of-range
// but non-zero date stays a hard decode error — it is NOT folded into
// the zero-date family, so --zero-date never silently rescues garbage.
func TestDecodeTimeMalformedIsHardError(t *testing.T) {
	for _, raw := range []string{"2026-13-01", "2026-02-30", "not-a-date", "2026-06"} {
		t.Run(raw, func(t *testing.T) {
			_, err := decodeValue(raw, ir.Date{})
			if err == nil {
				t.Fatalf("decodeValue(%q) err = nil; want a hard error", raw)
			}
			var zd *zeroDateValueError
			if errors.As(err, &zd) {
				t.Fatalf("decodeValue(%q) returned a zeroDateValueError; want a plain hard error", raw)
			}
		})
	}
}

func TestIsMySQLZeroDate(t *testing.T) {
	zero := []string{"0000-00-00", "0000-00-00 00:00:00", "2026-00-15", "2026-06-00", "2026-00-00"}
	notZero := []string{"2026-06-07", "2026-13-01", "0000-12-25", "garbage", "2026-06"}
	for _, s := range zero {
		if !isMySQLZeroDate(s) {
			t.Errorf("isMySQLZeroDate(%q) = false; want true", s)
		}
	}
	for _, s := range notZero {
		if isMySQLZeroDate(s) {
			t.Errorf("isMySQLZeroDate(%q) = true; want false", s)
		}
	}
}

// TestWithZeroDate pins the --zero-date builder (task 2.5, replacing
// SetZeroDateMode): each accepted value maps to its mode on the engine, and a bad
// value refuses loudly. The engine default is folded onto readers via
// resolveReaderZeroDate, so this also pins the per-instance precedence
// (DSN mode > engine default > refuse).
func TestWithZeroDate(t *testing.T) {
	cases := []struct {
		in   string
		want zeroDateMode
	}{
		{"", zeroDateRefuse},
		{"error", zeroDateRefuse},
		{"null", zeroDateAsNull},
		{"epoch", zeroDateAsEpoch},
	}
	for _, c := range cases {
		e, err := Engine{}.WithZeroDate(c.in)
		if err != nil {
			t.Fatalf("WithZeroDate(%q) err = %v", c.in, err)
		}
		if got := e.(Engine).opts.zeroDate; got != c.want {
			t.Errorf("WithZeroDate(%q): engine zeroDate = %v; want %v", c.in, got, c.want)
		}
	}
	if _, err := (Engine{}).WithZeroDate("bogus"); err == nil {
		t.Error("WithZeroDate(\"bogus\") err = nil; want an error")
	}

	// Per-instance precedence: the reader's DSN mode wins over the engine default;
	// an unset (inherit) DSN mode falls back to the engine default; both unset →
	// inherit (which applyZeroDatePolicy resolves to refuse).
	eNull, _ := Engine{}.WithZeroDate("null")
	en := eNull.(Engine)
	if got := en.resolveReaderZeroDate(zeroDateAsEpoch); got != zeroDateAsEpoch {
		t.Errorf("DSN epoch over engine null: got %v; want epoch", got)
	}
	if got := en.resolveReaderZeroDate(zeroDateInherit); got != zeroDateAsNull {
		t.Errorf("unset DSN falls back to engine null: got %v; want null", got)
	}
	if got := (Engine{}).resolveReaderZeroDate(zeroDateInherit); got != zeroDateInherit {
		t.Errorf("both unset stays inherit (→ refuse at decode): got %v; want inherit", got)
	}
}

// TestApplyZeroDatePolicy pins the resolution matrix: each --zero-date
// mode against nullable and NOT NULL columns.
func TestApplyZeroDatePolicy(t *testing.T) {
	zd := &zeroDateValueError{raw: "2026-00-00"}
	nullable := &ir.Column{Name: "d", Type: ir.Date{}, Nullable: true}
	notNull := &ir.Column{Name: "d", Type: ir.Date{}, Nullable: false}

	t.Run("error refuses", func(t *testing.T) {
		_, err := applyZeroDatePolicy(zd, nullable, zeroDateRefuse)
		if err == nil {
			t.Fatal("err = nil; want a refusal")
		}
		// The refusal carries the stable code + flag hint as metadata
		// (docs/operator/error-codes.md); the prose is unchanged.
		ce, ok := sluicecode.FromError(err)
		if !ok {
			t.Fatal("zero-date refusal does not carry a CodedError")
		}
		if ce.Code != sluicecode.CodeValueZeroDate {
			t.Errorf("Code = %q; want %q", ce.Code, sluicecode.CodeValueZeroDate)
		}
		if !strings.Contains(ce.Hint, "--zero-date") {
			t.Errorf("Hint = %q; want the --zero-date remedy", ce.Hint)
		}
	})
	t.Run("null on nullable yields NULL", func(t *testing.T) {
		v, err := applyZeroDatePolicy(zd, nullable, zeroDateAsNull)
		if err != nil {
			t.Fatalf("err = %v; want nil", err)
		}
		if v != nil {
			t.Fatalf("v = %v; want nil (SQL NULL)", v)
		}
	})
	t.Run("null on NOT NULL refuses loudly", func(t *testing.T) {
		_, err := applyZeroDatePolicy(zd, notNull, zeroDateAsNull)
		if err == nil {
			t.Fatal("err = nil; want a NOT NULL refusal")
		}
		// Same code as the plain refusal — one code per class, both
		// message shapes.
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeValueZeroDate {
			t.Errorf("NOT NULL refusal code = %v (found=%v); want %q", ce, ok, sluicecode.CodeValueZeroDate)
		}
	})
	t.Run("epoch substitutes 1970-01-01 00:00:01", func(t *testing.T) {
		v, err := applyZeroDatePolicy(zd, notNull, zeroDateAsEpoch)
		if err != nil {
			t.Fatalf("err = %v; want nil", err)
		}
		gt, ok := v.(time.Time)
		if !ok {
			t.Fatalf("v = %T; want time.Time", v)
		}
		// 00:00:01, not midnight: MySQL's TIMESTAMP floor is
		// 1970-01-01 00:00:01 UTC, so midnight is unrepresentable there
		// and would coerce back to the zero sentinel (Bug 133).
		if !gt.Equal(time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)) {
			t.Errorf("v = %v; want 1970-01-01 00:00:01 UTC", gt)
		}
	})
}
