// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// withZeroDatePolicy sets the process-global zero-date policy for the
// duration of a test and restores it afterward. The policy is a package
// global (mirroring sessionSQLMode); tests that exercise it must isolate
// the mutation.
func withZeroDatePolicy(t *testing.T, mode zeroDateMode) {
	t.Helper()
	prev := zeroDatePolicy
	zeroDatePolicy = mode
	t.Cleanup(func() { zeroDatePolicy = prev })
}

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

func TestSetZeroDateMode(t *testing.T) {
	withZeroDatePolicy(t, zeroDateRefuse)
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
		if err := SetZeroDateMode(c.in); err != nil {
			t.Fatalf("SetZeroDateMode(%q) err = %v", c.in, err)
		}
		if zeroDatePolicy != c.want {
			t.Errorf("SetZeroDateMode(%q): policy = %v; want %v", c.in, zeroDatePolicy, c.want)
		}
	}
	if err := SetZeroDateMode("bogus"); err == nil {
		t.Error("SetZeroDateMode(\"bogus\") err = nil; want an error")
	}
}

// TestApplyZeroDatePolicy pins the resolution matrix: each --zero-date
// mode against nullable and NOT NULL columns.
func TestApplyZeroDatePolicy(t *testing.T) {
	zd := &zeroDateValueError{raw: "2026-00-00"}
	nullable := &ir.Column{Name: "d", Type: ir.Date{}, Nullable: true}
	notNull := &ir.Column{Name: "d", Type: ir.Date{}, Nullable: false}

	t.Run("error refuses", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateRefuse)
		if _, err := applyZeroDatePolicy(zd, nullable); err == nil {
			t.Fatal("err = nil; want a refusal")
		}
	})
	t.Run("null on nullable yields NULL", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsNull)
		v, err := applyZeroDatePolicy(zd, nullable)
		if err != nil {
			t.Fatalf("err = %v; want nil", err)
		}
		if v != nil {
			t.Fatalf("v = %v; want nil (SQL NULL)", v)
		}
	})
	t.Run("null on NOT NULL refuses loudly", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsNull)
		if _, err := applyZeroDatePolicy(zd, notNull); err == nil {
			t.Fatal("err = nil; want a NOT NULL refusal")
		}
	})
	t.Run("epoch substitutes 1970-01-01", func(t *testing.T) {
		withZeroDatePolicy(t, zeroDateAsEpoch)
		v, err := applyZeroDatePolicy(zd, notNull)
		if err != nil {
			t.Fatalf("err = %v; want nil", err)
		}
		gt, ok := v.(time.Time)
		if !ok {
			t.Fatalf("v = %T; want time.Time", v)
		}
		if !gt.Equal(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("v = %v; want 1970-01-01 UTC", gt)
		}
	})
}
