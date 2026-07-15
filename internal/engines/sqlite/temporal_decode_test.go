// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDecodeTemporal_Matrix is the ADR-0129 VALUE-FIDELITY pin for declared
// temporal columns: {ir.Date, ir.Timestamp, ir.Time} × {iso, unixepoch,
// unixmillis, julian} × {TEXT, INTEGER, REAL, BLOB, NULL}. Each cell asserts
// a faithful instant for the storage class the encoding accepts, nil for
// NULL, and a LOUD refusal for every mismatch (wrong storage class for the
// encoding). The julian REAL math is independently ground-truthed against the
// real SQLite driver in TestRealDriver_TemporalEncodings (a same-formula unit
// pin alone could mask a constant error — the Bug-74 lesson).
func TestDecodeTemporal_Matrix(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	dateOnly := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	const timeStr = "03:04:05"

	epochSec := ts.Unix()
	epochMs := ts.UnixMilli()
	// julianday('2024-01-02 03:04:05') — the SQLite standard value. Carried
	// as a literal; the real-driver test confirms SQLite produces it.
	const julianReal = 2460311.627835648
	// An INTEGER Julian day is noon UTC (Julian days start at noon), so a
	// whole-day value cannot equal ts; it is exercised as "accepted" only.
	const julianNoonDay = int64(2460312)

	// Per IR type: the iso TEXT representative and the faithful expected value
	// (time.Time for Date/Timestamp, textual time-of-day for Time).
	irCases := []struct {
		name   string
		irType ir.Type
		isoTxt string
		want   any
	}{
		{"Date", ir.Date{}, "2024-01-02", dateOnly},
		{"Timestamp", ir.Timestamp{}, "2024-01-02 03:04:05", ts},
		{"Time", ir.Time{}, "03:04:05", timeStr},
	}

	// Per encoding: the classes that decode faithfully to the canonical
	// instant (faithfulRaws), classes accepted but NOT equal to the canonical
	// instant (alsoAccepted — julian INTEGER → noon), and the compare
	// tolerance (julian carries sub-second float error).
	encCases := []struct {
		name         string
		enc          dateEncoding
		faithfulRaws map[string]any
		alsoAccepted map[string]any
		tol          time.Duration
	}{
		{"iso", dateEncodingISO, nil /* set per IR type below */, nil, 0},
		{"unixepoch", dateEncodingUnixEpoch, map[string]any{"INTEGER": epochSec, "REAL": float64(epochSec)}, nil, 0},
		{"unixmillis", dateEncodingUnixMillis, map[string]any{"INTEGER": epochMs, "REAL": float64(epochMs)}, nil, 0},
		{"julian", dateEncodingJulian, map[string]any{"REAL": float64(julianReal)}, map[string]any{"INTEGER": julianNoonDay}, 2 * time.Second},
	}

	// Generic per-class samples for the REFUSE cells (value irrelevant; only
	// the storage class drives the refusal).
	refuseSample := map[string]any{
		"TEXT":    "not-a-temporal",
		"INTEGER": int64(123),
		"REAL":    float64(1.5),
		"BLOB":    []byte{0x01, 0x02},
	}
	allClasses := []string{"TEXT", "INTEGER", "REAL", "BLOB"}

	for _, ic := range irCases {
		for _, ec := range encCases {
			faithful := ec.faithfulRaws
			if ec.enc == dateEncodingISO {
				faithful = map[string]any{"TEXT": ic.isoTxt}
			}

			// Faithful classes (canonical instant).
			for class, raw := range faithful {
				name := ic.name + "/" + ec.name + "/" + class + "=faithful"
				t.Run(name, func(t *testing.T) {
					got, err := decodeCell(raw, ic.irType, ec.enc)
					if err != nil {
						t.Fatalf("unexpected refusal: %v", err)
					}
					if !temporalClose(t, got, ic.want, ec.tol) {
						t.Errorf("decoded %#v; want ~%#v (tol %v)", got, ic.want, ec.tol)
					}
				})
			}

			// Accepted-but-not-canonical classes (julian INTEGER → noon).
			for class, raw := range ec.alsoAccepted {
				name := ic.name + "/" + ec.name + "/" + class + "=accepted"
				t.Run(name, func(t *testing.T) {
					if _, err := decodeCell(raw, ic.irType, ec.enc); err != nil {
						t.Errorf("class %s should be accepted by %s; got refusal: %v", class, ec.name, err)
					}
				})
			}

			// Refuse classes: everything not faithful / not also-accepted.
			for _, class := range allClasses {
				if _, ok := faithful[class]; ok {
					continue
				}
				if _, ok := ec.alsoAccepted[class]; ok {
					continue
				}
				name := ic.name + "/" + ec.name + "/" + class + "=refuse"
				t.Run(name, func(t *testing.T) {
					got, err := decodeCell(refuseSample[class], ic.irType, ec.enc)
					if err == nil {
						t.Fatalf("got faithful %#v; want a LOUD refusal (silent coercion is the failure mode)", got)
					}
					if !strings.Contains(err.Error(), "mismatch") {
						t.Errorf("refusal %q must say \"mismatch\"", err.Error())
					}
				})
			}

			// NULL → nil for every (IR type, encoding).
			t.Run(ic.name+"/"+ec.name+"/NULL=nil", func(t *testing.T) {
				got, err := decodeCell(nil, ic.irType, ec.enc)
				if err != nil || got != nil {
					t.Errorf("NULL decoded to (%#v, %v); want (nil, nil)", got, err)
				}
			})
		}
	}
}

// TestDecodeTemporal_ISOUnparseable pins that under the default iso encoding,
// TEXT that matches no ISO layout is refused loudly (not silently dropped).
func TestDecodeTemporal_ISOUnparseable(t *testing.T) {
	for _, tc := range []struct {
		irType ir.Type
		raw    string
	}{
		{ir.Date{}, "01/02/2024"},          // US slash form — not ISO
		{ir.Timestamp{}, "Jan 2 2024 3am"}, // free text
		{ir.Time{}, "3 o'clock"},           // not HH:MM:SS
		{ir.Date{}, "2024-13-99"},          // ISO-shaped but invalid calendar
	} {
		got, err := decodeCell(tc.raw, tc.irType, dateEncodingISO)
		if err == nil {
			t.Errorf("%s %q: got faithful %#v; want a LOUD refusal", tc.irType, tc.raw, got)
			continue
		}
		if !strings.Contains(err.Error(), "mismatch") {
			t.Errorf("%s %q: refusal %q must say \"mismatch\"", tc.irType, tc.raw, err.Error())
		}
	}
}

// TestDecodeTemporal_ISOSeparatorZoneMatrix pins the full ISO datetime
// separator × zone × fraction family (the Bug-74 discipline): both RFC 3339
// 'T' and SQLite's space separator, naive / 'Z' / explicit-offset zones,
// with and without fractions. The T-separated NAIVE form was MISSING from
// isoDateTimeLayouts (the ADR-0144 inference GLOB validates `[ T]` and
// promotes such a column, then the decode refused it — caught by the
// ADR-0163 flat-file integration suite); this pins every cell so no
// separator/zone combination can silently drop out again.
func TestDecodeTemporal_ISOSeparatorZoneMatrix(t *testing.T) {
	naive := time.Date(2024, 6, 7, 8, 9, 10, 0, time.UTC)
	frac := time.Date(2024, 6, 7, 8, 9, 10, 123456000, time.UTC)
	for _, tc := range []struct {
		raw  string
		want time.Time
	}{
		{"2024-06-07 08:09:10", naive},
		{"2024-06-07T08:09:10", naive}, // the previously-refused cell
		{"2024-06-07 08:09:10.123456", frac},
		{"2024-06-07T08:09:10.123456", frac},
		{"2024-06-07T08:09:10Z", naive},
		{"2024-06-07T08:09:10.123456Z", frac},
		{"2024-06-07T09:09:10+01:00", naive.Add(0)}, // same instant, +01:00
	} {
		got, err := decodeCell(tc.raw, ir.Timestamp{}, dateEncodingISO)
		if err != nil {
			t.Errorf("%q: unexpected refusal: %v", tc.raw, err)
			continue
		}
		tm, ok := got.(time.Time)
		if !ok {
			t.Errorf("%q: got %T; want time.Time", tc.raw, got)
			continue
		}
		if !tm.Equal(tc.want) {
			t.Errorf("%q decoded to %v; want the instant %v", tc.raw, tm, tc.want)
		}
	}
}

// TestDecodeBoolean_Matrix pins the ADR-0129 boolean decode: INTEGER 0/1 and
// the case-insensitive truthy/falsy TEXT set decode faithfully; everything
// else (INTEGER other than 0/1, any REAL, any BLOB, non-truthy TEXT) is
// REFUSED LOUDLY; NULL → nil. Never coerced.
func TestDecodeBoolean_Matrix(t *testing.T) {
	const (
		faithfulT = "true"
		faithfulF = "false"
		refuse    = "refuse"
		isNil     = "nil"
	)
	cases := []struct {
		name    string
		raw     any
		outcome string
	}{
		{"int0", int64(0), faithfulF},
		{"int1", int64(1), faithfulT},
		{"int2", int64(2), refuse},
		{"int-neg", int64(-1), refuse},
		{"text true", "true", faithfulT},
		{"text TRUE", "TRUE", faithfulT},
		{"text t", "t", faithfulT},
		{"text yes", "yes", faithfulT},
		{"text YES", "YES", faithfulT},
		{"text 1", "1", faithfulT},
		{"text false", "false", faithfulF},
		{"text f", "f", faithfulF},
		{"text no", "no", faithfulF},
		{"text 0", "0", faithfulF},
		{"text maybe", "maybe", refuse},
		{"text empty", "", refuse},
		{"real 1.0", float64(1.0), refuse},
		{"real 0.0", float64(0.0), refuse},
		{"blob", []byte{0x01}, refuse},
		{"null", nil, isNil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeCell(c.raw, ir.Boolean{}, dateEncodingISO)
			switch c.outcome {
			case faithfulT, faithfulF:
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				want := c.outcome == faithfulT
				if got != want {
					t.Errorf("decoded %#v; want %v", got, want)
				}
			case isNil:
				if err != nil || got != nil {
					t.Errorf("NULL decoded to (%#v, %v); want (nil, nil)", got, err)
				}
			case refuse:
				if err == nil {
					t.Fatalf("got faithful %#v; want a LOUD refusal (a non-0/1 value in a bool column is a data problem)", got)
				}
				if !strings.Contains(err.Error(), "mismatch") {
					t.Errorf("refusal %q must say \"mismatch\"", err.Error())
				}
			}
		})
	}
}

// TestRealDriver_TemporalEncodings is the independent end-to-end ground truth:
// values written by the real SQLite driver (including SQLite's own
// julianday()/unixepoch math and an ISO date()/datetime() text) are read back
// through the full Engine.OpenRowReader path with the sqlite_date_encoding DSN
// param, asserting the decoded instants. This is what closes the Bug-74 risk
// for the REAL/julian path — modernc computes the bytes, sluice decodes them.
func TestRealDriver_TemporalEncodings(t *testing.T) {
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("iso text DATE/DATETIME/TIME", func(t *testing.T) {
		path := seedDB(
			t,
			`CREATE TABLE t (d DATE, ts DATETIME, tm TIME)`,
			`INSERT INTO t (d, ts, tm) VALUES (date('2024-01-02'), datetime('2024-01-02 03:04:05'), time('2024-01-02 03:04:05'))`,
		)
		rows := readTableEnc(t, path, "t", "iso")
		if len(rows) != 1 {
			t.Fatalf("rows = %d; want 1", len(rows))
		}
		assertTime(t, rows[0]["d"], time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
		assertTime(t, rows[0]["ts"], want)
		if s, _ := rows[0]["tm"].(string); s != "03:04:05" {
			t.Errorf("tm = %#v; want \"03:04:05\"", rows[0]["tm"])
		}
	})

	t.Run("julian REAL via julianday()", func(t *testing.T) {
		path := seedDB(
			t,
			`CREATE TABLE t (ts TIMESTAMP)`,
			`INSERT INTO t (ts) VALUES (julianday('2024-01-02 03:04:05'))`,
		)
		rows := readTableEnc(t, path, "t", "julian")
		assertTimeClose(t, rows[0]["ts"], want, 2*time.Second)
	})

	t.Run("unixepoch INTEGER and REAL", func(t *testing.T) {
		path := seedDB(
			t,
			`CREATE TABLE t (i TIMESTAMP, r TIMESTAMP)`,
			`INSERT INTO t (i, r) VALUES (strftime('%s','2024-01-02 03:04:05'), strftime('%s','2024-01-02 03:04:05') + 0.5)`,
		)
		rows := readTableEnc(t, path, "t", "unixepoch")
		assertTime(t, rows[0]["i"], want)
		assertTimeClose(t, rows[0]["r"], want.Add(500*time.Millisecond), 10*time.Millisecond)
	})

	// REGRESSION CANARY (value-fidelity review): an UNPARSEABLE text value in a
	// DATE-declared column, read under `iso`, must be REFUSED LOUDLY end-to-end
	// on the REAL driver — not silently accepted. This guards the coalesce wart
	// + the decoder's missing `case time.Time`: if modernc ever pre-parsed the
	// value (or someone "fixed" the decoder to accept time.Time), a non-date
	// could slip through. 'not-a-date' fails modernc's DATE parse → raw string →
	// sluice's iso layout match fails → loud refuse. If this ever passes
	// silently, the silent-fidelity hole has re-opened.
	t.Run("iso unparseable text in DATE column refuses (real driver)", func(t *testing.T) {
		path := seedDB(t, `CREATE TABLE t (d DATE)`, `INSERT INTO t (d) VALUES ('not-a-date')`)
		dsn := path + "?" + dsnDateEncodingParam + "=iso"
		eng := Engine{}
		ctx := context.Background()
		sr, err := eng.OpenSchemaReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenSchemaReader: %v", err)
		}
		defer func() { _ = sr.(*SchemaReader).Close() }()
		schema, err := sr.ReadSchema(ctx)
		if err != nil {
			t.Fatalf("ReadSchema: %v", err)
		}
		rr, err := eng.OpenRowReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowReader: %v", err)
		}
		defer func() { _ = rr.(*RowReader).Close() }()
		ch, err := rr.ReadRows(ctx, tableByName(schema, "t"))
		if err != nil {
			t.Fatalf("ReadRows: %v", err)
		}
		for range ch { //nolint:revive // drain
		}
		if err := rr.Err(); err == nil {
			t.Fatal("unparseable date read SUCCEEDED; want a loud refusal (silent-fidelity hole re-opened)")
		} else if !strings.Contains(err.Error(), "mismatch") && !strings.Contains(strings.ToLower(err.Error()), "date") {
			t.Errorf("refusal error = %v; want it to name the date mismatch", err)
		}
	})
}

// readTableEnc reads a table through the Engine with a per-source
// sqlite_date_encoding DSN param.
func readTableEnc(t *testing.T, path, table, enc string) []ir.Row {
	t.Helper()
	dsn := path + "?" + dsnDateEncodingParam + "=" + enc
	eng := Engine{}
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := tableByName(schema, table)
	if tbl == nil {
		t.Fatalf("table %q missing", table)
	}
	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(*RowReader).Close() }()
	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("Err after read of %q: %v", table, err)
	}
	return out
}

// temporalClose compares a decoded temporal value (time.Time for Date/
// Timestamp, textual time-of-day for Time) against want within tol. A tol of
// 0 requires an exact match (string-equal for Time).
func temporalClose(t *testing.T, got, want any, tol time.Duration) bool {
	t.Helper()
	switch w := want.(type) {
	case time.Time:
		g, ok := got.(time.Time)
		if !ok {
			t.Errorf("got %T; want time.Time", got)
			return false
		}
		return absDuration(g.Sub(w)) <= tol
	case string:
		g, ok := got.(string)
		if !ok {
			t.Errorf("got %T; want string", got)
			return false
		}
		if tol == 0 {
			return g == w
		}
		gt, err1 := time.Parse("15:04:05.999999999", g)
		wt, err2 := time.Parse("15:04:05.999999999", w)
		if err1 != nil || err2 != nil {
			t.Errorf("parse time-of-day %q/%q: %v/%v", g, w, err1, err2)
			return false
		}
		return absDuration(gt.Sub(wt)) <= tol
	default:
		t.Errorf("unsupported want type %T", want)
		return false
	}
}

func assertTime(t *testing.T, got any, want time.Time) {
	t.Helper()
	g, ok := got.(time.Time)
	if !ok {
		t.Fatalf("got %#v (%T); want time.Time", got, got)
	}
	if !g.Equal(want) {
		t.Errorf("got %s; want %s", g.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func assertTimeClose(t *testing.T, got any, want time.Time, tol time.Duration) {
	t.Helper()
	g, ok := got.(time.Time)
	if !ok {
		t.Fatalf("got %#v (%T); want time.Time", got, got)
	}
	if absDuration(g.Sub(want)) > tol {
		t.Errorf("got %s; want ~%s (tol %v)", g.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano), tol)
	}
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
