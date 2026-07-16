// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"sluicesync.dev/sluice/internal/ir"
)

// openInferReader seeds a temp SQLite file and returns its file SchemaReader
// (which implements ir.InferredTypeValidator). The validator runs the SAME
// aggregate SQL over the D1 transport, so pinning the file path pins the GLOB /
// json_valid / NOT-IN logic for both readers (the only difference is the row
// source).
func openInferReader(t *testing.T, stmts ...string) *SchemaReader {
	t.Helper()
	path := seedDB(t, stmts...)
	sr, err := Engine{}.OpenSchemaReader(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	r := sr.(*SchemaReader)
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// TestValidateInferredType_PerFamily pins EVERY inference family × shape (the
// Bug-74 discipline): each rich target has a CONFORMING column (promoted) and a
// NON-CONFORMING column with the same shape (kept). The temporal family also
// pins the tz resolution (all-offset → timestamptz; any-naive → timestamp) and
// the date-only shape. NULLs are skipped (they never contradict).
func TestValidateInferredType_PerFamily(t *testing.T) {
	r := openInferReader(
		t, `
		CREATE TABLE t (
			id        INTEGER PRIMARY KEY,
			bool_ok   INTEGER,
			bool_bad  INTEGER,
			ts_off    TEXT,
			ts_naive  TEXT,
			ts_mixed  TEXT,
			ts_date   TEXT,
			ts_bad    TEXT,
			ts_mixnz  TEXT,
			ts_subus  TEXT,
			ts_frac   TEXT,
			js_ok     TEXT,
			js_num    TEXT,
			js_free   TEXT,
			uuid_lc   TEXT,
			uuid_uc   TEXT,
			uuid_bad  TEXT,
			all_null  TEXT
		)`,
		`INSERT INTO t VALUES (
			1, 1, 2,
			'2024-01-15T10:30:00+05:00', '2024-01-15 10:30:00', '2024-01-15T10:30:00Z',
			'2024-01-15', 'not a date',
			'2024-01-15T10:30:00+05:00', '2024-01-15 10:30:00.1234567', '2024-01-15T10:30:00.123456+05:00',
			'{"a":1}', '123', 'free',
			'550e8400-e29b-41d4-a716-446655440000', '550E8400-E29B-41D4-A716-446655440000', 'cus_abc123',
			NULL)`,
		`INSERT INTO t VALUES (
			2, 0, 0,
			'2024-02-20T08:00:00-08:00', '2024-02-20 08:00:00', '2024-02-20 08:00:00',
			'2024-02-20', '2024-02-20',
			'2024-02-20 08:00:00', '2024-02-20 08:00:00.7654321', '2024-02-20T08:00:00.654321Z',
			'[1,2,3]', '456', '"hello"',
			'6ba7b810-9dad-11d1-80b4-00c04fd430c8', '6BA7B810-9DAD-11D1-80B4-00C04FD430C8', 'not-uuid',
			NULL)`,
		// Row 3 is all-NULL (except the PK) so every "ok" column has a NULL the
		// validation must skip — conformance must hold despite it.
		`INSERT INTO t VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
	)

	tsTZ := ir.Timestamp{Precision: 6, WithTimeZone: true}
	tsNaive := ir.Timestamp{Precision: 6, WithTimeZone: false}

	cases := []struct {
		col          string
		target       ir.Type
		wantConforms bool
		wantResolved ir.Type // checked only when wantConforms
		wantValid    int64
	}{
		{"bool_ok", ir.Boolean{}, true, ir.Boolean{}, 2},
		{"bool_bad", ir.Boolean{}, false, nil, 2},

		{"ts_off", ir.Timestamp{}, true, tsTZ, 2},
		{"ts_naive", ir.Timestamp{}, true, tsNaive, 2},
		{"ts_mixed", ir.Timestamp{}, false, nil, 2},   // MIXED offset(`Z`)+naive → REFUSED (value-fidelity fix)
		{"ts_date", ir.Timestamp{}, true, tsNaive, 2}, // bare date → naive
		{"ts_bad", ir.Timestamp{}, false, nil, 2},     // 'not a date' contradicts
		{"ts_mixnz", ir.Timestamp{}, false, nil, 2},   // MIXED +05:00 offset + naive → REFUSED (would silently UTC-shift the offset value into a naive column — the review's BLOCK)
		{"ts_subus", ir.Timestamp{}, false, nil, 2},   // >6 fractional digits → REFUSED (would silently round under timestamp(6))
		{"ts_frac", ir.Timestamp{}, true, tsTZ, 2},    // all-offset, exactly 6 frac digits → timestamptz(6) (precision contract)

		{"js_ok", ir.JSON{Binary: true}, true, ir.JSON{Binary: true}, 2},
		{"js_num", ir.JSON{Binary: true}, false, nil, 2},  // '123' is a bare number
		{"js_free", ir.JSON{Binary: true}, false, nil, 2}, // 'free' invalid / '"hello"' is a string

		{"uuid_lc", ir.UUID{}, true, ir.UUID{}, 2},
		{"uuid_uc", ir.UUID{}, true, ir.UUID{}, 2}, // upper-case hex (case-insensitive GLOB)
		{"uuid_bad", ir.UUID{}, false, nil, 2},     // 'cus_abc123' — the pscale data-loss case

		{"all_null", ir.Timestamp{}, false, nil, 0}, // no non-NULL values → never promote
	}

	for _, tc := range cases {
		t.Run(tc.col, func(t *testing.T) {
			conforms, resolved, validated, err := r.ValidateInferredType(context.Background(), "t", tc.col, tc.target)
			if err != nil {
				t.Fatalf("ValidateInferredType(%s): %v", tc.col, err)
			}
			if conforms != tc.wantConforms {
				t.Fatalf("%s: conforms=%v want %v", tc.col, conforms, tc.wantConforms)
			}
			if validated != tc.wantValid {
				t.Fatalf("%s: validated=%d want %d", tc.col, validated, tc.wantValid)
			}
			if tc.wantConforms && resolved != tc.wantResolved {
				t.Fatalf("%s: resolved=%v want %v", tc.col, resolved, tc.wantResolved)
			}
		})
	}
}

// TestValidateInferredType_ZoneSpellingMatrix pins the FULL zone-spelling
// classification (ADR-0163 F2 — validator==decoder over separator {space,T}
// × zone {Z, ±hh:mm, ±hhmm, ±hh} × fraction): each spelling column must
// resolve TIMESTAMPTZ (a spelling classified naive here but zoned by the
// decoder would abort mid-copy on a value the validator blessed — the
// original PG-COPY `…+00` finding); bare DATES must stay NAIVE (a date's
// `-02` tail is exactly the ±hh shape — the false-positive the anchored
// globs exist to avoid, since a misclassified date column would invent a
// zone); and a pg-style zoned value mixed with a naive one stays refused.
//
// Every zone spelling also has a whitespace-PADDED twin (the 2026-07-15
// audit's HIGH-2, Bug-74 discipline: each spelling family × the padded
// shape): the end-anchored zone globs run over the TRIMmed value, so a
// padded tail must not hide the zone suffix and demote the column to naive
// (which would silently UTC-shift every value at decode). Each twin uses a
// DIFFERENT pad character — space, tab, CR, CRLF, and NBSP — so the pin also
// covers the trim charset's Unicode breadth (wsTrimSQL == strings.TrimSpace).
func TestValidateInferredType_ZoneSpellingMatrix(t *testing.T) {
	r := openInferReader(
		t, `
		CREATE TABLE z (
			id           INTEGER PRIMARY KEY,
			ts_pgcopy    TEXT,
			ts_hh_t      TEXT,
			ts_hhmm      TEXT,
			ts_colon     TEXT,
			ts_zulu      TEXT,
			ts_dates     TEXT,
			ts_mix_pg    TEXT,
			ts_pg_pad    TEXT,
			ts_hh_t_pad  TEXT,
			ts_hhmm_pad  TEXT,
			ts_colon_pad TEXT,
			ts_zulu_pad  TEXT,
			ts_dates_pad TEXT,
			ts_naive_pad TEXT,
			ts_mix_pad   TEXT,
			ts_blank     TEXT
		)`,
		// Row 1: the Postgres COPY CSV timestamptz rendering (space separator,
		// fraction, 2-digit offset), every sibling zone spelling, and the
		// padded twins (built with Go escapes — the pad bytes are literal).
		"INSERT INTO z VALUES (1,\n"+
			"'2026-07-15 08:09:10.123456+00',\n"+
			"'2026-07-15T08:09:10.123456+02',\n"+
			"'2026-07-15 08:09:10+0530',\n"+
			"'2026-07-15T08:09:10-05:00',\n"+
			"'2026-07-15 08:09:10.123456Z',\n"+
			"'2024-01-02',\n"+
			"'2026-07-15 08:09:10+00',\n"+
			"'2026-07-15 08:09:10.123456+00 ',\n"+ // trailing space (the audit's exact repro shape)
			"'2026-07-15T08:09:10.123456+02\t',\n"+ // trailing tab
			"'2026-07-15 08:09:10+0530\r',\n"+ // trailing CR (CRLF-file residue)
			"'2026-07-15T08:09:10-05:00\r\n',\n"+ // trailing CRLF
			"'2026-07-15 08:09:10.123456Z\u00a0',\n"+ // trailing NBSP (the Unicode-wide charset)
			"'2024-01-02 ',\n"+
			"'2026-07-15 08:09:10 ',\n"+
			"'2026-07-15 08:09:10+02 ',\n"+ // padded zoned half of the mix
			"'2026-07-15 08:09:10')",
		// Row 2: same shapes, no-fraction / negative / leading-pad variants;
		// ts_mix_pg and ts_mix_pad go naive here (the mixed refusal), ts_dates
		// stays a bare date, ts_blank is ALL whitespace (trims to '' → the
		// whole column is kept text).
		"INSERT INTO z VALUES (2,\n"+
			"'2026-07-14 07:08:09+00',\n"+
			"'2026-07-14T07:08:09-08',\n"+
			"'2026-07-14T07:08:09.5-0800',\n"+
			"'2026-07-14 07:08:09+05:30',\n"+
			"'2026-07-14T07:08:09Z',\n"+
			"'2024-11-30',\n"+
			"'2026-07-14 07:08:09',\n"+
			"' 2026-07-14 07:08:09+00 ',\n"+ // leading AND trailing
			"'2026-07-14T07:08:09-08\t',\n"+
			"'2026-07-14T07:08:09.5-0800\r',\n"+
			"'2026-07-14 07:08:09+05:30\r\n',\n"+
			"'2026-07-14T07:08:09Z\u00a0',\n"+
			"'\t2024-11-30',\n"+ // leading tab on the bare date
			"'2026-07-14 07:08:09\r\n',\n"+
			"'2026-07-14 07:08:09',\n"+ // naive half of the mix
			"'   ')",
	)

	tsTZ := ir.Timestamp{Precision: 6, WithTimeZone: true}
	tsNaive := ir.Timestamp{Precision: 6, WithTimeZone: false}
	cases := []struct {
		col          string
		wantConforms bool
		wantResolved ir.Type
	}{
		{"ts_pgcopy", true, tsTZ},   // the flagship: space + ±hh
		{"ts_hh_t", true, tsTZ},     // T + ±hh
		{"ts_hhmm", true, tsTZ},     // compact ±hhmm, both separators
		{"ts_colon", true, tsTZ},    // ±hh:mm, both separators
		{"ts_zulu", true, tsTZ},     // Z, both separators
		{"ts_dates", true, tsNaive}, // bare dates: NOT zone-classified
		{"ts_mix_pg", false, nil},   // zoned + naive mix → kept text

		{"ts_pg_pad", true, tsTZ},       // space + ±hh, space-padded → still zoned
		{"ts_hh_t_pad", true, tsTZ},     // T + ±hh, tab-padded
		{"ts_hhmm_pad", true, tsTZ},     // ±hhmm, CR-padded
		{"ts_colon_pad", true, tsTZ},    // ±hh:mm, CRLF-padded
		{"ts_zulu_pad", true, tsTZ},     // Z, NBSP-padded
		{"ts_dates_pad", true, tsNaive}, // padded bare dates: conform, stay naive
		{"ts_naive_pad", true, tsNaive}, // padded naive: unchanged (no invented zone)
		{"ts_mix_pad", false, nil},      // padded-zoned + naive mix → still kept text
		{"ts_blank", false, nil},        // an all-whitespace value conforms to nothing
	}
	for _, tc := range cases {
		t.Run(tc.col, func(t *testing.T) {
			conforms, resolved, validated, err := r.ValidateInferredType(context.Background(), "z", tc.col, ir.Timestamp{})
			if err != nil {
				t.Fatalf("ValidateInferredType(%s): %v", tc.col, err)
			}
			if conforms != tc.wantConforms {
				t.Fatalf("%s: conforms=%v want %v", tc.col, conforms, tc.wantConforms)
			}
			if validated != 2 {
				t.Fatalf("%s: validated=%d want 2", tc.col, validated)
			}
			if tc.wantConforms && resolved != tc.wantResolved {
				t.Fatalf("%s: resolved=%v want %v", tc.col, resolved, tc.wantResolved)
			}
		})
	}
}

// TestValidateInferredType_PaddedValidatorDecoderParity is the end-to-end
// audit-HIGH-2 pin: for whitespace-padded rows the validator's resolution and
// the decoder's parse must AGREE — zoned+pad resolves TIMESTAMPTZ and decodes
// to the source instant (pre-fix it resolved NAIVE and the decode UTC-shifted
// the wall clock, silently, exit 0), naive+pad and bare-date+pad stay naive
// with the wall clock intact. The decode leg runs the promoted column's raw
// PADDED text through the REAL decoder (decodeCell, iso encoding) under the
// exact type the validator resolved.
func TestValidateInferredType_PaddedValidatorDecoderParity(t *testing.T) {
	tsTZ := ir.Timestamp{Precision: 6, WithTimeZone: true}
	tsNaive := ir.Timestamp{Precision: 6, WithTimeZone: false}
	rows := []struct {
		col          string
		raw          string
		wantResolved ir.Timestamp
		wantInstant  time.Time
	}{
		// +05:30 with a trailing space: the instant is the wall clock MINUS
		// the offset. A naive mis-resolution would store 02:39 as the wall
		// clock — the 5.5-hour silent shift this test exists to catch.
		{
			"zoned_pad", "2026-07-15 08:09:10.123456+05:30 ", tsTZ,
			time.Date(2026, 7, 15, 2, 39, 10, 123456000, time.UTC),
		},
		{
			"naive_pad", "2026-07-15 08:09:10\t", tsNaive,
			time.Date(2026, 7, 15, 8, 9, 10, 0, time.UTC),
		},
		{
			"date_pad", "2026-07-15\r\n", tsNaive,
			time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC),
		},
	}

	r := openInferReader(
		t,
		`CREATE TABLE p (id INTEGER PRIMARY KEY, zoned_pad TEXT, naive_pad TEXT, date_pad TEXT)`,
		"INSERT INTO p VALUES (1, '"+rows[0].raw+"', '"+rows[1].raw+"', '"+rows[2].raw+"')",
	)
	for _, tc := range rows {
		t.Run(tc.col, func(t *testing.T) {
			conforms, resolved, _, err := r.ValidateInferredType(context.Background(), "p", tc.col, ir.Timestamp{})
			if err != nil {
				t.Fatalf("ValidateInferredType(%s): %v", tc.col, err)
			}
			if !conforms || resolved != tc.wantResolved {
				t.Fatalf("%s: conforms=%v resolved=%v; want true/%v", tc.col, conforms, resolved, tc.wantResolved)
			}
			// The decoder half of the parity: the same padded raw text, decoded
			// under the resolved type, must land the expected instant.
			got, err := decodeCell(tc.raw, resolved, dateEncodingISO)
			if err != nil {
				t.Fatalf("%s: decoder refused the value the validator blessed: %v", tc.col, err)
			}
			tm, ok := got.(time.Time)
			if !ok {
				t.Fatalf("%s: decoded %T; want time.Time", tc.col, got)
			}
			if !tm.Equal(tc.wantInstant) {
				t.Fatalf("%s: decoded instant %v; want %v (validator promised %v)", tc.col, tm.UTC(), tc.wantInstant, resolved)
			}
		})
	}
}

// TestWSTrimSQL pins the validator's SQL trim charset to EXACTLY the set
// strings.TrimSpace trims (unicode.IsSpace) — the validator==decoder
// whitespace parity is by construction, and this fails if either side ever
// drifts (a validator trimming LESS re-opens the padded-zone UTC shift; one
// trimming MORE promises values the decoder then refuses).
func TestWSTrimSQL(t *testing.T) {
	var cps []string
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if unicode.IsSpace(r) {
			cps = append(cps, strconv.Itoa(int(r)))
		}
	}
	if want := "char(" + strings.Join(cps, ",") + ")"; wsTrimSQL != want {
		t.Fatalf("wsTrimSQL = %s; want %s", wsTrimSQL, want)
	}
}

// TestValidateInferredType_EmptyTable pins the zero-row case: nothing is
// validated, so nothing is promoted (validated=0, conforms=false) regardless of
// family — the empty/all-NULL decision (ADR-0144).
func TestValidateInferredType_EmptyTable(t *testing.T) {
	r := openInferReader(t, `CREATE TABLE e (id INTEGER PRIMARY KEY, c TEXT, n INTEGER)`)
	for _, target := range []ir.Type{ir.Boolean{}, ir.Timestamp{}, ir.JSON{Binary: true}, ir.UUID{}} {
		col := "c"
		if _, isBool := target.(ir.Boolean); isBool {
			col = "n"
		}
		conforms, _, validated, err := r.ValidateInferredType(context.Background(), "e", col, target)
		if err != nil {
			t.Fatalf("empty %T: %v", target, err)
		}
		if conforms || validated != 0 {
			t.Fatalf("empty %T: conforms=%v validated=%d, want false/0", target, conforms, validated)
		}
	}
}

// TestValidateInferredType_QuotedIdentifiers pins that table/column names with
// SQL-significant characters (a double-quote, a space) are quoted safely — the
// validation SQL stays well-formed and the count is correct.
func TestValidateInferredType_QuotedIdentifiers(t *testing.T) {
	r := openInferReader(
		t,
		`CREATE TABLE "we ird" ("my""col" TEXT)`,
		`INSERT INTO "we ird" VALUES ('{"x":1}')`,
	)
	conforms, resolved, validated, err := r.ValidateInferredType(
		context.Background(), "we ird", `my"col`, ir.JSON{Binary: true},
	)
	if err != nil {
		t.Fatalf("ValidateInferredType: %v", err)
	}
	if !conforms || validated != 1 || resolved != (ir.JSON{Binary: true}) {
		t.Fatalf("conforms=%v validated=%d resolved=%v", conforms, validated, resolved)
	}
}

// TestUUIDGlob pins the assembled UUID GLOB shape so a future edit can't silently
// loosen it (32 hex char-classes in the 8-4-4-4-12 grouping).
func TestUUIDGlob(t *testing.T) {
	want := `[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]` +
		`-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]` +
		`-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]` +
		`-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]` +
		`-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]`
	if uuidGlob != want {
		t.Fatalf("uuidGlob = %q want %q", uuidGlob, want)
	}
}
