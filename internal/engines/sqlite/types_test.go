// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestAffinityOf pins SQLite's declared-type → affinity rules
// (https://www.sqlite.org/datatype3.html §3.1) IN ORDER, including the
// rule-precedence traps: "INT" wins over "CHAR"/"FLOA" when both substrings
// are present, and an empty declared type is BLOB affinity (the no-type case).
func TestAffinityOf(t *testing.T) {
	cases := []struct {
		declared string
		want     affinity
	}{
		// Rule 1 — INTEGER affinity (contains "INT").
		{"INT", affinityInteger},
		{"INTEGER", affinityInteger},
		{"TINYINT", affinityInteger},
		{"BIGINT", affinityInteger},
		{"UNSIGNED BIG INT", affinityInteger},
		{"int8", affinityInteger},
		{"POINT", affinityInteger}, // contains "INT" — rule 1 fires (a real SQLite trap)
		// Rule 2 — TEXT affinity (CHAR/CLOB/TEXT).
		{"CHARACTER(20)", affinityText},
		{"VARCHAR(255)", affinityText},
		{"NCHAR(55)", affinityText},
		{"TEXT", affinityText},
		{"CLOB", affinityText},
		// Rule 1 precedence over rule 2: contains both "INT" and "CHAR".
		{"INT CHAR", affinityInteger},
		// Rule 3 — BLOB affinity (BLOB, or no declared type).
		{"BLOB", affinityBlob},
		{"", affinityBlob},
		// Rule 4 — REAL affinity (REAL/FLOA/DOUBLE).
		{"REAL", affinityReal},
		{"DOUBLE", affinityReal},
		{"DOUBLE PRECISION", affinityReal},
		{"FLOAT", affinityReal},
		// Rule 1 precedence over rule 4: "FLOATING POINT" has INT and FLOA.
		{"FLOATING POINT", affinityInteger},
		// Rule 5 — NUMERIC affinity (everything else).
		{"NUMERIC", affinityNumeric},
		{"DECIMAL(10,5)", affinityNumeric},
		{"BOOLEAN", affinityNumeric}, // no INT/CHAR/BLOB/REAL substring
		{"DATE", affinityNumeric},
		{"DATETIME", affinityNumeric},
		// Case-insensitivity.
		{"varchar(10)", affinityText},
		{"real", affinityReal},
	}
	for _, c := range cases {
		if got := affinityOf(c.declared); got != c.want {
			t.Errorf("affinityOf(%q) = %s; want %s", c.declared, got, c.want)
		}
	}
}

// TestIRTypeFor pins the affinity → IR type mapping. This is the schema
// half of the value-fidelity contract.
func TestIRTypeFor(t *testing.T) {
	cases := []struct {
		aff  affinity
		want ir.Type
	}{
		{affinityInteger, ir.Integer{Width: 64}},
		{affinityText, ir.Text{Size: ir.TextLong}},
		{affinityBlob, ir.Blob{Size: ir.BlobLong}},
		{affinityReal, ir.Float{Precision: ir.FloatDouble}},
		{affinityNumeric, ir.Decimal{Unconstrained: true}},
	}
	for _, c := range cases {
		got := irTypeFor(c.aff)
		if got != c.want {
			t.Errorf("irTypeFor(%s) = %#v; want %#v", c.aff, got, c.want)
		}
	}
}

// TestResolveColumnType pins ADR-0129's declared-type → IR resolution: the
// temporal/bool inference takes precedence over the affinity default for the
// explicit spellings, the load-bearing precedence order is honored (DATETIME/
// TIMESTAMP before DATE before TIME), the BIGDATE-style substring
// false-positive is acknowledged, and every non-temporal/bool declared type
// still follows the affinity mapping unchanged.
func TestResolveColumnType(t *testing.T) {
	cases := []struct {
		declared string
		want     ir.Type
	}{
		// Temporal/bool overrides (the new policy).
		{"DATE", ir.Date{}},
		{"date", ir.Date{}},
		// PrecisionUnspecified: SQLite temporals carry no declared
		// fractional-second precision (TRIAGE #3) — the PG writer emits
		// the bare form, the MySQL writer materializes (6).
		{"DATETIME", ir.Timestamp{PrecisionUnspecified: true}}, // DATETIME wins over the DATE/TIME substrings
		{"TIMESTAMP", ir.Timestamp{PrecisionUnspecified: true}},
		{"TIMESTAMPTZ", ir.Timestamp{PrecisionUnspecified: true}}, // still tz-naive in SQLite
		{"TIME", ir.Time{PrecisionUnspecified: true}},
		{"BOOL", ir.Boolean{}},
		{"BOOLEAN", ir.Boolean{}},
		{"boolean", ir.Boolean{}},
		// Precedence: "DATETIME" (no separator) is one token → Timestamp, but
		// "DATE_TIME" contains the "DATE" substring before "TIME", so DATE wins.
		{"DATE_TIME", ir.Date{}},
		{"BIGDATE", ir.Date{}}, // documented substring false-positive
		// Affinity fallthrough — NOT temporal/bool, so affinity rules apply.
		{"INTEGER", ir.Integer{Width: 64}}, // an INTEGER 0/1 flag is NOT guessed as bool
		{"INT", ir.Integer{Width: 64}},
		{"VARCHAR(255)", ir.Text{Size: ir.TextLong}},
		{"TEXT", ir.Text{Size: ir.TextLong}},
		{"REAL", ir.Float{Precision: ir.FloatDouble}},
		{"NUMERIC", ir.Decimal{Unconstrained: true}},
		{"DECIMAL(10,2)", ir.Decimal{Unconstrained: true}},
		{"BLOB", ir.Blob{Size: ir.BlobLong}},
		{"", ir.Blob{Size: ir.BlobLong}}, // no declared type → BLOB affinity
	}
	for _, c := range cases {
		got := resolveColumnType(c.declared)
		if got != c.want {
			t.Errorf("resolveColumnType(%q) = %#v; want %#v", c.declared, got, c.want)
		}
	}
}

// TestDeclaredTemporalBoolType pins the precedence directly, including the
// negative case (a non-temporal/bool declared type yields ok=false so the
// caller falls back to affinity).
func TestDeclaredTemporalBoolType(t *testing.T) {
	cases := []struct {
		declared string
		want     ir.Type
		wantOK   bool
	}{
		{"DATETIME", ir.Timestamp{PrecisionUnspecified: true}, true},
		{"TIMESTAMP", ir.Timestamp{PrecisionUnspecified: true}, true},
		{"DATE", ir.Date{}, true},
		{"TIME", ir.Time{PrecisionUnspecified: true}, true},
		{"BOOLEAN", ir.Boolean{}, true},
		{"BOOL", ir.Boolean{}, true},
		{"INTEGER", nil, false},
		{"VARCHAR(10)", nil, false},
		{"NUMERIC", nil, false},
		{"", nil, false},
	}
	for _, c := range cases {
		got, ok := declaredTemporalBoolType(c.declared)
		if ok != c.wantOK {
			t.Errorf("declaredTemporalBoolType(%q) ok = %v; want %v", c.declared, ok, c.wantOK)
			continue
		}
		if ok && got != c.want {
			t.Errorf("declaredTemporalBoolType(%q) = %#v; want %#v", c.declared, got, c.want)
		}
	}
}

// TestParseDateEncoding pins the shared encoding parser used by both the
// global flag setter and the per-source DSN param (so they can't drift).
func TestParseDateEncoding(t *testing.T) {
	cases := []struct {
		in      string
		want    dateEncoding
		wantErr bool
	}{
		{"", dateEncodingISO, false},
		{"iso", dateEncodingISO, false},
		{"ISO", dateEncodingISO, false},
		{"unixepoch", dateEncodingUnixEpoch, false},
		{"unixmillis", dateEncodingUnixMillis, false},
		{"julian", dateEncodingJulian, false},
		{"  julian  ", dateEncodingJulian, false},
		{"bogus", dateEncodingInherit, true},
	}
	for _, c := range cases {
		got, err := parseDateEncoding(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("parseDateEncoding(%q) err = %v; wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if !c.wantErr && got != c.want {
			t.Errorf("parseDateEncoding(%q) = %v; want %v", c.in, got, c.want)
		}
	}
}
