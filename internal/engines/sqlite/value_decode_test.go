// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDecodeCell pins the FULL affinity × storage-class fidelity matrix at
// the decode boundary — every IR type (the resolved affinity) crossed with
// every Go storage-class value modernc.org/sqlite can hand back. Each cell
// asserts EITHER a faithful IR value OR a loud refusal; never a silent
// coercion. This is the value-fidelity heart of the engine (CLAUDE.md
// "pin the class, not the representative").
func TestDecodeCell(t *testing.T) {
	const (
		faithful = "faithful"
		refuse   = "refuse"
	)
	// The five storage-class representatives modernc returns for *any scans.
	type sc struct {
		name string
		val  any
	}
	null := sc{"NULL", nil}
	integer := sc{"INTEGER", int64(7)}
	realv := sc{"REAL", float64(1.5)}
	text := sc{"TEXT", "hi"}
	blob := sc{"BLOB", []byte{0x01, 0xff}}

	type expect struct {
		outcome string
		want    any // only checked when outcome==faithful and non-nil
	}
	matrix := []struct {
		irType ir.Type
		name   string
		rows   map[string]expect // keyed by storage-class name
	}{
		{
			ir.Integer{Width: 64},
			"Integer",
			map[string]expect{
				"NULL":    {faithful, nil},
				"INTEGER": {faithful, int64(7)},
				"REAL":    {refuse, nil},
				"TEXT":    {refuse, nil},
				"BLOB":    {refuse, nil},
			},
		},
		{
			ir.Float{Precision: ir.FloatDouble},
			"Float",
			map[string]expect{
				"NULL":    {faithful, nil},
				"INTEGER": {refuse, nil},
				"REAL":    {faithful, float64(1.5)},
				"TEXT":    {refuse, nil},
				"BLOB":    {refuse, nil},
			},
		},
		{
			ir.Text{Size: ir.TextLong},
			"Text",
			map[string]expect{
				"NULL":    {faithful, nil},
				"INTEGER": {refuse, nil},
				"REAL":    {refuse, nil},
				"TEXT":    {faithful, "hi"},
				"BLOB":    {refuse, nil},
			},
		},
		{
			ir.Blob{Size: ir.BlobLong},
			"Blob",
			map[string]expect{
				"NULL":    {faithful, nil},
				"INTEGER": {refuse, nil},
				"REAL":    {refuse, nil},
				"TEXT":    {refuse, nil},
				"BLOB":    {faithful, []byte{0x01, 0xff}},
			},
		},
		{
			ir.Decimal{Unconstrained: true},
			"Decimal",
			map[string]expect{
				"NULL":    {faithful, nil},
				"INTEGER": {faithful, "7"},   // int64 → exact decimal string
				"REAL":    {faithful, "1.5"}, // float64 → shortest round-trippable string
				"TEXT":    {refuse, nil},     // non-numeric text in a NUMERIC column
				"BLOB":    {refuse, nil},
			},
		},
		// Bug 161: the string-affinity family that arrives only via
		// `--type-override` (Varchar/Char/JSON/UUID) decodes exactly like Text
		// — a SQLite TEXT value carries; any other storage class is a loud
		// mismatch. Before the fix these hit "no decoder for IR type" (a crash,
		// not a refusal), breaking the documented `--type-override` escape on
		// SQLite sources.
		{
			ir.Varchar{Length: 255},
			"Varchar",
			map[string]expect{
				"NULL": {faithful, nil}, "INTEGER": {refuse, nil}, "REAL": {refuse, nil},
				"TEXT": {faithful, "hi"}, "BLOB": {refuse, nil},
			},
		},
		{
			ir.Char{Length: 8},
			"Char",
			map[string]expect{
				"NULL": {faithful, nil}, "INTEGER": {refuse, nil}, "REAL": {refuse, nil},
				"TEXT": {faithful, "hi"}, "BLOB": {refuse, nil},
			},
		},
		{
			ir.JSON{},
			"JSON",
			map[string]expect{
				"NULL": {faithful, nil}, "INTEGER": {refuse, nil}, "REAL": {refuse, nil},
				"TEXT": {faithful, "hi"}, "BLOB": {refuse, nil},
			},
		},
		{
			ir.UUID{},
			"UUID",
			map[string]expect{
				"NULL": {faithful, nil}, "INTEGER": {refuse, nil}, "REAL": {refuse, nil},
				"TEXT": {faithful, "hi"}, "BLOB": {refuse, nil},
			},
		},
		// The binary-affinity family that arrives via `--type-override`
		// (Binary/Varbinary) decodes exactly like Blob.
		{
			ir.Binary{Length: 16},
			"Binary",
			map[string]expect{
				"NULL": {faithful, nil}, "INTEGER": {refuse, nil}, "REAL": {refuse, nil},
				"TEXT": {refuse, nil}, "BLOB": {faithful, []byte{0x01, 0xff}},
			},
		},
		{
			ir.Varbinary{Length: 16},
			"Varbinary",
			map[string]expect{
				"NULL": {faithful, nil}, "INTEGER": {refuse, nil}, "REAL": {refuse, nil},
				"TEXT": {refuse, nil}, "BLOB": {faithful, []byte{0x01, 0xff}},
			},
		},
	}

	classes := []sc{null, integer, realv, text, blob}
	for _, m := range matrix {
		for _, c := range classes {
			exp := m.rows[c.name]
			// Affinity types ignore the encoding; iso is an arbitrary concrete
			// pick. The temporal/bool matrices are pinned separately.
			got, err := decodeCell(c.val, m.irType, dateEncodingISO)
			switch exp.outcome {
			case faithful:
				if err != nil {
					t.Errorf("%s × %s: unexpected refusal: %v", m.name, c.name, err)
					continue
				}
				if !valuesEqual(got, exp.want) {
					t.Errorf("%s × %s: decoded %#v; want %#v", m.name, c.name, got, exp.want)
				}
			case refuse:
				if err == nil {
					t.Errorf("%s × %s: got faithful %#v; want a LOUD refusal (silent coercion is the failure mode)", m.name, c.name, got)
					continue
				}
				// The refusal must name the offending storage class so the
				// operator can locate it.
				if !strings.Contains(err.Error(), c.name) || !strings.Contains(err.Error(), "mismatch") {
					t.Errorf("%s × %s: refusal %q must name the storage class %q and say \"mismatch\"", m.name, c.name, err.Error(), c.name)
				}
			}
		}
	}
}

// TestDecodeDecimalPlainDigits pins Bug 163: a REAL stored in a NUMERIC/DECIMAL
// affinity column must decode to a PLAIN-DIGIT decimal string, never exponent
// notation. The 'g' verb sluice used before emitted "1e-10"/"1.23456789e+06"
// for magnitudes < 1e-4 or >= 1e6, which pgx's numeric (OID 1700) BINARY COPY
// encoder cannot encode -> the SQLite->PG migrate aborted at COPY. The 'f' verb
// renders the SAME shortest round-trippable value in plain digits. Includes the
// bug163-repro values plus the empirical 'g'-exponent thresholds (>= 1e6 and
// < 1e-4) that hit ordinary money values. Shared by the file reader AND the d1
// reader (both go through decodeCell).
func TestDecodeDecimalPlainDigits(t *testing.T) {
	cases := []struct {
		name string
		val  float64
		want string
	}{
		// bug163-repro/bug163.db values (declared DECIMAL(38,10), stored REAL).
		{"repro_19.99", 19.99, "19.99"},
		{"repro_0.1", 0.1, "0.1"},
		{"repro_1e-10", 0.0000000001, "0.0000000001"},
		// 12345678901234567890.12 is beyond float64 precision; SQLite stored it
		// as the nearest double, which 'f' renders plain (encodable by pgx).
		{"repro_big", 12345678901234567890.12, "12345678901234567000"},
		// The 'g' exponent thresholds that broke ordinary money values.
		{"money_>=1e6", 1234567.89, "1234567.89"},
		{"exactly_1e6", 1000000.0, "1000000"},
		{"rate_<1e-4", 0.00001, "0.00001"},
		// Normal-magnitude values: 'f' and 'g' are byte-identical here, so the
		// fix changes ONLY the rendering of the exponent cases.
		{"normal_999999.9999", 999999.9999, "999999.9999"},
		{"normal_0.0001", 0.0001, "0.0001"},
	}
	dec := ir.Decimal{Unconstrained: true}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeCell(c.val, dec, dateEncodingISO)
			if err != nil {
				t.Fatalf("decodeCell(%v): unexpected error: %v", c.val, err)
			}
			s, ok := got.(string)
			if !ok {
				t.Fatalf("decodeCell(%v): got %T; want decimal string", c.val, got)
			}
			if s != c.want {
				t.Errorf("decodeCell(%v) = %q; want %q", c.val, s, c.want)
			}
			if strings.ContainsAny(s, "eE") {
				t.Errorf("decodeCell(%v) = %q contains exponent notation (pgx numeric COPY cannot encode it)", c.val, s)
			}
		})
	}
}

// valuesEqual compares decoded IR values for the test, handling the []byte
// case (which == can't compare) and nil.
func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if ab, ok := a.([]byte); ok {
		bb, ok := b.([]byte)
		if !ok || len(ab) != len(bb) {
			return false
		}
		for i := range ab {
			if ab[i] != bb[i] {
				return false
			}
		}
		return true
	}
	return a == b
}
