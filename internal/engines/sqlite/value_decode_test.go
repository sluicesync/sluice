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
	}

	classes := []sc{null, integer, realv, text, blob}
	for _, m := range matrix {
		for _, c := range classes {
			exp := m.rows[c.name]
			got, err := decodeCell(c.val, m.irType)
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
