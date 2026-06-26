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
