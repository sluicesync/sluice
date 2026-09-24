// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
)

// TestReconcileMariaDBBinaryDefault_AcceptsEveryMeasuredShape runs the
// reconciliation over the whole GC-36 catalog matrix: each measured
// COLUMN_DEFAULT must accept its true DEFAULT() bytes.
func TestReconcileMariaDBBinaryDefault_AcceptsEveryMeasuredShape(t *testing.T) {
	for _, r := range gc36MariaDBBinaryCatalogRows() {
		if r.probeHex == "" {
			continue
		}
		b, err := hex.DecodeString(r.probeHex)
		if err != nil {
			t.Fatalf("%s: bad fixture hex %q", r.name, r.probeHex)
		}
		if err := reconcileMariaDBBinaryDefault(r.def.String, b); err != nil {
			t.Errorf("%s: catalog %q vs probe %s: %v", r.name, r.def.String, r.probeHex, err)
		}
	}
}

// TestReconcileMariaDBBinaryDefault_RefusesDisagreement pins the loud arms:
// a probe that does not match the catalog at a byte the catalog kept, a
// length mismatch, and a catalog value that is not one quoted literal each
// refuse rather than trust either side.
func TestReconcileMariaDBBinaryDefault_RefusesDisagreement(t *testing.T) {
	cases := []struct {
		name, catalog, probe, want string
	}{
		{"kept byte differs", `'\0?\0'`, "01AB00", "at byte 0"},
		{"probe longer", `'?\0?'`, "AB00CD00", "read 4 bytes"},
		{"probe shorter", `'?\0?'`, "AB00", "read 2 bytes"},
		{"unterminated catalog", `'\0?`, "00AB", "not a single quoted literal"},
		{"trailing text after literal", `'?' x`, "AB", "not a single quoted literal"},
	}
	for _, c := range cases {
		b, _ := hex.DecodeString(c.probe)
		err := reconcileMariaDBBinaryDefault(c.catalog, b)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v; want one containing %q", c.name, err, c.want)
		}
	}
}

// TestMariaDBHexLiteralBytes pins the MariaDB 11.8+ x'<hex>' parser: only a
// whole, non-empty, even-length literal is this form; anything else falls
// through to the translator's other branches.
func TestMariaDBHexLiteralBytes(t *testing.T) {
	cases := []struct {
		raw, want string
		ok        bool
	}{
		{"x'00ab00'", "00AB00", true},
		{"X'FF'", "FF", true},
		{"x''", "", false},
		{"x'abc'", "", false},
		{"x'zz'", "", false},
		{"x'ab' ", "", false},
		{"0xab", "", false},
		{"'ab'", "", false},
	}
	for _, c := range cases {
		b, ok := mariadbHexLiteralBytes(c.raw)
		if ok != c.ok || (ok && strings.ToUpper(hex.EncodeToString(b)) != c.want) {
			t.Errorf("%q: got (%X, %v); want (%s, %v)", c.raw, b, ok, c.want, c.ok)
		}
	}
}

// TestMariaDBBinaryDefaultNeedsProbe pins which catalog shapes are re-read:
// only a non-empty quoted literal on a binary-family column.
func TestMariaDBBinaryDefaultNeedsProbe(t *testing.T) {
	v := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	cases := []struct {
		name   string
		binary bool
		def    sql.NullString
		want   bool
	}{
		{"quoted high byte", true, v(`'\0?\0'`), true},
		{"quoted ascii", true, v(`'ab\0\0'`), true},
		{"empty literal", true, v(`''`), false},
		{"NULL keyword", true, v("NULL"), false},
		{"SQL NULL", true, sql.NullString{}, false},
		{"expression text", true, v("concat(0xab,uuid())"), false},
		{"non-binary quoted", false, v(`'?'`), false},
	}
	for _, c := range cases {
		if got := mariadbBinaryDefaultNeedsProbe(c.binary, c.def); got != c.want {
			t.Errorf("%s: got %v; want %v", c.name, got, c.want)
		}
	}
}
