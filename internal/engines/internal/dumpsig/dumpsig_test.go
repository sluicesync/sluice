// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package dumpsig

import (
	"testing"
)

// TestDetectHead pins the signature table: every recognised head shape and
// the near-miss negatives (the pg_dump marker only counts at a line start
// inside the head; ordinary CSV/JSON heads classify unknown).
func TestDetectHead(t *testing.T) {
	cases := []struct {
		name string
		head string
		want Kind
	}{
		{"pgdmp magic", "PGDMP\x01\x0e\x00", KindPGDumpCustom},
		{"sqlite magic", "SQLite format 3\x00", KindSQLiteDB},
		{"gzip magic", "\x1f\x8b\x08\x00", KindGzip},
		{"zstd magic", "\x28\xb5\x2f\xfd", KindZstd},
		{"utf16 le bom", "\xff\xfea\x00", KindUTF16},
		{"utf16 be bom", "\xfe\xffa\x00", KindUTF16},
		{"utf32 be bom", "\x00\x00\xfe\xff", KindUTF16},
		{"mysqldump banner", "-- MySQL dump 10.13  Distrib 8.0.36\n--\n", KindMySQLDumpSQL},
		{"pg_dump banner at head", "-- PostgreSQL database dump\n", KindPGDumpSQL},
		{"pg_dump banner after comment line", "--\n-- PostgreSQL database dump\n--\n", KindPGDumpSQL},
		{"sqlite3 .dump text", "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n", KindUnknown},
		{"csv head", "a,b,c\n1,2,3\n", KindUnknown},
		{"ndjson head", `{"a":1}` + "\n", KindUnknown},
		{"pg marker mid-field is not a line start", `x,"-- PostgreSQL database dump"` + "\n", KindUnknown},
		{"empty", "", KindUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DetectHead([]byte(tc.head)); got != tc.want {
				t.Errorf("DetectHead(%q) = %v; want %v", tc.head, got, tc.want)
			}
		})
	}
}

// TestFlatFileExtDriver pins the extension → driver hint map.
func TestFlatFileExtDriver(t *testing.T) {
	cases := map[string]string{
		"a.csv": "csv", "B.CSV": "csv", "a.tsv": "tsv",
		"a.ndjson": "ndjson", "a.jsonl": "ndjson",
	}
	for in, want := range cases {
		got, ok := FlatFileExtDriver(in)
		if !ok || got != want {
			t.Errorf("FlatFileExtDriver(%q) = %q,%v; want %q,true", in, got, ok, want)
		}
	}
	for _, in := range []string{"a.sql", "a.db", "a.json", "a.txt", "a"} {
		if got, ok := FlatFileExtDriver(in); ok {
			t.Errorf("FlatFileExtDriver(%q) = %q; want no hint", in, got)
		}
	}
}
