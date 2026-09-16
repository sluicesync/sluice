// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"
)

// The grammar's own pins. The connection path had NONE before this
// (audit A0915-VF2-PGDSN-1): `TestParseURIDSN_ValidDSNUnchanged` pinned
// the URI form and nothing pinned the key/value form, which is how a
// whitespace split survived there while the identity path was fixed.

// TestStripSettings_QuotedValueIsNotDismembered is the filed defect,
// pinned at the exact string the audit named.
//
// `password='s schema=x' dbname=real` is TWO settings to libpq — a
// password of `s schema=x`, and a dbname of `real`. A `strings.Fields`
// walk sees three tokens, recognises the middle one as a `schema=`
// setting, drops it, and hands the driver `password='s dbname=real`:
// a truncated credential and an unterminated quote.
func TestStripSettings_QuotedValueIsNotDismembered(t *testing.T) {
	const dsn = `password='s schema=x' dbname=real`

	got := stripSettings(dsn, "schema")
	if got != dsn {
		t.Errorf("stripSettings dismembered a quoted value:\n  in:   %s\n  out:  %s\n"+
			"the `schema=x'` run is INSIDE the password, so there is no schema setting to remove", dsn, got)
	}

	// The reason it must not be removed, stated as its own assertion so a
	// future reader does not have to trust the sentence above.
	kv := parseKVFields(dsn)
	if want := "s schema=x"; kv["password"] != want {
		t.Errorf("password = %q, want %q", kv["password"], want)
	}
	if _, ok := kv["schema"]; ok {
		t.Errorf("a `schema` setting was extracted from inside the password: %q", kv["schema"])
	}
}

// TestStripSettings_LeavesEveryOtherByteAlone is the round-trip property
// the span design exists for, and the mirror of the URI path's
// ValidDSNUnchanged pin: a DSN carrying none of the stripped keywords
// comes back BYTE-IDENTICAL, because nothing is ever re-rendered.
func TestStripSettings_LeavesEveryOtherByteAlone(t *testing.T) {
	for _, dsn := range []string{
		`host=localhost port=5432 dbname=mydb user=me`,
		`host=localhost  port=5432`,                // doubled separator
		`dbname='a b'`,                             // quoted value with a space
		`dbname=a\ b`,                              // backslash escape outside quotes
		`password='it\'s' dbname=real`,             // escaped quote inside quotes
		`host = localhost port =5432 dbname= mydb`, // whitespace around =
		`dbname=x trailing`,                        // malformed tail: keyword, no `=`
		``,
	} {
		if got := stripSettings(dsn, "schema", "replication"); got != dsn {
			t.Errorf("a DSN carrying neither keyword was rewritten:\n  in:  %q\n  out: %q", dsn, got)
		}
	}
}

// TestStripSettings_RemovesOnlyTheSetting covers the three positions a
// removed setting can occupy, because each leaves a different separator
// problem behind.
func TestStripSettings_RemovesOnlyTheSetting(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"leading", `schema=s host=h dbname=d`, `host=h dbname=d`},
		{"middle", `host=h schema=s dbname=d`, `host=h dbname=d`},
		{"trailing", `host=h dbname=d schema=s`, `host=h dbname=d`},
		{"only setting", `schema=s`, ``},
		{"quoted value of its own", `host=h schema='a b' dbname=d`, `host=h dbname=d`},
		{"repeated", `schema=a host=h schema=b`, `host=h`},
		{"case-insensitive keyword", `host=h SCHEMA=s`, `host=h`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripSettings(tc.in, "schema"); got != tc.want {
				t.Errorf("stripSettings(%q)\n  got  %q\n  want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestParseConnInfo_SpansCoverTheRawBytes binds the two halves of a
// setting together: Value is what the DRIVER sees (unescaped), the span
// is what the OPERATOR wrote (raw). A rewriter that confused them would
// re-render, which is the thing this design avoids — so the span is
// asserted to re-extract the original text, not the decoded value.
func TestParseConnInfo_SpansCoverTheRawBytes(t *testing.T) {
	const dsn = `host=h dbname='a b' user=me`

	got := parseConnInfo(dsn)
	if len(got) != 3 {
		t.Fatalf("parsed %d settings, want 3: %+v", len(got), got)
	}
	for _, s := range got {
		if s.Start < 0 || s.End > len(dsn) || s.Start >= s.End {
			t.Fatalf("setting %q has an unusable span [%d,%d) over a %d-byte DSN", s.Key, s.Start, s.End, len(dsn))
		}
	}
	if raw := dsn[got[1].Start:got[1].End]; raw != `dbname='a b'` {
		t.Errorf("span over the quoted setting = %q, want the RAW text `dbname='a b'`", raw)
	}
	if got[1].Value != "a b" {
		t.Errorf("Value = %q, want the UNQUOTED %q", got[1].Value, "a b")
	}
}

// TestParseConnInfo_LastOccurrenceWins is libpq's and pgx's rule, and it
// is the one a `strings.Fields` walk taking the FIRST match got wrong.
func TestParseConnInfo_LastOccurrenceWins(t *testing.T) {
	if got := parseKVFields(`dbname=a dbname=b`)["dbname"]; got != "b" {
		t.Errorf("dbname = %q, want %q — libpq and pgx both take the last occurrence", got, "b")
	}
}

// TestParseConnInfo_MalformedTailIsKeptNotTruncated pins the deliberate
// non-validation: the walk stops at a keyword with no `=`, and the
// rewriter must still emit those bytes, because dropping input it did not
// understand is precisely how A0915-VF2-PGDSN-1 corrupted a password.
// The driver refuses it, in the driver's own words.
func TestParseConnInfo_MalformedTailIsKeptNotTruncated(t *testing.T) {
	const dsn = `schema=s host=h garbage`
	got := stripSettings(dsn, "schema")
	if !strings.Contains(got, "garbage") {
		t.Errorf("stripSettings truncated at the malformed tail:\n  in:  %q\n  out: %q", dsn, got)
	}
	if got != `host=h garbage` {
		t.Errorf("stripSettings(%q) = %q, want %q", dsn, got, `host=h garbage`)
	}
}
