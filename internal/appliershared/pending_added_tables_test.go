// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestPendingAddedTablesCodecRoundTrip pins the pending_added_tables codec
// over every shape a table name can take on the engines that keep the
// record: the column stores what an add-table recorded, and the warm-resume
// door compares it byte for byte with the names the source reports, so a
// lossy round trip is either a false refusal or a missed one.
func TestPendingAddedTablesCodecRoundTrip(t *testing.T) {
	names := []string{
		"orders",            // plain
		"Orders",            // case is significant (Postgres)
		"naïve_表",           // non-ASCII UTF-8
		"a,b",               // the separator MySQL's live_added_tables uses
		"line\nbreak",       // this codec's own line separator
		`quo"te`,            // a quote
		`back\slash`,        // an escape character
		" padded ",          // significant whitespace
		"",                  // the empty name
		"\xff\xferaw-bytes", // not UTF-8 (a SQL_ASCII Postgres database)
	}
	for _, n := range names {
		got, err := DecodePendingAddedTables(EncodePendingAddedTables([]string{n}))
		if err != nil {
			t.Fatalf("%q: decode: %v", n, err)
		}
		if len(got) != 1 || got[0] != n {
			t.Fatalf("%q round-tripped to %q", n, got)
		}
	}

	t.Run("the whole set, deduplicated, in any order", func(t *testing.T) {
		in := append(append([]string(nil), names...), names[0], names[3])
		enc := EncodePendingAddedTables(in)
		for i := 0; i < len(enc); i++ {
			if enc[i] >= utf8.RuneSelf {
				t.Fatalf("encoded value is not ASCII at byte %d: %q", i, enc)
			}
		}
		got, err := DecodePendingAddedTables(enc)
		if err != nil {
			t.Fatal(err)
		}
		want := append([]string(nil), names...)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
		reversed := append([]string(nil), in...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		if EncodePendingAddedTables(reversed) != enc {
			t.Fatal("the encoding depends on input order")
		}
	})

	t.Run("the empty set is the empty string", func(t *testing.T) {
		if enc := EncodePendingAddedTables(nil); enc != "" {
			t.Fatalf("got %q", enc)
		}
		if got, err := DecodePendingAddedTables(""); err != nil || len(got) != 0 {
			t.Fatalf("got %q, %v", got, err)
		}
		// A set holding only the empty name is NOT the empty set.
		if enc := EncodePendingAddedTables([]string{""}); enc == "" {
			t.Fatal(`{""} encoded as the empty set`)
		}
	})

	t.Run("an unreadable record is an error, never empty", func(t *testing.T) {
		for _, bad := range []string{"orders", `"unterminated`, "\"ok\"\nbare", "\"ok\"\n"} {
			if got, err := DecodePendingAddedTables(bad); err == nil {
				t.Errorf("%q decoded to %q with no error", bad, got)
			} else if !strings.Contains(err.Error(), "not a quoted table name") {
				t.Errorf("%q: %v", bad, err)
			}
		}
	})
}
