// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// TestColumnNamesCodec_RoundTrip is the codec matrix for the column-order
// record (RENAME-SWAP-FORWARD): every name shape a target can hold
// round-trips exactly, and the two ways "nothing" can be spelled stay
// distinct — nil (never recorded, refused on write) and empty (a recorded
// table with no columns). The independent expected value is the input
// itself.
func TestColumnNamesCodec_RoundTrip(t *testing.T) {
	t.Parallel()
	wide := make([]string, 4096)
	for i := range wide {
		wide[i] = fmt.Sprintf("col_%04d_%s", i, strings.Repeat("x", 48))
	}
	cases := map[string][]string{
		"empty (a table with no columns)": {},
		"plain":                           {"id", "b", "c"},
		"case is significant":             {"Foo", "foo", "FOO"},
		"empty name":                      {""},
		"quotes and escapes":              {`a"b`, `a\b`, "a`b", "a'b", "\\u0041"},
		"html characters, unescaped":      {"<b>", "a&b", "x>y"},
		"control characters":              {"a\tb", "a\nb", "a\x01b", "a\x00b"},
		"unicode":                         {"café", "列名", "🦀", "Ωmega", "ﬁ"},
		"line and paragraph separators":   {"a" + string(rune(0x2028)) + "b", "a" + string(rune(0x2029)) + "b"},
		"spaces and dots":                 {" lead", "trail ", "a.b", "a b"},
		"4096 wide columns":               wide,
	}
	for name, cols := range cases {
		t.Run(name, func(t *testing.T) {
			stored, err := EncodeColumnNames(cols)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			back, err := DecodeColumnNames(stored)
			if err != nil {
				t.Fatalf("decode %q: %v", stored, err)
			}
			if back == nil || !slices.Equal(back, cols) {
				t.Fatalf("round trip: %q → %q → %q", cols, stored, back)
			}
		})
	}
}

// TestColumnNamesCodec_Refuses pins the loud edges: what the encoding
// would alter is refused on write, and what this codec did not write is
// refused on read — including JSON that decodes, but lossily or to a
// different spelling.
func TestColumnNamesCodec_Refuses(t *testing.T) {
	t.Parallel()
	if _, err := EncodeColumnNames(nil); err == nil {
		t.Error("nil (not recorded) was encoded as a record")
	}
	if _, err := EncodeColumnNames([]string{"ok", "bad\xffbyte"}); err == nil {
		t.Error("an invalid-UTF-8 name was encoded (encoding/json would replace it with U+FFFD)")
	}
	bs := string(rune(92)) // a backslash, for the JSON escapes below
	for _, stored := range []string{
		"",                                    // empty
		"null",                                // not a list
		"{}",                                  // not a list
		`[1]`,                                 // not strings
		`["a"] ["b"]`,                         // trailing data
		`[ "a" ]`,                             // not this codec's spelling
		`["` + bs + `u0061"]`,                 // an escape this codec does not write
		`["` + bs + `ud800"]`,                 // a lone surrogate: decodes to U+FFFD
		`["a","b"`,                            // truncated
		`["a"]` + "\n",                        // a trailing newline
		`["` + bs + `u003cb` + bs + `u003e"]`, // HTML-escaped: not this codec's form
	} {
		if cols, err := DecodeColumnNames(stored); err == nil {
			t.Errorf("decoded %q to %q; want a refusal", stored, cols)
		}
	}
}

// TestColumnOrderKey_DistinctTuples pins that no two distinct (stream,
// schema, table) tuples share a key, whatever bytes their parts hold.
func TestColumnOrderKey_DistinctTuples(t *testing.T) {
	t.Parallel()
	tuples := [][3]string{
		{"s", "a\x00b", "c"},
		{"s", "a", "b\x00c"},
		{"s", "ab", "c"},
		{"s", "a", "bc"},
		{"s", "", "abc"},
		{"s", "abc", ""},
		{"s", "Kd", "x"},
		{"s", "kd", "x"},
	}
	seen := map[string][3]string{}
	for _, tu := range tuples {
		k := ColumnOrderKey(tu[0], tu[1], tu[2])
		if len(k) != 64 {
			t.Fatalf("key %q is not 64 hex characters", k)
		}
		if other, dup := seen[k]; dup {
			t.Errorf("%q and %q share key %s", tu, other, k)
		}
		seen[k] = tu
	}
}
