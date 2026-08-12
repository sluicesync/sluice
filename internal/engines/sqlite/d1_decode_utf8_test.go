// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"strings"
	"testing"
)

// These pin the SQT-1 refusal (audit 2026-08-11) at the shared choke point:
// jsonString serves the sqlite-trigger CDC decode (via the trigger seam's
// CapturedCellDecoder), the D1 live read path, and D1 staging, so a refusal
// here covers all three. encoding/json.Unmarshal silently rewrites BOTH of
// these shapes to U+FFFD instead of erroring; before this refusal the mangled
// value was valid UTF-8, so no downstream check and no target could see it.
func TestJSONString_RefusesTheTwoSilentReplacementVectors(t *testing.T) {
	t.Run("invalid UTF-8 bytes refuse", func(t *testing.T) {
		// The audit's observed poison: CAST(x'FFFE61' AS TEXT) captured verbatim
		// into a JSON string token. Unmarshal would yield EF BF BD EF BF BD 61.
		raw := []byte{'"', 0xFF, 0xFE, 'a', '"'}
		_, _, err := jsonString(raw)
		if err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
			t.Fatalf("invalid UTF-8 string token: err = %v; want the loud not-valid-UTF-8 refusal", err)
		}
	})
	t.Run("invalid UTF-8 refuses through the text storage-class arm", func(t *testing.T) {
		// The same vector through d1StorageValue — the entry both the trigger
		// seam and the D1 row decode call.
		raw := []byte{'"', 0xFF, 0xFE, 'a', '"'}
		_, err := d1StorageValue("text", raw)
		if err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
			t.Fatalf("d1StorageValue text arm: err = %v; want the loud not-valid-UTF-8 refusal", err)
		}
	})
	t.Run("lone high surrogate escape refuses", func(t *testing.T) {
		_, _, err := jsonString([]byte(`"a\uD800b"`))
		if err == nil || !strings.Contains(err.Error(), "lone UTF-16 surrogate") {
			t.Fatalf("lone high surrogate: err = %v; want the loud surrogate refusal", err)
		}
	})
	t.Run("lone low surrogate escape refuses", func(t *testing.T) {
		_, _, err := jsonString([]byte(`"\uDC00"`))
		if err == nil || !strings.Contains(err.Error(), "lone UTF-16 surrogate") {
			t.Fatalf("lone low surrogate: err = %v; want the loud surrogate refusal", err)
		}
	})
	t.Run("high surrogate at end of token refuses without panicking", func(t *testing.T) {
		_, _, err := jsonString([]byte(`"\uD800"`))
		if err == nil || !strings.Contains(err.Error(), "lone UTF-16 surrogate") {
			t.Fatalf("trailing high surrogate: err = %v; want the loud surrogate refusal", err)
		}
	})
}

// The no-over-refusal controls: every shape that CAN round-trip faithfully
// must keep doing so — a refusal wider than the mangle class would break
// working configurations (embedded NUL and literal U+FFFD are both legal,
// faithful values).
func TestJSONString_FaithfulShapesStillPass(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"plain text", `"héllo→世界"`, "héllo→世界"},
		{"valid surrogate pair", `"😀"`, "😀"},
		{"embedded NUL escape", `"a\u0000b"`, "a\x00b"},
		{"literal replacement char round-trips", "\"a�b\"", "a�b"},
		{"escaped backslash before u is literal text", `"a\\uD800"`, `a\uD800`},
		{"ordinary escapes", `"a\n\t\"\\"`, "a\n\t\"\\"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, ok, err := jsonString([]byte(tc.raw))
			if err != nil || !ok {
				t.Fatalf("jsonString(%q): ok=%v err=%v; want faithful pass", tc.raw, ok, err)
			}
			if s != tc.want {
				t.Fatalf("jsonString(%q) = %q; want %q byte-exact", tc.raw, s, tc.want)
			}
		})
	}

	// A malformed \u escape is NOT this scan's job — Unmarshal refuses it
	// loudly itself; pin that the refusal stays loud rather than asserting
	// whose error it is.
	if _, _, err := jsonString([]byte(`"\uZZ00"`)); err == nil {
		t.Fatal("malformed \\u escape decoded silently; want a loud error")
	}
}

// The catalog sibling: jsonScalarString feeds type names and DEFAULT text into
// emitted DDL, so the same two vectors must refuse there too — while its
// number/bool arms (always valid UTF-8) stay untouched.
func TestJSONScalarString_RefusesTheSameVectors(t *testing.T) {
	if _, _, err := jsonScalarString([]byte{'"', 0xFF, 0xFE, '"'}); err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
		t.Fatalf("invalid UTF-8 catalog scalar: err = %v; want the loud refusal", err)
	}
	if _, _, err := jsonScalarString([]byte(`"\uD800"`)); err == nil || !strings.Contains(err.Error(), "lone UTF-16 surrogate") {
		t.Fatalf("lone surrogate catalog scalar: err = %v; want the loud refusal", err)
	}
	if s, ok, err := jsonScalarString([]byte(`42`)); err != nil || !ok || s != "42" {
		t.Fatalf("numeric catalog scalar: (%q, %v, %v); want faithful 42", s, ok, err)
	}
	if s, ok, err := jsonScalarString([]byte(`true`)); err != nil || !ok || s != "1" {
		t.Fatalf("bool catalog scalar: (%q, %v, %v); want faithful 1", s, ok, err)
	}
}
