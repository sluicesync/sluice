// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"strings"
	"testing"
)

// TestD1CellString_RefusesTheSilentReplacementVectors pins the SQT-1 guard at
// the d1-trigger transport's OWN string extraction — the one-layer-up sibling
// the pre-v0.122.0 value-fidelity review caught: d1CellString pulls the
// change-log before/after payloads (the captured row images), and an
// unguarded json.Unmarshal there rewrote invalid UTF-8 to U+FFFD before the
// guarded inner decode could ever see it (the mangle arrives downstream as
// VALID UTF-8 and passes). RED pre-fix on the poison vectors.
func TestD1CellString_RefusesTheSilentReplacementVectors(t *testing.T) {
	t.Run("raw invalid UTF-8 refuses", func(t *testing.T) {
		_, _, err := d1CellString([]byte{'"', 0xFF, 0xFE, 'a', '"'})
		if err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
			t.Fatalf("invalid UTF-8 payload cell: err = %v; want the loud SQT-1 refusal — an unguarded "+
				"decode here mangles the captured row image before the inner guard can see it", err)
		}
	})
	t.Run("lone surrogate escape refuses", func(t *testing.T) {
		_, _, err := d1CellString([]byte(`"\uD800"`))
		if err == nil || !strings.Contains(err.Error(), "lone UTF-16 surrogate") {
			t.Fatalf("lone surrogate payload cell: err = %v; want the loud SQT-1 refusal", err)
		}
	})
	t.Run("faithful shapes still pass", func(t *testing.T) {
		for raw, want := range map[string]string{
			`"héllo→世界"`: "héllo→世界",
			`"a\u0000b"`: "a\x00b",
			`"😀"`:        "😀",
		} {
			s, ok, err := d1CellString([]byte(raw))
			if err != nil || !ok || s != want {
				t.Fatalf("d1CellString(%q) = (%q, %v, %v); want faithful %q", raw, s, ok, err, want)
			}
		}
	})
	t.Run("null and absent stay distinguishable", func(t *testing.T) {
		for _, raw := range [][]byte{nil, []byte("null")} {
			if _, ok, err := d1CellString(raw); ok || err != nil {
				t.Fatalf("d1CellString(%q) = (ok=%v, err=%v); want (false, nil)", raw, ok, err)
			}
		}
	})
}
