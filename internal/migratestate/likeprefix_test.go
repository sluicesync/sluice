// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migratestate

import "testing"

// TestLikePrefix_EscapesEveryPatternCharacter pins the escaper both
// engines' listers render under `ESCAPE '#'` (audit 2026-09-09
// A0909-H1-ESCAPE). The production caller passes one constant prefix,
// so every cell here is a codec pin rather than a live path: the point
// is that the escape character escapes ITSELF, that the two LIKE
// wildcards are neutralised, and that nothing else — backslash, quote,
// multi-byte text — is touched, because under a declared ESCAPE the
// server treats every other byte literally.
func TestLikePrefix_EscapesEveryPatternCharacter(t *testing.T) {
	t.Parallel()
	cells := []struct{ in, want string }{
		{"sync-", "sync-%"},
		{"sync-prod_", "sync-prod#_%"},
		{"100%", "100#%%"},
		{"a#b", "a##b%"},
		{"##", "####%"},
		{`back\slash`, `back\slash%`},
		{"it's", "it's%"},
		{"名前_x", "名前#_x%"},
		{"", "%"},
	}
	for _, c := range cells {
		if got := likePrefix(c.in); got != c.want {
			t.Errorf("likePrefix(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}
