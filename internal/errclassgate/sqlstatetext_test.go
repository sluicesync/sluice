// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package errclassgate

import "testing"

// TestClassifiesByErrorText pins the gate's own PREDICATE.
//
// Why this test exists, specifically: while landing the gate, a mechanical edit
// of [sqlStateLiteral] dropped a backslash, turning `^\d[0-9A-Z]{4}$` into
// `^d[0-9A-Z]{4}$`. That is a valid regexp, so it lints clean; it matches
// nothing a SQLSTATE looks like, so the SQLSTATE half of the gate was disabled
// outright — and every instantiation still reported ok, because a gate with no
// findings and a gate that cannot find anything are the same green.
//
// The anti-vacuity floor does not help here: it counts strings.Contains calls,
// which were all still being seen. Only the predicate was blind. A gate needs
// its own matcher pinned for the same reason it needs the floor.
func TestClassifiesByErrorText(t *testing.T) {
	cases := []struct {
		val  string
		want bool
		why  string
	}{
		{"42P01", true, "undefined_table — the canonical case"},
		{"3D000", true, "invalid_catalog_name; the database-vanished shape"},
		{"55006", true, "object_in_use"},
		{"42703", true, "undefined_column"},
		{"does not exist", true, "the ambiguous prose token"},

		{"SQLSTATE 42P01", false, "the RENDERED form is a legitimate %v-flattened fallback"},
		{"42P0", false, "four chars is not a SQLSTATE"},
		{"42P011", false, "six chars is not a SQLSTATE"},
		{"P4201", false, "a SQLSTATE class is numeric; must not match a leading letter"},
		{"table", false, "an ordinary word"},
		{"", false, "empty"},
		{"no space left on device", false, "MySQL prose with no structural equivalent — out of scope by design"},
	}
	for _, tc := range cases {
		if got := classifiesByErrorText(tc.val); got != tc.want {
			t.Errorf("classifiesByErrorText(%q) = %v; want %v (%s)", tc.val, got, tc.want, tc.why)
		}
	}
}

// TestIsErrorProseToken pins the narrower predicate used inside slice literals,
// where bare SQLSTATEs are deliberately NOT findings (they are usually compared
// against pgErr.Code, which is correct).
func TestIsErrorProseToken(t *testing.T) {
	if !isErrorProseToken("does not exist") {
		t.Error(`"does not exist" must be a prose token`)
	}
	if isErrorProseToken("57P01") {
		t.Error("a bare SQLSTATE in a slice is usually a structural comparison; flagging it cries wolf")
	}
}
