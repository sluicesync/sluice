// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

func TestMariaDBGTIDDomains(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		set  string
		want []string
	}{
		{"", nil},
		{"0-1-3", []string{"0"}},
		{"0-1-3,7-4-4", []string{"0", "7"}},
		{" 0-1-3 , 12-9-100 ", []string{"0", "12"}},
		{"garbage", []string{"garbage"}},
	} {
		got := mariadbGTIDDomains(tc.set)
		if len(got) != len(tc.want) {
			t.Fatalf("%q: got %v, want %v", tc.set, got, tc.want)
		}
		for _, d := range tc.want {
			if !got[d] {
				t.Fatalf("%q: missing domain %q in %v", tc.set, d, got)
			}
		}
	}
}

func TestGTIDSetUUIDsSubset(t *testing.T) {
	t.Parallel()
	const a = "58e74464-8f3f-11f0-9d2c-0242ac110002"
	const b = "b8b646a3-8f3f-11f0-9d2c-0242ac110003"
	for _, tc := range []struct {
		resume, executed string
		want             bool
	}{
		// Lag: same UUID, executed behind → the UUID is present.
		{a + ":1-11", a + ":1-5", true},
		// Foreign: a UUID the source has never executed.
		{a + ":1-11", b + ":1-11", false},
		// Mixed: one present, one foreign.
		{a + ":1-11," + b + ":1-3", a + ":1-11", false},
		// Case-insensitive UUIDs, whitespace tolerant.
		{" " + a + ":1-2", a + ":1-2", true},
		// Malformed resume entry never counts as present.
		{"nonsense", a + ":1-2", false},
	} {
		if got := gtidSetUUIDsSubset(tc.resume, tc.executed); got != tc.want {
			t.Errorf("gtidSetUUIDsSubset(%q, %q) = %v, want %v", tc.resume, tc.executed, got, tc.want)
		}
	}
}
