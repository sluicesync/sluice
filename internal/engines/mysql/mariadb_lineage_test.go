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

func TestBinlogFileNumber(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		want uint64
		ok   bool
	}{
		{"mb.000009", 9, true},
		{"mysqld-bin.000123", 123, true},
		{"binlog.1", 1, true},
		{"noext", 0, false},
		{"mb.", 0, false},
		{"mb.00x9", 0, false},
	} {
		got, ok := binlogFileNumber(tc.name)
		if got != tc.want || ok != tc.ok {
			t.Errorf("binlogFileNumber(%q) = (%d, %v), want (%d, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestMariaDBStateCovers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		state, anchor string
		want          bool
	}{
		// The measured purge case: the oldest retained file's start state
		// is ahead of the anchor in the one domain.
		{"0-1-62", "0-1-12", true},
		{"0-1-12", "0-1-12", true},
		{"0-1-11", "0-1-12", false},
		// Domain order differs between BINLOG_GTID_POS and
		// @@gtid_binlog_state; only per-domain sequence matters.
		{"7-1-2,0-1-67", "0-1-12,7-1-1", true},
		// A domain the anchor names that the state lacks: not covered.
		{"0-1-99", "0-1-12,7-1-1", false},
		// Several server_ids in one domain: the maximum counts.
		{"0-2-5,0-1-40", "0-1-30", true},
		// Empty anchor is covered by anything (nothing to reach).
		{"0-1-1", "", true},
	} {
		if got := mariadbStateCovers(tc.state, tc.anchor); got != tc.want {
			t.Errorf("mariadbStateCovers(%q, %q) = %v, want %v", tc.state, tc.anchor, got, tc.want)
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
