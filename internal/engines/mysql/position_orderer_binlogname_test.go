// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Binlog file ordering answered BACKWARDS across the suffix widening
// (audit 2026-08-05 C-13).
//
// The file/pos comparison was `pp.File > ap.File`, justified by "the fixed
// zero-padded width makes string comparison correct". The width is not fixed:
// MySQL pads the rotation sequence to six digits and then grows it, so
// mysql-bin.999999 is followed by mysql-bin.1000000 — and lexically
// "1000000" < "999999". A position-ordering predicate that answers backwards
// is how a resume concludes a newer position is older.
//
// The premise held for the first 999999 files of any server's life, which is
// exactly why it reads as safe.

package mysql

import "testing"

func TestBinlogFileAfter_SurvivesTheSuffixWidening(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want bool
	}{
		// The boundary the lexical compare gets wrong, both directions.
		{"1000000 is after 999999", "mysql-bin.1000000", "mysql-bin.999999", true},
		{"999999 is not after 1000000", "mysql-bin.999999", "mysql-bin.1000000", false},
		{"1000001 after 1000000", "mysql-bin.1000001", "mysql-bin.1000000", true},
		{"far past the widening", "mysql-bin.9999999", "mysql-bin.1000000", true},

		// Ordinary within-width rotation still behaves.
		{"000002 after 000001", "mysql-bin.000002", "mysql-bin.000001", true},
		{"000001 not after 000002", "mysql-bin.000001", "mysql-bin.000002", false},
		{"equal is not after", "mysql-bin.000007", "mysql-bin.000007", false},
		{"100000 after 099999", "mysql-bin.100000", "mysql-bin.099999", true},

		// A different basename is a different lineage; fall back to lexical
		// rather than pretending the sequences are comparable.
		// (lexical: 'o' > 'm', so this is true — the point is only that a
		// different basename does not get numeric treatment.)
		{"different basename falls back to lexical", "other-bin.000001", "mysql-bin.999999", true},
		{"different basename falls back, other direction", "mysql-bin.000001", "other-bin.999999", false},

		// Non-numeric suffixes fall back to the previous behaviour.
		{"non-numeric falls back", "mysql-bin.index", "mysql-bin.000001", true},
		{"no dot falls back", "binlog", "mysql-bin.000001", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := binlogFileAfter(tc.a, tc.b); got != tc.want {
				t.Errorf("binlogFileAfter(%q, %q) = %v; want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// The ordering must be a strict total order on one lineage: exactly one of
// a>b, b>a, a==b holds. A comparator that reports both directions true (which
// a lexical compare does NOT, but a hand-rolled numeric one easily could) would
// make "at or after" incoherent.
func TestBinlogFileAfter_IsAntisymmetric(t *testing.T) {
	files := []string{
		"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.099999",
		"mysql-bin.100000", "mysql-bin.999999", "mysql-bin.1000000",
		"mysql-bin.1000001",
	}
	for _, a := range files {
		for _, b := range files {
			ab := binlogFileAfter(a, b)
			ba := binlogFileAfter(b, a)
			if a == b {
				if ab || ba {
					t.Errorf("%q vs itself reported after=%v/%v; want false both ways", a, ab, ba)
				}
				continue
			}
			if ab == ba {
				t.Errorf("binlogFileAfter(%q,%q)=%v and (%q,%q)=%v — exactly one must hold",
					a, b, ab, b, a, ba)
			}
		}
	}
	// And it must agree with the rotation order the slice is written in.
	for i := 0; i < len(files)-1; i++ {
		if !binlogFileAfter(files[i+1], files[i]) {
			t.Errorf("%q must sort after %q", files[i+1], files[i])
		}
	}
}
