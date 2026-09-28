// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import "testing"

// TestClassifySQLiteNowDefault pins every accepted surface form of each
// current-instant shape, and the near-misses that must NOT classify —
// above all the strftime specifiers whose case carries meaning (%S the
// seconds field vs %s the epoch; %M minutes vs %m months), which the old
// case-folding Postgres matcher conflated (GC-39 item 3).
func TestClassifySQLiteNowDefault(t *testing.T) {
	cases := []struct {
		in   string
		want SQLiteNowShape
	}{
		{"CURRENT_TIMESTAMP", SQLiteNowDateTime},
		{"current_timestamp", SQLiteNowDateTime},
		{"(CURRENT_TIMESTAMP)", SQLiteNowDateTime},
		{"datetime('now')", SQLiteNowDateTime},
		{"(datetime('now'))", SQLiteNowDateTime},
		{"  DateTime ( 'NOW' )  ", SQLiteNowDateTime},
		{`datetime("now")`, SQLiteNowDateTime},
		{"strftime('%Y-%m-%d %H:%M:%S','now')", SQLiteNowDateTime},
		{"(strftime('%Y-%m-%d %H:%M:%S', 'now'))", SQLiteNowDateTime},
		{"strftime('%Y-%m-%dT%H:%M:%SZ','now')", SQLiteNowISOZ},
		{"CURRENT_DATE", SQLiteNowDate},
		{"date('now')", SQLiteNowDate},
		{"strftime('%Y-%m-%d','now')", SQLiteNowDate},
		{"CURRENT_TIME", SQLiteNowTime},
		{"time('now')", SQLiteNowTime},
		{"strftime('%H:%M:%S','now')", SQLiteNowTime},
		{"strftime('%s','now')", SQLiteNowEpoch},

		// Case carries meaning inside a strftime format.
		{"strftime('%S','now')", SQLiteNowNone},
		{"strftime('%Y-%M-%d','now')", SQLiteNowNone},
		{"strftime('%y-%m-%d','now')", SQLiteNowNone},
		{"strftime('%h:%m:%s','now')", SQLiteNowNone},
		// Whitespace inside the literal is significant.
		{"strftime('%Y-%m-%d%H:%M:%S','now')", SQLiteNowNone},
		// Modifiers, other bases, composites, other functions.
		{"datetime('now','+1 day')", SQLiteNowNone},
		{"date('now','localtime')", SQLiteNowNone},
		{"datetime(created)", SQLiteNowNone},
		{"strftime('%Y','now')", SQLiteNowNone},
		{"strftime('%Y-%m-%d', mycol)", SQLiteNowNone},
		{"julianday('now')", SQLiteNowNone},
		{"unixepoch('now')", SQLiteNowNone},
		{"CURRENT_TIMESTAMP || ''", SQLiteNowNone},
		{"(a) + (b)", SQLiteNowNone},
		{"now()", SQLiteNowNone},
		{"'now'", SQLiteNowNone},
		{"datetime()", SQLiteNowNone},
		{"datetime('now'", SQLiteNowNone},
		{"", SQLiteNowNone},
	}
	for _, tc := range cases {
		if got := ClassifySQLiteNowDefault(tc.in); got != tc.want {
			t.Errorf("ClassifySQLiteNowDefault(%q) = %d; want %d", tc.in, got, tc.want)
		}
	}
}
