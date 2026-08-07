// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// resetLCTMemo clears the process-wide memo and restores it afterwards, so a
// test that warms or asserts on it cannot leak into a sibling.
func resetLCTMemo(t *testing.T) {
	t.Helper()
	lctMemo.mu.Lock()
	prev := lctMemo.byServer
	lctMemo.byServer = nil
	lctMemo.mu.Unlock()
	t.Cleanup(func() {
		lctMemo.mu.Lock()
		lctMemo.byServer = prev
		lctMemo.mu.Unlock()
	})
}

// TestLCTMemoKey_IsTheServerNotTheDSN is the property the whole memo rests on:
// the two callers reach the same server through DIFFERENT DSNs on purpose (the
// multi-database fan-out passes the SERVER DSN to the fold preflight while the
// namespace fold may see a database-scoped one), so a key derived from the DSN
// string would miss exactly the sharing this exists for.
func TestLCTMemoKey_IsTheServerNotTheDSN(t *testing.T) {
	same := []string{
		"root:pw@tcp(db.example:3306)/",
		"root:pw@tcp(db.example:3306)/appdb",
		"root:pw@tcp(db.example:3306)/other_db?parseTime=true",
		// A different user against the same server still reads the same
		// global; credentials are deliberately not part of the key.
		"reader:other@tcp(db.example:3306)/appdb",
	}
	var want string
	for i, dsn := range same {
		cfg, err := parseServerDSN(dsn)
		if err != nil {
			t.Fatalf("parse %q: %v", dsn, err)
		}
		got := lctMemoKey(cfg)
		if i == 0 {
			want = got
			continue
		}
		if got != want {
			t.Errorf("%q keys as %q; every DSN to one server must share a key (want %q)", dsn, got, want)
		}
	}

	// And the direction that matters for correctness: a DIFFERENT server must
	// not inherit an answer. lower_case_table_names is per-server, and two
	// servers on one host routinely differ.
	for _, dsn := range []string{
		"root:pw@tcp(db.example:3307)/appdb",
		"root:pw@tcp(other.example:3306)/appdb",
		"root:pw@unix(/var/run/mysqld/mysqld.sock)/appdb",
	} {
		cfg, err := parseServerDSN(dsn)
		if err != nil {
			t.Fatalf("parse %q: %v", dsn, err)
		}
		if got := lctMemoKey(cfg); got == want {
			t.Errorf("%q keys as %q, the same as a different server — a per-server setting would be "+
				"shared across servers", dsn, got)
		}
	}
}

func TestLCTMemo_RoundTrips(t *testing.T) {
	resetLCTMemo(t)

	if _, ok := lookupLCT("tcp|db:3306"); ok {
		t.Fatal("a cold memo must report a miss")
	}
	rememberLCT("tcp|db:3306", 1)
	lct, ok := lookupLCT("tcp|db:3306")
	if !ok || lct != 1 {
		t.Errorf("lookupLCT = (%d, %v); want (1, true)", lct, ok)
	}
	// Zero is a REAL answer (the stock Linux default) and must be
	// distinguishable from a miss — a memo that treated it as absent would
	// re-connect once per database on precisely the common configuration.
	rememberLCT("tcp|other:3306", 0)
	lct, ok = lookupLCT("tcp|other:3306")
	if !ok || lct != 0 {
		t.Errorf("lookupLCT for a memoised 0 = (%d, %v); want (0, true)", lct, ok)
	}
}
