// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestDatabaseInScope_FollowsTheServersFold pins the single-database
// scope compare's two regimes (audit 2026-09-09 RC-1b): byte-exact on a
// case-sensitive server, folded on a folding one — where the DSN may
// spell the database in a case the server never stores. The
// multi-database predicate is handed the event's name untouched in both
// regimes: its selected set was computed from the catalog listing, so
// folding there would be a second, disagreeing rule.
func TestDatabaseInScope_FollowsTheServersFold(t *testing.T) {
	t.Parallel()
	cells := []struct {
		name   string
		schema string
		fold   bool
		event  string
		want   bool
	}{
		{"exact_server_same_spelling", "source_db", false, "source_db", true},
		{"exact_server_dsn_uppercase_is_out_of_scope", "SOURCE_DB", false, "source_db", false}, // two databases on lct=0
		{"folding_server_dsn_uppercase_matches_stored", "SOURCE_DB", true, "source_db", true},  // RC-1b
		{"folding_server_same_spelling", "source_db", true, "source_db", true},
		{"folding_server_other_database_stays_out", "source_db", true, "other_db", false},
	}
	for _, c := range cells {
		r := &CDCReader{schema: c.schema, foldScopeNames: c.fold}
		if got := r.databaseInScope(c.event); got != c.want {
			t.Errorf("%s: schema=%q fold=%v event=%q → %v; want %v", c.name, c.schema, c.fold, c.event, got, c.want)
		}
	}

	t.Run("multi_database_predicate_sees_the_stored_name_unfolded", func(t *testing.T) {
		t.Parallel()
		var seen string
		r := &CDCReader{schema: "IGNORED", foldScopeNames: true}
		r.SetCDCDatabaseScope(func(db string) bool { seen = db; return db == "source_db" })
		if !r.databaseInScope("source_db") {
			t.Fatal("predicate admitting the stored name = false")
		}
		if seen != "source_db" {
			t.Fatalf("predicate saw %q; want the event's name as the server stored it", seen)
		}
	})
}
