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

// TestTruncateEmitNames_FollowsTheServersFold pins the emit-site pair of
// the TRUNCATE arm (audit 2026-09-15 A0915-MYSQL-HIGH-1) over both server
// regimes × every spelling shape the parser accepts: on a folding server
// the emitted names are the server's stored (lowercase) spelling
// whatever the statement or the USE-context spelled; on a case-sensitive
// server they are byte-exact, because there `T1` and `t1` are two tables
// and folding would be its own bug. The parser's raw output is pinned
// separately (TestParseTruncateTable); this is the layer above it.
func TestTruncateEmitNames_FollowsTheServersFold(t *testing.T) {
	t.Parallel()
	cells := []struct {
		name        string
		query       string
		eventSchema string
		fold        bool
		wantSchema  string
		wantTable   string
	}{
		// lct=0: byte-exact in every shape.
		{"exact_unqualified_lower", "TRUNCATE TABLE t1", "source_db", false, "source_db", "t1"},
		{"exact_unqualified_upper", "TRUNCATE TABLE T1", "source_db", false, "source_db", "T1"},
		{"exact_unqualified_upper_use_context_upper", "TRUNCATE TABLE T1", "SOURCE_DB", false, "SOURCE_DB", "T1"},
		{"exact_backticked_upper", "TRUNCATE TABLE `T1`", "source_db", false, "source_db", "T1"},
		{"exact_qualified_upper", "TRUNCATE TABLE source_db.T1", "other", false, "source_db", "T1"},
		{"exact_qualified_backticked_upper", "TRUNCATE TABLE `SOURCE_DB`.`T1`", "other", false, "SOURCE_DB", "T1"},
		{"exact_optional_table_keyword", "TRUNCATE T1", "source_db", false, "source_db", "T1"},
		// lct=1: the stored spelling in every shape, including the
		// USE-context the QueryEvent carries and the qualified schema.
		{"fold_unqualified_lower", "TRUNCATE TABLE t1", "source_db", true, "source_db", "t1"},
		{"fold_unqualified_upper", "TRUNCATE TABLE T1", "source_db", true, "source_db", "t1"},
		{"fold_unqualified_upper_use_context_upper", "TRUNCATE TABLE T1", "SOURCE_DB", true, "source_db", "t1"},
		{"fold_backticked_upper", "TRUNCATE TABLE `T1`", "source_db", true, "source_db", "t1"},
		{"fold_qualified_upper", "TRUNCATE TABLE source_db.T1", "other", true, "source_db", "t1"},
		{"fold_qualified_backticked_upper", "TRUNCATE TABLE `SOURCE_DB`.`T1`", "other", true, "source_db", "t1"},
		{"fold_optional_table_keyword", "TRUNCATE T1", "source_db", true, "source_db", "t1"},
		{"fold_mixed_case", "TRUNCATE TABLE Users", "App", true, "app", "users"},
	}
	for _, c := range cells {
		r := &CDCReader{foldScopeNames: c.fold}
		schema, table, ok := r.truncateEmitNames(c.query, c.eventSchema)
		if !ok {
			t.Errorf("%s: %q not recognised as a TRUNCATE", c.name, c.query)
			continue
		}
		if schema != c.wantSchema || table != c.wantTable {
			t.Errorf("%s: fold=%v %q (USE %q) → %q.%q; want %q.%q", c.name, c.fold, c.query, c.eventSchema,
				schema, table, c.wantSchema, c.wantTable)
		}
	}
	t.Run("non_truncate_is_not_recognised_in_either_regime", func(t *testing.T) {
		t.Parallel()
		for _, fold := range []bool{false, true} {
			r := &CDCReader{foldScopeNames: fold}
			if _, _, ok := r.truncateEmitNames("ALTER TABLE T1 ADD COLUMN x INT", "source_db"); ok {
				t.Errorf("fold=%v: ALTER recognised as TRUNCATE", fold)
			}
		}
	})
}
