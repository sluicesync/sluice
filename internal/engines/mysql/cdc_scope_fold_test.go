// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"testing"
)

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
		lct    int
		event  string
		want   bool
	}{
		{"exact_server_same_spelling", "source_db", 0, "source_db", true},
		{"exact_server_dsn_uppercase_is_out_of_scope", "SOURCE_DB", 0, "source_db", false}, // two databases on lct=0
		{"folding_server_dsn_uppercase_matches_stored", "SOURCE_DB", 1, "source_db", true}, // RC-1b
		{"folding_server_same_spelling", "source_db", 1, "source_db", true},
		{"folding_server_other_database_stays_out", "source_db", 1, "other_db", false},
		{"asis_server_compares_case_insensitively_too", "SOURCE_DB", 2, "Source_DB", true}, // lct=2 stores as created, compares folded
		{"asis_server_other_database_stays_out", "source_db", 2, "other_db", false},
	}
	for _, c := range cells {
		r := &CDCReader{schema: c.schema, lowerCaseTableNames: c.lct}
		if got := r.databaseInScope(c.event); got != c.want {
			t.Errorf("%s: schema=%q lct=%d event=%q → %v; want %v", c.name, c.schema, c.lct, c.event, got, c.want)
		}
	}

	t.Run("multi_database_predicate_sees_the_stored_name_unfolded", func(t *testing.T) {
		t.Parallel()
		var seen string
		r := &CDCReader{schema: "IGNORED", lowerCaseTableNames: 1}
		r.SetCDCDatabaseScope(func(db string) bool { seen = db; return db == "source_db" })
		if !r.databaseInScope("source_db") {
			t.Fatal("predicate admitting the stored name = false")
		}
		if seen != "source_db" {
			t.Fatalf("predicate saw %q; want the event's name as the server stored it", seen)
		}
	})
}

// storedNameLookupFake is the unit-test stand-in for the catalog half of
// truncateEmitNames' resolution (the reader's storedTableNameLookup
// seam). It records the arguments it was asked with so a cell can prove
// the lookup saw the statement's spelling, not a pre-folded one.
type storedNameLookupFake struct {
	schema, table string // the stored spelling to answer with
	found         bool
	err           error
	askedSchema   string
	askedTable    string
	calls         int
}

func (f *storedNameLookupFake) lookup(_ context.Context, _ rowQuerier, schema, table string) (storedSchema, storedTable string, found bool, err error) {
	f.calls++
	f.askedSchema, f.askedTable = schema, table
	return f.schema, f.table, f.found, f.err
}

// TestTruncateEmitNames_FollowsTheServersFold pins the emit-site pair of
// the TRUNCATE arm (audit 2026-09-15 A0915-MYSQL-HIGH-1) over the three
// server regimes × every spelling shape the parser accepts × every
// source the stored spelling can be resolved from:
//
//   - lct=0 (case-sensitive): byte-exact, and NOTHING is consulted —
//     there `T1` and `t1` are two tables, so any resolution would be its
//     own bug. The lookup fake fails the cell if reached.
//   - lct=1 (stored lowercase): the stored spelling from the Table_map
//     record, the schema cache, or the catalog when the reader has one;
//     the lowercase fold when it has none — exact there, because the
//     server's own storage rule IS the fold.
//   - lct=2 (stored as created, compared case-insensitively — the
//     regime no Linux container can run, so this fake is its only
//     grading): a stored `Users` must come back as `Users` whatever
//     case the statement used, from each source in turn; with no source
//     the statement's spelling is kept, never lowercased (the pre-tag
//     value-fidelity review of v0.153.2 — lowercasing here DIVERGES
//     from the row events, the HIGH-1 harm in the other regime).
//
// The source-hit cells hand the fake a WRONG answer or a failure so a
// resolution that consulted the catalog ahead of what the reader
// already mapped, or skipped the reader's own record, shows as red. The
// parser's raw output is pinned separately (TestParseTruncateTable);
// this is the layer above it.
func TestTruncateEmitNames_FollowsTheServersFold(t *testing.T) {
	t.Parallel()
	// mustNotLookup fails the cell if the catalog is consulted.
	mustNotLookup := &storedNameLookupFake{err: errors.New("the catalog must not be consulted on this cell")}
	notFound := &storedNameLookupFake{}
	cells := []struct {
		name        string
		lct         int
		query       string
		eventSchema string
		tableMap    []string              // stored qualified names the reader mapped off Table_map events
		cache       []string              // stored qualified names in the schema cache
		lookup      *storedNameLookupFake // nil = the catalog must not be consulted
		wantSchema  string
		wantTable   string
		wantAsked   string // "schema.table" the catalog fake must have been asked with, when it answers
	}{
		// lct=0: byte-exact in every shape; no source consulted.
		{name: "exact_unqualified_lower", lct: 0, query: "TRUNCATE TABLE t1", eventSchema: "source_db", wantSchema: "source_db", wantTable: "t1"},
		{name: "exact_unqualified_upper", lct: 0, query: "TRUNCATE TABLE T1", eventSchema: "source_db", wantSchema: "source_db", wantTable: "T1"},
		{name: "exact_unqualified_upper_use_context_upper", lct: 0, query: "TRUNCATE TABLE T1", eventSchema: "SOURCE_DB", wantSchema: "SOURCE_DB", wantTable: "T1"},
		{name: "exact_backticked_upper", lct: 0, query: "TRUNCATE TABLE `T1`", eventSchema: "source_db", wantSchema: "source_db", wantTable: "T1"},
		{name: "exact_qualified_upper", lct: 0, query: "TRUNCATE TABLE source_db.T1", eventSchema: "other", wantSchema: "source_db", wantTable: "T1"},
		{name: "exact_qualified_backticked_upper", lct: 0, query: "TRUNCATE TABLE `SOURCE_DB`.`T1`", eventSchema: "other", wantSchema: "SOURCE_DB", wantTable: "T1"},
		{name: "exact_optional_table_keyword", lct: 0, query: "TRUNCATE T1", eventSchema: "source_db", wantSchema: "source_db", wantTable: "T1"},
		{name: "exact_ignores_a_mapped_table_of_another_case", lct: 0, query: "TRUNCATE TABLE T1", eventSchema: "source_db", tableMap: []string{"source_db.t1"}, cache: []string{"source_db.t1"}, wantSchema: "source_db", wantTable: "T1"},
		// lct=1, no source: the stored (lowercase) spelling in every
		// shape, including the USE-context the QueryEvent carries and
		// the qualified schema.
		{name: "fold_unqualified_lower", lct: 1, query: "TRUNCATE TABLE t1", eventSchema: "source_db", lookup: notFound, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_unqualified_upper", lct: 1, query: "TRUNCATE TABLE T1", eventSchema: "source_db", lookup: notFound, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_unqualified_upper_use_context_upper", lct: 1, query: "TRUNCATE TABLE T1", eventSchema: "SOURCE_DB", lookup: notFound, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_backticked_upper", lct: 1, query: "TRUNCATE TABLE `T1`", eventSchema: "source_db", lookup: notFound, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_qualified_upper", lct: 1, query: "TRUNCATE TABLE source_db.T1", eventSchema: "other", lookup: notFound, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_qualified_backticked_upper", lct: 1, query: "TRUNCATE TABLE `SOURCE_DB`.`T1`", eventSchema: "other", lookup: notFound, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_optional_table_keyword", lct: 1, query: "TRUNCATE T1", eventSchema: "source_db", lookup: notFound, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_mixed_case", lct: 1, query: "TRUNCATE TABLE Users", eventSchema: "App", lookup: notFound, wantSchema: "app", wantTable: "users"},
		// lct=1, a source knows the table: the same answer, from the
		// source rather than the fold (the fold is exact here, so these
		// pin the resolution ORDER — the catalog is not asked when the
		// reader already mapped the table).
		{name: "fold_table_map_hit", lct: 1, query: "TRUNCATE TABLE T1", eventSchema: "SOURCE_DB", tableMap: []string{"source_db.t1"}, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_schema_cache_hit", lct: 1, query: "TRUNCATE TABLE T1", eventSchema: "SOURCE_DB", cache: []string{"source_db.t1"}, wantSchema: "source_db", wantTable: "t1"},
		{name: "fold_catalog_hit", lct: 1, query: "TRUNCATE TABLE T1", eventSchema: "SOURCE_DB", lookup: &storedNameLookupFake{schema: "source_db", table: "t1", found: true}, wantSchema: "source_db", wantTable: "t1", wantAsked: "SOURCE_DB.T1"},
		// lct=2: a stored `Users` in `App`, resolved from each source.
		{name: "asis_table_map_hit_lower_statement", lct: 2, query: "TRUNCATE TABLE users", eventSchema: "App", tableMap: []string{"App.Users"}, wantSchema: "App", wantTable: "Users"},
		{name: "asis_table_map_hit_upper_statement", lct: 2, query: "TRUNCATE TABLE USERS", eventSchema: "App", tableMap: []string{"App.Users"}, wantSchema: "App", wantTable: "Users"},
		{name: "asis_table_map_hit_qualified_lower", lct: 2, query: "TRUNCATE TABLE app.users", eventSchema: "other", tableMap: []string{"App.Users"}, wantSchema: "App", wantTable: "Users"},
		{name: "asis_table_map_ignores_out_of_scope_sentinel", lct: 2, query: "TRUNCATE TABLE users", eventSchema: "App", tableMap: []string{"", "App.Users"}, wantSchema: "App", wantTable: "Users"},
		{name: "asis_schema_cache_hit", lct: 2, query: "TRUNCATE TABLE users", eventSchema: "App", cache: []string{"App.Users"}, wantSchema: "App", wantTable: "Users"},
		{name: "asis_schema_cache_hit_upper_statement", lct: 2, query: "TRUNCATE TABLE `USERS`", eventSchema: "APP", cache: []string{"App.Users"}, wantSchema: "App", wantTable: "Users"},
		{name: "asis_catalog_hit", lct: 2, query: "TRUNCATE TABLE users", eventSchema: "App", lookup: &storedNameLookupFake{schema: "App", table: "Users", found: true}, wantSchema: "App", wantTable: "Users", wantAsked: "App.users"},
		{name: "asis_catalog_hit_upper_statement", lct: 2, query: "TRUNCATE TABLE USERS", eventSchema: "APP", lookup: &storedNameLookupFake{schema: "App", table: "Users", found: true}, wantSchema: "App", wantTable: "Users", wantAsked: "APP.USERS"},
		{name: "asis_other_table_mapped_does_not_match", lct: 2, query: "TRUNCATE TABLE users", eventSchema: "App", tableMap: []string{"App.Orders"}, cache: []string{"Other.Users"}, lookup: &storedNameLookupFake{schema: "App", table: "Users", found: true}, wantSchema: "App", wantTable: "Users", wantAsked: "App.users"},
		// lct=2, no source knows the table: the statement's spelling,
		// NEVER the lowercase fold.
		{name: "asis_not_found_keeps_statement_spelling", lct: 2, query: "TRUNCATE TABLE Users", eventSchema: "App", lookup: notFound, wantSchema: "App", wantTable: "Users"},
		{name: "asis_not_found_keeps_upper_statement_spelling", lct: 2, query: "TRUNCATE TABLE USERS", eventSchema: "APP", lookup: notFound, wantSchema: "APP", wantTable: "USERS"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			spec := c.lookup
			if spec == nil {
				spec = mustNotLookup
			}
			// A per-cell copy: the fake records the arguments it was
			// asked with, and the cells run in parallel.
			lookup := &storedNameLookupFake{schema: spec.schema, table: spec.table, found: spec.found, err: spec.err}
			r := &CDCReader{
				lowerCaseTableNames:   c.lct,
				tableMap:              map[uint64]string{},
				schemaCache:           map[string]*tableSchema{},
				storedTableNameLookup: lookup.lookup,
			}
			for i, qn := range c.tableMap {
				r.tableMap[uint64(i)] = qn
			}
			for _, qn := range c.cache {
				r.schemaCache[qn] = nil
			}
			schema, table, ok, err := r.truncateEmitNames(context.Background(), c.query, c.eventSchema)
			if err != nil {
				t.Fatalf("lct=%d %q (USE %q): %v", c.lct, c.query, c.eventSchema, err)
			}
			if !ok {
				t.Fatalf("%q not recognised as a TRUNCATE", c.query)
			}
			if schema != c.wantSchema || table != c.wantTable {
				t.Errorf("lct=%d %q (USE %q) → %q.%q; want %q.%q", c.lct, c.query, c.eventSchema,
					schema, table, c.wantSchema, c.wantTable)
			}
			if c.wantAsked != "" && qualifiedName(lookup.askedSchema, lookup.askedTable) != c.wantAsked {
				t.Errorf("the catalog was asked for %q; want the statement's own spelling %q", qualifiedName(lookup.askedSchema, lookup.askedTable), c.wantAsked)
			}
		})
	}

	t.Run("catalog_failure_is_loud_on_both_case_insensitive_regimes", func(t *testing.T) {
		t.Parallel()
		for _, lct := range []int{1, 2} {
			failing := &storedNameLookupFake{err: errors.New("information_schema unreachable")}
			r := &CDCReader{lowerCaseTableNames: lct, storedTableNameLookup: failing.lookup}
			_, _, ok, err := r.truncateEmitNames(context.Background(), "TRUNCATE TABLE users", "App")
			if err == nil || ok {
				t.Errorf("lct=%d: a failed catalog lookup answered ok=%v err=%v; want a loud error, never a guessed spelling", lct, ok, err)
			}
		}
	})

	t.Run("non_truncate_is_not_recognised_in_any_regime", func(t *testing.T) {
		t.Parallel()
		for _, lct := range []int{0, 1, 2} {
			r := &CDCReader{lowerCaseTableNames: lct, storedTableNameLookup: mustNotLookup.lookup}
			if _, _, ok, err := r.truncateEmitNames(context.Background(), "ALTER TABLE T1 ADD COLUMN x INT", "source_db"); ok || err != nil {
				t.Errorf("lct=%d: ALTER recognised as TRUNCATE (ok=%v err=%v)", lct, ok, err)
			}
		}
	})
}
