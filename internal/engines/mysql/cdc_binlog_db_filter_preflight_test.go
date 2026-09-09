// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// dbFilterDriver is a minimal fake driver whose DSN encodes the
// master-status row as "do=<list>|ignore=<list>" plus optional flags:
// "84" makes SHOW BINARY LOG STATUS the working spelling (8.4 sim;
// default is the 8.0 shape where it errors and SHOW MASTER STATUS
// works), "norows" returns an empty result (binlog disabled), "err"
// fails every spelling. The case-rule reads the preflight makes when a
// filter is present are simulated too: "lct=<n>" answers
// @@global.lower_case_table_names (default 0, the Linux default),
// "mariadb" makes SELECT VERSION() report a MariaDB server, "lcterr"
// fails the lower_case_table_names read.
type dbFilterDriver struct{}

type dbFilterConn struct {
	do, ignore         string
	spelling84, norows bool
	failAll            bool

	lct     string
	mariadb bool
	lctErr  bool
}

func (dbFilterDriver) Open(dsn string) (driver.Conn, error) {
	c := &dbFilterConn{lct: "0"}
	for _, kv := range strings.Split(dsn, "|") {
		k, v, _ := strings.Cut(kv, "=")
		switch k {
		case "do":
			c.do = v
		case "ignore":
			c.ignore = v
		case "84":
			c.spelling84 = true
		case "norows":
			c.norows = true
		case "err":
			c.failAll = true
		case "lct":
			c.lct = v
		case "mariadb":
			c.mariadb = true
		case "lcterr":
			c.lctErr = true
		}
	}
	return c, nil
}

func (c *dbFilterConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *dbFilterConn) Close() error              { return nil }
func (c *dbFilterConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }

func (c *dbFilterConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	working := "SHOW MASTER STATUS"
	if c.spelling84 {
		working = "SHOW BINARY LOG STATUS"
	}
	switch query {
	case "SHOW BINARY LOG STATUS", "SHOW MASTER STATUS", "SHOW BINLOG STATUS":
		if c.failAll {
			return nil, errors.New("simulated status failure")
		}
		if query != working {
			return nil, errors.New("You have an error in your SQL syntax")
		}
		return &masterStatusFilterRows{do: c.do, ignore: c.ignore, norows: c.norows}, nil
	case "SELECT @@global.lower_case_table_names":
		if c.lctErr {
			return nil, errors.New("simulated lower_case_table_names failure")
		}
		return &scalarFakeRows{col: "@@global.lower_case_table_names", val: c.lct}, nil
	case "SELECT VERSION()":
		v := "8.0.46"
		if c.mariadb {
			v = "11.4.13-MariaDB-ubu2404-log"
		}
		return &scalarFakeRows{col: "VERSION()", val: v}, nil
	}
	return nil, errors.New("unexpected query: " + query)
}

// masterStatusFilterRows fakes the 5-column master-status row with the
// filter columns populated.
type masterStatusFilterRows struct {
	do, ignore string
	norows     bool
	sent       bool
}

func (r *masterStatusFilterRows) Columns() []string {
	return []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
}
func (r *masterStatusFilterRows) Close() error { return nil }
func (r *masterStatusFilterRows) Next(dest []driver.Value) error {
	if r.sent || r.norows {
		return io.EOF
	}
	r.sent = true
	dest[0] = "mysql-bin.000003"
	dest[1] = int64(903)
	dest[2] = r.do
	dest[3] = r.ignore
	dest[4] = ""
	return nil
}

// scalarFakeRows fakes a one-column, one-row scalar result (a variable
// or function read).
type scalarFakeRows struct {
	col  string
	val  string
	sent bool
}

func (r *scalarFakeRows) Columns() []string { return []string{r.col} }
func (r *scalarFakeRows) Close() error      { return nil }
func (r *scalarFakeRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}
	r.sent = true
	dest[0] = r.val
	return nil
}

var registerDBFilterOnce sync.Once

func newDBFilterDB(t *testing.T, spec string) *sql.DB {
	t.Helper()
	registerDBFilterOnce.Do(func() { sql.Register("sluice-dbfilter-test", dbFilterDriver{}) })
	db, err := sql.Open("sluice-dbfilter-test", spec)
	if err != nil {
		t.Fatalf("open db-filter db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func listScope(dbs ...string) binlogFilterScope {
	return binlogFilterScope{databases: dbs}
}

func predicateScope(admit ...string) binlogFilterScope {
	set := map[string]bool{}
	for _, d := range admit {
		set[d] = true
	}
	return binlogFilterScope{inScope: func(db string) bool { return set[db] }}
}

func wantDBFilterRefusal(t *testing.T, err error, name string, phrases ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s = nil; want the coded refusal", name)
		return
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCBinlogDBFiltered {
		t.Errorf("%s: want %s; got %T: %v", name, sluicecode.CodeCDCBinlogDBFiltered, err, err)
		return
	}
	for _, phrase := range phrases {
		if !strings.Contains(err.Error(), phrase) {
			t.Errorf("%s: message missing %q; got: %v", name, phrase, err)
		}
	}
	if ce.Hint == "" || !strings.Contains(ce.Hint, "restart") {
		t.Errorf("%s: hint = %q; want the restart remedy", name, ce.Hint)
	}
}

// TestPreflightBinlogDBFilter pins the G6 door across both filter arms
// and both directions each (Bug 246 discipline: a filter on an
// UNRELATED database must pass — over-refusal on a working
// configuration is the class-2 failure this scoping exists to avoid).
//
// The case cells are audit 2026-09-09 RC-1: the door used to fold
// database names on every server, which under-refused the DO arm on
// the case-sensitive Linux default (`--binlog-do-db=APP` with database
// `app` logs nothing for `app`; the fold said it was covered). Every
// cell below tagged with a server setting is one row of the measured
// matrix in the preflight's file comment; the integration twin
// re-derives them from a real server's `SHOW BINLOG EVENTS`.
func TestPreflightBinlogDBFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("passes", func(t *testing.T) {
		t.Parallel()
		pass := map[string]struct {
			spec  string
			scope binlogFilterScope
		}{
			"no_filters":                    {"do=|ignore=", listScope("app")},
			"no_filters_case_rule_unread":   {"do=|ignore=|lcterr=1", listScope("app")}, // the common configuration pays for no extra read
			"ignore_on_unrelated_db":        {"do=|ignore=other", listScope("app")},
			"do_covers_synced_db":           {"do=app|ignore=", listScope("app")},
			"do_covers_all_synced":          {"do=app,billing|ignore=", listScope("app", "billing")},
			"do_wins_over_ignore":           {"do=app|ignore=app", listScope("app")}, // server rule: non-empty do-list makes ignore moot
			"ignore_unmatched_by_predicate": {"do=|ignore=other", predicateScope("app", "billing")},
			"binlog_disabled_no_row":        {"norows=1|do=|ignore=app", listScope("app")}, // existing anchor-time refusal owns this

			// Case cells — the arm the old fold OVER-refused, and the
			// folding servers where the fold is the server's own rule.
			"ignore_case_mismatch_on_exact_server_passes": {"do=|ignore=APP", listScope("app")},                 // MySQL/MariaDB lct=0: the server logs `app`
			"do_case_mismatch_on_folding_mysql_passes":    {"lct=1|do=APP|ignore=", listScope("app")},           // MySQL lct=1 folds the entry
			"ignore_case_mismatch_on_folding_mariadb":     {"lct=1|mariadb=1|do=|ignore=APP", listScope("app")}, // MariaDB never folds: `APP` ≠ stored `app`, so `app` IS logged
			"do_lowercase_entry_on_folding_mariadb":       {"lct=1|mariadb=1|do=app|ignore=", listScope("App")}, // the stored name is the fold; the entry matches it
		}
		for name, tc := range pass {
			if err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, tc.spec), tc.scope, FlavorVanilla); err != nil {
				t.Errorf("%s (%q) = %v; want nil", name, tc.spec, err)
			}
		}
	})

	t.Run("refuses", func(t *testing.T) {
		t.Parallel()
		refuse := map[string]struct {
			spec    string
			scope   binlogFilterScope
			phrases []string
		}{
			"ignore_contains_synced_db":    {"do=|ignore=other,app", listScope("app"), []string{`"app"`, "--binlog-ignore-db", "silently empty"}},
			"ignore_hits_predicate_scope":  {"do=|ignore=billing", predicateScope("app", "billing"), []string{`"billing"`}},
			"do_omits_synced_db":           {"do=whitelisted|ignore=", listScope("app"), []string{`"app"`, "--binlog-do-db", "silently empty"}},
			"do_omits_one_of_several":      {"do=app|ignore=", listScope("app", "billing"), []string{`"billing"`}},
			"do_with_unknowable_scope":     {"do=whitelisted|ignore=", predicateScope("app"), []string{"cannot enumerate"}}, // fail-closed: subset unprovable from a predicate
			"eightfour_spelling_same_scan": {"84=1|do=whitelisted|ignore=", listScope("app"), []string{"--binlog-do-db"}},

			// Case cells — the RC-1 loss and its siblings.
			"do_case_mismatch_on_exact_server":           {"do=APP|ignore=", listScope("app"), []string{`"app"`, "--binlog-do-db", "byte-exact"}},                 // RC-1: the server logs NOTHING for `app`
			"ignore_case_mismatch_on_folding_mysql":      {"lct=1|do=|ignore=APP", listScope("app"), []string{`"app"`, "--binlog-ignore-db", "case-insensitive"}}, // MySQL lct=1 folds the ignore entry too
			"do_case_mismatch_on_folding_mariadb":        {"lct=1|mariadb=1|do=APP|ignore=", listScope("app"), []string{`"app"`, "--binlog-do-db", "MariaDB"}},    // MariaDB: `APP` ≠ stored `app` even at lct=1
			"ignore_case_mismatch_on_lct2_mysql_is_fold": {"lct=2|do=|ignore=APP", listScope("app"), []string{`"app"`, "--binlog-ignore-db"}},                     // every predicate in this engine tests != 0, not == 1
			"ignore_case_mismatch_via_predicate":         {"do=|ignore=APP", predicateScope("APP"), []string{`"APP"`}},                                            // the predicate arm is the caller's own equality
		}
		for name, tc := range refuse {
			err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, tc.spec), tc.scope, FlavorVanilla)
			wantDBFilterRefusal(t, err, name+" ("+tc.spec+")", tc.phrases...)
		}
	})

	t.Run("flavor_is_believed_over_a_version_string_without_the_word", func(t *testing.T) {
		t.Parallel()
		// A proxy's server_version can omit "MariaDB"; the reader's own
		// flavor then decides, and the safe (byte-exact) branch wins.
		err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, "lct=1|do=APP|ignore="), listScope("app"), FlavorMariaDB)
		wantDBFilterRefusal(t, err, "flavor_or_version", `"app"`, "MariaDB")
	})

	t.Run("predicate_scope_on_a_folding_server_is_offered_the_stored_spelling", func(t *testing.T) {
		t.Parallel()
		// A predicate-only scope (no concrete list) admits `app`; the
		// server's entry is `APP` on a folding MySQL. Byte-exact alone
		// would pass here while the server skips `app` — silent.
		err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, "lct=1|do=|ignore=APP"), predicateScope("app"), FlavorVanilla)
		wantDBFilterRefusal(t, err, "predicate_folding", `"app"`, "--binlog-ignore-db")
		// On an exact server the same shape must NOT refuse: `APP` and
		// `app` are two databases and the server logs `app`.
		if err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, "lct=0|do=|ignore=APP"), predicateScope("app"), FlavorVanilla); err != nil {
			t.Fatalf("predicate scope on an exact server refused a working configuration: %v", err)
		}
	})

	t.Run("read_failure_is_plain_error", func(t *testing.T) {
		t.Parallel()
		for name, spec := range map[string]string{
			"status_read":    "err=1",
			"case_rule_read": "do=app|ignore=|lcterr=1",
		} {
			err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, spec), listScope("app"), FlavorVanilla)
			if err == nil {
				t.Fatalf("%s: preflight with a failing read = nil; want a loud error", name)
			}
			if _, ok := sluicecode.FromError(err); ok {
				t.Fatalf("%s: a failed read must not carry the refusal code: %v", name, err)
			}
		}
	})
}

// TestBinlogFilterCaseRule_MeasuredMatrix pins the rule function to the
// table measured on real servers on 2026-09-09 (uppercase entry
// `SOURCE_DB`, database `source_db`; see the preflight's file comment).
// The integration twin derives the same cells from `SHOW BINLOG EVENTS`;
// this one makes the unit of decision — one function for both arms —
// fail the moment someone "simplifies" it back to a fold.
func TestBinlogFilterCaseRule_MeasuredMatrix(t *testing.T) {
	t.Parallel()
	cells := []struct {
		name    string
		rule    binlogFilterCaseRule
		entry   string
		db      string
		matches bool
	}{
		{"mysql_lct0_mixed_case", binlogFilterCaseRule{lct: 0}, "SOURCE_DB", "source_db", false},
		{"mysql_lct0_exact", binlogFilterCaseRule{lct: 0}, "source_db", "source_db", true},
		{"mysql_lct1_mixed_case", binlogFilterCaseRule{lct: 1}, "SOURCE_DB", "source_db", true},
		{"mysql_lct2_mixed_case", binlogFilterCaseRule{lct: 2}, "SOURCE_DB", "source_db", true},
		{"mariadb_lct0_mixed_case", binlogFilterCaseRule{lct: 0, mariadb: true}, "SOURCE_DB", "source_db", false},
		{"mariadb_lct0_exact", binlogFilterCaseRule{lct: 0, mariadb: true}, "source_db", "source_db", true},
		{"mariadb_lct1_mixed_case", binlogFilterCaseRule{lct: 1, mariadb: true}, "SOURCE_DB", "source_db", false},
		{"mariadb_lct1_lowercase_entry", binlogFilterCaseRule{lct: 1, mariadb: true}, "source_db", "source_db", true},
		{"mariadb_lct1_scope_spelled_upper", binlogFilterCaseRule{lct: 1, mariadb: true}, "source_db", "SOURCE_DB", true}, // the operator's spelling resolves to the stored lowercase name
	}
	for _, c := range cells {
		if got := c.rule.match(c.entry, c.db); got != c.matches {
			t.Errorf("%s: match(%q, %q) under %+v = %v; want %v", c.name, c.entry, c.db, c.rule, got, c.matches)
		}
	}
}
