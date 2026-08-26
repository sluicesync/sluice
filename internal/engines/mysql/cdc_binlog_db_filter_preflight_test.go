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
// fails every spelling.
type dbFilterDriver struct{}

type dbFilterConn struct {
	do, ignore         string
	spelling84, norows bool
	failAll            bool
}

func (dbFilterDriver) Open(dsn string) (driver.Conn, error) {
	c := &dbFilterConn{}
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
			"ignore_on_unrelated_db":        {"do=|ignore=other", listScope("app")},
			"do_covers_synced_db":           {"do=app|ignore=", listScope("app")},
			"do_covers_all_synced":          {"do=app,billing|ignore=", listScope("app", "billing")},
			"do_wins_over_ignore":           {"do=app|ignore=app", listScope("app")}, // server rule: non-empty do-list makes ignore moot
			"ignore_unmatched_by_predicate": {"do=|ignore=other", predicateScope("app", "billing")},
			"binlog_disabled_no_row":        {"norows=1|do=|ignore=app", listScope("app")}, // existing anchor-time refusal owns this
		}
		for name, tc := range pass {
			if err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, tc.spec), tc.scope); err != nil {
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
			"ignore_case_insensitive":      {"do=|ignore=APP", listScope("app"), []string{"--binlog-ignore-db"}},
			"ignore_hits_predicate_scope":  {"do=|ignore=billing", predicateScope("app", "billing"), []string{`"billing"`}},
			"do_omits_synced_db":           {"do=whitelisted|ignore=", listScope("app"), []string{`"app"`, "--binlog-do-db", "silently empty"}},
			"do_omits_one_of_several":      {"do=app|ignore=", listScope("app", "billing"), []string{`"billing"`}},
			"do_with_unknowable_scope":     {"do=whitelisted|ignore=", predicateScope("app"), []string{"cannot enumerate"}}, // fail-closed: subset unprovable from a predicate
			"eightfour_spelling_same_scan": {"84=1|do=whitelisted|ignore=", listScope("app"), []string{"--binlog-do-db"}},
		}
		for name, tc := range refuse {
			err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, tc.spec), tc.scope)
			wantDBFilterRefusal(t, err, name+" ("+tc.spec+")", tc.phrases...)
		}
	})

	t.Run("read_failure_is_plain_error", func(t *testing.T) {
		t.Parallel()
		err := preflightBinlogDBFilter(ctx, newDBFilterDB(t, "err=1"), listScope("app"))
		if err == nil {
			t.Fatal("preflight with a failing status read = nil; want a loud error")
		}
		if _, ok := sluicecode.FromError(err); ok {
			t.Fatalf("a failed master-status read must not carry the refusal code: %v", err)
		}
	})
}
