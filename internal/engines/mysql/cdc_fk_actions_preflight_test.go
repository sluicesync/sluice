// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// fkActionsDriver is a minimal fake driver whose DSN selects the census
// result set. Mirrors the replicaSource / binlogFormat fixtures; kept
// separate so each preflight's fixture stays single-purpose.
//
//	cascade         one FK, ON DELETE CASCADE
//	update_setnull  one FK, ON UPDATE SET NULL (delete NO ACTION)
//	setdefault      one FK, ON DELETE SET DEFAULT
//	restrict        one FK, RESTRICT both ways
//	plain           one FK, NO ACTION both ways (the InnoDB default)
//	mixed           cascade on child_c + restrict on child_r
//	none            zero FK rows
//	err             the census query errors (privilege sim)
type fkActionsDriver struct{}

type fkActionsConn struct{ scenario string }

func (fkActionsDriver) Open(dsn string) (driver.Conn, error) {
	return &fkActionsConn{scenario: dsn}, nil
}

func (c *fkActionsConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *fkActionsConn) Close() error { return nil }
func (c *fkActionsConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected Begin")
}

// fkRow is (table_name, constraint_name, referenced_table_name,
// update_rule, delete_rule) — the census projection.
type fkRow [5]string

func (c *fkActionsConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "referential_constraints") {
		return nil, errors.New("unexpected query: " + query)
	}
	switch c.scenario {
	case "cascade":
		return &fkRows{rows: []fkRow{{"child", "fk_child_parent", "parent", "NO ACTION", "CASCADE"}}}, nil
	case "update_setnull":
		return &fkRows{rows: []fkRow{{"child", "fk_child_parent", "parent", "SET NULL", "NO ACTION"}}}, nil
	case "setdefault":
		return &fkRows{rows: []fkRow{{"child", "fk_child_parent", "parent", "NO ACTION", "SET DEFAULT"}}}, nil
	case "restrict":
		return &fkRows{rows: []fkRow{{"child", "fk_child_parent", "parent", "RESTRICT", "RESTRICT"}}}, nil
	case "plain":
		return &fkRows{rows: []fkRow{{"child", "fk_child_parent", "parent", "NO ACTION", "NO ACTION"}}}, nil
	case "mixed":
		return &fkRows{rows: []fkRow{
			{"child_c", "fk_c", "parent", "NO ACTION", "CASCADE"},
			{"child_r", "fk_r", "parent", "RESTRICT", "RESTRICT"},
		}}, nil
	case "none":
		return &fkRows{}, nil
	default:
		return nil, errors.New("SELECT command denied to user")
	}
}

type fkRows struct {
	rows []fkRow
	sent int
}

func (r *fkRows) Columns() []string {
	return []string{"table_name", "constraint_name", "referenced_table_name", "update_rule", "delete_rule"}
}
func (r *fkRows) Close() error { return nil }
func (r *fkRows) Next(dest []driver.Value) error {
	if r.sent >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.sent]
	r.sent++
	for i := range row {
		dest[i] = row[i]
	}
	return nil
}

var registerFKActionsOnce sync.Once

func newFKActionsDB(t *testing.T, scenario string) *sql.DB {
	t.Helper()
	registerFKActionsOnce.Do(func() { sql.Register("sluice-fkactions-test", fkActionsDriver{}) })
	db, err := sql.Open("sluice-fkactions-test", scenario)
	if err != nil {
		t.Fatalf("open fk-actions db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// captureFKWarnLog swaps the default slog handler for a WARN-level text
// handler writing into the returned buffer, restored on cleanup.
func captureFKWarnLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestPreflightFKReferentialActions pins the G9 WARN in BOTH
// directions: it fires for every invisible-action family (CASCADE /
// SET NULL / SET DEFAULT, on either rule) naming the FK, and stays
// SILENT for plain and RESTRICT/NO ACTION FKs — those cause no
// invisible source-side writes, and warning on them would fire on
// practically every FK-carrying schema.
//
// Mutation-verified in both directions (2026-08-26; mutants
// grep-confirmed before each run, targeted-revert after): forcing
// warnFKReferentialActions to return before its final WARN fails the
// firing cells; forcing fkActionInvisible to return true fails the
// restrict/plain silent cells.
func TestPreflightFKReferentialActions(t *testing.T) {
	scope := binlogFilterScope{databases: []string{"app"}}

	fires := map[string]string{
		"delete_cascade": "cascade",
		"update_setnull": "update_setnull",
		"setdefault":     "setdefault",
	}
	for name, scenario := range fires {
		t.Run("fires_"+name, func(t *testing.T) {
			buf := captureFKWarnLog(t)
			preflightFKReferentialActions(context.Background(), newFKActionsDB(t, scenario), scope)
			out := buf.String()
			if !strings.Contains(out, fkReferentialActionMarker) {
				t.Fatalf("no %s WARN for scenario %q; log: %s", fkReferentialActionMarker, scenario, out)
			}
			for _, phrase := range []string{
				"app.child",              // names the child table (where the divergence lives)
				"fk_child_parent",        // names the constraint
				"NEVER enter the binlog", // the mechanism
				"sluice verify",          // the remedy
			} {
				if !strings.Contains(out, phrase) {
					t.Errorf("WARN missing %q; log: %s", phrase, out)
				}
			}
		})
	}

	silent := map[string]string{
		"restrict_only": "restrict",
		"plain_fk":      "plain",
		"no_fks":        "none",
	}
	for name, scenario := range silent {
		t.Run("silent_"+name, func(t *testing.T) {
			buf := captureFKWarnLog(t)
			preflightFKReferentialActions(context.Background(), newFKActionsDB(t, scenario), scope)
			if out := buf.String(); strings.Contains(out, fkReferentialActionMarker) {
				t.Fatalf("scenario %q must stay silent; log: %s", scenario, out)
			}
		})
	}
}

// TestPreflightFKReferentialActions_TableScope pins the Bug 246
// scoping: a cascade-carrying child table the sync's table filter
// excludes stays silent, and the same schema WARNs when the child is
// in scope — the mixed scenario also proves the RESTRICT sibling never
// rides into the WARN.
func TestPreflightFKReferentialActions_TableScope(t *testing.T) {
	t.Run("excluded_child_stays_silent", func(t *testing.T) {
		buf := captureFKWarnLog(t)
		scope := binlogFilterScope{
			databases:    []string{"app"},
			tableAllowed: func(_, table string) bool { return table == "other" },
		}
		preflightFKReferentialActions(context.Background(), newFKActionsDB(t, "cascade"), scope)
		if out := buf.String(); strings.Contains(out, fkReferentialActionMarker) {
			t.Fatalf("out-of-scope child must stay silent; log: %s", out)
		}
	})
	t.Run("included_child_warns_without_restrict_sibling", func(t *testing.T) {
		buf := captureFKWarnLog(t)
		scope := binlogFilterScope{
			databases:    []string{"app"},
			tableAllowed: func(_, table string) bool { return table == "child_c" || table == "child_r" },
		}
		preflightFKReferentialActions(context.Background(), newFKActionsDB(t, "mixed"), scope)
		out := buf.String()
		if !strings.Contains(out, "app.child_c") {
			t.Fatalf("in-scope cascade child missing from WARN; log: %s", out)
		}
		if strings.Contains(out, "child_r") {
			t.Fatalf("RESTRICT sibling must not ride into the WARN; log: %s", out)
		}
	})
}

// TestPreflightFKReferentialActions_ProbeErrorWarns: a failed census
// probe WARNs ("cannot rule the blindness out") instead of silently
// skipping — the SL-1 probe-error discipline. It must never fail the
// open (the function has no error to return, so the assertion is the
// degraded WARN's presence).
func TestPreflightFKReferentialActions_ProbeErrorWarns(t *testing.T) {
	buf := captureFKWarnLog(t)
	preflightFKReferentialActions(context.Background(), newFKActionsDB(t, "err"),
		binlogFilterScope{databases: []string{"app"}})
	out := buf.String()
	if !strings.Contains(out, fkReferentialActionMarker) || !strings.Contains(out, "could not census") {
		t.Fatalf("probe error must emit the degraded %s WARN; log: %s", fkReferentialActionMarker, out)
	}
}

// TestPreflightFKReferentialActions_UnenumerableScopeWarns: a
// predicate-only scope (no concrete database list — a direct-API
// shape; both pipeline paths supply the list) cannot census, and says
// so rather than silently skipping.
func TestPreflightFKReferentialActions_UnenumerableScopeWarns(t *testing.T) {
	buf := captureFKWarnLog(t)
	preflightFKReferentialActions(context.Background(), newFKActionsDB(t, "cascade"),
		binlogFilterScope{inScope: func(string) bool { return true }})
	out := buf.String()
	if !strings.Contains(out, fkReferentialActionMarker) || !strings.Contains(out, "not") {
		t.Fatalf("unenumerable scope must emit the degraded %s WARN; log: %s", fkReferentialActionMarker, out)
	}
}

// TestFKActionInvisible pins the rule classifier's full family — the
// three invisible actions in both spellings' case, and the two
// blocking/no-op rules that must NOT trip the WARN.
func TestFKActionInvisible(t *testing.T) {
	t.Parallel()
	for rule, want := range map[string]bool{
		"CASCADE":     true,
		"cascade":     true,
		"SET NULL":    true,
		"set null":    true,
		"SET DEFAULT": true,
		"RESTRICT":    false,
		"NO ACTION":   false,
		"":            false,
	} {
		if got := fkActionInvisible(rule); got != want {
			t.Errorf("fkActionInvisible(%q) = %v; want %v", rule, got, want)
		}
	}
}
