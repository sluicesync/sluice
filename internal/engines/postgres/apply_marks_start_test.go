// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestStartApplyMarks_TransientProbeRetriesDefiniteVerdictDisables is the
// MySQL pin's Postgres twin (GC-41 (f)): a transient availability-probe
// failure must come back retriable with the tracker untouched and no WARN;
// only a definite verdict — absent, the privilege check false, a privilege
// or resolution error — disables and WARNs APPLY-MARKS-UNAVAILABLE. Both
// probe stages are covered: to_regclass and the privilege query.
func TestStartApplyMarks_TransientProbeRetriesDefiniteVerdictDisables(t *testing.T) {
	cases := []struct {
		name      string
		script    pgMarkProbeScript
		transient bool
	}{
		{"existence query: connection lost", pgMarkProbeScript{existsErr: io.EOF}, true},
		{"existence query: serialization failure", pgMarkProbeScript{existsErr: &pgconn.PgError{Code: "40001"}}, true},
		{"privilege query: connection lost", pgMarkProbeScript{exists: true, privErr: io.EOF}, true},
		{"table absent", pgMarkProbeScript{exists: false}, false},
		{"privileges missing", pgMarkProbeScript{exists: true, priv: false}, false},
		{"privilege query: permission denied for schema", pgMarkProbeScript{exists: true, privErr: &pgconn.PgError{Code: "42501", Message: "permission denied for schema ctl"}}, false},
		{"privilege query: relation vanished", pgMarkProbeScript{exists: true, privErr: &pgconn.PgError{Code: "42P01", Message: "relation does not exist"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &logcapture.Buffer{}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
			defer slog.SetDefault(prev)

			script := tc.script
			a := &ChangeApplier{db: newPGMarkProbeDB(t, &script), controlSchema: "public"}
			// A tracker a previous run loaded: a transient probe must leave it
			// as it is, a definite verdict must disable it.
			a.marks.Load("s", "", nil)
			err := a.startApplyMarks(context.Background(), "s")
			warned := strings.Contains(logs.String(), applymarks.UnavailableMarker)
			if tc.transient {
				if err == nil || !applymarks.Transient(err) {
					t.Fatalf("startApplyMarks = %v; want a retriable error — a transient probe failure must be retried, not read as the table being unusable", err)
				}
				if !a.marks.Enabled() || warned {
					t.Fatalf("a transient probe failure disabled the marks (enabled=%v) or WARNed %s (%v)", a.marks.Enabled(), applymarks.UnavailableMarker, warned)
				}
				return
			}
			if err != nil {
				t.Fatalf("startApplyMarks = %v; a definite verdict must WARN and run without marks, never refuse", err)
			}
			if a.marks.Enabled() || !warned {
				t.Fatalf("a definite verdict left the marks enabled (%v) or did not WARN %s (%v):\n%s", a.marks.Enabled(), applymarks.UnavailableMarker, warned, logs.String())
			}
		})
	}
}

// pgMarkProbeScript scripts the probe's two queries: to_regclass (existsErr,
// else exists) and the has_table_privilege conjunction (privErr, else priv).
type pgMarkProbeScript struct {
	exists    bool
	existsErr error
	priv      bool
	privErr   error
}

type pgMarkProbeDriver struct{ script *pgMarkProbeScript }

type pgMarkProbeConn struct{ script *pgMarkProbeScript }

func (d pgMarkProbeDriver) Open(string) (driver.Conn, error) { return pgMarkProbeConn(d), nil }

func (pgMarkProbeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (pgMarkProbeConn) Close() error                        { return nil }
func (pgMarkProbeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c pgMarkProbeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "to_regclass") {
		if c.script.existsErr != nil {
			return nil, c.script.existsErr
		}
		return &pgMarkProbeRows{value: c.script.exists}, nil
	}
	if c.script.privErr != nil {
		return nil, c.script.privErr
	}
	return &pgMarkProbeRows{value: c.script.priv}, nil
}

// pgMarkProbeRows serves one boolean row.
type pgMarkProbeRows struct {
	value bool
	done  bool
}

func (*pgMarkProbeRows) Columns() []string { return []string{"v"} }
func (*pgMarkProbeRows) Close() error      { return nil }

func (r *pgMarkProbeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = r.value
	r.done = true
	return nil
}

func newPGMarkProbeDB(t *testing.T, script *pgMarkProbeScript) *sql.DB {
	t.Helper()
	name := "sluice-pg-mark-probe-" + t.Name()
	sql.Register(name, pgMarkProbeDriver{script: script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
