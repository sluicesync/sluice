// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestStartApplyMarks_TransientProbeRetriesDefiniteVerdictDisables pins GC-41
// (f): startApplyMarks disabled the marks on ANY availability-probe error, so
// a target flapping on the retry right after a crash replayed without them.
// A transient probe failure must now come back retriable with the tracker
// untouched and no WARN; only a definite verdict (absent, no privilege, the
// table or a column missing under the probe) disables and WARNs
// APPLY-MARKS-UNAVAILABLE. Both probe stages are covered: the existence query
// and the statement-shape probes.
func TestStartApplyMarks_TransientProbeRetriesDefiniteVerdictDisables(t *testing.T) {
	cases := []struct {
		name      string
		script    markProbeScript
		transient bool
	}{
		{"existence query: connection lost", markProbeScript{queryErr: io.EOF}, true},
		{"statement probe: invalid connection", markProbeScript{exists: true, execErr: gomysql.ErrInvalidConn}, true},
		{"statement probe: lock wait timeout", markProbeScript{exists: true, execErr: &gomysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"}}, true},
		{"table absent", markProbeScript{exists: false}, false},
		{"statement probe: permission denied", markProbeScript{exists: true, execErr: &gomysql.MySQLError{Number: 1142, Message: "INSERT command denied to user"}}, false},
		{"statement probe: table vanished", markProbeScript{exists: true, execErr: &gomysql.MySQLError{Number: 1146, Message: "Table doesn't exist"}}, false},
		{"statement probe: column missing", markProbeScript{exists: true, execErr: &gomysql.MySQLError{Number: 1054, Message: "Unknown column 'scope_digest'"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &logcapture.Buffer{}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
			defer slog.SetDefault(prev)

			script := tc.script
			a := &ChangeApplier{db: newMarkProbeDB(t, &script)}
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

// markProbeScript scripts the availability probe's two stages: the
// information_schema existence query (queryErr, else a count from exists)
// and every statement-shape probe (execErr, else success).
type markProbeScript struct {
	exists   bool
	queryErr error
	execErr  error
}

type markProbeDriver struct{ script *markProbeScript }

type markProbeConn struct{ script *markProbeScript }

func (d markProbeDriver) Open(string) (driver.Conn, error) { return markProbeConn(d), nil }

func (markProbeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (markProbeConn) Close() error                        { return nil }
func (markProbeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c markProbeConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.script.execErr != nil {
		return nil, c.script.execErr
	}
	return driver.RowsAffected(0), nil
}

func (c markProbeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.script.queryErr != nil {
		return nil, c.script.queryErr
	}
	return &bootRows{value: countValue(c.script.exists)}, nil
}

func newMarkProbeDB(t *testing.T, script *markProbeScript) *sql.DB {
	t.Helper()
	name := "sluice-mark-probe-" + t.Name()
	sql.Register(name, markProbeDriver{script: script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
