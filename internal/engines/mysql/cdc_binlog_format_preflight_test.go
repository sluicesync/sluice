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

// binlogFormatDriver is a minimal fake driver whose DSN encodes the
// value SELECT @@GLOBAL.binlog_format returns ("query_error" makes the
// read fail). Mirrors the rowImageDriver fixture one file over; kept
// separate so each preflight's fixture stays single-purpose.
type binlogFormatDriver struct{}

type binlogFormatConn struct{ format string }

func (binlogFormatDriver) Open(dsn string) (driver.Conn, error) {
	return &binlogFormatConn{format: dsn}, nil
}

func (c *binlogFormatConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (c *binlogFormatConn) Close() error              { return nil }
func (c *binlogFormatConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }

func (c *binlogFormatConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT @@GLOBAL.binlog_format" {
		return nil, errors.New("unexpected query: " + query)
	}
	if c.format == "query_error" {
		return nil, errors.New("Unknown system variable 'binlog_format'")
	}
	return &oneFormatRow{val: c.format}, nil
}

type oneFormatRow struct {
	val  string
	done bool
}

func (r *oneFormatRow) Columns() []string { return []string{"@@GLOBAL.binlog_format"} }
func (r *oneFormatRow) Close() error      { return nil }
func (r *oneFormatRow) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.val
	return nil
}

var registerBinlogFormatOnce sync.Once

func newBinlogFormatDB(t *testing.T, format string) *sql.DB {
	t.Helper()
	registerBinlogFormatOnce.Do(func() { sql.Register("sluice-binlogformat-test", binlogFormatDriver{}) })
	db, err := sql.Open("sluice-binlogformat-test", format)
	if err != nil {
		t.Fatalf("open binlog-format db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestPreflightBinlogFormat pins the roadmap-68e gate across the full
// binlog_format family — ROW passes (case-insensitively; the server
// reports the value as configured), STATEMENT and MIXED (and any
// unknown non-ROW value) refuse with the coded error naming the value,
// the silent-empty-stream consequence, and the remedy. The variable
// exists identically on MariaDB (whose DEFAULT is the refused MIXED),
// so this table covers both binlog flavors' values.
func TestPreflightBinlogFormat(t *testing.T) {
	t.Parallel()
	for _, v := range []string{"ROW", "row", "Row"} {
		if err := preflightBinlogFormat(context.Background(), newBinlogFormatDB(t, v)); err != nil {
			t.Errorf("preflight(%q) = %v; want nil", v, err)
		}
	}

	refuse := []string{"STATEMENT", "statement", "MIXED", "mixed", "SOME_FUTURE_MODE"}
	for _, v := range refuse {
		err := preflightBinlogFormat(context.Background(), newBinlogFormatDB(t, v))
		if err == nil {
			t.Errorf("preflight(%q) = nil; want the coded refusal", v)
			continue
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCBinlogFormatNotRow {
			t.Errorf("preflight(%q): want %s; got %T: %v", v, sluicecode.CodeCDCBinlogFormatNotRow, err, err)
			continue
		}
		for _, phrase := range []string{
			"@@GLOBAL.binlog_format=" + v,  // names the value
			"silently applying NOTHING",    // names the consequence
			"SET GLOBAL binlog_format=ROW", // the remedy
			"MariaDB's default",            // the MIXED-by-default flavor note
		} {
			if !strings.Contains(err.Error(), phrase) {
				t.Errorf("preflight(%q) message missing %q; got: %v", v, phrase, err)
			}
		}
		if ce.Hint == "" || !strings.Contains(ce.Hint, "binlog_format=ROW") {
			t.Errorf("preflight(%q) hint = %q; want the remedy hint", v, ce.Hint)
		}
	}
}

// TestPreflightBinlogFormat_ReadFailureIsPlainError: a failed read is a
// loud plain (uncoded) error — a broken read is not evidence of
// STATEMENT, and the refusal's remedy would be wrong advice (mirrors
// the row-image preflight's failure posture).
func TestPreflightBinlogFormat_ReadFailureIsPlainError(t *testing.T) {
	t.Parallel()
	err := preflightBinlogFormat(context.Background(), newBinlogFormatDB(t, "query_error"))
	if err == nil {
		t.Fatal("preflight with a failing read = nil; want a loud error")
	}
	if _, ok := sluicecode.FromError(err); ok {
		t.Fatalf("a failed @@GLOBAL.binlog_format read must not carry the refusal code: %v", err)
	}
}
