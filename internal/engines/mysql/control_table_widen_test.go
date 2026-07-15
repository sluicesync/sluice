// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 65a unit pins: the TEXT → LONGTEXT position-column
// widen. Exercised against a scriptable fake driver (the
// row_writer_reparent_retry_test.go precedent) so the load-bearing
// shapes — detect-first (no DDL at all when the column is already
// LONGTEXT — the PlanetScale safe-migrations constraint), the ALTER
// firing exactly once, and the 1105-classified loud refusal — are
// pinned without a container. The end-to-end migration against real
// MySQL lives in control_table_integration_test.go.

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

// widenScript scripts the fake driver: detectTypes[i] is the DATA_TYPE
// the (i+1)-th information_schema.COLUMNS detect query returns (""
// means no row → sql.ErrNoRows); alterErr, when non-nil, is returned
// by any ALTER ... MODIFY exec. alterCalls counts MODIFY execs.
type widenScript struct {
	detectTypes []string
	alterErr    error

	detectCalls atomic.Int64
	alterCalls  atomic.Int64
}

type widenDriver struct{ script *widenScript }

type widenConn struct{ script *widenScript }

func (d widenDriver) Open(string) (driver.Conn, error) { return widenConn(d), nil }

func (widenConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (widenConn) Close() error                        { return nil }
func (widenConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c widenConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "MODIFY COLUMN") {
		c.script.alterCalls.Add(1)
		if c.script.alterErr != nil {
			return nil, c.script.alterErr
		}
	}
	return driver.RowsAffected(0), nil
}

func (c widenConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "information_schema.COLUMNS") {
		n := c.script.detectCalls.Add(1) // 1-based
		idx := int(n - 1)
		dataType := ""
		if idx < len(c.script.detectTypes) {
			dataType = c.script.detectTypes[idx]
		}
		if dataType == "" {
			return &dataTypeRows{done: true}, nil
		}
		return &dataTypeRows{value: dataType}, nil
	}
	return &dataTypeRows{done: true}, nil
}

// dataTypeRows serves a single-column (DATA_TYPE) result with zero or
// one row.
type dataTypeRows struct {
	value string
	done  bool
}

func (*dataTypeRows) Columns() []string { return []string{"DATA_TYPE"} }
func (*dataTypeRows) Close() error      { return nil }

func (r *dataTypeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = r.value
	r.done = true
	return nil
}

func newWidenDB(t *testing.T, script *widenScript) *sql.DB {
	t.Helper()
	name := "sluice-widen-test-" + t.Name()
	sql.Register(name, widenDriver{script: script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open widen db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestEnsureLongTextPositionColumn_WidensExactlyOnce pins the
// detect-then-ALTER idempotency: a TEXT column is widened on the first
// ensure, and the second ensure — now seeing longtext — issues NO DDL
// at all (the safe-migrations-critical property: an already-widened
// table must never send an ALTER a PlanetScale production branch would
// refuse).
func TestEnsureLongTextPositionColumn_WidensExactlyOnce(t *testing.T) {
	script := &widenScript{detectTypes: []string{"text", "longtext"}}
	db := newWidenDB(t, script)

	ctx := context.Background()
	if err := ensureLongTextPositionColumn(ctx, db, "", controlTableName, "source_position", "LONGTEXT NOT NULL"); err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	if got := script.alterCalls.Load(); got != 1 {
		t.Fatalf("ALTER MODIFY calls after first ensure = %d; want 1", got)
	}
	if err := ensureLongTextPositionColumn(ctx, db, "", controlTableName, "source_position", "LONGTEXT NOT NULL"); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	if got := script.alterCalls.Load(); got != 1 {
		t.Fatalf("ALTER MODIFY calls after second ensure = %d; want still 1 (idempotent, no DDL on the widened table)", got)
	}
}

// TestEnsureLongTextPositionColumn_MissingColumnIsNoOp pins the
// column-absent tolerance: no row from information_schema means the
// add-column migrations own the case (they now ADD as LONGTEXT), so
// the widen issues no DDL and no error.
func TestEnsureLongTextPositionColumn_MissingColumnIsNoOp(t *testing.T) {
	script := &widenScript{detectTypes: []string{""}}
	db := newWidenDB(t, script)

	if err := ensureLongTextPositionColumn(context.Background(), db, "", shardConsolidationLeaseTableName, "anchor_position", "LONGTEXT NULL"); err != nil {
		t.Fatalf("ensure on missing column: %v", err)
	}
	if got := script.alterCalls.Load(); got != 0 {
		t.Fatalf("ALTER MODIFY calls = %d; want 0", got)
	}
}

// TestEnsureLongTextPositionColumn_SafeMigrationsRefusalIsLoud pins
// the item-65a failure contract: when the widen ALTER itself trips the
// PlanetScale safe-migrations block (Error 1105 "direct DDL is
// disabled"), the error is LOUD and remedy-bearing — it wraps
// ErrSafeMigrationsBlocked, names the column, and carries the exact
// ALTER to ship via a deploy request. Never a silent skip.
func TestEnsureLongTextPositionColumn_SafeMigrationsRefusalIsLoud(t *testing.T) {
	script := &widenScript{
		detectTypes: []string{"text"},
		alterErr: &gomysql.MySQLError{
			Number:  1105,
			Message: "direct DDL is disabled",
		},
	}
	db := newWidenDB(t, script)

	err := ensureLongTextPositionColumn(context.Background(), db, "", controlTableName, "source_position", "LONGTEXT NOT NULL")
	if err == nil {
		t.Fatal("ensure = nil; want the loud safe-migrations refusal")
	}
	if !errors.Is(err, ErrSafeMigrationsBlocked) {
		t.Errorf("errors.Is(err, ErrSafeMigrationsBlocked) = false; err = %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		"Safe Migrations",                  // names the feature
		"deploy request",                   // names the remedy channel
		"sluice_cdc_state.source_position", // names the column
		"ALTER TABLE `sluice_cdc_state` MODIFY COLUMN `source_position` LONGTEXT NOT NULL", // the exact statement to ship
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q\nfull message:\n%s", want, msg)
		}
	}
}

// TestEnsureLongTextPositionColumn_OtherAlterErrorsKeepPlainWrap pins
// that a non-1105 ALTER failure keeps the plain widen wrap (no
// safe-migrations remedy misdirection).
func TestEnsureLongTextPositionColumn_OtherAlterErrorsKeepPlainWrap(t *testing.T) {
	script := &widenScript{
		detectTypes: []string{"text"},
		alterErr:    &gomysql.MySQLError{Number: 1142, Message: "ALTER command denied"},
	}
	db := newWidenDB(t, script)

	err := ensureLongTextPositionColumn(context.Background(), db, "", controlTableName, "source_position", "LONGTEXT NOT NULL")
	if err == nil {
		t.Fatal("ensure = nil; want the wrapped ALTER failure")
	}
	if errors.Is(err, ErrSafeMigrationsBlocked) {
		t.Errorf("non-1105 failure wrongly classified as safe-migrations: %v", err)
	}
	if !strings.Contains(err.Error(), "widen source_position to LONGTEXT") {
		t.Errorf("plain wrap missing the widen label: %v", err)
	}
}
