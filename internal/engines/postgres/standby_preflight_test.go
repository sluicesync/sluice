// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Bug 197 pins. The standby preflight (checkNotStandby) and its 25006
// belt (classifyStandbyReadOnly) are pinned with a fake driver so the
// refusal shape can't drift: in_recovery=true refuses with the coded
// SLUICE-E-CDC-STANDBY-SOURCE steer (naming the primary AND that a
// replica stays fine for bulk migrate); in_recovery=false passes; a
// SQLSTATE-25006 publication-write failure gains the same code while
// any other error passes through untouched.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// recoveryDriver is a minimal database/sql driver whose every query
// returns one row with a single bool — the pg_is_in_recovery() shape.
type recoveryDriver struct{ inRecovery bool }

func (d recoveryDriver) Open(string) (driver.Conn, error) { return recoveryConn(d), nil }

type recoveryConn struct{ inRecovery bool }

func (c recoveryConn) Prepare(string) (driver.Stmt, error) {
	return recoveryStmt(c), nil
}
func (recoveryConn) Close() error              { return nil }
func (recoveryConn) Begin() (driver.Tx, error) { return nil, errors.New("no tx") }

type recoveryStmt struct{ inRecovery bool }

func (recoveryStmt) Close() error  { return nil }
func (recoveryStmt) NumInput() int { return -1 }
func (recoveryStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("exec unsupported")
}

func (s recoveryStmt) Query([]driver.Value) (driver.Rows, error) {
	return &recoveryRows{v: s.inRecovery}, nil
}

type recoveryRows struct {
	v    bool
	done bool
}

func (*recoveryRows) Columns() []string { return []string{"pg_is_in_recovery"} }
func (*recoveryRows) Close() error      { return nil }
func (r *recoveryRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.v
	return nil
}

// newRecoveryDB opens a *sql.DB over the fake driver. Unique driver
// name per call — database/sql panics on duplicate registration.
func newRecoveryDB(t *testing.T, inRecovery bool) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("recovery-fake-%p", t)
	sql.Register(name, recoveryDriver{inRecovery: inRecovery})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestCheckNotStandby(t *testing.T) {
	ctx := context.Background()

	t.Run("in recovery — coded refusal steering to the primary", func(t *testing.T) {
		err := checkNotStandby(ctx, newRecoveryDB(t, true))
		if err == nil {
			t.Fatal("standby source passed the preflight; want the coded refusal")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCStandbySource {
			t.Fatalf("err = %v; want code %s", err, sluicecode.CodeCDCStandbySource)
		}
		for _, want := range []string{"read replica", "PRIMARY endpoint", "pg_is_in_recovery", "bulk `sluice migrate`"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q missing %q", err.Error(), want)
			}
		}
	})

	t.Run("primary — passes", func(t *testing.T) {
		if err := checkNotStandby(ctx, newRecoveryDB(t, false)); err != nil {
			t.Fatalf("primary source refused: %v", err)
		}
	})
}

func TestClassifyStandbyReadOnly(t *testing.T) {
	t.Run("25006 gains the coded steer", func(t *testing.T) {
		raw := fmt.Errorf("postgres: create publication %q: %w", "sluice_pub",
			&pgconn.PgError{Code: "25006", Message: "cannot execute CREATE PUBLICATION in a read-only transaction"})
		err := classifyStandbyReadOnly(raw)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCStandbySource {
			t.Fatalf("err = %v; want code %s", err, sluicecode.CodeCDCStandbySource)
		}
		if !strings.Contains(err.Error(), "PRIMARY endpoint") {
			t.Errorf("belt refusal %q does not steer to the primary", err.Error())
		}
		// The raw driver error stays in the chain (errors.As-traversable).
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Error("wrapped error lost the underlying *pgconn.PgError")
		}
	})

	t.Run("other errors pass through unchanged", func(t *testing.T) {
		raw := errors.New("postgres: create publication: connection reset")
		if got := classifyStandbyReadOnly(raw); got != raw { //nolint:errorlint // identity is the assertion
			t.Fatalf("non-25006 error was rewrapped: %v", got)
		}
		otherPg := fmt.Errorf("x: %w", &pgconn.PgError{Code: "42501"})
		if got := classifyStandbyReadOnly(otherPg); got != otherPg { //nolint:errorlint // identity is the assertion
			t.Fatalf("non-25006 PgError was rewrapped: %v", got)
		}
	})

	t.Run("nil passes through", func(t *testing.T) {
		if got := classifyStandbyReadOnly(nil); got != nil {
			t.Fatalf("nil in, %v out", got)
		}
	})
}
