// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsReadTransientSQLState pins the exact SQLSTATE set both ways —
// widening or narrowing the read-transient surface must fail here, not slip
// in silently (the triggercdc shape-set discipline).
func TestIsReadTransientSQLState(t *testing.T) {
	pg := func(code string) error {
		return fmt.Errorf("poll: %w", &pgconn.PgError{Code: code, Message: "x"})
	}
	transient := []struct {
		name string
		err  error
	}{
		{"57P01 admin_shutdown", pg("57P01")},
		{"57P02 crash_shutdown", pg("57P02")},
		{"57P03 cannot_connect_now", pg("57P03")},
		{"08000 connection_exception", pg("08000")},
		{"08006 connection_failure", pg("08006")},
		{"08P01 protocol_violation", pg("08P01")},
	}
	for _, c := range transient {
		if !IsReadTransientSQLState(c.err) {
			t.Errorf("IsReadTransientSQLState(%s) = false; want true", c.name)
		}
	}
	terminal := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"42P01 undefined_table (missing change-log = operator fault)", pg("42P01")},
		{"42703 undefined_column", pg("42703")},
		{"28P01 invalid_password", pg("28P01")},
		{"40001 serialization_failure (no poll retry semantics)", pg("40001")},
		{"53100 disk_full (cold-copy shape, not a poll's)", pg("53100")},
		{"55006 object_in_use", pg("55006")},
		{"non-PG error", errors.New("dial tcp: connection refused")},
	}
	for _, c := range terminal {
		if IsReadTransientSQLState(c.err) {
			t.Errorf("IsReadTransientSQLState(%s) = true; want false", c.name)
		}
	}
}
