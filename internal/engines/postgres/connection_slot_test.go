// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsConnectionSlotExhausted pins the connection-resilience Phase 2b
// classifier: ONLY the SQLSTATE-53300 slot-exhaustion class (plain
// too_many_connections AND the superuser-reserved-slots FATAL, which
// share that code) is retryable; every genuine error — bad DSN,
// permission denied, a real COPY failure, other transient SQLSTATEs —
// must return false so it fails loudly instead of being masked as
// backpressure.
func TestIsConnectionSlotExhausted(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// --- retryable: the slot-exhaustion class ---
		{
			name: "too_many_connections SQLSTATE 53300",
			err:  &pgconn.PgError{Code: "53300", Message: "sorry, too many clients already"},
			want: true,
		},
		{
			name: "superuser-reserved-slots FATAL (also 53300)",
			err:  &pgconn.PgError{Code: "53300", Severity: "FATAL", Message: "remaining connection slots are reserved for roles with the SUPERUSER attribute"},
			want: true,
		},
		{
			name: "53300 wrapped as the engine wraps open/ping errors",
			err:  fmt.Errorf("postgres: ping: %w", fmt.Errorf("postgres: open: %w", &pgconn.PgError{Code: "53300", Message: "too many clients"})),
			want: true,
		},
		{
			name: "superuser-reserved-slots FATAL as a bare startup string (no structured PgError)",
			err:  errors.New("postgres: open: FATAL: remaining connection slots are reserved for roles with the SUPERUSER attribute (SQLSTATE 53300)"),
			want: true,
		},

		// --- NOT retryable: genuine errors must fail loudly ---
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "permission denied 42501",
			err:  &pgconn.PgError{Code: "42501", Message: "permission denied for table foo"},
			want: false,
		},
		{
			name: "unique violation 23505 (a real data error, never backpressure)",
			err:  &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"},
			want: false,
		},
		{
			name: "serialization failure 40001 (retryable for the APPLIER, not a slot-exhaustion)",
			err:  &pgconn.PgError{Code: "40001", Message: "could not serialize access"},
			want: false,
		},
		{
			name: "cannot_connect_now 57P03 (server-restart transient, not slot exhaustion)",
			err:  &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"},
			want: false,
		},
		{
			name: "bad DSN / generic connection-refused string",
			err:  errors.New("postgres: open: dial tcp 127.0.0.1:5432: connect: connection refused"),
			want: false,
		},
		{
			name: "configuration_limit_exceeded 53400 (sibling class-53 code, NOT slot exhaustion)",
			err:  &pgconn.PgError{Code: "53400", Message: "configuration limit exceeded"},
			want: false,
		},
		{
			name: "out_of_memory 53200 (sibling class-53 code, NOT slot exhaustion)",
			err:  &pgconn.PgError{Code: "53200", Message: "out of memory"},
			want: false,
		},
		{
			name: "generic message containing 'too many' but not the reserved-slots fragment",
			err:  errors.New("too many open files"),
			want: false,
		},
	}

	var eng Engine
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := eng.IsConnectionSlotExhausted(tc.err); got != tc.want {
				t.Errorf("IsConnectionSlotExhausted(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
