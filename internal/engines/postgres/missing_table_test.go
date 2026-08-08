// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// These pin the CLASSIFICATION, not the happy path (audit backlog C-1). Every
// (code, message) pair below was produced on a real `postgres:16` container;
// the point of the table is that four different SQLSTATEs render the same
// English phrase, so the phrase was never the signal.
//
//	SELECT 1 FROM public.nosuch    → 42P01  relation "public.nosuch" does not exist
//	SELECT 1 FROM noschema.t       → 42P01  relation "noschema.t" does not exist
//	SELECT 1 FROM generate_serie() → 42883  function generate_serie(...) does not exist
//	SET ROLE nobody                → 22023  role "nobody" does not exist
//	psql -d nodb                   → 3D000  database "nodb" does not exist
//
// The 3D000 row is the one that matters: a pooled *sql.DB re-dials, so a
// dropped/renamed target database surfaces that error from an ordinary query —
// and the substring check read it as "the table is absent", i.e. as an EMPTY
// target, which is precisely the answer that lets a run past the populated-
// target refusal.
func TestIsUndefinedTableErr_ClassifiesOnTheSQLSTATENotTheText(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "42P01 missing relation",
			err:  &pgconn.PgError{Code: "42P01", Message: `relation "public.nosuch" does not exist`},
			want: true,
		},
		{
			name: "42P01 missing schema in a qualified reference",
			err:  &pgconn.PgError{Code: "42P01", Message: `relation "noschema.t" does not exist`},
			want: true,
		},
		{
			name: "wrapped 42P01 still classifies (errors.As)",
			err: fmt.Errorf("postgres: probe %q for emptiness: %w", "orders",
				&pgconn.PgError{Code: "42P01", Message: `relation "app.orders" does not exist`}),
			want: true,
		},

		// --- the controls: same English phrase, different condition ---
		{
			name: "3D000 the DATABASE is gone — must not read as an empty table",
			err:  &pgconn.PgError{Code: "3D000", Message: `database "app" does not exist`},
			want: false,
		},
		{
			name: "42883 missing function",
			err:  &pgconn.PgError{Code: "42883", Message: `function generate_serie(integer, integer) does not exist`},
			want: false,
		},
		{
			name: "22023 missing role",
			err:  &pgconn.PgError{Code: "22023", Message: `role "nobody" does not exist`},
			want: false,
		},
		{
			name: "42501 permission denied",
			err:  &pgconn.PgError{Code: "42501", Message: "permission denied for table orders"},
			want: false,
		},
		{
			name: "no SQLSTATE at all — unclassifiable, so not 'absent'",
			err:  errors.New(`relation "app.orders" does not exist`),
			want: false,
		},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUndefinedTableErr(tc.err); got != tc.want {
				t.Errorf("isUndefinedTableErr(%v) = %v; want %v", tc.err, got, tc.want)
			}
		})
	}
}
