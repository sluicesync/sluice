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

// TestIsMissingTableErr_TheTruncateSwallowGateUsesTheSameClassification pins the
// SITE, not just the classifier above.
//
// isMissingTableErr gates two `return nil`s — change_applier.go's truncate
// dispatch and the pipelined batch's "truncate" arm. Both SWALLOW the error and
// log a benign stale-publication skip, which makes a false positive here
// strictly worse than one at the read-side probe: the TRUNCATE did not happen,
// the applier reports success, and the stream advances past it.
//
// The expectations are the same real-`postgres:16` (code, message) pairs used
// above, so this is not `isMissingTableErr` compared against `isUndefinedTableErr`
// — it is the swallow gate compared against a server's answers. 3D000 is the row
// that carries the test: it renders the same English phrase and, under the
// substring match this replaced, a TRUNCATE issued against a vanished DATABASE
// was silently accepted as a completed truncate.
func TestIsMissingTableErr_TheTruncateSwallowGateUsesTheSameClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "42P01 undefined_table — the only swallowable shape",
			err:  &pgconn.PgError{Code: "42P01", Message: `relation "app.orders" does not exist`},
			want: true,
		},
		{
			name: "3D000 database gone — must NOT be swallowed as a skipped truncate",
			err:  &pgconn.PgError{Code: "3D000", Message: `database "app" does not exist`},
			want: false,
		},
		{
			name: "42883 missing function",
			err:  &pgconn.PgError{Code: "42883", Message: `function now(unknown) does not exist`},
			want: false,
		},
		{
			name: "42501 permission denied — a refused truncate is not a missing table",
			err:  &pgconn.PgError{Code: "42501", Message: "permission denied for table orders"},
			want: false,
		},
		{
			name: "bare text with the phrase and no SQLSTATE",
			err:  errors.New(`relation "app.orders" does not exist`),
			want: false,
		},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMissingTableErr(tc.err); got != tc.want {
				t.Errorf("isMissingTableErr(%v) = %v; want %v", tc.err, got, tc.want)
			}
		})
	}
}
