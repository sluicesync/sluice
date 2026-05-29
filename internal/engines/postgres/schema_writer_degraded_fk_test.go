// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestAppendNotValid pins the SQL-shape transform that turns a plain
// `ADD CONSTRAINT ... FOREIGN KEY ... ;` into the NOT VALID form by
// dropping the trailing semicolon and appending ` NOT VALID;`. The
// shape matters: PG accepts NOT VALID only between the constraint
// definition and the terminator, and a misplaced cast (e.g. appending
// without removing the `;`) would silently fail with a parser error.
func TestAppendNotValid(t *testing.T) {
	for _, c := range []struct {
		name string
		in   string
		want string
	}{
		{
			name: "basic FK",
			in:   `ALTER TABLE "public"."child" ADD CONSTRAINT "child_parent_fkey" FOREIGN KEY ("parent_id") REFERENCES "public"."parent" ("id");`,
			want: `ALTER TABLE "public"."child" ADD CONSTRAINT "child_parent_fkey" FOREIGN KEY ("parent_id") REFERENCES "public"."parent" ("id") NOT VALID;`,
		},
		{
			name: "FK with ON DELETE",
			in:   `ALTER TABLE "public"."child" ADD CONSTRAINT "fk" FOREIGN KEY ("p") REFERENCES "public"."parent" ("id") ON DELETE CASCADE;`,
			want: `ALTER TABLE "public"."child" ADD CONSTRAINT "fk" FOREIGN KEY ("p") REFERENCES "public"."parent" ("id") ON DELETE CASCADE NOT VALID;`,
		},
		{
			name: "no trailing semicolon (defensive — shouldn't happen but mustn't break)",
			in:   `ALTER TABLE x ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES b (id)`,
			want: `ALTER TABLE x ADD CONSTRAINT c FOREIGN KEY (a) REFERENCES b (id) NOT VALID;`,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := appendNotValid(c.in)
			if got != c.want {
				t.Errorf("appendNotValid:\n  in:   %q\n  got:  %q\n  want: %q", c.in, got, c.want)
			}
		})
	}
}

// TestIsFKViolation pins the SQLSTATE 23503 detection — what the
// CreateConstraints retry-on-degraded-FKs path keys off. Mis-matching
// the code (e.g. only the class "23") would either tolerate
// non-FK violations on the retry path (silent loss class) or miss
// the actual 23503 (degraded-FK opt-in does nothing).
func TestIsFKViolation(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
		{name: "pg 23503 (FK violation, the case we care about)", err: &pgconn.PgError{Code: "23503"}, want: true},
		{name: "pg 23502 (NOT NULL violation — sibling class, must NOT match)", err: &pgconn.PgError{Code: "23502"}, want: false},
		{name: "pg 23505 (unique violation — sibling class, must NOT match)", err: &pgconn.PgError{Code: "23505"}, want: false},
		{name: "pg 42883 (the json-equality case from PR #92 — different class)", err: &pgconn.PgError{Code: "42883"}, want: false},
		{name: "wrapped 23503", err: errors.Join(errors.New("ctx"), &pgconn.PgError{Code: "23503"}), want: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := isFKViolation(c.err); got != c.want {
				t.Errorf("isFKViolation(%v) = %v; want %v", c.err, got, c.want)
			}
		})
	}
}
