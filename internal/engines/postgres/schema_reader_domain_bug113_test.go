//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestSchemaReader_DomainRefusal_Bug113 pins the v0.95.1 Bug 113
// closure. Pre-fix, a column referencing a PG DOMAIN
// (`CREATE DOMAIN email_address AS text CHECK (VALUE ~ '...')`)
// surfaced through `information_schema.columns` as a plain `text`
// column — information_schema silently unwraps DOMAINs — so the
// PG schema reader translated the column to `ir.Text{}` and the
// DOMAIN's CHECK constraints disappeared on PG→PG migrate
// (CRITICAL silent-constraint-loss class). v0.95.1 reads
// `pg_type.typtype` alongside the column dispatch; when typtype
// == 'd' the reader refuses loudly at the read boundary so no
// partial schema lands on the target, and the operator gets a
// clear actionable error naming the table + column + domain name
// + the recovery `ALTER TABLE` shape. Round-trip DOMAIN carry
// is queued for v0.95.2; "Either is acceptable; silent-drop is
// not" per the BUG-CATALOG suggested-fix.
//
// The pin exercises the exact `gl_users` shape from the
// BUG-CATALOG repro (email_address DOMAIN with a regex CHECK on
// a text base type, referenced from a table column).
func TestSchemaReader_DomainRefusal_Bug113(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		DROP TABLE IF EXISTS gl_users CASCADE;
		DROP DOMAIN IF EXISTS email_address;
		CREATE DOMAIN email_address AS text
		  CHECK (VALUE ~ '^[^@]+@[^@]+\.[^@]+$');
		CREATE TABLE gl_users (
		  id       bigserial PRIMARY KEY,
		  username varchar(255) NOT NULL,
		  email    email_address NOT NULL
		);
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)

	_, err = r.ReadSchema(ctx)
	if err == nil {
		t.Fatalf("ReadSchema: want loud refusal naming the DOMAIN; got nil (Bug 113 silent-drop signature)")
	}

	// The refusal must name the table, column, and domain name so
	// the operator can locate the offending DDL without grep'ing
	// the source schema by hand.
	wantSubstrings := []string{
		`table "gl_users"`,
		`column "email"`,
		`DOMAIN "email_address"`,
		"Bug 113",
	}
	for _, s := range wantSubstrings {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error message missing required substring %q\nfull error: %s", s, err.Error())
		}
	}
}

// TestSchemaReader_DomainRefusal_NonDomainUserDefinedStillRoundTrips
// is the negative control: a column referencing an ENUM type
// (also USER-DEFINED in information_schema, but pg_type.typtype ==
// 'e', not 'd') must continue to round-trip cleanly. Without this
// pin a future refactor could over-broaden the Bug 113 refusal to
// every user-defined type and regress ENUM handling, which has
// been correct since v0.16.x.
func TestSchemaReader_DomainRefusal_NonDomainUserDefinedStillRoundTrips(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		DROP TABLE IF EXISTS gl_users CASCADE;
		DROP TYPE IF EXISTS user_role;
		CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');
		CREATE TABLE gl_users (
		  id   bigserial PRIMARY KEY,
		  role user_role NOT NULL DEFAULT 'user'
		);
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)

	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: want clean read for ENUM-typed column; got refusal: %v", err)
	}
	tab := findTable(schema, "gl_users")
	if tab == nil {
		t.Fatalf("missing gl_users table; have %v", tableNames(schema))
	}
}
