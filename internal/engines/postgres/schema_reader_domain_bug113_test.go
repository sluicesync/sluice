//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSchemaReader_DomainRoundTrip_Bug113 pins the v0.95.2 Bug 113
// round-trip closure. v0.95.1 shipped a loud refusal at the read
// boundary; v0.95.2 rotates to actual round-trip carry: the reader
// populates [ir.Domain] with the DOMAIN's name, base type, and CHECK
// definitions so the writer's Phase 1a' can emit `CREATE DOMAIN ... AS
// ... CHECK (...)` before any table that references it.
//
// information_schema.columns unwraps DOMAINs to their base type at
// every column it exposes (data_type, udt_name, char_max_len, etc.).
// The reader relies on that for the base IR type — translateType
// produces ir.Text{} for free — and reads pg_type.typtype + typname
// + pg_constraint(contypid) separately to wrap in ir.Domain.
func TestSchemaReader_DomainRoundTrip_Bug113(t *testing.T) {
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

	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: want clean read with ir.Domain wrap; got %v", err)
	}

	tab := findTable(schema, "gl_users")
	if tab == nil {
		t.Fatalf("missing gl_users table; have %v", tableNames(schema))
	}

	var email *ir.Column
	for _, c := range tab.Columns {
		if c.Name == "email" {
			email = c
			break
		}
	}
	if email == nil {
		t.Fatalf("missing email column on gl_users")
	}
	dom, ok := email.Type.(ir.Domain)
	if !ok {
		t.Fatalf("email column type = %T (%v); want ir.Domain", email.Type, email.Type)
	}
	if dom.Name != "email_address" {
		t.Errorf("Domain.Name = %q; want %q", dom.Name, "email_address")
	}
	if _, ok := dom.BaseType.(ir.Text); !ok {
		t.Errorf("Domain.BaseType = %T (%v); want ir.Text (information_schema unwraps to base type)", dom.BaseType, dom.BaseType)
	}
	if len(dom.Checks) != 1 {
		t.Fatalf("Domain.Checks len = %d; want 1", len(dom.Checks))
	}
	// pg_get_constraintdef strips the outer `CHECK (...)`; the IR's
	// DomainCheck.Body holds the bare expression. The constraint name
	// PG auto-generated will be `email_address_check` for an unnamed
	// CHECK on a DOMAIN; just sanity-check it's non-empty.
	if dom.Checks[0].Name == "" {
		t.Errorf("Domain.Checks[0].Name is empty; PG always auto-names a DOMAIN's CHECK")
	}
	if dom.Checks[0].Body == "" {
		t.Errorf("Domain.Checks[0].Body is empty; want the regex expression")
	}
}

// TestSchemaReader_DomainRoundTrip_NonDomainUserDefinedStillRoundTrips
// pins the negative control: a column referencing an ENUM type
// (pg_type.typtype == 'e', also USER-DEFINED in information_schema)
// continues to round-trip as ir.Enum, not ir.Domain. Without this
// pin a future refactor could over-broaden the DOMAIN wrap to every
// user-defined type and regress the v0.16.x ENUM handling.
func TestSchemaReader_DomainRoundTrip_NonDomainUserDefinedStillRoundTrips(t *testing.T) {
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
		t.Fatalf("ReadSchema: %v", err)
	}
	tab := findTable(schema, "gl_users")
	if tab == nil {
		t.Fatalf("missing gl_users table; have %v", tableNames(schema))
	}
	var role *ir.Column
	for _, c := range tab.Columns {
		if c.Name == "role" {
			role = c
			break
		}
	}
	if role == nil {
		t.Fatalf("missing role column")
	}
	if _, ok := role.Type.(ir.Enum); !ok {
		t.Errorf("role column type = %T (%v); want ir.Enum (negative control: DOMAIN wrap must not over-broaden)", role.Type, role.Type)
	}
	if _, isDomain := role.Type.(ir.Domain); isDomain {
		t.Errorf("role column was wrongly wrapped as ir.Domain (typtype mismatch)")
	}
}
