//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
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

// TestSchemaReader_NotValidConstraints_UPR1 pins the READER side of the
// NOT VALID carry — the half that touches captured text and, until this test,
// had no coverage at all.
//
// The emitter got a unit pin when the fix landed; the reader did not, and a
// pre-tag review found that reverting the `CutSuffix` block left the entire
// suite green. That is the shape of gap that lets a parse fix regress
// silently, so it is pinned against a real catalog rather than a fixture.
//
// The domain cell is the headline: `pg_get_constraintdef(oid, true)` renders
// an unvalidated constraint as `CHECK (VALUE > 0) NOT VALID`, and the reader
// used to strip `CHECK (` then TrimSuffix(")") — which matched nothing,
// because the string ends in "D". The captured Body kept an unbalanced paren
// and the emitted DDL was a syntax error, so a legal source could not be
// migrated at all.
//
// Cells 3 and 4 are the anti-vacuity half and they are not decoration: a cut
// that over-matched (a regex on `NOT VALID` anywhere, say) would pass cells 1
// and 2 and fail here, because the expression itself contains that text.
func TestSchemaReader_NotValidConstraints_UPR1(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		DROP TABLE IF EXISTS nv_child CASCADE;
		DROP TABLE IF EXISTS nv_parent CASCADE;
		DROP DOMAIN IF EXISTS nv_pos;
		DROP DOMAIN IF EXISTS nv_ok;
		DROP DOMAIN IF EXISTS nv_literal;

		CREATE DOMAIN nv_pos AS int;
		ALTER DOMAIN nv_pos ADD CONSTRAINT nv_pos_chk CHECK (VALUE > 0) NOT VALID;

		CREATE DOMAIN nv_ok AS int CHECK (VALUE > 0);

		-- The expression itself ends in the literal text a naive cut would eat.
		CREATE DOMAIN nv_literal AS text
		  CHECK (VALUE <> 'x NOT VALID');

		CREATE TABLE nv_parent (id int PRIMARY KEY);
		CREATE TABLE nv_child (
		  id  int PRIMARY KEY,
		  qty int
		);
		ALTER TABLE nv_child ADD CONSTRAINT nv_fk
		  FOREIGN KEY (id) REFERENCES nv_parent(id) NOT VALID;
		ALTER TABLE nv_child ADD CONSTRAINT nv_chk CHECK (qty >= 0) NOT VALID;
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	domainCheck := func(domainName string) (ir.DomainCheck, bool) {
		for _, tab := range schema.Tables {
			for _, col := range tab.Columns {
				d, ok := col.Type.(ir.Domain)
				if !ok || d.Name != domainName {
					continue
				}
				if len(d.Checks) > 0 {
					return d.Checks[0], true
				}
			}
		}
		return ir.DomainCheck{}, false
	}

	t.Run("an unvalidated DOMAIN check keeps a balanced body and carries NotValid", func(t *testing.T) {
		// Reached via a table column so the domain is in the read set.
		applyDDL(t, dsn, `ALTER TABLE nv_child ADD COLUMN p nv_pos;`)
		s2, err := r.ReadSchema(ctx)
		if err != nil {
			t.Fatalf("ReadSchema: %v", err)
		}
		schema = s2
		c, ok := domainCheck("nv_pos")
		if !ok {
			t.Fatal("nv_pos domain not found in the read schema")
		}
		if !c.NotValid {
			t.Error("NotValid=false for a constraint the source declared NOT VALID — the state was dropped")
		}
		if strings.Contains(c.Body, "NOT VALID") {
			t.Errorf("Body still carries the qualifier: %q.\n\nThis is the exact pre-fix defect: the "+
				"emitted DDL becomes a syntax error and the source cannot be migrated at all.", c.Body)
		}
		if strings.Count(c.Body, "(") != strings.Count(c.Body, ")") {
			t.Errorf("Body has unbalanced parentheses: %q", c.Body)
		}
	})

	t.Run("a VALIDATED domain check is unchanged", func(t *testing.T) {
		applyDDL(t, dsn, `ALTER TABLE nv_child ADD COLUMN o nv_ok;`)
		s2, err := r.ReadSchema(ctx)
		if err != nil {
			t.Fatalf("ReadSchema: %v", err)
		}
		schema = s2
		c, ok := domainCheck("nv_ok")
		if !ok {
			t.Fatal("nv_ok domain not found")
		}
		if c.NotValid {
			t.Error("NotValid=true for a VALIDATED constraint — inverted sign")
		}
		if strings.Contains(c.Body, "NOT VALID") {
			t.Errorf("Body polluted: %q", c.Body)
		}
	})

	t.Run("an expression ending in the literal 'NOT VALID' survives intact", func(t *testing.T) {
		applyDDL(t, dsn, `ALTER TABLE nv_child ADD COLUMN l nv_literal;`)
		s2, err := r.ReadSchema(ctx)
		if err != nil {
			t.Fatalf("ReadSchema: %v", err)
		}
		schema = s2
		c, ok := domainCheck("nv_literal")
		if !ok {
			t.Fatal("nv_literal domain not found")
		}
		if c.NotValid {
			t.Error("NotValid=true for a VALIDATED constraint whose EXPRESSION merely contains the text " +
				"'NOT VALID' — the cut over-matched, which is the failure mode this cell exists for")
		}
		if !strings.Contains(c.Body, "NOT VALID") {
			t.Errorf("the literal was eaten out of the expression body: %q — the constraint now means "+
				"something different from what the source enforces", c.Body)
		}
	})

	t.Run("table FK and CHECK carry NotValid", func(t *testing.T) {
		tab := findTable(schema, "nv_child")
		if tab == nil {
			t.Fatalf("nv_child missing; have %v", tableNames(schema))
		}
		var sawFK bool
		for _, fk := range tab.ForeignKeys {
			if fk.Name == "nv_fk" {
				sawFK = true
				if !fk.NotValid {
					t.Error("ForeignKey.NotValid=false for a NOT VALID source FK; convalidated was dropped " +
						"or its sign inverted")
				}
			}
		}
		if !sawFK {
			t.Error("nv_fk not read at all")
		}
		var sawChk bool
		for _, c := range tab.CheckConstraints {
			if c.Name == "nv_chk" {
				sawChk = true
				if !c.NotValid {
					t.Error("CheckConstraint.NotValid=false for a NOT VALID source CHECK — pg_get_expr " +
						"cannot carry the qualifier, so convalidated must be selected explicitly")
				}
			}
		}
		if !sawChk {
			t.Error("nv_chk not read at all")
		}
	})
}
