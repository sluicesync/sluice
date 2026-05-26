//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the Postgres SchemaReader. These boot a real
// PostgreSQL container via testcontainers-go, apply a fixture, and
// assert the IR shape the reader produces.
//
// Skipped on hosts without a usable Docker provider.

package postgres

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// startPostgres returns a DSN pointed at a freshly-reset database on
// the shared PG container booted by TestMain (see
// shared_container_integration_test.go). The cleanup is a no-op;
// container teardown is owned by TestMain.
func startPostgres(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	return newSharedPGDB(t, "sluice_test")
}

// applyDDL runs a possibly-multi-statement DDL block against the
// Postgres instance at dsn. Postgres's libpq accepts multi-statement
// queries natively when sent via Exec — no special flag needed.
func applyDDL(t *testing.T, dsn, ddl string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
}

// TestSchemaReader_TypeMatrix exercises the Postgres-specific corners
// of the IR: native UUID/INET/CIDR/MACADDR types, JSON vs JSONB,
// custom enum types, and single-dimension int arrays. Assertions
// focus on the column-level IR types — these are the load-bearing
// outputs the orchestrator and the writer will rely on.
func TestSchemaReader_TypeMatrix(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

		CREATE TABLE users (
			id           BIGSERIAL    PRIMARY KEY,
			email        VARCHAR(255) NOT NULL,
			active       BOOLEAN      NOT NULL DEFAULT TRUE,
			role         user_role    NOT NULL DEFAULT 'user',
			score        NUMERIC(8,2) NOT NULL DEFAULT 0,
			tags         INTEGER[]    NULL,
			profile      JSONB        NULL,
			external_id  UUID         NULL,
			ip_address   INET         NULL,
			network      CIDR         NULL,
			mac          MACADDR      NULL,
			created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
			birthday     DATE         NULL,
			start_time   TIME         NULL
		);

		CREATE UNIQUE INDEX users_email_unique ON users (email);

		CREATE TABLE posts (
			id       BIGSERIAL PRIMARY KEY,
			user_id  BIGINT    NOT NULL,
			body     TEXT      NOT NULL,
			CONSTRAINT posts_user_id_fk FOREIGN KEY (user_id)
				REFERENCES users(id) ON DELETE CASCADE ON UPDATE RESTRICT
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

	if len(schema.Tables) != 2 {
		t.Fatalf("got %d tables; want 2 (have: %v)", len(schema.Tables), tableNames(schema))
	}
	users := findTable(schema, "users")
	posts := findTable(schema, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing expected tables; have %v", tableNames(schema))
	}

	// ---- users column types ----
	wantTypes := map[string]ir.Type{
		"id":     ir.Integer{Width: 64, AutoIncrement: true},
		"email":  ir.Varchar{Length: 255},
		"active": ir.Boolean{},
		// Bug 19c: the source enum type name is now carried verbatim.
		"role":        ir.Enum{Values: []string{"admin", "user", "guest"}, TypeName: "user_role"},
		"score":       ir.Decimal{Precision: 8, Scale: 2},
		"tags":        ir.Array{Element: ir.Integer{Width: 32}},
		"profile":     ir.JSON{Binary: true},
		"external_id": ir.UUID{},
		"ip_address":  ir.Inet{},
		"network":     ir.Cidr{},
		"mac":         ir.Macaddr{},
		"created_at":  ir.Timestamp{Precision: 6, WithTimeZone: true},
		"birthday":    ir.Date{},
		"start_time":  ir.Time{Precision: 6},
	}
	for name, wantType := range wantTypes {
		col := findColumn(users, name)
		if col == nil {
			t.Errorf("users.%s missing", name)
			continue
		}
		if !reflect.DeepEqual(col.Type, wantType) {
			t.Errorf("users.%s.Type = %#v; want %#v", name, col.Type, wantType)
		}
	}

	// ---- users primary key ----
	if users.PrimaryKey == nil ||
		len(users.PrimaryKey.Columns) != 1 ||
		users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %#v; want PK on id", users.PrimaryKey)
	}

	// ---- users secondary unique index ----
	foundEmailUnique := false
	for _, idx := range users.Indexes {
		if idx.Name == "users_email_unique" && idx.Unique {
			foundEmailUnique = true
			break
		}
	}
	if !foundEmailUnique {
		t.Errorf("users secondary indexes = %#v; want users_email_unique", users.Indexes)
	}

	// ---- posts foreign key ----
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts: got %d FKs; want 1", len(posts.ForeignKeys))
	}
	fk := posts.ForeignKeys[0]
	if fk.ReferencedTable != "users" ||
		fk.OnDelete != ir.FKActionCascade ||
		fk.OnUpdate != ir.FKActionRestrict ||
		len(fk.Columns) != 1 || fk.Columns[0] != "user_id" ||
		len(fk.ReferencedColumns) != 1 || fk.ReferencedColumns[0] != "id" {
		t.Errorf("posts FK = %+v; want users(id) on-delete cascade on-update restrict", fk)
	}
}

// findTable searches by name (no schema qualifier — single-schema
// reader for now).
func findTable(s *ir.Schema, name string) *ir.Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func findColumn(t *ir.Table, name string) *ir.Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func tableNames(s *ir.Schema) []string {
	out := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		out = append(out, t.Name)
	}
	return out
}

func closeIf(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
