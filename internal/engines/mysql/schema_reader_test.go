//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the MySQL SchemaReader. These tests boot a real
// MySQL container via testcontainers-go and assert the IR shape the
// reader produces.
//
// To run locally:
//   make test-it
// or:
//   go test -tags=integration ./internal/engines/mysql/...
//
// Requires Docker (Docker Desktop or equivalent) to be running.

package mysql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// startMySQL returns a DSN pointed at a freshly-reset `sluice_test`
// database on the shard's shared mysqld container (see
// shared_container_integration_test.go). The (dsn, cleanup) shape
// is preserved so tests' `defer cleanup()` continues to compile;
// container teardown is owned by TestMain, so cleanup is a no-op.
func startMySQL(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	return newSharedDB(t, "sluice_test")
}

func applyDDL(t *testing.T, dsn, ddl string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
}

// TestSchemaReader_BasicShape exercises a fixture covering the common
// type cases and asserts the IR Schema we get back has the expected
// table, columns, primary key, secondary index, and foreign key.
func TestSchemaReader_BasicShape(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE users (
			id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			email         VARCHAR(255)    NOT NULL,
			active        TINYINT(1)      NOT NULL DEFAULT 1,
			role          ENUM('admin','user','guest') NOT NULL DEFAULT 'user',
			created_at    TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			profile       JSON            NULL,
			PRIMARY KEY (id),
			UNIQUE KEY users_email_unique (email)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE TABLE posts (
			id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			user_id   BIGINT UNSIGNED NOT NULL,
			body      LONGTEXT        NOT NULL,
			PRIMARY KEY (id),
			KEY posts_user_id_idx (user_id),
			CONSTRAINT posts_user_id_fk FOREIGN KEY (user_id)
				REFERENCES users (id) ON DELETE CASCADE ON UPDATE RESTRICT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`

	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := r.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("got %d tables; want 2", len(schema.Tables))
	}

	users := findTable(schema, "users")
	posts := findTable(schema, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing expected tables; have %#v", tableNames(schema))
	}

	// ----- users -----
	wantUsersCols := []struct {
		name string
		typ  ir.Type
	}{
		{"id", ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
		{"email", ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
		{"active", ir.Boolean{}},
		{"role", ir.Enum{Values: []string{"admin", "user", "guest"}}},
		{"created_at", ir.Timestamp{Precision: 6, WithTimeZone: true}},
		{"profile", ir.JSON{Binary: true}},
	}
	if len(users.Columns) != len(wantUsersCols) {
		t.Fatalf("users: got %d columns; want %d", len(users.Columns), len(wantUsersCols))
	}
	for i, w := range wantUsersCols {
		got := users.Columns[i]
		if got.Name != w.name {
			t.Errorf("users.columns[%d].Name = %q; want %q", i, got.Name, w.name)
		}
		if !reflect.DeepEqual(got.Type, w.typ) {
			t.Errorf("users.columns[%d=%s].Type = %#v; want %#v", i, w.name, got.Type, w.typ)
		}
	}

	if users.PrimaryKey == nil || len(users.PrimaryKey.Columns) != 1 || users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users primary key = %#v; want PK on id", users.PrimaryKey)
	}
	if len(users.Indexes) != 1 || users.Indexes[0].Name != "users_email_unique" || !users.Indexes[0].Unique {
		t.Errorf("users secondary indexes = %#v; want one unique on email", users.Indexes)
	}

	// ----- posts -----
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts: got %d FKs; want 1", len(posts.ForeignKeys))
	}
	fk := posts.ForeignKeys[0]
	if fk.ReferencedTable != "users" || fk.OnDelete != ir.FKActionCascade || fk.OnUpdate != ir.FKActionRestrict {
		t.Errorf("posts FK = %+v; want users on-delete cascade on-update restrict", fk)
	}
	if len(fk.Columns) != 1 || fk.Columns[0] != "user_id" {
		t.Errorf("posts FK columns = %v; want [user_id]", fk.Columns)
	}
	if len(fk.ReferencedColumns) != 1 || fk.ReferencedColumns[0] != "id" {
		t.Errorf("posts FK referenced columns = %v; want [id]", fk.ReferencedColumns)
	}
}

// TestSchemaReader_FunctionalIndex covers Bug 16: MySQL 8.0.13+
// functional indexes store COLUMN_NAME = NULL in
// information_schema.statistics and put the expression text in the
// EXPRESSION column. Pre-fix, the reader's bare-string scan blew up
// with "converting NULL to string is unsupported", which was a hard
// wall — sluice could not read any schema containing such an index.
//
// The IR shape is an expression-entry IndexColumn (Expression non-
// empty, Column empty); the writer renders it in parens to produce
// the canonical MySQL double-parens form `((LOWER(email)))`.
func TestSchemaReader_FunctionalIndex(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE users (
			id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			email VARCHAR(255)    NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE INDEX idx_lower_email ON users ((LOWER(email)));
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := r.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	users := findTable(schema, "users")
	if users == nil {
		t.Fatalf("missing users table; have %v", tableNames(schema))
	}

	var fnIdx *ir.Index
	for _, ix := range users.Indexes {
		if ix.Name == "idx_lower_email" {
			fnIdx = ix
			break
		}
	}
	if fnIdx == nil {
		t.Fatalf("missing idx_lower_email index; have %d indexes", len(users.Indexes))
	}
	if len(fnIdx.Columns) != 1 {
		t.Fatalf("idx_lower_email: got %d entries; want 1", len(fnIdx.Columns))
	}
	entry := fnIdx.Columns[0]
	if entry.Column != "" {
		t.Errorf("expression entry should have empty Column; got %q", entry.Column)
	}
	if entry.Expression == "" {
		t.Fatalf("expression entry should carry expression text; got empty")
	}
	// MySQL stores the expression with backtick-quoted identifiers and
	// possibly a charset introducer; the reader normalizes those out.
	// The exact form is server-dependent (e.g. lower(email) vs
	// `lower`(email) on some flavors), so assert the substantive
	// shape rather than an exact string.
	wantSubstrs := []string{"lower", "email"}
	for _, w := range wantSubstrs {
		if !strings.Contains(strings.ToLower(entry.Expression), w) {
			t.Errorf("expression %q missing substring %q", entry.Expression, w)
		}
	}
	// Backticks must be stripped at the read boundary.
	if strings.Contains(entry.Expression, "`") {
		t.Errorf("expression %q still contains backticks; should be normalized", entry.Expression)
	}
}

func findTable(s *ir.Schema, name string) *ir.Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
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
