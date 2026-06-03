//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the MySQL SchemaWriter. Build a known IR
// Schema in memory, apply it to a fresh MySQL container, then read it
// back via the SchemaReader and assert the structural shape was
// preserved end-to-end.

package mysql

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestSchemaWriter_RoundTrip(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ---- Build the input IR ----
	want := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
				{Name: "email", Type: ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
				{Name: "active", Type: ir.Boolean{}, Default: ir.DefaultLiteral{Value: "1"}},
				{Name: "role", Type: ir.Enum{Values: []string{"admin", "user", "guest"}}},
				{
					Name: "created_at", Type: ir.Timestamp{Precision: 6, WithTimeZone: true},
					Default: ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(6)"},
				},
				{Name: "profile", Type: ir.JSON{Binary: true}, Nullable: true},
			},
			PrimaryKey: &ir.Index{
				Name:    "PRIMARY",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{
				{
					Name:    "users_email_unique",
					Unique:  true,
					Kind:    ir.IndexKindBTree,
					Columns: []ir.IndexColumn{{Column: "email"}},
				},
			},
		},
		{
			Name: "posts",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
				{Name: "user_id", Type: ir.Integer{Width: 64, Unsigned: true}},
				{Name: "body", Type: ir.Text{Size: ir.TextLong}},
			},
			PrimaryKey: &ir.Index{
				Name:    "PRIMARY",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{
				{
					Name:    "posts_user_id_idx",
					Kind:    ir.IndexKindBTree,
					Columns: []ir.IndexColumn{{Column: "user_id"}},
				},
			},
			ForeignKeys: []*ir.ForeignKey{
				{
					Name:              "posts_user_id_fk",
					Columns:           []string{"user_id"},
					ReferencedTable:   "users",
					ReferencedColumns: []string{"id"},
					OnDelete:          ir.FKActionCascade,
					OnUpdate:          ir.FKActionRestrict,
				},
			},
		},
	}}

	// ---- Apply via SchemaWriter ----
	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(sw)

	if err := sw.CreateTablesWithoutConstraints(ctx, want); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := sw.CreateIndexes(ctx, want); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}
	if err := sw.CreateConstraints(ctx, want); err != nil {
		t.Fatalf("CreateConstraints: %v", err)
	}

	// ---- Read back via SchemaReader ----
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	// ---- Assertions ----
	if len(got.Tables) != 2 {
		t.Fatalf("got %d tables; want 2 (have: %v)", len(got.Tables), tableNames(got))
	}

	users := findTable(got, "users")
	posts := findTable(got, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing expected tables; have %v", tableNames(got))
	}

	// users columns: spot-check key types
	wantUsersCols := map[string]ir.Type{
		"id":     ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true},
		"active": ir.Boolean{},
		"role":   ir.Enum{Values: []string{"admin", "user", "guest"}},
	}
	for name, wantType := range wantUsersCols {
		col := findColumn(users, name)
		if col == nil {
			t.Errorf("users.%s missing", name)
			continue
		}
		if !reflect.DeepEqual(col.Type, wantType) {
			t.Errorf("users.%s.Type = %#v; want %#v", name, col.Type, wantType)
		}
	}

	// users PK
	if users.PrimaryKey == nil ||
		len(users.PrimaryKey.Columns) != 1 ||
		users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %#v; want PK on id", users.PrimaryKey)
	}

	// users secondary unique
	if len(users.Indexes) != 1 ||
		users.Indexes[0].Name != "users_email_unique" ||
		!users.Indexes[0].Unique {
		t.Errorf("users secondary indexes = %#v; want one unique on email", users.Indexes)
	}

	// posts FK round-trip
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

// findColumn searches a table's columns by name. Returns nil when no
// such column exists.
func findColumn(t *ir.Table, name string) *ir.Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}
