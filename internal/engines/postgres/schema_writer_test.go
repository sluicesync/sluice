//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the Postgres SchemaWriter. Build a known IR
// Schema in memory, apply it to a fresh Postgres container, then read
// it back via the SchemaReader and assert the structural shape was
// preserved end-to-end.

package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestSchemaWriter_RoundTrip(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	want := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "email", Type: ir.Varchar{Length: 255}},
				{
					Name:    "active",
					Type:    ir.Boolean{},
					Default: ir.DefaultLiteral{Value: "true"},
				},
				{Name: "role", Type: ir.Enum{Values: []string{"admin", "user", "guest"}}},
				{
					Name:    "created_at",
					Type:    ir.Timestamp{Precision: 6, WithTimeZone: true},
					Default: ir.DefaultExpression{Expr: "now()"},
				},
				{Name: "profile", Type: ir.JSON{Binary: true}, Nullable: true},
				{Name: "tags", Type: ir.Array{Element: ir.Integer{Width: 32}}, Nullable: true},
				{Name: "external_id", Type: ir.UUID{}, Nullable: true},
			},
			PrimaryKey: &ir.Index{
				Name:    "users_pkey",
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
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "user_id", Type: ir.Integer{Width: 64}},
				{Name: "body", Type: ir.Text{Size: ir.TextLong}},
			},
			PrimaryKey: &ir.Index{
				Name:    "posts_pkey",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
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

	// Spot-check column types that exercise the writer's distinct
	// emission paths.
	wantTypes := map[string]ir.Type{
		"id": ir.Integer{Width: 64, AutoIncrement: true},
		// Bug 19c: input Enum had no TypeName, so the writer synthesized
		// the deterministic `users_role_enum`; the reader now carries
		// that name back verbatim on round-trip.
		"role":        ir.Enum{Values: []string{"admin", "user", "guest"}, TypeName: "users_role_enum"},
		"created_at":  ir.Timestamp{Precision: 6, WithTimeZone: true},
		"profile":     ir.JSON{Binary: true},
		"tags":        ir.Array{Element: ir.Integer{Width: 32}},
		"external_id": ir.UUID{},
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

	// PK on id.
	if users.PrimaryKey == nil ||
		len(users.PrimaryKey.Columns) != 1 ||
		users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %#v; want PK on id", users.PrimaryKey)
	}

	// Secondary unique index.
	foundEmailUnique := false
	for _, idx := range users.Indexes {
		if idx.Name == "users_email_unique" && idx.Unique {
			foundEmailUnique = true
		}
	}
	if !foundEmailUnique {
		t.Errorf("users secondary indexes = %#v; want users_email_unique", users.Indexes)
	}

	// FK round-trip.
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts: got %d FKs; want 1", len(posts.ForeignKeys))
	}
	fk := posts.ForeignKeys[0]
	if fk.ReferencedTable != "users" ||
		fk.OnDelete != ir.FKActionCascade ||
		fk.OnUpdate != ir.FKActionRestrict ||
		len(fk.Columns) != 1 || fk.Columns[0] != "user_id" {
		t.Errorf("posts FK = %+v; want users(id) on-delete cascade on-update restrict", fk)
	}
}
