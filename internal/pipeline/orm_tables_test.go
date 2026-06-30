// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ormCol / ormTbl are terse builders for the recognition fixtures.
func ormCol(name string, t ir.Type) *ir.Column { return &ir.Column{Name: name, Type: t} }

func ormTbl(name string, cols ...*ir.Column) *ir.Table {
	return &ir.Table{Name: name, Columns: cols}
}

var (
	anInt  = ir.Integer{Width: 32}
	aText  = ir.Text{}
	aVarch = ir.Varchar{Length: 255}
	aChar  = ir.Char{Length: 32}
)

// TestRecognizeORMTableDistinctive pins the DISTINCTIVE-name class: every
// framework whose table name is a low-false-positive literal (or, for
// Drizzle, a prefix) is recognized on the name alone, case-insensitively.
// This is the "pin the class, not the representative" matrix for the
// name-only rules — one entry per framework, not one representative.
func TestRecognizeORMTableDistinctive(t *testing.T) {
	cases := []struct {
		name    string
		table   string
		wantORM string
	}{
		{"drizzle exact", "__drizzle_migrations", "Drizzle"},
		{"drizzle journal variant (prefix)", "__drizzle_migrations_journal", "Drizzle"},
		{"prisma", "_prisma_migrations", "Prisma"},
		{"knex", "knex_migrations", "Knex"},
		{"knex lock", "knex_migrations_lock", "Knex"},
		{"sequelize", "SequelizeMeta", "Sequelize"},
		{"rails ar metadata", "ar_internal_metadata", "Rails ActiveRecord"},
		{"flyway", "flyway_schema_history", "Flyway"},
		{"liquibase log", "databasechangelog", "Liquibase"},
		{"liquibase lock", "DATABASECHANGELOGLOCK", "Liquibase"},
		{"django", "django_migrations", "Django"},
		{"alembic", "alembic_version", "Alembic"},
		{"typeorm", "typeorm_metadata", "TypeORM"},
		{"goose", "goose_db_version", "Goose"},
		{"ef core", "__EFMigrationsHistory", "EF Core"},
		{"doctrine", "doctrine_migration_versions", "Doctrine/Symfony"},
		{"phinx", "phinxlog", "Phinx/CakePHP"},
		{"sqlx", "_sqlx_migrations", "sqlx"},
		{"diesel", "__diesel_schema_migrations", "Diesel"},
		{"seaorm", "seaql_migrations", "SeaORM"},
		{"mikroorm", "mikro_orm_migrations", "MikroORM"},
		{"node-pg-migrate", "pgmigrations", "node-pg-migrate"},
		{"atlas", "atlas_schema_revisions", "Atlas"},
		{"aerich", "aerich", "Aerich"},
		{"fluent", "_fluent_migrations", "Fluent"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Distinctive names match regardless of columns (give them a
			// couple of arbitrary columns to prove shape is irrelevant).
			tab := ormTbl(c.table, ormCol("id", anInt), ormCol("payload", aText))
			rule, ok := recognizeORMTable(tab)
			if !ok {
				t.Fatalf("recognizeORMTable(%q) = not recognized; want recognized as %s", c.table, c.wantORM)
			}
			if rule.orm != c.wantORM {
				t.Errorf("orm = %q; want %q", rule.orm, c.wantORM)
			}
			if rule.remediation == "" {
				t.Errorf("remediation is empty; want a non-empty notice reason")
			}
			// Distinctive names are never name-collisions.
			if _, collided := ormNameCollision(tab); collided {
				t.Errorf("ormNameCollision(%q) = true; distinctive names must not collide", c.table)
			}
		})
	}
}

// TestRecognizeORMTableGenericShape pins the GENERIC-name class: each
// generic name is recognized ONLY when both the name AND the column shape
// match; with a non-matching shape it is NOT recognized and the
// name-collision flag is set (so the caller keeps it as application data).
func TestRecognizeORMTableGenericShape(t *testing.T) {
	cases := []struct {
		name          string
		table         *ir.Table
		wantRecognize bool
		wantORM       string // expected orm on recognize OR on collision
		wantCollision bool
	}{
		// schema_migrations — exactly one `version` text column.
		{
			"schema_migrations rails shape (varchar)",
			ormTbl("schema_migrations", ormCol("version", aVarch)),
			true, "Rails/Ecto/golang-migrate/dbmate", false,
		},
		{
			"schema_migrations rails shape (text)",
			ormTbl("schema_migrations", ormCol("version", aText)),
			true, "Rails/Ecto/golang-migrate/dbmate", false,
		},
		{
			"schema_migrations VERSION case-insensitive",
			ormTbl("Schema_Migrations", ormCol("VERSION", aChar)),
			true, "Rails/Ecto/golang-migrate/dbmate", false,
		},
		{
			"schema_migrations wrong shape (id,data) — collision",
			ormTbl("schema_migrations", ormCol("id", anInt), ormCol("data", aText)),
			false, "Rails/Ecto/golang-migrate/dbmate", true,
		},
		{
			"schema_migrations version-but-int — collision",
			ormTbl("schema_migrations", ormCol("version", anInt)),
			false, "Rails/Ecto/golang-migrate/dbmate", true,
		},
		// migrations — Laravel {id int, migration text, batch int}.
		{
			"migrations laravel shape",
			ormTbl("migrations", ormCol("id", anInt), ormCol("migration", aVarch), ormCol("batch", anInt)),
			true, "Laravel/gormigrate", false,
		},
		{
			"migrations laravel without surrogate id",
			ormTbl("migrations", ormCol("migration", aVarch), ormCol("batch", anInt)),
			true, "Laravel/gormigrate", false,
		},
		{
			"migrations laravel order-independent + case",
			ormTbl("migrations", ormCol("Batch", anInt), ormCol("Migration", aText), ormCol("ID", anInt)),
			true, "Laravel/gormigrate", false,
		},
		{
			"migrations real app table (id,user_id,body) — collision",
			ormTbl("migrations", ormCol("id", anInt), ormCol("user_id", anInt), ormCol("body", aText)),
			false, "Laravel/gormigrate", true,
		},
		{
			"migrations batch-wrong-family — collision",
			ormTbl("migrations", ormCol("id", anInt), ormCol("migration", aVarch), ormCol("batch", aText)),
			false, "Laravel/gormigrate", true,
		},
		// migration — Yii {version text, apply_time int}.
		{
			"migration yii shape",
			ormTbl("migration", ormCol("version", aVarch), ormCol("apply_time", anInt)),
			true, "Yii", false,
		},
		{
			"migration yii extra column — collision",
			ormTbl("migration", ormCol("version", aVarch), ormCol("apply_time", anInt), ormCol("note", aText)),
			false, "Yii", true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			rule, ok := recognizeORMTable(c.table)
			if ok != c.wantRecognize {
				t.Fatalf("recognizeORMTable recognized = %v; want %v", ok, c.wantRecognize)
			}
			if ok && rule.orm != c.wantORM {
				t.Errorf("orm = %q; want %q", rule.orm, c.wantORM)
			}
			orm, collided := ormNameCollision(c.table)
			if collided != c.wantCollision {
				t.Fatalf("ormNameCollision collided = %v; want %v", collided, c.wantCollision)
			}
			if collided && orm != c.wantORM {
				t.Errorf("collision orm = %q; want %q", orm, c.wantORM)
			}
		})
	}
}

// TestRecognizeORMTableNonMatch confirms ordinary application tables are
// neither recognized nor flagged as collisions.
func TestRecognizeORMTableNonMatch(t *testing.T) {
	for _, name := range []string{"users", "orders", "audit_log", "schema_migration_log", "user_migrations"} {
		tab := ormTbl(name, ormCol("id", anInt))
		if _, ok := recognizeORMTable(tab); ok {
			t.Errorf("recognizeORMTable(%q) = recognized; want not", name)
		}
		if _, collided := ormNameCollision(tab); collided {
			t.Errorf("ormNameCollision(%q) = true; want false", name)
		}
	}
}

// TestApplyORMTableSkipPrunes drives the prune mechanism: with skip on, the
// recognized ORM tables are removed and the application tables remain, and a
// loud per-table notice naming the ORM + table is emitted.
func TestApplyORMTableSkipPrunes(t *testing.T) {
	logs := captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{
		ormTbl("users", ormCol("id", anInt)),
		ormTbl("flyway_schema_history", ormCol("installed_rank", anInt)),
		ormTbl("orders", ormCol("id", anInt)),
		ormTbl("_prisma_migrations", ormCol("id", aVarch)),
	}}
	applyORMTableSkip(context.Background(), schema, true, TableFilter{})

	got := tableNames(schema)
	want := []string{"users", "orders"}
	if !equalStrings(got, want) {
		t.Fatalf("kept tables = %v; want %v", got, want)
	}
	out := logs.String()
	for _, frag := range []string{"skipping ORM migration-history table", "flyway_schema_history", "Flyway", "_prisma_migrations", "Prisma"} {
		if !strings.Contains(out, frag) {
			t.Errorf("log missing %q; got %q", frag, out)
		}
	}
}

// TestApplyORMTableSkipDisabledIsIdentity is the zero-value-safe contract:
// skip=false (the default every programmatic caller gets) leaves the schema
// byte-identical.
func TestApplyORMTableSkipDisabledIsIdentity(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		ormTbl("users", ormCol("id", anInt)),
		ormTbl("flyway_schema_history", ormCol("installed_rank", anInt)),
	}}
	applyORMTableSkip(context.Background(), schema, false, TableFilter{})
	if got, want := tableNames(schema), []string{"users", "flyway_schema_history"}; !equalStrings(got, want) {
		t.Fatalf("skip=false changed schema: got %v; want %v", got, want)
	}
}

// TestApplyORMTableSkipExplicitIncludeWins confirms a table the operator
// named exactly via --include-table is kept even with skip on.
func TestApplyORMTableSkipExplicitIncludeWins(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		ormTbl("users", ormCol("id", anInt)),
		ormTbl("flyway_schema_history", ormCol("installed_rank", anInt)),
	}}
	// Include-mode filter naming the ORM table explicitly (case-insensitive).
	filter := TableFilter{Include: []string{"users", "FLYWAY_SCHEMA_HISTORY"}}
	applyORMTableSkip(context.Background(), schema, true, filter)
	if got, want := tableNames(schema), []string{"users", "flyway_schema_history"}; !equalStrings(got, want) {
		t.Fatalf("explicit --include-table not honored: got %v; want %v", got, want)
	}
}

// TestApplyORMTableSkipCollisionKept confirms a generic-name collision (real
// app table sharing the bookkeeping name) is KEPT and the prune emits a loud
// collision warning rather than silently dropping data.
func TestApplyORMTableSkipCollisionKept(t *testing.T) {
	logs := captureSlog(t)
	appMigrations := ormTbl("migrations", ormCol("id", anInt), ormCol("user_id", anInt), ormCol("body", aText))
	schema := &ir.Schema{Tables: []*ir.Table{
		ormTbl("users", ormCol("id", anInt)),
		appMigrations,
	}}
	applyORMTableSkip(context.Background(), schema, true, TableFilter{})
	if got, want := tableNames(schema), []string{"users", "migrations"}; !equalStrings(got, want) {
		t.Fatalf("collision table dropped: got %v; want %v", got, want)
	}
	out := logs.String()
	if !strings.Contains(out, "matches the") || !strings.Contains(out, "Laravel/gormigrate") {
		t.Errorf("expected collision warning naming the ORM; got %q", out)
	}
	if strings.Contains(out, "skipping ORM migration-history table") {
		t.Errorf("collision must NOT emit a skip notice; got %q", out)
	}
}

func tableNames(s *ir.Schema) []string {
	out := make([]string, len(s.Tables))
	for i, tb := range s.Tables {
		out[i] = tb.Name
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
