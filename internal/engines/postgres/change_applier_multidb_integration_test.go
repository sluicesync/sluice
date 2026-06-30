//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the ADR-0074 Phase 1b multi-database apply
// routing on the Postgres ChangeApplier (part B). On PG the
// multi-database fan-out lands each source database in a same-named
// target SCHEMA. One applier, per-change namespace routing keyed on
// change.Schema:
//
//   - routing ENABLED: a change whose Schema differs from the applier's
//     bound schema lands in `Schema.table`; a change whose Schema is
//     empty or equals the bound schema lands in the bound schema
//     (byte-identical single-database behaviour);
//   - routing DISABLED (default): change.Schema is ignored for
//     qualification — the cross-engine single-database back-compat guard.
//
// The target schemas are pre-created here (the cold-start owns namespace
// creation in Phase 1b.2; the applier assumes they exist).

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// countRowsInSchema counts rows of schema.table on the applier's target
// database so the test can verify rows landed in the CORRECT schema.
func countRowsInSchema(t *testing.T, dsn, schema, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int
	q := `SELECT COUNT(*) FROM "` + schema + `"."` + table + `"`
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count %s.%s: %v", schema, table, err)
	}
	return n
}

// TestChangeApplier_MultiDatabaseRouting pins the part-B routing class on
// Postgres: with routing enabled, changes fan out to same-named target
// schemas by change.Schema; a bound-schema or empty-Schema change lands
// unqualified in the bound schema.
func TestChangeApplier_MultiDatabaseRouting(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	const (
		boundSchema = "public" // the applier's DSN-derived bound schema
		schemaA     = "app_a"
		schemaB     = "app_b"
	)

	// Pre-create the two target schemas + the table in all three
	// namespaces (the applier does NOT create them — cold-start owns
	// namespace creation in Phase 1b.2).
	applyPGApplier(t, dsn, `
		CREATE SCHEMA IF NOT EXISTS app_a;
		CREATE SCHEMA IF NOT EXISTS app_b;
		CREATE TABLE public.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE app_a.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE app_b.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	router, ok := applier.(ir.MultiDatabaseRouter)
	if !ok {
		t.Fatalf("applier %T does not implement ir.MultiDatabaseRouter", applier)
	}
	router.SetMultiDatabaseRouting(true, nil)

	events := []ir.Change{
		ir.Insert{Schema: schemaA, Table: "widgets", Row: ir.Row{"id": int64(1), "name": "a1"}},
		ir.Insert{Schema: schemaB, Table: "widgets", Row: ir.Row{"id": int64(1), "name": "b1"}},
		ir.Insert{Schema: schemaA, Table: "widgets", Row: ir.Row{"id": int64(2), "name": "a2"}},
		ir.Insert{Schema: boundSchema, Table: "widgets", Row: ir.Row{"id": int64(1), "name": "bound-explicit"}},
		ir.Insert{Schema: "", Table: "widgets", Row: ir.Row{"id": int64(2), "name": "bound-empty"}},
	}
	pumpChanges(t, ctx, applier, events)

	if got := countRowsInSchema(t, dsn, schemaA, "widgets"); got != 2 {
		t.Errorf("schema %q row count = %d; want 2 (cross-schema routing missed)", schemaA, got)
	}
	if got := countRowsInSchema(t, dsn, schemaB, "widgets"); got != 1 {
		t.Errorf("schema %q row count = %d; want 1 (cross-schema routing missed)", schemaB, got)
	}
	if got := countRowsInSchema(t, dsn, boundSchema, "widgets"); got != 2 {
		t.Errorf("bound schema %q row count = %d; want 2 (bound + empty-Schema must stay bound)", boundSchema, got)
	}

	// Cross-namespace UPDATE + DELETE also route.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Update{
			Schema: schemaA, Table: "widgets",
			Before: ir.Row{"id": int64(1), "name": "a1"},
			After:  ir.Row{"id": int64(1), "name": "a1-updated"},
		},
		ir.Delete{
			Schema: schemaB, Table: "widgets",
			Before: ir.Row{"id": int64(1), "name": "b1"},
		},
	})

	if got := scalarStringSchema(t, dsn, `SELECT name FROM "`+schemaA+`"."widgets" WHERE id = 1`); got != "a1-updated" {
		t.Errorf("schema %q id=1 name = %q; want a1-updated (cross-schema UPDATE missed)", schemaA, got)
	}
	if got := countRowsInSchema(t, dsn, schemaB, "widgets"); got != 0 {
		t.Errorf("schema %q row count after delete = %d; want 0 (cross-schema DELETE missed)", schemaB, got)
	}
}

// TestChangeApplier_RoutingDisabled_IsByteIdentical is the back-compat
// pin: with routing DISABLED (the default), a change whose Schema
// differs from the bound schema lands in the BOUND schema — the
// cross-engine single-database case (a differently-named source schema
// must not start qualifying). This guards the Phase-1a over-qualification
// regression on the apply path. The applier is bound to `app_only`, and
// the change carries `Schema:"public"`; with routing off the row must
// land in `app_only`, NOT `public`.
func TestChangeApplier_RoutingDisabled_IsByteIdentical(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE SCHEMA IF NOT EXISTS app_only;
		CREATE TABLE app_only.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Bind the applier's user-data schema to app_only via SetSchema (the
	// orchestrator's --target-schema path). Routing stays at its default
	// (disabled).
	if s, ok := applier.(ir.SchemaSetter); ok {
		s.SetSchema("app_only")
	} else {
		t.Fatalf("applier %T does not implement ir.SchemaSetter", applier)
	}

	// change.Schema = "public" differs from the bound "app_only". With
	// routing off it must be ignored and the row lands in app_only.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "public", Table: "widgets", Row: ir.Row{"id": int64(1), "name": "x"}},
	})

	if got := countRowsInSchema(t, dsn, "app_only", "widgets"); got != 1 {
		t.Errorf("app_only row count = %d; want 1 (routing-off differing schema must stay bound)", got)
	}
	// public.widgets was never created — and routing-off must NOT have
	// qualified to the differing source schema "public" (which would have
	// errored the Apply with "relation public.widgets does not exist").
	// The fact that Apply succeeded and the row landed in app_only proves
	// the guard held; assert public.widgets genuinely does not exist as a
	// belt-and-suspenders check.
	if relationExists(t, dsn, "public", "widgets") {
		t.Errorf("public.widgets exists; routing-off must NOT qualify to the differing source schema")
	}
}

// relationExists reports whether schema.table exists on the target.
func relationExists(t *testing.T, dsn, schema, table string) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	const q = `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = $1 AND table_name = $2
	)`
	if err := db.QueryRowContext(ctx, q, schema, table).Scan(&exists); err != nil {
		t.Fatalf("relationExists %s.%s: %v", schema, table, err)
	}
	return exists
}

// scalarStringSchema runs a single-string-column query (table is
// fully-qualified in the query, so the DSN's search_path is irrelevant).
func scalarStringSchema(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("scalarStringSchema %q: %v", query, err)
	}
	return s
}
