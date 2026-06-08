//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestListDatabases_ExcludesSystemSchemas pins the engine-level schema
// enumeration (ADR-0075): user schemas are returned, the system set
// (pg_catalog, information_schema, pg_toast, pg_temp*) is NEVER returned,
// and a non-system lookalike (information_schema_data) IS returned.
func TestListDatabases_ExcludesSystemSchemas(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "listschemas_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create two user schemas + a deliberate non-system lookalike whose
	// name resembles a system schema but must NOT be excluded.
	if _, err := db.ExecContext(ctx, `
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE SCHEMA information_schema_data;
	`); err != nil {
		t.Fatalf("create schemas: %v", err)
	}

	got, err := Engine{}.ListDatabases(ctx, dsn)
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}

	set := map[string]struct{}{}
	for _, s := range got {
		set[s] = struct{}{}
	}

	// User schemas present (public is created by default; assert the ones
	// we made plus the lookalike).
	for _, want := range []string{"sales", "billing", "information_schema_data", "public"} {
		if _, ok := set[want]; !ok {
			t.Errorf("ListDatabases missing user schema %q; got=%v", want, got)
		}
	}
	// System schemas absent.
	for _, sys := range []string{"pg_catalog", "information_schema", "pg_toast"} {
		if _, ok := set[sys]; ok {
			t.Errorf("ListDatabases returned system schema %q; must be excluded (got=%v)", sys, got)
		}
	}
}

// TestEnsureDatabase_CreatesSchemaIdempotent pins EnsureDatabase =
// CREATE SCHEMA IF NOT EXISTS, including the idempotent re-run.
func TestEnsureDatabase_CreatesSchemaIdempotent(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "ensureschema_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := (Engine{}).EnsureDatabase(ctx, dsn, "warehouse"); err != nil {
		t.Fatalf("EnsureDatabase (first): %v", err)
	}
	// Idempotent: a second call must not error.
	if err := (Engine{}).EnsureDatabase(ctx, dsn, "warehouse"); err != nil {
		t.Fatalf("EnsureDatabase (idempotent): %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'warehouse'`,
	).Scan(&n); err != nil {
		t.Fatalf("count schema: %v", err)
	}
	if n != 1 {
		t.Errorf("warehouse schema count = %d; want 1", n)
	}
}
