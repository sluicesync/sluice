//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 150 integration pin: a forwarded DROP COLUMN of a synthesized ENUM
// column must also drop the per-column enum type sluice created for it, and a
// later drop-then-readd of a same-named column with DIFFERENT values must get
// a FRESH type — not silently reuse the stale orphan (the correctness edge).

package postgres

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestSchemaWriter_DropEnumColumn_DropsOrphanType_Bug150(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	swIface, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := swIface.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	sw := swIface.(*SchemaWriter)

	tbl := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			// Synthesized enum (no TypeName → MySQL-source shape): sluice
			// creates "widgets_status_enum" for it.
			{Name: "status", Type: ir.Enum{Values: []string{"draft", "live"}}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	if err := sw.CreateTablesWithoutConstraints(ctx, &ir.Schema{Tables: []*ir.Table{tbl}}); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	const typeName = "widgets_status_enum"
	if !typeExists(t, ctx, db, typeName) {
		t.Fatalf("enum type %q was not created by cold-start", typeName)
	}

	// Forwarded DROP COLUMN of the enum column → the orphan type must go too.
	if err := sw.AlterDropColumn(ctx, tbl, []*ir.Column{tbl.Columns[1]}); err != nil {
		t.Fatalf("AlterDropColumn: %v", err)
	}
	if typeExists(t, ctx, db, typeName) {
		t.Errorf("Bug 150: enum type %q left orphaned after DROP COLUMN; want it dropped", typeName)
	}

	// Correctness edge: re-add a same-named column with a DIFFERENT value set.
	// With the orphan gone, ensureEnumType creates a FRESH type carrying the
	// NEW values — not the stale {draft,live}.
	readd := &ir.Column{Name: "status", Type: ir.Enum{Values: []string{"archived", "active", "paused"}}}
	if err := sw.AlterAddColumn(ctx, tbl, []*ir.Column{readd}); err != nil {
		t.Fatalf("AlterAddColumn (re-add): %v", err)
	}
	got := enumValues(t, ctx, db, typeName)
	if want := []string{"archived", "active", "paused"}; !reflect.DeepEqual(got, want) {
		t.Errorf("re-added enum %q values = %v; want %v (stale orphan reuse would give the old set)", typeName, got, want)
	}
}

func typeExists(t *testing.T, ctx context.Context, db *sql.DB, typname string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
		   WHERE t.typname = $1 AND n.nspname = 'public')`, typname).Scan(&exists); err != nil {
		t.Fatalf("typeExists(%q): %v", typname, err)
	}
	return exists
}

func enumValues(t *testing.T, ctx context.Context, db *sql.DB, typname string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT e.enumlabel FROM pg_enum e
		   JOIN pg_type t      ON t.oid = e.enumtypid
		   JOIN pg_namespace n ON n.oid = t.typnamespace
		   WHERE t.typname = $1 AND n.nspname = 'public'
		   ORDER BY e.enumsortorder`, typname)
	if err != nil {
		t.Fatalf("enumValues(%q): %v", typname, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan enum label: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate enum labels: %v", err)
	}
	return out
}
