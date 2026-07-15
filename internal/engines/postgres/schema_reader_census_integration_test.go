//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the items 68a/68b preflight censuses against a
// real PostgreSQL: the FDW foreign-table census (ForeignTables) and
// the old-style inheritance-parent probe (InheritanceParents),
// including the ground truth that motivates each — the schema read's
// silent BASE-TABLE filter, and the parent-SELECT-returns-child-rows
// duplication mechanism.

package postgres

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"
)

// TestSchemaReader_ForeignTableCensus pins the item-68a census: a
// postgres_fdw foreign table (relkind='f') is invisible to ReadSchema
// — the pre-existing silent skip this census makes loud — while
// ForeignTables reports it with its foreign-server name. The FDW
// server needs no live remote: catalog rows exist without connecting.
func TestSchemaReader_ForeignTableCensus(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE local_users (id BIGINT PRIMARY KEY);

		CREATE EXTENSION postgres_fdw;
		CREATE SERVER erp_server FOREIGN DATA WRAPPER postgres_fdw
			OPTIONS (host 'erp.internal.example', dbname 'erp');
		CREATE FOREIGN TABLE remote_orders (
			id     BIGINT,
			amount NUMERIC(10,2)
		) SERVER erp_server;
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)

	// Ground truth: the schema read's BASE-TABLE filter drops the
	// foreign table (this is the silent skip the census surfaces).
	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if got := findTable(schema, "remote_orders"); got != nil {
		t.Errorf("ReadSchema unexpectedly returned the foreign table %q — the 68a WARN assumes the skip", "remote_orders")
	}
	if got := findTable(schema, "local_users"); got == nil {
		t.Errorf("ReadSchema should still return the plain table local_users")
	}

	sr, ok := r.(*SchemaReader)
	if !ok {
		t.Fatalf("reader is %T; want *SchemaReader", r)
	}
	foreign, err := sr.ForeignTables(ctx)
	if err != nil {
		t.Fatalf("ForeignTables: %v", err)
	}
	want := map[string]string{"remote_orders": "erp_server"}
	if !reflect.DeepEqual(foreign, want) {
		t.Errorf("ForeignTables = %v; want %v", foreign, want)
	}
}

// TestSchemaReader_InheritanceParents pins the item-68b probe against
// real catalogs: an old-style INHERITS parent (relkind='r') is
// reported; a DECLARATIVE partition parent is NOT (that's Bug 100's
// pg_partitioned_table probe — the relkind filter keeps the two
// preflights disjoint); plain unrelated tables are not reported. It
// also pins the duplication mechanism the refusal names: a row
// inserted into the child comes back from a parent SELECT without
// ONLY.
func TestSchemaReader_InheritanceParents(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE measurements (
			id  BIGINT NOT NULL,
			ts  TIMESTAMPTZ NOT NULL
		);
		CREATE TABLE measurements_2025 () INHERITS (measurements);

		CREATE TABLE events (
			id BIGINT NOT NULL,
			at DATE NOT NULL
		) PARTITION BY RANGE (at);
		CREATE TABLE events_2025 PARTITION OF events
			FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

		CREATE TABLE plain (id BIGINT PRIMARY KEY);

		INSERT INTO measurements_2025 (id, ts) VALUES (1, now());
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)
	sr, ok := r.(*SchemaReader)
	if !ok {
		t.Fatalf("reader is %T; want *SchemaReader", r)
	}

	parents, err := sr.InheritanceParents(ctx)
	if err != nil {
		t.Fatalf("InheritanceParents: %v", err)
	}
	if want := []string{"measurements"}; !reflect.DeepEqual(parents, want) {
		t.Errorf("InheritanceParents = %v; want %v (declarative partition parent %q must NOT be reported)",
			parents, want, "events")
	}

	// The duplication ground truth: the child's row is visible through
	// the parent (no ONLY) — exactly why an unguarded migration would
	// copy it twice.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var viaParent, viaOnly int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM measurements").Scan(&viaParent); err != nil {
		t.Fatalf("count via parent: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM ONLY measurements").Scan(&viaOnly); err != nil {
		t.Fatalf("count via ONLY: %v", err)
	}
	if viaParent != 1 || viaOnly != 0 {
		t.Errorf("parent SELECT sees %d rows, ONLY sees %d; want 1/0 — the duplication mechanism the refusal names", viaParent, viaOnly)
	}
}
