//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0049 sluice_cdc_schema_history
// control table (Chunk A), Postgres side. Mirrors the MySQL test:
// additive to sluice_cdc_state, idempotent ensure, write→resolve
// round-trip with the LSN total order, and the below-floor loud
// ir.ErrPositionInvalid.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestEnsureSchemaHistoryTable_AdditiveToCDCState(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position)
		VALUES ('live-stream', 'tok-1');
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'sluice_cdc_schema_history'`).Scan(&n); err != nil {
		t.Fatalf("table lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("sluice_cdc_schema_history missing after EnsureControlTable; count=%d", n)
	}

	var tok string
	if err := db.QueryRowContext(ctx,
		`SELECT source_position FROM "public"."sluice_cdc_state" WHERE stream_id = $1`, "live-stream").Scan(&tok); err != nil {
		t.Fatalf("cdc-state select: %v", err)
	}
	if tok != "tok-1" {
		t.Errorf("cdc-state row mutated: token = %q; want tok-1", tok)
	}

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

func TestSchemaHistory_WriteResolveRoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	mkPos := func(lsn string) ir.Position {
		p, err := encodePGPos(pgPos{Slot: "s1", LSN: lsn})
		if err != nil {
			t.Fatalf("encodePGPos: %v", err)
		}
		return p
	}

	anchorOld := mkPos("0/1000000")
	anchorNew := mkPos("0/2000000")

	tblOld := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	tblNew := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
	}}

	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorOld, tblOld); err != nil {
		t.Fatalf("writeSchemaVersion old: %v", err)
	}
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new: %v", err)
	}
	// Idempotent re-write.
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new (idempotent): %v", err)
	}

	// LSN between old and new → resolves to OLD (pre-ALTER) schema.
	got, err := resolveSchemaVersion(ctx, db, eng, schema, "stream-1", schema, "users", mkPos("0/1500000"))
	if err != nil {
		t.Fatalf("resolve between: %v", err)
	}
	if len(got.Columns) != 1 {
		t.Errorf("between-resolve should be 1-column pre-ALTER; got %d", len(got.Columns))
	}

	// LSN at-or-after new → NEW schema.
	got, err = resolveSchemaVersion(ctx, db, eng, schema, "stream-1", schema, "users", mkPos("0/2500000"))
	if err != nil {
		t.Fatalf("resolve after: %v", err)
	}
	if len(got.Columns) != 2 {
		t.Errorf("after-resolve should be 2-column post-ALTER; got %d", len(got.Columns))
	}

	// LSN before the oldest retained anchor → loud ir.ErrPositionInvalid.
	if _, err := resolveSchemaVersion(ctx, db, eng, schema, "stream-1", schema, "users", mkPos("0/500000")); !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("below-floor resolve must wrap ir.ErrPositionInvalid; got %v", err)
	}
}
