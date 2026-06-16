//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 149 applier pin. A MySQL SET decodes to a Go []string (Bug 148), and
// its PG target is TEXT[] — which the CDC applier's loadColumnTypes resolves
// as ir.Array{Text}. Before the fix, prepareValue's Array branch required
// []any and rejected the []string with "expected []any for Array column, got
// []string", killing the stream at apply (loud, no silent loss). This drives
// the []string SET shape through the applier's INSERT/UPSERT array-binding and
// asserts it lands as the expected text[] — INSERT, empty set, a >8-member
// set, and an UPDATE.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestApplier_SetTextArray_Bug149(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `CREATE TABLE s (id BIGINT PRIMARY KEY, tags text[]);`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	applier := openPipelinedApplier(t, ctx, dsn)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Each "tags" value is the []string shape the MySQL SET decoder yields.
	events := []ir.Change{
		ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "s1"}, Schema: "public", Table: "s", Row: ir.Row{"id": int64(1), "tags": []string{"a", "c"}}},
		ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "s2"}, Schema: "public", Table: "s", Row: ir.Row{"id": int64(2), "tags": []string{}}},
		ir.Insert{Position: ir.Position{Engine: engineNamePostgres, Token: "s3"}, Schema: "public", Table: "s", Row: ir.Row{"id": int64(3), "tags": []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"}}},
		ir.Update{Position: ir.Position{Engine: engineNamePostgres, Token: "s4"}, Schema: "public", Table: "s", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "tags": []string{"a", "b", "c"}}},
	}
	pumpBatchedChanges(t, ctx, applier, events, 100)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// tags::text gives PG's canonical array literal — stable to assert on.
	cases := []struct {
		id   int64
		want string
	}{
		{1, "{a,b,c}"}, // updated
		{2, "{}"},      // empty set
		{3, "{m0,m1,m2,m3,m4,m5,m6,m7,m8,m9}"},
	}
	for _, c := range cases {
		var got string
		if err := db.QueryRowContext(ctx, "SELECT tags::text FROM s WHERE id = $1", c.id).Scan(&got); err != nil {
			t.Fatalf("read tags for id=%d: %v", c.id, err)
		}
		if got != c.want {
			t.Errorf("id=%d tags = %q; want %q (SET []string must apply as text[])", c.id, got, c.want)
		}
	}
}
