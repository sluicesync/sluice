// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestChangeApplier_Apply_JSONColumnUnderReplicaIdentityFull is the
// pgcopydb-PR-#28-equivalent regression pin. PG's `json` (text-backed)
// has NO `=` operator — a bare `col = $N` against it errors with
// `42883 could not identify an equality operator for type json`. Under
// REPLICA IDENTITY FULL the apply WHERE includes every column of the
// OldTuple, so without the type-aware cast in [buildWhereClause] every
// UPDATE / DELETE apply against a target with a `json` column would
// silently break (42883 → applier.Apply returns error → stream stalls).
// This test exercises the path end-to-end so a regression to the bare
// `col = $N` form surfaces loudly here even if the unit shape pin
// ([TestBuildWhereClause_JSONCastUnderReplicaIdentityFull]) is bypassed.
//
// Mirrors pgcopydb PR #28. See
// `docs/dev/notes/pgcopydb-planetscale-fork-review.md`.
func TestChangeApplier_Apply_JSONColumnUnderReplicaIdentityFull(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE json_under_full (
			id   BIGINT PRIMARY KEY,
			doc  JSON   NOT NULL,
			lbl  TEXT   NOT NULL
		);
		ALTER TABLE json_under_full REPLICA IDENTITY FULL;
		INSERT INTO json_under_full (id, doc, lbl) VALUES (1, '{"a":1}', 'before');
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	eng := Engine{}
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

	// Construct an UPDATE that mimics what the pgoutput reader would
	// emit under REPLICA IDENTITY FULL: Before carries every column
	// (incl. the `json` column with its exact stored text), After
	// carries the new values. The applier's WHERE will be built from
	// Before — which is exactly where the 42883 fires pre-fix.
	upd := ir.Update{
		Position: ir.Position{Token: "p1"},
		Schema:   "public",
		Table:    "json_under_full",
		Before: ir.Row{
			"id":  int64(1),
			"doc": `{"a":1}`,
			"lbl": "before",
		},
		After: ir.Row{
			"id":  int64(1),
			"doc": `{"a":1,"b":2}`,
			"lbl": "after",
		},
	}

	ch := make(chan ir.Change, 1)
	ch <- upd
	close(ch)

	if err := applier.Apply(ctx, "json-where-stream", ch); err != nil {
		t.Fatalf("Apply: %v\n(this is the 42883 latent bug if the `col::text = $N::text` "+
			"cast for json columns is missing from buildWhereClause)", err)
	}

	// Verify the apply landed: the row's lbl flipped to 'after' and
	// the doc grew the new key. We compare doc as text since PG's
	// json round-trips raw text and we want a byte-shape check, not
	// a semantic JSON-equality (which json doesn't even support).
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var gotDoc, gotLbl string
	if err := db.QueryRowContext(
		ctx,
		`SELECT doc::text, lbl FROM public.json_under_full WHERE id = $1`,
		int64(1),
	).Scan(&gotDoc, &gotLbl); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if gotLbl != "after" {
		t.Errorf("lbl = %q; want %q (apply didn't land)", gotLbl, "after")
	}
	// PG normalises json whitespace on text-out; tolerate either form.
	if !strings.Contains(gotDoc, `"b":2`) && !strings.Contains(gotDoc, `"b": 2`) {
		t.Errorf("doc = %q; want the After value to include `b:2`", gotDoc)
	}
}
