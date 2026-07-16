// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SQL-shape pins for the backfill executor (ADR-0159). Same matrix as
// the mysql pins — {single PK, composite PK} × {first chunk, mid-walk
// chunk} × {where, no where} — with Postgres's quoting, clause-order
// $n placeholder numbering, and schema qualification.

package postgres

import (
	"bytes"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func backfillTestTable(pk ...string) *ir.Table {
	cols := make([]ir.IndexColumn, 0, len(pk))
	for _, c := range pk {
		cols = append(cols, ir.IndexColumn{Column: c})
	}
	return &ir.Table{
		Name: "items",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "region", Type: ir.Varchar{Length: 16}},
			{Name: "new_col", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: cols},
	}
}

func TestBackfillBuildUpdate_PG(t *testing.T) {
	x := &BackfillExecutor{schema: "public"}
	sets := []ir.BackfillSet{{Column: "new_col", Expr: "old_col + 1"}}
	multi := []ir.BackfillSet{{Column: "a", Expr: "x * 2"}, {Column: "b", Expr: "upper(y)"}}
	cases := []struct {
		name     string
		table    *ir.Table
		sets     []ir.BackfillSet
		where    string
		hasLower bool
		want     string
	}{
		{
			name: "single PK, first chunk, no where", table: backfillTestTable("id"),
			sets: sets, hasLower: false,
			want: `UPDATE "public"."items" SET "new_col" = old_col + 1 WHERE ("id") <= ($1)`,
		},
		{
			name: "single PK, mid walk, with where", table: backfillTestTable("id"),
			sets: sets, where: "new_col IS NULL", hasLower: true,
			want: `UPDATE "public"."items" SET "new_col" = old_col + 1 WHERE ("id") > ($1) AND ("id") <= ($2) AND (new_col IS NULL)`,
		},
		{
			name: "composite PK, mid walk, no where", table: backfillTestTable("id", "region"),
			sets: sets, hasLower: true,
			want: `UPDATE "public"."items" SET "new_col" = old_col + 1 WHERE ("id", "region") > ($1, $2) AND ("id", "region") <= ($3, $4)`,
		},
		{
			name: "multi-set, first chunk, with where", table: backfillTestTable("id"),
			sets: multi, where: "a IS NULL", hasLower: false,
			want: `UPDATE "public"."items" SET "a" = x * 2, "b" = upper(y) WHERE ("id") <= ($1) AND (a IS NULL)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := x.buildBackfillUpdate(tc.table, tc.sets, tc.where, tc.hasLower); got != tc.want {
				t.Errorf("got:  %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestBackfillBoundarySelect_PG(t *testing.T) {
	x := &BackfillExecutor{schema: "public"}
	cases := []struct {
		name      string
		table     *ir.Table
		hasCursor bool
		desc      bool
		limit     int
		want      string
	}{
		{
			name: "first chunk ascending", table: backfillTestTable("id"),
			limit: 10,
			want:  `SELECT "id" FROM "public"."items" ORDER BY "id" LIMIT 1 OFFSET 9`,
		},
		{
			name: "mid walk ascending", table: backfillTestTable("id"),
			hasCursor: true, limit: 10,
			want: `SELECT "id" FROM "public"."items" WHERE ("id") > ($1) ORDER BY "id" LIMIT 1 OFFSET 9`,
		},
		{
			name: "tail descending composite", table: backfillTestTable("id", "region"),
			hasCursor: true, desc: true, limit: 10,
			want: `SELECT "id", "region" FROM "public"."items" WHERE ("id", "region") > ($1, $2) ORDER BY "id" DESC, "region" DESC LIMIT 1`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := x.buildBackfillBoundarySelect(tc.table, tc.hasCursor, tc.desc, tc.limit); got != tc.want {
				t.Errorf("got:  %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestBackfillCursorNormalization_PG pins the cursor-scan contract:
// []byte passes through RAW — the store's cursor envelope round-trips
// it byte-exact and pgx binds it as bytea; stringifying it here is
// exactly what let encoding/json replace invalid-UTF-8 PK bytes with
// U+FFFD (audit 2026-07-15 CRITICAL-2). time.Time still becomes the
// PG literal form with the offset preserved.
func TestBackfillCursorNormalization_PG(t *testing.T) {
	raw := []byte{0x9F, 0x80, 0x41, 0xFE, 0x10} // invalid UTF-8 — the observed mangled cursor
	got := normalizeBackfillCursorValue(raw)
	if b, ok := got.([]byte); !ok || !bytes.Equal(b, raw) {
		t.Errorf("[]byte → %v (%T); want raw []byte passthrough", got, got)
	}
	ts := time.Date(2026, 7, 14, 10, 30, 0, 123456000, time.UTC)
	if got := normalizeBackfillCursorValue(ts); got != "2026-07-14 10:30:00.123456+00:00" {
		t.Errorf("time.Time → %v; want PG literal form with offset", got)
	}
	if got := normalizeBackfillCursorValue(int64(42)); got != int64(42) {
		t.Errorf("int64 → %v (%T); want passthrough", got, got)
	}
}
