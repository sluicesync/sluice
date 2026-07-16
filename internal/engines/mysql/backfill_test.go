// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SQL-shape pins for the backfill executor (ADR-0159). The matrix is
// {single PK, composite PK} × {first chunk, mid-walk chunk} ×
// {where, no where} — every UPDATE bound must be a row-comparison on
// the PK tuple (the ADR-0096 collation contract), and the boundary
// SELECTs must order by exactly the PK in both directions.

package mysql

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

func TestBackfillBuildUpdate_MySQL(t *testing.T) {
	sets := []ir.BackfillSet{{Column: "new_col", Expr: "old_col + 1"}}
	multi := []ir.BackfillSet{{Column: "a", Expr: "x * 2"}, {Column: "b", Expr: "UPPER(y)"}}
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
			want: "UPDATE `items` SET `new_col` = old_col + 1 WHERE (`id`) <= (?)",
		},
		{
			name: "single PK, mid walk, with where", table: backfillTestTable("id"),
			sets: sets, where: "new_col IS NULL", hasLower: true,
			want: "UPDATE `items` SET `new_col` = old_col + 1 WHERE (`id`) > (?) AND (`id`) <= (?) AND (new_col IS NULL)",
		},
		{
			name: "composite PK, mid walk, no where", table: backfillTestTable("id", "region"),
			sets: sets, hasLower: true,
			want: "UPDATE `items` SET `new_col` = old_col + 1 WHERE (`id`, `region`) > (?, ?) AND (`id`, `region`) <= (?, ?)",
		},
		{
			name: "multi-set, first chunk, with where", table: backfillTestTable("id"),
			sets: multi, where: "a IS NULL", hasLower: false,
			want: "UPDATE `items` SET `a` = x * 2, `b` = UPPER(y) WHERE (`id`) <= (?) AND (a IS NULL)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildBackfillUpdate(tc.table, tc.sets, tc.where, tc.hasLower); got != tc.want {
				t.Errorf("got:  %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func TestBackfillBoundarySelect_MySQL(t *testing.T) {
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
			want:  "SELECT `id` FROM `items` ORDER BY `id` LIMIT 1 OFFSET 9",
		},
		{
			name: "mid walk ascending", table: backfillTestTable("id"),
			hasCursor: true, limit: 10,
			want: "SELECT `id` FROM `items` WHERE (`id`) > (?) ORDER BY `id` LIMIT 1 OFFSET 9",
		},
		{
			name: "tail descending composite", table: backfillTestTable("id", "region"),
			hasCursor: true, desc: true, limit: 10,
			want: "SELECT `id`, `region` FROM `items` WHERE (`id`, `region`) > (?, ?) ORDER BY `id` DESC, `region` DESC LIMIT 1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildBackfillBoundarySelect(tc.table, tc.hasCursor, tc.desc, tc.limit); got != tc.want {
				t.Errorf("got:  %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestBackfillCursorNormalization_MySQL pins the cursor-scan contract:
// []byte passes through RAW — the store's cursor envelope round-trips
// it byte-exact, and stringifying it here is exactly what let
// encoding/json replace invalid-UTF-8 PK bytes with U+FFFD (audit
// 2026-07-15 CRITICAL-2). time.Time's RFC 3339 form is not a reliable
// MySQL comparison literal, so it still normalizes to the native
// literal form at scan.
func TestBackfillCursorNormalization_MySQL(t *testing.T) {
	raw := []byte{0x9F, 0x80, 0x41, 0xFE, 0x10} // invalid UTF-8 — the observed mangled cursor
	got := normalizeBackfillCursorValue(raw)
	if b, ok := got.([]byte); !ok || !bytes.Equal(b, raw) {
		t.Errorf("[]byte → %v (%T); want raw []byte passthrough", got, got)
	}
	ts := time.Date(2026, 7, 14, 10, 30, 0, 123456000, time.UTC)
	if got := normalizeBackfillCursorValue(ts); got != "2026-07-14 10:30:00.123456" {
		t.Errorf("time.Time → %v; want MySQL literal form", got)
	}
	if got := normalizeBackfillCursorValue(int64(42)); got != int64(42) {
		t.Errorf("int64 → %v (%T); want passthrough", got, got)
	}
}
