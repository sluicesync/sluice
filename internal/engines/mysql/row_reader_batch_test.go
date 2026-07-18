// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestBuildBatchedSelect_SinglePK_FirstBatch(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	got := buildBatchedSelect(table, 5000, false, false, "")
	want := "SELECT `id`, `email` FROM `users` ORDER BY `users`.`id` LIMIT 5000"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestBuildBatchedSelect_SinglePK_WithCursor(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	got := buildBatchedSelect(table, 1000, true, false, "")
	want := "SELECT `id` FROM `users` WHERE (`users`.`id`) > (?) ORDER BY `users`.`id` LIMIT 1000"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestBuildBatchedSelect_CompositePK(t *testing.T) {
	table := &ir.Table{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "sku", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "tenant"}, {Column: "sku"},
		}},
	}
	got := buildBatchedSelect(table, 1000, true, false, "")
	want := "SELECT `tenant`, `sku`, `name` FROM `products` WHERE (`products`.`tenant`, `products`.`sku`) > (?, ?) ORDER BY `products`.`tenant`, `products`.`sku` LIMIT 1000"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchedSelect_TemporalPK_QualifiesCursor pins the Vector A
// follow-up: when a PK column is temporal, selectColumnExpr projects it
// as `CAST(`c` AS CHAR) AS `c“, introducing a SELECT-list alias with the
// bare column name. An unqualified `ORDER BY `c“ would bind to that CHAR
// alias (a STRING sort) while the cursor predicate `(`c`) > (?)` compares
// the DATE column against a time.Time value — inconsistent, and the alias
// sort defeats the PRIMARY index. The cursor/order refs must therefore be
// TABLE-QUALIFIED so both bind the real column. This test is the only
// level that catches the regression: end-to-end pagination over valid ISO
// dates is correct under BOTH the string and date sort (ISO is lexically
// monotonic with date order), so only the SQL shape distinguishes them.
func TestBuildBatchedSelect_TemporalPK_QualifiesCursor(t *testing.T) {
	table := &ir.Table{
		Name: "snapshots",
		Columns: []*ir.Column{
			{Name: "taken_on", Type: ir.Date{}},
			{Name: "label", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "taken_on"}}},
	}
	got := buildBatchedSelect(table, 1000, true, false, "")
	want := "SELECT CAST(`taken_on` AS CHAR) AS `taken_on`, `label` FROM `snapshots` " +
		"WHERE (`snapshots`.`taken_on`) > (?) ORDER BY `snapshots`.`taken_on` LIMIT 1000"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
	// Guard against the specific regression: the ORDER BY / WHERE refs must
	// NOT be the bare alias (which would bind the CHAR projection).
	if strings.Contains(got, "ORDER BY `taken_on`") {
		t.Error("ORDER BY binds the bare CAST alias (string sort); want the qualified date column")
	}
	if strings.Contains(got, "(`taken_on`) >") {
		t.Error("cursor predicate is unqualified; ambiguous against the CAST alias")
	}
}

// TestBuildBatchedSelect_BothBounds confirms the ADR-0096 upper-bound
// clip is a SECOND row-comparison predicate on the same (table-qualified)
// PK tuple, ANDed after the lower bound. Both bounds being SQL row-
// comparisons is what makes the clip use the column's collation (the fix
// for utf8mb4_0900_ai_ci case-insensitive coverage gaps).
func TestBuildBatchedSelect_BothBounds(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Varchar{Length: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	got := buildBatchedSelect(table, 5000, true, true, "")
	want := "SELECT `id`, `email` FROM `users` " +
		"WHERE (`users`.`id`) > (?) AND (`users`.`id`) <= (?) ORDER BY `users`.`id` LIMIT 5000"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchedSelect_UpperOnly confirms a first-batch read (chunk 0,
// no lower) clipped to an upper bound emits only the `<=` predicate.
func TestBuildBatchedSelect_UpperOnly(t *testing.T) {
	table := &ir.Table{
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Varchar{Length: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	got := buildBatchedSelect(table, 5000, false, true, "")
	want := "SELECT `id` FROM `users` WHERE (`users`.`id`) <= (?) ORDER BY `users`.`id` LIMIT 5000"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchedSelect_CompositePK_BothBounds confirms both bounds emit
// the full composite tuple in declaration order.
func TestBuildBatchedSelect_CompositePK_BothBounds(t *testing.T) {
	table := &ir.Table{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "sku", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "tenant"}, {Column: "sku"}}},
	}
	got := buildBatchedSelect(table, 1000, true, true, "")
	want := "SELECT `tenant`, `sku` FROM `products` " +
		"WHERE (`products`.`tenant`, `products`.`sku`) > (?, ?) " +
		"AND (`products`.`tenant`, `products`.`sku`) <= (?, ?) " +
		"ORDER BY `products`.`tenant`, `products`.`sku` LIMIT 1000"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestReadRowsBatch_RejectsNoPK(t *testing.T) {
	r := &RowReader{}
	table := &ir.Table{
		Name:    "events",
		Columns: []*ir.Column{{Name: "data", Type: ir.Text{}}},
	}
	_, err := r.ReadRowsBatch(t.Context(), table, nil, 100)
	if err == nil {
		t.Fatal("ReadRowsBatch on no-PK table succeeded; want error")
	}
	if !strings.Contains(err.Error(), "no primary key") {
		t.Errorf("err = %v; want 'no primary key' wording", err)
	}
}
