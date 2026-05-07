// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestBuildBatchUpsert_SinglePK(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert(table, 2, pk)
	want := "INSERT INTO `users` (`id`, `email`, `name`) VALUES (?, ?, ?), (?, ?, ?) AS new ON DUPLICATE KEY UPDATE `email` = new.`email`, `name` = new.`name`"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestBuildBatchUpsert_CompositePK(t *testing.T) {
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
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert(table, 1, pk)
	want := "INSERT INTO `products` (`tenant`, `sku`, `name`) VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE `name` = new.`name`"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestBuildBatchUpsert_AllPKColumns(t *testing.T) {
	table := &ir.Table{
		Name: "tags",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "tag", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "tenant"}, {Column: "tag"},
		}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert(table, 1, pk)
	// No-op: re-assign first PK to itself so the statement is legal.
	if !strings.Contains(got, "`tenant` = new.`tenant`") {
		t.Errorf("got %q; want self-reassign of first PK column", got)
	}
}

func TestBuildBatchUpsert_NoPK(t *testing.T) {
	table := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}},
			{Name: "data", Type: ir.Text{}},
		},
	}
	got := buildBatchUpsert(table, 1, nil)
	if strings.Contains(got, "ON DUPLICATE KEY") {
		t.Errorf("expected plain INSERT for no-PK table; got %q", got)
	}
}

func TestPrimaryKeyColumns(t *testing.T) {
	table := &ir.Table{
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "a"}, {Column: "b"},
		}},
	}
	got := primaryKeyColumns(table)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("primaryKeyColumns: got %v; want [a b]", got)
	}
	if primaryKeyColumns(&ir.Table{}) != nil {
		t.Error("primaryKeyColumns: expected nil for table without PK")
	}
}
