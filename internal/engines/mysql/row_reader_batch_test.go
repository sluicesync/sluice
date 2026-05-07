// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
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
	got := buildBatchedSelect(table, 5000, false)
	want := "SELECT `id`, `email` FROM `users` ORDER BY `id` LIMIT 5000"
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
	got := buildBatchedSelect(table, 1000, true)
	want := "SELECT `id` FROM `users` WHERE (`id`) > (?) ORDER BY `id` LIMIT 1000"
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
	got := buildBatchedSelect(table, 1000, true)
	want := "SELECT `tenant`, `sku`, `name` FROM `products` WHERE (`tenant`, `sku`) > (?, ?) ORDER BY `tenant`, `sku` LIMIT 1000"
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
