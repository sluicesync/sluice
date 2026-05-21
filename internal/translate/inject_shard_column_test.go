// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestInjectShardColumn_AppendsColumnAndComposesPK(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{{
		Name: "customer",
		Columns: []*ir.Column{
			{Name: "customer_id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: false},
		},
		PrimaryKey: &ir.Index{
			Name:    "PRIMARY",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "customer_id"}},
		},
	}}}

	out, err := InjectShardColumn(src, "source_shard_id", ir.Varchar{Length: 64})
	if err != nil {
		t.Fatalf("InjectShardColumn: %v", err)
	}
	if len(out.Tables) != 1 {
		t.Fatalf("expected 1 table; got %d", len(out.Tables))
	}
	tbl := out.Tables[0]

	// Column appended last with SluiceInjected=true, NOT NULL, the
	// supplied value type.
	if got := len(tbl.Columns); got != 3 {
		t.Fatalf("expected 3 columns; got %d", got)
	}
	disc := tbl.Columns[2]
	if disc.Name != "source_shard_id" {
		t.Errorf("disc.Name = %q; want source_shard_id", disc.Name)
	}
	if !disc.SluiceInjected {
		t.Errorf("disc.SluiceInjected = false; want true")
	}
	if disc.Nullable {
		t.Errorf("disc.Nullable = true; want false (NOT NULL)")
	}
	if _, ok := disc.Type.(ir.Varchar); !ok {
		t.Errorf("disc.Type = %T; want ir.Varchar", disc.Type)
	}

	// PK rewritten: discriminator first, then original PK columns.
	if tbl.PrimaryKey == nil || len(tbl.PrimaryKey.Columns) != 2 {
		t.Fatalf("PK columns = %+v; want 2 entries", tbl.PrimaryKey)
	}
	if got := tbl.PrimaryKey.Columns[0].Column; got != "source_shard_id" {
		t.Errorf("PK[0] = %q; want source_shard_id", got)
	}
	if got := tbl.PrimaryKey.Columns[1].Column; got != "customer_id" {
		t.Errorf("PK[1] = %q; want customer_id", got)
	}

	// Source schema unchanged (copy-on-write).
	if len(src.Tables[0].Columns) != 2 {
		t.Errorf("source schema mutated: %d columns; want 2 unchanged", len(src.Tables[0].Columns))
	}
	if len(src.Tables[0].PrimaryKey.Columns) != 1 {
		t.Errorf("source PK mutated: %d cols; want 1", len(src.Tables[0].PrimaryKey.Columns))
	}
}

func TestInjectShardColumn_PreservesOriginalPKOrder(t *testing.T) {
	// Multi-column PK — the rewrite must keep the original order.
	src := &ir.Schema{Tables: []*ir.Table{{
		Name: "order_line",
		Columns: []*ir.Column{
			{Name: "order_id", Type: ir.Integer{Width: 64}},
			{Name: "line_no", Type: ir.Integer{Width: 32}},
			{Name: "sku", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "order_id"},
			{Column: "line_no"},
		}},
	}}}
	out, err := InjectShardColumn(src, "shard", ir.Varchar{Length: 32})
	if err != nil {
		t.Fatalf("InjectShardColumn: %v", err)
	}
	got := out.Tables[0].PrimaryKey.Columns
	if len(got) != 3 {
		t.Fatalf("PK has %d cols; want 3", len(got))
	}
	want := []string{"shard", "order_id", "line_no"}
	for i, w := range want {
		if got[i].Column != w {
			t.Errorf("PK[%d] = %q; want %q", i, got[i].Column, w)
		}
	}
}

func TestInjectShardColumn_RefusesTableWithoutPK(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{{
		Name:    "events_no_pk",
		Columns: []*ir.Column{{Name: "ts", Type: ir.Timestamp{}}},
		// PrimaryKey deliberately nil.
	}}}
	_, err := InjectShardColumn(src, "shard", ir.Varchar{Length: 32})
	if err == nil {
		t.Fatal("expected refusal for table without PK; got nil")
	}
	if !strings.Contains(err.Error(), "events_no_pk") {
		t.Errorf("error %q missing table name", err.Error())
	}
	if !strings.Contains(err.Error(), "--exclude-table") {
		t.Errorf("error %q missing recovery hint", err.Error())
	}
}

func TestInjectShardColumn_RefusesEmptyPKColumnList(t *testing.T) {
	// PrimaryKey non-nil but Columns is empty — equivalent to no-PK
	// for the purposes of composite-PK construction.
	src := &ir.Schema{Tables: []*ir.Table{{
		Name:       "weird_table",
		Columns:    []*ir.Column{{Name: "x", Type: ir.Integer{Width: 32}}},
		PrimaryKey: &ir.Index{Columns: nil},
	}}}
	_, err := InjectShardColumn(src, "shard", ir.Varchar{Length: 32})
	if err == nil {
		t.Fatal("expected refusal on empty PK column list; got nil")
	}
}

func TestInjectShardColumn_RefusesNameCollision(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{{
		Name: "customer",
		Columns: []*ir.Column{
			{Name: "customer_id", Type: ir.Integer{Width: 64}},
			{Name: "source_shard_id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "customer_id"}}},
	}}}
	_, err := InjectShardColumn(src, "source_shard_id", ir.Varchar{Length: 64})
	if err == nil {
		t.Fatal("expected refusal on column-name collision; got nil")
	}
	if !strings.Contains(err.Error(), "source_shard_id") {
		t.Errorf("error %q missing colliding column name", err.Error())
	}
}

func TestInjectShardColumn_NilArgs(t *testing.T) {
	cases := []struct {
		name      string
		s         *ir.Schema
		colName   string
		valueType ir.Type
		want      string
	}{
		{"nil schema", nil, "shard", ir.Varchar{Length: 32}, "schema is nil"},
		{"empty name", &ir.Schema{}, "", ir.Varchar{Length: 32}, "column name is empty"},
		{"nil value type", &ir.Schema{}, "shard", nil, "value type is nil"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := InjectShardColumn(tc.s, tc.colName, tc.valueType)
			if err == nil {
				t.Fatalf("expected refusal for %s; got nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q missing %q", err.Error(), tc.want)
			}
		})
	}
}

func TestInjectShardColumn_NoTables_NoOpEmpty(t *testing.T) {
	src := &ir.Schema{Tables: nil, Views: []*ir.View{{Name: "v"}}}
	out, err := InjectShardColumn(src, "shard", ir.Varchar{Length: 32})
	if err != nil {
		t.Fatalf("InjectShardColumn: %v", err)
	}
	if len(out.Tables) != 0 {
		t.Errorf("got %d tables; want 0", len(out.Tables))
	}
	if len(out.Views) != 1 {
		t.Errorf("expected views to pass through; got %d", len(out.Views))
	}
}

func TestInjectShardColumn_MixedFailureSurfacesFirst(t *testing.T) {
	// First refuse-worthy table fires; downstream tables aren't visited.
	// We can't observe non-traversal directly, but we CAN assert the
	// error names the FIRST offending table (the leftmost-failure
	// determinism callers rely on for predictable error messages).
	src := &ir.Schema{Tables: []*ir.Table{
		{Name: "a_no_pk", Columns: []*ir.Column{{Name: "x", Type: ir.Integer{Width: 32}}}},
		{Name: "b_no_pk", Columns: []*ir.Column{{Name: "y", Type: ir.Integer{Width: 32}}}},
	}}
	_, err := InjectShardColumn(src, "shard", ir.Varchar{Length: 32})
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	if !strings.Contains(err.Error(), "a_no_pk") || strings.Contains(err.Error(), "b_no_pk") {
		t.Errorf("expected error to name a_no_pk and NOT b_no_pk; got %q", err.Error())
	}
	// Sanity: the error is a real *errors* value, not a panic surrogate.
	if errors.Is(err, nil) {
		t.Fatalf("err should be non-nil")
	}
}
