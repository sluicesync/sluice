// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// idxCols builds a plain-column IndexColumn slice from column names.
func idxCols(names ...string) []ir.IndexColumn {
	cols := make([]ir.IndexColumn, len(names))
	for i, n := range names {
		cols[i] = ir.IndexColumn{Column: n}
	}
	return cols
}

// TestIndexLeftPrefixCovers pins the "does an existing index cover this FK's
// referencing columns as a left-prefix?" decision across the family of
// shapes: exact match, PK left-prefix, composite tuple order, expression /
// partial index carve-outs, and single vs multi-column.
func TestIndexLeftPrefixCovers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		idx    *ir.Index
		fkCols []string
		want   bool
	}{
		{"nil index", nil, []string{"a"}, false},
		{"empty fk cols", &ir.Index{Columns: idxCols("a")}, nil, false},
		{"exact single", &ir.Index{Columns: idxCols("a")}, []string{"a"}, true},
		{"exact composite", &ir.Index{Columns: idxCols("a", "b")}, []string{"a", "b"}, true},
		{"left-prefix cover (index wider)", &ir.Index{Columns: idxCols("a", "b", "c")}, []string{"a", "b"}, true},
		{"index too narrow", &ir.Index{Columns: idxCols("a")}, []string{"a", "b"}, false},
		{"wrong leading column", &ir.Index{Columns: idxCols("b", "a")}, []string{"a"}, false},
		// Composite tuple ORDER matters: an index on (b,a) does not cover an
		// FK on (a,b) — a right-prefix / reordered match is not a cover.
		{"composite order mismatch", &ir.Index{Columns: idxCols("b", "a")}, []string{"a", "b"}, false},
		{"composite order match", &ir.Index{Columns: idxCols("a", "b")}, []string{"a", "b"}, true},
		// A partial (WHERE) index indexes only a subset of rows → not a cover.
		{"partial index not a cover", &ir.Index{Columns: idxCols("a"), Predicate: "a IS NOT NULL"}, []string{"a"}, false},
		// An expression index entry (Column=="") never matches a plain column.
		{"expression index not a cover", &ir.Index{Columns: []ir.IndexColumn{{Expression: "lower(a)"}}}, []string{"a"}, false},
		// A MySQL prefix-LENGTH index still indexes the column → a cover.
		{"prefix-length index is a cover", &ir.Index{Columns: []ir.IndexColumn{{Column: "a", Length: 10}}}, []string{"a"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := indexLeftPrefixCovers(tc.idx, tc.fkCols); got != tc.want {
				t.Fatalf("indexLeftPrefixCovers(%+v, %v) = %v, want %v", tc.idx, tc.fkCols, got, tc.want)
			}
		})
	}
}

// TestFKColumnsCovered checks the table-level cover decision, including PK
// left-prefix coverage.
func TestFKColumnsCovered(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		table  *ir.Table
		fkCols []string
		want   bool
	}{
		{
			name:   "covered by PK left-prefix",
			table:  &ir.Table{PrimaryKey: &ir.Index{Columns: idxCols("tenant_id", "id")}},
			fkCols: []string{"tenant_id"},
			want:   true,
		},
		{
			name:   "not covered by PK (wrong lead)",
			table:  &ir.Table{PrimaryKey: &ir.Index{Columns: idxCols("id", "tenant_id")}},
			fkCols: []string{"tenant_id"},
			want:   false,
		},
		{
			name:   "covered by a secondary index",
			table:  &ir.Table{Indexes: []*ir.Index{{Columns: idxCols("customer_id")}}},
			fkCols: []string{"customer_id"},
			want:   true,
		},
		{
			name:   "no cover anywhere",
			table:  &ir.Table{Indexes: []*ir.Index{{Columns: idxCols("email")}}},
			fkCols: []string{"customer_id"},
			want:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := fkColumnsCovered(tc.table, tc.fkCols); got != tc.want {
				t.Fatalf("fkColumnsCovered = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestApplySkipForeignKeys_SynthesizesAndStrips is the end-to-end shape pin:
// an already-indexed FK is NOT duplicated, an un-indexed FK gets a synthesized
// backing index, a composite FK indexes the full tuple in order, all FKs are
// stripped, and the report is populated.
func TestApplySkipForeignKeys_SynthesizesAndStrips(t *testing.T) {
	t.Parallel()
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "orders",
			// customer_id already has a secondary index; product_id does not.
			Indexes: []*ir.Index{{Name: "orders_customer_idx", Columns: idxCols("customer_id")}},
			ForeignKeys: []*ir.ForeignKey{
				{Name: "fk_orders_customer", Columns: []string{"customer_id"}, ReferencedTable: "customers"},
				{Name: "fk_orders_product", Columns: []string{"product_id"}, ReferencedTable: "products"},
				// Composite FK, neither column indexed.
				{Name: "fk_orders_wh", Columns: []string{"region", "warehouse_id"}, ReferencedTable: "warehouses"},
			},
		},
	}}

	rep := applySkipForeignKeys(schema)

	orders := schema.Tables[0]
	if len(orders.ForeignKeys) != 0 {
		t.Fatalf("expected all FKs stripped, got %d", len(orders.ForeignKeys))
	}

	// Existing index preserved + exactly two synthesized (product_id, composite).
	// The pre-existing orders_customer_idx must NOT be duplicated.
	byName := map[string]*ir.Index{}
	for _, idx := range orders.Indexes {
		byName[idx.Name] = idx
	}
	if len(orders.Indexes) != 3 {
		t.Fatalf("expected 3 indexes (1 existing + 2 synthesized), got %d: %v", len(orders.Indexes), indexNames(orders.Indexes))
	}
	product := byName["orders_fk_product_id"]
	if product == nil {
		t.Fatalf("expected synthesized index orders_fk_product_id, have: %v", indexNames(orders.Indexes))
	}
	if len(product.Columns) != 1 || product.Columns[0].Column != "product_id" || product.Unique {
		t.Fatalf("bad synthesized product index: %+v", product)
	}
	composite := byName["orders_fk_region_warehouse_id"]
	if composite == nil {
		t.Fatalf("expected synthesized composite index orders_fk_region_warehouse_id, have: %v", indexNames(orders.Indexes))
	}
	if len(composite.Columns) != 2 || composite.Columns[0].Column != "region" || composite.Columns[1].Column != "warehouse_id" {
		t.Fatalf("composite index must index the full tuple in FK order, got: %+v", composite.Columns)
	}

	// Report: 3 skipped, 2 synthesized, 1 already covered.
	if len(rep.Skipped) != 3 {
		t.Fatalf("report should list 3 skipped FKs, got %d", len(rep.Skipped))
	}
	var synthesized, covered int
	for _, s := range rep.Skipped {
		if s.IndexName != "" {
			synthesized++
		}
		if s.CoveredExisting {
			covered++
		}
	}
	if synthesized != 2 || covered != 1 {
		t.Fatalf("report tally: synthesized=%d covered=%d, want 2/1", synthesized, covered)
	}
}

// TestApplySkipForeignKeys_DedupSameColumns proves two FKs on the SAME columns
// synthesize only one backing index (the second sees the first).
func TestApplySkipForeignKeys_DedupSameColumns(t *testing.T) {
	t.Parallel()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		ForeignKeys: []*ir.ForeignKey{
			{Name: "fk_a", Columns: []string{"x"}, ReferencedTable: "p"},
			{Name: "fk_b", Columns: []string{"x"}, ReferencedTable: "q"},
		},
	}}}
	applySkipForeignKeys(schema)
	if got := len(schema.Tables[0].Indexes); got != 1 {
		t.Fatalf("expected exactly 1 synthesized index for two same-column FKs, got %d", got)
	}
}

// TestApplySkipForeignKeys_UnsetIsByteIdentical is a guard: the transform is
// only ever entered behind the flag, so a schema with no FKs is untouched.
func TestApplySkipForeignKeys_NoFKsNoop(t *testing.T) {
	t.Parallel()
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Indexes: []*ir.Index{{Name: "t_idx", Columns: idxCols("a")}},
	}}}
	rep := applySkipForeignKeys(schema)
	if len(rep.Skipped) != 0 {
		t.Fatalf("no FKs should yield an empty report, got %d", len(rep.Skipped))
	}
	if len(schema.Tables[0].Indexes) != 1 {
		t.Fatalf("index set must be untouched, got %d", len(schema.Tables[0].Indexes))
	}
}

// TestSynthesizedFKIndexName covers the readable form and the hash fallback
// for over-long names (both must stay within the identifier ceiling and be
// deterministic).
func TestSynthesizedFKIndexName(t *testing.T) {
	t.Parallel()
	if got := synthesizedFKIndexName("orders", []string{"customer_id"}); got != "orders_fk_customer_id" {
		t.Fatalf("readable name = %q", got)
	}
	if got := synthesizedFKIndexName("orders", []string{"a", "b"}); got != "orders_fk_a_b" {
		t.Fatalf("composite name = %q", got)
	}
	// Over-long: must fall back to a stable, in-limit hash form.
	longTable := ""
	for i := 0; i < 80; i++ {
		longTable += "x"
	}
	got := synthesizedFKIndexName(longTable, []string{"col"})
	if len(got) > synthFKIndexNameMaxLen {
		t.Fatalf("overflow name %q is %d bytes, exceeds %d", got, len(got), synthFKIndexNameMaxLen)
	}
	if got != synthesizedFKIndexName(longTable, []string{"col"}) {
		t.Fatal("hash fallback must be deterministic")
	}
}

// TestMigratorValidate_SkipFKsAndDegradedFKsMutuallyExclusive pins the loud
// refusal when both opposite-intent FK flags are set.
func TestMigratorValidate_SkipFKsAndDegradedFKsMutuallyExclusive(t *testing.T) {
	t.Parallel()
	m := &Migrator{
		Source:           stubEngine{},
		Target:           stubEngine{},
		SourceDSN:        "src",
		TargetDSN:        "dst",
		SkipForeignKeys:  true,
		AllowDegradedFKs: true,
	}
	if err := m.validate(); err == nil {
		t.Fatal("expected a mutual-exclusion error, got nil")
	}
}

func indexNames(indexes []*ir.Index) []string {
	out := make([]string, len(indexes))
	for i, idx := range indexes {
		out[i] = idx.Name
	}
	return out
}
