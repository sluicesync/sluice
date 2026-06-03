// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug 80 pin: sourceReadableColumns must filter BOTH IsGenerated and
// SluiceInjected. The reader's SELECT projection is built from this
// helper; including a SluiceInjected column (which exists on the
// schema-mutated *ir.Table but NOT on the source) was the v0.72.0
// regression that fired `SQLSTATE 42703 "column does not exist"` on
// every Shape-A bulk-copy.
//
// nonGeneratedColumns (used by writers) deliberately does NOT filter
// SluiceInjected — the writer MUST land the discriminator on the
// target. The two helpers are intentionally asymmetric.
func TestSourceReadableColumns_FiltersSluiceInjected(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "name", Type: ir.Text{}},
		{Name: "source_shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true},
	}

	got := sourceReadableColumns(cols)
	if len(got) != 2 {
		t.Fatalf("sourceReadableColumns: got %d cols; want 2 (id + name; source_shard_id is SluiceInjected and must be filtered)", len(got))
	}
	if got[0].Name != "id" || got[1].Name != "name" {
		t.Errorf("sourceReadableColumns: got %v; want [id, name]",
			[]string{got[0].Name, got[1].Name})
	}

	// nonGeneratedColumns MUST include the SluiceInjected column —
	// the writer's projection needs it. Pin the asymmetry.
	gotWriter := nonGeneratedColumns(cols)
	if len(gotWriter) != 3 {
		t.Errorf("nonGeneratedColumns (writer-side): got %d cols; want 3 (must include SluiceInjected for target writes)", len(gotWriter))
	}
}

// Bug 80 pin: buildSelect's projection must NOT include the
// SluiceInjected column. The SELECT goes against the SOURCE, which
// doesn't have the column.
func TestBuildSelect_OmitsSluiceInjected(t *testing.T) {
	table := &ir.Table{
		Schema: "public",
		Name:   "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Text{}},
			{Name: "source_shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true},
		},
	}
	got := buildSelect("public", table)
	// SELECT must include id + name, MUST NOT include source_shard_id.
	if !strings.Contains(got, `"id"`) || !strings.Contains(got, `"name"`) {
		t.Errorf("buildSelect did not include real source columns: %s", got)
	}
	if strings.Contains(got, "source_shard_id") {
		t.Errorf("buildSelect included SluiceInjected column — would crash with SQLSTATE 42703 on the source: %s", got)
	}
}
