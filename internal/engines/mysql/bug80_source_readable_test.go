// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug 80 pin (MySQL side): sourceReadableColumns must filter BOTH
// IsGenerated and SluiceInjected. nonGeneratedColumns (writer-side)
// must NOT filter SluiceInjected — the discriminator MUST land on
// the target. The two helpers are intentionally asymmetric. See the
// PG-side test for the long-form rationale.

func TestSourceReadableColumns_FiltersSluiceInjected(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "name", Type: ir.Text{}},
		{Name: "source_shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true},
	}

	got := sourceReadableColumns(cols)
	if len(got) != 2 {
		t.Fatalf("sourceReadableColumns: got %d cols; want 2 (Bug 80 reader-side filter)", len(got))
	}
	if got[0].Name != "id" || got[1].Name != "name" {
		t.Errorf("sourceReadableColumns: got %v; want [id, name]",
			[]string{got[0].Name, got[1].Name})
	}

	gotWriter := nonGeneratedColumns(cols)
	if len(gotWriter) != 3 {
		t.Errorf("nonGeneratedColumns (writer-side): got %d cols; want 3 (must include SluiceInjected for target writes)", len(gotWriter))
	}
}

func TestBuildSelect_OmitsSluiceInjected(t *testing.T) {
	table := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Text{}},
			{Name: "source_shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true},
		},
	}
	got := buildSelect(table, false, "")
	if !strings.Contains(got, "`id`") || !strings.Contains(got, "`name`") {
		t.Errorf("buildSelect did not include real source columns: %s", got)
	}
	if strings.Contains(got, "source_shard_id") {
		t.Errorf("buildSelect included SluiceInjected column — would crash with Error 1054 on the source: %s", got)
	}
}
