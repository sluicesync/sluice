// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug 82 (ADR-0048 Amendment 2026-05-22): MySQL Shape A targets with
// an AUTO_INCREMENT column in the source PK get the column demoted to
// non-leading position by the Shape A rewrite (discriminator goes
// first). MySQL requires every AUTO_INCREMENT column to be a leading
// key column, so CREATE TABLE was rejected with Error 1075. The fix:
// inlineAutoIncrementIndex now synthesizes a UNIQUE supporting index
// on the AUTO_INCREMENT column when it's in the PK but not leading,
// so MySQL's rule is satisfied via the secondary unique key instead
// of the PK lead.

// TestInlineAutoIncrementIndex_Bug82_Synthesis_PKNotLeading covers
// the Shape A rewrite case: rewritten PK has the AUTO_INCREMENT
// column trailing (discriminator leads); no operator-declared
// supporting index. The detector must synthesize one.
func TestInlineAutoIncrementIndex_Bug82_Synthesis_PKNotLeading(t *testing.T) {
	// Mimics the Shape A IR-pass output: id is AUTO_INCREMENT, but
	// the PK has been rewritten to (source_shard_id, id) — id is no
	// longer leading.
	table := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "name", Type: ir.Varchar{Length: 64}},
			{Name: "source_shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true},
		},
		PrimaryKey: &ir.Index{
			Columns: []ir.IndexColumn{
				{Column: "source_shard_id"},
				{Column: "id"},
			},
		},
	}
	got := inlineAutoIncrementIndex(table)
	if got == nil {
		t.Fatal("Bug 82: AUTO_INCREMENT in PK but not leading must trigger synthesis; got nil")
	}
	if !got.Unique {
		t.Errorf("synthesized index must be UNIQUE; got %+v", got)
	}
	if got.Name != "uq_widgets_id" {
		t.Errorf("synthesized index name = %q; want %q (uq_<table>_<col> convention)",
			got.Name, "uq_widgets_id")
	}
	if len(got.Columns) != 1 || got.Columns[0].Column != "id" {
		t.Errorf("synthesized index columns = %+v; want [id]", got.Columns)
	}
}

// TestEmitTableDef_Bug82_EndToEnd_AutoIncrementInRewrittenPK pins the
// CREATE TABLE output for the Shape A rewrite case: discriminator
// leads PK, AUTO_INCREMENT trails, synthesized UNIQUE KEY satisfies
// MySQL's auto-column-is-key rule.
func TestEmitTableDef_Bug82_EndToEnd_AutoIncrementInRewrittenPK(t *testing.T) {
	table := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "name", Type: ir.Varchar{Length: 64}},
			{Name: "source_shard_id", Type: ir.Varchar{Length: 64}, SluiceInjected: true},
		},
		PrimaryKey: &ir.Index{
			Columns: []ir.IndexColumn{
				{Column: "source_shard_id"},
				{Column: "id"},
			},
		},
	}
	got, err := emitTableDef(table)
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	// Three load-bearing assertions:
	//   (1) discriminator leads PK — DP-2 invariant
	//   (2) auto column has AUTO_INCREMENT modifier
	//   (3) the synthesized UNIQUE KEY appears inline
	wants := []string{
		"PRIMARY KEY (`source_shard_id`, `id`)",
		"`id` BIGINT AUTO_INCREMENT NOT NULL",
		"UNIQUE KEY `uq_widgets_id` (`id`)",
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("output missing %q\n--- got ---\n%s", w, got)
		}
	}
}

// TestInlineAutoIncrementIndex_Bug82_Regression_LeadingPKStillReturnsNil
// pins the regression guard: the standard `id BIGINT AUTO_INCREMENT
// PRIMARY KEY` shape (the common case, NOT Shape A) must still return
// nil (PK leads — no supporting index needed). Bug 82's synthesis
// must NOT fire on the common case.
func TestInlineAutoIncrementIndex_Bug82_Regression_LeadingPKStillReturnsNil(t *testing.T) {
	table := &ir.Table{
		Name: "widgets",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "name", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
	if got := inlineAutoIncrementIndex(table); got != nil {
		t.Errorf("standard `id AUTO_INCREMENT PK` must return nil (PK leads); got %+v "+
			"(if synthesis fires here, Phase 2 would double-create the index)", got)
	}
}

// TestInlineAutoIncrementIndex_Bug82_OperatorIndexWinsOverSynthesis
// pins precedence: an operator-declared supporting index (with the
// auto column leading) takes precedence over Bug 82's synthesis.
// This preserves GitHub #25's existing behavior for schemas where the
// auto column is NOT in the PK (the v0.49.0 case).
func TestInlineAutoIncrementIndex_Bug82_OperatorIndexWinsOverSynthesis(t *testing.T) {
	// auto col NOT in PK + operator declared a supporting unique →
	// return the operator's index, not a synthesis.
	table := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "uid", Type: ir.Varchar{Length: 32}},
			{Name: "seq", Type: ir.Integer{Width: 32, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "uid"}}},
		Indexes: []*ir.Index{
			{Name: "uq_events_seq", Unique: true, Columns: []ir.IndexColumn{{Column: "seq"}}},
		},
	}
	got := inlineAutoIncrementIndex(table)
	if got == nil || got.Name != "uq_events_seq" {
		t.Errorf("operator-declared supporting index must take precedence; got %+v", got)
	}
}

// TestInlineAutoIncrementIndex_Bug82_NoSynthesisWhenAutoColNotInPK
// pins scope: when the auto column is NOT in the PK and NOT in any
// operator-declared index, the detector returns nil (the pre-v0.49.0
// behavior). Bug 82's synthesis is SCOPED to the in-PK-but-not-leading
// case; broadening it would mask GitHub #25's no-supporting-index
// hazard for non-Shape-A schemas.
func TestInlineAutoIncrementIndex_Bug82_NoSynthesisWhenAutoColNotInPK(t *testing.T) {
	table := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "uid", Type: ir.Varchar{Length: 32}},
			{Name: "seq", Type: ir.Integer{Width: 32, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "uid"}}},
		// No operator-declared index on seq; auto col not in PK.
	}
	if got := inlineAutoIncrementIndex(table); got != nil {
		t.Errorf("auto col NOT in PK AND no supporting index should return nil "+
			"(pre-v0.49.0 behavior; surfaces MySQL's Error 1075 loudly); got %+v", got)
	}
}
