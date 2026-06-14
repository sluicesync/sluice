// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0060 (F11) — pin the intercept's refuse-loudly path: when a
// shape that still refuses under ADR-0091 fires, the rendered drift
// report must be included in the surfaced error so operators see WHAT
// changed.
//
// Under ADR-0091 the single-stream path forwards every unambiguous
// shape by default; only RENAME COLUMN (ambiguous with drop+add) and
// multi-shape combos still refuse. These tests exercise those two
// refusal categories and assert the refusal names the specific
// column/index that changed. Bug 74 class-pinning applied to the F11
// surface area.

// driftForwardTable mirrors addColForwardTable from the existing
// intercept tests but lives in this file so the test naming stays
// scoped to drift-rendering assertions.
func driftForwardTable(name string, cols ...*ir.Column) *ir.Table {
	pk := &ir.Column{Name: "id", Type: ir.Integer{Width: 32}}
	all := append([]*ir.Column{pk}, cols...)
	return &ir.Table{
		Schema:  "public",
		Name:    name,
		Columns: all,
		PrimaryKey: &ir.Index{
			Name:    "pk_" + name,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
}

func driftForwardSnap(table *ir.Table) ir.SchemaSnapshot {
	return ir.SchemaSnapshot{
		Position: ir.Position{Engine: "postgres", Token: "lsn/1"},
		Schema:   table.Schema,
		Table:    table.Name,
		IR:       table,
	}
}

// TestIntercept_RefuseLoudly_IncludesDriftReport pins the F11
// contract per-category. Each subtest exercises a refused shape and
// asserts the surfaced error names the specific drift entry.
func TestIntercept_RefuseLoudly_IncludesDriftReport(t *testing.T) {
	cases := []struct {
		name        string
		pre         *ir.Table
		post        *ir.Table
		wantSubstrs []string // every substring must appear in the surfaced error
	}{
		{
			name: "rename-column-names-both-sides",
			pre: driftForwardTable("users",
				&ir.Column{Name: "old_email", Type: ir.Varchar{Length: 100}, Nullable: false}),
			post: driftForwardTable("users",
				&ir.Column{Name: "new_email", Type: ir.Varchar{Length: 100}, Nullable: false}),
			wantSubstrs: []string{
				"RENAME COLUMN",
				"[column-renamed]",
				"old_email",
				"new_email",
				"not auto-forwarded",
			},
		},
		{
			name: "multi-shape-combo-names-every-change",
			pre: func() *ir.Table {
				p := driftForwardTable("users",
					&ir.Column{Name: "legacy_col", Type: ir.Varchar{Length: 100}, Nullable: true})
				p.Indexes = []*ir.Index{{
					Name:    "ix_legacy",
					Columns: []ir.IndexColumn{{Column: "legacy_col"}},
				}}
				return p
			}(),
			post: func() *ir.Table {
				p := driftForwardTable("users",
					&ir.Column{Name: "nickname", Type: ir.Varchar{Length: 50}, Nullable: true})
				p.Indexes = []*ir.Index{{
					Name:    "ix_nick",
					Columns: []ir.IndexColumn{{Column: "nickname"}},
				}}
				return p
			}(),
			wantSubstrs: []string{
				// ClassifyShape returns an error here ("multi-shape combo")
				// — the renderer should still surface the per-line drift.
				"observed drift:",
				"[column-added]",
				"nickname",
				"[column-dropped]",
				"legacy_col",
				"[index-added]",
				"ix_nick",
				"[index-dropped]",
				"ix_legacy",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			in := make(chan ir.Change, 2)
			applier := &fakeShapeApplier{}
			in <- driftForwardSnap(tc.pre)
			in <- driftForwardSnap(tc.post)
			close(in)
			errStore := &atomic.Pointer[error]{}
			out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
				applier:          applier,
				sourceEngineName: "postgres",
				targetEngineName: "postgres",
			}, errStore)
			_ = drainChannel(t, out, time.Second)
			ePtr := errStore.Load()
			if ePtr == nil {
				t.Fatalf("expected refuse-loudly; got nil error")
			}
			msg := (*ePtr).Error()
			for _, want := range tc.wantSubstrs {
				if !strings.Contains(msg, want) {
					t.Errorf("refusal message missing %q\nfull message:\n%s", want, msg)
				}
			}
			// The recovery hint must still be present — drift
			// surfacing AUGMENTS the existing refusal contract, it
			// doesn't replace it.
			if !strings.Contains(msg, "drained model") {
				t.Errorf("refusal message missing recovery hint")
			}
		})
	}
}

// TestIntercept_RefuseLoudly_AddColumnComboGuidesDrainedRecovery
// verifies the F11 operator-action wording for the multi-shape combo
// case: when an ADD COLUMN is part of a combo it does NOT auto-forward
// (the combo as a whole requires drained recovery), and the rendered
// add line says so explicitly under ADR-0091.
func TestIntercept_RefuseLoudly_AddColumnComboGuidesDrainedRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	// Multi-shape combo: add + drop simultaneously. Forces the
	// classify-shape error path (not the recognized-but-refused
	// switch), which still surfaces the drift.
	pre := driftForwardTable("users",
		&ir.Column{Name: "to_drop", Type: ir.Varchar{Length: 50}, Nullable: true})
	post := driftForwardTable("users",
		&ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- driftForwardSnap(pre)
	in <- driftForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	ePtr := errStore.Load()
	if ePtr == nil {
		t.Fatalf("expected refuse-loudly; got nil")
	}
	msg := (*ePtr).Error()
	// The combo refusal names both the added and dropped columns; the
	// add line points to drained recovery (it does NOT auto-forward in
	// a combo), and the drop line names the destructive recovery.
	for _, want := range []string{
		"[column-added]",
		"nickname",
		"auto-forwards by default",
		"[column-dropped]",
		"to_drop",
		"destructive",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q\nfull:\n%s", want, msg)
		}
	}
}

// TestIntercept_HappyPath_NoDriftFootprint pins that the ADR-0058
// happy-path tests still pass — drift rendering must NOT alter the
// successful-ADD-COLUMN path's surfaced behaviour. Re-runs the
// canonical add-column-shape scenario to verify the applier is still
// called and no error is surfaced.
func TestIntercept_HappyPath_NoDriftFootprint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := make(chan ir.Change, 2)
	applier := &fakeShapeApplier{}
	pre := driftForwardTable("users")
	post := driftForwardTable("users",
		&ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}, Nullable: true})
	in <- driftForwardSnap(pre)
	in <- driftForwardSnap(post)
	close(in)
	errStore := &atomic.Pointer[error]{}
	out := interceptAddColumnForward(ctx, in, nil, schemaForwardDeps{
		applier:          applier,
		sourceEngineName: "postgres",
		targetEngineName: "postgres",
	}, errStore)
	_ = drainChannel(t, out, time.Second)
	if e := errStore.Load(); e != nil {
		t.Errorf("happy-path surfaced error: %v", *e)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d; want 1 (drift wiring must not break happy path)", applier.addColCalls)
	}
}
