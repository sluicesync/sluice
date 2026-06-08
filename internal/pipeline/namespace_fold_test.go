// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// foldEngine is a stub ir.Engine that also implements ir.NamespaceFolder
// with a configurable fold function, for exercising the multi-namespace
// fold-collision pre-flight (ADR-0075 resolved decision #1) without a
// live database.
type foldEngine struct {
	stubEngineBase
	fold func(name string) string
}

func (foldEngine) Name() string { return "foldStub" }

func (e foldEngine) FoldNamespace(_ context.Context, _, name string) (string, error) {
	if e.fold == nil {
		return name, nil
	}
	return e.fold(name), nil
}

// nonFoldEngine implements ir.Engine but NOT ir.NamespaceFolder, so the
// pre-flight must treat it as identity (no collision possible) — the PG-
// target case.
type nonFoldEngine struct{ stubEngineBase }

func (nonFoldEngine) Name() string { return "nonFoldStub" }

func TestPreflightNamespaceFoldCollisions(t *testing.T) {
	lower := strings.ToLower
	identity := func(s string) string { return s }

	cases := []struct {
		name     string
		target   ir.Engine
		selected []string
		wantErr  bool
		wantMsg  []string
	}{
		{
			name:     "non-folding target (PG) — no collision",
			target:   nonFoldEngine{},
			selected: []string{"Sales", "sales"}, // would collide IF folded
			wantErr:  false,
		},
		{
			name:     "folding target, distinct names — ok",
			target:   foldEngine{fold: lower},
			selected: []string{"sales", "billing", "warehouse"},
			wantErr:  false,
		},
		{
			name:     "folding target, identity fold (lct=0) — ok even with mixed case",
			target:   foldEngine{fold: identity},
			selected: []string{"Sales", "sales"},
			wantErr:  false,
		},
		{
			name:     "folding target, case collision — refuse loudly",
			target:   foldEngine{fold: lower},
			selected: []string{"Sales", "sales"},
			wantErr:  true,
			wantMsg:  []string{"Sales", "sales", "fold", "silently merge"},
		},
		{
			name:     "folding target, three-way with one collision",
			target:   foldEngine{fold: lower},
			selected: []string{"app", "billing", "APP"},
			wantErr:  true,
			wantMsg:  []string{"app", "APP"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := preflightNamespaceFoldCollisions(context.Background(), c.target, "tgt", c.selected)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected a fold-collision refusal; got nil")
				}
				for _, sub := range c.wantMsg {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("err %q missing substring %q", err.Error(), sub)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
