// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestStripOuterCollate removes the writer's pinning wrapper — an outer
// COLLATE naming the part's own hidden collation, at any nesting of
// redundant parentheses (a canonical form both sides share) — and nothing inside a literal or under another collation.
func TestStripOuterCollate(t *testing.T) {
	for _, tc := range []struct{ expr, coll, want string }{
		{"(lower(k) collate utf8mb4_bin)", "utf8mb4_bin", "lower(k)"},
		{"((lower(k)) COLLATE utf8mb4_bin)", "utf8mb4_bin", "lower(k)"},
		{"((lower(k) collate utf8mb4_bin) collate utf8mb4_bin)", "utf8mb4_bin", "lower(k)"},
		{"(lower(k) collate utf8mb4_bin)", "utf8mb4_0900_ai_ci", "lower(k) collate utf8mb4_bin"}, // only redundant parentheses go
		{"lower(k)", "utf8mb4_bin", "lower(k)"},
		{"(a) || (b collate utf8mb4_bin)", "utf8mb4_bin", "(a) || (b collate utf8mb4_bin)"},
		{"(concat(k, ') collate utf8mb4_bin'))", "utf8mb4_bin", "concat(k, ') collate utf8mb4_bin')"}, // a literal is not a COLLATE
		{"(lower(k) collate utf8mb4_bin)", "", "(lower(k) collate utf8mb4_bin)"},
	} {
		if got := stripOuterCollate(tc.expr, tc.coll); got != tc.want {
			t.Errorf("stripOuterCollate(%q, %q) = %q; want %q", tc.expr, tc.coll, got, tc.want)
		}
	}
}

// TestRenderIndexColumns_HiddenCollation: the source key and the target key
// the writer rebuilt with its pinning wrapper render alike; a target whose
// hidden collation differs (a stale key) renders differently; cross-family
// (withHidden false) the hidden collation stays out.
func TestRenderIndexColumns_HiddenCollation(t *testing.T) {
	part := func(expr, hidden string) *ir.Index {
		return &ir.Index{Name: "fk", Unique: true, Columns: []ir.IndexColumn{{Expression: expr, HiddenCollation: hidden}}}
	}
	src := renderIndexColumns(part("lower(k)", "utf8mb4_bin"), true)
	if tgt := renderIndexColumns(part("(lower(k) collate utf8mb4_bin)", "utf8mb4_bin"), true); tgt != src {
		t.Errorf("the writer's pinned key renders %q; the source %q", tgt, src)
	}
	if stale := renderIndexColumns(part("lower(k)", "utf8mb4_0900_ai_ci"), true); stale == src {
		t.Errorf("a stale key under another hidden collation renders like the source: %q", stale)
	}
	if got := renderIndexColumns(part("lower(k)", "utf8mb4_bin"), false); got != "((lower(k)))" {
		t.Errorf("cross-family render %q carries the hidden collation", got)
	}
}
