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
// (withHidden false) the hidden collation stays out; and (sixth review F6)
// a pair where only ONE side records the hidden collation — a vtgate read,
// a manifest from before the field — is not drift, the pinned wrapper
// included.
func TestRenderIndexColumns_HiddenCollation(t *testing.T) {
	part := func(expr, hidden string) *ir.Index {
		return &ir.Index{Name: "fk", Unique: true, Columns: []ir.IndexColumn{{Expression: expr, HiddenCollation: hidden}}}
	}
	differ := func(exp, act *ir.Index, withHidden bool) bool {
		return renderIndexColumns(exp, act, withHidden) != renderIndexColumns(act, exp, withHidden)
	}
	src := part("lower(k)", "utf8mb4_bin")
	for _, tc := range []struct {
		name       string
		exp, act   *ir.Index
		withHidden bool
		want       bool
	}{
		{"the writer's pinned key", src, part("(lower(k) collate utf8mb4_bin)", "utf8mb4_bin"), true, false},
		{"a stale key under another hidden collation", src, part("lower(k)", "utf8mb4_0900_ai_ci"), true, true},
		{"cross-family: the hidden collation stays out", src, part("lower(k)", "utf8mb4_0900_ai_ci"), false, false},
		{"F6: the target read through vtgate (no hidden collation)", src, part("lower(k)", ""), true, false},
		{"F6: the pinned key read through vtgate", src, part("(lower(k) collate utf8mb4_bin)", ""), true, false},
		{"F6: a manifest from before the field", part("lower(k)", ""), part("(lower(k) collate utf8mb4_bin)", "utf8mb4_bin"), true, false},
		{"F6: one-sided, but the expressions differ", src, part("upper(k)", ""), true, true},
	} {
		if got := differ(tc.exp, tc.act, tc.withHidden); got != tc.want {
			t.Errorf("%s: drift %v; want %v (%q vs %q)", tc.name, got, tc.want,
				renderIndexColumns(tc.exp, tc.act, tc.withHidden), renderIndexColumns(tc.act, tc.exp, tc.withHidden))
		}
	}
}
