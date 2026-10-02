// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTableFilterSluiceOwned pins the filter half of the custom-named
// heartbeat exclusion: a sluice-owned name is refused by Allows under every
// filter mode — including an include pattern that matches it, which the
// operator wrote for their own tables — matched case-insensitively, while
// every other table keeps its normal verdict. The receiver must not change
// (TableFilter is copied around by value), the prune must run for an
// otherwise-empty filter, a merge of engine defaults must keep it, and it is
// not a pattern, so it is never reported unmatched.
func TestTableFilterSluiceOwned(t *testing.T) {
	modes := map[string]TableFilter{
		"empty":        {},
		"include *":    {Include: []string{"*"}},
		"include ops_": {Include: []string{"ops_*", "users"}},
		"exclude":      {Exclude: []string{"audit_*"}},
	}
	for name, base := range modes {
		owned := base.WithSluiceOwnedTable("ops_HB")
		for _, spelling := range []string{"ops_HB", "ops_hb", "OPS_HB"} {
			if owned.Allows(spelling) {
				t.Errorf("%s: sluice-owned table %q is in scope", name, spelling)
			}
			if !owned.IsSluiceOwned(spelling) {
				t.Errorf("%s: IsSluiceOwned(%q) = false", name, spelling)
			}
		}
		if !owned.Allows("users") {
			t.Errorf("%s: marking a sluice-owned table changed the verdict for a user table", name)
		}
		if base.IsSluiceOwned("ops_hb") {
			t.Errorf("%s: WithSluiceOwnedTable mutated its receiver", name)
		}
		if owned.IsEmpty() {
			t.Errorf("%s: a filter refusing a table reports itself empty — the prune and push-down would skip it", name)
		}
		if got := owned.UnmatchedPatterns([]string{"users"}); len(got) > len(base.UnmatchedPatterns([]string{"users"})) {
			t.Errorf("%s: the sluice-owned name was reported as an unmatched pattern: %v", name, got)
		}
	}

	if f := (TableFilter{}).WithSluiceOwnedTable(""); !f.IsEmpty() {
		t.Error("an empty name must leave the filter unchanged")
	}

	// The cold-copy prune: an otherwise-empty filter must still drop it.
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}, {Name: "ops_hb"}}}
	if err := ApplyTableFilter(context.Background(), schema, TableFilter{}.WithSluiceOwnedTable("ops_hb")); err != nil {
		t.Fatalf("ApplyTableFilter: %v", err)
	}
	if len(schema.Tables) != 1 || schema.Tables[0].Name != "users" {
		t.Errorf("cold-copy prune kept %v, want only users", tableNames(schema))
	}

	// A merge of engine-default exclusions builds a new filter; it must
	// carry the sluice-owned set or a later merge would readmit the table.
	merged, added := EffectiveTableFilter(TableFilter{}.WithSluiceOwnedTable("ops_hb"), fakeDefaultExcluder{}, "dsn")
	if len(added) == 0 {
		t.Fatal("engine default not merged — the carry below would prove nothing")
	}
	if merged.Allows("ops_hb") {
		t.Error("EffectiveTableFilter dropped the sluice-owned exclusion")
	}
}

func tableNames(s *ir.Schema) []string {
	out := make([]string, 0, len(s.Tables))
	for _, tb := range s.Tables {
		out = append(out, tb.Name)
	}
	return out
}
