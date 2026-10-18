// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"reflect"
	"testing"
)

// TestWithAddedTables is Bug 321 R7b's copy scope: an include-mode filter
// widened by the stream's live-added tables admits exactly those names (a
// name with glob metacharacters admits only itself), never reports them as
// unmatched patterns, and leaves the receiver alone. An exclude-mode or
// unfiltered filter is unchanged: the first already admits them unless an
// explicit exclusion names them (a contradiction the stream refuses), the
// second admits everything.
func TestWithAddedTables(t *testing.T) {
	base, err := NewTableFilter([]string{"kd", "app_*"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := base.WithAddedTables([]string{"users", "we*ird", "app_x", ""})
	for _, c := range []struct {
		table string
		want  bool
	}{
		{"kd", true},
		{"app_y", true},
		{"users", true},
		{"we*ird", true},
		{"weXird", false},
		{"orders", false},
		{"user", false},
	} {
		if got := f.Allows(c.table); got != c.want {
			t.Errorf("Allows(%q) = %v, want %v", c.table, got, c.want)
		}
	}
	if !reflect.DeepEqual(base.Include, []string{"kd", "app_*"}) || base.Allows("users") {
		t.Errorf("the receiver changed: %v", base.Include)
	}
	if got := f.UnmatchedPatterns([]string{"kd"}); !reflect.DeepEqual(got, []string{"app_*"}) {
		t.Errorf("UnmatchedPatterns = %v; want only the operator's app_* (added names are not the operator's)", got)
	}

	excl, err := NewTableFilter(nil, []string{"tmp_*"})
	if err != nil {
		t.Fatal(err)
	}
	if got := excl.WithAddedTables([]string{"tmp_keep", "users"}); got.Allows("tmp_keep") || !got.Allows("users") {
		t.Errorf("exclude mode changed: tmp_keep %v users %v", got.Allows("tmp_keep"), got.Allows("users"))
	}
	if got := (TableFilter{}).WithAddedTables([]string{"users"}); !got.IsEmpty() {
		t.Errorf("an unfiltered filter gained rules: %+v", got)
	}
}
