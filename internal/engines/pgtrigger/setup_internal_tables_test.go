// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"strings"
	"testing"
)

// The capture function INSERTs into sluice_change_log, so a capture trigger on
// that table re-fires on every insert → unbounded recursion → PostgreSQL
// "stack depth limit exceeded", which fails EVERY write on EVERY triggered
// table. Both internal tables carry a PK, so any caller enumerating "all PK
// tables" sweeps them in on a re-run once they exist. Setup must filter them
// out regardless of caller hygiene (live regression: a migrator re-configure
// installed sluice_capture on sluice_change_log and blocked all source writes).
func TestFilterEngineInternalTables(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		in           []string
		wantKept     []string
		wantExcluded []string
	}{
		{
			name:         "mixed list drops both internal tables, preserves order",
			in:           []string{"orders", ChangeLogTable, "customers", ChangeLogMetaTable, "events"},
			wantKept:     []string{"orders", "customers", "events"},
			wantExcluded: []string{ChangeLogTable, ChangeLogMetaTable},
		},
		{
			name:         "only internal tables → nothing kept",
			in:           []string{ChangeLogTable, ChangeLogMetaTable},
			wantKept:     nil,
			wantExcluded: []string{ChangeLogTable, ChangeLogMetaTable},
		},
		{
			name:         "no internal tables → everything kept, nothing excluded",
			in:           []string{"orders", "customers"},
			wantKept:     []string{"orders", "customers"},
			wantExcluded: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kept, excluded := filterEngineInternalTables(tc.in)
			if !equalStrings(kept, tc.wantKept) {
				t.Errorf("kept = %v, want %v", kept, tc.wantKept)
			}
			if !equalStrings(excluded, tc.wantExcluded) {
				t.Errorf("excluded = %v, want %v", excluded, tc.wantExcluded)
			}
		})
	}
}

// Defense-in-depth: even if the filter were bypassed, the rendered DDL for the
// kept set must never attach a capture trigger to the change-log itself. Render
// the post-filter table set and assert no CREATE TRIGGER targets an internal
// table.
func TestRenderSetupDDL_NeverTriggersInternalTables(t *testing.T) {
	t.Parallel()
	in := []string{"orders", ChangeLogTable, ChangeLogMetaTable, "customers"}
	kept, _ := filterEngineInternalTables(in)
	specs := make([]tableTriggerSpec, len(kept))
	for i, name := range kept {
		specs[i] = tableTriggerSpec{Name: name, PKCols: []string{"id"}}
	}
	ddl := strings.Join(renderSetupDDL("public", specs, false, CapturePayloadFull), "\n")

	for _, internal := range []string{ChangeLogTable, ChangeLogMetaTable} {
		// "CREATE TRIGGER ... ON "public"."<internal>"" must not appear. The
		// change-log table legitimately appears in CREATE TABLE / INSERT INTO
		// statements, so match specifically on the trigger-target shape.
		needle := `ON "public"."` + internal + `" FOR EACH`
		if strings.Contains(ddl, needle) {
			t.Errorf("rendered DDL attaches a trigger to internal table %q (recursion risk):\nfound %q", internal, needle)
		}
	}
	// Sanity: it DOES create triggers on the real user tables.
	for _, user := range []string{"orders", "customers"} {
		if !strings.Contains(ddl, `ON "public"."`+user+`" FOR EACH`) {
			t.Errorf("rendered DDL is missing the capture trigger for user table %q", user)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
