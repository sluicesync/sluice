// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/appliershared"
)

// TestRecordColumnsPresent_ByName pins R9e: the type check is cached as
// passed only when every record column exists by name. A count let
// live_added_tables stand in for a missing record column.
func TestRecordColumnsPresent_ByName(t *testing.T) {
	pending, established, dropped := appliershared.PendingAddedTablesColumn, appliershared.EstablishedTablesColumn,
		appliershared.DroppedTablesColumn
	for _, tc := range []struct {
		name  string
		found []string
		want  bool
	}{
		{"all three", []string{pending, established, dropped}, true},
		{"all three and live_added_tables", []string{pending, established, dropped, liveAddedTablesColumn}, true},
		{"live_added_tables standing in for dropped_tables", []string{pending, established, liveAddedTablesColumn}, false},
		{"one missing", []string{pending, established}, false},
		{"none", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			found := map[string]bool{}
			for _, c := range tc.found {
				found[c] = true
			}
			if got := recordColumnsPresent(found); got != tc.want {
				t.Fatalf("recordColumnsPresent(%v) = %v; want %v", tc.found, got, tc.want)
			}
		})
	}
}
