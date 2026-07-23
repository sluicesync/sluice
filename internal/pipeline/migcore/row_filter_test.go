// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"strings"
	"testing"
)

// --- ApplyRowFilters loud-failure guard (Bug 201's surviving half) ---

// filterlessReader is a reader that does NOT implement ir.RowFilterSetter —
// the shape ApplyRowFilters must refuse when filters are configured, because
// a reader that ignores filters would silently copy every row.
type filterlessReader struct{}

// TestApplyRowFilters_ReaderWithoutSetterRefusesLoudly pins the guard the
// Bug 201 fix must NOT weaken: fixing the concurrent MySQL reader by
// implementing the setter is the correct move; loosening this gate (e.g.
// "warn and continue") would convert the loud refusal into the silent
// scope-escape it exists to prevent (SQLite/D1/flat-file sources still lack
// the setter in v1).
func TestApplyRowFilters_ReaderWithoutSetterRefusesLoudly(t *testing.T) {
	err := ApplyRowFilters(filterlessReader{}, map[string]string{"users": "id > 5"}, "sqlite")
	if err == nil {
		t.Fatal("ApplyRowFilters accepted a reader without ir.RowFilterSetter while filters are configured; want a loud refusal (silent unfiltered copy otherwise)")
	}
	if !strings.Contains(err.Error(), "sqlite") {
		t.Errorf("refusal does not name the engine: %v", err)
	}
	if !strings.Contains(err.Error(), "row-level filtering") {
		t.Errorf("refusal does not name the capability gap: %v", err)
	}
}

// TestApplyRowFilters_EmptyFiltersNoOp pins the common unfiltered path: an
// empty/nil map never touches the reader, so engines without the setter stay
// fully usable when no `--where` is configured.
func TestApplyRowFilters_EmptyFiltersNoOp(t *testing.T) {
	if err := ApplyRowFilters(filterlessReader{}, nil, "sqlite"); err != nil {
		t.Fatalf("nil filters: %v; want nil", err)
	}
	if err := ApplyRowFilters(filterlessReader{}, map[string]string{}, "sqlite"); err != nil {
		t.Fatalf("empty filters: %v; want nil", err)
	}
}
