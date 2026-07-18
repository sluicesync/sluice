// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ApplyRowFilters threads the operator's per-table `--where` predicates
// (ADR-0173 Phase 1) onto a freshly-opened SOURCE reader — an
// [ir.RowReader] on the migrate copy path, or a verify-side [ir.Verifier]
// SchemaReader — via the optional [ir.RowFilterSetter] surface. The map is
// keyed by SOURCE table name; the engine ANDs the matching predicate
// (always parenthesized) into its read/count SQL, so filtering happens on
// the source and only matching rows are copied/counted.
//
// An empty/nil map is a no-op — the common unfiltered path never touches
// the reader. When filters ARE configured but the reader does NOT implement
// the setter (SQLite/D1/flat-file sources in v1), it returns a loud refusal
// naming the engine rather than silently copying every row (the loud-
// failure tenet). engineName is used only for that refusal message.
func ApplyRowFilters(reader any, filters map[string]string, engineName string) error {
	if len(filters) == 0 {
		return nil
	}
	setter, ok := reader.(ir.RowFilterSetter)
	if !ok {
		return fmt.Errorf("--where: source engine %q does not support row-level filtering "+
			"(ADR-0173 Phase 1 covers mysql and postgres sources)", engineName)
	}
	setter.SetRowFilters(filters)
	return nil
}
