// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ValidateRowFilterKeys checks every `--where TABLE=<predicate>` key names a
// real table in the (already table-scoped) source schema and returns the
// filter map RE-KEYED to the schema's canonical table casing, so the readers'
// exact-case `rowFilters[table.Name]` lookups always hit (audit F0-4/M-Q1).
//
// The plain migrate/verify/cold-start readers look the predicate up by exact
// table name; without this gate a typo (`--where user=...` missing the `s`) or
// a case-fold mismatch (`--where Users=...` against a lower-cased PG relname)
// finds no predicate, silently drops the WHERE, and copies/counts the WHOLE
// table — a scope-escape that `verify`, riding the same lookup, would then
// confirm as a false PASS. An unmatched key is refused loudly with a coded
// [sluicecode.CodeWhereFilterUnknownTable] error, mirroring the CDC sibling
// ([buildWhereCDCFilter]) and `--map`'s unknown-table rejection.
//
// Matching is case-insensitive. Two keys that fold to the same schema table
// are refused (the same duplicate hazard [parseWhereFilters] guards at the
// argv layer, but reachable here via case-variants that pass argv-level exact
// dedup). A nil/empty map returns (nil, nil) unchanged — the common
// unfiltered path never allocates.
func ValidateRowFilterKeys(schema *ir.Schema, filters map[string]string) (map[string]string, error) {
	if len(filters) == 0 {
		return filters, nil
	}
	// Canonical (schema-cased) name keyed by its lower-cased form.
	canon := make(map[string]string, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil {
			canon[strings.ToLower(t.Name)] = t.Name
		}
	}
	out := make(map[string]string, len(filters))
	for key, predicate := range filters {
		name, ok := canon[strings.ToLower(strings.TrimSpace(key))]
		if !ok {
			return nil, sluicecode.Wrap(
				sluicecode.CodeWhereFilterUnknownTable,
				"correct the --where table name (matching is case-insensitive) or remove the entry; pass the same --where to migrate and verify",
				fmt.Errorf(
					"--where names table %q, which is not in the source schema "+
						"(it may be misspelled or excluded by --include/--exclude-table); the readers match "+
						"the filter by exact table name, so an unmatched key would silently disable the filter "+
						"and copy/count the whole table",
					key,
				),
			)
		}
		if _, dup := out[name]; dup {
			return nil, sluicecode.Wrap(
				sluicecode.CodeWhereFilterUnknownTable,
				"give the table a single --where entry (combine conditions with AND)",
				fmt.Errorf(
					"--where names table %q more than once (case-insensitively); two predicates for one "+
						"table would silently keep only one",
					name,
				),
			)
		}
		out[name] = predicate
	}
	return out, nil
}

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
