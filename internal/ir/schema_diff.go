package ir

// Schema-level structural diff for `sluice schema diff` (ADR-0029).
// DiffSchemas is a pure function — no I/O, no engine knowledge, no
// rendering. The pipeline orchestrator wraps it with the source-side
// translation pass and the target-side schema read; engine writers
// turn the diff back into DDL suggestions.
//
// Comparison is by name (set semantics) for tables, columns, and
// indexes. Column ordering, index column ordering, and FK constraint
// names are deliberately out of scope (ADR-0029 §"Out of scope for
// v1") — operators don't reconcile drift on those without manual
// review anyway, and surfacing them as mismatches produces too much
// noise for too little operator value.

import (
	"fmt"
	"sort"
	"strings"
)

// SchemaDiff is the structural delta between two schemas. The naming
// follows ADR-0029: "Missing" entries are present in expected but
// absent on the actual side ("missing on target"); "Extra" entries
// are present on the actual side but absent from expected ("extra on
// target").
//
// All slices are sorted alphabetically for deterministic rendering;
// callers can rely on the order being stable across runs against the
// same inputs.
type SchemaDiff struct {
	TablesMissing    []string    `json:"tables_missing,omitempty"`
	TablesExtra      []string    `json:"tables_extra,omitempty"`
	TablesMismatched []TableDiff `json:"tables_mismatched,omitempty"`
}

// HasChanges reports whether any drift was detected. The CLI uses
// this to pick exit code 0 vs 1; same-shape unit-test predicate.
func (d SchemaDiff) HasChanges() bool {
	return len(d.TablesMissing) > 0 || len(d.TablesExtra) > 0 || len(d.TablesMismatched) > 0
}

// Summary returns a short human-readable rollup of the diff (e.g.
// "1 missing table, 2 type mismatches"). Used by the CLI's drift
// exit-code path to print a one-line summary on stderr alongside the
// non-zero exit. Returns "in sync" when there is no drift.
func (d SchemaDiff) Summary() string {
	parts := make([]string, 0, 8)
	add := func(n int, singular, plural string) {
		if n == 0 {
			return
		}
		if n == 1 {
			parts = append(parts, fmt.Sprintf("%d %s", n, singular))
			return
		}
		parts = append(parts, fmt.Sprintf("%d %s", n, plural))
	}
	add(len(d.TablesMissing), "missing table", "missing tables")
	add(len(d.TablesExtra), "extra table", "extra tables")
	var colMissing, colExtra, colMismatched, idxMissing, idxExtra int
	for _, td := range d.TablesMismatched {
		colMissing += len(td.ColumnsMissing)
		colExtra += len(td.ColumnsExtra)
		colMismatched += len(td.ColumnsMismatched)
		idxMissing += len(td.IndexesMissing)
		idxExtra += len(td.IndexesExtra)
	}
	add(colMissing, "missing column", "missing columns")
	add(colExtra, "extra column", "extra columns")
	add(colMismatched, "type mismatch", "type mismatches")
	add(idxMissing, "missing index", "missing indexes")
	add(idxExtra, "extra index", "extra indexes")
	if len(parts) == 0 {
		return "in sync"
	}
	return strings.Join(parts, ", ")
}

// TableDiff is a per-table delta. A table appears in
// SchemaDiff.TablesMismatched only when at least one of these slices
// is non-empty.
type TableDiff struct {
	Name              string       `json:"name"`
	ColumnsMissing    []string     `json:"columns_missing,omitempty"`
	ColumnsExtra      []string     `json:"columns_extra,omitempty"`
	ColumnsMismatched []ColumnDiff `json:"columns_mismatched,omitempty"`
	IndexesMissing    []string     `json:"indexes_missing,omitempty"`
	IndexesExtra      []string     `json:"indexes_extra,omitempty"`
}

// ColumnDiff captures a single column's expected-vs-actual mismatch.
// Only the fields that actually differ are populated on the
// expected/actual sides; equal fields are left zero.
//
// Type comparison uses the IR Type's String() rendering. Two columns
// with semantically-equivalent types (e.g. PG `varchar(45)` and
// `character varying(45)`) compare equal because both schema readers
// land them on the same `ir.Varchar{Length: 45}` value.
type ColumnDiff struct {
	Name             string `json:"name"`
	ExpectedType     string `json:"expected_type,omitempty"`
	ActualType       string `json:"actual_type,omitempty"`
	ExpectedNullable *bool  `json:"expected_nullable,omitempty"`
	ActualNullable   *bool  `json:"actual_nullable,omitempty"`
}

// DiffOptions configures DiffSchemas. The zero value is the strict
// default (every drift surfaces).
type DiffOptions struct {
	// IgnoreExtras drops "extra on target" entries from the result —
	// useful when the target hosts other applications' tables that
	// sluice should ignore. ADR-0029.
	IgnoreExtras bool
}

// DiffSchemas computes the structural delta between expected and
// actual. Both arguments must be non-nil; either may have zero tables
// (an empty schema is a valid input).
//
// The function is pure: same inputs → same output, no I/O. The
// orchestrator (internal/pipeline) handles reading the schemas,
// applying translation passes, and rendering the result.
func DiffSchemas(expected, actual *Schema, opts DiffOptions) SchemaDiff {
	var d SchemaDiff
	if expected == nil || actual == nil {
		return d
	}

	expByName := tablesByName(expected)
	actByName := tablesByName(actual)

	// Tables missing from actual.
	for name := range expByName {
		if _, ok := actByName[name]; !ok {
			d.TablesMissing = append(d.TablesMissing, name)
		}
	}
	sort.Strings(d.TablesMissing)

	// Tables extra on actual.
	if !opts.IgnoreExtras {
		for name := range actByName {
			if _, ok := expByName[name]; !ok {
				d.TablesExtra = append(d.TablesExtra, name)
			}
		}
		sort.Strings(d.TablesExtra)
	}

	// Per-table column/index diffs for tables present on both sides.
	commonNames := make([]string, 0, len(expByName))
	for name := range expByName {
		if _, ok := actByName[name]; ok {
			commonNames = append(commonNames, name)
		}
	}
	sort.Strings(commonNames)
	for _, name := range commonNames {
		td := diffTable(expByName[name], actByName[name], opts)
		if td.hasChanges() {
			d.TablesMismatched = append(d.TablesMismatched, td)
		}
	}
	return d
}

// hasChanges reports whether td carries any non-empty delta. Used by
// DiffSchemas to suppress empty TableDiff entries from the result.
func (td TableDiff) hasChanges() bool {
	return len(td.ColumnsMissing) > 0 ||
		len(td.ColumnsExtra) > 0 ||
		len(td.ColumnsMismatched) > 0 ||
		len(td.IndexesMissing) > 0 ||
		len(td.IndexesExtra) > 0
}

func diffTable(expected, actual *Table, opts DiffOptions) TableDiff {
	td := TableDiff{Name: expected.Name}

	expCols := columnsByName(expected)
	actCols := columnsByName(actual)

	for name := range expCols {
		if _, ok := actCols[name]; !ok {
			td.ColumnsMissing = append(td.ColumnsMissing, name)
		}
	}
	sort.Strings(td.ColumnsMissing)

	if !opts.IgnoreExtras {
		for name := range actCols {
			if _, ok := expCols[name]; !ok {
				td.ColumnsExtra = append(td.ColumnsExtra, name)
			}
		}
		sort.Strings(td.ColumnsExtra)
	}

	commonCols := make([]string, 0, len(expCols))
	for name := range expCols {
		if _, ok := actCols[name]; ok {
			commonCols = append(commonCols, name)
		}
	}
	sort.Strings(commonCols)
	for _, name := range commonCols {
		if cd, mismatched := diffColumn(expCols[name], actCols[name]); mismatched {
			td.ColumnsMismatched = append(td.ColumnsMismatched, cd)
		}
	}

	expIdx := indexNames(expected)
	actIdx := indexNames(actual)
	for name := range expIdx {
		if _, ok := actIdx[name]; !ok {
			td.IndexesMissing = append(td.IndexesMissing, name)
		}
	}
	sort.Strings(td.IndexesMissing)
	if !opts.IgnoreExtras {
		for name := range actIdx {
			if _, ok := expIdx[name]; !ok {
				td.IndexesExtra = append(td.IndexesExtra, name)
			}
		}
		sort.Strings(td.IndexesExtra)
	}

	return td
}

// diffColumn returns a ColumnDiff and a flag indicating whether any
// mismatch was found. Fields that match between expected and actual
// are left zero on the returned struct, so a renderer can emit only
// the changed fields without re-comparing.
func diffColumn(expected, actual *Column) (ColumnDiff, bool) {
	cd := ColumnDiff{Name: expected.Name}
	mismatched := false

	expType := typeString(expected.Type)
	actType := typeString(actual.Type)
	if expType != actType {
		cd.ExpectedType = expType
		cd.ActualType = actType
		mismatched = true
	}
	if expected.Nullable != actual.Nullable {
		exp := expected.Nullable
		act := actual.Nullable
		cd.ExpectedNullable = &exp
		cd.ActualNullable = &act
		mismatched = true
	}
	return cd, mismatched
}

// typeString returns the IR Type's stable rendering. Returns "<nil>"
// for a nil Type so a malformed Column produces a visible mismatch
// rather than panicking.
func typeString(t Type) string {
	if t == nil {
		return "<nil>"
	}
	return t.String()
}

func tablesByName(s *Schema) map[string]*Table {
	out := make(map[string]*Table, len(s.Tables))
	for _, t := range s.Tables {
		out[t.Name] = t
	}
	return out
}

func columnsByName(t *Table) map[string]*Column {
	out := make(map[string]*Column, len(t.Columns))
	for _, c := range t.Columns {
		out[c.Name] = c
	}
	return out
}

// indexNames returns the set of named indexes on t — both the primary
// key (when named) and secondary indexes. Indexes with empty Name
// (e.g. PG implicit PKs) are skipped: an unnamed index can't be
// matched across sides without column-set comparison, which v1
// deliberately doesn't do.
func indexNames(t *Table) map[string]struct{} {
	out := make(map[string]struct{}, len(t.Indexes)+1)
	if t.PrimaryKey != nil && t.PrimaryKey.Name != "" {
		out[t.PrimaryKey.Name] = struct{}{}
	}
	for _, idx := range t.Indexes {
		if idx.Name == "" {
			continue
		}
		out[idx.Name] = struct{}{}
	}
	return out
}
