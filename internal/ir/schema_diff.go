// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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
//
// Default values, generated-column expressions, and CHECK constraints
// are compared at v0.8.0+. Cross-engine textual comparison of default
// expressions is fraught (`now()` vs `CURRENT_TIMESTAMP`,
// `gen_random_uuid()` vs `UUID()`, etc.). The diff handles this two
// ways: a tiny equivalence map (defaultEquivalents) suppresses
// false-positive drift on the known cross-engine pairs; mismatches
// outside that map surface as drift but are flagged
// LowConfidence=true so the renderer can soften the language. Future
// expansion of the equivalence map is additive.

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

	// ViewsMissing / ViewsExtra / ViewsMismatched mirror the table
	// drift slices for views. View support Phase 1 compares by name
	// (set semantics) for missing/extra, and by definition text for
	// mismatched. Cross-engine definition comparison is high-noise
	// (PG `pg_views.definition` reformats / canonicalises; MySQL
	// returns the source as the parser stored it), so a "mismatched
	// view" in cross-engine diffs surfaces low-confidence by design;
	// future Phase 3 translator work will tighten this.
	ViewsMissing    []string   `json:"views_missing,omitempty"`
	ViewsExtra      []string   `json:"views_extra,omitempty"`
	ViewsMismatched []ViewDiff `json:"views_mismatched,omitempty"`
}

// ViewDiff captures a single view's expected-vs-actual definition
// drift. Only used when a view with the same Name appears on both
// sides but the definition text differs.
type ViewDiff struct {
	Name               string `json:"name"`
	ExpectedDefinition string `json:"expected_definition,omitempty"`
	ActualDefinition   string `json:"actual_definition,omitempty"`

	// ExpectedMaterialized / ActualMaterialized are populated only
	// when the materialized-flag differs between sides (one side has
	// a regular view, the other a materialized view of the same name).
	// Both nil when both sides agree on the materialized-flag and
	// only the definition body diverges.
	ExpectedMaterialized *bool `json:"expected_materialized,omitempty"`
	ActualMaterialized   *bool `json:"actual_materialized,omitempty"`
}

// HasChanges reports whether any drift was detected. The CLI uses
// this to pick exit code 0 vs 1; same-shape unit-test predicate.
func (d SchemaDiff) HasChanges() bool {
	return len(d.TablesMissing) > 0 || len(d.TablesExtra) > 0 || len(d.TablesMismatched) > 0 ||
		len(d.ViewsMissing) > 0 || len(d.ViewsExtra) > 0 || len(d.ViewsMismatched) > 0
}

// Summary returns a short human-readable rollup of the diff (e.g.
// "1 missing table, 2 type mismatches"). Used by the CLI's drift
// exit-code path to print a one-line summary on stderr alongside the
// non-zero exit. Returns "in sync" when there is no drift.
func (d SchemaDiff) Summary() string {
	parts := make([]string, 0, 12)
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
	var (
		colMissing, colExtra, colMismatched int
		idxMissing, idxExtra                int
		chkMissing, chkExtra, chkMismatched int
		exMissing, exExtra, exMismatched    int
	)
	for _, td := range d.TablesMismatched {
		colMissing += len(td.ColumnsMissing)
		colExtra += len(td.ColumnsExtra)
		colMismatched += len(td.ColumnsMismatched)
		idxMissing += len(td.IndexesMissing)
		idxExtra += len(td.IndexesExtra)
		chkMissing += len(td.ChecksMissing)
		chkExtra += len(td.ChecksExtra)
		chkMismatched += len(td.ChecksMismatched)
		exMissing += len(td.ExcludesMissing)
		exExtra += len(td.ExcludesExtra)
		exMismatched += len(td.ExcludesMismatched)
	}
	add(colMissing, "missing column", "missing columns")
	add(colExtra, "extra column", "extra columns")
	add(colMismatched, "type mismatch", "type mismatches")
	add(idxMissing, "missing index", "missing indexes")
	add(idxExtra, "extra index", "extra indexes")
	add(chkMissing, "missing CHECK", "missing CHECKs")
	add(chkExtra, "extra CHECK", "extra CHECKs")
	add(chkMismatched, "CHECK mismatch", "CHECK mismatches")
	add(exMissing, "missing EXCLUDE", "missing EXCLUDEs")
	add(exExtra, "extra EXCLUDE", "extra EXCLUDEs")
	add(exMismatched, "EXCLUDE mismatch", "EXCLUDE mismatches")
	add(len(d.ViewsMissing), "missing view", "missing views")
	add(len(d.ViewsExtra), "extra view", "extra views")
	add(len(d.ViewsMismatched), "view mismatch", "view mismatches")
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
	ChecksMissing     []string     `json:"checks_missing,omitempty"`
	ChecksExtra       []string     `json:"checks_extra,omitempty"`
	ChecksMismatched  []CheckDiff  `json:"checks_mismatched,omitempty"`
	// EXCLUDE constraint deltas (ADR-0053). PG-only; MySQL sides always
	// have empty slices. Definition equality is byte-exact against PG's
	// canonical pg_get_constraintdef output.
	ExcludesMissing    []string      `json:"excludes_missing,omitempty"`
	ExcludesExtra      []string      `json:"excludes_extra,omitempty"`
	ExcludesMismatched []ExcludeDiff `json:"excludes_mismatched,omitempty"`
}

// ColumnDiff captures a single column's expected-vs-actual mismatch.
// Only the fields that actually differ are populated on the
// expected/actual sides; equal fields are left zero.
//
// Type comparison uses the IR Type's String() rendering. Two columns
// with semantically-equivalent types (e.g. PG `varchar(45)` and
// `character varying(45)`) compare equal because both schema readers
// land them on the same `ir.Varchar{Length: 45}` value.
//
// Default and generated-expression comparisons are textual. Known
// cross-engine equivalents (e.g. PG `now()` ↔ MySQL
// `CURRENT_TIMESTAMP`) are normalized through defaultEquivalents
// before comparison, suppressing false-positive drift on the most
// common patterns. Mismatches outside the equivalence map surface as
// drift with DefaultLowConfidence=true so the renderer can hedge —
// the IR can't know whether "1" and "1::int4" are the same default
// without engine-specific parsing.
type ColumnDiff struct {
	Name             string `json:"name"`
	ExpectedType     string `json:"expected_type,omitempty"`
	ActualType       string `json:"actual_type,omitempty"`
	ExpectedNullable *bool  `json:"expected_nullable,omitempty"`
	ActualNullable   *bool  `json:"actual_nullable,omitempty"`

	// ExpectedDefault / ActualDefault carry the rendered DEFAULT
	// clause from each side as a single string ("none", "1",
	// "now()", etc.); empty when the defaults match. The "none"
	// sentinel is used in rendering only — the JSON renderer
	// distinguishes "no default" from "literal default of empty
	// string" via the IR-level DefaultValue interface, but the
	// diff's textual comparison flattens to a stable string.
	ExpectedDefault string `json:"expected_default,omitempty"`
	ActualDefault   string `json:"actual_default,omitempty"`

	// DefaultLowConfidence is set when the only mismatch is on the
	// DEFAULT clause and the textual comparison may be a cross-
	// engine spelling difference rather than a real drift (e.g.
	// `1` vs `'1'`, `nextval('seq')` with no MySQL counterpart).
	// The renderer softens the language for these.
	DefaultLowConfidence bool `json:"default_low_confidence,omitempty"`

	// ExpectedGeneratedExpr / ActualGeneratedExpr carry the
	// generated-column expression text. Empty on either side when
	// the column is not generated on that side. Comparison is
	// verbatim string equality on the trimmed expression — the IR
	// already strips identifier quoting at the read boundary so
	// `(price * 1.1)` and `(price*1.1)` would both arrive
	// canonical, but cross-engine function-name differences (e.g.
	// `CONCAT_WS` vs `concat_ws` casing, `IF` vs `CASE`) surface
	// as a generated-expr mismatch by design — no equivalence
	// table for these.
	ExpectedGeneratedExpr string `json:"expected_generated_expr,omitempty"`
	ActualGeneratedExpr   string `json:"actual_generated_expr,omitempty"`

	// ExpectedCharset / ActualCharset carry the column's character-
	// set name (MySQL information_schema.columns.character_set_name).
	// Empty when both sides match or when the column isn't a
	// character type. Postgres has no per-column charset (server
	// encoding is database-wide) so these surface only on MySQL
	// sources / targets. The schema-diff renderer suppresses these
	// fields when --ignore-charset-collation is set.
	ExpectedCharset string `json:"expected_charset,omitempty"`
	ActualCharset   string `json:"actual_charset,omitempty"`

	// ExpectedCollation / ActualCollation carry the column's
	// collation name. Both engines populate it: MySQL from
	// information_schema.columns.collation_name, Postgres from
	// pg_attribute.attcollation → pg_collation.collname (only when
	// explicitly set per-column; database-default collations leave
	// it empty). Cross-engine comparison is high-noise — a MySQL
	// `utf8mb4_general_ci` and a PG `en_US.utf8` rarely match by
	// name even when the operator considers them equivalent — so
	// `--ignore-charset-collation` is the typical default for
	// cross-engine diffs.
	ExpectedCollation string `json:"expected_collation,omitempty"`
	ActualCollation   string `json:"actual_collation,omitempty"`
}

// CheckDiff captures a single CHECK constraint's expected-vs-actual
// expression mismatch. Only used when a constraint with the same
// Name appears on both sides but the body differs. CHECK constraints
// missing from one side surface in TableDiff.ChecksMissing /
// ChecksExtra (set semantics, by name).
type CheckDiff struct {
	Name         string `json:"name"`
	ExpectedExpr string `json:"expected_expr,omitempty"`
	ActualExpr   string `json:"actual_expr,omitempty"`
}

// ExcludeDiff captures a single EXCLUDE constraint's
// expected-vs-actual Definition mismatch. Only used when a constraint
// with the same Name appears on both sides but the Definition
// (pg_get_constraintdef byte-text) differs. EXCLUDE constraints
// missing from one side surface in TableDiff.ExcludesMissing /
// ExcludesExtra (set semantics, by name). ADR-0053.
type ExcludeDiff struct {
	Name               string `json:"name"`
	ExpectedDefinition string `json:"expected_definition,omitempty"`
	ActualDefinition   string `json:"actual_definition,omitempty"`
}

// DiffOptions configures DiffSchemas. The zero value is the strict
// default (every drift surfaces).
type DiffOptions struct {
	// IgnoreExtras drops "extra on target" entries from the result —
	// useful when the target hosts other applications' tables that
	// sluice should ignore. ADR-0029.
	IgnoreExtras bool

	// IgnoreCharsetCollation suppresses charset/collation drift
	// from the column-diff comparison. When set, mismatches that
	// would otherwise populate ExpectedCharset/ActualCharset/
	// ExpectedCollation/ActualCollation are dropped at compare
	// time, and column entries whose only drift was charset /
	// collation are not surfaced as mismatched. Useful for cross-
	// engine diffs where MySQL `utf8mb4_general_ci` and PG
	// `en_US.utf8` rarely match by name even when operators
	// consider them equivalent.
	IgnoreCharsetCollation bool
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

	diffViews(&d, expected, actual, opts)
	return d
}

// diffViews populates the view-related slices on d. Views are matched
// by name (set semantics, mirroring tables). Definition comparison is
// trim-and-equal — no SQL parser, no canonicalization. Cross-engine
// drift is therefore high-noise; the renderer hedges accordingly.
func diffViews(d *SchemaDiff, expected, actual *Schema, opts DiffOptions) {
	expByName := viewsByName(expected)
	actByName := viewsByName(actual)

	for name := range expByName {
		if _, ok := actByName[name]; !ok {
			d.ViewsMissing = append(d.ViewsMissing, name)
		}
	}
	sort.Strings(d.ViewsMissing)

	if !opts.IgnoreExtras {
		for name := range actByName {
			if _, ok := expByName[name]; !ok {
				d.ViewsExtra = append(d.ViewsExtra, name)
			}
		}
		sort.Strings(d.ViewsExtra)
	}

	common := make([]string, 0, len(expByName))
	for name := range expByName {
		if _, ok := actByName[name]; ok {
			common = append(common, name)
		}
	}
	sort.Strings(common)
	for _, name := range common {
		exp := expByName[name]
		act := actByName[name]
		expDef := strings.TrimSpace(exp.Definition)
		actDef := strings.TrimSpace(act.Definition)
		matFlagDiffers := exp.Materialized != act.Materialized
		if expDef == actDef && !matFlagDiffers {
			continue
		}
		vd := ViewDiff{Name: name}
		if expDef != actDef {
			vd.ExpectedDefinition = expDef
			vd.ActualDefinition = actDef
		}
		if matFlagDiffers {
			expM := exp.Materialized
			actM := act.Materialized
			vd.ExpectedMaterialized = &expM
			vd.ActualMaterialized = &actM
		}
		d.ViewsMismatched = append(d.ViewsMismatched, vd)
	}
}

// viewsByName indexes a schema's Views by name. Returns an empty map
// for a nil schema or a schema with no views.
func viewsByName(s *Schema) map[string]*View {
	if s == nil {
		return nil
	}
	out := make(map[string]*View, len(s.Views))
	for _, v := range s.Views {
		if v == nil || v.Name == "" {
			continue
		}
		out[v.Name] = v
	}
	return out
}

// hasChanges reports whether td carries any non-empty delta. Used by
// DiffSchemas to suppress empty TableDiff entries from the result.
func (td TableDiff) hasChanges() bool {
	return len(td.ColumnsMissing) > 0 ||
		len(td.ColumnsExtra) > 0 ||
		len(td.ColumnsMismatched) > 0 ||
		len(td.IndexesMissing) > 0 ||
		len(td.IndexesExtra) > 0 ||
		len(td.ChecksMissing) > 0 ||
		len(td.ChecksExtra) > 0 ||
		len(td.ChecksMismatched) > 0 ||
		len(td.ExcludesMissing) > 0 ||
		len(td.ExcludesExtra) > 0 ||
		len(td.ExcludesMismatched) > 0
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
		cd, mismatched := diffColumn(expCols[name], actCols[name])
		if !mismatched {
			continue
		}
		if opts.IgnoreCharsetCollation {
			cd, mismatched = stripCharsetCollation(cd)
			if !mismatched {
				continue
			}
		}
		td.ColumnsMismatched = append(td.ColumnsMismatched, cd)
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

	diffChecks(&td, expected, actual, opts)
	diffExcludes(&td, expected, actual, opts)

	return td
}

// diffChecks populates the CHECK-related slices on td. CHECK
// constraints are matched by Name (set semantics, same as indexes).
// Unnamed CHECKs are skipped: an anonymous constraint can't be
// matched across sides without expression-text comparison, which
// would produce false positives on cross-engine spelling differences.
func diffChecks(td *TableDiff, expected, actual *Table, opts DiffOptions) {
	expChecks := checksByName(expected)
	actChecks := checksByName(actual)

	for name := range expChecks {
		if _, ok := actChecks[name]; !ok {
			td.ChecksMissing = append(td.ChecksMissing, name)
		}
	}
	sort.Strings(td.ChecksMissing)

	if !opts.IgnoreExtras {
		for name := range actChecks {
			if _, ok := expChecks[name]; !ok {
				td.ChecksExtra = append(td.ChecksExtra, name)
			}
		}
		sort.Strings(td.ChecksExtra)
	}

	common := make([]string, 0, len(expChecks))
	for name := range expChecks {
		if _, ok := actChecks[name]; ok {
			common = append(common, name)
		}
	}
	sort.Strings(common)
	for _, name := range common {
		exp := expChecks[name]
		act := actChecks[name]
		expExpr := strings.TrimSpace(exp.Expr)
		actExpr := strings.TrimSpace(act.Expr)
		if expExpr == actExpr {
			continue
		}
		td.ChecksMismatched = append(td.ChecksMismatched, CheckDiff{
			Name:         name,
			ExpectedExpr: expExpr,
			ActualExpr:   actExpr,
		})
	}
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

	// Default-value comparison. Both sides are rendered to a stable
	// string form, normalized through defaultEquivalents, then
	// compared. A mismatch surfaces with both rendered forms so the
	// renderer can show "expected X / actual Y"; LowConfidence is
	// set unless one side is DefaultNone (a clear missing-default
	// drift the operator should reconcile regardless of dialect).
	expDefault := renderDefault(expected.Default)
	actDefault := renderDefault(actual.Default)
	if !defaultsEqual(expDefault, actDefault) {
		cd.ExpectedDefault = expDefault
		cd.ActualDefault = actDefault
		// "no default on one side" is high-confidence drift; only
		// expression-vs-expression mismatches are uncertain.
		_, expIsExpr := expected.Default.(DefaultExpression)
		_, actIsExpr := actual.Default.(DefaultExpression)
		if expIsExpr && actIsExpr {
			cd.DefaultLowConfidence = true
		}
		mismatched = true
	}

	// Generated-column expression comparison. Verbatim text after
	// trim — no equivalence map (cross-engine generated-expr
	// equivalents are too rare to enumerate).
	expGen := strings.TrimSpace(expected.GeneratedExpr)
	actGen := strings.TrimSpace(actual.GeneratedExpr)
	if expGen != actGen {
		cd.ExpectedGeneratedExpr = expGen
		cd.ActualGeneratedExpr = actGen
		mismatched = true
	}

	// Charset / collation comparison. Both fields ride on the IR's
	// character-type structs (Char, Varchar, Text); the helper below
	// extracts them and returns empty strings for non-character
	// types.
	//
	// **Empty-on-source means "no opinion".** When the source/expected
	// side has an empty charset (or empty collation), the comparison
	// is skipped for that field — any actual value is acceptable.
	// This avoids false-positive drift on three legitimate cases:
	//   1. Source is Postgres (PG doesn't expose per-column charset
	//      via information_schema; collation can be empty for non-
	//      explicit columns).
	//   2. Source column is non-character (Integer, JSON, etc.) —
	//      both sides are empty and the skip is a no-op.
	//   3. Source column was retargeted from a PG-native type (UUID,
	//      Inet, Macaddr, ...) by translate.RetargetForEngine: the
	//      retarget picks a Char/Varchar shape but doesn't carry
	//      charset/collation since the source type never had one.
	// When the source DOES have a charset/collation, the comparison is
	// strict-string — `utf8mb4` vs `latin1` surfaces as drift, and
	// the operator suppresses with `--ignore-charset-collation` if
	// the divergence is intentional. v0.11.2 fix; pre-fix every
	// PG→MySQL diff on retargeted columns surfaced as bogus drift.
	expCharset, expCollation := charsetCollationOf(expected.Type)
	actCharset, actCollation := charsetCollationOf(actual.Type)
	if expCharset != "" && expCharset != actCharset {
		cd.ExpectedCharset = expCharset
		cd.ActualCharset = actCharset
		mismatched = true
	}
	if expCollation != "" && expCollation != actCollation {
		cd.ExpectedCollation = expCollation
		cd.ActualCollation = actCollation
		mismatched = true
	}

	return cd, mismatched
}

// stripCharsetCollation clears the four charset/collation fields on
// a ColumnDiff and reports whether any other drift remains. Used
// under DiffOptions.IgnoreCharsetCollation to suppress charset /
// collation drift at compare time: column entries whose only
// mismatch was on those fields drop out of ColumnsMismatched
// entirely, while entries with additional drift (type, nullability,
// default, generated expression) keep surfacing minus the
// charset/collation noise.
func stripCharsetCollation(cd ColumnDiff) (ColumnDiff, bool) {
	cd.ExpectedCharset = ""
	cd.ActualCharset = ""
	cd.ExpectedCollation = ""
	cd.ActualCollation = ""
	hasOther := cd.ExpectedType != "" || cd.ActualType != "" ||
		cd.ExpectedNullable != nil || cd.ActualNullable != nil ||
		cd.ExpectedDefault != "" || cd.ActualDefault != "" ||
		cd.ExpectedGeneratedExpr != "" || cd.ActualGeneratedExpr != ""
	return cd, hasOther
}

// charsetCollationOf extracts the Charset and Collation fields off
// any of the IR's character-type structs (ir.Char, ir.Varchar,
// ir.Text). Returns ("", "") for non-character types and for
// character types where neither field is set. The diff comparison
// uses string equality on the returned values, so an unset field
// (zero value) equals another unset field — only when both sides
// have a value and they differ does drift surface.
func charsetCollationOf(t Type) (charset, collation string) {
	switch v := t.(type) {
	case Char:
		return v.Charset, v.Collation
	case Varchar:
		return v.Charset, v.Collation
	case Text:
		return v.Charset, v.Collation
	}
	return "", ""
}

// renderDefault produces a stable textual rendering of a
// DefaultValue for use in diff comparison and output. The "<none>"
// sentinel distinguishes "no DEFAULT clause" from "DEFAULT ”" (the
// empty literal). Callers use defaultsEqual rather than direct
// string comparison so the equivalence map kicks in.
func renderDefault(d DefaultValue) string {
	switch v := d.(type) {
	case nil, DefaultNone:
		return "<none>"
	case DefaultLiteral:
		return "'" + v.Value + "'"
	case DefaultExpression:
		return v.Expr
	}
	return "<unknown>"
}

// defaultsEqual reports whether two rendered defaults represent the
// same value, accounting for the cross-engine equivalence map.
// Comparison is case-insensitive and whitespace-tolerant on the
// expression form; literal defaults compare verbatim (changing a
// literal value is real drift, not a dialect difference).
func defaultsEqual(a, b string) bool {
	if a == b {
		return true
	}
	if defaultExpressionsEquivalent(a, b) {
		return true
	}
	return false
}

// defaultExpressionsEquivalent checks whether two default-expression
// strings are known cross-engine equivalents. Returns false if either
// side looks like a literal default (single-quoted) or the "<none>"
// sentinel — those fall back to verbatim equality in defaultsEqual.
//
// The check is symmetric: lookup with a→b and b→a so the orchestrator
// doesn't care which side is the source and which is the target.
func defaultExpressionsEquivalent(a, b string) bool {
	if isLiteralOrNone(a) || isLiteralOrNone(b) {
		return false
	}
	na := normalizeDefaultExpr(a)
	nb := normalizeDefaultExpr(b)
	if na == nb {
		return true
	}
	if equivs, ok := defaultEquivalents[na]; ok {
		for _, e := range equivs {
			if e == nb {
				return true
			}
		}
	}
	if equivs, ok := defaultEquivalents[nb]; ok {
		for _, e := range equivs {
			if e == na {
				return true
			}
		}
	}
	return false
}

// isLiteralOrNone reports whether s is a literal-default rendering
// ('foo') or the no-default sentinel. Used to keep the equivalence
// map from accidentally treating literal '1' as equivalent to expr 1.
func isLiteralOrNone(s string) bool {
	if s == "<none>" {
		return true
	}
	return strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")
}

// normalizeDefaultExpr returns a comparison-friendly form of an
// expression: lowercased, trimmed, internal whitespace collapsed,
// and spaces stripped around parentheses and commas so
// `current_timestamp ( 6 )` and `CURRENT_TIMESTAMP(6)` compare
// equal. Keep this a pure string-shape pass; semantic normalization
// (parsing argument lists, etc.) is deliberately out of scope.
func normalizeDefaultExpr(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	// Collapse internal whitespace runs to single spaces.
	var sb strings.Builder
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' {
			if !prevSpace {
				sb.WriteByte(' ')
			}
			prevSpace = true
			continue
		}
		prevSpace = false
		sb.WriteRune(r)
	}
	// Strip spaces adjacent to parentheses and commas — purely
	// syntactic noise that varies by hand-written DDL convention.
	out := sb.String()
	for _, pair := range [...][2]string{
		{" (", "("},
		{"( ", "("},
		{" )", ")"},
		{") ", ")"},
		{" ,", ","},
		{", ", ","},
	} {
		out = strings.ReplaceAll(out, pair[0], pair[1])
	}
	return out
}

// defaultEquivalents lists known cross-engine default-expression
// equivalents. Keys and values are the result of normalizeDefaultExpr
// (lowercased, whitespace-collapsed). Lookups go both directions so
// callers don't need to know which side is the source.
//
// Conservative by design — adding wrong entries here suppresses real
// drift, so each new pair should be backed by a documented engine-
// reader behaviour. Future expansion is additive.
//
// Notable omissions:
//
//   - PG `nextval('seq')` has no MySQL counterpart (auto-increment
//     is a column attribute, not a default expression).
//   - PG `gen_random_uuid()` and MySQL `UUID()` produce
//     incompatible binary representations even when both columns
//     are CHAR(36); the equivalence is semantic but the wire-form
//     drift is real, so we surface it as low-confidence drift
//     rather than silently equate.
var defaultEquivalents = map[string][]string{
	"now()": {
		"current_timestamp",
		"current_timestamp()",
		"current_timestamp(6)",
	},
	"current_timestamp": {
		"now()",
		"current_timestamp()",
	},
	"current_date": {
		"current_date()",
	},
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

// checksByName indexes a table's CheckConstraints by name. Unnamed
// constraints (Name == "") are skipped — see diffChecks for why.
func checksByName(t *Table) map[string]*CheckConstraint {
	out := make(map[string]*CheckConstraint, len(t.CheckConstraints))
	for _, c := range t.CheckConstraints {
		if c == nil || c.Name == "" {
			continue
		}
		out[c.Name] = c
	}
	return out
}

// excludesByName indexes a table's ExcludeConstraints by name.
// Unnamed entries (Name == "") are skipped on the same rationale as
// checksByName — the PG schema reader always assigns a name (system-
// generated when not explicitly named at source), so an unnamed
// entry would mean a corrupt or hand-built IR. ADR-0053.
func excludesByName(t *Table) map[string]*ExcludeConstraint {
	out := make(map[string]*ExcludeConstraint, len(t.ExcludeConstraints))
	for _, c := range t.ExcludeConstraints {
		if c == nil || c.Name == "" {
			continue
		}
		out[c.Name] = c
	}
	return out
}

// diffExcludes populates the EXCLUDE-related slices on td. EXCLUDE
// constraints are matched by Name (set semantics, same as CHECKs).
// Definition equality is byte-exact: PG's pg_get_constraintdef is
// canonicalized server-side, so two structurally-identical
// constraints produce byte-identical text — any divergence (even
// whitespace) is treated as a real change (operator hand-edited the
// target / target PG version normalised differently). ADR-0053.
func diffExcludes(td *TableDiff, expected, actual *Table, opts DiffOptions) {
	expExcl := excludesByName(expected)
	actExcl := excludesByName(actual)

	for name := range expExcl {
		if _, ok := actExcl[name]; !ok {
			td.ExcludesMissing = append(td.ExcludesMissing, name)
		}
	}
	sort.Strings(td.ExcludesMissing)

	if !opts.IgnoreExtras {
		for name := range actExcl {
			if _, ok := expExcl[name]; !ok {
				td.ExcludesExtra = append(td.ExcludesExtra, name)
			}
		}
		sort.Strings(td.ExcludesExtra)
	}

	common := make([]string, 0, len(expExcl))
	for name := range expExcl {
		if _, ok := actExcl[name]; ok {
			common = append(common, name)
		}
	}
	sort.Strings(common)
	for _, name := range common {
		exp := expExcl[name]
		act := actExcl[name]
		expDef := strings.TrimSpace(exp.Definition)
		actDef := strings.TrimSpace(act.Definition)
		if expDef == actDef {
			continue
		}
		td.ExcludesMismatched = append(td.ExcludesMismatched, ExcludeDiff{
			Name:               name,
			ExpectedDefinition: expDef,
			ActualDefinition:   actDef,
		})
	}
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
