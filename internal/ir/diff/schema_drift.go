// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// ADR-0060 — Per-table schema-drift report for CDC apply-side refuse-
// loudly messages.
//
// F11 (Reddit-research severity-A): when sluice refuses to forward a
// source DDL on the CDC stream, the operator needs to know WHAT
// changed — not just "schema change detected on table X". This file
// defines the pure-function diff evaluator over two snapshots of the
// same table (the pre-DDL `*Table` cached at cold-start / last
// boundary, and the post-DDL `*Table` observed from the CDC
// projection).
//
// DiffTable is intentionally narrower than [DiffSchemas]: it covers
// the single-table per-boundary delta and is intended for inclusion
// in refusal error messages, NOT for full schema reconciliation.
// [DiffSchemas] continues to own the `sluice schema diff` flow.
//
// Engine-neutrality: the diff structure compares IR struct fields
// only; no engine-specific knowledge leaks in. When a source's CDC
// projection drops information (e.g. pgoutput's RelationMessage
// doesn't carry attdefault; MySQL's TableMapEvent doesn't carry the
// generated-column expression), the corresponding drift-entry's
// "after" field is empty and the operator-action wording in
// [pipeline.RenderSchemaDriftReport] handles the gap. The ADR
// documents the projection-gap class.

import (
	"reflect"
	"sort"

	"sluicesync.dev/sluice/internal/ir"
)

// SchemaDriftReport is the structured per-table delta between two IR
// snapshots, suitable for inclusion in a refuse-loudly error message.
//
// "Before" entries existed on the pre-DDL side and were removed.
// "After" entries appeared on the post-DDL side. "Altered" entries
// were present on both sides but changed shape.
//
// All slices are deterministically ordered (alphabetical on the
// identifying name) so the rendered message is stable across runs —
// operators copy/paste these into tickets and chat, so deterministic
// output is load-bearing.
type SchemaDriftReport struct {
	// Schema is the source schema/database the table lives in.
	// Mirrors [ir.Table.Schema]; empty for MySQL.
	Schema string

	// Table is the source table name. Mirrors [ir.Table.Name].
	Table string

	// ColumnsAdded are the columns present in post but absent from
	// pre. Each carries the post-side Type, Nullable, and DEFAULT.
	ColumnsAdded []ColumnDriftEntry

	// ColumnsDropped are the columns present in pre but absent from
	// post. Each carries the pre-side Type, Nullable, and DEFAULT
	// (so the operator sees what they're losing).
	ColumnsDropped []ColumnDriftEntry

	// ColumnsAltered are columns present on both sides whose Type,
	// Nullable, Default, or generated-expression differ. The
	// ColumnDriftEntry's Before/After fields carry both sides; the
	// AlterKinds list names what specifically changed.
	ColumnsAltered []ColumnAlterEntry

	// ColumnsRenamed are pairs detected via the rename heuristic
	// (single added + single dropped with otherwise-identical
	// attributes). When the heuristic can't pair them (multiple
	// add/drops, attribute mismatch), the entries fall through to
	// ColumnsAdded / ColumnsDropped — see [diffRenameColumnIR].
	ColumnsRenamed []ColumnRenameEntry

	// IndexesAdded are named indexes present in post but absent from
	// pre. Unnamed indexes are excluded by design (same convention as
	// [DiffSchemas] and the pipeline shape classifier).
	IndexesAdded []IndexDriftEntry

	// IndexesDropped are named indexes present in pre but absent
	// from post.
	IndexesDropped []IndexDriftEntry

	// ChecksAdded are CHECK constraints (matched by Name) present in
	// post but absent from pre.
	ChecksAdded []CheckDriftEntry

	// ChecksDropped are CHECK constraints present in pre but absent
	// from post.
	ChecksDropped []CheckDriftEntry

	// ChecksAltered are CHECK constraints with the same Name but
	// different Expr text. Cross-engine textual comparison limits
	// apply (see [DiffSchemas]'s notes), but inside CDC-apply the
	// pre and post originate from the SAME source engine, so the
	// noise floor is much lower than `schema diff`.
	ChecksAltered []CheckAlterEntry

	// ForeignKeysAdded / Dropped / Altered mirror the CHECK slices for
	// foreign keys. Matching is by Name; unnamed FKs (engine-generated)
	// are matched by (Columns, ReferencedTable, ReferencedColumns) to
	// avoid false-positive churn on cross-version catalog renames.
	ForeignKeysAdded   []ForeignKeyDriftEntry
	ForeignKeysDropped []ForeignKeyDriftEntry
	ForeignKeysAltered []ForeignKeyAlterEntry
}

// ColumnDriftEntry describes a single added or dropped column. Used
// in both ColumnsAdded and ColumnsDropped — the difference is which
// side the column was observed on, encoded by which slice it lives in.
type ColumnDriftEntry struct {
	Name     string
	Type     string // rendered via Type.String(); "<nil>" if Type is nil
	Nullable bool
	Default  string // rendered via [renderDriftDefault]
}

// ColumnAlterEntry describes a column present on both sides whose
// shape changed. Before / After carry the same projection; AlterKinds
// lists the categorical changes that fired (so the renderer can
// produce "type changed: int → bigint, nullability changed: NOT NULL
// → NULL").
type ColumnAlterEntry struct {
	Name       string
	Before     ColumnDriftEntry
	After      ColumnDriftEntry
	AlterKinds []ColumnAlterKind
}

// ColumnAlterKind classifies which aspect of a column changed. A
// single ColumnAlterEntry may carry more than one kind (e.g. type +
// nullability change on the same boundary).
type ColumnAlterKind uint8

const (
	// ColumnAlterType — the rendered Type.String() differs.
	ColumnAlterType ColumnAlterKind = iota + 1

	// ColumnAlterNullable — Nullable differs.
	ColumnAlterNullable

	// ColumnAlterDefault — the rendered DEFAULT clause differs.
	// Note: the CDC projection often drops DEFAULTs (pgoutput's
	// RelationMessage; MySQL's TableMapEvent) — the renderer
	// suppresses false-positives by skipping entries where the
	// before and after both rendered to the empty-default sentinel.
	ColumnAlterDefault

	// ColumnAlterGeneratedExpr — the GeneratedExpr text differs.
	ColumnAlterGeneratedExpr
)

// String renders a ColumnAlterKind for log lines and refusal text.
func (k ColumnAlterKind) String() string {
	switch k {
	case ColumnAlterType:
		return "type"
	case ColumnAlterNullable:
		return "nullable"
	case ColumnAlterDefault:
		return "default"
	case ColumnAlterGeneratedExpr:
		return "generated-expr"
	}
	return "unknown"
}

// ColumnRenameEntry pairs a dropped column on the pre side with an
// added column on the post side when the heuristic detects an
// otherwise-identical attribute set (mirrors the pipeline shape
// classifier's RENAME COLUMN detection — see ADR-0054 v0.78.0).
type ColumnRenameEntry struct {
	OldName string
	NewName string
	Type    string // rendered post-side Type.String()
}

// IndexDriftEntry describes an added or dropped index. Columns is the
// rendered column list (e.g. "name, email") for operator-paste clarity.
type IndexDriftEntry struct {
	Name    string
	Columns string
	Unique  bool
}

// CheckDriftEntry describes an added or dropped CHECK constraint.
type CheckDriftEntry struct {
	Name string
	Expr string
}

// CheckAlterEntry describes a CHECK constraint present on both sides
// with a changed expression.
type CheckAlterEntry struct {
	Name       string
	BeforeExpr string
	AfterExpr  string
}

// ForeignKeyDriftEntry describes an added or dropped foreign key.
type ForeignKeyDriftEntry struct {
	Name              string
	Columns           string // joined "a, b"
	ReferencedTable   string
	ReferencedColumns string // joined "a, b"
}

// ForeignKeyAlterEntry describes a foreign key present on both sides
// with at least one changed attribute.
type ForeignKeyAlterEntry struct {
	Name   string
	Before ForeignKeyDriftEntry
	After  ForeignKeyDriftEntry
}

// HasChanges reports whether the report carries any drift entries.
// Used by callers to decide whether to include the rendered report in
// their refusal text. A no-change report indicates the two snapshots
// are structurally identical (modulo cosmetic differences ADR-0049's
// SchemaSignature.Equal filters); callers typically short-circuit.
func (r SchemaDriftReport) HasChanges() bool {
	return len(r.ColumnsAdded) > 0 ||
		len(r.ColumnsDropped) > 0 ||
		len(r.ColumnsAltered) > 0 ||
		len(r.ColumnsRenamed) > 0 ||
		len(r.IndexesAdded) > 0 ||
		len(r.IndexesDropped) > 0 ||
		len(r.ChecksAdded) > 0 ||
		len(r.ChecksDropped) > 0 ||
		len(r.ChecksAltered) > 0 ||
		len(r.ForeignKeysAdded) > 0 ||
		len(r.ForeignKeysDropped) > 0 ||
		len(r.ForeignKeysAltered) > 0 ||
		false
}

// DiffTable computes the per-table drift report between two IR
// snapshots. Both arguments may be nil — a nil pre is treated as
// "table absent before" (every post-column surfaces as added), and a
// nil post is treated as "table dropped" (every pre-column surfaces
// as dropped). A double-nil call returns the zero-value report.
//
// Comparison policy:
//
//   - Columns are matched by [ir.Column.Name]; same-name-different-type
//     surfaces as ColumnsAltered.
//   - Single-add + single-drop with otherwise-identical attributes
//     surfaces as ColumnsRenamed (see [diffRenameColumnIR]); the
//     dropped+added entries are then omitted from the add/drop slices
//     (no double-counting).
//   - Indexes are matched by [ir.Index.Name]; unnamed indexes are
//     skipped (same convention as [DiffSchemas]).
//   - CHECK constraints are matched by [ir.CheckConstraint.Name];
//     unnamed checks are skipped.
//   - Foreign keys with non-empty Name match by name; unnamed FKs
//     match by (Columns, ReferencedTable, ReferencedColumns) tuple.
//
// The function is pure: same inputs → same output, no I/O. Slice
// orderings are deterministic (alphabetical by identifying name).
func DiffTable(pre, post *ir.Table) SchemaDriftReport {
	var report SchemaDriftReport
	switch {
	case pre == nil && post == nil:
		return report
	case pre == nil:
		report.Schema = post.Schema
		report.Table = post.Name
	case post == nil:
		report.Schema = pre.Schema
		report.Table = pre.Name
	default:
		report.Schema = post.Schema
		report.Table = post.Name
	}

	preCols := columnsByNameDrift(pre)
	postCols := columnsByNameDrift(post)

	// Collect adds, drops, alters in one pass.
	var addedColEntries []ColumnDriftEntry
	var droppedColEntries []ColumnDriftEntry
	var alteredEntries []ColumnAlterEntry
	addedColLookup := map[string]*ir.Column{}
	droppedColLookup := map[string]*ir.Column{}

	if post != nil {
		for _, c := range post.Columns {
			if c == nil {
				continue
			}
			if _, ok := preCols[c.Name]; !ok {
				addedColEntries = append(addedColEntries, makeColumnDriftEntry(c))
				addedColLookup[c.Name] = c
			}
		}
	}
	if pre != nil {
		for _, c := range pre.Columns {
			if c == nil {
				continue
			}
			if _, ok := postCols[c.Name]; !ok {
				droppedColEntries = append(droppedColEntries, makeColumnDriftEntry(c))
				droppedColLookup[c.Name] = c
			}
		}
	}

	// Rename detection: exactly one add + one drop with otherwise-
	// equal attributes pairs as a rename.
	if oldName, newName, paired := diffRenameColumnIR(addedColLookup, droppedColLookup); paired {
		report.ColumnsRenamed = []ColumnRenameEntry{{
			OldName: oldName,
			NewName: newName,
			Type:    addedColEntries[0].Type, // exactly one entry in each
		}}
		addedColEntries = nil
		droppedColEntries = nil
	}

	// Alter detection: same-name columns with differing Type /
	// Nullable / Default / GeneratedExpr.
	if pre != nil && post != nil {
		for _, postCol := range post.Columns {
			if postCol == nil {
				continue
			}
			preCol, ok := preCols[postCol.Name]
			if !ok {
				continue
			}
			entry, changed := makeColumnAlterEntry(preCol, postCol)
			if changed {
				alteredEntries = append(alteredEntries, entry)
			}
		}
	}

	sort.Slice(addedColEntries, func(i, j int) bool {
		return addedColEntries[i].Name < addedColEntries[j].Name
	})
	sort.Slice(droppedColEntries, func(i, j int) bool {
		return droppedColEntries[i].Name < droppedColEntries[j].Name
	})
	sort.Slice(alteredEntries, func(i, j int) bool {
		return alteredEntries[i].Name < alteredEntries[j].Name
	})
	report.ColumnsAdded = addedColEntries
	report.ColumnsDropped = droppedColEntries
	report.ColumnsAltered = alteredEntries

	report.IndexesAdded, report.IndexesDropped = diffIndexesIR(pre, post)
	report.ChecksAdded, report.ChecksDropped, report.ChecksAltered = diffChecksIR(pre, post)
	report.ForeignKeysAdded, report.ForeignKeysDropped, report.ForeignKeysAltered = diffForeignKeysIR(pre, post)

	return report
}

// makeColumnDriftEntry projects an *ir.Column to the drift-entry
// shape. Used for both add and drop entries.
func makeColumnDriftEntry(c *ir.Column) ColumnDriftEntry {
	return ColumnDriftEntry{
		Name:     c.Name,
		Type:     typeStringDrift(c.Type),
		Nullable: c.Nullable,
		Default:  renderDriftDefault(c.Default),
	}
}

// makeColumnAlterEntry compares two same-name columns and returns
// (entry, true) when any tracked attribute differs.
func makeColumnAlterEntry(pre, post *ir.Column) (ColumnAlterEntry, bool) {
	entry := ColumnAlterEntry{
		Name:   post.Name,
		Before: makeColumnDriftEntry(pre),
		After:  makeColumnDriftEntry(post),
	}
	if entry.Before.Type != entry.After.Type {
		entry.AlterKinds = append(entry.AlterKinds, ColumnAlterType)
	}
	if entry.Before.Nullable != entry.After.Nullable {
		entry.AlterKinds = append(entry.AlterKinds, ColumnAlterNullable)
	}
	if entry.Before.Default != entry.After.Default {
		entry.AlterKinds = append(entry.AlterKinds, ColumnAlterDefault)
	}
	if pre.GeneratedExpr != post.GeneratedExpr {
		entry.AlterKinds = append(entry.AlterKinds, ColumnAlterGeneratedExpr)
	}
	return entry, len(entry.AlterKinds) > 0
}

// columnsByNameDrift indexes a table's columns by name. Returns an
// empty map when t is nil.
func columnsByNameDrift(t *ir.Table) map[string]*ir.Column {
	if t == nil {
		return map[string]*ir.Column{}
	}
	out := make(map[string]*ir.Column, len(t.Columns))
	for _, c := range t.Columns {
		if c == nil {
			continue
		}
		out[c.Name] = c
	}
	return out
}

// diffRenameColumnIR mirrors the pipeline.diffRenameColumn heuristic
// for the IR-level diff. Returns the (oldName, newName, true) pair
// when exactly one add + one drop with attribute-equality minus Name
// is detected.
//
// Mirrors pipeline.shard_consolidation_probe.go's rename detection —
// see ADR-0054 v0.78.0 for the locked heuristic and the
// indistinguishable-from-drop-add-same-attributes edge.
func diffRenameColumnIR(added, dropped map[string]*ir.Column) (oldName, newName string, ok bool) {
	if len(added) != 1 || len(dropped) != 1 {
		return "", "", false
	}
	var addCol, dropCol *ir.Column
	for _, c := range added {
		addCol = c
	}
	for _, c := range dropped {
		dropCol = c
	}
	if addCol == nil || dropCol == nil {
		return "", "", false
	}
	aCopy, dCopy := *addCol, *dropCol
	aCopy.Name = ""
	dCopy.Name = ""
	aCopy.SourceColumnType = nil
	dCopy.SourceColumnType = nil
	aCopy.SluiceInjected = false
	dCopy.SluiceInjected = false
	aCopy.Default = normalizeDefaultForRenameDrift(aCopy.Default)
	dCopy.Default = normalizeDefaultForRenameDrift(dCopy.Default)
	if !reflect.DeepEqual(aCopy, dCopy) {
		return "", "", false
	}
	return dropCol.Name, addCol.Name, true
}

// normalizeDefaultForRenameDrift folds the two encodings of "no
// default" — nil and ir.DefaultNone{} — into a single canonical nil.
// Same shape as pipeline.normalizeDefaultForRename; kept duplicated
// here so the ir package stays self-contained.
func normalizeDefaultForRenameDrift(d ir.DefaultValue) ir.DefaultValue {
	if d == nil {
		return nil
	}
	if _, isNone := d.(ir.DefaultNone); isNone {
		return nil
	}
	return d
}

// diffIndexesIR returns added and dropped named indexes. Unnamed
// indexes (Name == "") are skipped.
func diffIndexesIR(pre, post *ir.Table) (added, dropped []IndexDriftEntry) {
	preIdx := indexesByNameDrift(pre)
	postIdx := indexesByNameDrift(post)
	for name, idx := range postIdx {
		if _, ok := preIdx[name]; !ok {
			added = append(added, makeIndexDriftEntry(idx))
		}
	}
	for name, idx := range preIdx {
		if _, ok := postIdx[name]; !ok {
			dropped = append(dropped, makeIndexDriftEntry(idx))
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].Name < added[j].Name })
	sort.Slice(dropped, func(i, j int) bool { return dropped[i].Name < dropped[j].Name })
	return added, dropped
}

// indexesByNameDrift indexes a table's named indexes. Includes the
// primary key when [ir.Index.Name] is non-empty.
func indexesByNameDrift(t *ir.Table) map[string]*ir.Index {
	if t == nil {
		return map[string]*ir.Index{}
	}
	out := make(map[string]*ir.Index, len(t.Indexes)+1)
	if t.PrimaryKey != nil && t.PrimaryKey.Name != "" {
		out[t.PrimaryKey.Name] = t.PrimaryKey
	}
	for _, idx := range t.Indexes {
		if idx == nil || idx.Name == "" {
			continue
		}
		out[idx.Name] = idx
	}
	return out
}

// makeIndexDriftEntry projects an *ir.Index to the drift-entry shape.
func makeIndexDriftEntry(idx *ir.Index) IndexDriftEntry {
	cols := make([]string, 0, len(idx.Columns))
	for _, c := range idx.Columns {
		switch {
		case c.Column != "":
			cols = append(cols, c.Column)
		case c.Expression != "":
			cols = append(cols, "("+c.Expression+")")
		}
	}
	return IndexDriftEntry{
		Name:    idx.Name,
		Columns: joinCols(cols),
		Unique:  idx.Unique,
	}
}

// diffChecksIR returns added, dropped, and altered CHECK constraints
// (matched by Name). Unnamed CHECK constraints are skipped.
func diffChecksIR(pre, post *ir.Table) (added, dropped []CheckDriftEntry, altered []CheckAlterEntry) {
	preChecks := checksByNameDrift(pre)
	postChecks := checksByNameDrift(post)
	for name, c := range postChecks {
		preCheck, ok := preChecks[name]
		if !ok {
			added = append(added, CheckDriftEntry{Name: name, Expr: c.Expr})
			continue
		}
		if preCheck.Expr != c.Expr {
			altered = append(altered, CheckAlterEntry{
				Name:       name,
				BeforeExpr: preCheck.Expr,
				AfterExpr:  c.Expr,
			})
		}
	}
	for name, c := range preChecks {
		if _, ok := postChecks[name]; !ok {
			dropped = append(dropped, CheckDriftEntry{Name: name, Expr: c.Expr})
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].Name < added[j].Name })
	sort.Slice(dropped, func(i, j int) bool { return dropped[i].Name < dropped[j].Name })
	sort.Slice(altered, func(i, j int) bool { return altered[i].Name < altered[j].Name })
	return added, dropped, altered
}

// checksByNameDrift indexes named CHECK constraints.
func checksByNameDrift(t *ir.Table) map[string]*ir.CheckConstraint {
	if t == nil {
		return map[string]*ir.CheckConstraint{}
	}
	out := make(map[string]*ir.CheckConstraint, len(t.CheckConstraints))
	for _, c := range t.CheckConstraints {
		if c == nil || c.Name == "" {
			continue
		}
		out[c.Name] = c
	}
	return out
}

// diffForeignKeysIR returns added / dropped / altered foreign keys.
// Named FKs are matched by Name; unnamed FKs are matched by the
// (Columns, ReferencedTable, ReferencedColumns) tuple.
func diffForeignKeysIR(pre, post *ir.Table) (added, dropped []ForeignKeyDriftEntry, altered []ForeignKeyAlterEntry) {
	preFKs := foreignKeysByKey(pre)
	postFKs := foreignKeysByKey(post)
	for k, fk := range postFKs {
		preFK, ok := preFKs[k]
		if !ok {
			added = append(added, makeFKDriftEntry(fk))
			continue
		}
		if !fkAttributesEqual(preFK, fk) {
			altered = append(altered, ForeignKeyAlterEntry{
				Name:   fk.Name,
				Before: makeFKDriftEntry(preFK),
				After:  makeFKDriftEntry(fk),
			})
		}
	}
	for k, fk := range preFKs {
		if _, ok := postFKs[k]; !ok {
			dropped = append(dropped, makeFKDriftEntry(fk))
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].Name < added[j].Name })
	sort.Slice(dropped, func(i, j int) bool { return dropped[i].Name < dropped[j].Name })
	sort.Slice(altered, func(i, j int) bool { return altered[i].Name < altered[j].Name })
	return added, dropped, altered
}

// foreignKeysByKey indexes FKs by Name when set, else by the
// structural tuple. The empty-name tuple key prefixes with "@" so it
// can never collide with a named FK whose name doesn't start with @.
func foreignKeysByKey(t *ir.Table) map[string]*ir.ForeignKey {
	if t == nil {
		return map[string]*ir.ForeignKey{}
	}
	out := make(map[string]*ir.ForeignKey, len(t.ForeignKeys))
	for _, fk := range t.ForeignKeys {
		if fk == nil {
			continue
		}
		key := fk.Name
		if key == "" {
			key = "@" + joinCols(fk.Columns) + "->" + fk.ReferencedTable + "(" + joinCols(fk.ReferencedColumns) + ")"
		}
		out[key] = fk
	}
	return out
}

// fkAttributesEqual reports whether two foreign keys agree on every
// load-bearing attribute (columns, parent reference, referential
// actions). Name equality is established by the caller's keying.
func fkAttributesEqual(a, b *ir.ForeignKey) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !stringSlicesEqual(a.Columns, b.Columns) {
		return false
	}
	if a.ReferencedSchema != b.ReferencedSchema || a.ReferencedTable != b.ReferencedTable {
		return false
	}
	if !stringSlicesEqual(a.ReferencedColumns, b.ReferencedColumns) {
		return false
	}
	if a.OnDelete != b.OnDelete || a.OnUpdate != b.OnUpdate {
		return false
	}
	return true
}

// makeFKDriftEntry projects an *ir.ForeignKey to the drift-entry shape.
func makeFKDriftEntry(fk *ir.ForeignKey) ForeignKeyDriftEntry {
	return ForeignKeyDriftEntry{
		Name:              fk.Name,
		Columns:           joinCols(fk.Columns),
		ReferencedTable:   fk.ReferencedTable,
		ReferencedColumns: joinCols(fk.ReferencedColumns),
	}
}

// typeStringDrift renders an IR Type to its String() form, with a
// "<nil>" sentinel for nil Types (defensive — production IR never
// has nil Type, but hand-built test fixtures sometimes do).
func typeStringDrift(t ir.Type) string {
	if t == nil {
		return "<nil>"
	}
	return t.String()
}

// renderDriftDefault produces a stable textual rendering of a
// DefaultValue for drift comparison. Mirrors [renderDefault] in
// schema_diff.go but duplicates the helper so neither file depends
// on the other's package-internal naming.
func renderDriftDefault(d ir.DefaultValue) string {
	switch v := d.(type) {
	case nil, ir.DefaultNone:
		return "<none>"
	case ir.DefaultLiteral:
		return "'" + v.Value + "'"
	case ir.DefaultExpression:
		return v.Expr
	}
	return "<unknown>"
}

// joinCols joins a slice of column names with ", " — used for the
// rendered Columns / ReferencedColumns fields on index and FK
// drift entries.
func joinCols(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	out := cols[0]
	for i := 1; i < len(cols); i++ {
		out += ", " + cols[i]
	}
	return out
}

// stringSlicesEqual reports element-by-element equality.
func stringSlicesEqual(a, b []string) bool {
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
