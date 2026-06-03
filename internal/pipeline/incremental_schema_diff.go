// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Phase 3 schema-delta diff: turns a (before, after) pair of source
// schemas into the [ir.SchemaDeltaEntry] slice that lands on the
// incremental manifest. Distinct from [ir.DiffSchemas] (which powers
// `sluice schema diff` and produces a richer drift shape with names
// + low-confidence flags) — incrementals only need the simple
// "added / dropped / altered" shape so chain-restore can replay the
// DDL.
//
// Comparison is structural: a column added/removed/retyped surfaces
// as an alter_table delta; a whole-table add/drop surfaces as the
// matching kind. Column-rename detection is deliberately out of scope
// for v1 (it's ambiguous without operator intent — an "ADD col_b /
// DROP col_a" diff could be either rename or independent edits).
// Operators with rename-heavy chains take a fresh full per the
// design doc's "force fresh full" recommendation.

import (
	"reflect"

	"sluicesync.dev/sluice/internal/ir"
)

// diffSchemas returns the structural delta from before to after.
// Empty slice (not nil) when the two schemas are identical.
//
// The order of returned entries is: drops first (so a restore-side
// applier can DROP before CREATE if the same table name appears on
// both sides — a v1 unsupported shape, but the order keeps the
// applier safe), then adds, then alters. Within each group, ordering
// is the after-schema's table order (or before-schema's for drops),
// which is stable across runs.
func diffSchemas(before, after *ir.Schema) []*ir.SchemaDeltaEntry {
	var (
		drops  []*ir.SchemaDeltaEntry
		adds   []*ir.SchemaDeltaEntry
		alters []*ir.SchemaDeltaEntry
	)

	beforeIdx := indexTablesByQualifiedName(before)
	afterIdx := indexTablesByQualifiedName(after)

	// Drops: in before, not in after.
	if before != nil {
		for _, t := range before.Tables {
			key := qualifiedTableKey(t.Schema, t.Name)
			if _, ok := afterIdx[key]; ok {
				continue
			}
			drops = append(drops, &ir.SchemaDeltaEntry{
				Kind:   ir.SchemaDeltaDropTable,
				Schema: t.Schema,
				Table:  t.Name,
				Before: t,
			})
		}
	}

	// Adds + Alters: walk the after-schema in order so the resulting
	// slice's table order matches the post-window source state.
	if after != nil {
		for _, tAfter := range after.Tables {
			key := qualifiedTableKey(tAfter.Schema, tAfter.Name)
			tBefore, ok := beforeIdx[key]
			if !ok {
				adds = append(adds, &ir.SchemaDeltaEntry{
					Kind:   ir.SchemaDeltaAddTable,
					Schema: tAfter.Schema,
					Table:  tAfter.Name,
					After:  tAfter,
				})
				continue
			}
			if !tablesEqual(tBefore, tAfter) {
				alters = append(alters, &ir.SchemaDeltaEntry{
					Kind:   ir.SchemaDeltaAlterTable,
					Schema: tAfter.Schema,
					Table:  tAfter.Name,
					Before: tBefore,
					After:  tAfter,
				})
			}
		}
	}

	out := make([]*ir.SchemaDeltaEntry, 0, len(drops)+len(adds)+len(alters))
	out = append(out, drops...)
	out = append(out, adds...)
	out = append(out, alters...)
	return out
}

// indexTablesByQualifiedName returns a "schema.name" → table map for
// fast lookup during diffSchemas.
func indexTablesByQualifiedName(s *ir.Schema) map[string]*ir.Table {
	if s == nil {
		return nil
	}
	out := make(map[string]*ir.Table, len(s.Tables))
	for _, t := range s.Tables {
		if t == nil {
			continue
		}
		out[qualifiedTableKey(t.Schema, t.Name)] = t
	}
	return out
}

// qualifiedTableKey is the local cousin of [manifestTableKey], named
// to avoid the "manifest" prefix at the source-schema diff site.
func qualifiedTableKey(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// tablesEqual compares two table values for structural equality. The
// comparison covers: column count, per-column name + type-string +
// nullable + IsGenerated, primary-key column list, and index name set.
//
// Deliberately coarse — the chain-restore applier reads the After
// shape verbatim and emits its own ALTER, so a column-comment-only
// drift that produced an alter_table entry is harmless. The opposite
// (subtle drift the diff misses) would matter; the comparison
// includes column types via Type.String() which catches the common
// shape changes (TINYINT → BOOLEAN, VARCHAR(50) → VARCHAR(100), etc.)
// and the comment fields are compared via reflect.DeepEqual on the
// table struct as a whole.
func tablesEqual(a, b *ir.Table) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Schema != b.Schema || a.Name != b.Name {
		return false
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i, ca := range a.Columns {
		cb := b.Columns[i]
		if !columnsEqual(ca, cb) {
			return false
		}
	}
	if !indicesEqual(a.PrimaryKey, b.PrimaryKey) {
		return false
	}
	if len(a.Indexes) != len(b.Indexes) {
		return false
	}
	for i, ia := range a.Indexes {
		if !indicesEqual(ia, b.Indexes[i]) {
			return false
		}
	}
	return true
}

func columnsEqual(a, b *ir.Column) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Name != b.Name {
		return false
	}
	// Type.String() captures width / nullable / etc. on every concrete
	// IR type — the same property the cross-engine diff relies on.
	if typeString(a.Type) != typeString(b.Type) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if a.IsGenerated() != b.IsGenerated() {
		return false
	}
	// Default values compared via canonicalized form: nil and
	// DefaultNone{} are operationally equivalent (both mean "no
	// default"), but reflect.DeepEqual distinguishes them. The IR's
	// Column.UnmarshalJSON normalises absent defaults to
	// DefaultNone{}, so a parent manifest round-tripped through JSON
	// arrives with DefaultNone{} where the in-memory source struct
	// might have nil. Treat them as equal here.
	if !defaultsEqual(a.Default, b.Default) {
		return false
	}
	return true
}

// defaultsEqual compares two DefaultValue interfaces, treating nil
// and DefaultNone{} as equivalent.
func defaultsEqual(a, b ir.DefaultValue) bool {
	an := isNoneDefault(a)
	bn := isNoneDefault(b)
	if an && bn {
		return true
	}
	if an != bn {
		return false
	}
	return reflect.DeepEqual(a, b)
}

func isNoneDefault(d ir.DefaultValue) bool {
	if d == nil {
		return true
	}
	_, ok := d.(ir.DefaultNone)
	return ok
}

// indicesEqual compares two indexes by name + column-name list.
// Coarse intentionally — chain restore applies the after-schema's
// indexes wholesale. Falls back to reflect.DeepEqual on the column
// slice since IndexColumn is a small struct (Column / Expression /
// Desc / etc.) and direct struct equality would miss expression
// changes.
func indicesEqual(a, b *ir.Index) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Name != b.Name || a.Unique != b.Unique {
		return false
	}
	return reflect.DeepEqual(a.Columns, b.Columns)
}

// typeString renders an IR Type to its canonical String form. nil
// types format as "nil" so a column with an unset type doesn't equal
// every other unset-type column on accident.
func typeString(t ir.Type) string {
	if t == nil {
		return "nil"
	}
	return t.String()
}
