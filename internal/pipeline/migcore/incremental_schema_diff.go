// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

// Phase 3 schema-delta diff: turns a (before, after) pair of source
// schemas into the [irbackup.SchemaDeltaEntry] slice that lands on the
// incremental manifest. Distinct from the ir/diff package's Schemas (which powers
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
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// DiffSchemas returns the structural delta from before to after.
// Empty slice (not nil) when the two schemas are identical.
//
// The order of returned entries is: drops first (so a restore-side
// applier can DROP before CREATE if the same table name appears on
// both sides — a v1 unsupported shape, but the order keeps the
// applier safe), then adds, then alters. Within each group, ordering
// is the after-schema's table order (or before-schema's for drops),
// which is stable across runs.
func DiffSchemas(before, after *ir.Schema) []*irbackup.SchemaDeltaEntry {
	var (
		drops  []*irbackup.SchemaDeltaEntry
		adds   []*irbackup.SchemaDeltaEntry
		alters []*irbackup.SchemaDeltaEntry
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
			drops = append(drops, &irbackup.SchemaDeltaEntry{
				Kind:   irbackup.SchemaDeltaDropTable,
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
				adds = append(adds, &irbackup.SchemaDeltaEntry{
					Kind:   irbackup.SchemaDeltaAddTable,
					Schema: tAfter.Schema,
					Table:  tAfter.Name,
					After:  tAfter,
				})
				continue
			}
			if !tablesEqual(tBefore, tAfter) {
				alters = append(alters, &irbackup.SchemaDeltaEntry{
					Kind:   irbackup.SchemaDeltaAlterTable,
					Schema: tAfter.Schema,
					Table:  tAfter.Name,
					Before: tBefore,
					After:  tAfter,
				})
			}
		}
	}

	out := make([]*irbackup.SchemaDeltaEntry, 0, len(drops)+len(adds)+len(alters))
	out = append(out, drops...)
	out = append(out, adds...)
	out = append(out, alters...)
	return out
}

// indexTablesByQualifiedName returns a "schema.name" → table map for
// fast lookup during DiffSchemas.
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

// tablesEqual compares two table values for structural equality.
//
// The comparison is DEFINED BY [ClassifyAlterDelta]: two tables are
// equal exactly when the classifier reports no differing aspect. That
// is deliberate rather than incidental — the chain-replay side has to
// dispose of every difference the diff can see (apply it, prove it
// needs no DDL, or refuse), and a hand-written equality here plus a
// hand-written aspect list there is exactly how a shape reaches a
// silent skip. One list, two consumers. See incremental_delta_shape.go
// for the aspect registry and the per-aspect disposition.
//
// Deliberately coarse — the chain-restore applier reads the After
// shape verbatim, so a column-comment-only drift produces no delta at
// all (Comment is not a compared aspect). The opposite (subtle drift
// the diff misses) would matter; the comparison includes column types
// via Type.String(), which catches the common shape changes (TINYINT →
// BOOLEAN, VARCHAR(50) → VARCHAR(100), …).
//
// Index comparison is a name-keyed SET, not positional: index order is
// not semantic, and pre-task-#41 manifests recorded Indexes in
// randomized map-iteration order — so a recorded parent schema vs a
// fresh (now-deterministic) catalog read can present the identical
// index set in different order. Positional comparison turned that into
// phantom alter_table deltas (observed live: schema_deltas=6 on a
// DDL-free incremental, 2026-06-10 benchmark).
func tablesEqual(a, b *ir.Table) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return ClassifyAlterDelta(a, b).Equal()
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

// TypeString renders an IR Type to its canonical String form. nil
// types format as "nil" so a column with an unset type doesn't equal
// every other unset-type column on accident.
func TypeString(t ir.Type) string {
	if t == nil {
		return "nil"
	}
	return t.String()
}
