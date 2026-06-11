// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "sort"

// tableObjectKey identifies one named per-table object (index, foreign
// key) while the schema reader collects multi-row catalog results into
// per-object values.
type tableObjectKey struct{ table, name string }

// sortedTableObjectKeys returns m's keys ordered by (table, name).
// Catalog collections that aggregate through a map MUST drain through
// this instead of ranging the map directly: Go map iteration is
// randomized, and an unordered Indexes/ForeignKeys slice makes two
// reads of the SAME schema structurally unequal — recorded manifests
// then diff against fresh reads as phantom alter_table deltas, and
// [ir.ComputeSchemaHash] fingerprints diverge for identical schemas
// (task #41; observed live as schema_deltas=6 on a DDL-free
// incremental in the 2026-06-10 backup benchmark).
func sortedTableObjectKeys[V any](m map[tableObjectKey]V) []tableObjectKey {
	keys := make([]tableObjectKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].table != keys[j].table {
			return keys[i].table < keys[j].table
		}
		return keys[i].name < keys[j].name
	})
	return keys
}
