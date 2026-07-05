// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"sort"

	"sluicesync.dev/sluice/internal/ir"
)

// RotationSegmentDirPrefix is the on-disk prefix of every
// rotation-opened segment sub-directory (`seg-<unix-millis>/`). Single
// source of truth shared by the producer (root's [performRotation]
// provisional-dir construction) and the consumer ([ResolveLineage]'s
// missing-catalog multi-segment-evidence guard, Bug 66): if any
// `seg-*` path exists but lineage.json is absent, the backup is a
// rotated multi-segment lineage that cannot be reconstructed from a
// bare walk — a loud refusal, never a silent root-only partial.
const RotationSegmentDirPrefix = "seg-"

// VerbatimExtensionColumnsIn returns the sorted "schema.table.column"
// references in s whose IR type is [ir.VerbatimType] (ADR-0047). The
// schema segment is omitted when empty (flat-scope engines). Returns
// nil when the schema carries no verbatim columns — the common case,
// so the marker stays absent on every non-verbatim backup.
//
// This is the recorded-marker source for
// [Segment.VerbatimExtensionColumns]: the lineage-catalog write
// path records the marker from the full's schema, and the restore-time
// engine gate (root's refuseVerbatim* helpers) enforces it loudly. Both
// sides call this so the two derivations are consistent by construction.
func VerbatimExtensionColumnsIn(s *ir.Schema) []string {
	if s == nil {
		return nil
	}
	var refs []string
	for _, tbl := range s.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			if _, ok := col.Type.(ir.VerbatimType); !ok {
				continue
			}
			ref := tbl.Name + "." + col.Name
			if tbl.Schema != "" {
				ref = tbl.Schema + "." + ref
			}
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		return nil
	}
	sort.Strings(refs)
	return refs
}
