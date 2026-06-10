// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0047 — backup capability marker + loud restore-time engine gate
// for verbatim (uncatalogued) PG extension types.
//
// A PG backup whose schema carries [ir.VerbatimType] columns is
// PG-restore-only: the verbatim type spelling has no portable
// non-PG form, and the restore-target engine is unknown at backup
// time. Per the codebase's record-never-sniff / fail-loud-on-mismatch
// idioms (DefaultExpression.Dialect, the per-segment Codec, ADR-0046 /
// Bug 66's "lineage.json is the authoritative structural record"), the
// mechanism is a RECORDED marker on the lineage segment
// ([LineageSegment.VerbatimExtensionColumns]) enforced LOUDLY at
// restore preflight against the actual target engine — never an
// operator opt-in flag, never a silent drop/mangle.
//
// This is the same severity class as Bug 66 and the ADR-0035
// PostGIS-absent refusal: checked before any data moves, operator-
// actionable, naming the offending columns.

import (
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// verbatimExtensionColumnsIn returns the sorted "schema.table.column"
// references in s whose IR type is [ir.VerbatimType] (ADR-0047). The
// schema segment is omitted when empty (flat-scope engines). Returns
// nil when the schema carries no verbatim columns — the common case,
// so the marker stays absent on every non-verbatim backup.
func verbatimExtensionColumnsIn(s *ir.Schema) []string {
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

// refuseVerbatimManifestRestoreToNonPG is the single-manifest-path
// counterpart of [refuseVerbatimRestoreToNonPG]. The single-full
// restore path does not always have a recorded lineage marker to
// consult (a legacy backup, or a freshly-written full whose
// lineage.json synthesises from the manifest rather than carrying the
// recorded marker), so it gates directly on the manifest's schema —
// the SAME schema the marker is derived from, so the two checks are
// consistent by construction. Loud, before any data moves, naming the
// columns. Returns nil for a target declaring
// [ir.Capabilities.VerbatimExtensionTypes] (the verbatim columns
// re-create exactly) and for a schema with no verbatim columns (the
// legacy / common case — unaffected).
func refuseVerbatimManifestRestoreToNonPG(schema *ir.Schema, target ir.Engine) error {
	if target == nil || target.Capabilities().VerbatimExtensionTypes {
		return nil // nil target is caught by the caller's validate()
	}
	refs := verbatimExtensionColumnsIn(schema)
	if len(refs) == 0 {
		return nil
	}
	return fmt.Errorf(
		"restore: this backup carries verbatim (uncatalogued) PG "+
			"extension-typed columns and is PG-restore-only, but the "+
			"restore target engine is %q. ADR-0047 verbatim passthrough "+
			"has no portable non-PG form — restoring it to a non-PG "+
			"target would silently drop or mangle the affected columns, "+
			"which sluice refuses (loud-failure tenet). Affected "+
			"column(s): %s. Recovery: restore to a PostgreSQL target "+
			"(with the owning extension installed), or take a fresh "+
			"backup against a schema that excludes these columns "+
			"(--exclude-table) if a cross-engine copy is required",
		target.Name(), strings.Join(refs, ", "),
	)
}

// refuseVerbatimRestoreToNonPG is the ADR-0047 loud restore-time
// engine gate. It scans every segment of the lineage for the recorded
// PG-restore-only marker; if ANY segment carries it AND the restore
// target engine doesn't declare
// [ir.Capabilities.VerbatimExtensionTypes], it returns a loud,
// operator-actionable refusal naming the verbatim columns and the
// PG-restore-only constraint. Returns nil for a verbatim-capable
// target (the verbatim columns re-create exactly), and nil when no
// segment carries the marker (every pre-ADR-0047 / non-verbatim
// backup — legacy backups unaffected).
//
// The check is recorded-marker-driven, NOT schema-sniffing: the
// marker is the authoritative structural record (ADR-0046 / Bug 66
// idiom). It fires before any data moves so the operator never gets a
// partial cross-engine restore of a PG-only backup.
func refuseVerbatimRestoreToNonPG(cat *LineageCatalog, target ir.Engine) error {
	if cat == nil || target == nil || target.Capabilities().VerbatimExtensionTypes {
		return nil // nil target is caught by the caller's validate()
	}
	var marked []string
	for i := range cat.Segments {
		seg := &cat.Segments[i]
		if seg.hasVerbatimExtensionColumns() {
			marked = append(marked, seg.VerbatimExtensionColumns...)
		}
	}
	if len(marked) == 0 {
		return nil
	}
	sort.Strings(marked)
	return fmt.Errorf(
		"restore: this backup carries verbatim (uncatalogued) PG "+
			"extension-typed columns and is PG-restore-only, but the "+
			"restore target engine is %q. ADR-0047 verbatim passthrough "+
			"has no portable non-PG form — restoring it to a non-PG "+
			"target would silently drop or mangle the affected columns, "+
			"which sluice refuses (loud-failure tenet). Affected "+
			"column(s): %s. Recovery: restore to a PostgreSQL target "+
			"(with the owning extension installed), or take a fresh "+
			"backup against a schema that excludes these columns "+
			"(--exclude-table) if a cross-engine copy is required",
		target.Name(), strings.Join(marked, ", "),
	)
}
