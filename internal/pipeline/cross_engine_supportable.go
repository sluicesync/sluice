// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Cross-engine supportability check. Phase 5 of the logical-backup
// feature (`docs/dev/design-logical-backups-phase-5.md`): cross-engine
// chain restore reuses [translate.RetargetForEngine] to rewrite types
// where a clean translation exists (UUID → CHAR(36) etc.), but a
// handful of source-engine-native types have no portable target-engine
// equivalent — PostGIS geometry on PG → MySQL, hstore on PG → MySQL.
// These shapes are caught here so chain restore can refuse with an
// operator-actionable message naming the offending entity, rather than
// bubbling up an opaque emit-time error from the schema writer.
//
// Same-engine pairs always succeed (the source engine's emitter
// natively handles its own types). Unknown engine pairs (a future
// engine) fall through as "supportable" — the schema writer will
// surface its own error if needed; this check is a conservative
// pre-flight, not an exhaustive one.

import (
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// checkCrossEngineSupportable scans the schema for column types that
// can't be cleanly translated from sourceEngine to targetEngine and
// returns a non-nil error naming the first offending (table, column,
// type) triple. Returns nil when every column either has a portable
// target-engine equivalent (handled by the engine's auto-emit) or a
// rewrite rule in [translate.RetargetForEngine].
//
// Used by chain restore's cross-engine path to refuse early with an
// operator-actionable message. Same-engine pairs and unknown engine
// pairs return nil — the latter on the principle that a new engine's
// emitter will surface its own error if it can't handle a type.
func checkCrossEngineSupportable(
	schema *ir.Schema,
	sourceEngine, targetEngine string,
	contextID string,
) error {
	if schema == nil || sourceEngine == targetEngine {
		return nil
	}
	// Today's only supported cross-engine direction is PG ↔ MySQL.
	// We refuse PG-native types that have no MySQL equivalent in
	// either RetargetForEngine's rewrite table or MySQL's auto-emit
	// rules. PostGIS Geometry is the load-bearing case (the IR type
	// is shared between PG and MySQL spatial, but PG's PostGIS
	// extension carries SRID + complex spatial-reference metadata
	// that doesn't round-trip through a MySQL target without
	// operator intervention).
	pgToMySQL := sourceEngine == "postgres" &&
		(targetEngine == "mysql" || targetEngine == "planetscale")
	if !pgToMySQL {
		return nil
	}
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil {
				continue
			}
			if reason := unsupportablePGtoMySQL(col.Type); reason != "" {
				return fmt.Errorf(
					"%s: column %q.%q has %s — no clean cross-engine translation. "+
						"Recovery: re-run with --exclude-table=%s to skip the table, "+
						"or supply a --type-override mapping the column to a portable IR type",
					contextID, tbl.Name, col.Name, reason, tbl.Name)
			}
		}
	}
	return nil
}

// unsupportablePGtoMySQL returns a non-empty human-readable reason
// when t can't be cleanly emitted on a MySQL target via the existing
// RetargetForEngine rules + MySQL auto-emit. Returns "" for
// supportable types.
//
// PostGIS Geometry: even though MySQL has spatial types, the SRID +
// extension metadata don't round-trip — operators see silent
// truncation of spatial-reference identifiers. Refuse loudly.
//
// PG extension passthrough types (ADR-0032) — pgvector and the v1
// shortlist's other entries — have no portable MySQL equivalent;
// the refusal here keeps the cross-engine loud-failure default in
// place even when the source side was opened with
// `--enable-pg-extension`. Operators wanting a translation supply
// `--type-override TABLE.COL=<MySQL_type>`.
func unsupportablePGtoMySQL(t ir.Type) string {
	switch v := t.(type) {
	case ir.Geometry:
		return "PostGIS geometry type"
	case ir.ExtensionType:
		return fmt.Sprintf("PG extension type %s.%s", v.Extension, v.Name)
	}
	return ""
}

// checkCrossEngineDeltaSupportable scans an incremental's schema-delta
// entries for shapes whose translated form would not be cleanly
// supportable on the target engine. Mirrors
// [checkCrossEngineSupportable] but only inspects the after-shape of
// AddTable / AlterTable entries (DropTable / DropColumn don't carry a
// portable-type concern). Returns nil for same-engine pairs and unknown
// engine pairs; a wrapped error naming the offending column otherwise.
func checkCrossEngineDeltaSupportable(
	deltas []*ir.SchemaDeltaEntry,
	sourceEngine, targetEngine, backupID string,
) error {
	if sourceEngine == targetEngine || sourceEngine == "" {
		return nil
	}
	for _, d := range deltas {
		if d == nil || d.After == nil {
			continue
		}
		switch d.Kind {
		case ir.SchemaDeltaAddTable, ir.SchemaDeltaAlterTable:
			tbl := &ir.Schema{Tables: []*ir.Table{d.After}}
			ctxID := fmt.Sprintf("chain restore: incremental %s schema delta on table %q",
				backupID, d.Table)
			if err := checkCrossEngineSupportable(tbl, sourceEngine, targetEngine, ctxID); err != nil {
				return err
			}
		}
	}
	return nil
}
