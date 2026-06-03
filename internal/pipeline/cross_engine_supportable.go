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

	"sluicesync.dev/sluice/internal/ir"
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
	//
	// `postgres-trigger` (the trigger-based capture engine for
	// slot-less managed PG, ADR-0066) is a PG source for the purposes
	// of these refusals: its schema surface delegates to the vanilla
	// postgres engine, so a `postgres-trigger` source can carry every
	// PG-native shape (PostGIS Geometry, pg_trgm opclass indexes,
	// EXCLUDE constraints) that has no portable MySQL form. Treating
	// it as `postgres` here keeps the cross-engine loud-failure default
	// in place for its Phase 2 cross-engine targets (task #72) — without
	// it a trigger source would silently skip every PG-native refusal.
	// The literal (not pgtrigger.EngineName) keeps the orchestrator
	// engine-neutral — the pipeline package never imports an engine
	// package (see CLAUDE.md "IR-first" / "engine-neutral orchestrator").
	pgToMySQL := isPGSourceEngine(sourceEngine) &&
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
					contextID, tbl.Name, col.Name, reason, tbl.Name,
				)
			}
		}
		// ADR-0032 Tier 2 lite: indexes carrying an extension-owned
		// operator class (today's load-bearing case is pg_trgm's
		// `gin_trgm_ops` / `gist_trgm_ops` on a `text` column) have
		// no clean MySQL translation either. Sluice never populates
		// [ir.IndexColumn.OperatorClass] for core-PG opclasses (Bug
		// 47 design), so any non-empty OperatorClass on a PG-source
		// index is by construction extension-introduced; the cross-
		// engine refusal mirrors the column-type refusal above.
		if reason, idxName, colRef := unsupportablePGIndexToMySQL(tbl); reason != "" {
			return fmt.Errorf(
				"%s: index %q on table %q has %s (column %q) — "+
					"no clean cross-engine translation. "+
					"Recovery: re-run with --exclude-table=%s to skip the table, "+
					"drop the index on the source before migrating, "+
					"or supply a --type-override / --index-override mapping",
				contextID, idxName, tbl.Name, reason, colRef, tbl.Name,
			)
		}
		// ADR-0053: EXCLUDE constraints are PG-only. MySQL has no
		// equivalent type, no equivalent semantics. Pre-ADR sluice
		// silently dropped them from the IR (the schema reader never
		// queried contype='x'); post-ADR the reader populates them and
		// this refusal stops a cross-engine restore from landing
		// tables without the source's semantic invariant.
		if len(tbl.ExcludeConstraints) > 0 {
			return fmt.Errorf(
				"%s: table %q carries EXCLUDE constraint %q (PG-only — "+
					"no MySQL equivalent exists). EXCLUDE constraints are "+
					"PG-only by definition; cross-engine targets cannot "+
					"preserve the source's semantic invariant. "+
					"Recovery: re-run with --exclude-table=%s to skip the "+
					"table, or migrate to a PG target",
				contextID, tbl.Name,
				tbl.ExcludeConstraints[0].Name, tbl.Name,
			)
		}
	}
	return nil
}

// unsupportablePGIndexToMySQL scans a table's indexes (including
// PrimaryKey) for any [ir.IndexColumn.OperatorClass] that's
// non-empty. Returns a (reason, indexName, columnRef) triple naming
// the offending shape, or empty strings when every index is
// MySQL-portable. Sluice's PG schema reader populates OperatorClass
// only for extension-introduced opclasses (ADR-0032 / Bug 47); a
// non-empty value passing through the IR therefore indicates an
// extension-owned opclass with no MySQL counterpart.
func unsupportablePGIndexToMySQL(tbl *ir.Table) (reason, indexName, columnRef string) {
	if tbl == nil {
		return "", "", ""
	}
	check := func(idx *ir.Index) (string, string, string) {
		if idx == nil {
			return "", "", ""
		}
		// Extension-introduced operator class on any column. The PG
		// reader populates OperatorClass only for extension-owned
		// opclasses (ADR-0032 / Bug 47), so non-empty here means MySQL
		// has no counterpart.
		for _, c := range idx.Columns {
			if c.OperatorClass != "" {
				ref := c.Column
				if ref == "" {
					ref = c.Expression
				}
				return fmt.Sprintf("PG extension-owned operator class %q", c.OperatorClass),
					idx.Name, ref
			}
		}
		// PG index kinds MySQL has no counterpart for. v0.30.0 caught
		// the opclass-bearing case but missed the no-flag scenario:
		// the operator-class gets stripped from IR yet idx.Kind stays
		// `IndexKindGIN` / `IndexKindGIST`. MySQL has btree, hash,
		// FULLTEXT, and SPATIAL — gin/gist don't translate. The PG
		// schema reader sets `idx.Kind` from `pg_am.amname` regardless
		// of whether the AM is extension-owned, so this check fires
		// for core-PG gin/gist indexes even when `idx.Method` is
		// empty (only extension-introduced AMs like pgvector's
		// ivfflat / hnsw populate `idx.Method`). FULLTEXT and SPATIAL
		// are MySQL-portable so they stay unflagged here; ir.Geometry
		// auto-emits MySQL SPATIAL at write-time.
		if kind := idx.Kind; kind == ir.IndexKindGIN || kind == ir.IndexKindGIST ||
			kind == ir.IndexKindSPGist || kind == ir.IndexKindBRIN {
			ref := ""
			if len(idx.Columns) > 0 {
				ref = idx.Columns[0].Column
				if ref == "" {
					ref = idx.Columns[0].Expression
				}
			}
			label := "GIN"
			switch kind {
			case ir.IndexKindGIST:
				label = "GiST"
			case ir.IndexKindSPGist:
				label = "SP-GiST"
			case ir.IndexKindBRIN:
				label = "BRIN"
			}
			return fmt.Sprintf("PG %s index has no MySQL counterpart", label),
				idx.Name, ref
		}
		return "", "", ""
	}
	if r, n, ref := check(tbl.PrimaryKey); r != "" {
		return r, n, ref
	}
	for _, idx := range tbl.Indexes {
		if r, n, ref := check(idx); r != "" {
			return r, n, ref
		}
	}
	return "", "", ""
}

// unsupportablePGtoMySQL returns a non-empty human-readable reason
// when t can't be cleanly emitted on a MySQL target via the existing
// RetargetForEngine rules + MySQL auto-emit. Returns "" for
// supportable types.
//
// PostGIS Geometry was refused pre-v0.28.0 because the SRID metadata
// didn't round-trip cleanly. ADR-0035 closes that gap: the PG schema
// reader populates ir.Geometry.SRID from PostGIS's geometry_columns
// view, and the MySQL writer emits `SRID <n>` on the column DDL
// (8.0+) so ST_SRID(col) on the target returns the source's SRID
// instead of 0. Cross-engine geometry values pass through as
// supportable now.
//
// PG extension passthrough types (ADR-0032) — pgvector / pg_trgm /
// postgis — have no portable MySQL equivalent; the refusal here
// keeps the cross-engine loud-failure default in place even when
// the source side was opened with `--enable-pg-extension`.
// Operators wanting a translation supply `--type-override
// TABLE.COL=<MySQL_type>`.
//
// Exception: hstore and citext have default cross-engine
// translators (hstore → MySQL JSON; citext → MySQL VARCHAR with
// case-insensitive collation), so the refusal carves them out.
// The MySQL writer's `emitColumnType` handles the type rewrite
// directly; the writer's `prepareValue` translates hstore wire
// format to JSON at value-write time. See the catalog's
// `crossEngineDefaultTranslatedExtensions` for the policy source
// of truth.
func unsupportablePGtoMySQL(t ir.Type) string {
	if v, ok := t.(ir.ExtensionType); ok {
		if isCrossEngineTranslatablePGExtension(v.Extension) {
			return ""
		}
		return fmt.Sprintf("PG extension type %s.%s", v.Extension, v.Name)
	}
	// ADR-0047: ir.VerbatimType is an uncatalogued PG extension type
	// carried verbatim for same-engine PG → PG / PG-backup ONLY. It is
	// PG-native by definition with no portable MySQL equivalent — the
	// cross-engine loud-failure default applies (and there is no
	// per-extension translator that could carve it out, unlike
	// hstore/citext). This refusal mirrors the ExtensionType branch
	// above and keeps the cross-engine default strictly unweakened: a
	// migrate/restore PG → MySQL with such a column refuses before any
	// data moves with an operator-actionable message.
	if v, ok := t.(ir.VerbatimType); ok {
		return fmt.Sprintf("PG verbatim extension type %q (ADR-0047 — "+
			"same-engine PG / PG-restore only; no cross-engine MySQL form)",
			v.Definition)
	}
	return ""
}

// isPGSourceEngine reports whether engine is a Postgres-family source for
// cross-engine supportability purposes. Both the vanilla `postgres` engine
// and the trigger-based `postgres-trigger` engine (ADR-0066) carry the
// full PG-native type surface — `postgres-trigger`'s schema reader
// delegates to the vanilla postgres engine — so both must trip the same
// PG → MySQL cross-engine refusals (PostGIS Geometry, pg_trgm opclass
// indexes, EXCLUDE constraints). String literals (not pgtrigger.EngineName)
// keep the orchestrator engine-neutral: the pipeline package never imports
// a specific engine package.
func isPGSourceEngine(engine string) bool {
	return engine == "postgres" || engine == "postgres-trigger"
}

// isCrossEngineTranslatablePGExtension reports whether an
// ir.ExtensionType column from a PG source has a default
// cross-engine translator on the MySQL side. Mirrors the catalog's
// `crossEngineDefaultTranslatedExtensions` set; kept duplicated
// in the pipeline package to avoid importing the postgres engine
// package (engine-neutral orchestrator rule). The two lists must
// stay in lock-step — see `internal/engines/postgres/extension_catalog.go`.
func isCrossEngineTranslatablePGExtension(name string) bool {
	switch name {
	case "hstore", "citext":
		return true
	}
	return false
}

// checkShardColumnSupport refuses loudly when the operator engaged
// Shape A (`--inject-shard-column NAME=VALUE`) but the target engine
// doesn't implement [ir.ShardColumnSetter] — without the applier-side
// stamp CDC events would land on the consolidated target with the
// discriminator column NULL, then violate the rewritten composite-PK
// NOT NULL constraint, then silently mis-target rows across shards
// on Update/Delete. Pre-flighting here keeps the loud-failure tenet
// (no silent cross-shard corruption) when a future engine ships
// without the surface; the two currently-shipping engines (mysql,
// postgres) both implement it, so this is a defence-in-depth gate
// rather than a routinely-fired refusal.
//
// `target` is a freshly-opened engine handle (typically a
// [ir.ChangeApplier] for sync runs, or a [ir.RowWriter] for migrate
// runs); the check uses the same type-assertion shape the runtime
// wiring uses. Returns nil when the operator hasn't engaged Shape A
// or when the target implements the setter.
func checkShardColumnSupport(target any, shard ShardColumnSpec, contextID string) error {
	if !shard.Engaged() {
		return nil
	}
	if _, ok := target.(ir.ShardColumnSetter); ok {
		return nil
	}
	return fmt.Errorf(
		"%s: target engine does not implement ir.ShardColumnSetter — "+
			"--inject-shard-column %s=%v requires the CDC/bulk-apply path "+
			"to stamp the discriminator onto every row before SQL emission. "+
			"Without it, consolidated CDC events would land with the column "+
			"NULL and either violate the rewritten composite-PK NOT NULL "+
			"constraint or silently mis-target rows across shards (ADR-0048). "+
			"Recovery: pick a target engine that implements the surface "+
			"(today's shipping mysql/postgres both do), or drop "+
			"--inject-shard-column for this stream",
		contextID, shard.Name, shard.Value,
	)
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
