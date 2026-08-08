// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// The Postgres target's index-NAME transformation, stated once.
//
// Postgres index names are SCHEMA-scoped; MySQL's and SQLite's are
// TABLE-scoped. So the Postgres writer table-prefixes a source index
// name before creating it, and the catalog then reads that prefixed name
// back. Two consumers need that answer and they must not disagree:
//
//   - the Postgres DDL emitter, which creates the index
//     (internal/engines/postgres.effectivePGIndexIdent, a thin delegate
//     to [PGEffectiveIndexName]);
//   - the EXPECTED side of a shape comparison
//     ([RetargetForShapeCompare]), which has to name the index the way
//     the target holds it or every comparison reports phantom drift.
//
// The rule lives here rather than in the engine package because the
// second consumer is engine-neutral by construction (internal/translate
// imports no engine package). A second statement of the rule would drift,
// and a drift here is indistinguishable from real index drift — which is
// exactly the class Bug 234 belongs to.

// MaxPGIdentifierLen is PostgreSQL's NAMEDATALEN-1 ceiling: identifiers
// longer than this are SILENTLY TRUNCATED at CREATE time, so two
// prepended names sharing their first 63 bytes collapse onto one
// relation (GitHub #26).
const MaxPGIdentifierLen = 63

// PGIndexNamingConventionPrefixes is the set of index-naming convention
// prefixes that operator schemas use to mean "this index is already
// scoped to a single table." When [PGIndexName] sees a source name
// shaped like `ix_<table>_<rest>` / `idx_<table>_<rest>` /
// `fk_<table>_<rest>` / `uq_<table>_<rest>` / etc., the table-name
// portion is already encoded; prepending another table prefix would (a)
// double the table-name presence in the identifier and (b) push past 63
// bytes on long table names.
//
// Coverage drawn from the conventions of SQLAlchemy/Alembic, Django,
// Rails AR / ActiveRecord, Hibernate, Diesel, and operator-written hand
// schemas. The list is intentionally generous on the read side — false
// positives (treating an unconventional name as already-scoped) are
// emit-verbatim, which is the same behavior the explicit `<table>_`
// prefix check already handles.
var PGIndexNamingConventionPrefixes = []string{
	"ix_", "idx_", "uix_", "uidx_", "uniq_", "uq_",
	"pk_", "fk_", "chk_", "ck_",
}

// PGIndexName disambiguates a source-side index name against the
// schema-scoped Postgres namespace. The rule (after GitHub #26):
//
//  1. If sourceName already starts with `<tableName>_`, emit verbatim.
//  2. If sourceName matches a known convention prefix (`ix_<table>_`,
//     `idx_<table>_`, etc., per [PGIndexNamingConventionPrefixes]) AND
//     the convention-prefix + tableName segment matches, emit verbatim.
//     This covers real-world SQLAlchemy / Alembic / Rails / Django
//     shapes where the table name is already encoded in the index name.
//  3. Otherwise, prepend `<tableName>_`. If the result would exceed PG's
//     63-byte NAMEDATALEN limit, emit sourceName verbatim instead — the
//     operator's source-declared name fits PG, and avoiding the
//     truncation collision is the load-bearing concern. The historical
//     reason for prefixing (sibling-table name disambiguation) is
//     preserved for short names; long names sacrifice it.
//
// Idempotent by construction: rule (1) makes a second application of the
// transformation a no-op, which is what lets the expected side of a
// comparison run it over a schema read from a Postgres source without
// double-prefixing.
func PGIndexName(tableName, sourceName string) string {
	if sourceName == "" {
		return ""
	}
	prefix := tableName + "_"
	if strings.HasPrefix(sourceName, prefix) {
		return sourceName
	}
	// Convention-prefix detection: if source name is shaped like
	// `<convention_prefix><tableName>_<rest>`, treat as already
	// table-scoped.
	for _, conv := range PGIndexNamingConventionPrefixes {
		if strings.HasPrefix(sourceName, conv+tableName+"_") {
			return sourceName
		}
		// Edge case: source name is exactly `<conv><tableName>` (table
		// name suffix with no trailing column part) — still
		// already-table-scoped.
		if sourceName == conv+tableName {
			return sourceName
		}
	}
	full := prefix + sourceName
	if len(full) > MaxPGIdentifierLen {
		// Prepending would overflow → emit verbatim. The historical
		// disambiguation against sibling-table indexes
		// (`idx_fk_film_id` on multiple tables) is sacrificed for the
		// truncation-collision-free path, which is the more urgent
		// failure mode.
		return sourceName
	}
	return full
}

// PGEffectiveIndexName returns the pg_class relation name a Postgres
// target will ACTUALLY hold for one of a table's indexes.
//
// A constraint-backed unique index is re-created as `ALTER TABLE … ADD
// CONSTRAINT <source name>` (verbatim, so `ON CONFLICT ON CONSTRAINT`
// keeps working); everything else goes through [PGIndexName]'s
// table-scoping transformation.
func PGEffectiveIndexName(tableName string, idx *ir.Index) string {
	if idx == nil {
		return ""
	}
	if idx.ConstraintBacked {
		return idx.Name
	}
	return PGIndexName(tableName, idx.Name)
}

// indexNameRule maps one of a table's secondary indexes to the name the
// TARGET engine will hold it under. Returning "" means "no rewrite".
//
// PRIMARY KEY indexes are deliberately out of scope: every engine names
// the primary key by its own convention (MySQL calls it `PRIMARY`,
// Postgres `<table>_pkey`, SQLite leaves it unnamed), so no rename rule
// could make the two sides' NAMES agree. That half of the index-name
// axis is closed in internal/ir/diff, which matches the primary key by
// its structural ROLE instead of by name.
type indexNameRule func(tableName string, idx *ir.Index) string

// indexNameRuleFor returns the rule for a target engine, or nil when the
// target holds source index names verbatim.
//
// The roster, enumerated rather than promised (the sibling-sweep step) —
// these are every engine whose SchemaWriter creates a secondary index:
//
//   - postgres / postgres-trigger: TRANSFORMS. Index names are
//     schema-scoped, so the writer table-prefixes them
//     ([PGEffectiveIndexName]).
//   - mysql / planetscale / vitess: verbatim
//     (internal/engines/mysql.emitCreateIndex quotes idx.Name as-is;
//     MySQL index names are table-scoped and need no disambiguation).
//   - sqlite / d1 (+ their trigger variants): verbatim
//     (internal/engines/sqlite.emitCreateIndex, same reasoning).
//
// Pinned by TestIndexNameRuleRosterCoversEveryRegisteredEngine.
func indexNameRuleFor(targetEngine string) indexNameRule {
	if storageShapeFamily(targetEngine) == storageFamilyPostgres {
		return PGEffectiveIndexName
	}
	return nil
}
