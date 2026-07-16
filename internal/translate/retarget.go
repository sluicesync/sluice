// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// RetargetForEngine rewrites a schema's column types from their
// source-engine-native shapes to the target engine's default-emit
// equivalents — the same translations the target engine's DDL writer
// would apply if asked to emit the schema. Used by `sluice schema diff`
// (ADR-0029) to produce a cross-engine-comparable "expected" schema:
// without this pass, every PG-native column (uuid, inet, array, ...)
// surfaces as a type mismatch against the actual MySQL target storage,
// even when the target IS what sluice would produce.
//
// The pass is the IR-layer mirror of what the target engine's
// emitColumnType does at the DDL boundary. Keeping it in the translate
// package (engine-agnostic, no engine-package imports) means new
// engines opt into the diff's cross-engine comparison by adding rules
// here, not by surfacing new interfaces on the engine side.
//
// Operator-supplied mappings (via [ApplyMappings]) take precedence —
// callers should run ApplyMappings BEFORE RetargetForEngine so an
// operator's `--type-override col=binary_uuid` survives the retarget
// (the override has already replaced the IR type by the time retarget
// runs; retarget's pattern match only fires on still-source-native
// types).
//
// Identity for unknown engine pairs and same-engine pairs. The rule
// table today covers the PG-storage → MySQL-dialect direction (the
// v0.7.0 auto-emit defaults); other directions retarget no types and
// the diff falls back to the pre-retarget IR comparison. Consumers
// that REFUSE on the comparison (rather than report, as the diff
// does) must gate on [HasStorageShapeMapping] first — a raw compare
// against a foreign catalog's read-back mistakes translation for
// drift.
func RetargetForEngine(s *ir.Schema, sourceEngine, targetEngine string) *ir.Schema {
	if s == nil {
		return nil
	}
	rule := retargetRuleFor(sourceEngine, targetEngine)
	if rule == nil {
		return s
	}
	out := &ir.Schema{
		Tables: make([]*ir.Table, len(s.Tables)),
		// Schema-level objects pass through untouched: these passes
		// rewrite table/column shapes only, and dropping Views /
		// Sequences here would silently strip them from every run that
		// engages the pass (the item-51 lesson).
		Views:     s.Views,
		Sequences: s.Sequences,
	}
	for i, tbl := range s.Tables {
		out.Tables[i] = retargetTable(tbl, rule)
	}
	return out
}

// retargetRule maps a source IR type to the target IR type the engine
// would emit. Returning nil signals "no rewrite for this type" — the
// type is either already cross-engine-portable (Integer, Boolean, ...)
// or the source engine handles it natively on the target side.
type retargetRule func(ir.Type) ir.Type

// retargetRuleFor returns the rule for a (source, target) engine pair,
// or nil when no rewriting applies. Keyed on the storage families, not
// literal engine names, so every PG-storage source (postgres,
// postgres-trigger) and every MySQL-dialect target (mysql,
// planetscale, vitess) shares the one rule — the literal-name match
// this replaces silently missed the vitess flavor (ADR-0166
// follow-up (c)).
func retargetRuleFor(sourceEngine, targetEngine string) retargetRule {
	if storageShapeFamily(sourceEngine) == storageFamilyPostgres && IsMySQLFamily(targetEngine) {
		return retargetPGtoMySQL
	}
	return nil
}

// Storage-shape family labels for [storageShapeFamily].
const (
	storageFamilyMySQL    = "mysql"
	storageFamilyPostgres = "postgres"
	storageFamilySQLite   = "sqlite"
)

// storageShapeFamily buckets engine names by the catalog surface their
// schema readers share: two engines in one family read a
// shape-identical table back to the SAME IR, so an identity comparison
// between them is faithful. The MySQL flavors share one engine
// implementation (and mydumper parses MySQL-dialect DDL into the same
// shapes); the trigger-CDC variants delegate their SchemaReaders to
// the composed base engine. Engines with no shared-storage sibling
// bucket as their own lowercased name, so same-name pairs are always
// one family.
//
// Maintenance: the MySQL half rides [IsMySQLFamily] (registry-parity
// tested); the postgres/sqlite lists enumerate the composed variants.
// A new flavor or trigger variant must be added here to keep the
// ADR-0166 shape gate active for its pairs — a miss fails SAFE (the
// pair falls back to the gate's WARN-and-proceed posture, never a
// false refusal).
func storageShapeFamily(engine string) string {
	switch {
	case IsMySQLFamily(engine):
		return storageFamilyMySQL
	case strings.EqualFold(engine, "postgres"), strings.EqualFold(engine, "postgres-trigger"):
		return storageFamilyPostgres
	case strings.EqualFold(engine, "sqlite"), strings.EqualFold(engine, "sqlite-trigger"),
		strings.EqualFold(engine, "d1"), strings.EqualFold(engine, "d1-trigger"):
		return storageFamilySQLite
	}
	return strings.ToLower(engine)
}

// HasStorageShapeMapping reports whether [RetargetForEngine] can render
// source-native IR in the target's storage shapes for this engine
// pair: either both engines share a storage-shape family (identity is
// faithful) or a retarget rule exists. Consumers that COMPARE the
// retargeted schema against a target catalog read-back — the ADR-0166
// migrate pre-create shape gate — must check this first: with no
// mapping, source-native IR lands against the target's lossy read-back
// (MySQL INT UNSIGNED reads back from PG as BIGINT, TEXT tiers
// collapse, VARBINARY becomes BYTEA) and translation is
// indistinguishable from drift. That raw compare is the 2026-07-16
// audit's HIGH-1: it false-refused every mysql→postgres re-run over
// tables sluice itself had created.
func HasStorageShapeMapping(sourceEngine, targetEngine string) bool {
	return storageShapeFamily(sourceEngine) == storageShapeFamily(targetEngine) ||
		retargetRuleFor(sourceEngine, targetEngine) != nil
}

// retargetPGtoMySQL mirrors the PG→MySQL emit rules from
// internal/engines/mysql/ddl_emit.go::emitColumnType. The rules track
// the v0.7.0 auto-emit defaults (ADR §"PG-native types auto-emit").
// Operators wanting non-default storage shapes for these columns
// (e.g. binary_uuid for UUID) supply --type-override before this pass
// runs — the override replaces the IR type via [ApplyMappings] and
// the rewrite below sees a non-matching type and passes through.
//
// ADR-0032 cross-engine default translators: hstore → JSON and citext
// → VARCHAR with case-insensitive collation. These mirror the
// engine-side emit (mysql/ddl_emit.go::emitColumnType) so a `sluice
// schema diff` against a MySQL target sees the same translated shape
// the migrate path lands on.
func retargetPGtoMySQL(t ir.Type) ir.Type {
	switch v := t.(type) {
	case ir.UUID:
		return ir.Char{Length: 36}
	case ir.Inet, ir.Cidr:
		return ir.Varchar{Length: 45}
	case ir.Macaddr:
		return ir.Varchar{Length: 30}
	case ir.Varchar:
		// Bug 72: a wide bounded varchar(N) is down-mapped to a MySQL
		// TEXT tier by the engine emitter (mysql/ddl_emit.go). Mirror
		// it here so `sluice schema diff` against a MySQL target sees
		// the same translated shape the migrate path lands on. Narrow
		// varchars pass through unchanged (nil → no rewrite).
		if size, downmap := mysqlTextTierForWideVarcharIR(v.Length); downmap {
			return ir.Text{Size: size, Charset: v.Charset, Collation: v.Collation}
		}
		return nil
	case ir.Array:
		return ir.JSON{Binary: true}
	case ir.ExtensionType:
		switch v.Extension {
		case "hstore":
			return ir.JSON{Binary: true}
		case "citext":
			return ir.Varchar{Length: 255, Collation: "utf8mb4_0900_ai_ci"}
		}
	}
	return nil
}

// retargetTable returns a copy of tbl with each column's Type
// replaced by rule(col.Type) when the rule produces a rewrite. Tables
// with no rewritten columns return the original pointer to avoid
// gratuitous copies.
func retargetTable(tbl *ir.Table, rule retargetRule) *ir.Table {
	if tbl == nil {
		return nil
	}
	var rewrittenCols []*ir.Column
	for i, col := range tbl.Columns {
		newType := rule(col.Type)
		if newType == nil {
			continue
		}
		if rewrittenCols == nil {
			rewrittenCols = make([]*ir.Column, len(tbl.Columns))
			copy(rewrittenCols, tbl.Columns)
		}
		colCopy := *col
		colCopy.Type = newType
		rewrittenCols[i] = &colCopy
	}
	if rewrittenCols == nil {
		return tbl
	}
	out := *tbl
	out.Columns = rewrittenCols
	return &out
}
