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
//
// This is the EMIT-lane entry point: use it when the result is handed
// to a SchemaWriter or a RowWriter. Consumers that COMPARE the result
// against a target catalog's read-back want
// [RetargetForShapeCompare] instead; the difference is one thing (a
// `CREATE DOMAIN` wrapper) and picking wrong is silent in both
// directions, so TestRetargetCallSitesDeclareTheirLane in
// internal/pipeline makes every call site declare which it is on.
func RetargetForEngine(s *ir.Schema, sourceEngine, targetEngine string) *ir.Schema {
	return retargetSchema(s, retargetRuleFor(sourceEngine, targetEngine))
}

// RetargetForShapeCompare is [RetargetForEngine] for the consumers that
// COMPARE the result against a target catalog's read-back rather than
// emit DDL from it — the ADR-0166 pre-create shape gate and `sluice
// schema diff`. It adds exactly one thing: a Postgres `CREATE DOMAIN`
// wrapper is read through [ir.UnwrapDomain] before the rule table runs,
// so the expected side names the STORAGE the target will actually hold.
//
// # Why this is a second entry point and not a rule arm (roadmap item 153)
//
// A DOMAIN is a CONSTRAINT wrapper, not a storage one, and
// internal/ir/domain_storage.go already splits its two readings: every
// decision about what a value LANDS AS unwraps ([ir.UnwrapDomain]);
// the two decisions genuinely about the wrapper read it whole
// ([ir.DomainOf]). A comparison against what a MySQL catalog HOLDS is
// the first kind — MySQL has no DOMAIN, `emitColumnType` downgrades the
// column to its base type, and the catalog reads that base type back —
// so without the unwrap the gate compares `Domain{d AS JSON[text]}`
// against `JSON[binary]` and refuses every re-run.
//
// Flattening inside [RetargetForEngine] itself was considered and
// REJECTED on evidence: its other consumers (`restore`, `chain
// restore`, the broker's schema-delta apply, schema-forward) feed the
// result to a SchemaWriter, and MySQL's DDL emitter translates a
// domain's CHECKs into inline table CHECKs — and its CHECK-drop WARN
// fires — through [ir.DomainOf], which reads `Column.Type` and would
// stop matching. That trades this item's loud false-refusal for a
// SILENT constraint drop on the restore lane, which is the worse
// direction. Which lanes take which entry point is not left to memory:
// TestRetargetCallSitesDeclareTheirLane fails on a new call site.
func RetargetForShapeCompare(s *ir.Schema, sourceEngine, targetEngine string) *ir.Schema {
	rule := retargetRuleFor(sourceEngine, targetEngine)
	if rule == nil {
		// No rule means the pair is same-storage-family (or has no
		// mapping at all — see [HasStorageShapeMapping], which the
		// refusing consumers gate on first). A same-family target holds
		// the DOMAIN itself, so unwrapping here would compare a wrapper
		// against a wrapper the target really has.
		return s
	}
	return retargetSchema(s, storageShapeRule(rule))
}

// retargetSchema applies rule to every column of every table. A nil
// rule is the identity (unknown/same-engine pairs), returning the input
// pointer so callers keep the untouched schema.
func retargetSchema(s *ir.Schema, rule retargetRule) *ir.Schema {
	if s == nil {
		return nil
	}
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

// storageShapeRule decorates a rule so a DOMAIN-typed column is graded
// on the storage its base type lands in: unwrap first, then run the
// rule table on the base, and fall back to the bare base type when no
// rule matches it. One place, one reading — the rule table itself never
// grows a DOMAIN arm, so there is no second statement of the emit rules
// to drift.
//
// A malformed nil-BaseType domain is NOT rewritten: [ir.UnwrapDomain]
// hands the wrapper back, and passing it through unchanged keeps the
// DDL emitter's named `DOMAIN %q has nil BaseType` refusal as the loud
// path rather than turning it into a shape mismatch.
func storageShapeRule(rule retargetRule) retargetRule {
	return func(t ir.Type) ir.Type {
		if _, isDomain := t.(ir.Domain); !isDomain {
			return rule(t)
		}
		base := ir.UnwrapDomain(t)
		if _, stillDomain := base.(ir.Domain); stillDomain {
			return nil
		}
		if rewritten := rule(base); rewritten != nil {
			return rewritten
		}
		return base
	}
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

// HasStorageShapeMapping reports whether [RetargetForShapeCompare] can
// render source-native IR in the target's storage shapes for this
// engine pair: either both engines share a storage-shape family
// (identity is faithful) or a retarget rule exists. Consumers that
// COMPARE the retargeted schema against a target catalog read-back —
// the ADR-0166 migrate pre-create shape gate — must check this first
// AND use [RetargetForShapeCompare] to build the expected side: with no
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
	case ir.JSON:
		// MySQL has ONE JSON type and its catalog reads it back as the
		// binary form, so a PG `json` (text storage) column lands
		// indistinguishably from a `jsonb` one — mysql/ddl_emit.go emits
		// a bare `JSON` for both. Without this the expected side says
		// JSON[text] against a read-back of JSON[binary] and the
		// pre-create gate refuses every re-run over a `json` column
		// (roadmap item 153). `jsonb` already matched, which is why the
		// defect survived: the two spellings share every code path here
		// and differ only in the field this arm normalises.
		if !v.Binary {
			return ir.JSON{Binary: true}
		}
		return nil
	case ir.Bit:
		// MySQL has no varying-bit type: mysql/ddl_emit.go lands a fixed
		// BIT(N) for a PG `bit varying(N)` too (catalog Bug 75, values
		// zero-extended). The catalog reads back Varying=false, so an
		// un-normalised expected side false-refuses the same way `json`
		// does — the second bare-column half of item 153, which the
		// filing did not name because the report's fixture had no
		// `bit varying` column.
		if v.Varying {
			return ir.Bit{Length: v.Length}
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
//
// # The rewrite RECORDS what it replaced (Bug 232)
//
// A rule can rewrite a COMPOSITE type into a scalar one, and when it
// does, information the target's value writer still needs goes with it.
// `ir.Array{Element: ir.JSON{}}` → `ir.JSON{}` is the load-bearing case:
// MySQL has no array type, so the column becomes JSON — and the MySQL
// writer's per-element leaf policy (arrayLeafForJSON) then has no
// element family to dispatch on and REFUSES every json[]/jsonb[]/bytea[]
// value. That is what broke `sluice restore` of a PG-sourced chain into
// MySQL in v0.116.0: the element type is in the manifest, and the
// retarget dropped it one step before the writer asked.
//
// So the pre-rewrite type is parked in [ir.Column.SourceColumnType] —
// the field that already carries exactly this ("what did sluice replace
// here?") for `--type-override`, and the pair the MySQL writer already
// consults. An override that fired FIRST is never clobbered: it recorded
// the operator's true source type, which is further back than ours.
//
// It reaches every consumer of the retarget in one place rather than at
// each row-writing lane, which is what makes `restore`, `chain restore`
// and the from-backup broker's initial full load agree with `migrate`.
// The lanes that only DDL-emit a retargeted table (the broker's and
// chain replay's schema-delta apply, schema-forward) are unaffected:
// schema writers emit from Type and ignore this field.
//
// # The premise the provenance rests on, re-derived (roadmap item 153)
//
// The one consumer a newly non-nil value could flip is MySQL's
// columnIsNativelyBinary, which reads nil as "natively binary" and is
// only ever asked about a column whose (post-rewrite) Type is already
// binary. Through v0.116.1 the pin here was the strictly stronger claim
// that NO rule produces a binary type at all — true then, and false
// now: [RetargetForShapeCompare]'s DOMAIN unwrap rewrites
// `Domain{d AS Blob}` to `Blob`, which is exactly a binary output.
//
// What columnIsNativelyBinary actually needs is narrower than "no
// binary output", and it is this: **the retarget must never MANUFACTURE
// binariness.** A binary output must come from an input whose own
// storage type ([ir.UnwrapDomain]) is already binary, so the provenance
// this records answers the same question the un-retargeted column would
// have answered. The DOMAIN case satisfies it — a PG domain over
// `bytea` IS natively binary — and it satisfies it only because
// columnIsNativelyBinary unwraps domains; the two facts are bound
// rather than separately believed by
// TestColumnIsNativelyBinary_SurvivesTheRetargetsProvenance in
// internal/engines/mysql, which drives the real pass.
//
// Pinned here by TestRetargetRules_BinaryOutputOnlyFromABinarySource.
// What that no longer guarantees, stated plainly: a rule MAY now output
// a binary type, so any future consumer that relies on "the retarget's
// output is never binary" has lost its evidence and must re-derive it.
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
		if colCopy.SourceColumnType == nil {
			colCopy.SourceColumnType = col.Type
		}
		rewrittenCols[i] = &colCopy
	}
	if rewrittenCols == nil {
		return tbl
	}
	out := *tbl
	out.Columns = rewrittenCols
	return &out
}
