package translate

import (
	"github.com/orware/sluice/internal/ir"
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
// Identity for unknown engine pairs and same-engine pairs. v0.8.0
// scope is the PG→MySQL direction (the v0.7.0 auto-emit defaults);
// other directions retarget no types and the diff falls back to the
// pre-retarget IR comparison.
func RetargetForEngine(s *ir.Schema, sourceEngine, targetEngine string) *ir.Schema {
	if s == nil {
		return nil
	}
	rule := retargetRuleFor(sourceEngine, targetEngine)
	if rule == nil {
		return s
	}
	out := &ir.Schema{Tables: make([]*ir.Table, len(s.Tables))}
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
// or nil when no rewriting applies.
func retargetRuleFor(sourceEngine, targetEngine string) retargetRule {
	if sourceEngine == "postgres" && (targetEngine == "mysql" || targetEngine == "planetscale") {
		return retargetPGtoMySQL
	}
	return nil
}

// retargetPGtoMySQL mirrors the PG→MySQL emit rules from
// internal/engines/mysql/ddl_emit.go::emitColumnType. The rules track
// the v0.7.0 auto-emit defaults (ADR §"PG-native types auto-emit").
// Operators wanting non-default storage shapes for these columns
// (e.g. binary_uuid for UUID) supply --type-override before this pass
// runs — the override replaces the IR type via [ApplyMappings] and
// the rewrite below sees a non-matching type and passes through.
func retargetPGtoMySQL(t ir.Type) ir.Type {
	switch t.(type) {
	case ir.UUID:
		return ir.Char{Length: 36}
	case ir.Inet, ir.Cidr:
		return ir.Varchar{Length: 45}
	case ir.Macaddr:
		return ir.Varchar{Length: 30}
	case ir.Array:
		return ir.JSON{Binary: true}
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
