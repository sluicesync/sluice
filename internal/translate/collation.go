// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Collation-dialect inference — the "charset-paired collation" rule.
//
// The IR's string types (Char / Varchar / Text) carry Collation as an
// opaque source-dialect name with no dialect tag. The two readers that
// populate it follow documented, structurally different conventions:
//
//   - The MySQL reader records information_schema's collation_name for
//     EVERY string column, and information_schema pairs it with a
//     non-NULL character_set_name — so a MySQL-dialect collation always
//     arrives with Charset set (e.g. Charset="utf8mb4",
//     Collation="utf8mb4_0900_ai_ci").
//   - The Postgres reader records pg_collation.collname only when the
//     column carries an EXPLICIT non-default collation, and PG has no
//     per-column charset concept at all (server_encoding is
//     database-wide) — so a PG-dialect collation always arrives with
//     Charset empty (e.g. Charset="", Collation="C"). See the
//     populateColumns doc in internal/engines/postgres/schema_reader.go.
//   - The SQLite reader never populates Collation.
//
// Charset-presence therefore identifies the dialect a collation name
// belongs to without an IR schema change. This is a named wart: it
// leans on reader invariants rather than an explicit tag (the
// GeneratedExprDialect convention), chosen because tagging would ripple
// through the wire format (schema history), ir/diff, and the CDC
// normalizers for what is a two-producer namespace. If a third engine
// ever records per-column collations, revisit with a real dialect tag.
//
// Collation names do NOT translate across engines (MySQL's
// "utf8mb4_0900_ai_ci" is meaningless to PG; PG's "C" is meaningless to
// MySQL), so writers emit only same-dialect collations and drop foreign
// ones with a WARN — see docs/type-mapping.md "Charsets and collations".

import "sluicesync.dev/sluice/internal/ir"

// CollationDialect reports which engine dialect a (charset, collation)
// pair read off an IR string type belongs to: "mysql", "postgres", or
// "" when no collation is carried. See the package-level rule above
// for why charset-presence is the discriminator.
func CollationDialect(charset, collation string) string {
	switch {
	case collation == "":
		return ""
	case charset != "":
		return "mysql"
	default:
		return "postgres"
	}
}

// ColumnCollation extracts the (charset, collation) pair from the IR
// string types that carry one. ir.Domain recurses into its base type:
// the PG reader resolves a domain column's EFFECTIVE collation
// (pg_attribute.attcollation — the domain's own or a column-level
// override) onto the base string type before the ir.Domain wrap, and
// the column definition is the only safe place to re-emit it (the
// CREATE DOMAIN statement can't carry a per-column override). All
// other types — including ir.Array, whose element metadata never
// carries a collation (the PG reader's array-element resolution drops
// it; a known read-side gap) — return empty strings.
func ColumnCollation(t ir.Type) (charset, collation string) {
	switch v := t.(type) {
	case ir.Char:
		return v.Charset, v.Collation
	case ir.Varchar:
		return v.Charset, v.Collation
	case ir.Text:
		return v.Charset, v.Collation
	case ir.Domain:
		return ColumnCollation(v.BaseType)
	}
	return "", ""
}

// DroppedCollationColumns lists the columns of tbl whose carried
// collation a writer of targetDialect cannot emit ("column (collation)"
// per entry, declaration order). A writer drops exactly these — the
// caller owns surfacing the drop as a WARN so the loss is policy, not
// silence. targetDialect is the writer's own dialect in the readers'
// canonical form ("mysql", "postgres"); any other value (e.g.
// "sqlite") treats every carried collation as foreign.
func DroppedCollationColumns(tbl *ir.Table, targetDialect string) []string {
	if tbl == nil {
		return nil
	}
	var out []string
	for _, col := range tbl.Columns {
		if col == nil {
			continue
		}
		charset, collation := ColumnCollation(col.Type)
		if collation == "" || CollationDialect(charset, collation) == targetDialect {
			continue
		}
		out = append(out, col.Name+" ("+collation+")")
	}
	return out
}
