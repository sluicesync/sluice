// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Un-indexable TEXT/BLOB/JSON index key-part refusal — surfaces, loudly
// and BEFORE any DDL/data moves, that a translated index carries a key
// part on a column whose MySQL type has no inline key length (catalog
// Bug 136).
//
// MySQL refuses an index key part on a TEXT/BLOB-family column unless
// the part declares an explicit prefix length (`col(N)`) — Error 1170
// ("BLOB/TEXT column used in key specification without a key length");
// JSON columns cannot be key parts at all (Error 3152). A PG source
// never carries prefix lengths (PG has no prefix indexes), so a PG
// `text` column under a UNIQUE or secondary index translated to MySQL
// emitted invalid index DDL that failed at the create-indexes step —
// loud, but LATE: after every row had already copied.
//
// sluice deliberately does NOT auto-emit a prefix key length: a prefix
// index — above all a UNIQUE one — changes the index's matching and
// uniqueness semantics, and silently changing index shape violates the
// surface-explicitly tenet. Instead the failure moves EARLY: `migrate`
// (and chain restore) refuse before any data moves via
// [TextIndexRefusalError], and `schema preview` renders the same
// advisory in a dedicated section so the operator sees it before
// running anything. The escape hatch is the operator-supplied
// `--type-override TABLE.COL=varchar(N)` (the index then carries the
// column's full value, preserving uniqueness semantics), or dropping /
// redefining the index on the source.
//
// Same-engine pairs are unaffected: a MySQL source's TEXT-column index
// reads back with its prefix length in [ir.IndexColumn.Length] and
// re-emits verbatim; PG → PG never consults MySQL's rules.

import (
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// TextIndexNotice names one index key part whose column lands on MySQL
// as a type with no inline key length (TEXT/BLOB family, or JSON which
// cannot be a key part at all).
type TextIndexNotice struct {
	// Table is the source-side table the index lives on.
	Table string
	// Index is the offending index's name ("PRIMARY KEY" when the part
	// belongs to the table's primary key).
	Index string
	// Column is the offending key part's column name.
	Column string
	// SourceType is a short PG-idiom rendering of the column's IR type
	// (e.g. "text", "bytea", "varchar(70000)").
	SourceType string
	// TargetType is the MySQL type keyword the column would land as
	// (e.g. "LONGTEXT", "BLOB", "JSON").
	TargetType string
	// Unique reports whether the index enforces uniqueness — the case
	// where a prefix key length would be most semantically corrosive.
	Unique bool
	// PrimaryKey reports whether the part belongs to the table's
	// primary key.
	PrimaryKey bool
}

// ScanTextIndexNotices walks schema and returns one [TextIndexNotice]
// per index key part (PRIMARY KEY included) on a column whose
// translated MySQL type has no inline key length. Cross-engine
// PG-family → MySQL-family only — returns nil for any other engine
// pair or a nil schema; MySQL → MySQL re-emits the source's prefix
// lengths verbatim and PG targets have no such rule.
//
// Per Bug 74 doctrine the scan covers every type family the MySQL
// emitter maps to a no-key-length type — see [mysqlKeylessIndexTarget]
// — not just `text`. Skipped key parts (each deliberate):
//
//   - parts carrying a prefix length (Length > 0): valid MySQL prefix
//     syntax, only reachable from a MySQL-family source;
//   - expression entries (Column == ""): no column type to resolve;
//     non-portable expressions stay loud-failure on the target;
//   - FULLTEXT / SPATIAL indexes: MySQL accepts TEXT (resp. geometry)
//     key parts there without a prefix length.
//
// Results follow schema declaration order (table order × PK-then-
// Indexes × key-part order) so rendering is stable.
func ScanTextIndexNotices(schema *ir.Schema, sourceEngine, targetEngine string) []TextIndexNotice {
	if schema == nil || !isPGFamilySource(sourceEngine) || !IsMySQLFamily(targetEngine) {
		return nil
	}

	var out []TextIndexNotice
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		colTypes := make(map[string]ir.Type, len(tbl.Columns))
		for _, col := range tbl.Columns {
			if col != nil {
				colTypes[col.Name] = col.Type
			}
		}
		scan := func(idx *ir.Index, primary bool) {
			if idx == nil {
				return
			}
			if idx.Kind == ir.IndexKindFullText || idx.Kind == ir.IndexKindSpatial {
				return
			}
			for _, part := range idx.Columns {
				if part.Column == "" || part.Length > 0 {
					continue
				}
				t, ok := colTypes[part.Column]
				if !ok {
					continue
				}
				target, keyless := mysqlKeylessIndexTarget(t)
				if !keyless {
					continue
				}
				name := idx.Name
				if primary {
					name = "PRIMARY KEY"
				}
				out = append(out, TextIndexNotice{
					Table:      tbl.Name,
					Index:      name,
					Column:     part.Column,
					SourceType: renderTypeForNote(t, "postgres"),
					TargetType: target,
					Unique:     idx.Unique || primary,
					PrimaryKey: primary,
				})
			}
		}
		scan(tbl.PrimaryKey, true)
		for _, idx := range tbl.Indexes {
			scan(idx, false)
		}
	}
	return out
}

// TextIndexRefusalError renders a hard refusal naming every index key
// part [ScanTextIndexNotices] flags, or nil when there are none. This
// is a REFUSAL, not an advisory: the alternative outcomes are a raw
// MySQL Error 1170 after every row has copied (the Bug 136 shape) or a
// silently prefix-narrowed index — both worse than failing early with
// the workaround spelled out.
//
// contextID is the caller's phase label ("migrate" / "chain restore:
// …") so the same diagnostic reads correctly at either surface.
//
// Returns nil for non-PG→MySQL pairs (ScanTextIndexNotices
// short-circuits those).
func TextIndexRefusalError(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
) error {
	notices := ScanTextIndexNotices(schema, sourceEngine, targetEngine)
	if len(notices) == 0 {
		return nil
	}
	return errors.New(renderTextIndexRefusal(notices, contextID))
}

// renderTextIndexRefusal builds the multi-line operator-facing message
// body. Split out so the preview formatter and the migrate refusal
// share identical framing.
func renderTextIndexRefusal(notices []TextIndexNotice, contextID string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d index key part(s) cover column(s) that land on MySQL as a "+
		"TEXT/BLOB/JSON type; MySQL cannot index these without an explicit key "+
		"length (Error 1170; JSON columns cannot be key parts at all)", contextID, len(notices))
	b.WriteString(". sluice does not auto-emit a prefix key length — a prefix " +
		"index (above all a UNIQUE one) silently changes the index's matching " +
		"and uniqueness semantics. Refusing before any data moves. " +
		"Offending index key parts:")
	for _, n := range notices {
		b.WriteString("\n  - ")
		fmt.Fprintf(&b, "%s.%s (%s -> %s) — %s", n.Table, n.Column, n.SourceType, n.TargetType, describeTextIndex(n))
	}
	b.WriteString("\nRecovery: bound each column with `--type-override TABLE.COL=varchar(N)` " +
		"(choose N >= the column's longest value; the index then carries the " +
		"column's full value, preserving uniqueness semantics), or drop / " +
		"redefine the index on the source before migrating.")
	return b.String()
}

// describeTextIndex renders the index half of a notice line:
// `PRIMARY KEY`, `UNIQUE index "users_email_key"`, or `index "idx"`.
func describeTextIndex(n TextIndexNotice) string {
	switch {
	case n.PrimaryKey:
		return "PRIMARY KEY"
	case n.Unique:
		return fmt.Sprintf("UNIQUE index %q", n.Index)
	default:
		return fmt.Sprintf("index %q", n.Index)
	}
}

// mysqlKeylessIndexTarget reports whether the MySQL type t would emit
// as has no inline key length — i.e. an index key part on it needs the
// prefix-length MySQL syntax (TEXT/BLOB families) or is impossible
// outright (JSON) — and if so which MySQL type keyword the column
// lands as (for the operator-facing message). Mirrors the dispatch in
// internal/engines/mysql/ddl_emit.go's emitColumnType; the two must
// move together (same lock-step rule as wideVarcharThresholdChars).
//
// The covered families (Bug 74 doctrine — every family that maps to a
// no-key-length MySQL type, not just `text`):
//
//   - ir.Text (every tier)        → TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT
//   - ir.Blob (every tier)        → TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB
//   - wide ir.Varchar             → the Bug 72 TEXT-tier down-map
//   - ir.JSON / ir.Array          → JSON (Error 3152 as a key part)
//   - ir.ExtensionType "hstore"   → JSON (the ADR-0032 carve-out)
//   - ir.Domain                   → recurses into the base type
//
// Indexable string shapes stay clear: narrow Varchar, Char, UUID
// (CHAR(36)), citext (VARCHAR(255)), Inet/Cidr/Macaddr (VARCHAR),
// Binary/Varbinary. Other un-emittable extension/verbatim types are
// refused upstream by the column-type supportability check before any
// index-level concern applies.
func mysqlKeylessIndexTarget(t ir.Type) (string, bool) {
	switch v := t.(type) {
	case ir.Text:
		return mysqlTextKeyword(v.Size), true
	case ir.Blob:
		return mysqlBlobKeyword(v.Size), true
	case ir.Varchar:
		if size, downmap := mysqlTextTierForWideVarcharIR(v.Length); downmap {
			return mysqlTextKeyword(size), true
		}
	case ir.JSON, ir.Array:
		return "JSON", true
	case ir.ExtensionType:
		if v.Extension == "hstore" {
			return "JSON", true
		}
	case ir.Domain:
		if v.BaseType != nil {
			return mysqlKeylessIndexTarget(v.BaseType)
		}
	}
	return "", false
}

// mysqlTextKeyword returns the MySQL TEXT-family keyword for a tier.
// Message-rendering mirror of emitTextType in the MySQL engine.
func mysqlTextKeyword(size ir.TextSize) string {
	switch size {
	case ir.TextTiny:
		return "TINYTEXT"
	case ir.TextMedium:
		return "MEDIUMTEXT"
	case ir.TextLong:
		return "LONGTEXT"
	default:
		return "TEXT"
	}
}

// mysqlBlobKeyword returns the MySQL BLOB-family keyword for a tier.
// Message-rendering mirror of emitBlobType in the MySQL engine.
func mysqlBlobKeyword(size ir.BlobSize) string {
	switch size {
	case ir.BlobTiny:
		return "TINYBLOB"
	case ir.BlobMedium:
		return "MEDIUMBLOB"
	case ir.BlobLong:
		return "LONGBLOB"
	default:
		return "BLOB"
	}
}

// isPGFamilySource reports whether engine is a Postgres-family source
// name. Both the vanilla `postgres` engine and the trigger-based
// `postgres-trigger` engine (ADR-0066) carry the full PG-native schema
// surface — `postgres-trigger`'s schema reader delegates to the
// vanilla postgres engine — so both must trip the same PG → MySQL
// translation policies. Mirrors the pipeline package's
// isPGSourceEngine; string literals (not an engine-package import)
// keep translate engine-neutral.
func isPGFamilySource(engine string) bool {
	return strings.EqualFold(engine, "postgres") ||
		strings.EqualFold(engine, "postgres-trigger")
}
