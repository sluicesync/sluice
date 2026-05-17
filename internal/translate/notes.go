// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Translation-notes and advisory-hints registries for `sluice schema
// preview` (ADR-0024). Notes describe what changes when a column
// crosses an engine boundary; hints suggest a `--type-override`
// invocation for cases where sluice's default has a known operator-
// preferable alternative.
//
// Both registries are intentionally tiny. Each entry is maintenance
// forever; growth happens when real-world testing surfaces new
// surprises, not in anticipation of them.

import (
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// Note describes a single per-column translation note. Returned by
// [NotesFor] when a cross-engine type translation is non-trivial; the
// preview formatter renders it as an inline `-- source: X → target:
// Y` comment on the column line.
type Note struct {
	// Column is the column name the note applies to.
	Column string
	// SourceType is a short rendering of the source IR type (e.g.
	// "uuid", "datetime(6)", "text").
	SourceType string
	// TargetType is a short rendering of the target IR type (e.g.
	// "char(36)", "timestamp(6)", "longtext").
	TargetType string
	// Message is the operator-facing prose. Concise; one short
	// sentence. Empty for the common case where source-type +
	// target-type alone are self-explanatory.
	Message string
}

// Hint describes an advisory hint suggesting a `--type-override`
// alternative for the given column. Returned by [HintsFor] for cases
// where sluice's default has a known operator-preferable alternative.
// The preview formatter renders it after the table's DDL block as a
// `-- hint: ...` line.
type Hint struct {
	// Table is the table the hint applies to.
	Table string
	// Column is the column the hint applies to.
	Column string
	// Message is the human-readable reason for the suggested
	// override.
	Message string
	// SuggestedOverride is the exact `--type-override TABLE.COL=TYPE`
	// invocation the operator can copy/paste. The preview formatter
	// emits this verbatim.
	SuggestedOverride string
}

// NotesFor returns translation notes for col when migrating from the
// source engine to the target engine. Returns nil for same-engine
// migrations (nothing to translate) and for column types whose
// cross-engine translation is unremarkable.
//
// The function takes the *original* source-side column (as the
// SchemaReader produced it) and the post-mapping target column (after
// any `--type-override` rewrites have been applied). A note that
// references an override the operator already supplied would be
// distracting noise; the preview orchestrator suppresses notes whose
// SourceType and TargetType match (operator wrote the column to its
// preferred shape directly).
func NotesFor(srcCol, tgtCol *ir.Column, srcEngine, tgtEngine string) []Note {
	if srcEngine == tgtEngine {
		return nil
	}
	if srcCol == nil || tgtCol == nil {
		return nil
	}

	srcStr := renderTypeForNote(srcCol.Type, srcEngine)
	tgtStr := renderTypeForNote(tgtCol.Type, tgtEngine)
	if srcStr == tgtStr {
		return nil
	}

	out := make([]Note, 0, 1)
	for _, entry := range noteEntries {
		if entry.matches(srcCol, tgtCol, srcEngine, tgtEngine) {
			out = append(out, Note{
				Column:     srcCol.Name,
				SourceType: srcStr,
				TargetType: tgtStr,
				Message:    entry.message,
			})
		}
	}
	if len(out) == 0 {
		// Fall through with a bare source/target rendering and no
		// message — the preview formatter still surfaces it as a
		// translation note so the operator sees the type change. The
		// per-engine-pair message-bearing entries above add prose
		// only for cases where the type shift carries a semantic
		// caveat worth calling out.
		out = append(out, Note{
			Column:     srcCol.Name,
			SourceType: srcStr,
			TargetType: tgtStr,
		})
	}
	return out
}

// HintsFor returns advisory hints for col when migrating from src to
// tgt. Returns nil when no hint applies. The hints registry is small
// on purpose (~6 entries today); see ADR-0024 for the criteria for
// adding new ones.
//
// Hints fire on the *source* column type — the operator's
// `--type-override` decision is what we want to surface, so a hint
// for "PG uuid → MySQL char(36)" should keep firing even after the
// column has been mapped to something else (the operator may want to
// see what they're already doing). The preview orchestrator can
// suppress hints whose suggested override the operator has already
// applied; that's a cosmetic convenience and lives in the formatter,
// not here.
func HintsFor(table string, srcCol, tgtCol *ir.Column, srcEngine, tgtEngine string) []Hint {
	if srcEngine == tgtEngine {
		return nil
	}
	if srcCol == nil || tgtCol == nil {
		return nil
	}

	out := make([]Hint, 0, 1)
	for _, entry := range hintEntries {
		if entry.matches(srcCol, tgtCol, srcEngine, tgtEngine) {
			out = append(out, Hint{
				Table:             table,
				Column:            srcCol.Name,
				Message:           entry.message,
				SuggestedOverride: fmt.Sprintf("--type-override %s.%s=%s", table, srcCol.Name, entry.suggestedAlias),
			})
		}
	}
	return out
}

// noteEntry is one entry in the translation-notes registry. The
// matches predicate gates emission; the message is the prose the
// formatter renders alongside the type rendering.
type noteEntry struct {
	matches func(src, tgt *ir.Column, srcEngine, tgtEngine string) bool
	message string
}

// noteEntries is the per-engine-pair list of cross-engine type
// translations that carry a semantic caveat worth emitting on a
// column line. Order is not significant — the preview formatter
// emits all matching notes in registry order.
var noteEntries = []noteEntry{
	// MySQL JSON → PG JSONB. JSONB is the canonical fast path on PG;
	// the note exists to remind the operator they can downgrade to
	// `json` (text) if they need key-order preservation. No advisory
	// hint — JSONB is correct for the vast majority of operators.
	{
		matches: func(src, tgt *ir.Column, srcEngine, tgtEngine string) bool {
			if srcEngine != "mysql" && srcEngine != "planetscale" {
				return false
			}
			if tgtEngine != "postgres" {
				return false
			}
			_, srcJSON := src.Type.(ir.JSON)
			tgtJSON, ok := tgt.Type.(ir.JSON)
			return srcJSON && ok && tgtJSON.Binary
		},
		message: "binary JSONB preserves value semantics; for key-order preservation, override to json (text)",
	},
}

// hintEntry is one entry in the advisory-hints registry. The matches
// predicate gates emission; suggestedAlias names the target_type the
// operator should use in `--type-override`.
type hintEntry struct {
	matches        func(src, tgt *ir.Column, srcEngine, tgtEngine string) bool
	message        string
	suggestedAlias string
}

// hintEntries is the v0.7.0 advisory-hints registry from ADR-0024
// §"Advisory-hints registry". Each entry surfaces a known operator-
// preferable alternative for a default cross-engine translation.
var hintEntries = []hintEntry{
	// PG uuid → MySQL CHAR(36). Default is human-readable but
	// expands 2.25× over a binary representation; for storage-
	// optimal uuids, override to BINARY(16).
	{
		matches: func(src, _ *ir.Column, srcEngine, tgtEngine string) bool {
			if srcEngine != "postgres" {
				return false
			}
			if tgtEngine != "mysql" && tgtEngine != "planetscale" {
				return false
			}
			_, ok := src.Type.(ir.UUID)
			return ok
		},
		message:        "PG uuid expands 2.25x as CHAR(36); for binary storage, override to binary_uuid",
		suggestedAlias: "binary_uuid",
	},

	// MySQL ENUM → PG: removed. The original ADR-0024 entry
	// anticipated a TEXT+CHECK default and recommended the
	// `pg_enum` override to switch to a real enum type. Sluice
	// actually emits `CREATE TYPE … AS ENUM` by default (see
	// internal/engines/postgres/ddl_emit.go:emitCreateEnumType),
	// and no `pg_enum` alias is registered — the hint as written
	// would point operators at a non-existent override. The
	// default is already what operators want; no surprise to flag.

	// PG text (unbounded) → MySQL LONGTEXT. The 4GB cap is rarely
	// the right default; if the operator knows the column is
	// bounded, MEDIUMTEXT (16MB) or VARCHAR(N) saves significant
	// per-row overhead.
	{
		matches: func(src, _ *ir.Column, srcEngine, tgtEngine string) bool {
			if srcEngine != "postgres" {
				return false
			}
			if tgtEngine != "mysql" && tgtEngine != "planetscale" {
				return false
			}
			t, ok := src.Type.(ir.Text)
			return ok && t.Size == ir.TextLong
		},
		message:        "PG text -> MySQL LONGTEXT (4GB cap, large overhead); if column is bounded, override to varchar:length=N or mediumtext",
		suggestedAlias: "mediumtext",
	},

	// MySQL DATETIME(N) → PG TIMESTAMP(N). DATETIME drops the
	// timezone; if the source values are UTC-encoded, the operator
	// may want TIMESTAMPTZ on the target.
	{
		matches: func(src, _ *ir.Column, srcEngine, tgtEngine string) bool {
			if srcEngine != "mysql" && srcEngine != "planetscale" {
				return false
			}
			if tgtEngine != "postgres" {
				return false
			}
			_, ok := src.Type.(ir.DateTime)
			return ok
		},
		message:        "MySQL DATETIME has no timezone; if source values are UTC-encoded, override to timestamptz",
		suggestedAlias: "timestamptz",
	},

	// PG numeric (unbounded) → MySQL DECIMAL(65,30). The MySQL max
	// is the safest fit but rarely the desired storage shape; if
	// the operator knows the precision/scale, override.
	{
		matches: func(src, _ *ir.Column, srcEngine, tgtEngine string) bool {
			if srcEngine != "postgres" {
				return false
			}
			if tgtEngine != "mysql" && tgtEngine != "planetscale" {
				return false
			}
			d, ok := src.Type.(ir.Decimal)
			// Unbounded PG numeric arrives as ir.Decimal{Unconstrained:
			// true} from the PG schema reader (catalog Bug 69); bounded
			// numerics carry their declared precision/scale.
			return ok && d.Unconstrained
		},
		message:        "PG unbounded numeric -> MySQL DECIMAL(65,30); for narrower storage, override to decimal:precision=N,scale=M",
		suggestedAlias: "decimal:precision=N,scale=M",
	},
}

// renderTypeForNote returns a short engine-neutral textual rendering
// of an IR type, suitable for a translation-note line. The format is
// modelled on the engine's native DDL idiom but not validated against
// it — the goal is human readability in a one-line comment, not
// round-trippable DDL. Engines whose native rendering carries
// information not in the IR (e.g. MySQL CHARACTER SET on string
// types) are rendered with the structural shape only; the note's
// message field carries the semantic caveat when there is one.
func renderTypeForNote(t ir.Type, engine string) string {
	if t == nil {
		return "<nil>"
	}
	switch v := t.(type) {
	case ir.Boolean:
		if engine == "mysql" || engine == "planetscale" {
			return "tinyint(1)"
		}
		return "boolean"
	case ir.Integer:
		return renderInteger(v, engine)
	case ir.Decimal:
		if v.Unconstrained {
			if engine == "postgres" {
				return "numeric"
			}
			// MySQL target renders the unconstrained numeric as the
			// widest representable DECIMAL (catalog Bug 69).
			return "decimal(65,30)"
		}
		if engine == "postgres" {
			return fmt.Sprintf("numeric(%d,%d)", v.Precision, v.Scale)
		}
		return fmt.Sprintf("decimal(%d,%d)", v.Precision, v.Scale)
	case ir.Float:
		if v.Precision == ir.FloatSingle {
			if engine == "postgres" {
				return "real"
			}
			return "float"
		}
		if engine == "postgres" {
			return "double precision"
		}
		return "double"
	case ir.Char:
		return fmt.Sprintf("char(%d)", v.Length)
	case ir.Varchar:
		return fmt.Sprintf("varchar(%d)", v.Length)
	case ir.Text:
		return renderTextSize(v.Size, engine)
	case ir.Binary:
		return fmt.Sprintf("binary(%d)", v.Length)
	case ir.Varbinary:
		return fmt.Sprintf("varbinary(%d)", v.Length)
	case ir.Blob:
		return renderBlobSize(v.Size, engine)
	case ir.Bit:
		return fmt.Sprintf("bit(%d)", v.Length)
	case ir.Date:
		return "date"
	case ir.Time:
		return renderWithPrecision("time", v.Precision)
	case ir.DateTime:
		if engine == "postgres" {
			return renderWithPrecision("timestamp", v.Precision)
		}
		return renderWithPrecision("datetime", v.Precision)
	case ir.Timestamp:
		base := renderWithPrecision("timestamp", v.Precision)
		if v.WithTimeZone {
			if engine == "postgres" {
				return base + "tz"
			}
			return base // MySQL timestamp is implicitly zoned
		}
		return base
	case ir.JSON:
		if v.Binary && engine == "postgres" {
			return "jsonb"
		}
		return "json"
	case ir.UUID:
		if engine == "postgres" {
			return "uuid"
		}
		return "char(36)"
	case ir.Enum:
		return "enum"
	case ir.Set:
		return "set"
	case ir.Array:
		if v.Element == nil {
			return "array<unknown>"
		}
		return renderTypeForNote(v.Element, engine) + "[]"
	case ir.Geometry:
		return "geometry"
	case ir.Inet:
		return "inet"
	case ir.Cidr:
		return "cidr"
	case ir.Macaddr:
		return "macaddr"
	}
	return t.String()
}

func renderInteger(v ir.Integer, engine string) string {
	switch engine {
	case "mysql", "planetscale":
		var name string
		switch v.Width {
		case 8:
			name = "tinyint"
		case 16:
			name = "smallint"
		case 24:
			name = "mediumint"
		case 32:
			name = "int"
		case 64:
			name = "bigint"
		default:
			name = "bigint"
		}
		if v.Unsigned {
			name += " unsigned"
		}
		return name
	default:
		// Postgres et al.
		switch v.Width {
		case 8, 16:
			return "smallint"
		case 24, 32:
			return "integer"
		case 64:
			// Bug 11: `bigint unsigned` maps uniformly to PG bigint
			// (PK, FK, standalone) so FK-to-IDENTITY-PK types match by
			// construction. The (2^63, 2^64) range loss is surfaced
			// loudly via the dedicated unsigned-bigint notice, not via
			// a divergent type rendering here.
			return "bigint"
		default:
			return "bigint"
		}
	}
}

func renderTextSize(s ir.TextSize, engine string) string {
	if engine == "postgres" {
		return "text"
	}
	switch s {
	case ir.TextTiny:
		return "tinytext"
	case ir.TextRegular:
		return "text"
	case ir.TextMedium:
		return "mediumtext"
	case ir.TextLong:
		return "longtext"
	}
	return "text"
}

func renderBlobSize(s ir.BlobSize, engine string) string {
	if engine == "postgres" {
		return "bytea"
	}
	switch s {
	case ir.BlobTiny:
		return "tinyblob"
	case ir.BlobRegular:
		return "blob"
	case ir.BlobMedium:
		return "mediumblob"
	case ir.BlobLong:
		return "longblob"
	}
	return "blob"
}

func renderWithPrecision(name string, precision int) string {
	if precision == 0 {
		return name
	}
	return fmt.Sprintf("%s(%d)", name, precision)
}
