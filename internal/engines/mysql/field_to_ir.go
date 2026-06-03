// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// errFieldMetadataUnavailable is returned by projectVStreamFields when
// a FIELD event omits the per-column ColumnType DDL string entirely —
// i.e. the position-anchored metadata needed to build a faithful
// schema-history snapshot is simply not present on the wire.
//
// This is DISTINCT from an unmappable-but-present ColumnType (a real
// unknown type → a hard loud error, the loud-failure tenet): an absent
// ColumnType is "the source didn't give us enough to snapshot", not
// "we found a type we refuse to guess at". The Chunk-B2 boundary path
// treats this sentinel as "skip the schema-history version for this
// table" — the stream continues, the existing ROW-without-FIELD loud
// floor is untouched, and a later resume across a real DDL on that
// table falls back to the pre-ADR-0049 safe behaviour (no retained
// version → ir.ErrPositionInvalid → ADR-0022 cold-start). Crashing a
// live stream over a metadata shape the existing decode path
// (decodeVStreamCell) already tolerates would be an availability
// regression with zero correctness gain — the ADR is an efficiency
// upgrade ON TOP of the loud floor, never a new way to halt a sync.
//
// FLAGGED FOR LEAD REVIEW (not pre-specified): the locked decisions
// say a projector failure is fatal/loud (#4b) and the snapshot is
// built from in-stream metadata, never re-introspection (#2). They do
// NOT specify the behaviour when the in-stream metadata is *absent*
// (empty ColumnType). Real Vitess populates ColumnType (decodeVStream
// cell + isMySQLBoolColumnType already depend on it), so this path is
// not expected in production; the degrade-to-cold-start choice keeps
// the loud floor intact while not regressing availability on a
// minimal/edge FIELD shape. The alternative — hard-fail the stream —
// is one line away if the lead prefers strict #4b here.
var errFieldMetadataUnavailable = errors.New("mysql/vstream: FIELD event omits column_type metadata")

// ADR-0049 Chunk B2 — the VStream []*query.Field → ir.Table projector.
//
// This is the single largest new surface in Chunk B (ADR-0049 locked
// decision #2 / readiness ambiguity #2). The snapshot persisted at a
// VStream FIELD-event boundary MUST be built from the in-stream
// position-anchored field metadata, NEVER a fresh information_schema
// re-introspection (the ADR rejects re-introspection in Alternatives;
// re-introspection reads "schema now", which races events still in
// flight for the pre-DDL shape and is wrong on resume/replay).
//
// # Why this reuses translateType rather than a fresh type switch
//
// VStream's query.Field carries ColumnType — the column's MySQL DDL
// string ("tinyint(1)", "decimal(10,2)", "enum('a','b')", "bit(8)",
// "varchar(255) CHARACTER SET utf8mb4", …). That is the SAME rich
// form information_schema.columns.column_type carries, which the
// engine's single canonical MySQL→IR mapping — translateType — already
// consumes. A hand-rolled second type switch here would be a Bug-74
// silent-loss trap: it would inevitably diverge from translateType on
// a parameter family (decimal precision, enum value-set, bit width,
// temporal fractional precision, unsigned-ness, tinyint(1) bool) and
// the divergence would only surface as a corrupt resume snapshot.
// Projecting through a derived columnMeta keeps ONE mapping authority.
//
// decodeVStreamCell's per-cell type knowledge (ColumnType drives the
// tinyint(1)→bool distinction there too) is the precedent this mirrors:
// the wire type alone is lossy, ColumnType is the source of truth.
//
// # The Vitess-field-family → ir.Type matrix
//
// Because the projection is "parse ColumnType → columnMeta →
// translateType", correctness is exactly translateType's correctness
// PLUS the columnMeta-extraction below. The pin
// (field_to_ir_test.go) therefore exercises EVERY Vitess proto-Type
// family AND every ColumnType parameter shape — native int (signed/
// unsigned/tinyint(1)), float, decimal(p,s), string-leaf (char/
// varchar/text/binary/varbinary/blob with and without CHARACTER SET),
// temporal (date/time/datetime(n)/timestamp(n)/year), enum, set, bit,
// json, geometry — not one representative. A projector that is right
// for int/text but silently wrong for decimal/enum/bit/temporal is the
// exact silent-loss class ADR-0049 + the Bug-74 lesson forbid.

// projectVStreamFields builds an [ir.Table] from the cached
// []*query.Field metadata VStream delivered for one table. schema is
// the keyspace (sluice's per-engine schema), table the unqualified
// table name. It is a pure function (no DB, no proto leakage past the
// boundary) so it is exhaustively unit-testable without a live stream.
//
// An unmappable column surfaces a loud error naming the column and its
// ColumnType — never a silent fallback (the loud-failure tenet; a
// guessed type in a persisted schema-history version is a delayed
// silent corruption on every resume across that boundary).
func projectVStreamFields(schema, table string, fields []*query.Field) (*ir.Table, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("mysql/vstream: cannot project schema for %q.%q: no FIELD metadata", schema, table)
	}
	cols := make([]*ir.Column, 0, len(fields))
	for _, f := range fields {
		if strings.TrimSpace(f.GetColumnType()) == "" {
			// Metadata-absent: not a refuse-to-guess unknown type, but
			// "the source didn't give us enough to snapshot". Surface
			// the sentinel so the boundary path degrades to the
			// cold-start floor rather than halting the stream.
			return nil, fmt.Errorf("%w (column %q, proto type %s)",
				errFieldMetadataUnavailable, f.GetName(), f.GetType())
		}
		meta, err := columnMetaFromField(f)
		if err != nil {
			return nil, fmt.Errorf("mysql/vstream: project column %q (%s): %w",
				f.GetName(), f.GetColumnType(), err)
		}
		typ, err := translateType(meta)
		if err != nil {
			return nil, fmt.Errorf("mysql/vstream: project column %q: %w", f.GetName(), err)
		}
		cols = append(cols, &ir.Column{
			Name: f.GetName(),
			Type: typ,
			// Nullable is derived from the proto NOT_NULL flag (1<<12 in
			// MySQL's column-flags space, which Vitess forwards verbatim
			// in Field.Flags). The schema-history decode contract
			// (SchemaSignature) only compares name+type, so nullability
			// is informational here, but populating it keeps the
			// snapshot a faithful table rather than a lossy projection.
			Nullable: f.GetFlags()&mysqlFlagNotNull == 0,
		})
	}
	return &ir.Table{Schema: schema, Name: table, Columns: cols}, nil
}

// mysqlFlagNotNull is MySQL's NOT_NULL column flag bit (1). Vitess
// forwards the source column flags on query.Field.Flags unchanged, so
// this is the canonical not-null test for projected fields.
const mysqlFlagNotNull = 1

// columnMetaFromField derives the [columnMeta] translateType consumes
// from a single VStream query.Field. The ColumnType string is the
// rich source (same form as information_schema.columns.column_type);
// the parenthesised parameters (length / precision,scale / fractional
// precision) are parsed out so the decimal / char / temporal branches
// of translateType see the same inputs they would from a real
// information_schema row.
//
// SrsID is intentionally 0: VStream's FieldEvent does not carry a
// per-column SRID, so a projected geometry column lands SRID 0 — the
// same outcome translateType produces for a source geometry column
// declared without an explicit SRID (no regression vs the pre-existing
// no-SRID path; the schema-history decode contract does not depend on
// SRID anyway).
func columnMetaFromField(f *query.Field) (columnMeta, error) {
	ct := f.GetColumnType()
	if strings.TrimSpace(ct) == "" {
		return columnMeta{}, fmt.Errorf("empty column_type (proto type %s)", f.GetType())
	}
	lower := strings.ToLower(ct)

	meta := columnMeta{
		// translateType lowercases data_type at its information_schema
		// source; mirror that. data_type is the leading identifier of
		// the DDL string ("varchar" from "varchar(255)", "int" from
		// "int unsigned", "double" from "double").
		DataType:   leadingTypeWord(lower),
		ColumnType: ct,
	}

	// CHARACTER SET / COLLATE survive in the VStream ColumnType DDL
	// suffix for character columns; translateType threads Charset /
	// Collation onto ir.Char / ir.Varchar / ir.Text. Extract them so a
	// charset-only change is still reflected in the projected type
	// (the SchemaSignature compares the full ir.Type, charset
	// included).
	meta.Charset = extractDDLToken(lower, "character set")
	meta.Collation = extractDDLToken(lower, "collate")

	// Unsigned / auto_increment are detected by translateType from
	// ColumnType / Extra respectively. VStream does not surface a
	// separate Extra; the AUTO_INCREMENT flag rides query.Field.Flags
	// (MySQL's AUTO_INCREMENT_FLAG = 512). Surface it via the Extra
	// channel translateType already reads ("auto_increment" substring),
	// so a serial/identity column projects as ir.Integer{AutoIncrement}
	// identically to the information_schema path.
	if f.GetFlags()&mysqlFlagAutoIncrement != 0 {
		meta.Extra = "auto_increment"
	}

	// Parenthesised parameter extraction. enum/set/bit are parsed by
	// translateType directly from ColumnType, so they need nothing
	// here. The numeric/char/temporal branches read the dedicated
	// columnMeta fields.
	switch meta.DataType {
	case "decimal", "numeric":
		p, s, ok := parseDecimalParams(lower)
		if ok {
			meta.NumPrec = int64p(int64(p))
			meta.NumScale = int64p(int64(s))
		}
	case "char", "varchar", "binary", "varbinary":
		if n, ok := parseSingleLenParam(lower); ok {
			meta.CharMaxLen = int64p(int64(n))
		}
	case "time", "datetime", "timestamp":
		// Fractional-seconds precision: datetime(6) → DTPrec 6. Absent
		// parens → precision 0, exactly translateType's int64Ptr(nil)
		// behaviour.
		if n, ok := parseSingleLenParam(lower); ok {
			meta.DTPrec = int64p(int64(n))
		}
	}
	return meta, nil
}

// leadingTypeWord returns the leading bare type identifier of a
// lowercased MySQL column_type DDL string: "varchar(255)" → "varchar",
// "int unsigned" → "int", "double" → "double", "enum('a','b')" →
// "enum". Stops at the first '(' or whitespace.
func leadingTypeWord(lowerCT string) string {
	for i := 0; i < len(lowerCT); i++ {
		c := lowerCT[i]
		if c == '(' || c == ' ' || c == '\t' {
			return lowerCT[:i]
		}
	}
	return lowerCT
}

// parseSingleLenParam pulls the single integer N from a "(N)" suffix
// of a lowercased column_type: "varchar(255)" → 255, "datetime(6)" →
// 6. ok=false when there is no parenthesised integer (e.g. plain
// "datetime", "blob"). A multi-arg paren ("decimal(10,2)") returns
// ok=false here — decimal uses parseDecimalParams.
func parseSingleLenParam(lowerCT string) (n int, ok bool) {
	open := strings.IndexByte(lowerCT, '(')
	if open < 0 {
		return 0, false
	}
	closeIdx := strings.IndexByte(lowerCT[open:], ')')
	if closeIdx < 0 {
		return 0, false
	}
	inner := lowerCT[open+1 : open+closeIdx]
	if strings.ContainsRune(inner, ',') {
		return 0, false
	}
	v, err := strconv.Atoi(strings.TrimSpace(inner))
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

// parseDecimalParams pulls (precision, scale) from a lowercased
// "decimal(P,S)" / "numeric(P)" column_type. A missing scale defaults
// to 0 (MySQL's DECIMAL(P) == DECIMAL(P,0)). ok=false when there is no
// parenthesised parameter at all (bare "decimal" — MySQL implies
// (10,0); translateType then reads NumPrec/NumScale as 0, which the
// pre-existing information_schema path also does for the rare
// metadata-less case, so behaviour is unchanged).
func parseDecimalParams(lowerCT string) (precision, scale int, ok bool) {
	open := strings.IndexByte(lowerCT, '(')
	if open < 0 {
		return 0, 0, false
	}
	closeIdx := strings.IndexByte(lowerCT[open:], ')')
	if closeIdx < 0 {
		return 0, 0, false
	}
	inner := lowerCT[open+1 : open+closeIdx]
	parts := strings.SplitN(inner, ",", 2)
	p, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || p < 0 {
		return 0, 0, false
	}
	if len(parts) == 1 {
		return p, 0, true
	}
	s, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || s < 0 {
		return 0, 0, false
	}
	return p, s, true
}

// extractDDLToken returns the identifier following a `<token> ` marker
// in a lowercased column_type DDL string (e.g. token "character set"
// in "varchar(255) character set utf8mb4 collate utf8mb4_bin" →
// "utf8mb4"). Returns "" when the token is absent. The captured value
// stops at the next space, so a trailing "collate ..." does not bleed
// into the charset capture.
func extractDDLToken(lowerCT, token string) string {
	idx := strings.Index(lowerCT, token+" ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(lowerCT[idx+len(token):])
	for i := 0; i < len(rest); i++ {
		if rest[i] == ' ' || rest[i] == '\t' {
			return rest[:i]
		}
	}
	return rest
}

// int64p boxes an int64 for the *int64 columnMeta fields. Distinct
// from the package's int64Ptr (which is the inverse — *int64 → int64).
func int64p(v int64) *int64 { return &v }

// mysqlFlagAutoIncrement is MySQL's AUTO_INCREMENT_FLAG column-flag
// bit (512). Vitess forwards source column flags on
// query.Field.Flags unchanged.
const mysqlFlagAutoIncrement = 512
