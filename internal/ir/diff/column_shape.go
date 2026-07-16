// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// Focused column-shape comparison for the migrate pre-create gate
// (ADR-0166): given the table the migration INTENDS to create and the
// same-named table that already exists on the target, decide whether
// the existing table is shape-equal enough to skip the CREATE — or
// name exactly which columns differ so the refusal is actionable.
//
// The gate deliberately compares LESS than Schemas does: column names
// (order-insensitive), types (the same typeString rendering the
// schema-diff command trusts), nullability, and primary-key PRESENCE
// (expected-has-one vs target-has-none — audit 2026-07-16 M1.7).
// Indexes, constraints, defaults, generated expressions, and comments
// are all OUT of scope — later migrate phases create indexes/
// constraints idempotently (detect-then-skip), and a deploy-request-
// bootstrapped table legitimately carries them already; defaults are
// the schema-diff's documented cross-engine noise source and don't
// affect whether the bulk copy can land rows faithfully.
//
// Charset/collation is compared ONLY when the caller opts in via
// [ShapeCompareOptions.CompareCharset] (same-storage-family MySQL
// pairs, where the catalog surfaces RESOLVED values on both sides —
// audit 2026-07-16 M1.6): a latin1 pre-existing table under a utf8mb4
// intent passed the pre-fix compare and died mid-copy on the first
// non-latin1 row. Cross-engine pairs keep charset out — it is
// translation noise there, not drift.

import (
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// ColumnShapeMismatch names one column whose shape differs between the
// intended table and the existing target table. Expected/Actual carry
// a rendered "type, NULL|NOT NULL" description (or the absent
// sentinel) so refusal messages can show both sides verbatim.
type ColumnShapeMismatch struct {
	Column   string
	Expected string
	Actual   string
}

// columnShapeAbsent is the rendered side of a mismatch where the
// column exists on only one side.
const columnShapeAbsent = "(absent)"

// PrimaryKeyMismatchColumn is the sentinel Column value of the M1.7
// primary-key-presence mismatch, so refusal renderers can phrase it as
// "primary key: …" rather than as a column. Parenthesised so it can
// never collide with a real column name the readers would surface.
const PrimaryKeyMismatchColumn = "(primary key)"

// ShapeCompareOptions widens the compare for engine pairs where extra
// catalog facts are faithful drift signals rather than translation
// noise. The zero value is the base compare.
type ShapeCompareOptions struct {
	// CompareCharset includes each string column's charset/collation
	// (audit 2026-07-16 M1.6). Set it only for same-storage-family
	// MySQL pairs, where information_schema resolves both to concrete
	// values on both sides. A side that surfaces NO value for a field
	// (mydumper DDL without an explicit per-column charset, a
	// column riding the table default) makes no claim: that field is
	// blanked on BOTH sides before the compare, so partially-populated
	// IR can never false-refuse (only a latin1-vs-utf8mb4-style
	// both-sides-resolved conflict does).
	CompareCharset bool
}

// TableColumnShape compares expected (the table the migration intends
// to create, already translated/retargeted to the target engine's
// storage shapes) against actual (the same-named table read back from
// the target catalog) and returns the column-shape mismatches, sorted
// by column name. An empty result means the existing table's column
// shape matches and the CREATE can be skipped.
//
// Nullability is NOT compared for columns in expected's primary key:
// both MySQL and PG force PK columns NOT NULL regardless of the
// declared flag, and readers report the enforced state — comparing the
// redundant flag would false-refuse a table the engine itself
// normalized. A wrong-nullability PK is unrepresentable on the target
// anyway. Integer AutoIncrement is likewise excluded for every column
// (see actualForCompare): it doesn't affect copy correctness and PG
// doesn't round-trip it.
//
// Primary-key PRESENCE is a mismatch in ONE direction (M1.7): expected
// has a PK, the actual table has none — the copy would land rows into
// a table that can silently accumulate duplicates the intended PK
// forbids, and every later phase assumes keyed access. The REVERSE
// (actual carries a PK the expected schema doesn't declare) is
// deliberately tolerated: the copy still works and a genuine collision
// fails loudly on the target's own constraint mid-copy; callers can
// surface it as a WARN via [UnexpectedPrimaryKey].
func TableColumnShape(expected, actual *ir.Table) []ColumnShapeMismatch {
	return TableColumnShapeWithOptions(expected, actual, ShapeCompareOptions{})
}

// TableColumnShapeWithOptions is [TableColumnShape] with the widened
// compare of [ShapeCompareOptions].
func TableColumnShapeWithOptions(expected, actual *ir.Table, opts ShapeCompareOptions) []ColumnShapeMismatch {
	if expected == nil || actual == nil {
		return nil
	}
	expCols := columnsByName(expected)
	actCols := columnsByName(actual)
	pkCols := primaryKeyColumnSet(expected)

	var out []ColumnShapeMismatch
	for name, exp := range expCols {
		act, ok := actCols[name]
		if !ok {
			out = append(out, ColumnShapeMismatch{
				Column:   name,
				Expected: renderColumnShapeOpts(exp, pkCols, opts),
				Actual:   columnShapeAbsent,
			})
			continue
		}
		expCmp, actCmp := exp, actualForCompare(act, exp, pkCols)
		if opts.CompareCharset {
			expCmp, actCmp = normalizeCharsetPair(expCmp, actCmp)
		}
		expShape := renderColumnShapeOpts(expCmp, pkCols, opts)
		actShape := renderColumnShapeOpts(actCmp, pkCols, opts)
		if expShape != actShape {
			out = append(out, ColumnShapeMismatch{Column: name, Expected: expShape, Actual: actShape})
		}
	}
	for name, act := range actCols {
		if _, ok := expCols[name]; ok {
			continue
		}
		out = append(out, ColumnShapeMismatch{
			Column:   name,
			Expected: columnShapeAbsent,
			Actual:   renderColumnShapeOpts(act, nil, opts),
		})
	}
	if len(pkCols) > 0 && len(primaryKeyColumnSet(actual)) == 0 {
		out = append(out, ColumnShapeMismatch{
			Column:   PrimaryKeyMismatchColumn,
			Expected: renderPrimaryKeyColumns(expected.PrimaryKey),
			Actual:   "none",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Column < out[j].Column })
	return out
}

// UnexpectedPrimaryKey reports (rendered, true) when the actual table
// carries a primary key the expected schema does not declare — the
// M1.7 tolerated direction, surfaced as a caller-side WARN rather than
// a mismatch (see TableColumnShape's doc comment).
func UnexpectedPrimaryKey(expected, actual *ir.Table) (string, bool) {
	if expected == nil || actual == nil {
		return "", false
	}
	if len(primaryKeyColumnSet(expected)) > 0 || len(primaryKeyColumnSet(actual)) == 0 {
		return "", false
	}
	return renderPrimaryKeyColumns(actual.PrimaryKey), true
}

// renderPrimaryKeyColumns renders a primary key's column list —
// "(id, ts)" — for the presence mismatch and the tolerated-extra WARN.
func renderPrimaryKeyColumns(pk *ir.Index) string {
	if pk == nil || len(pk.Columns) == 0 {
		return "none"
	}
	names := make([]string, len(pk.Columns))
	for i, ic := range pk.Columns {
		names[i] = ic.Column
	}
	return "(" + strings.Join(names, ", ") + ")"
}

// actualForCompare normalizes the actual column for the comparison:
// excluded fields are overwritten with the expected side's value so
// they can never produce a mismatch (see TableColumnShape's doc
// comment). The copy keeps the inputs pristine. Excluded:
//   - PK-member nullability (engines force NOT NULL; readers report
//     the enforced state).
//   - Integer AutoIncrement, for EVERY column: it never affects
//     whether the bulk copy can land rows (inserts carry explicit
//     values either way), and readers cannot always round-trip it —
//     PG reads a table whose id was created as bigserial/identity
//     back without the flag, which false-refused the Bug-81
//     sibling-shard load (a second shard migrating into the first
//     shard's tables) in CI. DDL for genuinely new tables still
//     carries the flag; only the skip-vs-refuse decision ignores it.
func actualForCompare(act, exp *ir.Column, pkCols map[string]struct{}) *ir.Column {
	normalized := *act
	if actInt, ok := normalized.Type.(ir.Integer); ok {
		if expInt, ok2 := exp.Type.(ir.Integer); ok2 {
			actInt.AutoIncrement = expInt.AutoIncrement
			normalized.Type = actInt
		}
	}
	if _, isPK := pkCols[act.Name]; isPK {
		normalized.Nullable = exp.Nullable
	}
	return &normalized
}

// renderColumnShapeOpts renders the compared portion of a column —
// type (+ charset/collation when opted in) + nullability — as one
// stable string, shared by the compare and the refusal message so what
// is compared is exactly what is shown.
func renderColumnShapeOpts(c *ir.Column, pkCols map[string]struct{}, opts ShapeCompareOptions) string {
	if c == nil {
		return columnShapeAbsent
	}
	shape := typeString(c.Type)
	if opts.CompareCharset {
		if charset, collation := columnCharset(c.Type); charset != "" || collation != "" {
			// Rendered exactly as compared: MySQL's own DDL vocabulary
			// so the refusal reads like the fix the operator will write.
			if charset != "" {
				shape += " CHARACTER SET " + charset
			}
			if collation != "" {
				shape += " COLLATE " + collation
			}
		}
	}
	if _, isPK := pkCols[c.Name]; isPK {
		// PK nullability is excluded from the compare; render the
		// column without a nullability clause so the two sides can't
		// disagree on the excluded field.
		return shape + ", PRIMARY KEY member"
	}
	if c.Nullable {
		return shape + ", NULL"
	}
	return shape + ", NOT NULL"
}

// columnCharset extracts the charset/collation pair from the string-
// leaf IR types that carry one. Every other type reports empty (no
// claim).
func columnCharset(t ir.Type) (charset, collation string) {
	switch v := t.(type) {
	case ir.Char:
		return v.Charset, v.Collation
	case ir.Varchar:
		return v.Charset, v.Collation
	case ir.Text:
		return v.Charset, v.Collation
	}
	return "", ""
}

// withColumnCharset returns t with its charset/collation replaced, for
// the both-sides normalization. Identity for non-string types.
func withColumnCharset(t ir.Type, charset, collation string) ir.Type {
	switch v := t.(type) {
	case ir.Char:
		v.Charset, v.Collation = charset, collation
		return v
	case ir.Varchar:
		v.Charset, v.Collation = charset, collation
		return v
	case ir.Text:
		v.Charset, v.Collation = charset, collation
		return v
	}
	return t
}

// normalizeCharsetPair applies the both-sides rule of the M1.6 charset
// compare: a field (charset and collation independently) participates
// only when BOTH sides surface a non-empty value — a side with no
// recorded value makes no claim, so the field is blanked on both
// (mirroring actualForCompare's exclusion technique). Copies; inputs
// stay pristine.
func normalizeCharsetPair(exp, act *ir.Column) (expNorm, actNorm *ir.Column) {
	expCS, expColl := columnCharset(exp.Type)
	actCS, actColl := columnCharset(act.Type)
	if expCS == "" || actCS == "" {
		expCS, actCS = "", ""
	}
	if expColl == "" || actColl == "" {
		expColl, actColl = "", ""
	}
	expOut, actOut := *exp, *act
	expOut.Type = withColumnCharset(exp.Type, expCS, expColl)
	actOut.Type = withColumnCharset(act.Type, actCS, actColl)
	return &expOut, &actOut
}

// primaryKeyColumnSet returns the set of column names in t's primary
// key (empty for a PK-less table).
func primaryKeyColumnSet(t *ir.Table) map[string]struct{} {
	if t == nil || t.PrimaryKey == nil {
		return nil
	}
	out := make(map[string]struct{}, len(t.PrimaryKey.Columns))
	for _, ic := range t.PrimaryKey.Columns {
		out[ic.Column] = struct{}{}
	}
	return out
}
